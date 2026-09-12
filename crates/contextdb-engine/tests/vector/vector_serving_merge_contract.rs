use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use std::collections::HashMap;
use std::sync::Arc;

struct Clock;
impl DeadlineClock for Clock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn parameters(query: &[f32]) -> HashMap<String, Value> {
    HashMap::from([
        ("query".into(), Value::Vector(query.to_vec())),
        ("bucket".into(), Value::Int64(-1)),
    ])
}

fn bounded_query(db: &Database, sql: &str, query: &[f32]) -> QueryResult {
    let limits = ReadLimits {
        work: 10_000_000,
        memory: 64 * 1024 * 1024,
        ..ReadLimits::default()
    };
    bounded::execute(
        db,
        &bounded::BoundedReadRequest::new(sql, parameters(query), limits, Arc::new(Clock)),
    )
    .expect("bounded maintained query")
    .result
}

fn fixture(mode: &str, vectors: &[Vec<f32>]) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, bucket INT, embedding VECTOR({}) WITH (quantization = '{mode}'))", vectors[0].len()), &HashMap::new()).unwrap();
    db.execute(
        "CREATE INDEX items_bucket ON items (bucket)",
        &HashMap::new(),
    )
    .unwrap();
    for (i, vector) in vectors.iter().enumerate() {
        db.execute(
            "INSERT INTO items (id, bucket, embedding) VALUES ($id, 0, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(i as i64 + 1)),
                ("vector".into(), Value::Vector(vector.clone())),
            ]),
        )
        .unwrap();
    }
    for _ in 0..32 {
        db.run_maintenance_cycle().unwrap();
    }
    db
}

fn ids(result: &QueryResult) -> Vec<i64> {
    result
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int64(id) => id,
            ref v => panic!("unexpected id {v:?}"),
        })
        .collect()
}

#[test]
fn query_policy_discloses_serving_snapshot_and_full_partition_search_breadth() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(directory.path().join("policy.redb")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, bucket INT, embedding VECTOR(3))",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX items_bucket ON items (bucket)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin().unwrap();
    for i in 0..201 {
        db.insert_row(
            tx,
            "items",
            HashMap::from([
                ("id".into(), Value::Int64(i + 1)),
                ("bucket".into(), Value::Int64(0)),
                (
                    "embedding".into(),
                    Value::Vector(vec![1.0, i as f32 / 201.0, 0.1]),
                ),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.run_maintenance_cycle().unwrap();
    let snapshot = db.snapshot();
    let _pin = db.pin_snapshot(snapshot);
    let query = [0.7, 0.3, 0.1];
    let sql = "SELECT id FROM items WHERE bucket = 0 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";
    db.execute(
        "UPDATE items SET bucket = 1 WHERE id > 100",
        &HashMap::new(),
    )
    .unwrap();
    let first = db.execute(sql, &parameters(&query)).unwrap();
    let initial = first.trace.vector_search.as_ref().unwrap().partition_hnsw[0].clone();
    assert_eq!(
        initial.hnsw_ef_search, 201,
        "residual cardinality does not choose graph effort"
    );
    for result in [first, bounded_query(&db, sql, &query)] {
        let disclosure = result.trace.vector_search.unwrap();
        assert_eq!(disclosure.aggregate_allowed_vectors, Some(100));
        assert_eq!(disclosure.partition_hnsw[0], initial);
    }
    assert!(
        db.__statement_cache_len() > 0,
        "the preceding UPDATE has a prepared plan"
    );
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET HNSW (M = 24, EF_SEARCH = 9)",
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(
        db.__statement_cache_len(),
        0,
        "public policy DDL invalidates cached write plans"
    );
    for result in [
        db.execute(sql, &parameters(&query)).unwrap(),
        bounded_query(&db, sql, &query),
    ] {
        let serving = &result.trace.vector_search.as_ref().unwrap().partition_hnsw[0];
        assert_eq!(serving.hnsw_m, initial.hnsw_m);
        assert_eq!(
            serving.policy_revision, initial.policy_revision,
            "unpublished topology is not serving"
        );
        assert_eq!(serving.hnsw_ef_search, 10);
        assert_eq!(serving.ef_search_source, "declared_raised_to_k");
    }
    db.execute(
        "INSERT INTO items VALUES (202, 0, [0.6, 0.4, 0.1])",
        &HashMap::new(),
    )
    .unwrap();
    db.run_maintenance_cycle().unwrap();
    let current = db.execute(sql, &parameters(&query)).unwrap();
    let serving = &current.trace.vector_search.as_ref().unwrap().partition_hnsw[0];
    assert_eq!(serving.hnsw_m, 24);
    assert!(serving.policy_revision > initial.policy_revision);
    let old = db
        .execute_at_snapshot(sql, &parameters(&query), snapshot)
        .unwrap();
    let serving = &old.trace.vector_search.as_ref().unwrap().partition_hnsw[0];
    assert_eq!(serving.hnsw_m, initial.hnsw_m);
    assert_eq!(serving.policy_revision, initial.policy_revision);
}

#[test]
fn updated_base_and_tail_publish_one_visible_row_per_identity() {
    let db = fixture("F32", &[vec![1., 0., 0.], vec![-1., 0., 0.]]);
    db.execute(
        "UPDATE items SET embedding = $query WHERE id = 1",
        &parameters(&[0.8, 0.2, -0.3]),
    )
    .unwrap();
    let sql =
        "SELECT id, score FROM items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";
    for result in [
        db.execute(sql, &parameters(&[0.7, 0.3, -0.2])).unwrap(),
        bounded_query(&db, sql, &[0.7, 0.3, -0.2]),
    ] {
        assert_eq!(ids(&result), vec![1, 2]);
    }
}

#[test]
fn filtered_completeness_belongs_to_the_merged_serving_layers() {
    let db = fixture("F32", &[vec![1., 0., 0.], vec![-1., 0., 0.]]);
    let sql = "SELECT id FROM items WHERE bucket < 5 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";
    for updated in [false, true] {
        if updated {
            db.execute(
                "UPDATE items SET embedding = $query WHERE id = 1",
                &parameters(&[0.8, 0.2, -0.3]),
            )
            .unwrap();
        }
        assert_eq!(
            ids(&db.execute(sql, &parameters(&[0.7, 0.3, -0.2])).unwrap()),
            vec![1, 2]
        );
        assert_eq!(ids(&bounded_query(&db, sql, &[0.7, 0.3, -0.2])), vec![1, 2]);
    }
}

#[test]
fn quantized_candidates_use_the_original_query_and_native_visible_scores() {
    let vectors = vec![
        vec![
            0.92420006,
            -0.065344706,
            -0.0862059,
            0.28458133,
            0.040478345,
            0.032273527,
            0.052576475,
            0.21845351,
        ],
        vec![
            0.8323085,
            0.04206606,
            -0.24857007,
            0.1824492,
            -0.410783,
            -0.023992881,
            0.19846992,
            0.041347172,
        ],
    ];
    let query = [
        0.9194869,
        -0.029469544,
        -0.27123886,
        0.047080915,
        -0.016611757,
        0.25136214,
        0.11614887,
        -0.030639917,
    ];
    for mode in ["F32", "SQ8", "SQ4"] {
        let db = fixture(mode, &vectors);
        let mut reference = SemanticQuery::new("items", "embedding", query.to_vec(), 10);
        reference.search_mode = Some(VectorSearchMode::Exact);
        let exact = db.semantic_search(reference).unwrap();
        let sql =
            "SELECT id, score FROM items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";
        let ordinary = db.execute(sql, &parameters(&query)).unwrap();
        let bounded = bounded_query(&db, sql, &query);
        assert_eq!(ordinary.rows, bounded.rows, "reader parity for {mode}");
        let expected = db
            .execute(&sql.replace("INDEXED", "EXACT"), &parameters(&query))
            .unwrap();
        assert_eq!(
            ordinary.rows, expected.rows,
            "native order and scores for {mode}: {exact:?}"
        );
        if mode == "SQ4" {
            assert_eq!(ids(&ordinary), vec![1, 2]);
            assert_eq!(
                ordinary.rows[0][1],
                Value::Float64(f32::from_bits(0x3f61efca) as f64)
            );
            assert_eq!(
                ordinary.rows[1][1],
                Value::Float64(f32::from_bits(0x3f604e99) as f64)
            );
        }
    }
}

#[test]
fn signed_constants_and_parameters_share_the_index_route() {
    let db = fixture("F32", &[vec![1., 0., 0.], vec![-1., 0., 0.]]);
    for predicate in [
        "bucket = -1",
        "bucket = $bucket",
        "bucket < -1",
        "bucket = -(1 + 2)",
    ] {
        let sql = format!(
            "SELECT id FROM items WHERE {predicate} ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
        );
        assert!(
            db.execute(&sql, &parameters(&[1., 0., 0.]))
                .unwrap()
                .rows
                .is_empty()
        );
        assert!(bounded_query(&db, &sql, &[1., 0., 0.]).rows.is_empty());
    }
}

#[test]
fn exact_discloses_its_actual_allowed_membership() {
    let db = fixture("F32", &[vec![1., 0., 0.], vec![-1., 0., 0.]]);
    let sql = "SELECT id FROM items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10";
    for result in [
        db.execute(sql, &parameters(&[1., 0., 0.])).unwrap(),
        bounded_query(&db, sql, &[1., 0., 0.]),
    ] {
        assert_eq!(
            result
                .trace
                .vector_search
                .unwrap()
                .aggregate_allowed_vectors,
            Some(2)
        );
    }
}

#[test]
fn partition_key_only_update_is_visible_in_the_writing_transaction() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket))", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO items VALUES (1, 1, [1.0, 0.0, 0.0]), (2, 2, [-1.0, 0.0, 0.0])",
        &HashMap::new(),
    )
    .unwrap();
    for _ in 0..32 {
        db.run_maintenance_cycle().unwrap();
    }
    for commit in [false, true] {
        db.execute("BEGIN", &HashMap::new()).unwrap();
        db.execute("UPDATE items SET bucket = 2 WHERE id = 1", &HashMap::new())
            .unwrap();
        for mode in ["AUTO", "EXACT", "INDEXED"] {
            for bucket in [1, 2] {
                let sql = format!(
                    "SELECT id FROM items WHERE bucket = {bucket} ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
                );
                let result = db.execute(&sql, &parameters(&[1., 0., 0.])).unwrap();
                assert_eq!(
                    ids(&result),
                    if bucket == 1 { vec![] } else { vec![1, 2] },
                    "staged {mode} bucket {bucket}"
                );
                assert_eq!(
                    bounded_query(&db, &sql, &[1., 0., 0.]).rows,
                    result.rows,
                    "bounded transaction {mode} bucket {bucket}"
                );
            }
        }
        db.execute(if commit { "COMMIT" } else { "ROLLBACK" }, &HashMap::new())
            .unwrap();
    }
}

fn generated_vector(seed: u64, dimensions: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(17);
    (0..dimensions)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as i32 - (1 << 23)) as f32 / (1 << 23) as f32
        })
        .collect()
}

#[test]
fn held_out_filtered_graph_search_preserves_quality_at_sparse_and_broad_selectivities() {
    for mode in ["F32", "SQ8", "SQ4"] {
        let db = fixture(
            mode,
            &(0..1000)
                .map(|i| generated_vector(i + 1, 32))
                .collect::<Vec<_>>(),
        );
        for id in 1..=1000 {
            db.execute(
                "UPDATE items SET bucket = $bucket WHERE id = $id",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("bucket".into(), Value::Int64(id % 100)),
                ]),
            )
            .unwrap();
        }
        for selected in [1, 10, 50, 100] {
            let mut recovered = 0;
            for query_id in 0..8 {
                let query = generated_vector(10_000 + query_id, 32);
                let sql = format!(
                    "SELECT id, score FROM items WHERE bucket < {selected} ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10"
                );
                let expected = db
                    .execute(&sql.replace("INDEXED", "EXACT"), &parameters(&query))
                    .unwrap();
                let actual = db
                    .execute(&sql, &parameters(&query))
                    .unwrap_or_else(|error| panic!("{mode} selectivity {selected}: {error}"));
                let mut semantic = SemanticQuery::new("items", "embedding", query.clone(), 10);
                semantic.where_clause = Some(format!("bucket < {selected}"));
                semantic.search_mode = Some(VectorSearchMode::Indexed);
                db.__reset_last_query_vector_trace_for_test();
                let embedded = db.semantic_search(semantic.clone()).unwrap();
                let plain_trace = db.__take_last_query_vector_trace_for_test().unwrap();
                if selected == 100 && query_id == 0 {
                    for threshold in [0.0, embedded[4].vector_score] {
                        semantic.min_similarity = Some(threshold);
                        db.__reset_last_query_vector_trace_for_test();
                        let filtered = db.semantic_search(semantic.clone()).unwrap();
                        let trace = db.__take_last_query_vector_trace_for_test().unwrap();
                        println!(
                            "minimum_similarity quantization={mode} threshold={threshold} plain_ef={:?} threshold_ef={:?} plain_candidates={} threshold_candidates={}",
                            plain_trace.hnsw_ef_search,
                            trace.hnsw_ef_search,
                            plain_trace.hnsw_candidate_count,
                            trace.hnsw_candidate_count
                        );
                        assert!(trace.used_hnsw);
                        assert_eq!(
                            trace.hnsw_ef_search, plain_trace.hnsw_ef_search,
                            "minimum similarity filters the requested neighbours without expanding search breadth"
                        );
                        assert_eq!(
                            trace.hnsw_candidate_row_ids,
                            plain_trace.hnsw_candidate_row_ids
                        );
                        assert_eq!(
                            filtered
                                .iter()
                                .map(|row| (row.row_id, row.vector_score))
                                .collect::<Vec<_>>(),
                            embedded
                                .iter()
                                .filter(|row| row.vector_score >= threshold)
                                .map(|row| (row.row_id, row.vector_score))
                                .collect::<Vec<_>>()
                        );
                    }
                }
                assert_eq!(
                    embedded
                        .iter()
                        .map(|r| r.values["id"].as_i64().unwrap())
                        .collect::<Vec<_>>(),
                    ids(&actual),
                    "{mode} selectivity {selected} embedded parity"
                );
                let bounded = bounded_query(&db, &sql, &query);
                assert_eq!(
                    actual.rows, bounded.rows,
                    "{mode} selectivity {selected} reader parity"
                );
                let expected_ids = ids(&expected);
                recovered += ids(&actual)
                    .iter()
                    .filter(|id| expected_ids.contains(id))
                    .count();
                for row in &actual.rows {
                    if let Some(reference) = expected
                        .rows
                        .iter()
                        .find(|reference| reference[0] == row[0])
                    {
                        assert_eq!(
                            row, reference,
                            "every retained candidate has its native score"
                        );
                    }
                }
            }
            println!("quantization={mode} selected_percent={selected} recovered={recovered}/80");
            assert!(
                recovered >= 76,
                "{mode} selectivity {selected} recovered {recovered}/80"
            );
        }
    }
}

#[test]
fn bounded_search_and_newer_declaration_complete_while_replacement_encoding_is_paused() {
    use contextdb_core::{VectorIndexRef, VectorPartitionKey};
    use contextdb_engine::VectorMaintenancePreparationPhaseForTest;
    use contextdb_vector::VectorPartitionRef;
    use std::{sync::mpsc, thread, time::Duration};
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(directory.path().join("online.redb")).unwrap());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(8) PARTITION_KEY (bucket) MAX_PARTITIONS 256 AUTO_INDEX_AT 1234 HNSW (M = 24, EF_CONSTRUCTION = 400))", &HashMap::new()).unwrap();
    for id in 1..=200 {
        db.execute(
            "INSERT INTO items VALUES ($id, 0, $query)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                (
                    "query".into(),
                    Value::Vector(generated_vector(id as u64, 8)),
                ),
            ]),
        )
        .unwrap();
    }
    for _ in 0..4 {
        db.run_maintenance_cycle().unwrap();
    }
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET HNSW (EF_SEARCH = 7)",
        &HashMap::new(),
    )
    .unwrap();
    let sql = "SELECT id FROM items WHERE bucket = 0 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";
    let expected = ids(&bounded_query(&db, sql, &generated_vector(9000, 8)));
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET HNSW (M = 32, EF_CONSTRUCTION = 500)",
        &HashMap::new(),
    )
    .unwrap();
    let route = VectorPartitionRef::new(
        VectorIndexRef::new("items", "embedding"),
        VectorPartitionKey::from_values(&[Value::Int64(0)]).unwrap(),
    );
    let pause = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route,
        VectorMaintenancePreparationPhaseForTest::EncodeGraph,
    );
    let maintenance_db = db.clone();
    let maintenance = thread::spawn(move || maintenance_db.run_maintenance_cycle());
    pause
        .wait_until_reached_timeout(Duration::from_secs(10))
        .unwrap();
    let reader = db.clone();
    let (sender, receiver) = mpsc::channel();
    let query = thread::spawn(move || {
        let result = reader
            .read_session(ReadLimits::default())
            .unwrap()
            .execute(sql, &parameters(&generated_vector(9000, 8)))
            .unwrap();
        reader.execute("ALTER TABLE items ALTER COLUMN embedding SET HNSW (M = 28, EF_CONSTRUCTION = 420, EF_SEARCH = 128)", &HashMap::new()).unwrap();
        sender.send(ids(&result)).unwrap();
    });
    let completed_before_release = receiver.recv_timeout(Duration::from_secs(5));
    pause.release();
    maintenance.join().unwrap().unwrap();
    query.join().unwrap();
    assert_eq!(
        completed_before_release
            .expect("query and newer declaration must complete before the safety release"),
        expected
    );
    let observed = db
        .execute(
            "SHOW VECTOR_PARTITIONS FOR items.embedding",
            &HashMap::new(),
        )
        .unwrap();
    let value = |name: &str| {
        &observed.rows[0][observed
            .columns
            .iter()
            .position(|column| column == name)
            .unwrap()]
    };
    assert_eq!(
        value("serving_hnsw_m"),
        &Value::Int64(24),
        "the stale candidate cannot replace the serving base"
    );
    assert_eq!(value("desired_hnsw_m"), &Value::Int64(28));
}

#[test]
fn a_long_update_history_does_not_fill_indexed_result_slots_with_one_identity() {
    let db = fixture("SQ8", &[vec![1., 0., 0.], vec![-1., 0., 0.]]);
    for update in 0..160 {
        db.execute(
            "UPDATE items SET embedding = $query WHERE id = 1",
            &parameters(&[0.8, 0.2 + update as f32 * 0.0001, -0.3]),
        )
        .unwrap();
    }
    let sql =
        "SELECT id, score FROM items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10";
    let exact = db
        .execute(
            &sql.replace("INDEXED", "EXACT"),
            &parameters(&[0.7, 0.3, -0.2]),
        )
        .unwrap();
    let indexed = db.execute(sql, &parameters(&[0.7, 0.3, -0.2])).unwrap();
    assert_eq!(indexed.rows, exact.rows);
    assert_eq!(bounded_query(&db, sql, &[0.7, 0.3, -0.2]).rows, exact.rows);
}

#[test]
fn quantized_partition_moves_preserve_native_scores_and_rollback() {
    for mode in ["F32", "SQ8", "SQ4"] {
        let db = Database::open_memory();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(32) WITH (quantization = '{mode}') PARTITION_KEY (bucket))"), &HashMap::new()).unwrap();
        for id in 1..=2 {
            db.execute(
                "INSERT INTO items VALUES ($id, $id, $query)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    (
                        "query".into(),
                        Value::Vector(generated_vector(id as u64, 32)),
                    ),
                ]),
            )
            .unwrap();
        }
        for _ in 0..4 {
            db.run_maintenance_cycle().unwrap();
        }
        let query = generated_vector(9021, 32);
        let before = db.execute("SELECT id, score FROM items WHERE bucket = 1 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10", &parameters(&query)).unwrap();
        db.execute("BEGIN", &HashMap::new()).unwrap();
        db.execute("UPDATE items SET bucket = 2 WHERE id = 1", &HashMap::new())
            .unwrap();
        for route in ["AUTO", "EXACT", "INDEXED"] {
            let sql = format!(
                "SELECT id, score FROM items WHERE bucket = 2 ORDER BY embedding <=> $query USE VECTOR {route} LIMIT 10"
            );
            for result in [
                db.execute(&sql, &parameters(&query)).unwrap(),
                bounded_query(&db, &sql, &query),
            ] {
                assert_eq!(
                    result.rows.iter().find(|row| row[0] == Value::Int64(1)),
                    before.rows.first(),
                    "native moved score {mode} {route}"
                );
            }
        }
        db.execute("ROLLBACK", &HashMap::new()).unwrap();
    }
}

#[test]
fn durable_native_scores_and_unique_results_survive_updates_and_restart() {
    for mode in ["F32", "SQ8", "SQ4"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("native.redb");
        let mut db = Database::open(&path).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, bucket INT, embedding VECTOR(32) WITH (quantization = '{mode}'))"), &HashMap::new()).unwrap();
        db.execute(
            "CREATE INDEX items_bucket ON items (bucket)",
            &HashMap::new(),
        )
        .unwrap();
        for id in 1..=100 {
            db.execute(
                "INSERT INTO items VALUES ($id, 0, $query)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    (
                        "query".into(),
                        Value::Vector(generated_vector(id as u64, 32)),
                    ),
                ]),
            )
            .unwrap();
        }
        for _ in 0..4 {
            db.run_maintenance_cycle().unwrap();
        }
        for stage in 0..4 {
            if stage == 1 {
                db.execute(
                    "UPDATE items SET embedding = $query WHERE id = 1",
                    &parameters(&generated_vector(510, 32)),
                )
                .unwrap();
            }
            if stage == 2 {
                for _ in 0..4 {
                    db.run_maintenance_cycle().unwrap();
                }
            }
            if stage == 3 {
                db.close().unwrap();
                drop(db);
                db = Database::open(&path).unwrap();
                db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            }
            for probe in 0..4 {
                let query = generated_vector(9000 + probe, 32);
                for filter in ["", "WHERE bucket < 5"] {
                    let sql = format!(
                        "SELECT id, score FROM items {filter} ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 100"
                    );
                    let reference = db
                        .execute(&sql.replace("INDEXED", "EXACT"), &parameters(&query))
                        .unwrap();
                    for result in [
                        db.execute(&sql, &parameters(&query)).unwrap(),
                        bounded_query(&db, &sql, &query),
                    ] {
                        assert_eq!(
                            result.rows, reference.rows,
                            "native durable {mode} stage {stage} probe {probe}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn current_only_cleanup_keeps_native_candidate_order_across_readers_and_restart() {
    use contextdb_core::{VectorIndexRef, VectorPartitionKey};
    use contextdb_vector::VectorPartitionRef;

    for quantization in ["F32", "SQ8", "SQ4"] {
        for partitioned in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("cleanup-score.redb");
            let mut db = Database::open(&path).unwrap();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            let layout = if partitioned {
                "PARTITION_KEY (bucket)"
            } else {
                ""
            };
            db.execute(
                &format!(
                    "CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, \
                     embedding VECTOR(3) WITH (quantization = '{quantization}') \
                     {layout} AUTO_INDEX_AT 1) HISTORY CURRENT ONLY SYNC OFF"
                ),
                &HashMap::new(),
            )
            .unwrap();
            db.execute(
                "INSERT INTO items VALUES (1, 0, [1.0, 0.0, 0.0]), \
                 (2, 0, [0.8, 0.6, 0.0])",
                &HashMap::new(),
            )
            .unwrap();
            let index = VectorIndexRef::new("items", "embedding");
            let key = if partitioned {
                VectorPartitionKey::from_values(&[Value::Int64(0)]).unwrap()
            } else {
                VectorPartitionKey::default()
            };
            let route = VectorPartitionRef::new(index.clone(), key);
            let generation = (0..32)
                .find_map(|_| {
                    db.run_maintenance_cycle().unwrap();
                    db.vector_store_for_test()
                        .partition_graph_generation_status(&route)
                        .and_then(|status| status.base)
                })
                .expect("the counterexample needs an immutable base with the old score");
            let query = [1.0, 0.0, 0.0];
            let old_snapshot = db.snapshot();
            let pin = db.pin_snapshot(old_snapshot);
            let sql = "SELECT id, score FROM items ORDER BY embedding <=> $query \
                       USE VECTOR INDEXED LIMIT 2";
            let old_answer = db.execute(sql, &parameters(&query)).unwrap();
            assert_eq!(ids(&old_answer), vec![1, 2]);
            db.execute(
                "UPDATE items SET embedding = [0.0, 1.0, 0.0] WHERE id = 1",
                &HashMap::new(),
            )
            .unwrap();
            let deferred = db.compact_currency_versions().unwrap();
            assert_eq!(deferred.pruned_versions, 0);
            assert_eq!(deferred.versions_deferred_for_readers, 1);
            assert_eq!(
                db.execute_at_snapshot(sql, &parameters(&query), old_snapshot)
                    .unwrap()
                    .rows,
                old_answer.rows,
                "cleanup preserves the old reader's native score"
            );
            let old_bounded = db.__with_snapshot_override_for_test(old_snapshot, || {
                bounded_query(&db, sql, &query)
            });
            assert_eq!(old_bounded.rows, old_answer.rows);
            drop(pin);
            let cleaned = db.compact_currency_versions().unwrap();
            assert_eq!(cleaned.pruned_versions, 1);
            assert_eq!(cleaned.vector_keys_rewritten, 1);
            assert_eq!(
                db.vector_store_for_test().raw_directory_entries().len(),
                2,
                "cleanup must actually leave one version of each row"
            );
            assert_eq!(
                db.vector_store_for_test()
                    .partition_graph_generation_status(&route)
                    .unwrap()
                    .base,
                Some(generation),
                "cleanup must leave the old graph point in the serving base"
            );

            for restart in [false, true] {
                if restart {
                    db.close().unwrap();
                    drop(db);
                    db = Database::open(&path).unwrap();
                    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
                }
                let expected = db
                    .execute(&sql.replace("INDEXED", "EXACT"), &parameters(&query))
                    .unwrap();
                assert_eq!(ids(&expected), vec![2, 1], "the stable row is truly nearer");
                db.vector_store_for_test().evict_reloadable_raw_partitions();
                let activity_before = db.__vector_passive_activity_counters_for_test();
                for mode in ["AUTO", "INDEXED"] {
                    for limit in [1, 2] {
                        let sql = format!(
                            "SELECT id, score FROM items ORDER BY embedding <=> $query \
                             USE VECTOR {mode} LIMIT {limit}"
                        );
                        db.__reset_last_query_vector_trace_for_test();
                        let ordinary = db.execute(&sql, &parameters(&query)).unwrap();
                        let trace = db.__take_last_query_vector_trace_for_test().unwrap();
                        assert!(trace.used_hnsw);
                        assert_eq!(trace.supplemented_row_count, 0);
                        assert_eq!(ordinary.rows, expected.rows[..limit]);
                        db.__reset_last_query_vector_trace_for_test();
                        let bounded = bounded_query(&db, &sql, &query);
                        let trace = db.__take_last_query_vector_trace_for_test().unwrap();
                        assert!(trace.used_hnsw);
                        assert_eq!(trace.supplemented_row_count, 0);
                        assert_eq!(
                            bounded.rows, ordinary.rows,
                            "{quantization}, partitioned={partitioned}, restart={restart}"
                        );
                    }
                }
                let activity_after = db.__vector_passive_activity_counters_for_test();
                assert_eq!(
                    activity_after.raw_partition_loads,
                    activity_before.raw_partition_loads
                );
                assert_eq!(activity_after.hnsw_builds, activity_before.hnsw_builds);
                assert_eq!(
                    activity_after.hnsw_repairs_or_compactions,
                    activity_before.hnsw_repairs_or_compactions
                );
                let again = db.compact_currency_versions().unwrap();
                assert_eq!(again.pruned_versions, 0);
                assert_eq!(again.vector_keys_rewritten, 0);
                assert_eq!(again.commit_index_keys_removed, 0);
            }
            db.close().unwrap();
        }
    }
}

#[test]
fn embedded_graph_scratch_obeys_memory_admission_and_returns_its_charge() {
    let db = fixture(
        "F32",
        &(1..=256)
            .map(|seed| generated_vector(seed, 32))
            .collect::<Vec<_>>(),
    );
    let resident = db.accountant().usage().used;
    let mut query = SemanticQuery::new("items", "embedding", generated_vector(10_000, 32), 10);
    query.search_mode = Some(VectorSearchMode::Indexed);
    db.set_memory_limit(Some(resident)).unwrap();
    let error = db.semantic_search(query.clone()).unwrap_err();
    assert!(
        matches!(error, contextdb_core::Error::MemoryBudgetExceeded { ref operation, .. } if operation == "graph_scratch"),
        "{error:?}"
    );
    assert_eq!(db.accountant().usage().used, resident);
    db.set_memory_limit(Some(resident + 1024 * 1024)).unwrap();
    assert_eq!(db.semantic_search(query).unwrap().len(), 10);
    assert!(db.__debug_last_query_vector_used_hnsw_for_test());
    assert_eq!(db.accountant().usage().used, resident);
}

#[test]
fn closer_noncurrent_rows_do_not_change_current_results_or_add_complete_entry_passes() {
    use contextdb_vector::hnsw::{
        HnswCandidateDistanceEvent, HnswCandidateObserver, with_hnsw_candidate_observer,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Distances(AtomicUsize);
    impl HnswCandidateObserver for Distances {
        fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(directory.path().join("current-history.redb")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, is_current BOOLEAN NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (bucket) AUTO_INDEX_AT 16 \
         HNSW (M = 8, EF_CONSTRUCTION = 64, EF_SEARCH = 64))",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX current_items ON items (is_current)",
        &HashMap::new(),
    )
    .unwrap();
    let tx = db.begin().unwrap();
    for id in 1..=32 {
        db.insert_row(
            tx,
            "items",
            HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("bucket".into(), Value::Int64(0)),
                ("is_current".into(), Value::Bool(true)),
                (
                    "embedding".into(),
                    Value::Vector(vec![1.0, 0.1 + id as f32 * 0.01, 0.0]),
                ),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    for _ in 0..32 {
        db.run_maintenance_cycle().unwrap();
    }
    let query = [1.0, 0.001, 0.0]; // Held out: none of the stored vectors equals this query.
    let current_sql = "SELECT id, score FROM items WHERE is_current = TRUE \
                       ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";
    let exact = db
        .execute(
            &current_sql.replace("INDEXED", "EXACT"),
            &parameters(&query),
        )
        .unwrap();
    assert_eq!(ids(&exact), vec![1, 2, 3, 4, 5]);
    let snapshot = db.snapshot();
    let _pin = db.pin_snapshot(snapshot);

    for (start, end) in [(100, 612), (612, 4196)] {
        let tx = db.begin().unwrap();
        for id in start..end {
            db.insert_row(
                tx,
                "items",
                HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("bucket".into(), Value::Int64(0)),
                    ("is_current".into(), Value::Bool(false)),
                    ("embedding".into(), Value::Vector(vec![1.0, 0.0, 0.0])),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        // The unfiltered exact control proves that historical rows really are
        // closer and that the whole-entry source observation is armed.
        let before = db
            .vector_store_for_test()
            .passive_activity_counters_for_test();
        let all = db
            .execute(
                "SELECT id FROM items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 5",
                &parameters(&query),
            )
            .unwrap();
        assert_eq!(ids(&all), vec![100, 101, 102, 103, 104]);
        let after = db
            .vector_store_for_test()
            .passive_activity_counters_for_test();
        assert!(after.whole_entry_slice_requests > before.whole_entry_slice_requests);
        assert!(
            after.whole_entry_slice_entries_exposed - before.whole_entry_slice_entries_exposed
                >= (end - 100 + 32) as u64
        );

        for mode in ["AUTO", "INDEXED"] {
            let sql = current_sql.replace("INDEXED", mode);
            let before = db
                .vector_store_for_test()
                .passive_activity_counters_for_test();
            let distances = Arc::new(Distances::default());
            db.__reset_last_query_vector_trace_for_test();
            let ordinary = with_hnsw_candidate_observer(distances.clone(), || {
                db.execute(&sql, &parameters(&query)).unwrap()
            });
            assert_eq!(
                ordinary.rows,
                exact.rows,
                "{mode} history={} filters before top-k",
                end - 100
            );
            let trace = db.__take_last_query_vector_trace_for_test().unwrap();
            assert!(trace.used_hnsw);
            assert_eq!(trace.supplemented_row_count, 0);
            assert!(distances.0.load(Ordering::SeqCst) > 0);
            assert!(
                distances.0.load(Ordering::SeqCst) < 256,
                "current membership bounds graph distance work"
            );
            let request = bounded::BoundedReadRequest::new(
                &sql,
                parameters(&query),
                ReadLimits {
                    work: 10_000_000,
                    memory: 64 * 1024 * 1024,
                    ..ReadLimits::default()
                },
                Arc::new(Clock),
            );
            let bounded = bounded::execute(&db, &request).unwrap();
            assert_eq!(bounded.result.rows, exact.rows);
            let trace = db.__take_last_query_vector_trace_for_test().unwrap();
            assert!(trace.used_hnsw);
            assert_eq!(trace.supplemented_row_count, 0);
            assert_eq!(
                bounded
                    .result
                    .trace
                    .vector_search
                    .as_ref()
                    .unwrap()
                    .aggregate_allowed_vectors,
                Some(32)
            );
            let source_work = bounded
                .telemetry
                .source_work
                .get(&bounded::TestWorkSource::VectorCandidates)
                .copied()
                .unwrap_or(0);
            assert!(
                source_work > 0 && source_work < 4096,
                "history={} vector source work={source_work}",
                end - 100
            );
            println!(
                "mode={mode} history={} current=32 ordinary_ids={:?} bounded_ids={:?} distances={} source={source_work}",
                end - 100,
                ids(&ordinary),
                ids(&bounded.result),
                distances.0.load(Ordering::SeqCst)
            );
            let after = db
                .vector_store_for_test()
                .passive_activity_counters_for_test();
            assert_eq!(
                after.whole_entry_slice_requests, before.whole_entry_slice_requests,
                "neither reader may reacquire a complete entry slice for graph coverage or supplementation"
            );
            assert_eq!(
                after.whole_entry_slice_entries_exposed,
                before.whole_entry_slice_entries_exposed
            );
        }
    }

    // Replacing a current passage changes new membership but retains the complete
    // old answer for the already-open snapshot, including the old current flag.
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "UPDATE items SET is_current = FALSE WHERE id = 1",
        &HashMap::new(),
    )
    .unwrap();
    db.insert_row(
        tx,
        "items",
        HashMap::from([
            ("id".into(), Value::Int64(9999)),
            ("bucket".into(), Value::Int64(0)),
            ("is_current".into(), Value::Bool(true)),
            ("embedding".into(), Value::Vector(vec![1.0, 0.01, 0.0])),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    for mode in ["AUTO", "EXACT", "INDEXED"] {
        let sql = current_sql.replace("INDEXED", mode);
        assert_eq!(
            ids(&db.execute(&sql, &parameters(&query)).unwrap()),
            vec![9999, 2, 3, 4, 5]
        );
        assert_eq!(
            ids(&bounded_query(&db, &sql, &query)),
            vec![9999, 2, 3, 4, 5]
        );
        assert_eq!(
            db.execute_at_snapshot(&sql, &parameters(&query), snapshot)
                .unwrap()
                .rows,
            exact.rows
        );
        assert_eq!(
            db.__with_snapshot_override_for_test(snapshot, || bounded_query(&db, &sql, &query))
                .rows,
            exact.rows
        );
    }
}

#[test]
fn global_ties_keep_internal_row_order_across_partition_load_permutations() {
    use contextdb_core::{VectorIndexRef, VectorPartitionKey};
    use contextdb_vector::VectorPartitionRef;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tied-partitions.redb");
    let mut db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (bucket) AUTO_INDEX_AT 1)",
        &HashMap::new(),
    )
    .unwrap();
    let insertion_order = [(90, 2), (20, 1), (80, 0), (10, 2), (70, 0), (30, 1)];
    let tx = db.begin().unwrap();
    let mut row_ids = Vec::new();
    for (id, bucket) in insertion_order {
        row_ids.push(
            db.insert_row(
                tx,
                "items",
                HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("bucket".into(), Value::Int64(bucket)),
                    ("embedding".into(), Value::Vector(vec![0.8, 0.6, 0.0])),
                ]),
            )
            .unwrap(),
        );
    }
    assert!(row_ids.windows(2).all(|pair| pair[0] < pair[1]));
    db.commit(tx).unwrap();
    for _ in 0..32 {
        db.run_maintenance_cycle().unwrap();
    }
    let route = |bucket| {
        VectorPartitionRef::new(
            VectorIndexRef::new("items", "embedding"),
            VectorPartitionKey::from_values(&[Value::Int64(bucket)]).unwrap(),
        )
    };
    let query = [0.8, 0.6, 0.0];
    let all_sql =
        "SELECT id, score FROM items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 4";
    let before_close = db.execute(all_sql, &parameters(&query)).unwrap();
    assert_eq!(ids(&before_close), vec![90, 20, 80, 10]);
    db.close().unwrap();
    drop(db);

    for order in [
        [2, 0, 1],
        [1, 0, 2],
        [0, 2, 1],
        [2, 1, 0],
        [1, 2, 0],
        [0, 1, 2],
    ] {
        db = Database::open(&path).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        for bucket in 0..3 {
            let status = db
                .vector_store_for_test()
                .partition_graph_generation_status(&route(bucket))
                .unwrap();
            assert!(status.base.is_some() || status.dormant_base.is_some());
            assert!(
                !status.base_resident && !status.change_resident,
                "each permutation starts with genuinely unloaded saved partitions"
            );
        }
        fn with_loaded_order(
            db: &Database,
            order: &[i64],
            loaded: &mut Vec<i64>,
            insertion_order: &[(i64, i64)],
            query: &[f32],
            route: &impl Fn(i64) -> VectorPartitionRef,
            check: &impl Fn(),
        ) {
            let Some((&bucket, remaining)) = order.split_first() else {
                check();
                return;
            };
            let sql = format!(
                "SELECT id FROM items WHERE bucket = {bucket} ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 4"
            );
            let result = db.execute(&sql, &parameters(query)).unwrap();
            let expected = insertion_order
                .iter()
                .filter(|(_, b)| *b == bucket)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            assert_eq!(ids(&result), expected);
            loaded.push(bucket);
            for candidate in 0..3 {
                let status = db
                    .vector_store_for_test()
                    .partition_graph_generation_status(&route(candidate))
                    .unwrap();
                assert_eq!(
                    status.base_resident || status.change_resident,
                    loaded.contains(&candidate),
                    "warmup must really load only the partitions named so far"
                );
            }
            db.vector_store_for_test()
                .with_resident_partition_generation(&route(bucket), true, |_, _| {
                    with_loaded_order(db, remaining, loaded, insertion_order, query, route, check);
                })
                .expect("the selected saved base stays pinned by this read guard");
        }
        // Idle graphs are evictable; keep actual readers on the earlier loads
        // while verifying each permutation's simultaneous resident topology.
        with_loaded_order(
            &db,
            &order,
            &mut Vec::new(),
            &insertion_order,
            &query,
            &route,
            &|| {
                for (predicate, expected) in [
                    ("WHERE bucket = 2", vec![90, 10]),
                    ("WHERE bucket IN (2, 1)", vec![90, 20, 10, 30]),
                    ("", vec![90, 20, 80, 10]),
                ] {
                    for mode in ["AUTO", "EXACT", "INDEXED"] {
                        let sql = format!(
                            "SELECT id, score FROM items {predicate} ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 4"
                        );
                        let ordinary = db.execute(&sql, &parameters(&query)).unwrap();
                        assert_eq!(
                            ids(&ordinary),
                            expected,
                            "load order={order:?}, mode={mode}, scope={predicate}"
                        );
                        assert_eq!(bounded_query(&db, &sql, &query).rows, ordinary.rows);
                        assert!(
                            ordinary
                                .rows
                                .windows(2)
                                .all(|pair| pair[0][1] == pair[1][1])
                        );
                        if predicate.is_empty() {
                            assert_eq!(ordinary.rows, before_close.rows);
                        }
                    }
                }
            },
        );
        db.close().unwrap();
        drop(db);
    }
}
