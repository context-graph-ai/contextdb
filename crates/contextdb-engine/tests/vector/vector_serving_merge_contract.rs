use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::table_meta::VectorQuantization;
use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use contextdb_vector::stored_vector_value;
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
    db
}

fn ids(result: &QueryResult) -> Vec<i64> {
    result
        .rows()
        .iter()
        .map(|r| match r[0] {
            Value::Int64(id) => id,
            ref v => panic!("unexpected id {v:?}"),
        })
        .collect()
}

fn used_indexed_route(trace: &contextdb_engine::database::VectorSearchTraceObservation) -> bool {
    trace
        .partition_routes
        .iter()
        .any(|route| route.route == contextdb_vector::mem::VectorPartitionSearchRoute::Indexed)
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
    let snapshot = db.snapshot();
    let _pin = db.pin_snapshot(snapshot).unwrap();
    let query = [0.7, 0.3, 0.1];
    let sql = "SELECT id FROM items WHERE bucket = 0 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10";
    db.execute(
        "UPDATE items SET bucket = 1 WHERE id > 100",
        &HashMap::new(),
    )
    .unwrap();
    let first = db.execute(sql, &parameters(&query)).unwrap();
    for result in [first, bounded_query(&db, sql, &query)] {
        let disclosure = result.trace().vector_search.as_ref().unwrap();
        assert_eq!(disclosure.aggregate_allowed_vectors, Some(100));
    }
    assert!(
        db.__statement_cache_len() > 0,
        "the preceding UPDATE has a prepared plan"
    );
    db.execute(
        "ALTER TABLE items ALTER COLUMN embedding SET IVF (PROBES = 9)",
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
        let disclosure = result.trace().vector_search.as_ref().unwrap();
        assert_eq!(disclosure.aggregate_allowed_vectors, Some(100));
    }
    db.execute(
        "INSERT INTO items VALUES (202, 0, [0.6, 0.4, 0.1])",
        &HashMap::new(),
    )
    .unwrap();
    let current = db.execute(sql, &parameters(&query)).unwrap();
    assert_eq!(
        current
            .trace()
            .vector_search
            .as_ref()
            .unwrap()
            .aggregate_allowed_vectors,
        Some(101),
        "the current answer counts the rows allowed now"
    );
    let old = db
        .execute_at_snapshot(sql, &parameters(&query), snapshot)
        .unwrap();
    let old_disclosure = old.trace().vector_search.as_ref().unwrap();
    assert_eq!(
        old_disclosure.aggregate_allowed_vectors,
        Some(201),
        "the held snapshot counts its own live rows"
    );
}

#[test]
fn updated_base_and_tail_publish_one_visible_row_per_identity() {
    let db = fixture("F32", &[vec![1., 0., 0.], vec![-1., 0., 0.]]);
    db.execute(
        "UPDATE items SET embedding = $query WHERE id = 1",
        &parameters(&[0.8, 0.2, -0.3]),
    )
    .unwrap();
    let sql = "SELECT id, score FROM items ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 10";
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
    let sql = "SELECT id FROM items WHERE bucket < 5 ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 10";
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
            "SELECT id, score FROM items ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 10";
        let ordinary = db.execute(sql, &parameters(&query)).unwrap();
        let bounded = bounded_query(&db, sql, &query);
        assert_eq!(ordinary.rows(), bounded.rows(), "reader parity for {mode}");
        let expected = db
            .execute(&sql.replace("AUTO", "EXACT"), &parameters(&query))
            .unwrap();
        assert_eq!(
            ordinary.rows(),
            expected.rows(),
            "native order and scores for {mode}: {exact:?}"
        );
        // The score is the canonical cosine between the original query and the
        // vector as the column stores it. `stored_vector_value` is the vector
        // crate's one statement of what a quantized column stores; the cosine
        // is written out independently: f64 accumulation, rounded once to f32.
        let quantization = match mode {
            "F32" => VectorQuantization::F32,
            "SQ8" => VectorQuantization::SQ8,
            _ => VectorQuantization::SQ4,
        };
        assert_eq!(ids(&ordinary), vec![1, 2]);
        for (row, vector) in ordinary.rows().iter().zip(&vectors) {
            let stored = stored_vector_value(vector, quantization);
            let (mut dot, mut v2, mut q2) = (0.0_f64, 0.0_f64, 0.0_f64);
            for (&v, &q) in stored.iter().zip(&query) {
                let (v, q) = (f64::from(v), f64::from(q));
                dot += v * q;
                v2 += v * v;
                q2 += q * q;
            }
            let reference = (dot / (v2.sqrt() * q2.sqrt())) as f32;
            assert_eq!(
                row[1],
                Value::Float64(f64::from(reference)),
                "the {mode} score is the cosine against the stored vector"
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
        for mode in ["EXACT", "AUTO"] {
            let sql = format!(
                "SELECT id FROM items WHERE {predicate} ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
            );
            assert!(
                db.execute(&sql, &parameters(&[1., 0., 0.]))
                    .unwrap()
                    .rows()
                    .is_empty()
            );
            assert!(bounded_query(&db, &sql, &[1., 0., 0.]).rows().is_empty());
        }
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
                .trace()
                .vector_search
                .as_ref()
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
    for commit in [false, true] {
        db.execute("BEGIN", &HashMap::new()).unwrap();
        db.execute("UPDATE items SET bucket = 2 WHERE id = 1", &HashMap::new())
            .unwrap();
        for mode in ["AUTO", "EXACT"] {
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
                    bounded_query(&db, &sql, &[1., 0., 0.]).rows(),
                    result.rows(),
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
fn a_long_update_history_does_not_fill_indexed_result_slots_with_one_identity() {
    let db = fixture("SQ8", &[vec![1., 0., 0.], vec![-1., 0., 0.]]);
    for update in 0..160 {
        db.execute(
            "UPDATE items SET embedding = $query WHERE id = 1",
            &parameters(&[0.8, 0.2 + update as f32 * 0.0001, -0.3]),
        )
        .unwrap();
    }
    let sql = "SELECT id, score FROM items ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 10";
    let exact = db
        .execute(
            &sql.replace("AUTO", "EXACT"),
            &parameters(&[0.7, 0.3, -0.2]),
        )
        .unwrap();
    let indexed = db.execute(sql, &parameters(&[0.7, 0.3, -0.2])).unwrap();
    assert_eq!(indexed.rows(), exact.rows());
    assert_eq!(
        bounded_query(&db, sql, &[0.7, 0.3, -0.2]).rows(),
        exact.rows()
    );
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
        let query = generated_vector(9021, 32);
        let before = db.execute("SELECT id, score FROM items WHERE bucket = 1 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 10", &parameters(&query)).unwrap();
        db.execute("BEGIN", &HashMap::new()).unwrap();
        db.execute("UPDATE items SET bucket = 2 WHERE id = 1", &HashMap::new())
            .unwrap();
        for route in ["AUTO", "EXACT"] {
            let sql = format!(
                "SELECT id, score FROM items WHERE bucket = 2 ORDER BY embedding <=> $query USE VECTOR {route} LIMIT 10"
            );
            for result in [
                db.execute(&sql, &parameters(&query)).unwrap(),
                bounded_query(&db, &sql, &query),
            ] {
                assert_eq!(
                    result.rows().iter().find(|row| row[0] == Value::Int64(1)),
                    before.rows().first(),
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
        for stage in 0..3 {
            if stage == 1 {
                db.execute(
                    "UPDATE items SET embedding = $query WHERE id = 1",
                    &parameters(&generated_vector(510, 32)),
                )
                .unwrap();
            }
            if stage == 2 {
                db.close().unwrap();
                drop(db);
                db = Database::open(&path).unwrap();
                db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            }
            for probe in 0..4 {
                let query = generated_vector(9000 + probe, 32);
                for filter in ["", "WHERE bucket < 5"] {
                    let sql = format!(
                        "SELECT id, score FROM items {filter} ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 100"
                    );
                    let reference = db
                        .execute(&sql.replace("AUTO", "EXACT"), &parameters(&query))
                        .unwrap();
                    for result in [
                        db.execute(&sql, &parameters(&query)).unwrap(),
                        bounded_query(&db, &sql, &query),
                    ] {
                        assert_eq!(
                            result.rows(),
                            reference.rows(),
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
                     {layout}) HISTORY CURRENT ONLY SYNC OFF"
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
            let query = [1.0, 0.0, 0.0];
            let old_snapshot = db.snapshot();
            let pin = db.pin_snapshot(old_snapshot).unwrap();
            let sql = "SELECT id, score FROM items ORDER BY embedding <=> $query \
                       USE VECTOR AUTO LIMIT 2";
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
                    .rows(),
                old_answer.rows(),
                "cleanup preserves the old reader's native score"
            );
            let old_bounded = db.__with_snapshot_override_for_test(old_snapshot, || {
                bounded_query(&db, sql, &query)
            });
            assert_eq!(old_bounded.rows(), old_answer.rows());
            drop(pin);
            let cleaned = db.compact_currency_versions().unwrap();
            assert_eq!(cleaned.pruned_versions, 1);
            assert_eq!(cleaned.vector_keys_rewritten, 1);
            let vector_fixture_reader_1 = db
                .declared_reader_memory()
                .expect("admit the vector fixture reader");
            let mut vector_fixture_copy_1 =
                contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

            assert_eq!(
                db.with_vector_store_for_test(|store| {
                    store
                        .raw_directory_entries(&mut vector_fixture_copy_1)
                        .expect("read or publish the admitted vector fixture")
                })
                .len(),
                2,
                "cleanup must actually leave one version of each row"
            );
            for restart in [false, true] {
                if restart {
                    db.close().unwrap();
                    drop(db);
                    db = Database::open(&path).unwrap();
                    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
                }
                let expected = db
                    .execute(&sql.replace("AUTO", "EXACT"), &parameters(&query))
                    .unwrap();
                assert_eq!(ids(&expected), vec![2, 1], "the stable row is truly nearer");
                for limit in [1, 2] {
                    let sql = format!(
                        "SELECT id, score FROM items ORDER BY embedding <=> $query \
                         USE VECTOR AUTO LIMIT {limit}"
                    );
                    let ordinary = db.execute(&sql, &parameters(&query)).unwrap();
                    assert_eq!(ordinary.rows(), &expected.rows()[..limit]);
                    let bounded = bounded_query(&db, &sql, &query);
                    assert_eq!(
                        bounded.rows(),
                        ordinary.rows(),
                        "{quantization}, partitioned={partitioned}, restart={restart}"
                    );
                }
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
#[ignore = "exact work counters for the int8 scan are unavailable"]
fn closer_noncurrent_rows_do_not_change_current_results_or_add_complete_entry_passes() {
    use contextdb_engine::vector_ivf_observations::{
        NativeIvfFaultBoundary, NativeIvfSealEvent, NativeIvfSealObserver,
        with_native_ivf_seal_observer_for_test,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Distances(AtomicUsize, AtomicUsize, AtomicUsize);
    impl NativeIvfSealObserver for Distances {
        fn observe(&self, event: NativeIvfSealEvent<'_>) {
            if let NativeIvfSealEvent::QueryExecuted { work, .. } = event {
                self.1
                    .fetch_add(work.exact_score_count as usize, Ordering::SeqCst);
                self.2.fetch_add(
                    (work.centroid_evaluations + work.quantized_evaluations) as usize,
                    Ordering::SeqCst,
                );
                self.0.fetch_add(
                    (work.centroid_evaluations
                        + work.quantized_evaluations
                        + work.full_precision_evaluations
                        + work.buffer_evaluations) as usize,
                    Ordering::SeqCst,
                );
            }
        }

        fn before_boundary(
            &self,
            _boundary: NativeIvfFaultBoundary<'_>,
        ) -> contextdb_core::Result<()> {
            Ok(())
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(directory.path().join("current-history.redb")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, bucket INT NOT NULL, is_current BOOLEAN NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (bucket) AUTO_INDEX_AT 16 \
         IVF (PROBES = 64))",
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
    let _pin = db.pin_snapshot(snapshot).unwrap();

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
        // The unfiltered control must really score every visible body while
        // keeping whole-entry slices and raw partition residency absent.
        let before = db
            .vector_store_for_test()
            .passive_activity_counters_for_test();
        let exact_scores = Arc::new(Distances::default());
        let all = with_native_ivf_seal_observer_for_test(exact_scores.clone(), || {
            db.execute(
                "SELECT id FROM items ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 5",
                &parameters(&query),
            )
            .unwrap()
        });
        assert_eq!(ids(&all), vec![100, 101, 102, 103, 104]);
        let after = db
            .vector_store_for_test()
            .passive_activity_counters_for_test();
        let scored = (end - 100 + 32) as u64;
        assert_eq!(exact_scores.1.load(Ordering::SeqCst) as u64, scored);
        assert_eq!(after.raw_partition_loads, before.raw_partition_loads);
        assert_eq!(
            after.whole_entry_slice_requests,
            before.whole_entry_slice_requests
        );
        assert_eq!(
            after.whole_entry_slice_entries_exposed,
            before.whole_entry_slice_entries_exposed
        );

        for mode in ["AUTO", "INDEXED"] {
            let sql = current_sql.replace("INDEXED", mode);
            let before = db
                .vector_store_for_test()
                .passive_activity_counters_for_test();
            let distances = Arc::new(Distances::default());
            db.__reset_last_query_vector_trace_for_test();
            let ordinary = with_native_ivf_seal_observer_for_test(distances.clone(), || {
                db.execute(&sql, &parameters(&query)).unwrap()
            });
            assert_eq!(
                ordinary.rows(),
                exact.rows(),
                "{mode} history={} filters before top-k",
                end - 100
            );
            let trace = {
                let seam_read_memory = db
                    .declared_reader_memory()
                    .expect("admit the fixture operation reader");
                db.__take_last_query_vector_trace_for_test(seam_read_memory)
                    .expect("read the admitted trace observation")
            }
            .unwrap();
            assert_eq!(
                used_indexed_route(&trace),
                mode == "INDEXED",
                "source route: {trace:?}"
            );
            assert_eq!(trace.supplemented_row_count, 0);
            if mode == "INDEXED" {
                assert!(distances.0.load(Ordering::SeqCst) > 0);
                assert!(
                    distances.0.load(Ordering::SeqCst) < 256,
                    "current membership bounds graph distance work"
                );
            } else {
                // AUTO compares the allowed current rows exactly: it scores exactly
                // that membership and walks no clustered index.
                assert_eq!(distances.1.load(Ordering::SeqCst), 32);
                assert_eq!(distances.2.load(Ordering::SeqCst), 0);
            }
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
            assert_eq!(bounded.result.rows(), exact.rows());
            let trace = {
                let seam_read_memory = db
                    .declared_reader_memory()
                    .expect("admit the fixture operation reader");
                db.__take_last_query_vector_trace_for_test(seam_read_memory)
                    .expect("read the admitted trace observation")
            }
            .unwrap();
            assert_eq!(
                used_indexed_route(&trace),
                mode == "INDEXED",
                "source route: {trace:?}"
            );
            assert_eq!(trace.supplemented_row_count, 0);
            assert_eq!(
                bounded
                    .result
                    .trace()
                    .vector_search
                    .as_ref()
                    .unwrap()
                    .aggregate_allowed_vectors,
                Some(32),
                "{mode} history={}: the bounded read discloses exactly the allowed current rows",
                end - 100
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
                .rows(),
            exact.rows()
        );
        assert_eq!(
            db.__with_snapshot_override_for_test(snapshot, || bounded_query(&db, &sql, &query))
                .rows(),
            exact.rows()
        );
    }
}
