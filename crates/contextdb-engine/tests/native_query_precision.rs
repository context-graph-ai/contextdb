//! Ranking uses canonical cosine distance even when exposed f32 scores tie.
use contextdb_core::Value;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

#[test]
fn native_queries_keep_canonical_precision_when_display_scores_tie() {
    for partitioned in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let db = Database::open_with_options(
            root.path().join("precision.db"),
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        let partition = if partitioned {
            "PARTITION_KEY (scope)"
        } else {
            ""
        };
        db.execute(&format!("CREATE TABLE vectors (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(2) {partition} SEARCH_MODE INDEXED IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 2.00) WITH (training_samples = 1, training_iterations = 1))"),&HashMap::new()).unwrap();
        for (id, axis) in [(1, 0.0002_f32), (2, 0.0001_f32)] {
            db.execute(
                "INSERT INTO vectors VALUES ($id,$scope,$v)",
                &HashMap::from([
                    ("id".to_string(), Value::Int64(id)),
                    (
                        "scope".to_string(),
                        Value::Int64(if partitioned { id } else { 0 }),
                    ),
                    ("v".to_string(), Value::Vector(vec![1.0, axis])),
                ]),
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
        for mode in ["EXACT", "INDEXED"] {
            let result = db
                .execute(
                    &format!("SELECT id FROM vectors ORDER BY v <=> $q USE VECTOR {mode} LIMIT 2"),
                    &HashMap::from([("q".to_string(), Value::Vector(vec![1.0, 0.0]))]),
                )
                .unwrap();
            let ids: Vec<_> = result.rows().iter().map(|row| row[0].clone()).collect();
            assert_eq!(
                ids,
                vec![Value::Int64(2), Value::Int64(1)],
                "{mode}, partitioned={partitioned}: canonical distances differ even though their displayed f32 scores are equal"
            );
        }
        db.close().unwrap();
    }
}
