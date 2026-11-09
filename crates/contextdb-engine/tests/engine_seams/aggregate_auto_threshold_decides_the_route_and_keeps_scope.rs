//! Acceptance: "Apply the aggregate AUTO threshold before choosing the route."
//! "Policies 21–23 preserve authorized scopes, filters and one globally ordered answer."

use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, MaintenancePolicy, VectorSearchRoute};
use std::collections::HashMap;

#[test]
#[ignore = "The clustered vector index is switched off in this release, so a test that needs a published saved index cannot run."]
fn aggregate_auto_threshold_decides_the_route_and_keeps_scope() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for total in [63_i64, 64, 65] {
        for sizes in [
            vec![total],
            vec![total / 2, total - total / 2],
            vec![1, total - 1],
        ] {
            let database = Database::open_memory();
            database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            database.execute("CREATE TABLE automatic_items (id INTEGER PRIMARY KEY, scope INTEGER NOT NULL, keep BOOLEAN NOT NULL, embedding VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 4 SEARCH_MODE AUTO AUTO_INDEX_AT 64 SEARCH (TARGET_ACCURACY = 95, EXPECTED_VECTORS = DEFAULT))", &HashMap::new()).unwrap();
            database
                .execute(
                    "CREATE INDEX automatic_scope_keep ON automatic_items (scope, keep, id)",
                    &HashMap::new(),
                )
                .unwrap();
            let mut id = 1;
            for (partition, size) in sizes.iter().enumerate() {
                for local in 0..*size {
                    // Each partition's first vector ties globally. Insertion order
                    // supplies stable row identities for the exact/automatic oracle.
                    database
                        .execute(
                            "INSERT INTO automatic_items VALUES ($id, $scope, TRUE, $vector)",
                            &HashMap::from([
                                ("id".to_owned(), Value::Int64(id)),
                                ("scope".to_owned(), Value::Int64(partition as i64)),
                                (
                                    "vector".to_owned(),
                                    Value::Vector(vec![1.0, local as f32 / 100.0, 0.0]),
                                ),
                            ]),
                        )
                        .unwrap();
                    id += 1;
                }
            }
            // One excluded tuple and one excluded residual row are closer than
            // most selected rows; either accidental inclusion changes the oracle.
            for (scope, keep, id) in [(99, true, 900), (0, false, 901)] {
                database
                    .execute(
                        "INSERT INTO automatic_items VALUES ($id, $scope, $keep, $vector)",
                        &HashMap::from([
                            ("id".to_owned(), Value::Int64(id)),
                            ("scope".to_owned(), Value::Int64(scope)),
                            ("keep".to_owned(), Value::Bool(keep)),
                            ("vector".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0])),
                        ]),
                    )
                    .unwrap();
            }
            let mut ready = false;
            for _ in 0..128 {
                let report = database.run_maintenance_cycle().unwrap();
                if report.vector.ready_partitions == sizes.len() + 1
                    && report.vector.remaining_partitions == 0
                {
                    ready = true;
                    break;
                }
            }
            assert!(
                ready,
                "all saved routes are ready before threshold planning is measured"
            );
            let params = HashMap::from([("query".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
            let prefix = "SELECT id FROM automatic_items WHERE scope IN (0, 1) AND keep = TRUE ORDER BY embedding <=> $query";
            let exact = database
                .execute(&format!("{prefix} USE VECTOR EXACT LIMIT 65"), &params)
                .unwrap();
            assert_eq!(exact.rows().len(), total as usize);
            assert!(
                exact
                    .rows()
                    .iter()
                    .all(|row| row[0] != Value::Int64(900) && row[0] != Value::Int64(901)),
                "the exact oracle excludes both neighbouring scope and residual nonmember"
            );
            let automatic = database
                .execute(&format!("{prefix} USE VECTOR AUTO LIMIT 65"), &params)
                .unwrap();
            assert_eq!(
                automatic.rows(),
                exact.rows(),
                "automatic planning preserves complete scope, ties and global order at total={total}, sizes={sizes:?}"
            );
            let disclosure = automatic
                .trace()
                .vector_search
                .as_ref()
                .expect("completed route evidence");
            assert_eq!(disclosure.aggregate_allowed_vectors, Some(total as usize));
            assert_eq!(disclosure.effective_auto_index_at, 64);
            if total < 64 {
                assert_eq!(
                    disclosure.route,
                    Some(VectorSearchRoute::Exact),
                    "aggregate below threshold chooses the exact route; sizes={sizes:?}"
                );
            } else {
                assert_ne!(
                    disclosure.route,
                    Some(VectorSearchRoute::Exact),
                    "aggregate at/above threshold leaves the exact route even when each partition is individually below threshold; sizes={sizes:?}"
                );
            }
        }
    }
}
