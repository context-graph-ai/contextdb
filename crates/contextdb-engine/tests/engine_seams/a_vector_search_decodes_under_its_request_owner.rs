#![cfg(feature = "test-seams")]
//! A vector search's membership and body decodes are the request's own reads:
//! they are charged to the request's reader budget, never to the private
//! unlimited default accountant a decode without an owner falls back to.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::vector_read_memory::unowned_decodes_for_test;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;

#[test]
fn an_indexed_vector_search_decodes_nothing_without_its_request_owner() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        root.path().join("owned-search.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            memory_limit: Some(64 * 1024 * 1024),
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE)", &HashMap::new()).unwrap();
    for id in 0..64i64 {
        db.execute(
            "INSERT INTO points VALUES ($id, 1, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                (
                    "vector".into(),
                    Value::Vector(vec![0.0, 1.0, id as f32 + 1.0]),
                ),
            ]),
        )
        .unwrap();
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    // One more row stays in the uncovered suffix, so both the graph and the
    // pending tail are read.
    db.execute(
        "INSERT INTO points VALUES (1000, 1, $vector)",
        &HashMap::from([("vector".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
    )
    .unwrap();
    let before = unowned_decodes_for_test();
    let rows = db
        .execute(
            "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
        )
        .unwrap()
        .rows;
    let unowned = unowned_decodes_for_test() - before;
    assert_eq!(rows, vec![vec![Value::Int64(1000)]]);
    assert_eq!(
        unowned, 0,
        "every decode of the search is funded by its request"
    );
}
