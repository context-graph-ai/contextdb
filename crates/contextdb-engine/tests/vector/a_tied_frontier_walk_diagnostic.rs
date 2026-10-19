//! Diagnostic sibling of `a_tied_frontier_cannot_hide_a_smaller_row`: the same
//! 120 equidistant rows in two graph partitions (M = 4), reporting for each
//! partition walk whether it drained its reachable graph or a stop rule ended
//! it, and how many of its rows it evaluated. Run with `--ignored --nocapture`;
//! `TIED_DIAGNOSTIC_REPEATS` (default 5) rebuilds the fixture that many times.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::hnsw::with_union_walk_traces;
use std::collections::HashMap;

#[test]
#[ignore = "diagnostic: run explicitly with --ignored"]
fn a_tied_frontier_walk_diagnostic() {
    let repeats = std::env::var("TIED_DIAGNOSTIC_REPEATS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5usize);
    for repeat in 0..repeats {
        let db = Database::open_memory();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        for (table, reverse) in [("ties_forward", false), ("ties_reverse", true)] {
            db.execute(
                &format!(
                    "CREATE TABLE {table} (id INT PRIMARY KEY, bucket INT NOT NULL, \
                     embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2 \
                     SEARCH_MODE INDEXED AUTO_INDEX_AT 1 \
                     HNSW (M = 4, EF_CONSTRUCTION = 64, EF_SEARCH = 32))"
                ),
                &HashMap::new(),
            )
            .expect("declare two tied graph partitions");
            let tx = db.begin().expect("begin");
            for id in 1..=120_i64 {
                let bucket = if reverse { 1 - id % 2 } else { id % 2 };
                db.insert_row(
                    tx,
                    table,
                    HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("bucket".into(), Value::Int64(bucket)),
                        (
                            "embedding".into(),
                            Value::Vector(vec![0.0, 1.0, ((id * 11) % 121) as f32 / 120.0]),
                        ),
                    ]),
                )
                .expect("insert one tied vector");
            }
            db.commit(tx).expect("commit tied vectors");
        }
        for _ in 0..512 {
            db.run_maintenance_cycle()
                .expect("advance graph maintenance");
            let status = db.vector_store_for_test().hnsw_maintenance_status();
            if status.remaining_partitions == 0 && status.ready_partitions == 4 {
                break;
            }
        }
        let params = HashMap::from([("query".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]);
        for table in ["ties_forward", "ties_reverse"] {
            let sql = format!(
                "SELECT id FROM {table} WHERE bucket IN (0, 1) \
                 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3"
            );
            let (result, traces) = with_union_walk_traces(0, || db.execute(&sql, &params));
            let ids: Vec<_> = result
                .expect("indexed page")
                .rows
                .iter()
                .map(|row| row[0].clone())
                .collect();
            for trace in &traces {
                let distinct: std::collections::BTreeSet<_> = trace
                    .evaluated
                    .iter()
                    .map(|entry| entry.distance.to_bits())
                    .collect();
                eprintln!(
                    "repeat {repeat} {table} partition {}: expansions={} evaluated={} stop={:?} distinct_distances={}",
                    trace.partition,
                    trace.expansions,
                    trace.evaluated.len(),
                    trace.stop,
                    distinct.len(),
                );
            }
            eprintln!("repeat {repeat} {table}: ids={ids:?}");
        }
    }
}
