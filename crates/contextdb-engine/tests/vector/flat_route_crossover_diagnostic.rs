//! Evidence for the default `FLAT_BELOW`: where a partition's graph walk
//! starts to beat a flat comparison of its rows. Not an acceptance test.
//!
//! Frozen inputs, fixed before any default is chosen: the frozen 768-d corpus
//! and its 100 held-out queries, one partition of 625, 1,250, 2,500, 5,000 or
//! 10,000 live rows, unfiltered and with half of the rows excluded by an
//! indexed `is_current` filter, k = 10, the compatibility HNSW profile, and
//! the default budgets. Timing is same-process, observer-free, interleaved and
//! rotated per query, planning included; a query's latency is its fastest of
//! three rounds. Each size reports INDEXED forced flat (by `FLAT_BELOW`),
//! INDEXED by graph, EXACT, and the EXACT-vs-EXACT noise bound: a gap inside
//! that bound is noise, not a crossover. Run with `--ignored --nocapture`.

use super::maintained_search;
use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use uuid::Uuid;

const SIZES: [usize; 5] = [625, 1_250, 2_500, 5_000, 10_000];
const ROUNDS: usize = 3;

fn fixture(rows: usize) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE crossover (id INT PRIMARY KEY, context_id UUID NOT NULL, \
         is_current BOOL NOT NULL, embedding VECTOR(768) PARTITION_KEY (context_id) \
         MAX_PARTITIONS 8 SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX crossover_current_idx ON crossover (context_id, is_current)",
        &HashMap::new(),
    )
    .unwrap();
    for ordinal in 0..rows {
        db.execute(
            "INSERT INTO crossover VALUES ($id, $context, $current, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(ordinal as i64 + 1)),
                ("context".into(), Value::Uuid(Uuid::from_u128(7))),
                ("current".into(), Value::Bool(ordinal % 2 == 0)),
                (
                    "embedding".into(),
                    Value::Vector(maintained_search::corpus_vector(ordinal)),
                ),
            ]),
        )
        .unwrap();
    }
    for _ in 0..64 {
        db.run_maintenance_cycle().unwrap();
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == 1 && status.remaining_partitions == 0 {
            return db;
        }
    }
    panic!("maintenance did not publish the crossover partition");
}

fn timed(db: &Database, sql: &str, params: &HashMap<String, Value>) -> u128 {
    let started = std::time::Instant::now();
    let result = db.execute(sql, params).unwrap();
    let micros = started.elapsed().as_micros();
    drop(result);
    micros
}

fn p95(values: &[u128]) -> u128 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * 95).div_ceil(100) - 1]
}

#[test]
#[ignore = "diagnostic: run explicitly with --ignored"]
fn flat_route_crossover_diagnostic() {
    for rows in SIZES {
        let db = fixture(rows);
        for filtered in [false, true] {
            let predicate = if filtered {
                "WHERE context_id = $context AND is_current = TRUE "
            } else {
                "WHERE context_id = $context "
            };
            let statement = |mode: &str| {
                format!(
                    "SELECT id FROM crossover {predicate}ORDER BY embedding <=> $query \
                     USE VECTOR {mode} LIMIT 10"
                )
            };
            let (indexed, exact) = (statement("INDEXED"), statement("EXACT"));
            // [flat, graph, exact, exact again]
            let mut best = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
            for probe in 0..maintained_search::HELD_OUT_ROWS {
                let params = HashMap::from([
                    ("context".to_owned(), Value::Uuid(Uuid::from_u128(7))),
                    (
                        "query".to_owned(),
                        Value::Vector(maintained_search::held_out_vector(probe)),
                    ),
                ]);
                let mut fastest = [u128::MAX; 4];
                for round in 0..ROUNDS {
                    for slot in 0..4 {
                        let which = (slot + probe + round) % 4;
                        let micros = match which {
                            0 => {
                                db.execute(
                                    &format!(
                                        "ALTER TABLE crossover ALTER COLUMN embedding SET FLAT_BELOW {}",
                                        rows + 1
                                    ),
                                    &HashMap::new(),
                                )
                                .unwrap();
                                let micros = timed(&db, &indexed, &params);
                                db.execute(
                                    "ALTER TABLE crossover ALTER COLUMN embedding SET FLAT_BELOW DEFAULT",
                                    &HashMap::new(),
                                )
                                .unwrap();
                                micros
                            }
                            1 => timed(&db, &indexed, &params),
                            _ => timed(&db, &exact, &params),
                        };
                        fastest[which] = fastest[which].min(micros);
                    }
                }
                for (which, micros) in fastest.into_iter().enumerate() {
                    best[which].push(micros);
                }
            }
            let noise: Vec<u128> = best[2]
                .iter()
                .zip(&best[3])
                .map(|(left, right)| left.abs_diff(*right))
                .collect();
            let (flat, graph, exact_p95, noise_bound) =
                (p95(&best[0]), p95(&best[1]), p95(&best[2]), p95(&noise));
            let verdict = if graph + noise_bound < flat {
                "graph_faster"
            } else if flat + noise_bound < graph {
                "flat_faster"
            } else {
                "within_noise"
            };
            eprintln!(
                "CROSSOVER rows={rows} filtered={filtered} flat_p95_us={flat} graph_p95_us={graph} exact_p95_us={exact_p95} noise_bound_us={noise_bound} verdict={verdict}"
            );
        }
    }
}
