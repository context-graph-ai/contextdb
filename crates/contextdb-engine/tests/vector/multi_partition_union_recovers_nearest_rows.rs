//! A global nearest-neighbour page must search each selected graph deeply
//! enough to recover rows spread across the selected partitions. The same
//! stored vectors supply the exhaustive and indexed answers.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use contextdb_vector::hnsw::with_union_search_receipts;
use std::collections::{HashMap, HashSet};

const PARTITIONS: usize = 8;
const ROWS_PER_PARTITION: usize = 1_250;
const DIMENSIONS: usize = 768;
const QUERIES: usize = 100;
const LIMIT: usize = 10;
const M: usize = 16;
const CLUSTERS_PER_PARTITION: usize = 64;
const FEW_PARTITIONS_EF: usize = 100;
const ALL_PARTITIONS_EF: usize = 400;
/// The breadth times the layer-0 maximum degree: twice `M`, plus the links
/// that keep every point reachable from the entry point.
const BASE_BUDGET: usize =
    FEW_PARTITIONS_EF * (2 * M + contextdb_vector::hnsw::LAYER0_REACHABILITY_LINKS);
const ALL_PARTITIONS_BUDGET: usize =
    ALL_PARTITIONS_EF * (2 * M + contextdb_vector::hnsw::LAYER0_REACHABILITY_LINKS);

#[derive(Clone, Copy, Debug)]
enum Fixture {
    Uniform,
    Clustered,
}

fn embedding(ordinal: usize) -> Vec<f32> {
    let mut state = (ordinal as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15).max(1);
    (0..DIMENSIONS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ((state >> 40) as f32 / 16_777_216.0) - 0.5
        })
        .collect()
}

fn next_uniform(state: &mut u64) -> f64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    // The half-step excludes zero, so Box-Muller never takes ln(0).
    (((*state >> 11) as f64) + 0.5) / ((1_u64 << 53) as f64)
}

fn gaussian_noise(seed: u64, scale: f32) -> Vec<f32> {
    let mut state = seed.max(1);
    let mut noise = Vec::with_capacity(DIMENSIONS);
    while noise.len() < DIMENSIONS {
        let radius = (-2.0 * next_uniform(&mut state).ln()).sqrt();
        let angle = std::f64::consts::TAU * next_uniform(&mut state);
        noise.push((radius * angle.cos()) as f32 * scale);
        noise.push((radius * angle.sin()) as f32 * scale);
    }
    noise
}

fn clustered_embedding(center: &[f32], seed: u64, scale: f32) -> Vec<f32> {
    center
        .iter()
        .zip(gaussian_noise(seed, scale))
        .map(|(coordinate, noise)| coordinate + noise)
        .collect()
}

fn ids(result: &QueryResult) -> HashSet<i64> {
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("nearest-row query must project one integer id: {other:?}"),
        })
        .collect()
}

fn seeded_store(fixture: Fixture) -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE union_recall (id INTEGER PRIMARY KEY, scope_id INTEGER NOT NULL, \
         embedding VECTOR(768) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 \
         SEARCH_MODE INDEXED AUTO_INDEX_AT 1 \
         HNSW (M = 16, EF_CONSTRUCTION = 200))",
        &HashMap::new(),
    )
    .expect("declare eight maintained vector partitions");

    // Each partition has 64 Gaussian clusters. Sharing their centre locations
    // makes a query near a centre compete across all selected partitions.
    let centers: Vec<_> = (0..CLUSTERS_PER_PARTITION)
        .map(|cluster| embedding(20_000_000 + cluster))
        .collect();
    let tx = db.begin().expect("begin fixture ingestion");
    for partition in 0..PARTITIONS {
        for ordinal in 0..ROWS_PER_PARTITION {
            let global = partition * ROWS_PER_PARTITION + ordinal;
            let vector = match fixture {
                Fixture::Uniform => embedding(global + 1),
                Fixture::Clustered => clustered_embedding(
                    &centers[ordinal % CLUSTERS_PER_PARTITION],
                    ((global + 1) as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
                    0.03,
                ),
            };
            db.insert_row(
                tx,
                "union_recall",
                HashMap::from([
                    ("id".into(), Value::Int64((global + 1) as i64)),
                    ("scope_id".into(), Value::Int64(partition as i64)),
                    ("embedding".into(), Value::Vector(vector)),
                ]),
            )
            .expect("stage one stored vector");
        }
    }
    db.commit(tx).expect("commit all stored vectors");
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("advance deterministic maintenance");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == PARTITIONS && status.remaining_partitions == 0 {
            return db;
        }
    }
    panic!("all eight partitions must publish their maintained graphs");
}

#[derive(Default)]
struct Measurement {
    matched: usize,
    refused: usize,
    admission: Vec<usize>,
    expansion: Vec<usize>,
    answer_digest: u64,
}

fn mean(values: &[usize]) -> f64 {
    values.iter().sum::<usize>() as f64 / values.len() as f64
}

fn digest_page(mut digest: u64, ids: &HashSet<i64>) -> u64 {
    let mut ordered: Vec<_> = ids.iter().copied().collect();
    ordered.sort_unstable();
    for id in ordered {
        for byte in id.to_le_bytes() {
            digest = (digest ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    (digest ^ 0xff).wrapping_mul(0x100000001b3)
}

fn check_shared_walk(fixture: Fixture) {
    let db = seeded_store(fixture);
    let scopes = [
        (1, " WHERE scope_id = 0", FEW_PARTITIONS_EF, BASE_BUDGET),
        (
            3,
            " WHERE scope_id IN (0, 1, 2)",
            FEW_PARTITIONS_EF,
            BASE_BUDGET,
        ),
        (8, "", ALL_PARTITIONS_EF, ALL_PARTITIONS_BUDGET),
    ];
    for (scope, predicate, resolved_ef, budget) in scopes
        .into_iter()
        .filter(|(scope, ..)| matches!(fixture, Fixture::Clustered) || *scope != 1)
    {
        let mut measured = Measurement {
            answer_digest: 0xcbf29ce484222325,
            ..Measurement::default()
        };
        let mut repeated_digest = 0xcbf29ce484222325;
        let mut low_recall_queries = Vec::new();
        for probe in 0..QUERIES {
            let query = match fixture {
                Fixture::Uniform => embedding(10_000_000 + probe),
                Fixture::Clustered => clustered_embedding(
                    &embedding(20_000_000 + probe % CLUSTERS_PER_PARTITION),
                    ((10_000_000 + probe) as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
                    0.003,
                ),
            };
            let params = HashMap::from([("query".into(), Value::Vector(query))]);
            let statement = |mode| {
                format!(
                    "SELECT id FROM union_recall{predicate} ORDER BY embedding <=> $query \
                     USE VECTOR {mode} LIMIT {LIMIT}"
                )
            };
            let exact = db
                .execute(&statement("EXACT"), &params)
                .expect("exhaustive search of the stored vectors");
            let truth = ids(&exact);
            assert_eq!(truth.len(), LIMIT);
            let (indexed, receipts) =
                with_union_search_receipts(|| db.execute(&statement("INDEXED"), &params));
            let [receipt] = receipts.as_slice() else {
                panic!("one union search must serve each query: {receipts:?}");
            };
            assert_eq!(receipt.partitions, scope);
            assert_eq!(receipt.resolved_ef_search, resolved_ef);
            assert_eq!(receipt.budget, budget * scope);
            assert!(
                receipt.admission_evaluations + receipt.expansion_evaluations <= budget * scope
            );
            measured.admission.push(receipt.admission_evaluations);
            measured.expansion.push(receipt.expansion_evaluations);
            match indexed {
                Ok(indexed) => {
                    assert_eq!(receipt.terminal, "answer_complete");
                    let found = ids(&indexed);
                    assert_eq!(found.len(), LIMIT);
                    let matched = found.intersection(&truth).count();
                    let threshold = 0.95;
                    if (matched as f64 / LIMIT as f64) < threshold {
                        low_recall_queries.push((probe, matched));
                    }
                    measured.matched += matched;
                    let repeated = ids(&db
                        .execute(&statement("INDEXED"), &params)
                        .expect("the repeated indexed query must not refuse"));
                    repeated_digest = digest_page(repeated_digest, &repeated);
                    measured.answer_digest = digest_page(measured.answer_digest, &found);
                }
                Err(_) if receipt.terminal == "refused" => measured.refused += 1,
                Err(error) => panic!("unexpected indexed error at probe {probe}: {error}"),
            }
        }
        eprintln!(
            "{fixture:?} {scope}/8 B={budget} recall={}/{} refused={} D_total={} D_mean={:.1} D_max={} W_total={} W_mean={:.1} W_max={} answer_digest={:016x}",
            measured.matched,
            QUERIES * LIMIT,
            measured.refused,
            measured.admission.iter().sum::<usize>(),
            mean(&measured.admission),
            measured.admission.iter().max().unwrap(),
            measured.expansion.iter().sum::<usize>(),
            mean(&measured.expansion),
            measured.expansion.iter().max().unwrap(),
            measured.answer_digest,
        );
        let admission = measured.admission.iter().sum::<usize>();
        let expansion = measured.expansion.iter().sum::<usize>();
        assert!(admission + expansion <= QUERIES * budget * scope);
        assert_eq!(
            measured.answer_digest, repeated_digest,
            "approximate answer pages must be deterministic on the same build"
        );
        assert!(
            low_recall_queries.is_empty(),
            "each indexed query must meet its recall threshold: {low_recall_queries:?}"
        );
        assert_eq!(measured.refused, 0);
        if matches!(fixture, Fixture::Clustered) {
            assert!(measured.matched >= 950, "clustered recall fell below 95%");
        }
        if matches!(fixture, Fixture::Uniform) && scope == 8 {
            assert!(
                measured.matched >= 826,
                "uniform all-scope recall regressed"
            );
        }
        // Every scope also keeps the per-query recall floor above.
    }
}

#[test]
fn uniform_three_and_eight_partition_shared_walk_regression() {
    check_shared_walk(Fixture::Uniform);
}

#[test]
fn clustered_one_three_and_eight_partition_shared_walk_regression() {
    check_shared_walk(Fixture::Clustered);
}
