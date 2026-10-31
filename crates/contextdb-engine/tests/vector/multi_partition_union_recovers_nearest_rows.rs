//! A global nearest-neighbour page must search each selected graph deeply
//! enough to recover rows spread across the selected partitions. The same
//! stored vectors supply the exhaustive and indexed answers.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::{HashMap, HashSet};

const PARTITIONS: usize = 8;
const ROWS_PER_PARTITION: usize = 1_250;
const DIMENSIONS: usize = 768;
const QUERIES: usize = 100;
const LIMIT: usize = 10;
const CLUSTERS_PER_PARTITION: usize = 64;

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
        .rows()
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
         )",
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
        let report = db.run_maintenance_cycle().unwrap().vector;
        if report.ready_partitions == PARTITIONS && report.remaining_partitions == 0 {
            return db;
        }
    }
    panic!("all eight partitions must publish their maintained graphs");
}

#[derive(Default)]
struct Measurement {
    matched: usize,
    answer_digest: u64,
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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn check_shared_walk(fixture: Fixture) {
    let db = seeded_store(fixture);
    let scopes = [
        (1, " WHERE scope_id = 0"),
        (3, " WHERE scope_id IN (0, 1, 2)"),
        (8, ""),
    ];
    for (scope, predicate) in scopes
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
            let found = ids(&db
                .execute(&statement("INDEXED"), &params)
                .unwrap_or_else(|error| {
                    panic!("unexpected indexed error at probe {probe}: {error}")
                }));
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
        eprintln!(
            "{fixture:?} {scope}/8 recall={}/{} answer_digest={:016x}",
            measured.matched,
            QUERIES * LIMIT,
            measured.answer_digest,
        );
        assert_eq!(
            measured.answer_digest, repeated_digest,
            "approximate answer pages must be deterministic on the same build"
        );
        assert!(
            low_recall_queries.is_empty(),
            "each indexed query must meet its recall threshold: {low_recall_queries:?}"
        );
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
fn uniform_three_and_eight_partition_recall_regression() {
    check_shared_walk(Fixture::Uniform);
}

#[test]
fn clustered_one_three_and_eight_partition_recall_regression() {
    check_shared_walk(Fixture::Clustered);
}
