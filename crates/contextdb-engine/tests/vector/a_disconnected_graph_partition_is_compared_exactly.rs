//! Deleted history the index still holds does not move a connected partition
//! off its index, changes no answer, and costs no more search work than the
//! first round that carried history.

use contextdb_core::Value;
use contextdb_engine::vector_ivf_observations::{
    NativeIvfFaultBoundary, NativeIvfSealEvent, NativeIvfSealObserver,
    with_native_ivf_seal_observer_for_test,
};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const DIMENSION: usize = 8;
const LIVE_ROWS: usize = 300;
const POLICY: &str = "SEARCH_MODE INDEXED AUTO_INDEX_AT 1 IVF (PROBES = 300)";

#[derive(Default)]
struct Evaluations {
    searches: AtomicU64,
    evaluations: AtomicU64,
}

impl NativeIvfSealObserver for Evaluations {
    fn observe(&self, event: NativeIvfSealEvent<'_>) {
        if let NativeIvfSealEvent::QueryExecuted { indexed, work, .. } = event {
            if indexed {
                self.searches.fetch_add(1, Ordering::SeqCst);
            }
            self.evaluations.fetch_add(
                work.centroid_evaluations
                    + work.quantized_evaluations
                    + work.full_precision_evaluations
                    + work.buffer_evaluations,
                Ordering::SeqCst,
            );
        }
    }

    fn before_boundary(&self, _boundary: NativeIvfFaultBoundary<'_>) -> contextdb_core::Result<()> {
        Ok(())
    }
}

/// A deterministic vector near `direction` (1 or -1 on every axis).
fn vector(seed: usize, direction: f32) -> Vec<f32> {
    (0..DIMENSION)
        .map(|axis| {
            let mixed = (seed as u64 + 11)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add((axis as u64 + 5).wrapping_mul(0xbf58_476d_1ce4_e5b9));
            direction + ((mixed ^ (mixed >> 29)) % 10_007) as f32 / 10_007.0 * 0.4 - 0.2
        })
        .collect()
}

fn query() -> Vec<f32> {
    vector(1_000_000, 1.0)
}

fn insert(db: &Database, seed: usize, direction: f32) {
    db.execute(
        "INSERT INTO gaps (id, embedding) VALUES ($id, $embedding)",
        &HashMap::from([
            ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
            (
                "embedding".to_owned(),
                Value::Vector(vector(seed, direction)),
            ),
        ]),
    )
    .expect("insert one deterministic row");
}

fn settle(db: &Database) {
    for _ in 0..64 {
        let report = db
            .run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle")
            .vector;
        if report.ready_partitions == 1 && report.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance did not publish the index");
}

struct Run {
    result: QueryResult,
    searches: u64,
    evaluations: u64,
}

fn run(db: &Database, mode: &str) -> Run {
    let observer = Arc::new(Evaluations::default());
    let result = with_native_ivf_seal_observer_for_test(observer.clone(), || {
        db.execute(
            &format!(
                "SELECT id FROM gaps ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
            ),
            &HashMap::from([("query".to_owned(), Value::Vector(query()))]),
        )
        .unwrap_or_else(|error| panic!("the {mode} search executes: {error:?}"))
    });
    Run {
        result,
        searches: observer.searches.load(Ordering::SeqCst),
        evaluations: observer.evaluations.load(Ordering::SeqCst),
    }
}

fn ids(result: &QueryResult) -> Vec<Value> {
    result.rows().iter().map(|row| row[0].clone()).collect()
}

#[test]
#[ignore = "the vector index route is switched off in this release"]
fn deleted_history_does_not_move_a_connected_partition_off_its_graph() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!("CREATE TABLE gaps (id UUID PRIMARY KEY, embedding VECTOR({DIMENSION}) {POLICY})"),
        &HashMap::new(),
    )
    .expect("declare the INDEXED table");
    for seed in 1..=LIVE_ROWS {
        insert(&db, seed, 1.0);
    }
    settle(&db);
    let answer = ids(&run(&db, "INDEXED").result);
    // The first round with history is the baseline the later, longer
    // histories must not exceed.
    let mut first = None;
    let mut next_seed = 10_000;
    for round in 1..=3 {
        // Rows far from the query, inserted, maintained into the index and
        // deleted: the live population stays at LIVE_ROWS.
        let seeds = next_seed..next_seed + LIVE_ROWS;
        next_seed += LIVE_ROWS;
        for seed in seeds.clone() {
            insert(&db, seed, -1.0);
        }
        settle(&db);
        for seed in seeds {
            db.execute(
                "DELETE FROM gaps WHERE id = $id",
                &HashMap::from([("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128)))]),
            )
            .expect("delete one history row");
        }
        settle(&db);
        let observed = run(&db, "INDEXED");
        assert_eq!(ids(&observed.result), answer, "round {round}: same answer");
        assert!(
            observed.searches > 0,
            "round {round}: deleted history leaves the partition on its index"
        );
        let baseline = *first.get_or_insert(observed.evaluations);
        assert!(
            observed.evaluations <= baseline,
            "round {round}: work {} does not grow with the deleted history (first round {baseline})",
            observed.evaluations
        );
    }
}
