#![cfg(feature = "nightly-tests")]

use super::maintained_search;
use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, with_hnsw_candidate_observer,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tempfile::TempDir;

const CEILING: u64 = 133_653_078;

struct Counter(AtomicU64);

impl HnswCandidateObserver for Counter {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn graph_build_distance_evaluations_stay_under_the_ratchet() {
    let test_start = Instant::now();
    let root = TempDir::new().expect("create the durable baseline fixture directory");
    let db = Database::open(root.path().join("graph-baseline.redb"))
        .expect("open the durable baseline fixture");
    assert!(
        db.disk_file_size().is_some(),
        "the ratchet must use the frozen baseline's file-backed storage"
    );
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE graph_baseline (id INTEGER PRIMARY KEY, partition_id INTEGER NOT NULL, embedding VECTOR(768) PARTITION_KEY (partition_id) SEARCH_MODE AUTO)",
        &HashMap::new(),
    )
    .expect("create the frozen single-partition baseline table");

    let tx = db
        .begin()
        .expect("begin the one baseline ingestion transaction");
    for ordinal in 0..maintained_search::CORPUS_ROWS {
        db.insert_row(
            tx,
            "graph_baseline",
            HashMap::from([
                (
                    "id".into(),
                    Value::Int64(maintained_search::corpus_id(ordinal)),
                ),
                ("partition_id".into(), Value::Int64(0)),
                (
                    "embedding".into(),
                    Value::Vector(maintained_search::corpus_vector(ordinal)),
                ),
            ]),
        )
        .expect("stage one frozen vector in the baseline transaction");
    }
    db.commit(tx)
        .expect("commit the one frozen baseline ingestion transaction");

    let counter = Arc::new(Counter(AtomicU64::new(0)));
    let maintenance_start = Instant::now();
    let maintenance = with_hnsw_candidate_observer(counter.clone(), || {
        db.run_maintenance_cycle()
            .expect("run exactly one baseline maintenance cycle")
    });
    let maintenance_elapsed_ns = maintenance_start.elapsed().as_nanos();
    println!("GRAPH_BUILD_MAINTENANCE_CYCLE_ELAPSED_NS={maintenance_elapsed_ns}");
    assert_eq!(
        maintenance.vector.nonempty_indexes, 1,
        "one nonempty vector index is present in the frozen baseline"
    );
    assert_eq!(
        maintenance.vector.ready_indexes, 1,
        "the only vector index is ready after the one baseline cycle"
    );
    assert_eq!(
        maintenance.vector.built_indexes, 1,
        "the one baseline cycle builds the only vector index"
    );
    assert_eq!(
        maintenance.vector.remaining_indexes, 0,
        "the one baseline cycle leaves no vector index unfinished"
    );
    assert_eq!(
        maintenance.vector.nonempty_partitions, 1,
        "one nonempty vector partition is present in the frozen baseline"
    );
    assert_eq!(
        maintenance.vector.ready_partitions, 1,
        "the only vector partition is ready after the one baseline cycle"
    );
    assert_eq!(
        maintenance.vector.built_partitions, 1,
        "the one baseline cycle builds the only vector partition"
    );
    assert_eq!(
        maintenance.vector.remaining_partitions, 0,
        "the one baseline cycle leaves no vector partition unfinished"
    );
    assert!(
        maintenance.vector.first_failure.is_none(),
        "the baseline maintenance cycle has no vector failure"
    );
    assert!(
        maintenance.vector.first_failure_details.is_none(),
        "the baseline maintenance cycle has no vector failure detail"
    );

    let observed = counter.0.load(Ordering::SeqCst);
    println!("graph_build_distance_evaluations={observed}");
    assert!(
        observed > 0,
        "maintenance must build the single frozen graph"
    );
    assert!(
        observed <= CEILING,
        "the frozen 10,000-vector build used {observed} distance evaluations, over the pre-fix ceiling {CEILING}"
    );

    let whole_test_elapsed_ns = test_start.elapsed().as_nanos();
    println!("GRAPH_BUILD_WHOLE_TEST_ELAPSED_NS={whole_test_elapsed_ns}");
}
