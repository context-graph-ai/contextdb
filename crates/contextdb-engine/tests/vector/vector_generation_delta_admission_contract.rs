//! A small actual delta must admit its larger prior-base owner before loading it.
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, VectorMaintenancePreparationPhaseForTest,
};
use contextdb_vector::{
    VectorMaintenanceFailure, VectorPartitionRef, store::VectorGraphGeneration,
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

const WRITER_LIMIT: usize = 1_610_612_736;
const DIMENSIONS: usize = 768;
const BATCH: usize = 128;

fn route() -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("delta_admission_items", "embedding"),
        VectorPartitionKey::from_values(&[Value::Int64(1)]).unwrap(),
    )
}

fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            memory_limit: Some(WRITER_LIMIT),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap()
}

// Dense clustered embeddings with a stable per-row residual, rather than
// sparse axes or identical vectors. Every value and normalization is seeded.
fn embedding(id: usize, revision: usize) -> Vec<f32> {
    fn noise(mut state: u64) -> f32 {
        state ^= state >> 30;
        state = state.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        state ^= state >> 27;
        state = state.wrapping_mul(0x94d0_49bb_1331_11eb);
        state ^= state >> 31;
        (state >> 40) as f32 / 8_388_608.0 - 1.0
    }
    let mut values: Vec<f32> = (0..DIMENSIONS)
        .map(|dimension| {
            let shared = noise(((id % 64 + 1) as u64) << 32 | dimension as u64);
            let residual = noise(((id + 1 + revision * 10_000) as u64) << 32 | dimension as u64);
            shared + residual * 0.15
        })
        .collect();
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    for value in &mut values {
        *value /= norm;
    }
    values
}

fn seed(db: &Database, rows: usize, m: usize, ef: usize) {
    db.execute(
        &format!(
            "CREATE TABLE delta_admission_items (id INT PRIMARY KEY, scope INT NOT NULL, \
             embedding VECTOR({DIMENSIONS}) PARTITION_KEY (scope) MAX_PARTITIONS 1 \
             SEARCH_MODE INDEXED HNSW (M = {m}, EF_CONSTRUCTION = {ef}) \
             CONSOLIDATION (CHANGE_PERCENT = 10))"
        ),
        &HashMap::new(),
    )
    .unwrap();
    for first in (0..rows).step_by(BATCH) {
        let tx = db.begin().unwrap();
        for id in first..(first + BATCH).min(rows) {
            db.insert_row(
                tx,
                "delta_admission_items",
                HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("scope".into(), Value::Int64(1)),
                    ("embedding".into(), Value::Vector(embedding(id, 0))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    }
}

fn generation(db: &Database) -> VectorGraphGeneration {
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route())
        .unwrap();
    status
        .base
        .or(status.dormant_base)
        .expect("a durably published base generation")
}

fn assert_indexed(db: &Database, id: usize, revision: usize) {
    let result = db
        .execute(
            "SELECT id FROM delta_admission_items WHERE scope = 1 \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("query".into(), Value::Vector(embedding(id, revision)))]),
        )
        .expect("the production indexed route remains usable");
    assert_eq!(result.rows, vec![vec![Value::Int64(id as i64)]]);
}

#[test]
fn small_delta_refuses_growing_prior_base_candidate_before_allocation_then_retries() {
    const BASE_ROWS: usize = 512;
    const DELTA_ROWS: usize = 64;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("delta-candidate.db");
    let db = Arc::new(open(&path));
    seed(&db, BASE_ROWS, 24, 400);
    let initial = db.run_maintenance_cycle().unwrap();
    assert_eq!(initial.vector.built_partitions, 1, "{initial:?}");
    assert!(initial.vector.first_failure.is_none(), "{initial:?}");
    assert_indexed(&db, BASE_ROWS - 1, 0);
    let old_generation = generation(&db);
    assert_eq!(old_generation.hnsw_m, 24);
    assert_eq!(old_generation.hnsw_ef_construction, 400);
    let tx = db.begin().unwrap();
    for id in BASE_ROWS..BASE_ROWS + DELTA_ROWS {
        db.insert_row(
            tx,
            "delta_admission_items",
            HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("scope".into(), Value::Int64(1)),
                ("embedding".into(), Value::Vector(embedding(id, 0))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    let before = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route())
        .unwrap();
    assert!(before.change.or(before.dormant_change).is_none());
    assert_eq!(before.fresh_tail_entries, DELTA_ROWS);
    let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory()
            .expect("admit the partition observation reader"),
    );

    let info = db
        .vector_store_for_test()
        .partition_info(
            &route().index,
            &route().partition_key,
            &mut partition_observation_owner_1,
        )
        .unwrap()
        .unwrap();
    assert_eq!(info.live_rows, BASE_ROWS + DELTA_ROWS);
    assert_eq!(info.retained_rows, 0);
    assert_eq!(info.pending_inserts, DELTA_ROWS);
    let retained = db.__debug_vector_partition_generation_retention_for_test(&route());
    let journal = db.__debug_vector_partition_journal_file_for_test(&route());
    let admission = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route(),
        VectorMaintenancePreparationPhaseForTest::AdmitCandidate,
    );
    let admitted = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route(),
        VectorMaintenancePreparationPhaseForTest::CandidateAdmitted,
    );
    let (finished_tx, finished_rx) = mpsc::channel();
    let worker_db = Arc::clone(&db);
    let worker = thread::spawn(move || {
        finished_tx.send(worker_db.run_maintenance_cycle()).unwrap();
    });
    if let Err(error) = admission.wait_until_reached_timeout(Duration::from_secs(120)) {
        admission.release();
        admitted.release();
        worker.join().unwrap();
        let report = finished_rx.try_recv();
        panic!("candidate boundary not reached: {error}; {report:?}");
    }
    assert_eq!(generation(&db), old_generation);
    let usage = db.accountant().usage();
    let held = db
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    assert!(held > 0);
    db.accountant().set_budget(Some(usage.used)).unwrap();
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        held
    );
    admission.release();
    // The post-admission pause remains armed. Completion itself proves this
    // attempt refused before that event, hence before prior-base loading.
    let result = finished_rx.recv_timeout(Duration::from_secs(120));
    admitted.release();
    worker.join().unwrap();
    db.accountant().set_budget(Some(WRITER_LIMIT)).unwrap();
    let refused = result
        .expect("growth must refuse before CandidateAdmitted")
        .unwrap();
    assert_eq!(refused.vector.built_partitions, 0, "{refused:?}");
    assert_eq!(
        refused.vector.first_failure,
        Some(VectorMaintenanceFailure::MemoryLimit)
    );
    let details = refused.vector.first_failure_details.unwrap();
    assert_eq!(details.operation, "prepare_vector_generation");
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    assert_eq!(details.available_bytes, Some(0));
    assert_eq!(details.current_bytes, Some(usage.used as u64));
    assert_eq!(details.budget_limit_bytes, Some(usage.used as u64));
    assert!(details.requested_bytes.unwrap() > 0);
    assert_eq!(
        db.vector_store_for_test()
            .partition_graph_generation_status(&route())
            .unwrap(),
        before
    );
    assert_eq!(
        db.__debug_vector_partition_generation_retention_for_test(&route()),
        retained
    );
    assert_eq!(
        db.__debug_vector_partition_journal_file_for_test(&route()),
        journal
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_indexed(&db, BASE_ROWS - 1, 0);
    assert_indexed(&db, BASE_ROWS + DELTA_ROWS - 1, 0);
    drop(admission);
    drop(admitted);

    let retry_before = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route(),
        VectorMaintenancePreparationPhaseForTest::AdmitCandidate,
    );
    let retry_after = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route(),
        VectorMaintenancePreparationPhaseForTest::CandidateAdmitted,
    );
    let worker_db = Arc::clone(&db);
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    if let Err(error) = retry_before.wait_until_reached_timeout(Duration::from_secs(120)) {
        retry_before.release();
        retry_after.release();
        let report = worker.join().unwrap();
        panic!("retry did not reach candidate boundary: {error}; {report:?}");
    }
    let retry_usage = db.accountant().usage();
    let retry_held = db
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    assert_eq!(
        retry_held, held,
        "the unchanged delta retains the same encoding envelope"
    );
    retry_before.release();
    if let Err(error) = retry_after.wait_until_reached_timeout(Duration::from_secs(120)) {
        retry_after.release();
        let report = worker.join().unwrap();
        panic!("retry candidate was not admitted: {error}; {report:?}");
    }
    let successor = db
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    assert!(
        successor > held,
        "candidate must genuinely grow: {held} -> {successor}"
    );
    assert_eq!(details.requested_bytes, Some((successor - held) as u64));
    assert_eq!(
        db.accountant().usage().used,
        retry_usage.used - retry_held + successor
    );
    assert_eq!(generation(&db), old_generation);
    eprintln!(
        "prior-base candidate base_rows={BASE_ROWS} delta_rows={DELTA_ROWS} dimensions={DIMENSIONS} old_workspace={held} successor_workspace={successor} refusal={details:?}"
    );
    retry_after.release();
    let report = worker.join().unwrap().unwrap();
    assert_eq!(report.vector.built_partitions, 1, "{report:?}");
    assert!(report.vector.first_failure.is_none(), "{report:?}");
    assert_eq!(report.vector.remaining_partitions, 0);
    let published = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route())
        .unwrap();
    assert_eq!(
        generation(&db),
        old_generation,
        "the actual delta publishes as change, preserving base"
    );
    let change = published
        .change
        .or(published.dormant_change)
        .expect("a published actual change");
    assert_ne!(change.generation_id, old_generation.generation_id);
    assert_eq!(change.covered_tx, contextdb_core::TxId(db.snapshot().0));
    assert_eq!(change.hnsw_m, 24);
    assert_eq!(change.hnsw_ef_construction, 400);
    assert_eq!(published.fresh_tail_entries, 0);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_indexed(&db, BASE_ROWS + DELTA_ROWS - 1, 0);
    db.close().unwrap();
    drop(db);
    let reopened = open(&path);
    let restarted = reopened
        .vector_store_for_test()
        .partition_graph_generation_status(&route())
        .unwrap();
    assert_eq!(generation(&reopened), old_generation);
    assert_eq!(restarted.change.or(restarted.dormant_change), Some(change));
    assert_indexed(&reopened, BASE_ROWS - 1, 0);
    assert_indexed(&reopened, BASE_ROWS + DELTA_ROWS - 1, 0);
    reopened.close().unwrap();
}
