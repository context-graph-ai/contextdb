//! Generation admission follows the selected graph and the real phase owners,
//! while retained raw history and unpublished candidates keep their authority.
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, VectorMaintenancePreparationPhaseForTest,
};
use contextdb_vector::{
    VectorMaintenanceFailure, VectorPartitionRef, store::VectorGraphGeneration,
};
use std::{collections::HashMap, path::Path, sync::Arc, thread, time::Duration};

const WRITER_LIMIT: usize = 1_610_612_736;
const DIMENSIONS: usize = 768;
const BATCH: usize = 128;

#[test]
fn repeated_generation_publication_keeps_only_live_charged_owners() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cycles.db");
    let db = open(&path);
    seed(&db, 256, 24, 400);
    db.run_maintenance_cycle().unwrap();
    let mut residual = None;
    for cycle in 0..12 {
        let tx = db.begin().unwrap();
        for id in 0..64 {
            db.execute_in_tx(
                tx,
                "UPDATE admission_items SET embedding = $vector WHERE id = $id",
                &HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("vector".into(), Value::Vector(embedding(id, cycle + 1))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        db.execute(&format!("ALTER TABLE admission_items ALTER COLUMN embedding SET HNSW (M = {}, EF_CONSTRUCTION = 400)", 24 + cycle % 2), &HashMap::new()).unwrap();
        let report = db.run_maintenance_cycle().unwrap();
        let receipt = db.vector_memory_ownership_receipt_for_test();
        let retention = db.__debug_vector_partition_generation_retention_for_test(&route());
        eprintln!(
            "J1_CHARGE cycle={cycle} receipt={receipt:?} retention={retention:?} writer_retained={} report={:?}",
            db.__durable_writer_charged_bytes_for_test(),
            report.vector
        );
        assert_eq!(receipt.temporary_workspace, 0);
        assert_eq!(receipt.retired_pinned_graph_bytes, 0);
        assert!(report.vector.first_failure.is_none(), "{report:?}");
        assert_eq!(db.accountant().underflow_count_for_test(), 0);
        let live_non_vector =
            receipt.non_vector_total as i64 - db.__durable_writer_charged_bytes_for_test() as i64;
        if let Some(expected) = residual {
            assert_eq!(
                live_non_vector, expected,
                "dead publication owners do not accumulate outside named retained owners"
            );
        } else {
            residual = Some(live_non_vector);
        }
    }
    let steady = db.vector_memory_ownership_receipt_for_test();
    assert_eq!(
        db.__physical_version_count_for_test("admission_items"),
        256 + 12 * 64
    );
    for cycle in 0..12 {
        let report = db.run_maintenance_cycle().unwrap();
        let receipt = db.vector_memory_ownership_receipt_for_test();
        eprintln!(
            "J1_CHARGE quiet_cycle={cycle} receipt={receipt:?} writer_retained={}",
            db.__durable_writer_charged_bytes_for_test()
        );
        assert!(report.vector.first_failure.is_none(), "{report:?}");
        assert_eq!(
            receipt, steady,
            "retained-version steady state returns every cycle reservation"
        );
    }
    db.execute(
        "UPDATE admission_items SET embedding = $vector WHERE id = 255",
        &HashMap::from([("vector".into(), Value::Vector(embedding(255, 13)))]),
    )
    .expect(
        "a small foreground vector write remains admitted after the retained-version steady state",
    );
    assert_indexed(&db, 255, 13);
    assert_eq!(db.accountant().underflow_count_for_test(), 0);
    db.close().unwrap();
}

fn route() -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("admission_items", "embedding"),
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
            "CREATE TABLE admission_items (id INT PRIMARY KEY, scope INT NOT NULL, \
             embedding VECTOR({DIMENSIONS}) PARTITION_KEY (scope) MAX_PARTITIONS 1 \
             SEARCH_MODE INDEXED HNSW (M = {m}, EF_CONSTRUCTION = {ef}))"
        ),
        &HashMap::new(),
    )
    .unwrap();
    for first in (0..rows).step_by(BATCH) {
        let tx = db.begin().unwrap();
        for id in first..(first + BATCH).min(rows) {
            db.insert_row(
                tx,
                "admission_items",
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
            "SELECT id FROM admission_items WHERE scope = 1 \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
            &HashMap::from([("query".into(), Value::Vector(embedding(id, revision)))]),
        )
        .expect("the production indexed route remains usable");
    assert_eq!(result.rows, vec![vec![Value::Int64(id as i64)]]);
}

fn assert_published(db: &Database, report: contextdb_engine::MaintenanceReport) {
    assert_eq!(report.vector.built_partitions, 1, "{report:?}");
    assert_eq!(report.vector.remaining_partitions, 0, "{report:?}");
    assert!(report.vector.first_failure.is_none(), "{report:?}");
    assert!(report.vector.first_failure_details.is_none());
    let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory()
            .expect("admit the partition observation reader"),
    );

    let partition = db
        .vector_store_for_test()
        .partition_info(
            &route().index,
            &route().partition_key,
            &mut partition_observation_owner_1,
        )
        .unwrap()
        .unwrap();
    assert!(partition.graph_available);
    assert_eq!(partition.pending_inserts, 0);
    assert_eq!(
        generation(db).covered_tx,
        contextdb_core::TxId(db.snapshot().0)
    );
    assert_eq!(generation(db).hnsw_m, 24);
    assert_eq!(generation(db).hnsw_ef_construction, 400);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert!(db.accountant().usage().used <= WRITER_LIMIT);
}

#[test]
fn fresh_ten_thousand_dense_vectors_publish_and_restart_under_writer_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fresh.db");
    let db = open(&path);
    seed(&db, 10_000, 24, 400);
    let mut partition_observation_owner_2 = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory()
            .expect("admit the partition observation reader"),
    );

    let partitions = db
        .vector_store_for_test()
        .partition_infos(&route().index, &mut partition_observation_owner_2)
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].live_rows, 10_000);
    assert_eq!(partitions[0].retained_rows, 0);
    assert!(!partitions[0].graph_available);
    assert_eq!(partitions[0].pending_inserts, 10_000);
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route())
        .unwrap();
    assert!(status.base.is_none() && status.dormant_base.is_none());
    let report = db.run_maintenance_cycle().unwrap();
    assert_published(&db, report);
    assert_indexed(&db, 9_999, 0);
    let published = generation(&db);
    eprintln!(
        "fresh rows=10000 dimensions={DIMENSIONS} partitions=1 limit={WRITER_LIMIT} published={published:?}"
    );
    db.close().unwrap();
    drop(db);
    let reopened = open(&path);
    assert_eq!(generation(&reopened), published);
    assert_indexed(&reopened, 9_999, 0);
    reopened.close().unwrap();
}

#[test]
fn retained_versions_and_tombstones_admit_base_replacement_and_serve_old_graph() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retained.db");
    let db = Arc::new(open(&path));
    seed(&db, 10_000, 16, 200);
    let first = db.run_maintenance_cycle().unwrap();
    assert_eq!(first.vector.built_partitions, 1, "{first:?}");
    assert!(first.vector.first_failure.is_none());
    let old_snapshot = db.snapshot();
    let pin = db.pin_snapshot(old_snapshot).unwrap();
    let old_generation = generation(&db);
    db.execute(
        "DELETE FROM admission_items WHERE id < 3700",
        &HashMap::new(),
    )
    .unwrap();
    for first in (3700..6700).step_by(BATCH) {
        let tx = db.begin().unwrap();
        for id in first..(first + BATCH).min(6700) {
            db.execute_in_tx(
                tx,
                "UPDATE admission_items SET embedding = $vector WHERE id = $id",
                &HashMap::from([
                    ("id".into(), Value::Int64(id as i64)),
                    ("vector".into(), Value::Vector(embedding(id, 1))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    }
    let mut partition_observation_owner_3 = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory()
            .expect("admit the partition observation reader"),
    );

    let info = db
        .vector_store_for_test()
        .partition_info(
            &route().index,
            &route().partition_key,
            &mut partition_observation_owner_3,
        )
        .unwrap()
        .unwrap();
    assert_eq!(info.live_rows, 6300);
    assert_eq!(info.retained_rows, 6700);
    assert!(
        info.tombstones >= 3700,
        "the deleted rows remain in retained history"
    );
    db.execute(
        "ALTER TABLE admission_items ALTER COLUMN embedding SET HNSW (M = 24, EF_CONSTRUCTION = 400)",
        &HashMap::new(),
    ).unwrap();
    assert_indexed(&db, 9_999, 0);
    let pause = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route(),
        VectorMaintenancePreparationPhaseForTest::EncodeGraph,
    );
    let worker_db = Arc::clone(&db);
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    // This is only a deadlock guard; success is observed at a phase event.
    let reached = pause.wait_until_reached_timeout(Duration::from_secs(900));
    if let Err(error) = reached {
        pause.release();
        let result = worker.join().unwrap();
        panic!("replacement did not reach encoding: {error}; result={result:?}");
    }
    assert_eq!(generation(&db), old_generation);
    assert_indexed(&db, 9_999, 0);
    let old_answer = db.execute_at_snapshot(
        "SELECT id FROM admission_items WHERE scope = 1 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("query".into(), Value::Vector(embedding(0, 0)))]),
        old_snapshot,
    ).unwrap();
    assert_eq!(old_answer.rows, vec![vec![Value::Int64(0)]]);
    pause.release();
    let report = worker.join().unwrap().unwrap();
    assert_published(&db, report);
    let replacement = generation(&db);
    eprintln!(
        "retained live=6300 retained=6700 limit={WRITER_LIMIT} old={old_generation:?} replacement={replacement:?}"
    );
    assert_ne!(replacement.generation_id, old_generation.generation_id);
    assert_indexed(&db, 9_999, 0);
    drop(pin);
    db.close().unwrap();
    drop(db);
    let reopened = open(&path);
    assert_eq!(generation(&reopened), replacement);
    assert_indexed(&reopened, 9_999, 0);
    reopened.close().unwrap();
}

#[test]
fn tiny_limit_refuses_with_typed_details_without_partial_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("refused.db");
    let db = open(&path);
    seed(&db, 64, 16, 200);
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    assert_indexed(&db, 63, 0);
    db.execute(
        "ALTER TABLE admission_items ALTER COLUMN embedding SET HNSW (M = 24, EF_CONSTRUCTION = 400)",
        &HashMap::new(),
    ).unwrap();
    let before = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route())
        .unwrap();
    let retained = db.__debug_vector_partition_generation_retention_for_test(&route());
    let journal = db.__debug_vector_partition_journal_file_for_test(&route());
    let limit = db.accountant().usage().used + 4096;
    db.set_memory_limit(Some(limit)).unwrap();
    let report = db.run_maintenance_cycle().unwrap();
    assert_eq!(report.vector.built_partitions, 0);
    assert_eq!(
        report.vector.first_failure,
        Some(VectorMaintenanceFailure::MemoryLimit)
    );
    let details = report.vector.first_failure_details.unwrap();
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    assert_eq!(details.operation, "prepare_vector_generation");
    eprintln!("tiny-limit atomic refusal={details:?}");
    assert!(details.requested_bytes.unwrap() > details.available_bytes.unwrap());
    assert_eq!(details.budget_limit_bytes, Some(limit as u64));
    assert_eq!(
        details.available_bytes.unwrap() + details.current_bytes.unwrap(),
        limit as u64
    );
    let after = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route())
        .unwrap();
    assert_eq!(
        after.base.or(after.dormant_base),
        before.base.or(before.dormant_base)
    );
    assert_eq!(
        after.change.or(after.dormant_change),
        before.change.or(before.dormant_change)
    );
    assert_eq!(after.fresh_tail_entries, before.fresh_tail_entries);
    assert_eq!(
        after.pending_generation_count,
        before.pending_generation_count
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
    db.set_memory_limit(Some(WRITER_LIMIT)).unwrap();
    assert_indexed(&db, 63, 0);
    let published = generation(&db);
    db.close().unwrap();
    drop(db);
    let reopened = open(&path);
    assert_eq!(generation(&reopened), published);
    assert_indexed(&reopened, 63, 0);
    reopened.close().unwrap();
}

fn replace_vectors(db: &Database, revision: usize) {
    let tx = db.begin().unwrap();
    for id in 0..64 {
        db.execute_in_tx(
            tx,
            "UPDATE admission_items SET embedding = $vector WHERE id = $id",
            &HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("vector".into(), Value::Vector(embedding(id, revision))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
}

fn indexed_at_snapshot(db: &Database, snapshot: contextdb_core::SnapshotId, revision: usize) {
    let answer = db.execute_at_snapshot(
        "SELECT id FROM admission_items WHERE scope = 1 ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("query".into(), Value::Vector(embedding(63, revision)))]),
        snapshot,
    ).unwrap();
    assert_eq!(answer.rows, vec![vec![Value::Int64(63)]]);
}

#[test]
fn multiple_pinned_generations_remain_charged_until_their_final_snapshot_release() {
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory.path().join("multiple-pins.db"));
    seed(&db, 64, 16, 200);
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    assert_indexed(&db, 63, 0);
    let first_generation = generation(&db);
    let first_snapshot = db.snapshot();
    let first_pin = db.pin_snapshot(first_snapshot).unwrap();
    replace_vectors(&db, 1);
    db.execute(
        "ALTER TABLE admission_items ALTER COLUMN embedding SET HNSW (M = 20, EF_CONSTRUCTION = 300)",
        &HashMap::new(),
    ).unwrap();
    let second_report = db.run_maintenance_cycle().unwrap();
    assert_eq!(
        second_report.vector.built_partitions, 1,
        "{second_report:?}"
    );
    assert!(
        second_report.vector.first_failure.is_none(),
        "{second_report:?}"
    );
    assert_indexed(&db, 63, 1);
    let second_generation = generation(&db);
    assert_ne!(
        first_generation.generation_id,
        second_generation.generation_id
    );
    let second_snapshot = db.snapshot();
    let second_pin = db.pin_snapshot(second_snapshot).unwrap();
    // Two independent reader holds at the same generation: releasing one is
    // insufficient to reclaim the graph still reachable by the other.
    let second_pin_again = db.pin_snapshot(second_snapshot).unwrap();
    replace_vectors(&db, 2);
    db.execute(
        "ALTER TABLE admission_items ALTER COLUMN embedding SET HNSW (M = 24, EF_CONSTRUCTION = 400)",
        &HashMap::new(),
    ).unwrap();
    assert_published(&db, db.run_maintenance_cycle().unwrap());
    let current_generation = generation(&db);
    assert_ne!(
        second_generation.generation_id,
        current_generation.generation_id
    );
    indexed_at_snapshot(&db, first_snapshot, 0);
    indexed_at_snapshot(&db, second_snapshot, 1);
    assert_indexed(&db, 63, 2);
    let both = db.__debug_vector_partition_generation_retention_for_test(&route());
    assert!(both.resident_superseded_generations >= 2, "{both:?}");
    assert!(both.durable_superseded_generations >= 2, "{both:?}");
    let receipt = db.vector_memory_ownership_receipt_for_test();
    assert_eq!(
        receipt.retired_pinned_graph_bytes,
        both.resident_superseded_bytes
    );
    assert!(receipt.retired_pinned_graph_bytes > 0);
    assert!(receipt.accountant_total >= receipt.retained_total);
    drop(first_pin);
    db.run_maintenance_cycle().unwrap();
    indexed_at_snapshot(&db, second_snapshot, 1);
    let second_held = db.__debug_vector_partition_generation_retention_for_test(&route());
    assert!(
        second_held.resident_superseded_generations >= 1,
        "{second_held:?}"
    );
    assert!(second_held.durable_superseded_bytes > 0, "{second_held:?}");
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .retired_pinned_graph_bytes,
        second_held.resident_superseded_bytes,
    );
    drop(second_pin);
    db.run_maintenance_cycle().unwrap();
    let still_held = db.__debug_vector_partition_generation_retention_for_test(&route());
    assert_eq!(
        still_held, second_held,
        "one reader still owns the same snapshot"
    );
    indexed_at_snapshot(&db, second_snapshot, 1);
    drop(second_pin_again);
    let mut released = false;
    for _ in 0..8 {
        db.run_maintenance_cycle().unwrap();
        let retained = db.__debug_vector_partition_generation_retention_for_test(&route());
        if retained.resident_superseded_generations == 0
            && retained.durable_superseded_generations == 0
        {
            assert_eq!(retained.resident_superseded_bytes, 0);
            assert_eq!(retained.durable_superseded_bytes, 0);
            released = true;
            break;
        }
    }
    assert!(
        released,
        "all superseded generations reclaim after their final hold releases"
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .retired_pinned_graph_bytes,
        0
    );
    assert_eq!(generation(&db), current_generation);
    assert_indexed(&db, 63, 2);
    db.close().unwrap();
}

#[test]
fn growing_persistence_transitions_refuse_atomically_for_base_and_change_publication() {
    for publish_change in [false, true] {
        let phase = VectorMaintenancePreparationPhaseForTest::AdmitPersistence;
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(open(&directory.path().join("transition.db")));
        seed(&db, 64, 16, 200);
        assert_eq!(
            db.run_maintenance_cycle().unwrap().vector.built_partitions,
            1
        );
        assert_indexed(&db, 63, 0);
        if publish_change {
            replace_vectors(&db, 1);
        } else {
            db.execute(
                "ALTER TABLE admission_items ALTER COLUMN embedding SET HNSW (M = 24, EF_CONSTRUCTION = 400)",
                &HashMap::new(),
            ).unwrap();
        }
        let old_generation = generation(&db);
        let before = db
            .vector_store_for_test()
            .partition_graph_generation_status(&route())
            .unwrap();
        let journal = db.__debug_vector_partition_journal_file_for_test(&route());
        let retained = db.__debug_vector_partition_generation_retention_for_test(&route());
        let pause =
            db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(&route(), phase);
        let worker_db = Arc::clone(&db);
        let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
        if let Err(error) = pause.wait_until_reached_timeout(Duration::from_secs(120)) {
            pause.release();
            let result = worker.join().unwrap();
            panic!("transition {phase:?} did not reach admission boundary: {error}; {result:?}");
        }
        assert_eq!(generation(&db), old_generation);
        // Change only the test accountant, avoiding an unrelated durable
        // control write and leaving exactly no headroom for the next owner.
        let usage = db.accountant().usage();
        let held_workspace = db
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace;
        let limit = usage.used;
        db.accountant().set_budget(Some(limit)).unwrap();
        pause.release();
        let report = worker.join().unwrap().unwrap();
        db.accountant().set_budget(Some(WRITER_LIMIT)).unwrap();
        assert_eq!(
            report.vector.built_partitions, 0,
            "phase={phase:?}; {report:?}"
        );
        assert_eq!(
            report.vector.first_failure,
            Some(VectorMaintenanceFailure::MemoryLimit),
            "phase={phase:?}; {report:?}"
        );
        let details = report.vector.first_failure_details.unwrap();
        eprintln!(
            "transition={phase:?} change={publish_change} old_workspace={held_workspace} typed_refusal={details:?}"
        );
        assert_eq!(details.operation, "prepare_vector_generation");
        assert_eq!(details.available_bytes, Some(0));
        assert_eq!(details.current_bytes, Some(usage.used as u64));
        assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
        assert!(!details.operation.is_empty());
        assert!(details.requested_bytes.unwrap() > details.available_bytes.unwrap());
        assert_eq!(details.budget_limit_bytes, Some(limit as u64));
        assert_eq!(
            details.available_bytes.unwrap() + details.current_bytes.unwrap(),
            limit as u64
        );
        let after = db
            .vector_store_for_test()
            .partition_graph_generation_status(&route())
            .unwrap();
        assert_eq!(
            after.base.or(after.dormant_base),
            before.base.or(before.dormant_base)
        );
        assert_eq!(
            after.change.or(after.dormant_change),
            before.change.or(before.dormant_change)
        );
        assert_eq!(after.fresh_tail_entries, before.fresh_tail_entries);
        assert_eq!(
            after.pending_generation_count,
            before.pending_generation_count
        );
        assert_eq!(
            db.__debug_vector_partition_journal_file_for_test(&route()),
            journal
        );
        assert_eq!(
            db.__debug_vector_partition_generation_retention_for_test(&route()),
            retained
        );
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
        assert_eq!(generation(&db), old_generation);
        assert_indexed(&db, 63, usize::from(publish_change));
        let retry_before = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
            &route(),
            VectorMaintenancePreparationPhaseForTest::AdmitPersistence,
        );
        let retry_after = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
            &route(),
            VectorMaintenancePreparationPhaseForTest::PersistenceAdmitted,
        );
        let retry_db = Arc::clone(&db);
        let retry_worker = thread::spawn(move || retry_db.run_maintenance_cycle());
        if let Err(error) = retry_before.wait_until_reached_timeout(Duration::from_secs(120)) {
            retry_before.release();
            retry_after.release();
            let result = retry_worker.join().unwrap();
            panic!("persistence retry not reached: {error}; {result:?}");
        }
        let retry_usage = db.accountant().usage();
        let retry_held = db
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace;
        // Seeded construction reproduces the refused phase's owned capacity.
        assert_eq!(retry_held, held_workspace);
        retry_before.release();
        if let Err(error) = retry_after.wait_until_reached_timeout(Duration::from_secs(120)) {
            retry_after.release();
            let result = retry_worker.join().unwrap();
            panic!("persistence retry transfer refused: {error}; {result:?}");
        }
        let retry_new = db
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace;
        assert!(retry_new > retry_held);
        assert_eq!(
            details.requested_bytes,
            Some((retry_new - retry_held) as u64)
        );
        assert_eq!(
            db.accountant().usage().used,
            retry_usage.used - retry_held + retry_new
        );
        assert_eq!(generation(&db), old_generation);
        eprintln!(
            "persistence admitted change={publish_change} old={retry_held} new={retry_new} growth={}",
            retry_new - retry_held
        );
        retry_after.release();
        let retry = retry_worker.join().unwrap().unwrap();
        assert_eq!(retry.vector.built_partitions, 1, "{retry:?}");
        assert!(retry.vector.first_failure.is_none(), "{retry:?}");
        let retried_status = db
            .vector_store_for_test()
            .partition_graph_generation_status(&route())
            .unwrap();
        if publish_change {
            assert_eq!(generation(&db), old_generation);
            assert!(
                retried_status
                    .change
                    .or(retried_status.dormant_change)
                    .is_some()
            );
        } else {
            assert_ne!(generation(&db).generation_id, old_generation.generation_id);
        }
        assert_indexed(&db, 63, usize::from(publish_change));
        db.close().unwrap();
    }
}

#[test]
fn encoding_shrink_transfers_held_workspace_without_additive_headroom() {
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(open(&directory.path().join("encoding-shrink.db")));
    seed(&db, 64, 16, 200);
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    assert_indexed(&db, 63, 0);
    db.execute(
        "ALTER TABLE admission_items ALTER COLUMN embedding SET HNSW (M = 24, EF_CONSTRUCTION = 400)",
        &HashMap::new(),
    ).unwrap();
    let old_generation = generation(&db);
    let before = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route(),
        VectorMaintenancePreparationPhaseForTest::AdmitEncoding,
    );
    let after = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &route(),
        VectorMaintenancePreparationPhaseForTest::EncodeGraph,
    );
    let worker_db = Arc::clone(&db);
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    if let Err(error) = before.wait_until_reached_timeout(Duration::from_secs(120)) {
        before.release();
        after.release();
        let result = worker.join().unwrap();
        panic!("encoding transfer not reached: {error}; {result:?}");
    }
    let usage = db.accountant().usage();
    let held = db
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    db.accountant().set_budget(Some(usage.used)).unwrap();
    before.release();
    if let Err(error) = after.wait_until_reached_timeout(Duration::from_secs(120)) {
        after.release();
        let result = worker.join().unwrap();
        panic!("encoding transfer refused despite held capacity: {error}; {result:?}");
    }
    let transferred = db
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    let transferred_usage = db.accountant().usage();
    // The codec envelope phase replaced the builder phase in place. There was
    // no room to admit both charges; only the replacement's size is retained.
    assert!(
        transferred > 0 && transferred < held,
        "{held} -> {transferred}"
    );
    assert_eq!(transferred_usage.used, usage.used - held + transferred);
    assert_eq!(generation(&db), old_generation);
    eprintln!(
        "encoding zero-headroom shrink old={held} new={transferred} used_before={} used_after={}",
        usage.used, transferred_usage.used
    );
    db.accountant().set_budget(Some(WRITER_LIMIT)).unwrap();
    after.release();
    assert_published(&db, worker.join().unwrap().unwrap());
    assert_indexed(&db, 63, 0);
    db.close().unwrap();
}

#[test]
fn candidate_shrink_transfers_held_workspace_without_additive_headroom() {
    for publish_change in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(open(&directory.path().join("candidate-shrink.db")));
        seed(&db, 64, 16, 200);
        assert_eq!(
            db.run_maintenance_cycle().unwrap().vector.built_partitions,
            1
        );
        assert_indexed(&db, 63, 0);
        if publish_change {
            replace_vectors(&db, 1);
        } else {
            db.execute(
                "ALTER TABLE admission_items ALTER COLUMN embedding SET HNSW (M = 24, EF_CONSTRUCTION = 400)",
                &HashMap::new(),
            ).unwrap();
        }
        let old_generation = generation(&db);
        let before = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
            &route(),
            VectorMaintenancePreparationPhaseForTest::AdmitCandidate,
        );
        let after = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
            &route(),
            VectorMaintenancePreparationPhaseForTest::CandidateAdmitted,
        );
        let worker_db = Arc::clone(&db);
        let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
        if let Err(error) = before.wait_until_reached_timeout(Duration::from_secs(120)) {
            before.release();
            after.release();
            let result = worker.join().unwrap();
            panic!("candidate transition not reached: {error}; {result:?}");
        }
        let usage = db.accountant().usage();
        let held = db
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace;
        db.accountant().set_budget(Some(usage.used)).unwrap();
        before.release();
        if let Err(error) = after.wait_until_reached_timeout(Duration::from_secs(120)) {
            after.release();
            let result = worker.join().unwrap();
            panic!("candidate transition refused despite held capacity: {error}; {result:?}");
        }
        let transferred = db
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace;
        let transferred_usage = db.accountant().usage();
        assert!(
            transferred > 0 && transferred < held,
            "change={publish_change}: {held} -> {transferred}"
        );
        assert_eq!(transferred_usage.used, usage.used - held + transferred);
        assert_eq!(generation(&db), old_generation);
        eprintln!(
            "candidate zero-headroom shrink change={publish_change} old={held} new={transferred} used_before={} used_after={}",
            usage.used, transferred_usage.used
        );
        // Restore the normal budget while paused, before prior-base source
        // pages or candidate preparation allocate their own admitted owners.
        db.accountant().set_budget(Some(WRITER_LIMIT)).unwrap();
        after.release();
        let report = worker.join().unwrap().unwrap();
        assert_eq!(report.vector.built_partitions, 1, "{report:?}");
        assert!(report.vector.first_failure.is_none(), "{report:?}");
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
        if publish_change {
            assert_eq!(generation(&db), old_generation);
            let status = db
                .vector_store_for_test()
                .partition_graph_generation_status(&route())
                .unwrap();
            assert!(status.change.or(status.dormant_change).is_some());
        } else {
            assert_published(&db, report);
        }
        assert_indexed(&db, 63, usize::from(publish_change));
        db.close().unwrap();
    }
}
