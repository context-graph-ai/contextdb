use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::{VectorPartitionRef, store::VectorMaintenancePreparationPhaseForTest};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

fn seed(db: &Database, scopes: usize, _m: usize) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let max_partitions = scopes.max(3);
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS {max_partitions} SEARCH_MODE INDEXED )"), &HashMap::new()).unwrap();
    for scope in 0..scopes {
        for row in 0..16 {
            db.execute(
                "INSERT INTO items VALUES ($id, $scope, $v)",
                &HashMap::from([
                    ("id".into(), Value::Int64((scope * 16 + row) as i64)),
                    ("scope".into(), Value::Int64(scope as i64)),
                    (
                        "v".into(),
                        Value::Vector(vec![1.0, row as f32 / 16.0, scope as f32 / 16.0]),
                    ),
                ]),
            )
            .unwrap();
        }
    }
}
fn partition(scope: i64) -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
    )
}
fn query(db: &Database, scope: Option<i64>) {
    let filter = scope
        .map(|s| format!("WHERE scope = {s}"))
        .unwrap_or_default();
    let result = db.execute(&format!("SELECT id FROM items {filter} ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR INDEXED LIMIT 3"), &HashMap::new()).unwrap();
    assert_eq!(result.rows().len(), 3);
}
#[test]
fn declared_topology_changes_retained_graph_charge() {
    let mut charges = Vec::new();
    for m in [4, 24] {
        let db = Database::open_memory();
        seed(&db, 1, m);
        db.run_maintenance_cycle().unwrap();
        query(&db, Some(0));
        charges.push(db.vector_memory_ownership_receipt_for_test().mutable_tail);
    }
    eprintln!("retained topology charges: {charges:?}");
    assert!(
        charges[1] > charges[0],
        "declared link capacity owns memory"
    );
}

#[test]
#[ignore = "vector index construction remains disabled"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn in_memory_construction_leaves_same_partition_vector_writes_usable() {
    let db = Arc::new(Database::open_memory());
    seed(&db, 1, 4);
    let target = partition(0);
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&target, 3);
    let worker_db = db.clone();
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(
        pause.wait_until_reached(Duration::from_secs(10)),
        "the production in-memory build reaches its third inserted vector"
    );

    let publication_is_free = db
        .vector_store_for_test()
        .try_partition_maintenance_lock_for_test(&target);
    if !publication_is_free {
        pause.release();
        worker
            .join()
            .expect("join the blocked maintenance proof")
            .expect("the maintenance build itself succeeds");
        panic!("full graph construction must not hold the mutex a same-partition commit needs");
    }

    db.execute(
        "INSERT INTO items VALUES (1000, 0, '[0,0,1]')",
        &HashMap::new(),
    )
    .expect("a same-partition vector commit completes while construction remains paused");
    let visible = db
        .execute(
            "SELECT id FROM items WHERE scope = 0 ORDER BY v <=> [0,0,1] USE VECTOR EXACT LIMIT 1",
            &HashMap::new(),
        )
        .expect("the committed vector is exact-searchable before construction resumes");
    assert_eq!(visible.rows(), vec![vec![Value::Int64(1000)]]);

    pause.release();
    let first = worker
        .join()
        .expect("join the in-memory maintenance worker")
        .expect("a concurrently stale build returns normally");
    assert_eq!(
        first.vector.built_partitions, 1,
        "the same finite wake discards the stale sample and rebuilds from the committed write"
    );
    let indexed = db
        .execute(
            "SELECT id FROM items WHERE scope = 0 ORDER BY v <=> [0,0,1] USE VECTOR INDEXED LIMIT 1",
            &HashMap::new(),
        )
        .expect("the replacement route includes the concurrent vector");
    assert_eq!(indexed.rows(), vec![vec![Value::Int64(1000)]]);
}

#[test]
#[ignore = "vector index construction remains disabled"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn in_memory_construction_leaves_new_partition_vector_writes_usable() {
    let db = Arc::new(Database::open_memory());
    seed(&db, 1, 4);
    let building = partition(0);
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&building, 3);
    let worker_db = db.clone();
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(
        pause.wait_until_reached(Duration::from_secs(10)),
        "the production in-memory build reaches its third inserted vector"
    );

    let structural_publication_is_free = db
        .vector_store_for_test()
        .try_bulk_maintenance_lock_for_test();
    if !structural_publication_is_free {
        pause.release();
        worker
            .join()
            .expect("join the blocked maintenance proof")
            .expect("the maintenance build itself succeeds");
        panic!("full graph construction must not hold the gate a new-partition commit needs");
    }

    db.execute(
        "INSERT INTO items VALUES (1000, 1, '[0,0,1]')",
        &HashMap::new(),
    )
    .expect("a new-partition vector commit completes while construction remains paused");
    let visible = db
        .execute(
            "SELECT id FROM items WHERE scope = 1 ORDER BY v <=> [0,0,1] USE VECTOR EXACT LIMIT 1",
            &HashMap::new(),
        )
        .expect("the new partition is exact-searchable before construction resumes");
    assert_eq!(visible.rows(), vec![vec![Value::Int64(1000)]]);

    pause.release();
    assert_eq!(
        worker
            .join()
            .expect("join the in-memory maintenance worker")
            .expect("the sampled maintenance cycle succeeds")
            .vector
            .built_partitions,
        1,
        "the fixed candidate snapshot completes while the new partition waits for the next wake"
    );
    assert_eq!(
        db.run_maintenance_cycle()
            .expect("the next finite cycle advances the new partition")
            .vector
            .built_partitions,
        1
    );
    let indexed = db
        .execute(
            "SELECT id FROM items WHERE scope = 1 ORDER BY v <=> [0,0,1] USE VECTOR INDEXED LIMIT 1",
            &HashMap::new(),
        )
        .expect("the next maintenance wake publishes the new partition's route");
    assert_eq!(indexed.rows(), vec![vec![Value::Int64(1000)]]);
}

#[test]
fn in_memory_consolidation_keeps_an_active_old_snapshot_on_its_indexed_route() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (
            id INT PRIMARY KEY,
            scope INT NOT NULL,
            v VECTOR(3) PARTITION_KEY (scope) SEARCH_MODE INDEXED
                
                CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1)
        )",
        &HashMap::new(),
    )
    .expect("declare an in-memory maintained index");
    db.execute(
        "INSERT INTO items VALUES (1, 0, '[1,0,0]'), (2, 0, '[0,1,0]')",
        &HashMap::new(),
    )
    .expect("seed the old indexed answer");
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let old_snapshot = db.snapshot();
    let old_pin = db.pin_snapshot(old_snapshot).unwrap();
    let sql = "SELECT id FROM items WHERE scope = 0 \
               ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 1";
    assert_eq!(
        db.execute_at_snapshot(sql, &HashMap::new(), old_snapshot)
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(1)]]
    );

    db.execute(
        "UPDATE items SET v = '[0,0,1]' WHERE id = 1",
        &HashMap::new(),
    )
    .expect("replace the nearest vector after the old snapshot is active");
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1,
        "the replacement crosses the declared consolidation threshold"
    );
    assert_eq!(
        db.execute_at_snapshot(sql, &HashMap::new(), old_snapshot)
            .expect("consolidation retains the active snapshot's indexed route")
            .rows(),
        vec![vec![Value::Int64(1)]]
    );
    drop(old_pin);
}

#[test]
#[ignore = "vector index construction remains disabled"]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn changing_the_poll_interval_returns_while_engine_maintenance_is_active() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE items (
            id INT PRIMARY KEY,
            scope INT NOT NULL,
            v VECTOR(3) PARTITION_KEY (scope) SEARCH_MODE INDEXED
                
        )",
        &HashMap::new(),
    )
    .expect("declare engine-owned vector maintenance");
    for row in 0..16 {
        db.execute(
            "INSERT INTO items VALUES ($id, 0, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(row))]),
        )
        .expect("seed active maintenance work");
    }

    let clock = db
        .__hold_engine_owned_vector_maintenance_clock_for_test()
        .expect("hold the real engine-owned maintenance clock");
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&partition(0), 1);
    let cycle = thread::spawn(move || clock.wake_once());
    assert!(
        pause.wait_until_reached(Duration::from_secs(10)),
        "the controlled wake must reach the index-construction progress point"
    );

    let (returned_tx, returned_rx) = mpsc::channel();
    let setter_db = db.clone();
    let setter = thread::spawn(move || {
        let result = setter_db.set_maintenance_poll_interval(Duration::from_millis(37));
        returned_tx
            .send(())
            .expect("report the setter's completion event");
        result
    });
    returned_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the interval setter returns while graph construction is still paused");
    assert_eq!(
        db.maintenance_poll_interval(),
        Duration::from_millis(37),
        "the setter publishes its value while graph construction is still paused"
    );
    setter
        .join()
        .expect("join the interval setter")
        .expect("the interval setter succeeds");

    pause.release();
    cycle
        .join()
        .expect("join the controlled maintenance wake")
        .expect("the controlled maintenance wake completes");
}

#[test]
fn in_memory_maintenance_honours_declared_consolidation_thresholds() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (\
         id INT PRIMARY KEY, \
         scope INT NOT NULL, \
         v VECTOR(3) PARTITION_KEY (scope) SEARCH_MODE INDEXED \
              \
             CONSOLIDATION (CHANGE_PERCENT = 50, TOMBSTONE_PERCENT = 50))",
        &HashMap::new(),
    )
    .expect("declare a high consolidation threshold for the in-memory route");
    for row in 0..16 {
        db.execute(
            "INSERT INTO items VALUES ($id, 0, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(row))]),
        )
        .expect("seed one in-memory vector");
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let first_generation = db
        .__debug_vector_partition_generation_for_test(target_index(), target_key())
        .expect("the initial in-memory build has a generation identity");

    db.execute(
        "UPDATE items SET v = '[0,1,0]' WHERE id = 0",
        &HashMap::new(),
    )
    .expect("make a below-threshold replacement");
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        0,
        "declared below-threshold changes leave the healthy graph idle"
    );
    assert_eq!(
        db.__debug_vector_partition_generation_for_test(target_index(), target_key()),
        Some(first_generation)
    );

    for row in 1..8 {
        db.execute(
            "UPDATE items SET v = '[0,1,0]' WHERE id = $id",
            &HashMap::from([("id".into(), Value::Int64(row))]),
        )
        .expect("cross the declared consolidation threshold");
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1,
        "above-threshold changes rebuild through the production maintenance door"
    );
    let mut partition_observation_owner_3 = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory()
            .expect("admit the partition observation reader"),
    );

    let consolidated = db
        .vector_store_for_test()
        .partition_info(
            &target_index(),
            &target_key(),
            &mut partition_observation_owner_3,
        )
        .unwrap()
        .unwrap();
    assert!(
        db.__debug_vector_partition_generation_for_test(target_index(), target_key())
            .unwrap()
            > first_generation
    );
    assert_eq!(
        (consolidated.pending_inserts, consolidated.tombstones),
        (0, 0)
    );
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        0
    );
}

fn target_index() -> VectorIndexRef {
    VectorIndexRef::new("items", "v")
}

fn target_key() -> VectorPartitionKey {
    VectorPartitionKey::from_values(&[Value::Int64(0)]).unwrap()
}
#[test]
fn unlimited_memory_keeps_loaded_graphs_warm_across_search_and_idle_maintenance() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("working-set.db");
    let db = Database::open(&path).unwrap();
    seed(&db, 4, 4);
    for _ in 0..4 {
        db.run_maintenance_cycle().unwrap();
    }
    db.close().unwrap();
    drop(db);
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    query(&db, None);
    let broad = db.vector_memory_ownership_receipt_for_test();
    let events = db
        .vector_store_for_test()
        .passive_activity_counters_for_test();
    eprintln!(
        "broad residency={} evictions={}",
        broad.base_graph, events.sealed_generation_evictions
    );
    assert!(
        broad.base_graph > 0,
        "without a declared memory limit, a completed search keeps its loaded graph working set"
    );
    assert_eq!(
        events.sealed_generation_evictions, 0,
        "an unlimited store has no pressure signal that permits graph eviction"
    );
    query(&db, Some(0));
    let before_idle_maintenance = db.vector_memory_ownership_receipt_for_test().base_graph;
    db.run_maintenance_cycle().unwrap();
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test().base_graph,
        before_idle_maintenance,
        "idle maintenance does not invent an undeclared graph-residency ceiling"
    );
    assert_eq!(
        db.vector_store_for_test()
            .passive_activity_counters_for_test()
            .sealed_generation_evictions,
        0
    );
}
#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn durable_build_admits_before_loading_and_reports_completed_work() {
    for phase in [
        VectorMaintenancePreparationPhaseForTest::LoadAuthoritativeRawVectors,
        VectorMaintenancePreparationPhaseForTest::HashStateDigest,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::open(dir.path().join("build.db")).unwrap());
        seed(&db, 1, 4);
        let pause =
            db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(&partition(0), phase);
        let worker_db = db.clone();
        let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
        let reached = pause.wait_until_reached_timeout(Duration::from_secs(10));
        let charge = db.vector_memory_ownership_receipt_for_test();
        let mut partition_observation_owner_4 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let info = db
            .vector_store_for_test()
            .partition_info(
                &partition(0).index,
                &partition(0).partition_key,
                &mut partition_observation_owner_4,
            )
            .unwrap()
            .unwrap();
        pause.release();
        let result = worker.join().unwrap();
        reached.unwrap();
        result.unwrap();
        eprintln!(
            "phase={phase:?} workspace={} progress={:?}",
            charge.temporary_workspace, info.maintenance_progress
        );
        assert!(
            charge.temporary_workspace > 0,
            "admission precedes authoritative input allocation"
        );
        let progress = info
            .maintenance_progress
            .expect("real durable build is visible");
        assert_eq!(progress.state, "building");
        assert_eq!(progress.vectors_total, 16);
        if phase == VectorMaintenancePreparationPhaseForTest::HashStateDigest {
            assert_eq!(
                progress.vectors_done, 16,
                "actual inserted points advance progress"
            );
        }
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
    }
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn dropping_a_sampled_vector_column_does_not_abort_cycle_closing() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path().join("dropped-mid-wake.db")).unwrap());
    seed(&db, 1, 4);
    db.clear_maintenance_cycle_stamp_for_test();
    let pause = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &partition(0),
        VectorMaintenancePreparationPhaseForTest::LoadAuthoritativeRawVectors,
    );
    let worker_db = db.clone();
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    pause
        .wait_until_reached_timeout(Duration::from_secs(10))
        .expect("the sampled durable build reaches its authoritative-load boundary");

    db.execute("ALTER TABLE items DROP COLUMN v", &HashMap::new())
        .expect("DDL removes the sampled vector identity without waiting for graph construction");
    pause.release();
    let report = worker
        .join()
        .expect("join maintenance after concurrent DDL")
        .expect("an absent sampled partition is reportable rather than cycle-fatal");
    assert_eq!(
        report.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::BuildFailure)
    );
    let details = report
        .vector
        .first_failure_details
        .expect("the dropped route keeps a safe report detail");
    assert_eq!(details.operation, "locate_vector_partition");
    assert!(details.message.contains("no longer present in memory"));
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0,
        "the failed sampled build releases its admitted workspace"
    );
    assert!(
        db.execute("SHOW VECTOR_INDEXES", &HashMap::new())
            .unwrap()
            .rows()
            .is_empty(),
        "failure recording cannot recreate the dropped vector identity"
    );
}

fn cell_text(result: &contextdb_engine::QueryResult, row: usize, column: &str) -> String {
    let pos = result
        .columns()
        .iter()
        .position(|name| name == column)
        .unwrap();
    match &result.rows()[row][pos] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text: {other:?}"),
    }
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn failed_partition_does_not_starve_actual_work_and_reports_active_progress() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(if durable {
            Database::open(dir.path().join("fair.db")).unwrap()
        } else {
            Database::open_memory()
        });
        seed(&db, 2, 4);
        db.set_memory_limit(Some(db.accountant().usage().used + 4096))
            .unwrap();
        assert_eq!(
            db.run_maintenance_cycle().unwrap().vector.first_failure,
            Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
        );
        db.set_memory_limit(None).unwrap();
        let pause = db.__arm_vector_maintenance_progress_pause_for_test(&partition(1), 5);
        let copy = db.clone();
        let worker = thread::spawn(move || copy.run_maintenance_cycle());
        let reached = pause.wait_until_reached(Duration::from_secs(15));
        let observed = db.execute("SHOW VECTOR_INDEXES", &HashMap::new()).unwrap();
        let mut partition_observation_owner_5 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let info = db
            .vector_store_for_test()
            .partition_info(
                &partition(1).index,
                &partition(1).partition_key,
                &mut partition_observation_owner_5,
            )
            .unwrap()
            .unwrap();
        pause.release();
        worker.join().unwrap().unwrap();
        assert!(
            reached,
            "the next finite cycle must select the other partition"
        );
        assert_eq!(
            cell_text(&observed, 0, "maintenance_state"),
            "building",
            "a previously refused partition that already succeeded in this all-needy wake no longer reports a stale stall"
        );
        let progress = info.maintenance_progress.unwrap();
        assert_eq!((progress.vectors_done, progress.vectors_total), (5, 16));
        eprintln!(
            "durable={durable} mixed_state actual_insertions={}/{}",
            progress.vectors_done, progress.vectors_total
        );
        query(&db, Some(1));
        db.run_maintenance_cycle().unwrap();
        query(&db, Some(0));
    }
}

#[test]
fn working_set_releases_other_partitions_before_the_next_selected_load() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("pressure.db")).unwrap();
    seed(&db, 2, 4);
    for _ in 0..2 {
        db.run_maintenance_cycle().unwrap();
    }
    query(&db, Some(0));
    let first = db.vector_memory_ownership_receipt_for_test().base_graph;
    query(&db, Some(1));
    let second = db.vector_memory_ownership_receipt_for_test().base_graph;
    assert_eq!(
        first, second,
        "moving the active scope must release the other reloadable graph"
    );
    assert!(first > 0);
    eprintln!("selected_graph_bytes={second} prior_graph_released={first}");
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn delta_replay_and_generation_compaction_report_actual_work() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path().join("states.db")).unwrap());
    seed(&db, 1, 4);
    db.run_maintenance_cycle().unwrap();
    for (first, phase, total) in [(0, "replaying", 8), (8, "compacting", 16)] {
        for id in first..first + 8 {
            db.execute(
                "UPDATE items SET v = '[0,1,0]' WHERE id = $id",
                &HashMap::from([("id".into(), Value::Int64(id))]),
            )
            .unwrap();
        }
        let pause = db.__arm_vector_maintenance_progress_pause_for_test(&partition(0), 3);
        let copy = db.clone();
        let worker = thread::spawn(move || copy.run_maintenance_cycle());
        let reached = pause.wait_until_reached(Duration::from_secs(10));
        let shown = db.execute("SHOW VECTOR_INDEXES", &HashMap::new()).unwrap();
        let mut partition_observation_owner_6 = contextdb_engine::vector_read_memory::admission(
            db.declared_reader_memory()
                .expect("admit the partition observation reader"),
        );

        let progress = db
            .vector_store_for_test()
            .partition_info(
                &partition(0).index,
                &partition(0).partition_key,
                &mut partition_observation_owner_6,
            )
            .unwrap()
            .unwrap()
            .maintenance_progress;
        pause.release();
        worker.join().unwrap().unwrap();
        assert!(reached);
        assert_eq!(cell_text(&shown, 0, "maintenance_state"), phase);
        let progress = progress.unwrap();
        assert_eq!((progress.vectors_done, progress.vectors_total), (3, total));
        eprintln!("maintenance_state={phase} actual_insertions=3 total={total}");
        query(&db, Some(0));
    }
}
