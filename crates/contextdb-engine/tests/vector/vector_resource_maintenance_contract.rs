use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, MaintenancePolicy, VectorJournalTruncationPhaseForTest,
    VectorPartitionJournalFileForTest,
};
use contextdb_vector::{VectorPartitionRef, store::VectorMaintenancePreparationPhaseForTest};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

fn seed(db: &Database, scopes: usize, m: usize) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let max_partitions = scopes.max(3);
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS {max_partitions} SEARCH_MODE INDEXED HNSW (M = {m}, EF_CONSTRUCTION = 64))"), &HashMap::new()).unwrap();
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
    assert_eq!(result.rows.len(), 3);
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
fn in_memory_construction_leaves_same_partition_vector_writes_usable() {
    let db = Arc::new(Database::open_memory());
    seed(&db, 1, 4);
    let target = partition(0);
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&target, 3);
    let worker_db = db.clone();
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(
        pause.wait_until_reached_blocking(),
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
    assert_eq!(visible.rows, vec![vec![Value::Int64(1000)]]);

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
    assert_eq!(indexed.rows, vec![vec![Value::Int64(1000)]]);
}

#[test]
fn in_memory_construction_leaves_new_partition_vector_writes_usable() {
    let db = Arc::new(Database::open_memory());
    seed(&db, 1, 4);
    let building = partition(0);
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&building, 3);
    let worker_db = db.clone();
    let worker = thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(
        pause.wait_until_reached_blocking(),
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
    assert_eq!(visible.rows, vec![vec![Value::Int64(1000)]]);

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
    assert_eq!(indexed.rows, vec![vec![Value::Int64(1000)]]);
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
                HNSW (M = 4, EF_CONSTRUCTION = 64)
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
    let old_pin = db.pin_snapshot(old_snapshot);
    let sql = "SELECT id FROM items WHERE scope = 0 \
               ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 1";
    assert_eq!(
        db.execute_at_snapshot(sql, &HashMap::new(), old_snapshot)
            .unwrap()
            .rows,
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
            .rows,
        vec![vec![Value::Int64(1)]]
    );
    drop(old_pin);
}

#[test]
fn changing_the_poll_interval_returns_while_engine_maintenance_is_active() {
    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE items (
            id INT PRIMARY KEY,
            scope INT NOT NULL,
            v VECTOR(3) PARTITION_KEY (scope) SEARCH_MODE INDEXED
                HNSW (M = 4, EF_CONSTRUCTION = 64)
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
    assert!(pause.wait_until_reached_blocking());

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
        .recv()
        .expect("observe the setter's completion event without an elapsed-time predicate");
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
             HNSW (M = 4, EF_CONSTRUCTION = 64) \
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
        .vector_store_for_test()
        .partition_info(&target_index(), &target_key())
        .expect("the maintained partition is inspectable")
        .base_generation
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
        db.vector_store_for_test()
            .partition_info(&target_index(), &target_key())
            .unwrap()
            .base_generation,
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
    let consolidated = db
        .vector_store_for_test()
        .partition_info(&target_index(), &target_key())
        .unwrap();
    assert!(consolidated.base_generation.unwrap() > first_generation);
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
        let info = db
            .vector_store_for_test()
            .partition_info(&partition(0).index, &partition(0).partition_key)
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
            .rows
            .is_empty(),
        "failure recording cannot recreate the dropped vector identity"
    );
}

fn cell_text(result: &contextdb_engine::QueryResult, row: usize, column: &str) -> String {
    let pos = result
        .columns
        .iter()
        .position(|name| name == column)
        .unwrap();
    match &result.rows[row][pos] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text: {other:?}"),
    }
}

#[test]
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
        let info = db
            .vector_store_for_test()
            .partition_info(&partition(1).index, &partition(1).partition_key)
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
fn replacement_refusal_precedes_workspace_and_keeps_the_serving_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("refusal.db");
    let db = Database::open(&path).unwrap();
    seed(&db, 1, 4);
    db.run_maintenance_cycle().unwrap();
    for id in 0..4 {
        db.execute(
            "UPDATE items SET v = '[0,1,0]' WHERE id = $id",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    let before = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition(0))
        .unwrap();
    let disk_limit = std::fs::metadata(&path).unwrap().len() + 1024;
    db.set_disk_limit(Some(disk_limit)).unwrap();
    let disk_refusal = db.run_maintenance_cycle().unwrap();
    assert_eq!(
        disk_refusal.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::DiskLimit)
    );
    let disk_details = disk_refusal
        .vector
        .first_failure_details
        .expect("disk refusal retains its typed maintenance detail");
    assert_eq!(disk_details.operation, "prepare_vector_generation");
    assert!(disk_details.current_bytes.is_some());
    assert_eq!(disk_details.budget_limit_bytes, Some(disk_limit));
    assert!(disk_details.recovery_instruction.contains("SET DISK_LIMIT"));
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(
        db.vector_store_for_test()
            .partition_graph_generation_status(&partition(0))
            .unwrap(),
        before
    );
    query(&db, Some(0));
    db.set_disk_limit(None).unwrap();
    db.set_memory_limit(Some(db.accountant().usage().used + 4096))
        .unwrap();
    let memory_refusal = db.run_maintenance_cycle().unwrap();
    assert_eq!(
        memory_refusal.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );
    let memory_details = memory_refusal
        .vector
        .first_failure_details
        .expect("memory refusal retains its typed maintenance detail");
    assert_eq!(memory_details.operation, "prepare_vector_generation");
    assert!(
        memory_details.requested_bytes.unwrap() > memory_details.available_bytes.unwrap(),
        "the report keeps the exact failed reservation comparison"
    );
    assert_eq!(
        memory_details.budget_limit_bytes,
        db.accountant().usage().limit.map(|limit| limit as u64)
    );
    assert!(
        memory_details
            .recovery_instruction
            .contains("SET MEMORY_LIMIT")
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    db.set_memory_limit(None).unwrap();
    query(&db, Some(0));
    db.run_maintenance_cycle().unwrap();
    query(&db, Some(0));
}

#[test]
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
        let progress = db
            .vector_store_for_test()
            .partition_info(&partition(0).index, &partition(0).partition_key)
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

/// Builds the independent `cleanup_items` route, publishes its first
/// generation, then stages a durable-but-uncleaned journal by updating a row
/// and interrupting the very next cycle after its catalog publication but
/// before the covered journal record is deleted. Returns the route and the
/// journal file the interrupted cycle left pending, so a caller can drive
/// further cycles against a route that already carries pending cleanup.
fn pending_cleanup_route(
    db: &Database,
    key: String,
) -> (VectorPartitionRef, VectorPartitionJournalFileForTest) {
    db.execute(
        "CREATE TABLE cleanup_items (\
         id INT PRIMARY KEY, \
         scope TEXT NOT NULL, \
         v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 3 SEARCH_MODE INDEXED\
         )",
        &HashMap::new(),
    )
    .expect("create the independent cleanup-retry route");
    db.execute(
        "INSERT INTO cleanup_items VALUES ($id, $scope, $v)",
        &HashMap::from([
            ("id".into(), Value::Int64(1)),
            ("scope".into(), Value::Text(key.clone())),
            ("v".into(), Value::Vector(vec![1.0, 0.0, 0.0])),
        ]),
    )
    .expect("seed the long-key cleanup route");
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let cleanup_route = VectorPartitionRef::new(
        VectorIndexRef::new("cleanup_items", "v"),
        VectorPartitionKey::from_values(&[Value::Text(key)]).unwrap(),
    );
    db.execute(
        "UPDATE cleanup_items SET v = '[0,1,0]' WHERE id = 1",
        &HashMap::new(),
    )
    .expect("create one covered journal record for retry");
    let crash = db.__arm_vector_journal_truncation_crash_for_test(
        &cleanup_route,
        VectorJournalTruncationPhaseForTest::CatalogDurableBeforeCoveredJournalDelete,
    );
    let interrupted = db.run_maintenance_cycle().unwrap();
    assert_eq!(
        interrupted.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::BuildFailure)
    );
    assert_eq!(
        interrupted.vector.built_partitions, 1,
        "the durable catalog publication counts even while covered-journal cleanup is pending"
    );
    let interrupted_details = interrupted
        .vector
        .first_failure_details
        .expect("the cleanup interruption remains actionable in the cycle report");
    assert_eq!(
        interrupted_details.operation,
        "truncate_vector_partition_journal"
    );
    assert!(interrupted_details.message.contains("graph is durable"));
    assert!(
        interrupted_details
            .recovery_instruction
            .contains("another maintenance cycle")
    );
    assert_eq!(
        db.vector_store_for_test()
            .partition_info(&cleanup_route.index, &cleanup_route.partition_key)
            .and_then(|partition| partition.maintenance_failure_details),
        Some(interrupted_details),
        "the actual cleanup route retains the same safe detail for inspection"
    );
    drop(crash);
    let pending_cleanup = db.__debug_vector_partition_journal_file_for_test(&cleanup_route);
    assert!(!pending_cleanup.physical_record_lsns.is_empty());
    (cleanup_route, pending_cleanup)
}

/// Whichever generation is presently serving the route -- a route freshly
/// selected by one build carries it on `base`; a route mid-consolidation
/// carries the newer one on `change`.
fn selected_generation_id(status: &contextdb_vector::store::VectorGraphGenerationStatus) -> u64 {
    status
        .change
        .or(status.base)
        .map(|generation| generation.generation_id)
        .expect("the route holds a resident selected generation")
}

#[test]
fn refused_partition_still_closes_the_cycle_and_retires_a_sibling_partition() {
    let dir = tempfile::tempdir().unwrap();
    let calibration = Arc::new(Database::open(dir.path().join("calibration.db")).unwrap());
    seed(&calibration, 1, 4);
    let pause = calibration.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &partition(0),
        VectorMaintenancePreparationPhaseForTest::LoadAuthoritativeRawVectors,
    );
    let copy = calibration.clone();
    let worker = thread::spawn(move || copy.run_maintenance_cycle());
    let reached = pause.wait_until_reached_timeout(Duration::from_secs(10));
    let small_workspace = calibration
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    pause.release();
    worker.join().unwrap().unwrap();
    reached.unwrap();
    assert!(small_workspace > 0);
    calibration.close().unwrap();
    let db = Database::open(dir.path().join("starvation.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    // Cleanup duplicates the route key while validating its catalog and
    // covered journal keys. Keep that workspace larger than the two-build
    // headroom below.
    let cleanup_key = "k".repeat(small_workspace.saturating_mul(3).max(16 * 1024));
    let (cleanup_route, pending_cleanup) = pending_cleanup_route(&db, cleanup_key);

    seed(&db, 3, 4);
    for id in 48..176 {
        db.execute(
            "INSERT INTO items VALUES ($id, 0, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.set_memory_limit(Some(db.accountant().usage().used + small_workspace * 2))
        .unwrap();
    db.clear_maintenance_cycle_stamp_for_test();
    assert!(
        !db.maintenance_cycle_stamp_is_set_for_test(),
        "the refusal cycle must write a fresh closing stamp"
    );
    let first = db
        .run_maintenance_cycle()
        .expect("a partition-local refusal is reported without aborting cycle closing");
    assert_eq!(
        first.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );
    let cleanup_failure = first
        .vector
        .first_failure_details
        .expect("the unrelated cleanup refusal remains attached to the report");
    assert_eq!(
        cleanup_failure.operation,
        "truncate_vector_partition_journal"
    );
    assert!(cleanup_failure.message.contains("graph is durable"));
    let retained_cleanup_failure = db
        .vector_store_for_test()
        .partition_info(&cleanup_route.index, &cleanup_route.partition_key)
        .and_then(|partition| partition.maintenance_failure_details)
        .expect("the refusing cleanup route retains an inspectable detail");
    assert_eq!(retained_cleanup_failure.failure, cleanup_failure.failure);
    assert_eq!(
        retained_cleanup_failure.operation,
        cleanup_failure.operation
    );
    assert_eq!(
        retained_cleanup_failure.recovery_instruction,
        cleanup_failure.recovery_instruction
    );
    assert_eq!(first.vector.built_partitions, 2);
    assert_eq!(first.vector.built_indexes, 1);
    for healthy_scope in [1, 2] {
        assert!(
            db.vector_store_for_test()
                .partition_info(
                    &partition(healthy_scope).index,
                    &partition(healthy_scope).partition_key,
                )
                .unwrap()
                .maintenance_failure_details
                .is_none(),
            "an unrelated cleanup refusal cannot mark a published sibling as failed"
        );
    }
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(
        db.__debug_vector_partition_journal_file_for_test(&cleanup_route),
        pending_cleanup,
        "cleanup refusal preserves its covered journal while sibling builds finish"
    );
    query(&db, Some(1));
    query(&db, Some(2));
    let held = db.pin_snapshot(db.snapshot());
    for id in 32..48 {
        db.execute(
            "DELETE FROM items WHERE id = $id",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    let before_retirement = db.vector_memory_ownership_receipt_for_test();
    let retained = db
        .execute("SHOW VECTOR_PARTITIONS FOR items.v", &HashMap::new())
        .unwrap();
    assert_eq!(retained.rows.len(), 3, "the held snapshot pins scope 2");
    drop(held);
    db.clear_maintenance_cycle_stamp_for_test();
    assert!(
        !db.maintenance_cycle_stamp_is_set_for_test(),
        "the retirement cycle must write a fresh closing stamp"
    );
    let second = db
        .run_maintenance_cycle()
        .expect("the recurring refusal cannot skip sibling publication or retirement");
    assert_eq!(
        second.vector.first_failure,
        Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
    );
    assert_eq!(second.vector.built_partitions, 0);
    assert_eq!(second.vector.built_indexes, 0);
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(
        db.__debug_vector_partition_journal_file_for_test(&cleanup_route),
        pending_cleanup,
        "recurring cleanup refusal remains isolated through sibling retirement"
    );
    let shown = db
        .execute("SHOW VECTOR_PARTITIONS FOR items.v", &HashMap::new())
        .unwrap();
    assert_eq!(
        shown.rows.len(),
        2,
        "the empty scope-2 partition is retired"
    );
    let after_retirement = db.vector_memory_ownership_receipt_for_test();
    assert!(
        after_retirement.retained_total < before_retirement.retained_total
            && after_retirement.retired_pinned_graph_bytes == 0
            && after_retirement.mutable_tail < before_retirement.mutable_tail,
        "retirement releases the sibling's charged graph bytes: before={before_retirement:?} after={after_retirement:?}"
    );
    query(&db, Some(1));
    db.execute(
        "INSERT INTO items VALUES (1000, 3, '[0,0,1]')",
        &HashMap::new(),
    )
    .expect("the retired partition releases its declared slot");
    eprintln!(
        "cleanup_and_build_refusals=2 healthy_builds=1 headroom={} calibration_workspace={small_workspace}",
        small_workspace * 2
    );
}

/// A route with a durably-published catalog but an uncleaned covered journal
/// keeps serving its old maintained graph plus every fresh write while the
/// journal cleanup keeps being refused for lack of memory. No progress is
/// lost: once memory is available again, the very next cycle installs the
/// pending catalog and rebuilds the route in the same cycle.
#[test]
fn refused_cleanup_keeps_serving_under_writes_and_publishes_when_memory_returns() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("refused-cleanup.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    // Cleanup validates the whole route key against the catalog and the
    // covered journal keys; a long key alone makes that validation's first
    // reservation exceed a memory limit set just above current usage,
    // independent of how much the journal has grown.
    let cleanup_key = "k".repeat(16 * 1024);
    let (route, j0) = pending_cleanup_route(&db, cleanup_key.clone());
    let r0 = db.__debug_vector_partition_generation_retention_for_test(&route);
    let status0 = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route)
        .expect("the pending-cleanup route retains generation status");
    let g0 = selected_generation_id(&status0);
    let tail0 = status0.fresh_tail_entries;

    let write_row = |id: i64| {
        db.execute(
            "INSERT INTO cleanup_items VALUES ($id, $scope, $v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("scope".into(), Value::Text(cleanup_key.clone())),
                ("v".into(), Value::Vector(vec![0.0, 0.0, 1.0])),
            ]),
        )
        .expect("writes to the maintained route are never refused by the pending cleanup");
    };
    let search_written = |limit: usize| -> Vec<i64> {
        let result = db
            .execute(
                &format!(
                    "SELECT id FROM cleanup_items WHERE scope = $key \
                     ORDER BY v <=> [0,0,1] USE VECTOR INDEXED LIMIT {limit}"
                ),
                &HashMap::from([("key".into(), Value::Text(cleanup_key.clone()))]),
            )
            .unwrap();
        let mut ids: Vec<i64> = result
            .rows
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int64(id)) => *id,
                other => panic!("expected an integer id, got {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        ids
    };

    const K: i64 = 4;
    let mut written = 0usize;
    for wake in 1..=3i64 {
        db.set_memory_limit(None).unwrap();
        for i in 0..K {
            write_row(100 * wake + i);
        }
        written += K as usize;
        let limit = db.accountant().usage().used + 4096;
        db.set_memory_limit(Some(limit)).unwrap();

        let report = db
            .run_maintenance_cycle()
            .expect("a refused cleanup is reported without aborting cycle closing");
        assert_eq!(
            report.vector.first_failure,
            Some(contextdb_vector::VectorMaintenanceFailure::MemoryLimit)
        );
        let details = report
            .vector
            .first_failure_details
            .expect("the refusal remains actionable in the cycle report");
        assert_eq!(details.operation, "truncate_vector_partition_journal");
        assert_eq!(
            details.message,
            "the maintained graph is durable and journal cleanup could not reserve its \
             required memory"
        );
        assert!(details.requested_bytes.unwrap() > details.available_bytes.unwrap());
        assert_eq!(details.budget_limit_bytes, Some(limit as u64));
        assert!(details.recovery_instruction.contains("SET MEMORY_LIMIT"));
        assert_eq!(
            db.vector_store_for_test()
                .partition_info(&route.index, &route.partition_key)
                .and_then(|partition| partition.maintenance_failure_details),
            Some(details),
            "the refusing route retains the same detail for inspection"
        );
        assert_eq!(report.vector.built_partitions, 0);

        let retention = db.__debug_vector_partition_generation_retention_for_test(&route);
        assert_eq!(
            retention, r0,
            "a refused cleanup leaves no orphan generation"
        );

        let journal = db.__debug_vector_partition_journal_file_for_test(&route);
        assert_eq!(journal.truncated_through_lsn, j0.truncated_through_lsn);
        assert_eq!(
            journal.physical_record_lsns.len(),
            j0.physical_record_lsns.len() + written
        );
        assert_eq!(
            journal.physical_record_lsns[..j0.physical_record_lsns.len()],
            j0.physical_record_lsns[..],
            "the covered prefix stays exactly as the pending cleanup left it"
        );

        let status = db
            .vector_store_for_test()
            .partition_graph_generation_status(&route)
            .expect("route retains status");
        assert_eq!(selected_generation_id(&status), g0);
        assert_eq!(status.fresh_tail_entries, tail0 + written);
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );

        // The route's continued service is what this proves, not a bound on
        // the search's own memory cost: lift the limit before reading back,
        // exactly as the next wake's writes already do.
        db.set_memory_limit(None).unwrap();
        let expected: Vec<i64> = (1..=wake)
            .flat_map(|w| (0..K).map(move |i| 100 * w + i))
            .collect();
        assert_eq!(
            search_written(written),
            expected,
            "the old maintained route keeps serving every write made under the refusal"
        );
    }

    db.set_memory_limit(None).unwrap();
    let report = db
        .run_maintenance_cycle()
        .expect("memory returning lets the pending cleanup and the rebuild both complete");
    assert!(report.vector.first_failure.is_none());
    assert_eq!(report.vector.built_partitions, 1);
    assert!(
        db.vector_store_for_test()
            .partition_info(&route.index, &route.partition_key)
            .unwrap()
            .maintenance_failure_details
            .is_none()
    );
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route)
        .expect("route retains status");
    assert_eq!(selected_generation_id(&status), g0 + 2);
    assert_eq!(status.fresh_tail_entries, 0);
    let journal = db.__debug_vector_partition_journal_file_for_test(&route);
    assert!(journal.physical_record_lsns.is_empty());
    assert!(journal.truncated_through_lsn > j0.truncated_through_lsn);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    let expected: Vec<i64> = (1..=3i64)
        .flat_map(|w| (0..K).map(move |i| 100 * w + i))
        .collect();
    assert_eq!(search_written(12), expected);

    write_row(999);
    let status = db
        .vector_store_for_test()
        .partition_graph_generation_status(&route)
        .expect("route retains status");
    assert_eq!(status.fresh_tail_entries, 1);
    let mut expected_with_new = expected;
    expected_with_new.push(999);
    expected_with_new.sort_unstable();
    assert_eq!(search_written(13), expected_with_new);
}
