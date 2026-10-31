//! Vector upkeep must finish beside admitted writes when catalogs exceed one inventory admission.
use crate::resident_history_support::*;
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, database::resident_history_test_seams::observe};
use contextdb_vector::{VectorPartitionRef, store::VectorMaintenanceFailure};
use serde_json::{Value as Json, json};

fn route(scope: usize) -> VectorPartitionRef {
    VectorPartitionRef::new(index("building"), partition(scope as i64))
}

fn state(db: &Database, scope: usize) -> (bool, Option<u64>, usize) {
    let route = route(scope);
    let mut owner =
        contextdb_engine::vector_read_memory::admission(db.declared_reader_memory().unwrap());
    let info = db
        .vector_store_for_test()
        .partition_info(&route.index, &route.partition_key, &mut owner)
        .unwrap()
        .unwrap();
    (
        info.graph_available,
        info.base_generation,
        info.maintenance_progress.map_or(0, |p| p.vectors_done),
    )
}

fn retained(db: &Database, scope: usize) -> usize {
    let r = db.__debug_vector_partition_generation_retention_for_test(&route(scope));
    r.durable_superseded_generations + r.resident_superseded_generations
}

fn search(db: &Database, scope: usize, mode: &str) {
    let result = db.execute(
        &format!("SELECT id FROM building WHERE scope = {scope} ORDER BY v <=> $v USE VECTOR {mode} LIMIT 1"),
        &std::collections::HashMap::from([("v".into(), Value::Vector(vector(scope, 3)))]),
    ).unwrap();
    assert_eq!(result.rows(), vec![vec![Value::Int64(scope as i64)]]);
}

// Shared rows exercise initial construction, replacement and finite reclamation.
// A single-catalog control establishes that the allowance admits useful work;
// only the number of unrelated durable catalogs changes in the pressure row.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn maintenance_row(catalogs: usize, cycles: usize) -> Json {
    const WORKSPACE: usize = 256 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let db = open(
        &directory.path().join("catalogs.db"),
        Some(64 * 1024 * 1024),
    );
    for table in ["catalogs", "building"] {
        exec(
            &db,
            &format!(
 "CREATE TABLE {table} (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 2048 SEARCH_MODE INDEXED ) HISTORY CURRENT ONLY SYNC OFF"
            ),
        );
    }
    exec(
        &db,
        "CREATE TABLE current_versions (id INT PRIMARY KEY, revision INT) HISTORY CURRENT ONLY SYNC OFF",
    );
    exec(&db, "CREATE TABLE writes (id INT PRIMARY KEY)");
    insert_vector(&db, "building", 0, Some(0), 3);
    for scope in 1..catalogs {
        insert_vector(&db, "catalogs", scope, Some(scope as i64), 3);
    }
    for _ in 0..64 {
        db.run_maintenance_cycle().unwrap();
        if state(&db, 0).0
            && db
                .__durable_vector_storage_inventory_for_test(&route(0))
                .unwrap()
                .catalogs
                == catalogs
        {
            break;
        }
    }
    assert!(state(&db, 0).0, "setup must publish real durable catalogs");
    let inventory = db
        .__durable_vector_storage_inventory_for_test(&route(0))
        .unwrap();
    assert_eq!(inventory.catalogs, catalogs);
    let old = db.pin_snapshot(db.snapshot()).unwrap();
    let initial_generation = state(&db, 0).1;
    // Change authoritative vectors as well as topology: an equal-frontier
    // replacement need not retain an identical predecessor for the old snapshot.
    exec(&db, "UPDATE building SET v = '[0,1,0]' WHERE scope = 0");
    // Publish one superseding generation while its predecessor is protected.
    exec(
        &db,
 "ALTER TABLE building ALTER COLUMN v SET IVF DEFAULT",
    );
    for _ in 0..64 {
        db.run_maintenance_cycle().unwrap();
        if state(&db, 0).1 > initial_generation {
            break;
        }
    }
    let replacement_generation = state(&db, 0).1;
    assert!(replacement_generation > initial_generation);
    let retained_before = retained(&db, 0);
    assert!(
        retained_before > 0,
        "a protected real superseded generation is required"
    );
    drop(old);
    // One selected route needs another replacement and one needs its initial build.
    exec(&db, "UPDATE building SET v = '[0,0,1]' WHERE scope = 0");
    exec(
        &db,
 "ALTER TABLE building ALTER COLUMN v SET IVF DEFAULT",
    );
    insert_vector(&db, "building", catalogs, Some(catalogs as i64), 3);
    for id in 0..8 {
        exec(
            &db,
            &format!("INSERT INTO current_versions VALUES ({id},0)"),
        );
        exec(
            &db,
            &format!("UPDATE current_versions SET revision = 1 WHERE id = {id}"),
        );
    }
    let versions_before = db
        .__durable_version_count_for_test("current_versions")
        .unwrap();
    assert_eq!(versions_before, 16);
    let ownership = db.writer_memory_ownership_receipt_for_test();
    let used_before = db.accountant().usage().used;
    let limit = ownership.named_owner_total + WORKSPACE;
    db.set_memory_limit(Some(limit)).unwrap();
    let mut observations = Vec::new();
    let mut refusals = 0;
    let mut built = 0;
    for cycle in 0..cycles {
        db.accountant().reset_high_water_for_test();
        let (result, work) = observe(|| db.run_maintenance_cycle());
        let report = result.as_ref().ok();
        let details = report.and_then(|report| report.vector.first_failure_details);
        let error = result.as_ref().err();
        let inventory_refused = details.is_some_and(|d| {
            d.failure == VectorMaintenanceFailure::MemoryLimit
                && d.operation == "prepare_vector_generation"
        }) || matches!(error, Some(contextdb_core::Error::MemoryBudgetExceeded { operation, .. })
                if *operation == "prepare_vector_generation");
        if inventory_refused {
            refusals += 1;
        }
        let cycle_built = report.map_or(0, |report| report.vector.built_partitions);
        built += cycle_built;
        exec(&db, &format!("INSERT INTO writes VALUES ({cycle})"));
        assert_eq!(
            exec(&db, "SELECT id FROM writes ORDER BY id"),
            (0..=cycle)
                .map(|id| vec![Value::Int64(id as i64)])
                .collect::<Vec<_>>()
        );
        assert_eq!(
            exec(&db, "SELECT id,revision FROM current_versions ORDER BY id"),
            (0..8)
                .map(|id| vec![Value::Int64(id), Value::Int64(1)])
                .collect::<Vec<_>>()
        );
        search(&db, 0, "EXACT");
        search(&db, catalogs, "EXACT");
        let writer_peak = db.accountant().high_water_for_test();
        assert!(
            writer_peak <= limit,
            "every cycle and admitted write stays within the declared writer ceiling"
        );
        assert!(
            work.workspace_peak <= WORKSPACE,
            "construction stays within the shared workspace allowance"
        );
        observations.push(json!({"cycle":cycle,"built":cycle_built,"cycle_ok":result.is_ok(),
            "pruned_versions":report.map_or(0, |report| report.currency.pruned_versions),"failure":format!("{details:?}"),"error":format!("{error:?}"),
            "initial":state(&db,catalogs),"replacement":state(&db,0),
            "source_keys":work.maintenance_source_keys,"source_visits":work.maintenance_source_visits,
            "workspace_peak":work.workspace_peak,"writer_peak":writer_peak,"writer_used":db.accountant().usage().used,
            "retained":retained(&db,0),
            "versions":db.__durable_version_count_for_test("current_versions").unwrap()}));
    }
    let initial = state(&db, catalogs);
    let replacement = state(&db, 0);
    let retained_after = retained(&db, 0);
    let versions_after = db
        .__durable_version_count_for_test("current_versions")
        .unwrap();
    if initial.0 && replacement.1 > replacement_generation {
        search(&db, 0, "INDEXED");
        search(&db, catalogs, "INDEXED");
    }
    let receipt = json!({"catalogs":catalogs,"cycles":cycles,"workspace":WORKSPACE,
        "writer_limit":limit,"named_owners":ownership.named_owner_total,"used_before":used_before,
        "inventory_refusals":refusals,"built":built,"initial":initial,
        "replacement":replacement,"replacement_generation_before":replacement_generation,
        "retained_before":retained_before,"retained_after":retained_after,
        "versions_before":versions_before,"versions_after":versions_after,
        "admitted_writes":cycles,"observations":observations});
    db.close().unwrap();
    receipt
}

#[test]
fn many_catalogs_do_not_starve_builds_or_eligible_reclamation_under_writes() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let control = maintenance_row(1, 32);
    let pressure = maintenance_row(356, 32);
    let receipt = json!({"control":control,"pressure":pressure});
    // An optional consumer parses these existing observations as a progress receipt.
    if let Some(path) = std::env::var_os("CONTEXTDB_MAINTENANCE_PROOF_RECEIPT") {
        std::fs::write(path, serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();
    }
    assert_eq!(
        receipt["control"]["writer_limit"].as_u64().unwrap()
            - receipt["control"]["used_before"].as_u64().unwrap(),
        receipt["pressure"]["writer_limit"].as_u64().unwrap()
            - receipt["pressure"]["used_before"].as_u64().unwrap(),
        "control and pressure rows must provide equal spare writer memory"
    );
    for row in [&receipt["control"], &receipt["pressure"]] {
        assert_eq!(
            row["initial"][0], true,
            "initial build must complete: {row}"
        );
        assert!(
            row["replacement"][1].as_u64().unwrap()
                > row["replacement_generation_before"].as_u64().unwrap(),
            "replacement must publish a newer generation: {row}"
        );
        assert_eq!(
            row["retained_after"], 0,
            "eligible superseded generations must be reclaimed: {row}"
        );
        assert_eq!(
            row["versions_after"], 8,
            "current-only physical versions must be reclaimed: {row}"
        );
    }
}
