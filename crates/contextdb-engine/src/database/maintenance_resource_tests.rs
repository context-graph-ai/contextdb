use super::*;
use serial_test::serial;

#[test]
#[cfg(target_pointer_width = "64")]
fn durable_generation_workspace_separates_build_and_persistence_owners() {
    const LIMIT: usize = 2_147_483_648;
    const RETAINED: usize = 268_935_190;
    let layout = VectorIndexLayout::unpartitioned(64, VectorQuantization::F32);
    let policy = layout.resolve_policy(50_000, 1);
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("receipt_items", "embedding"),
        VectorPartitionKey::from_values(&[Value::Int64(3)]).unwrap(),
    );
    let plan = VectorGenerationWorkspacePlan::new(50_000, &layout, &partition, policy, 0);
    let available = LIMIT - RETAINED;

    assert_eq!(
        (
            policy.hnsw_m,
            policy.hnsw_ef_construction,
            policy.hnsw_ef_search
        ),
        (24, 400, 400)
    );
    assert_eq!(plan.graph_build_bytes, 681_223_128);
    assert_eq!(plan.raw_entry_bytes, 25_900_000);
    assert_eq!(plan.durable_metadata_bytes, 4_664);
    assert_eq!(plan.former_combined_bytes(), 4_872_189_880);
    assert_eq!(plan.build_phase_bytes(), 1_414_250_920);
    assert!(plan.former_combined_bytes() > available);
    assert!(plan.build_phase_bytes() <= available);
    assert!(plan.build_phase_bytes() < plan.former_combined_bytes());
    eprintln!(
        "graph_build_bytes={} raw_entry_bytes={} durable_metadata_bytes={} former_requested_bytes={} build_phase_requested_bytes={} available_bytes={available}",
        plan.graph_build_bytes,
        plan.raw_entry_bytes,
        plan.durable_metadata_bytes,
        plan.former_combined_bytes(),
        plan.build_phase_bytes(),
    );
}

#[test]
fn cancellation_during_actual_build_returns_workspace_and_preserves_serving_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("cancel.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.run_maintenance_cycle().unwrap();
    for id in 0..8 {
        db.execute(
            "UPDATE items SET v = '[0,1,0]' WHERE id = $id",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let persistence = db.persistence.as_ref().unwrap();
    let catalog = persistence
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    let owned = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition)
        .unwrap();
    let pause = db.__arm_vector_maintenance_progress_pause_for_test(&partition, 3);
    let stop = Arc::new(AtomicBool::new(false));
    let mut context = db.maintenance_context();
    context.worker_stop = Some(stop.clone());
    let worker = std::thread::spawn(move || context.run_cycle(CurrencyGate::Always));
    let reached = pause.wait_until_reached(Duration::from_secs(10));
    let workspace = db
        .vector_memory_ownership_receipt_for_test()
        .temporary_workspace;
    stop.store(true, Ordering::SeqCst);
    pause.release();
    let result = worker.join().unwrap();
    assert!(reached && workspace > 0);
    assert!(matches!(result, Err(Error::ReadCancelled)), "{result:?}");
    assert_stopped_maintenance_is_pending(&db, &partition);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(
        db.vector_store_for_test()
            .partition_graph_generation_status(&partition)
            .unwrap(),
        owned
    );
    assert_eq!(
        persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key
            )
            .unwrap()
            .unwrap(),
        catalog
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM items ORDER BY v <=> [0,1,0] USE VECTOR INDEXED LIMIT 3",
            &HashMap::new()
        )
        .unwrap()
        .rows
        .len(),
        3
    );
    db.run_maintenance_cycle().unwrap();
    eprintln!(
        "cancelled_after_actual_insertions=3 workspace_released={workspace} serving_catalog_unchanged=true"
    );
}

#[test]
// The post-publication snapshot-sample pause this test arms
// (`VECTOR_REPAIR_SNAPSHOT_SAMPLE_TRANSITION_PAUSE`) is process-wide, keyed
// by the whole binary rather than by the `Database` that armed it, so any
// sibling test in this file that also repairs a quarantined partition and
// reaches that boundary would steal or share the pause. Serialize on that
// shared resource instead of threading it through `Database`.
#[serial(vector_repair_snapshot_sample_transition_pause)]
fn cancellation_after_durable_repair_publication_preserves_repair_and_skips_cycle_closing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("late-repair-stop.db");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.run_maintenance_cycle().unwrap();
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let serving_before = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition)
        .and_then(|status| status.base.or(status.dormant_base))
        .expect("the fixture starts with one durable serving generation");
    assert!(
        db.vector_store_for_test()
            .quarantine_partition_route(&partition, VectorRouteQuarantineReason::CorruptBase,)
    );

    *db.last_maintenance_cycle_at.lock() = None;
    #[cfg(feature = "test-seams")]
    let retirement_passes_before = db.vector_partition_retirement_pass_count_for_test();
    let pause = db.__arm_one_shot_vector_repair_snapshot_sample_transition_pause_for_test();
    let stop = Arc::new(AtomicBool::new(false));
    let mut context = db.maintenance_context();
    context.worker_stop = Some(stop.clone());
    let worker = std::thread::spawn(move || context.run_cycle(CurrencyGate::Always));
    assert!(
        pause.wait_until_reached(Duration::from_secs(10)),
        "repair reaches the post-publication snapshot-sample boundary"
    );
    let durable_during_stop = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .expect("repair publishes its replacement catalog before snapshot finalization")
        .base
        .generation_id;
    assert!(durable_during_stop > serving_before.generation_id);
    stop.store(true, Ordering::SeqCst);
    pause.release();

    let result = worker.join().unwrap();
    assert!(matches!(result, Err(Error::ReadCancelled)), "{result:?}");
    assert!(
        db.last_maintenance_cycle_at.lock().is_none(),
        "a stopped cycle does not record closing success"
    );
    #[cfg(feature = "test-seams")]
    assert_eq!(
        db.vector_partition_retirement_pass_count_for_test(),
        retirement_passes_before,
        "the late stop is observed before partition retirement begins"
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0,
        "repair releases its publication workspace before returning cancellation"
    );

    let repaired = db
        .vector_store_for_test()
        .partition_graph_generation_status(&partition)
        .and_then(|status| status.base.or(status.dormant_base))
        .expect("the already-durable repair remains the serving generation");
    assert_eq!(repaired.generation_id, durable_during_stop);
    assert_eq!(
        db.execute(
            "SELECT id FROM items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3",
            &HashMap::new(),
        )
        .unwrap()
        .rows
        .len(),
        3
    );
    db.close().unwrap();
    drop(db);

    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let reopened_generation = reopened
        .vector_store_for_test()
        .partition_graph_generation_status(&partition)
        .and_then(|status| status.base.or(status.dormant_base))
        .expect("restart selects the repaired durable generation");
    assert_eq!(reopened_generation.generation_id, repaired.generation_id);
    assert_eq!(
        reopened
            .execute(
                "SELECT id FROM items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3",
                &HashMap::new(),
            )
            .unwrap()
            .rows
            .len(),
        3
    );
    reopened.close().unwrap();
}

#[test]
fn retention_cleanup_commits_generation_catalog_and_eligible_commit_index_removals_together() {
    use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};
    const CHILD: &str = "CONTEXTDB_RETENTION_GENERATION_ATOMIC_PATH";
    const CASE: &str = "CONTEXTDB_RETENTION_GENERATION_ATOMIC_CASE";
    if let Ok(path) = std::env::var(CHILD) {
        let _clock = Wallclock::test_clock_guard(|| 1_000_000);
        let db = Database::open(path).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.__arm_retention_reclaim_crash_for_test(std::env::var(CASE).unwrap() == "after");
        db.run_pruning_cycle_checked().unwrap();
        panic!("actual retention crash boundary was not reached");
    }
    for case in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retention.db");
        let mut scalar_lsns = Vec::new();
        {
            let _clock = Wallclock::test_clock_guard(|| 0);
            let db = Database::open(&path).unwrap();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            db.execute("CREATE TABLE vectors (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES, v VECTOR(3) SEARCH_MODE INDEXED) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
            db.execute("CREATE TABLE scalar (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
            for id in 0..3 {
                db.execute(
                    "INSERT INTO vectors VALUES ($id, $expires, '[1,0,0]')",
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("expires".into(), Value::Timestamp(100)),
                    ]),
                )
                .unwrap();
            }
            for id in 0..2 {
                db.execute(
                    "INSERT INTO scalar VALUES ($id, $expires)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("expires".into(), Value::Timestamp(100)),
                    ]),
                )
                .unwrap();
                scalar_lsns.push(db.tx_mgr.current_lsn().0);
            }
            db.run_maintenance_cycle().unwrap();
            db.close().unwrap();
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "database::maintenance_resource_tests::retention_cleanup_commits_generation_catalog_and_eligible_commit_index_removals_together", "--nocapture"])
            .env(CHILD, &path).env(CASE, case).output().unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("RETENTION_RECLAIM_CRASH="),
            "{output:?}"
        );
        {
            let file = redb::Database::open(&path).unwrap();
            let read = file.begin_read().unwrap();
            for name in [
                "vector_partition_base_generations",
                "vector_partition_change_generations",
                "vector_partition_generation_catalog",
                "vector_partition_membership",
            ] {
                let count = read
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                    .map(|table| table.len().unwrap())
                    .unwrap_or(0);
                if case == "after" {
                    assert_eq!(count, 0, "{name} belongs to the retention commit");
                } else if name != "vector_partition_change_generations" {
                    assert!(count > 0, "{name} remains before commit");
                }
            }
            let index = read
                .open_table(TableDefinition::<u64, u64>::new("commit_index"))
                .unwrap();
            for lsn in &scalar_lsns {
                assert_eq!(
                    index.get(*lsn).unwrap().is_some(),
                    case == "before",
                    "eligible scalar commit index follows the same durable commit"
                );
            }
        }
        let _clock = Wallclock::test_clock_guard(|| 1_000_000);
        let db = Database::open(&path).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.run_pruning_cycle_checked().unwrap();
        assert!(
            db.execute("SELECT id FROM vectors", &HashMap::new())
                .unwrap()
                .rows
                .is_empty()
        );
        assert!(
            db.execute("SELECT id FROM scalar", &HashMap::new())
                .unwrap()
                .rows
                .is_empty()
        );
        eprintln!("retention_crash={case} generation_tables=4 scalar_commit_entries=2 retry=ok");
    }
}

fn assert_stopped_maintenance_is_pending(db: &Database, partition: &VectorPartitionRef) {
    let info = db
        .vector_store_for_test()
        .partition_info(&partition.index, &partition.partition_key)
        .unwrap();
    assert_eq!(info.maintenance_failure, None);
    assert!(info.maintenance_progress.is_none());
    assert!(info.maintenance_policy_revision.is_none());
    let shown = db
        .execute("SHOW VECTOR_PARTITIONS FOR items.v", &HashMap::new())
        .unwrap();
    assert_eq!(shown.rows.len(), 1);
    for (column, expected) in [
        ("maintenance_state", "idle"),
        ("recovery_action", "run_maintenance_cycle"),
    ] {
        let position = shown
            .columns
            .iter()
            .position(|name| name == column)
            .unwrap();
        assert_eq!(shown.rows[0][position], Value::Text(expected.into()));
    }
}

#[test]
fn switching_to_caller_driven_during_build_reports_pending_before_retry() {
    for durable in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if durable {
            Database::open(dir.path().join("stop.db")).unwrap()
        } else {
            Database::open_memory()
        };
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
            &HashMap::new(),
        )
        .unwrap();
        for id in 0..16 {
            db.execute(
                "INSERT INTO items VALUES ($id, '[1,0,0]')",
                &HashMap::from([("id".into(), Value::Int64(id))]),
            )
            .unwrap();
        }
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("items", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        let before = db.accountant.usage().used;
        let pause = db.__arm_vector_maintenance_progress_pause_for_test(&partition, 3);
        db.set_maintenance_policy(MaintenancePolicy::EngineOwned);
        let clock = db
            .__hold_engine_owned_vector_maintenance_clock_for_test()
            .unwrap();
        clock.wait_until_held_asleep();
        std::thread::scope(|scope| {
            let wake = scope.spawn(|| clock.wake_once());
            let reached = pause.wait_until_reached(Duration::from_secs(10));
            let during = db.accountant.usage().used;
            let stop = scope.spawn(|| db.set_maintenance_policy(MaintenancePolicy::CallerDriven));
            // The policy setter signals this existing clock before joining the
            // paused worker. Release construction only after that stop is visible.
            {
                let mut state = clock.clock.state.lock();
                while !clock.shutdown.load(Ordering::SeqCst) {
                    clock.clock.changed.wait(&mut state);
                }
            }
            pause.release();
            stop.join().unwrap();
            let _wake_result = wake.join().unwrap();
            assert!(
                reached,
                "durable={durable}: stop during actual graph construction"
            );
            assert!(during > before, "construction owns an admitted reservation");
        });
        assert!(clock.worker_exited());
        assert_eq!(db.maintenance_policy(), MaintenancePolicy::CallerDriven);
        assert!(!db.maintenance_status().running);
        assert_eq!(
            db.accountant.usage().used,
            before,
            "cancelled build returns its reservation"
        );
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
        assert_stopped_maintenance_is_pending(&db, &partition);
        if let Some(persistence) = db.persistence.as_ref() {
            assert!(
                persistence
                    .load_vector_partition_generation_catalog_for(
                        &partition.index,
                        &partition.partition_key
                    )
                    .unwrap()
                    .is_none()
            );
        }
        db.run_maintenance_cycle().unwrap();
        assert_eq!(
            db.execute(
                "SELECT id FROM items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3",
                &HashMap::new()
            )
            .unwrap()
            .rows
            .len(),
            3
        );
    }
}

#[test]
fn last_membership_cleanup_keeps_catalog_targets_with_a_pinned_retired_chain() {
    use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};
    for with_change in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let _clock = Wallclock::test_clock_guard(|| 0);
        let db = Database::open(dir.path().join("retained-catalog.db")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE items (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES, v VECTOR(3) SEARCH_MODE INDEXED) RETAIN 1 HOURS SYNC OFF",
            &HashMap::new(),
        ).unwrap();
        let insert = |id, expires| {
            db.execute(
                "INSERT INTO items VALUES ($id, $expires, '[1,0,0]')",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("expires".into(), Value::Timestamp(expires)),
                ]),
            )
            .unwrap();
        };
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("items", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        let persistence = db.persistence.as_ref().unwrap();
        let catalog = || {
            persistence
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
                .unwrap()
        };
        let sql = "SELECT id FROM items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 8";
        insert(0, 100);
        insert(1, 100);
        db.run_maintenance_cycle().unwrap();
        let first_base = catalog().base.generation_id;
        db.execute("DELETE FROM items", &HashMap::new()).unwrap();
        let snapshot = db.snapshot();
        let pin = db.pin_snapshot(snapshot);
        let old_rows = db
            .execute_at_snapshot(sql, &HashMap::new(), snapshot)
            .unwrap()
            .rows;
        assert!(old_rows.is_empty());
        insert(2, 100);
        insert(3, 100);
        db.run_maintenance_cycle().unwrap();
        assert!(catalog().change.is_some());
        db.execute("UPDATE items SET v = '[0,1,0]'", &HashMap::new())
            .unwrap();
        db.run_maintenance_cycle().unwrap();
        let replacement = catalog();
        assert_ne!(replacement.base.generation_id, first_base);
        assert!(replacement.change.is_none());
        if with_change {
            db.execute("UPDATE items SET v = '[0,0,1]'", &HashMap::new())
                .unwrap();
            db.run_maintenance_cycle().unwrap();
            assert!(catalog().change.is_some());
        }
        let selected = catalog();
        let retained = db
            .vector_store_for_test()
            .snapshot_retained_partition_chain_ids(&partition, &[snapshot]);
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].0, first_base);
        assert_ne!(retained[0].0, selected.base.generation_id);
        db.execute("DELETE FROM items", &HashMap::new()).unwrap();
        let _expired = Wallclock::test_clock_guard(|| 1_000_000);
        assert!(db.run_pruning_cycle_checked().unwrap().pruned_rows > 0);
        let memberships = persistence
            .load_vector_partition_memberships_for(&partition.index, &partition.partition_key)
            .unwrap();
        assert!(
            memberships.is_empty(),
            "cleanup must actually remove the final memberships, got {memberships:?}"
        );
        assert_eq!(catalog(), selected);
        persistence
            .load_vector_partition_base_generation(
                &partition.index,
                &partition.partition_key,
                selected.base.generation_id,
            )
            .expect("every retained catalog still selects readable base data");
        if let Some(change) = selected.change {
            persistence
                .load_vector_partition_change_generation(
                    &partition.index,
                    &partition.partition_key,
                    selected.base.generation_id,
                    change.generation_id,
                )
                .expect("a retained catalog's change data stays readable too");
        }
        persistence
            .load_vector_partition_base_generation(
                &partition.index,
                &partition.partition_key,
                first_base,
            )
            .expect("cleanup preserves the pinned retired chain");
        assert_eq!(
            db.execute_at_snapshot(sql, &HashMap::new(), snapshot)
                .unwrap()
                .rows,
            old_rows
        );
        insert(4, 2_000_000);
        insert(5, 2_000_000);
        db.run_maintenance_cycle()
            .expect("reinsertion maintains the retained catalog successfully");
        let rows = db.execute(sql, &HashMap::new()).unwrap().rows;
        let ids = rows
            .iter()
            .map(|row| match row[0] {
                Value::Int64(id) => id,
                ref other => panic!("expected id: {other:?}"),
            })
            .collect::<HashSet<_>>();
        assert_eq!(ids, HashSet::from([4, 5]));
        assert_eq!(
            db.execute_at_snapshot(sql, &HashMap::new(), snapshot)
                .unwrap()
                .rows,
            old_rows
        );
        drop(pin);
        db.run_maintenance_cycle().unwrap();
        assert_eq!(
            persistence
                .debug_superseded_vector_generation_retention_for_test(&catalog())
                .unwrap()
                .0,
            usize::from(catalog().change.is_some()),
            "only the current base behind a current change may remain after releasing the pin"
        );
        assert!(
            db.vector_store_for_test()
                .retained_partition_chain_ids(&partition)
                .is_empty()
        );
        let _expired_again = Wallclock::test_clock_guard(|| 3_000_000);
        db.run_pruning_cycle_checked().unwrap();
        db.run_maintenance_cycle().unwrap();
        assert!(
            persistence
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key
                )
                .unwrap()
                .is_none(),
            "empty unpinned catalogs are eventually reclaimed"
        );
        persistence
            .with_db(|file| {
                let read = file.begin_read().unwrap();
                for name in [
                    "vector_partition_base_generations",
                    "vector_partition_change_generations",
                    "vector_partition_generation_catalog",
                ] {
                    let table = read
                        .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                        .unwrap();
                    assert_eq!(
                        table.len().unwrap(),
                        0,
                        "unpinned empty {name} is reclaimed"
                    );
                }
                Ok(())
            })
            .unwrap();
        assert!(
            persistence
                .load_vector_partition_memberships_for(&partition.index, &partition.partition_key)
                .unwrap()
                .is_empty()
        );
    }
}
