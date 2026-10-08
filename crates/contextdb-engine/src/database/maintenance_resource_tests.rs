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
    let plan =
        VectorGenerationWorkspacePlan::new(50_000, 50_000, &layout, &partition, policy, 0).unwrap();
    let available = LIMIT - RETAINED;
    assert_eq!(
        (
            policy.hnsw_m,
            policy.hnsw_ef_construction,
            policy.hnsw_ef_search
        ),
        (24, 400, 400)
    );
    let graph_build_bytes =
        HnswIndex::estimated_build_reservation(50_000, 64, VectorQuantization::F32, policy);
    assert_eq!(plan.graph_build_bytes, graph_build_bytes);
    assert_eq!(plan.raw_entry_bytes, 25_900_000);
    assert_eq!(plan.selected_entry_bytes, plan.raw_entry_bytes);
    assert_eq!(plan.durable_metadata_bytes, 4_664);
    assert_eq!(
        plan.build_phase_bytes(),
        graph_build_bytes
            + plan.raw_entry_bytes
            + plan.raw_decode_scratch_bytes
            + plan.selected_entry_bytes
            + plan.durable_metadata_bytes
    );
    let graph_resident_bytes = HnswIndex::estimated_resident_bytes_with_m(
        50_000,
        64,
        VectorQuantization::F32,
        policy.hnsw_m,
    );
    assert_eq!(
        plan.encoding_phase_bytes(graph_resident_bytes),
        graph_resident_bytes
            + plan.raw_entry_bytes
            + plan.encoded_graph_bound
            + plan.encoding_scratch_bytes
            + plan.durable_metadata_bytes
    );
    assert!(plan.build_phase_bytes() <= available);
    assert!(plan.encoding_phase_bytes(graph_resident_bytes) <= available);
    let history =
        VectorGenerationWorkspacePlan::new(100_000, 50_000, &layout, &partition, policy, 0)
            .unwrap();
    assert_eq!(
        history.graph_build_bytes, plan.graph_build_bytes,
        "history never sizes the builder"
    );
    assert_eq!(
        history.encoded_graph_bound, plan.encoded_graph_bound,
        "history never sizes the codec"
    );
    assert_eq!(history.raw_entry_bytes, plan.raw_entry_bytes * 2);
    let delta =
        VectorGenerationWorkspacePlan::new(100_000, 32, &layout, &partition, policy, 12_345)
            .unwrap();
    assert!(delta.graph_build_bytes < history.graph_build_bytes);
    assert_eq!(delta.prior_base_bytes, 12_345);
    assert_eq!(
        delta.build_phase_bytes(),
        delta.graph_build_bytes
            + delta.raw_entry_bytes
            + delta.raw_decode_scratch_bytes
            + delta.selected_entry_bytes
            + delta.durable_metadata_bytes,
        "prior base is not loaded during construction"
    );
    assert_eq!(
        delta.persistence_phase_bytes(4096),
        delta.raw_entry_bytes
            + 4096
            + delta.prior_base_bytes
            + delta.durable_metadata_bytes
            + delta.digest_reference_bytes,
        "candidate preparation does not retain future serialized copies"
    );
    assert_eq!(
        delta.prepared_phase_bytes(4096),
        (4096 + delta.prior_base_bytes + delta.durable_metadata_bytes)
            * VECTOR_GENERATION_PERSISTENCE_COPIES
            + 64 * 1024,
        "prepared owners replace the dead raw snapshot atomically"
    );
}

#[test]
fn a_retained_raw_body_has_decode_scratch_even_without_live_graph_points() {
    let layout = VectorIndexLayout::unpartitioned(262_145, VectorQuantization::F32);
    let policy = layout.resolve_policy(0, 1);
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("retained_items", "embedding"),
        VectorPartitionKey::unpartitioned(),
    );
    let plan = VectorGenerationWorkspacePlan::new(1, 0, &layout, &partition, policy, 0).unwrap();
    assert_eq!(plan.selected_entry_bytes, 0);
    assert_eq!(
        plan.raw_decode_scratch_bytes,
        1024 * 1024
            + 3 * layout.dimension * std::mem::size_of::<f32>()
            + std::mem::size_of::<VectorEntry>()
    );
    let small_layout = VectorIndexLayout::unpartitioned(1, VectorQuantization::F32);
    let small_plan = VectorGenerationWorkspacePlan::new(
        1,
        0,
        &small_layout,
        &partition,
        small_layout.resolve_policy(0, 1),
        0,
    )
    .unwrap();
    assert!(
        small_plan.raw_decode_scratch_bytes > 1024 * 1024,
        "the malformed-prefix allocation cap is additive to declared-body scratch"
    );
    assert_eq!(
        plan.build_phase_bytes(),
        plan.graph_build_bytes
            + plan.raw_entry_bytes
            + plan.raw_decode_scratch_bytes
            + plan.durable_metadata_bytes
    );
    let accountant = MemoryAccountant::with_budget(plan.build_phase_bytes() - 1);
    assert!(
        accountant
            .try_allocate_for(
                plan.build_phase_bytes(),
                "vector_index",
                "prepare_vector_generation",
                "test"
            )
            .is_err()
    );
    assert_eq!(
        accountant.usage().used,
        0,
        "decode cannot start with a partial workspace charge"
    );
}

#[test]
fn streamed_state_digest_preserves_the_canonical_live_entry_encoding() {
    // A complete native build must commit its final publication batch under
    // the existing outer maintenance boundary, then become an indexed route.
    // The registered gate nextest configuration bounds the pre-fix self-wait;
    // this row asserts published state and answers, never elapsed time.
    {
        use contextdb_vector::VectorCopyAdmission;
        use contextdb_vector::hnsw::paged_builder::PagedVectorPhase;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path().join("native-publication.db")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE publication_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 2, EF_CONSTRUCTION = 4))",
            &HashMap::new(),
        ).unwrap();
        for (id, sql) in [
            (1, "INSERT INTO publication_items VALUES ($id, '[1,0,0]')"),
            (2, "INSERT INTO publication_items VALUES ($id, '[0,1,0]')"),
        ] {
            db.execute(sql, &HashMap::from([("id".into(), Value::Int64(id))]))
                .unwrap();
        }
        let route = VectorPartitionRef::new(
            VectorIndexRef::new("publication_items", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        let persistence = db.persistence.as_ref().unwrap().clone();
        vector_paged_build::install_paged_root_capture(&persistence, &db.tx_mgr);
        let mut owner = VectorCopyAdmission::new(db.accountant.clone());
        let context = db.maintenance_context();
        let record = context
            .start_paged_vector_build(&route, &persistence, &mut owner, true)
            .unwrap()
            .unwrap();
        let plan = vector_paged_build::paged_batch_plan(&record.declaration).unwrap();
        for _ in 0..4 {
            if db
                .vector_store
                .partition_paged_build_progress(&route)
                .unwrap()
                .phase
                == PagedVectorPhase::Complete
            {
                break;
            }
            assert!(
                db.vector_store
                    .step_partition_paged_build_with_plan(&route, plan, &mut || Ok(()),)
                    .unwrap()
            );
        }
        let complete = persistence
            .load_paged_build_record(&route)
            .unwrap()
            .unwrap()
            .progress()
            .unwrap();
        assert_eq!(
            (
                complete.phase,
                complete.graph.points,
                complete.encoded_points
            ),
            (
                PagedVectorPhase::Complete,
                complete.total_points,
                complete.total_points
            ),
        );
        let published = context
            .publish_paged_vector_build(
                &route,
                &persistence,
                &record,
                &complete,
                plan,
                db.declared_reader_memory().unwrap(),
            )
            .unwrap();
        assert!(published.published && published.cleanup_failure.is_none());
        let catalog = persistence
            .load_vector_partition_generation_catalog_for(&route.index, &route.partition_key)
            .unwrap()
            .unwrap();
        assert_eq!(
            (catalog.base.generation_id, catalog.change.is_none()),
            (complete.generation_id, true),
        );
        assert!(
            persistence
                .load_paged_build_record(&route)
                .unwrap()
                .is_none()
        );
        assert!(
            db.vector_store
                .partition_paged_build_progress(&route)
                .is_none()
        );
        let shown = db
            .execute(
                "SHOW VECTOR_PARTITIONS FOR publication_items.v",
                &HashMap::new(),
            )
            .unwrap();
        let state = |column| {
            let position = shown
                .columns
                .iter()
                .position(|name| name == column)
                .unwrap();
            shown.rows[0][position].clone()
        };
        assert_eq!(
            (
                shown.rows.len(),
                state("query_state"),
                state("maintenance_state")
            ),
            (1, Value::Text("ready".into()), Value::Text("idle".into())),
        );
        let answer = db.execute(
            "SELECT id FROM publication_items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 2",
            &HashMap::new(),
        ).unwrap();
        assert_eq!(
            answer.rows,
            vec![vec![Value::Int64(1)], vec![Value::Int64(2)]]
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("state-digest.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .unwrap();
    for id in (0..16).rev() {
        db.execute(
            "INSERT INTO items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    let before_updates = SnapshotId(db.tx_mgr.current_tx_max().0);
    db.execute(
        "UPDATE items SET v = '[0,1,0]' WHERE id < 4",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("DELETE FROM items WHERE id >= 14", &HashMap::new())
        .unwrap();
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let vector_fixture_reader_1 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_1 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());
    let vector_fixture_count_1 = db
        .vector_store_for_test()
        .partition_generation_entry_counts(
            &partition.index,
            &partition.partition_key,
            before_updates,
            None,
            &mut vector_fixture_copy_1,
        )
        .expect("count the real selected generation")
        .0;
    let vector_fixture_workspace_1 = db
        .vector_store_for_test()
        .reserve_workspace(
            vector_fixture_copy_1.accountant_for_test().clone(),
            db.vector_store_for_test()
                .generation_copy_bytes_for_test(
                    &partition.index,
                    &partition.partition_key,
                    vector_fixture_count_1,
                )
                .expect("price the retained generation"),
            "fixture_generation_copy",
            "Use the exact selected fixture generation.",
        )
        .expect("admit the actual generation workspace");

    let past_entries = db
        .vector_store_for_test()
        .entries_for_partition_at_snapshot(
            &partition.index,
            &partition.partition_key,
            before_updates,
            &vector_fixture_workspace_1,
        )
        .unwrap();
    assert_eq!(past_entries.len(), 16);
    assert_eq!(
        past_entries.capacity(),
        16,
        "snapshot slots exclude later versions and avoid reallocating old/new arrays"
    );
    drop(past_entries);
    let snapshot = SnapshotId(db.tx_mgr.current_tx_max().0);
    let vector_fixture_reader_2 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_2 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_2.clone());
    let vector_fixture_count_2 = db
        .vector_store_for_test()
        .partition_generation_entry_counts(
            &partition.index,
            &partition.partition_key,
            snapshot,
            None,
            &mut vector_fixture_copy_2,
        )
        .expect("count the real selected generation")
        .0;
    let vector_fixture_workspace_2 = db
        .vector_store_for_test()
        .reserve_workspace(
            vector_fixture_copy_2.accountant_for_test().clone(),
            db.vector_store_for_test()
                .generation_copy_bytes_for_test(
                    &partition.index,
                    &partition.partition_key,
                    vector_fixture_count_2,
                )
                .expect("price the retained generation"),
            "fixture_generation_copy",
            "Use the exact selected fixture generation.",
        )
        .expect("admit the actual generation workspace");

    let entries = db
        .vector_store_for_test()
        .entries_for_partition_at_snapshot(
            &partition.index,
            &partition.partition_key,
            snapshot,
            &vector_fixture_workspace_2,
        )
        .unwrap();
    let mut live = entries
        .iter()
        .filter(|entry| entry.deleted_tx.is_none())
        .collect::<Vec<_>>();
    live.sort_by_key(|entry| (entry.row_id, entry.created_tx, entry.lsn));
    let canonical = bincode::serde::encode_to_vec(&live, bincode::config::standard()).unwrap();
    let expected = *blake3::hash(&canonical).as_bytes();
    let report = db.run_maintenance_cycle().unwrap();
    assert_eq!(report.vector.built_partitions, 1, "{report:?}");
    let persistence = db.persistence.as_ref().unwrap();
    let catalog = persistence
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    let base = persistence
        .load_vector_partition_base_generation(
            &partition.index,
            &partition.partition_key,
            catalog.base.generation_id,
        )
        .unwrap();
    assert_eq!(
        base.state_digest, expected,
        "streaming must preserve durable digest identity"
    );
}

#[test]
fn cancellation_during_actual_build_returns_workspace_and_preserves_serving_catalog() {
    // Cancellation is observed during actual ownership audit before any
    // destructive commit (maintenance intent line 41; root policy 22).
    {
        use contextdb_vector::VectorCopyAdmission;
        use contextdb_vector::hnsw::paged_builder::PagedVectorPhase;
        use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
        let directory = tempfile::tempdir().unwrap();
        let db = Database::open(directory.path().join("audit-cancel.db")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute("CREATE TABLE orphan_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 2, EF_CONSTRUCTION = 4))", &HashMap::new()).unwrap();
        db.execute(
            "INSERT INTO orphan_items VALUES (1, '[1,0,0]'), (2, '[0,1,0]')",
            &HashMap::new(),
        )
        .unwrap();
        let route = VectorPartitionRef::new(
            VectorIndexRef::new("orphan_items", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        let persistence = db.persistence.as_ref().unwrap().clone();
        vector_paged_build::install_paged_root_capture(&persistence, &db.tx_mgr);
        let mut owner = VectorCopyAdmission::new(db.accountant.clone());
        let context = db.maintenance_context();
        let record = context
            .start_paged_vector_build(&route, &persistence, &mut owner, true)
            .unwrap()
            .unwrap();
        let plan = vector_paged_build::paged_batch_plan(&record.declaration).unwrap();
        for _ in 0..4 {
            if db
                .vector_store
                .partition_paged_build_progress(&route)
                .unwrap()
                .phase
                == PagedVectorPhase::Complete
            {
                break;
            }
            db.vector_store
                .step_partition_paged_build_with_plan(&route, plan, &mut || Ok(()))
                .unwrap();
        }
        let complete = persistence
            .load_paged_build_record(&route)
            .unwrap()
            .unwrap()
            .progress()
            .unwrap();
        context
            .publish_paged_vector_build(
                &route,
                &persistence,
                &record,
                &complete,
                plan,
                db.declared_reader_memory().unwrap(),
            )
            .unwrap();

        let pages = TableDefinition::<&[u8], &[u8]>::new("vector_paged_generation_pages");
        let cleanup = TableDefinition::<u64, &[u8]>::new("vector_paged_cleanup");
        db.tx_mgr
            .with_maintenance_publication(|publication| {
                publication.reserve()?;
                persistence.with_db(|storage| {
                    let write = storage.begin_write().unwrap();
                    for definition in [
                        TableDefinition::<&[u8], &[u8]>::new("vector_partition_generation_catalog"),
                        TableDefinition::<&[u8], &[u8]>::new("vector_partition_base_generations"),
                    ] {
                        let mut table = write.open_table(definition).unwrap();
                        let keys = table
                            .iter()
                            .unwrap()
                            .map(|r| r.unwrap().0.value().to_vec())
                            .collect::<Vec<_>>();
                        for key in keys {
                            table.remove(key.as_slice()).unwrap();
                        }
                    }
                    write
                        .open_table(cleanup)
                        .unwrap()
                        .insert(0, b"damaged-fence".as_slice())
                        .unwrap();
                    write.commit().unwrap();
                    Ok(())
                })?;
                publication.complete();
                Ok(())
            })
            .unwrap();
        let durable = || {
            persistence
                .with_db(|storage| {
                    let read = storage.begin_read().unwrap();
                    let p = read.open_table(pages).unwrap();
                    let c = read.open_table(cleanup).unwrap();
                    let page_rows = p
                        .iter()
                        .unwrap()
                        .map(|r| {
                            let (k, v) = r.unwrap();
                            (k.value().to_vec(), v.value().to_vec())
                        })
                        .collect::<Vec<_>>();
                    let cleanup_rows = c
                        .iter()
                        .unwrap()
                        .map(|r| {
                            let (k, v) = r.unwrap();
                            (k.value(), v.value().to_vec())
                        })
                        .collect::<Vec<_>>();
                    Ok((page_rows, cleanup_rows))
                })
                .unwrap()
        };
        let before = durable();
        assert!(!before.0.is_empty() && before.1.iter().any(|(key, _)| *key == 0));
        drop(owner);
        let baseline = db.accountant.usage().used;
        let stop = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let held = Arc::new(AtomicBool::new(false));
        let observer = {
            let stop = stop.clone();
            let calls = calls.clone();
            let held = held.clone();
            let vector = db.vector_store.clone();
            Arc::new(move || {
                // Entry, admitted outer workspace, locked audit entry, audit
                // reader entry, then its first scan-loop cancellation check.
                if calls.fetch_add(1, Ordering::SeqCst) + 1 == 5 {
                    held.store(
                        vector
                            .memory_ownership_snapshot_for_test()
                            .temporary_workspace
                            > 0,
                        Ordering::SeqCst,
                    );
                    stop.store(true, Ordering::SeqCst);
                }
            })
        };
        let mut context = db.maintenance_context();
        context.worker_stop = Some(stop);
        let interrupted = resident_history_test_seams::with_checkpoint_observer(observer, || {
            context.reclaim_paged_vector_storage()
        });
        let observed = (
            matches!(interrupted, Err(Error::ReadCancelled)),
            calls.load(Ordering::SeqCst) >= 5 && held.load(Ordering::SeqCst),
            durable(),
            db.accountant.usage().used,
        );
        assert_eq!(
            observed,
            (true, true, before, baseline),
            "mid-audit interruption must leave every page/fence/queue byte unchanged and release cursor/workspace"
        );
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
    }
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
    let fixture_operation_reader_1 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let worker = std::thread::spawn(move || {
        context.run_cycle(CurrencyGate::Always, fixture_operation_reader_1.clone())
    });
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
    let fixture_operation_reader_2 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let worker = std::thread::spawn(move || {
        context.run_cycle(CurrencyGate::Always, fixture_operation_reader_2.clone())
    });
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
    // Repair survives process exit after retirement and build registration,
    // before any insertion batch (maintenance intent line 41; policy 22).
    {
        use contextdb_vector::hnsw::{HnswBuildObserver, with_hnsw_build_observer};
        use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
        const REPAIR_CHILD: &str = "CONTEXTDB_REPAIR_RETIRED_NATIVE_PATH";
        const RETIRED_EXIT: i32 = 86;
        let route = VectorPartitionRef::new(
            VectorIndexRef::new("t", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        if let Ok(path) = std::env::var(REPAIR_CHILD) {
            let db = Database::open(&path).unwrap();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            db.execute(
                "SELECT id FROM t ORDER BY v <=> [1] USE VECTOR INDEXED LIMIT 1",
                &HashMap::new(),
            )
            .expect_err("the selected saved graph is actually corrupt");
            assert!(db.vector_store.partition_route_quarantine(&route).is_some());
            let baseline = db.accountant.usage().used;
            db.accountant
                .set_budget(Some(baseline + 512 * 1024))
                .unwrap();
            // Implement only the existing observer's required method. It is
            // a fixture adapter, not an additional test or product seam.
            struct RetirementCut {
                persistence: Arc<RedbPersistence>,
                route: VectorPartitionRef,
            }
            impl HnswBuildObserver for RetirementCut {
                fn before_hnsw_build(&self) {
                    assert!(
                        self.persistence
                            .load_vector_partition_generation_catalog_for(
                                &self.route.index,
                                &self.route.partition_key
                            )
                            .unwrap()
                            .is_none(),
                        "the damaged catalog retired before the first native insertion batch"
                    );
                    let record = self
                        .persistence
                        .load_paged_build_record(&self.route)
                        .unwrap()
                        .expect("restart record is durable before the first batch");
                    let progress = record.progress().unwrap();
                    assert_eq!(
                        (
                            progress.graph.points,
                            progress.encoded_points,
                            progress.total_points
                        ),
                        (0, 0, 1)
                    );
                    self.persistence
                        .with_db(|storage| {
                            let read = storage.begin_read().unwrap();
                            let cleanup = read
                                .open_table(TableDefinition::<u64, &[u8]>::new(
                                    "vector_paged_cleanup",
                                ))
                                .unwrap();
                            assert!(cleanup.get(0).unwrap().is_some());
                            Ok(())
                        })
                        .unwrap();
                    // Real child-process exit skips all Database/redb drops.
                    std::process::exit(RETIRED_EXIT);
                }
            }
            let observer = Arc::new(RetirementCut {
                persistence: db.persistence.as_ref().unwrap().clone(),
                route: route.clone(),
            });
            with_hnsw_build_observer(observer, || {
                db.maintenance_context()
                    .persist_built_vector_partition(&route, db.declared_reader_memory().unwrap())
            })
            .expect("a fitting finite repair reaches its existing pre-batch observer");
            panic!("repair did not reach the registered pre-first-batch cut");
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("retired-before-first-batch.db");
        {
            let db = Database::open(&path).unwrap();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            db.execute("CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(1) SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 2, EF_CONSTRUCTION = 2))", &HashMap::new()).unwrap();
            db.execute("INSERT INTO t VALUES (1, '[1]')", &HashMap::new())
                .unwrap();
            for _ in 0..8 {
                db.run_maintenance_cycle().unwrap();
            }
            db.close().unwrap();
        }
        RedbPersistence::corrupt_vector_partition_base_record_for_test(&path, &route).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "database::maintenance_resource_tests::retention_cleanup_commits_generation_catalog_and_eligible_commit_index_removals_together", "--nocapture"])
            .env(REPAIR_CHILD, &path).output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(RETIRED_EXIT),
            "retirement child reaches its exact durable cut: {output:?}"
        );
        let db = Database::open(&path).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let persistence = db.persistence.as_ref().unwrap();
        let record = persistence
            .load_paged_build_record(&route)
            .unwrap()
            .expect("reopen restores the zero-point restart record");
        assert_eq!(record.progress().unwrap().graph.points, 0);
        persistence
            .with_db(|storage| {
                let read = storage.begin_read().unwrap();
                let cleanup = read
                    .open_table(TableDefinition::<u64, &[u8]>::new("vector_paged_cleanup"))
                    .unwrap();
                assert!(cleanup.get(0).unwrap().is_some());
                let parents = read
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(
                        "vector_partition_base_generations",
                    ))
                    .unwrap();
                assert!(
                    parents.len().unwrap() > 0,
                    "damaged parent evidence survives the crash"
                );
                Ok(())
            })
            .unwrap();
        let exact = db
            .execute(
                "SELECT id, v FROM t ORDER BY v <=> [1] USE VECTOR EXACT LIMIT 1",
                &HashMap::new(),
            )
            .unwrap()
            .rows;
        assert_eq!(exact.len(), 1);
        let context = db.maintenance_context();
        for _ in 0..16 {
            context
                .persist_built_vector_partition(&route, db.declared_reader_memory().unwrap())
                .unwrap();
            if persistence
                .load_vector_partition_generation_catalog_for(&route.index, &route.partition_key)
                .unwrap()
                .is_some()
            {
                break;
            }
        }
        assert!(db.vector_store.partition_has_paged_storage(&route));
        assert_eq!(
            db.execute(
                "SELECT id, v FROM t ORDER BY v <=> [1] USE VECTOR INDEXED LIMIT 1",
                &HashMap::new()
            )
            .unwrap()
            .rows,
            exact
        );
    }
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
    let mut partition_observation_owner_1 = contextdb_engine::vector_read_memory::admission(
        db.declared_reader_memory()
            .expect("admit the partition observation reader"),
    );

    let info = db
        .vector_store_for_test()
        .partition_info(
            &partition.index,
            &partition.partition_key,
            &mut partition_observation_owner_1,
        )
        .unwrap()
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
    // Inspection samples committed progress while an actual native write batch
    // owns the maintenance commit gate. The existing insertion callback pauses
    // inside that batch; it does not publish speculative progress.
    {
        use crate::persistence::vector_paged::NativePagedGeneration;
        use contextdb_vector::VectorCopyAdmission;
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(dir.path().join("inspection-batch.db")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute(
            "CREATE TABLE inspection_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 2, EF_CONSTRUCTION = 4))",
            &HashMap::new(),
        ).unwrap();
        for id in 0..33 {
            db.execute(
                "INSERT INTO inspection_items VALUES ($id, '[1,0,0]')",
                &HashMap::from([("id".into(), Value::Int64(id))]),
            )
            .unwrap();
        }
        let route = VectorPartitionRef::new(
            VectorIndexRef::new("inspection_items", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        let persistence = db.persistence.as_ref().unwrap().clone();
        vector_paged_build::install_paged_root_capture(&persistence, &db.tx_mgr);
        let mut owner = VectorCopyAdmission::new(db.accountant.clone());
        let context = db.maintenance_context();
        let record = context
            .start_paged_vector_build(&route, &persistence, &mut owner, true)
            .unwrap()
            .unwrap();
        db.vector_store.forget_partition_paged_build(&route);
        let mut hooks = vector_paged_build::paged_build_hooks(
            &db.vector_store,
            &db.tx_mgr,
            &db.resource_closed,
            &db.publication_interrupted,
            &route,
            &record,
        )
        .unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let callback_armed = armed.clone();
        let (reached, reach_seen) = std::sync::mpsc::sync_channel(1);
        let callback_reached = reached.clone();
        let (release, release_seen) = std::sync::mpsc::sync_channel(1);
        let release_seen = std::sync::Mutex::new(release_seen);
        hooks.inserted = Arc::new(move |_| {
            if callback_armed.swap(false, Ordering::SeqCst) {
                callback_reached.send(true).unwrap();
                release_seen.lock().unwrap().recv().unwrap();
            }
        });
        let native = Arc::new(NativePagedGeneration::new(
            persistence.clone(),
            route.clone(),
            &record,
            hooks,
        ));
        let plan = vector_paged_build::paged_batch_plan(&record.declaration).unwrap();
        db.vector_store
            .register_partition_paged_builder(
                &route,
                record.progress().unwrap(),
                native,
                plan,
                db.accountant.clone(),
                &mut owner,
            )
            .unwrap();
        assert!(
            db.vector_store
                .step_partition_paged_build_with_plan(&route, plan, &mut || Ok(()),)
                .unwrap()
        );
        let before = persistence
            .load_paged_build_record(&route)
            .unwrap()
            .unwrap()
            .progress()
            .unwrap();
        assert!(before.graph.points > 0 && before.graph.points < before.total_points);
        db.vector_store.report_partition_paged_build_progress(
            &route,
            "building",
            record.reason.as_str(),
            before.total_points,
            before.graph.points,
            before.frontier_tx,
        );
        let failure = VectorMaintenanceFailure::BuildFailure;
        db.vector_store
            .record_partition_maintenance_failure(&route, failure)
            .unwrap();
        let inspection_reader = db
            .read_session(contextdb_core::read_contract::ReadLimits::default())
            .unwrap();
        let inspect = || {
            let mut observations = Vec::new();
            for (sql, columns) in [
                (
                    "SHOW VECTOR_INDEXES",
                    &[
                        "maintenance_state",
                        "maintenance_vectors_total",
                        "maintenance_vectors_done",
                        "maintenance_vectors_remaining",
                        "stalled_partitions",
                        "broad_route_reason",
                    ][..],
                ),
                (
                    "SHOW VECTOR_PARTITIONS FOR inspection_items.v",
                    &[
                        "maintenance_state",
                        "maintenance_vectors_total",
                        "maintenance_vectors_done",
                        "maintenance_vectors_remaining",
                        "maintenance_reason",
                        "maintenance_checkpoint_tx",
                        "recovery_action",
                    ][..],
                ),
            ] {
                // Public inspection keeps its ordinary default active limit.
                // Retain a refusal as an observation so the batch is always
                // released before the assertion on the pre-fix tree.
                observations.push(
                    inspection_reader.execute(sql, &HashMap::new())
                        .map_err(|error| format!("{error:?}"))
                        .map(|shown| {
                            if shown.rows.len() != 1 {
                                return Err(format!(
                                    "unexpected inspection rows: {:?}",
                                    shown.rows
                                ));
                            }
                            columns
                                .iter()
                                .map(|column| {
                                    let position = shown
                                        .columns
                                        .iter()
                                        .position(|name| name.as_str() == *column)
                                        .ok_or_else(|| {
                                            format!("missing inspection column {column}")
                                        })?;
                                    Ok(shown.rows[0][position].clone())
                                })
                                .collect::<std::result::Result<Vec<_>, String>>()
                        })
                        .and_then(|row| row),
                );
            }
            observations
        };
        armed.store(true, Ordering::SeqCst);
        let (paused, observed, advanced) = std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    db.vector_store
                        .step_partition_paged_build_with_plan(&route, plan, &mut || Ok(()))
                }));
                let _ = reached.send(false);
                result
            });
            let paused = reach_seen.recv().unwrap();
            let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let gate_held = db
                    .tx_mgr
                    .try_with_commit_lock_for(Duration::ZERO, || ())
                    .is_none();
                let stalled = inspect();
                let clear = db.vector_store.clear_partition_maintenance_failure(&route);
                let building = inspect();
                let restore = db
                    .vector_store
                    .record_partition_maintenance_failure(&route, failure);
                (gate_held, stalled, clear, building, restore)
            }));
            if paused {
                release.send(()).unwrap();
            }
            (paused, observed, worker.join().unwrap())
        });
        let (gate_held, stalled, clear, building, restore) = observed.unwrap();
        assert!(
            paused && gate_held,
            "native batch owns the maintenance commit gate"
        );
        clear.unwrap();
        restore.unwrap();
        assert!(advanced.unwrap().unwrap());
        let after = persistence
            .load_paged_build_record(&route)
            .unwrap()
            .unwrap()
            .progress()
            .unwrap();
        assert!(
            after.graph.points > before.graph.points && after.graph.points < after.total_points
        );
        db.vector_store.report_partition_paged_build_progress(
            &route,
            "building",
            record.reason.as_str(),
            after.total_points,
            after.graph.points,
            after.frontier_tx,
        );
        // This fixture explicitly controls the recorded mark. It does not
        // change the production rule for clearing failures on publication.
        db.vector_store
            .clear_partition_maintenance_failure(&route)
            .unwrap();
        let resumed = inspect();
        let expected = |progress: &contextdb_vector::hnsw::paged_builder::PagedVectorProgress,
                        failed| {
            let state = if failed { "stalled" } else { "building" };
            let common = vec![
                Value::Text(state.into()),
                Value::Int64(progress.total_points as i64),
                Value::Int64(progress.graph.points as i64),
                Value::Int64((progress.total_points - progress.graph.points) as i64),
            ];
            let mut index = common.clone();
            index.extend([
                Value::Int64(i64::from(failed)),
                Value::Text(
                    if failed {
                        failure.reason()
                    } else {
                        record.reason.as_str()
                    }
                    .into(),
                ),
            ]);
            let mut partition = common;
            partition.extend([
                Value::Text(
                    if failed {
                        failure.reason()
                    } else {
                        record.reason.as_str()
                    }
                    .into(),
                ),
                Value::Int64(progress.frontier_tx.0 as i64),
                Value::Text(
                    if failed {
                        failure.recovery_action()
                    } else {
                        "run_maintenance_cycle"
                    }
                    .into(),
                ),
            ]);
            vec![Ok(index), Ok(partition)]
        };
        assert_eq!(
            [stalled, building, resumed],
            [
                expected(&before, true),
                expected(&before, false),
                expected(&after, false)
            ],
            "both inspection statements sample state, failure and committed count without waiting for the native gate",
        );
    }
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
    // A corrupt neighbour cannot stop a fenced route's maintenance or healthy
    // serving (maintenance intent line 41; root policy 22).
    {
        use redb::{ReadableDatabase, ReadableTable, TableDefinition};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("two-damaged-partitions.db");
        let route = |scope| {
            VectorPartitionRef::new(
                VectorIndexRef::new("repair_items", "v"),
                VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
            )
        };
        let a = route(0);
        let b = route(1);
        let old_a;
        {
            let db = Database::open(&path).unwrap();
            db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
            db.execute("CREATE TABLE repair_items (id INT PRIMARY KEY, scope_id INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 2, EF_CONSTRUCTION = 4))", &HashMap::new()).unwrap();
            for scope in 0..3 {
                for ordinal in 0..2 {
                    db.execute(
                        "INSERT INTO repair_items VALUES ($id, $scope, '[1,0,0]')",
                        &HashMap::from([
                            ("id".into(), Value::Int64(scope * 100 + ordinal)),
                            ("scope".into(), Value::Int64(scope)),
                        ]),
                    )
                    .unwrap();
                }
            }
            for _ in 0..8 {
                db.run_maintenance_cycle().unwrap();
            }
            let persistence = db.persistence.as_ref().unwrap();
            old_a = persistence
                .load_vector_partition_generation_catalog_for(&a.index, &a.partition_key)
                .unwrap()
                .unwrap()
                .base
                .generation_id;
            let definitions = (
                TableDefinition::<&[u8], &[u8]>::new("vector_partition_generation_catalog"),
                TableDefinition::<u64, &[u8]>::new("vector_paged_cleanup"),
            );
            let keys = persistence
                .with_db(|storage| {
                    let read = storage.begin_read().unwrap();
                    let table = read.open_table(definitions.0).unwrap();
                    let mut selected = (None, None);
                    for row in table.iter().unwrap() {
                        let (key, value) = row.unwrap();
                        let catalog = persistence
                            .decode_maintenance_vector_catalog(value.value())
                            .unwrap();
                        if catalog.index == a.index && catalog.partition_key == a.partition_key {
                            selected.0 = Some(key.value().to_vec());
                        }
                        if catalog.index == b.index && catalog.partition_key == b.partition_key {
                            selected.1 = Some(key.value().to_vec());
                        }
                    }
                    Ok((selected.0.unwrap(), selected.1.unwrap()))
                })
                .unwrap();
            // Reconstruct the durable retirement cut for A identically on
            // both trees. B retains an independently damaged catalog.
            // The preceding child-process row proves real retirement creates
            // this cut; here only cross-partition scheduling is under test.
            persistence
                .with_db(|storage| {
                    let write = storage.begin_write().unwrap();
                    {
                        let mut table = write.open_table(definitions.0).unwrap();
                        table.remove(keys.0.as_slice()).unwrap();
                        let mut bytes = table
                            .get(keys.1.as_slice())
                            .unwrap()
                            .unwrap()
                            .value()
                            .to_vec();
                        *bytes.last_mut().unwrap() ^= 0xff;
                        table.insert(keys.1.as_slice(), bytes.as_slice()).unwrap();
                    }
                    // Retirement keeps authenticated membership history but
                    // resets its serving frontier in the same transaction.
                    {
                        let definition =
                            TableDefinition::<&[u8], &[u8]>::new("vector_membership_summary_pages");
                        let mut summaries = write.open_table(definition).unwrap();
                        let bytes = summaries
                            .get(keys.0.as_slice())
                            .unwrap()
                            .unwrap()
                            .value()
                            .to_vec();
                        let end = bytes.len() - 32;
                        let mut authenticated = blake3::Hasher::new();
                        authenticated.update(&keys.0);
                        authenticated.update(&bytes[..end]);
                        if !bytes.starts_with(b"CDBVMS01")
                            || authenticated.finalize().as_bytes().as_slice() != &bytes[end..]
                        {
                            return Err(Error::Other(
                                "fixture membership summary authentication failed".into(),
                            ));
                        }
                        // Same field order and codec as the private summary.
                        let (mut summary, consumed): (
                            (
                                u64,
                                u64,
                                TxId,
                                [u8; 32],
                                (TxId, u64, u64),
                                Option<(TxId, u64, u64)>,
                                Option<(TxId, u64, u64)>,
                            ),
                            usize,
                        ) = bincode::serde::decode_from_slice(
                            &bytes[8..end],
                            bincode::config::standard(),
                        )
                        .unwrap();
                        if consumed != end - 8 {
                            return Err(Error::Other(
                                "fixture membership summary trailing bytes".into(),
                            ));
                        }
                        summary.4 = (TxId(0), summary.1, summary.0 - summary.1);
                        summary.5 = None;
                        summary.6 = None;
                        let payload =
                            bincode::serde::encode_to_vec(&summary, bincode::config::standard())
                                .unwrap();
                        let mut encoded = b"CDBVMS01".to_vec();
                        encoded.extend_from_slice(&payload);
                        let mut checksum = blake3::Hasher::new();
                        checksum.update(&keys.0);
                        checksum.update(&encoded);
                        encoded.extend_from_slice(checksum.finalize().as_bytes());
                        summaries
                            .insert(keys.0.as_slice(), encoded.as_slice())
                            .unwrap();
                        drop(summaries);
                        let fingerprint = write
                            .table_root_fingerprint(definition)
                            .unwrap()
                            .unwrap_or([0; 32]);
                        write
                            .open_table(TableDefinition::<&str, &[u8]>::new(
                                "__content_identity_metadata",
                            ))
                            .unwrap()
                            .insert(
                                "vector_membership_summary_pages_root_v1",
                                fingerprint.as_slice(),
                            )
                            .unwrap();
                    }
                    write
                        .open_table(definitions.1)
                        .unwrap()
                        .insert(0, b"damaged-fence".as_slice())
                        .unwrap();
                    write
                        .open_table(TableDefinition::<&str, u64>::new("vector_paged_counters"))
                        .unwrap()
                        .insert("next_storage_id", 2)
                        .unwrap();
                    write.commit().unwrap();
                    Ok(())
                })
                .unwrap();
            db.close().unwrap();
        }
        let db = Database::open(&path).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let persistence = db.persistence.as_ref().unwrap();
        assert!(
            persistence
                .load_vector_partition_generation_catalog_for(&a.index, &a.partition_key)
                .unwrap()
                .is_none()
        );
        assert!(
            persistence
                .load_vector_partition_generation_catalog_for(&b.index, &b.partition_key)
                .is_err()
        );
        let query = "SELECT id FROM repair_items WHERE scope_id = $scope ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 2";
        let healthy_params = HashMap::from([("scope".into(), Value::Int64(2))]);
        let healthy = db.execute(query, &healthy_params).unwrap().rows;
        assert_eq!(healthy.len(), 2);
        let a_params = HashMap::from([("scope".into(), Value::Int64(0))]);
        let exact = db
            .execute(&query.replace("INDEXED", "EXACT"), &a_params)
            .unwrap()
            .rows;
        for _ in 0..32 {
            assert_eq!(db.execute(query, &healthy_params).unwrap().rows, healthy);
            db.run_maintenance_cycle()
                .expect("a corrupt B catalog stays local while A and other routes advance");
            if persistence
                .load_vector_partition_generation_catalog_for(&a.index, &a.partition_key)
                .unwrap()
                .is_some()
            {
                break;
            }
        }
        let replacement = persistence
            .load_vector_partition_generation_catalog_for(&a.index, &a.partition_key)
            .unwrap()
            .expect("A publishes its replacement despite B's present corrupt catalog");
        assert!(replacement.base.generation_id > old_a);
        assert!(db.vector_store.partition_has_paged_storage(&a));
        assert_eq!(db.execute(query, &a_params).unwrap().rows, exact);
        assert_eq!(db.execute(query, &healthy_params).unwrap().rows, healthy);
    }
    use redb::{ReadableDatabase, ReadableTableMetadata, TableDefinition};
    // A whole generation that outgrows the declared reader is replaced even
    // when the ordinary consolidation policy has no work. The default writer
    // is unchanged; all stores here are created on the same current source.
    {
        let reader_directory = tempfile::tempdir().unwrap();
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("reader_items", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        let create = |db: &Database, prepare_whole_seed: bool| {
            db.execute("CREATE TABLE reader_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED CONSOLIDATION (CHANGE_PERCENT = 16, TOMBSTONE_PERCENT = 100)) SYNC OFF", &HashMap::new()).unwrap();
            for id in 0..5_i64 {
                let vector = match id {
                    0 => vec![1.0, 0.0, 0.0],
                    1 => vec![0.9, (1.0_f32 - 0.81).sqrt(), 0.0],
                    _ => vec![-1.0, 0.0, 0.0],
                };
                db.execute(
                    "INSERT INTO reader_items VALUES ($id,$v)",
                    &HashMap::from([
                        ("id".into(), Value::Int64(id)),
                        ("v".into(), Value::Vector(vector)),
                    ]),
                )
                .unwrap();
            }
            if prepare_whole_seed {
                db.vector_store_for_test().quarantine_partition_route(
                    &partition,
                    VectorRouteQuarantineReason::CorruptBase,
                );
            }
            for _ in 0..64 {
                if db
                    .persistence
                    .as_ref()
                    .unwrap()
                    .load_vector_partition_generation_catalog_for(
                        &partition.index,
                        &partition.partition_key,
                    )
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                db.run_maintenance_cycle().unwrap();
            }
        };
        let selected = |db: &Database| {
            db.persistence
                .as_ref()
                .unwrap()
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
                .unwrap()
        };
        let format = |db: &Database, generation| {
            db.persistence
                .as_ref()
                .unwrap()
                .load_vector_partition_base_generation(
                    &partition.index,
                    &partition.partition_key,
                    generation,
                )
                .unwrap()
                .graph_format_version
        };
        let whole_peak = |catalog: &crate::persistence::VectorPartitionGenerationCatalogRecord| {
            let metadata = vector_route_metadata_workspace(&partition)
                .unwrap()
                .max(64 * 1024) as u64;
            let resident = catalog.base.resident_bytes
                + catalog.change.map_or(0, |change| change.resident_bytes);
            let durable = catalog.base.durable_bytes
                + catalog.change.map_or(0, |change| change.durable_bytes);
            resident + 2 * (durable + metadata) + metadata
        };
        let path = reader_directory.path().join("grown-whole-reader.db");
        let seed = Database::open_with_options(
            &path,
            crate::DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        create(&seed, true);
        let base = selected(&seed);
        let whole_format = format(&seed, base.base.generation_id);
        let base_peak = whole_peak(&base);
        seed.execute(
            "INSERT INTO reader_items VALUES (5,'[-1,0,0]')",
            &HashMap::new(),
        )
        .unwrap();
        seed.run_maintenance_cycle().unwrap();
        let combined = selected(&seed);
        assert!(
            combined.change.is_some(),
            "one append seals a change without ordinary consolidation"
        );
        assert_eq!(combined.base.generation_id, base.base.generation_id);
        let mut inventory_owner =
            crate::vector_read_memory::admission(seed.declared_reader_memory().unwrap());
        let layout = seed
            .vector_store_for_test()
            .index_layout(&partition.index, &mut inventory_owner)
            .unwrap();
        let info = seed
            .vector_store_for_test()
            .partition_info(
                &partition.index,
                &partition.partition_key,
                &mut inventory_owner,
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            layout.maintenance_need(&info),
            None,
            "one of six rows does not reach the ordinary consolidation percentage"
        );
        drop(info);
        drop(layout);
        drop(inventory_owner);
        assert!(!seed
            .persistence
            .as_ref()
            .unwrap()
            .vector_partition_has_covered_journal_records(&combined)
            .unwrap());
        assert!(!seed
            .persistence
            .as_ref()
            .unwrap()
            .vector_partition_has_uncovered_journal_records(
                &partition.index,
                &partition.partition_key,
                combined.change.unwrap().covered_lsn
            )
            .unwrap());
        let combined_peak = whole_peak(&combined);
        assert!(combined_peak > base_peak);
        let declared = base_peak + (combined_peak - base_peak) / 2;
        assert!(base_peak <= declared && declared < combined_peak);
        assert!(declared < seed.owner_read_config().limits.limits.memory);
        let exact_sql =
            "SELECT id FROM reader_items ORDER BY v <=> [1,0,0] USE VECTOR EXACT LIMIT 2";
        let indexed_sql =
            "SELECT id FROM reader_items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 2";
        let control = seed.execute(exact_sql, &HashMap::new()).unwrap().rows;
        seed.close().unwrap();
        drop(seed);
        let mut options = crate::DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        };
        options.owner_reads.limits.limits.memory = declared;
        let reader = Database::open_with_options(&path, options).unwrap();
        let before = selected(&reader);
        assert_eq!(
            before, combined,
            "reopen leaves the old catalog selected until maintenance publishes"
        );
        let old_answer = reader.execute(indexed_sql, &HashMap::new()).unwrap().rows;
        for _ in 0..64 {
            reader.run_maintenance_cycle().unwrap();
            if selected(&reader).base.generation_id != before.base.generation_id {
                break;
            }
        }
        let replacement = selected(&reader);
        let replacement_format = format(&reader, replacement.base.generation_id);
        let replacement_answer = reader.execute(indexed_sql, &HashMap::new()).unwrap().rows;
        let replacement_no_change = replacement.change.is_none();
        let replacement_changed = replacement.base.generation_id != before.base.generation_id;
        reader.close().unwrap();
        drop(reader);
        // A fresh build under the same reader bound must choose native pages.
        // Use the saved single-base requirement itself, just below its bound.
        let mut options = crate::DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        };
        options.owner_reads.limits.limits.memory = base_peak - 1;
        let fresh = Database::open_with_options(
            reader_directory.path().join("fresh-reader-pages.db"),
            options,
        )
        .unwrap();
        create(&fresh, false);
        let published = selected(&fresh);
        let published_format = format(&fresh, published.base.generation_id);
        let fresh_control = fresh.execute(exact_sql, &HashMap::new()).unwrap().rows;
        let published_answer = fresh.execute(indexed_sql, &HashMap::new()).unwrap().rows;
        fresh.close().unwrap();
        drop(fresh);
        let paged = crate::persistence::vector_paged::VECTOR_GRAPH_PAGED_FORMAT_VERSION;
        assert_ne!(
            whole_format, paged,
            "the current-tree default seed really publishes whole"
        );
        assert_eq!(
            old_answer, control,
            "the old generation serves while replacement is pending"
        );
        assert_eq!(replacement_answer, control);
        assert_eq!(published_answer, fresh_control);
        assert_eq!(
            (
                published_format,
                replacement_format,
                replacement_no_change,
                replacement_changed
            ),
            (paged, paged, true, true),
            "collect fresh publication and idle reader-only replacement before the format matrix"
        );
    }
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
        db.vector_store_for_test().quarantine_partition_route(
            &partition,
            VectorRouteQuarantineReason::CorruptBase,
        );
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
        if with_change {
            db.vector_store_for_test().quarantine_partition_route(
                &partition,
                VectorRouteQuarantineReason::CorruptBase,
            );
        }
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

fn seed_retained_generation_selection(
    path: &std::path::Path,
    dimension: usize,
    quantization: &str,
    scalar_updates: usize,
) -> Database {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE selection_items (id INT PRIMARY KEY, selected BOOLEAN NOT NULL, \
             v VECTOR({dimension}) WITH (quantization = '{quantization}') SEARCH_MODE INDEXED \
             AUTO_INDEX_AT 1 CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1))"
        ),
        &HashMap::new(),
    )
    .unwrap();
    let mut vector = vec![0.0; dimension];
    vector[0] = 1.0;
    vector[1] = -0.0;
    vector[2] = 0.13;
    for id in (0..16).rev() {
        db.execute(
            "INSERT INTO selection_items VALUES ($id, false, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("vector".into(), Value::Vector(vector.clone())),
            ]),
        )
        .unwrap();
    }
    for _ in 0..scalar_updates {
        db.execute(
            "UPDATE selection_items SET selected = true",
            &HashMap::new(),
        )
        .unwrap();
    }
    db
}

fn selection_partition() -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("selection_items", "v"),
        VectorPartitionKey::unpartitioned(),
    )
}

fn canonical_retained_selection_digest(
    db: &Database,
    partition: &VectorPartitionRef,
    snapshot: SnapshotId,
) -> [u8; 32] {
    let vector_fixture_reader_3 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_3 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_3.clone());
    let vector_fixture_count_3 = db
        .vector_store
        .partition_generation_entry_counts(
            &partition.index,
            &partition.partition_key,
            snapshot,
            None,
            &mut vector_fixture_copy_3,
        )
        .expect("count the real selected generation")
        .0;
    let vector_fixture_workspace_3 = db
        .vector_store
        .reserve_workspace(
            vector_fixture_copy_3.accountant_for_test().clone(),
            db.vector_store
                .generation_copy_bytes_for_test(
                    &partition.index,
                    &partition.partition_key,
                    vector_fixture_count_3,
                )
                .expect("price the retained generation"),
            "fixture_generation_copy",
            "Use the exact selected fixture generation.",
        )
        .expect("admit the actual generation workspace");

    let entries = db
        .vector_store
        .entries_for_partition_at_snapshot(
            &partition.index,
            &partition.partition_key,
            snapshot,
            &vector_fixture_workspace_3,
        )
        .unwrap();
    let mut live = entries
        .iter()
        .filter(|entry| entry.deleted_tx.is_none())
        .collect::<Vec<_>>();
    live.sort_by_key(|entry| (entry.row_id, entry.created_tx, entry.lsn));
    let canonical = bincode::serde::encode_to_vec(&live, bincode::config::standard()).unwrap();
    *blake3::hash(&canonical).as_bytes()
}

#[test]
fn resident_generation_selection_preserves_retained_canonical_digests() {
    for quantization in ["F32", "SQ8", "SQ4"] {
        let directory = tempfile::tempdir().unwrap();
        let db = seed_retained_generation_selection(
            &directory.path().join("selection-digest.db"),
            3,
            quantization,
            8,
        );
        let partition = selection_partition();
        let before_change = SnapshotId(db.tx_mgr.current_tx_max().0);
        db.execute(
            "UPDATE selection_items SET v = '[0,1,0.26]' WHERE id < 4",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "DELETE FROM selection_items WHERE id >= 14",
            &HashMap::new(),
        )
        .unwrap();
        let snapshot = SnapshotId(db.tx_mgr.current_tx_max().0);
        let vector_fixture_reader_4 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_4 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_4.clone());
        let vector_fixture_count_4 = db
            .vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &mut vector_fixture_copy_4,
            )
            .expect("count the real selected generation")
            .0;
        let vector_fixture_workspace_4 = db
            .vector_store
            .reserve_workspace(
                vector_fixture_copy_4.accountant_for_test().clone(),
                db.vector_store
                    .generation_copy_bytes_for_test(
                        &partition.index,
                        &partition.partition_key,
                        vector_fixture_count_4,
                    )
                    .expect("price the retained generation"),
                "fixture_generation_copy",
                "Use the exact selected fixture generation.",
            )
            .expect("admit the actual generation workspace");

        let full = db
            .vector_store
            .entries_for_partition_at_snapshot(
                &partition.index,
                &partition.partition_key,
                snapshot,
                &vector_fixture_workspace_4,
            )
            .unwrap();
        assert_eq!(full.len(), 16 * 9 + 4);
        let vector_fixture_reader_5 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_5 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_5.clone());
        let vector_fixture_count_5 = db
            .vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &mut vector_fixture_copy_5,
            )
            .expect("count the real selected generation")
            .1;
        let vector_fixture_workspace_5 = db
            .vector_store
            .reserve_workspace(
                vector_fixture_copy_5.accountant_for_test().clone(),
                db.vector_store
                    .generation_copy_bytes_for_test(
                        &partition.index,
                        &partition.partition_key,
                        vector_fixture_count_5,
                    )
                    .expect("price the retained generation"),
                "fixture_generation_copy",
                "Use the exact selected fixture generation.",
            )
            .expect("admit the actual generation workspace");

        let selected = db
            .vector_store
            .entries_for_partition_generation_at_snapshot(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &vector_fixture_workspace_5,
            )
            .unwrap();
        assert_eq!(selected.len(), 14);
        assert_eq!(selected.capacity(), 14);
        let expected = canonical_retained_selection_digest(&db, &partition, snapshot);
        let vector_fixture_reader_6 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_6 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_6.clone());
        let vector_fixture_count_6 = db
            .vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                before_change,
                None,
                &mut vector_fixture_copy_6,
            )
            .expect("count the real selected generation")
            .1;
        let vector_fixture_workspace_6 = db
            .vector_store
            .reserve_workspace(
                vector_fixture_copy_6.accountant_for_test().clone(),
                db.vector_store
                    .generation_copy_bytes_for_test(
                        &partition.index,
                        &partition.partition_key,
                        vector_fixture_count_6,
                    )
                    .expect("price the retained generation"),
                "fixture_generation_copy",
                "Use the exact selected fixture generation.",
            )
            .expect("admit the actual generation workspace");

        let old_selected = db
            .vector_store
            .entries_for_partition_generation_at_snapshot(
                &partition.index,
                &partition.partition_key,
                before_change,
                None,
                &vector_fixture_workspace_6,
            )
            .unwrap();
        assert_eq!(old_selected.len(), 16);
        assert!(old_selected.iter().all(|entry| entry.deleted_tx.is_none()));
        drop(full);
        drop(selected);
        drop(old_selected);
        let report = db.run_maintenance_cycle().unwrap();
        assert_eq!(report.vector.first_failure, None, "{report:?}");
        assert_eq!(report.vector.built_partitions, 1, "{report:?}");
        let persistence = db.persistence.as_ref().unwrap();
        let catalog = persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap()
            .unwrap();
        let base = persistence
            .load_vector_partition_base_generation(
                &partition.index,
                &partition.partition_key,
                catalog.base.generation_id,
            )
            .unwrap();
        assert_eq!(base.state_digest, expected, "{quantization}");
        assert_eq!(base.base_tx, TxId(snapshot.0));
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
        assert_eq!(db.accountant.underflow_count_for_test(), 0);
        let vector_fixture_reader_7 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_7 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_7.clone());
        let vector_fixture_count_7 = db
            .vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &mut vector_fixture_copy_7,
            )
            .expect("count the real selected generation")
            .0;
        let vector_fixture_workspace_7 = db
            .vector_store
            .reserve_workspace(
                vector_fixture_copy_7.accountant_for_test().clone(),
                db.vector_store
                    .generation_copy_bytes_for_test(
                        &partition.index,
                        &partition.partition_key,
                        vector_fixture_count_7,
                    )
                    .expect("price the retained generation"),
                "fixture_generation_copy",
                "Use the exact selected fixture generation.",
            )
            .expect("admit the actual generation workspace");

        assert_eq!(
            db.vector_store
                .entries_for_partition_at_snapshot(
                    &partition.index,
                    &partition.partition_key,
                    snapshot,
                    &vector_fixture_workspace_7
                )
                .unwrap()
                .len(),
            16 * 9 + 4
        );
    }
}

#[test]
fn resident_generation_selection_keeps_the_pinned_frontier_during_concurrent_writes() {
    // Maintenance completes within the declared writer budget, and old reads
    // keep their generation (maintenance intent line 41; root policy 22).
    {
        use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
        const HEADROOM: usize = 512 * 1024;
        let directory = tempfile::tempdir().unwrap();
        let db = Database::open(directory.path().join("refused-whole-repair.db")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v VECTOR(1) SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 2, EF_CONSTRUCTION = 2) CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1))", &HashMap::new()).unwrap();
        db.execute("INSERT INTO t VALUES (1, '[1]')", &HashMap::new())
            .unwrap();
        db.execute("CREATE TABLE healthy (id INT PRIMARY KEY, v VECTOR(1) SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 2, EF_CONSTRUCTION = 2))", &HashMap::new()).unwrap();
        db.execute("INSERT INTO healthy VALUES (2, '[1]')", &HashMap::new())
            .unwrap();
        let partition = VectorPartitionRef::new(
            VectorIndexRef::new("t", "v"),
            VectorPartitionKey::unpartitioned(),
        );
        for _ in 0..8 {
            db.run_maintenance_cycle().unwrap();
        }
        let persistence = db.persistence.as_ref().unwrap();
        let catalog = || {
            persistence
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
        };
        let original = catalog().unwrap();
        let old_snapshot = db.snapshot();
        let pin = db.pin_snapshot(old_snapshot);
        let query = "SELECT id, v FROM t ORDER BY v <=> [1] USE VECTOR INDEXED LIMIT 1";
        let query_params = HashMap::new();
        let old_answer = db
            .execute_at_snapshot(query, &query_params, old_snapshot)
            .unwrap()
            .rows;
        for value in [2, 3] {
            db.execute(&format!("UPDATE t SET v = '[{value}]'"), &HashMap::new())
                .unwrap();
            for _ in 0..8 {
                db.run_maintenance_cycle().unwrap();
            }
        }
        let before = catalog().unwrap();
        assert!(before.base.generation_id > original.base.generation_id);
        let exact = db
            .execute(&query.replace("INDEXED", "EXACT"), &query_params)
            .unwrap()
            .rows;
        let healthy_sql = "SELECT id, v FROM healthy ORDER BY v <=> [1] USE VECTOR INDEXED LIMIT 1";
        let healthy = db.execute(healthy_sql, &HashMap::new()).unwrap().rows;
        let catalog_key = persistence
            .with_db(|storage| {
                let read = storage.begin_read().unwrap();
                let table = read
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(
                        "vector_partition_generation_catalog",
                    ))
                    .unwrap();
                let mut selected = None;
                for row in table.iter().unwrap() {
                    let (key, value) = row.unwrap();
                    let record = persistence
                        .decode_maintenance_vector_catalog(value.value())
                        .unwrap();
                    if record.index == partition.index
                        && record.partition_key == partition.partition_key
                    {
                        selected = Some(key.value().to_vec());
                    }
                }
                Ok(selected.unwrap())
            })
            .unwrap();
        db.tx_mgr
            .with_maintenance_publication(|publication| {
                publication.reserve()?;
                persistence.with_db(|storage| {
                    let write = storage.begin_write().unwrap();
                    {
                        let mut table = write
                            .open_table(TableDefinition::<&[u8], &[u8]>::new(
                                "vector_partition_generation_catalog",
                            ))
                            .unwrap();
                        let mut bytes = table
                            .get(catalog_key.as_slice())
                            .unwrap()
                            .unwrap()
                            .value()
                            .to_vec();
                        *bytes.last_mut().unwrap() ^= 0xff;
                        table
                            .insert(catalog_key.as_slice(), bytes.as_slice())
                            .unwrap();
                    }
                    write.commit().unwrap();
                    Ok(())
                })?;
                publication.complete();
                Ok(())
            })
            .unwrap();
        assert!(
            persistence
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key
                )
                .is_err()
        );
        db.vector_store
            .quarantine_partition_route(&partition, VectorRouteQuarantineReason::CorruptBase);
        let operation = db.declared_reader_memory().unwrap();
        let mut copy = crate::vector_read_memory::admission(operation.clone());
        let layout = db
            .vector_store
            .index_layout(&partition.index, &mut copy)
            .unwrap();
        let policy = layout.resolve_policy(1, 1);
        let mut plan =
            VectorGenerationWorkspacePlan::new(1, 1, &layout, &partition, policy, 0).unwrap();
        plan.assign_raw_decode_owner(true, false);
        let declaration = crate::persistence::vector_paged::PagedDeclaration {
            dimension: layout.dimension.try_into().unwrap(),
            quantization: layout.quantization,
            hnsw_m: policy.hnsw_m.try_into().unwrap(),
            hnsw_ef_construction: policy.hnsw_ef_construction.try_into().unwrap(),
            hnsw_ef_search: policy.hnsw_ef_search.try_into().unwrap(),
            policy_revision: policy.policy_revision,
        };
        let finite = vector_paged_build::paged_batch_plan(&declaration)
            .unwrap()
            .construction_workspace_bytes()
            .unwrap();
        let inventory = (partition.partition_key.estimated_bytes()
            + partition.index.table.len()
            + partition.index.column.len()
            + 1024)
            * 64;
        let cleanup_peak =
            inventory * 8 + db.snapshot_registry.state.lock().active.len() * 256 + 64 * 1024;
        let repair_peak = plan
            .build_phase_bytes()
            .max(plan.encoding_phase_bytes(plan.graph_build_bytes))
            .max(plan.persistence_phase_bytes(plan.encoded_graph_bound))
            .max(plan.prepared_phase_bytes(plan.encoded_graph_bound))
            .max(plan.prepared_phase_bytes(plan.encoded_graph_bound) + cleanup_peak);
        assert!(
            finite < HEADROOM && repair_peak > HEADROOM,
            "the finite builder fits while full whole-repair publication and cleanup do not: finite={finite}, repair={repair_peak}"
        );
        drop(layout);
        drop(copy);
        drop(operation);
        let baseline = db.accountant.usage().used;
        db.accountant.set_budget(Some(baseline + HEADROOM)).unwrap();
        let context = db.maintenance_context();
        for _ in 0..64 {
            assert_eq!(
                db.execute(healthy_sql, &HashMap::new()).unwrap().rows,
                healthy
            );
            context.persist_built_vector_partition(&partition, db.declared_reader_memory().unwrap())
                .expect("an over-budget corrupt whole replacement progresses through the finite paged builder");
            if let Some(current) = catalog() {
                if current.base.generation_id > before.base.generation_id {
                    break;
                }
            }
        }
        let repaired = catalog().expect("bounded repair publishes a complete generation");
        assert!(repaired.base.generation_id > before.base.generation_id);
        assert!(db.vector_store.partition_has_paged_storage(&partition));
        assert_eq!(db.execute(query, &query_params).unwrap().rows, exact);
        assert_eq!(
            db.execute_at_snapshot(query, &query_params, old_snapshot)
                .unwrap()
                .rows,
            old_answer
        );
        assert_eq!(
            db.execute(healthy_sql, &HashMap::new()).unwrap().rows,
            healthy
        );
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
        drop(pin);
    }
    for quantization in ["F32", "SQ8", "SQ4"] {
        let directory = tempfile::tempdir().unwrap();
        let db = Arc::new(seed_retained_generation_selection(
            &directory.path().join("selection-frontier.db"),
            3,
            quantization,
            4,
        ));
        let partition = selection_partition();
        let snapshot = SnapshotId(db.tx_mgr.current_tx_max().0);
        let covered_lsn = db.tx_mgr.current_lsn();
        let expected = canonical_retained_selection_digest(&db, &partition, snapshot);
        let vector_fixture_reader_8 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_8 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_8.clone());

        let counts = db
            .vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &mut vector_fixture_copy_8,
            )
            .unwrap();
        assert_eq!(counts, (16 * 5, 16));
        let pause = db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(
            &partition,
            VectorMaintenancePreparationPhaseForTest::LoadAuthoritativeRawVectors,
        );
        let maintenance_db = Arc::clone(&db);
        let worker = std::thread::spawn(move || maintenance_db.run_maintenance_cycle());
        let reached = pause.wait_until_reached_timeout(Duration::from_secs(10));
        if reached.is_err() {
            pause.release();
            let result = worker.join().unwrap();
            panic!("generation did not reach extraction pause: {reached:?}; {result:?}");
        }
        assert!(db.__try_global_commit_lock_for_test());
        let writer_db = Arc::clone(&db);
        let (writer_tx, writer_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let result = (|| {
                writer_db.execute(
                    "UPDATE selection_items SET v = '[0,1,0]' WHERE id = 0",
                    &HashMap::new(),
                )?;
                writer_db.execute("DELETE FROM selection_items WHERE id = 1", &HashMap::new())?;
                writer_db.execute(
                    "INSERT INTO selection_items VALUES (16, true, '[0,0,1]')",
                    &HashMap::new(),
                )?;
                Ok::<(), Error>(())
            })();
            writer_tx.send(result).unwrap();
        });
        let writes_before_release = writer_rx.recv_timeout(Duration::from_secs(10));
        if writes_before_release.is_err() {
            pause.release();
            let result = worker.join().unwrap();
            writer.join().unwrap();
            panic!(
                "commits did not finish before extraction: {writes_before_release:?}; {result:?}"
            );
        }
        writes_before_release.unwrap().unwrap();
        writer.join().unwrap();
        let vector_fixture_reader_9 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_9 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_9.clone());

        assert_eq!(
            db.vector_store
                .partition_generation_entry_counts(
                    &partition.index,
                    &partition.partition_key,
                    snapshot,
                    None,
                    &mut vector_fixture_copy_9
                )
                .unwrap(),
            counts
        );
        let vector_fixture_reader_10 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_10 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_10.clone());
        let vector_fixture_count_10 = db
            .vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &mut vector_fixture_copy_10,
            )
            .expect("count the real selected generation")
            .1;
        let vector_fixture_workspace_10 = db
            .vector_store
            .reserve_workspace(
                vector_fixture_copy_10.accountant_for_test().clone(),
                db.vector_store
                    .generation_copy_bytes_for_test(
                        &partition.index,
                        &partition.partition_key,
                        vector_fixture_count_10,
                    )
                    .expect("price the retained generation"),
                "fixture_generation_copy",
                "Use the exact selected fixture generation.",
            )
            .expect("admit the actual generation workspace");

        let old = db
            .vector_store
            .entries_for_partition_generation_at_snapshot(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &vector_fixture_workspace_10,
            )
            .unwrap();
        assert_eq!(old.len(), 16);
        assert_eq!(old.capacity(), 16);
        assert!(old.iter().all(|entry| entry.deleted_tx.is_none()));
        drop(old);
        pause.release();
        let report = worker.join().unwrap().unwrap();
        assert_eq!(report.vector.first_failure, None, "{report:?}");
        assert_eq!(report.vector.built_partitions, 1, "{report:?}");
        let persistence = db.persistence.as_ref().unwrap();
        let catalog = persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap()
            .unwrap();
        let base = persistence
            .load_vector_partition_base_generation(
                &partition.index,
                &partition.partition_key,
                catalog.base.generation_id,
            )
            .unwrap();
        assert_eq!(base.base_tx, TxId(snapshot.0));
        assert_eq!(base.base_lsn, covered_lsn);
        assert_eq!(base.state_digest, expected, "{quantization}");
        assert_eq!(
            canonical_retained_selection_digest(&db, &partition, snapshot),
            expected
        );
        let now = SnapshotId(db.tx_mgr.current_tx_max().0);
        let vector_fixture_reader_11 = db
            .declared_reader_memory()
            .expect("admit the vector fixture reader");
        let mut vector_fixture_copy_11 =
            contextdb_engine::vector_read_memory::admission(vector_fixture_reader_11.clone());
        let vector_fixture_count_11 = db
            .vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                now,
                Some(covered_lsn),
                &mut vector_fixture_copy_11,
            )
            .expect("count the real selected generation")
            .0;
        let vector_fixture_workspace_11 = db
            .vector_store
            .reserve_workspace(
                vector_fixture_copy_11.accountant_for_test().clone(),
                db.vector_store
                    .generation_copy_bytes_for_test(
                        &partition.index,
                        &partition.partition_key,
                        vector_fixture_count_11,
                    )
                    .expect("price the retained generation"),
                "fixture_generation_copy",
                "Use the exact selected fixture generation.",
            )
            .expect("admit the actual generation workspace");

        let current = db
            .vector_store
            .entries_for_partition_generation_at_snapshot(
                &partition.index,
                &partition.partition_key,
                now,
                Some(covered_lsn),
                &vector_fixture_workspace_11,
            )
            .unwrap();
        assert_eq!(
            current.len(),
            2,
            "later update and insert remain in the suffix"
        );
        assert_eq!(
            db.execute(
                "SELECT id FROM selection_items ORDER BY v <=> [0,1,0] USE VECTOR INDEXED LIMIT 1",
                &HashMap::new(),
            )
            .unwrap()
            .rows,
            vec![vec![Value::Int64(0)]]
        );
        drop(current);
        drop(vector_fixture_workspace_11);
        drop(vector_fixture_workspace_10);
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
        assert_eq!(db.accountant.underflow_count_for_test(), 0);
    }
}

#[test]
fn resident_generation_selection_admits_history_below_the_full_copy_bound_and_refuses_before_bodies()
 {
    const HEADROOM: usize = 2 * 1024 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let db = seed_retained_generation_selection(
        &directory.path().join("selection-admission.db"),
        768,
        "F32",
        64,
    );
    let partition = selection_partition();
    let snapshot = SnapshotId(db.tx_mgr.current_tx_max().0);
    let vector_fixture_reader_12 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_12 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_12.clone());

    let (raw_count, graph_count) = db
        .vector_store
        .partition_generation_entry_counts(
            &partition.index,
            &partition.partition_key,
            snapshot,
            None,
            &mut vector_fixture_copy_12,
        )
        .unwrap();
    assert_eq!((raw_count, graph_count), (16 * 65, 16));
    let vector_fixture_reader_13 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_13 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_13.clone());
    let vector_fixture_count_13 = db
        .vector_store
        .partition_generation_entry_counts(
            &partition.index,
            &partition.partition_key,
            snapshot,
            None,
            &mut vector_fixture_copy_13,
        )
        .expect("count the real selected generation")
        .0;
    let vector_fixture_workspace_13 = db
        .vector_store
        .reserve_workspace(
            vector_fixture_copy_13.accountant_for_test().clone(),
            db.vector_store
                .generation_copy_bytes_for_test(
                    &partition.index,
                    &partition.partition_key,
                    vector_fixture_count_13,
                )
                .expect("price the retained generation"),
            "fixture_generation_copy",
            "Use the exact selected fixture generation.",
        )
        .expect("admit the actual generation workspace");

    let full_entries = db
        .vector_store
        .entries_for_partition_at_snapshot(
            &partition.index,
            &partition.partition_key,
            snapshot,
            &vector_fixture_workspace_13,
        )
        .unwrap();
    let vector_fixture_reader_14 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_14 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_14.clone());
    let vector_fixture_count_14 = db
        .vector_store
        .partition_generation_entry_counts(
            &partition.index,
            &partition.partition_key,
            snapshot,
            None,
            &mut vector_fixture_copy_14,
        )
        .expect("count the real selected generation")
        .1;
    let vector_fixture_workspace_14 = db
        .vector_store
        .reserve_workspace(
            vector_fixture_copy_14.accountant_for_test().clone(),
            db.vector_store
                .generation_copy_bytes_for_test(
                    &partition.index,
                    &partition.partition_key,
                    vector_fixture_count_14,
                )
                .expect("price the retained generation"),
            "fixture_generation_copy",
            "Use the exact selected fixture generation.",
        )
        .expect("admit the actual generation workspace");

    let selected_entries = db
        .vector_store
        .entries_for_partition_generation_at_snapshot(
            &partition.index,
            &partition.partition_key,
            snapshot,
            None,
            &vector_fixture_workspace_14,
        )
        .unwrap();
    let body_bytes = |entries: &[VectorEntry]| -> usize {
        entries
            .iter()
            .map(|entry| entry.vector.capacity() * 4)
            .sum()
    };
    assert_eq!(full_entries.capacity(), 1040);
    assert_eq!(selected_entries.capacity(), 16);
    assert_eq!(body_bytes(&full_entries), 1040 * 768 * 4);
    assert_eq!(body_bytes(&selected_entries), 16 * 768 * 4);
    assert_eq!(
        body_bytes(&full_entries) - body_bytes(&selected_entries),
        3_145_728
    );
    assert_eq!(
        (full_entries.capacity() - selected_entries.capacity())
            * std::mem::size_of::<VectorEntry>(),
        1024 * std::mem::size_of::<VectorEntry>()
    );
    drop(full_entries);
    drop(selected_entries);
    let vector_fixture_reader_15 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_15 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_15.clone());

    let layout = db
        .vector_store
        .index_layout(&partition.index, &mut vector_fixture_copy_15)
        .unwrap();
    let policy = layout.resolve_policy(graph_count, 1);
    let mut full =
        VectorGenerationWorkspacePlan::new(raw_count, graph_count, &layout, &partition, policy, 0)
            .unwrap();
    let mut selected = VectorGenerationWorkspacePlan::new(
        graph_count,
        graph_count,
        &layout,
        &partition,
        policy,
        0,
    )
    .unwrap();
    full.assign_raw_decode_owner(true, false);
    selected.assign_raw_decode_owner(true, false);
    assert!(full.build_phase_bytes() > HEADROOM);
    assert!(selected.build_phase_bytes() < HEADROOM);
    assert_eq!(
        full.raw_entry_bytes - selected.raw_entry_bytes,
        (raw_count - graph_count)
            * (std::mem::size_of::<VectorEntry>()
                + 768 * 4
                + partition.index.table.len()
                + partition.index.column.len()
                + 128)
    );
    let baseline = db.accountant.usage().used;
    let snapshot_holds = db.snapshot_registry.state.lock().active.len();
    // Whole-build refusal must yield to a finite paged build when it fits.
    // Refuse one byte below that finite construction floor, before bodies.
    let declaration = crate::persistence::vector_paged::PagedDeclaration {
        dimension: layout.dimension.try_into().unwrap(),
        quantization: layout.quantization,
        hnsw_m: policy.hnsw_m.try_into().unwrap(),
        hnsw_ef_construction: policy.hnsw_ef_construction.try_into().unwrap(),
        hnsw_ef_search: policy.hnsw_ef_search.try_into().unwrap(),
        policy_revision: policy.policy_revision,
    };
    let finite_floor = vector_paged_build::paged_batch_plan(&declaration)
        .unwrap()
        .construction_workspace_bytes()
        .unwrap();
    assert_eq!(
        finite_floor, 208_224,
        "the F32/768/M16/EF200 paged construction floor"
    );
    assert!(finite_floor < selected.build_phase_bytes());
    db.accountant
        .set_budget(Some(baseline + finite_floor - 1))
        .unwrap();
    let refuse = || {
        db.maintenance_context().persist_built_vector_partition(
            &partition,
            db.declared_reader_memory()
                .expect("admit the partition fixture reader"),
        )
    };
    #[cfg(feature = "test-seams")]
    let result = {
        let ((result, physical), visits) =
            VectorStore::observe_generation_source_visits_for_test(|| {
                crate::writer_query_test_seams::observe(refuse)
            });
        assert_eq!(
            visits.entry_bodies_copied, 0,
            "refusal precedes resident vector-entry body copies"
        );
        assert!(
            !physical.physical_sources.iter().flatten().any(
                |(family, _)| *family == crate::writer_query_test_seams::PhysicalFamily::Vector
            ),
            "refusal precedes durable vector-body source calls: {physical:?}"
        );
        result
    };
    #[cfg(not(feature = "test-seams"))]
    let result = refuse();
    let error = result.unwrap_err();
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes, available_bytes, ..
    } if subsystem == "vector_index" && operation == "prepare_vector_generation"
        && *requested_bytes == finite_floor && *available_bytes == finite_floor - 1),
        "finite construction floor={finite_floor}; refusal={error:?}"
    );
    assert_eq!(db.accountant.usage().used, baseline);
    assert_eq!(
        db.snapshot_registry.state.lock().active.len(),
        snapshot_holds
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap()
            .is_none()
    );
    let vector_fixture_reader_16 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_16 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_16.clone());

    assert_eq!(
        db.vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &mut vector_fixture_copy_16
            )
            .unwrap(),
        (raw_count, graph_count)
    );
    db.accountant.set_budget(Some(baseline + HEADROOM)).unwrap();
    let report = db.run_maintenance_cycle().unwrap();
    assert_eq!(report.vector.first_failure, None, "{report:?}");
    assert_eq!(report.vector.built_partitions, 1, "{report:?}");
    assert!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap()
            .is_some()
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    let vector_fixture_reader_17 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_17 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_17.clone());

    assert_eq!(
        db.vector_store
            .partition_generation_entry_counts(
                &partition.index,
                &partition.partition_key,
                snapshot,
                None,
                &mut vector_fixture_copy_17
            )
            .unwrap(),
        (raw_count, graph_count)
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    eprintln!(
        "resident_selection full_build_bytes={} selected_build_bytes={} raw_snapshot_savings={} fixed_headroom={HEADROOM}",
        full.build_phase_bytes(),
        selected.build_phase_bytes(),
        full.raw_entry_bytes - selected.raw_entry_bytes
    );
}
