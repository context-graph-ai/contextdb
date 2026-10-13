use super::*;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

#[test]
fn published_retry_admits_cold_sources_and_keeps_cleanup_pending_on_refusal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("retry-owner.db");
    let copy = directory.path().join("retry-cold.db");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE retry_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO retry_items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("retry_items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let old = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    for id in 16..20 {
        db.execute(
            "INSERT INTO retry_items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    let crash = db.__arm_vector_journal_truncation_crash_for_test(
        &partition,
        VectorJournalTruncationPhaseForTest::CatalogDurableBeforeCoveredJournalDelete,
    );
    let published = db.run_maintenance_cycle().unwrap();
    assert_eq!(published.vector.built_partitions, 1);
    assert!(published.vector.first_failure.is_some());
    drop(crash);
    let catalog = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    assert_ne!(catalog, old);
    let query = "SELECT id FROM retry_items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3";
    let old_rows = db.execute(query, &HashMap::new()).unwrap().rows;
    // Give the task-owned snapshot its own writer identity, then carry the
    // exact published generation and pending journal bytes into it. The
    // logical export deliberately omits these rebuildable generation tables.
    db.export_snapshot(&copy).unwrap();
    let persistence = Arc::new(RedbPersistence::open_with_cache(&copy, 0).unwrap());
    db.persistence
        .as_ref()
        .unwrap()
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            persistence.with_db(|destination| {
                let write = destination.begin_write().unwrap();
                for name in [
                    "vector_partition_base_generations",
                    "vector_partition_change_generations",
                    "vector_partition_graph_pages",
                    "vector_partition_generation_catalog",
                    "vector_partition_journal",
                    "vector_paged_generation_pages",
                ] {
                    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name);
                    write.delete_table(definition).unwrap();
                    let Ok(source_table) = read.open_table(definition) else {
                        continue;
                    };
                    let mut destination_table = write.open_table(definition).unwrap();
                    for entry in source_table.iter().unwrap() {
                        let (key, value) = entry.unwrap();
                        destination_table
                            .insert(key.value(), value.value())
                            .unwrap();
                    }
                }
                write.commit().unwrap();
                Ok(())
            })
        })
        .unwrap();
    persistence.close();
    drop(persistence);
    // Reopen after the copy so no source extent comes from a write buffer.
    let persistence = Arc::new(RedbPersistence::open_with_cache(&copy, 0).unwrap());
    persistence.attach_memory_accountant(db.accountant.clone());
    let pending = persistence
        .debug_vector_partition_journal_for_test(&partition)
        .unwrap();
    assert!(!pending.1.is_empty());
    // A producer's current generation owns both native pages and a captured root.
    let source_root = persistence
        .initialize_writer_snapshot(db.snapshot(), db.current_lsn())
        .unwrap();
    let retained_root = Arc::clone(&source_root);
    persistence.set_paged_root_capture(
        Arc::new(move || Ok(Arc::clone(&retained_root))),
        Arc::new(|| Ok(None)),
    );
    drop(source_root);
    let mut context = db.maintenance_context();
    context.persistence = Some(Arc::clone(&persistence));
    let selected = catalog.change.unwrap_or(catalog.base).generation_id;
    let before = context
        .vector
        .partition_graph_generation_status(&partition)
        .unwrap();
    assert!(
        ![
            before.base,
            before.change,
            before.dormant_base,
            before.dormant_change
        ]
        .into_iter()
        .flatten()
        .any(|generation| generation.generation_id == selected)
    );
    let accountant = db.accountant.clone();
    let baseline = accountant.usage().used;
    let limit = baseline + 1024;
    accountant.set_budget(Some(limit)).unwrap();
    let backend_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&backend_reads);
    let observation = redb::observe_backend_reads(Arc::new(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
    }));
    let fixture_operation_reader_1 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let error = context
        .retry_one_vector_journal_truncation(
            &persistence,
            &catalog,
            &partition,
            true,
            fixture_operation_reader_1.clone(),
        )
        .unwrap_err();
    drop(observation);
    assert_eq!(backend_reads.load(Ordering::SeqCst), 0);
    assert!(matches!(&error, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes, available_bytes, budget_limit_bytes, ..
    } if subsystem == "vector_maintenance"
        && operation == "read_vector_generation_source_page"
        && *requested_bytes > *available_bytes
        && *available_bytes <= 1024
        && *budget_limit_bytes == limit));
    let details = VectorMaintenanceFailureDetails::from_published_cleanup_error(&error);
    assert_eq!(details.operation, "read_vector_generation_source_page");
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    assert_eq!(accountant.usage().used, baseline);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    accountant.set_budget(None).unwrap();
    assert_eq!(
        persistence
            .debug_vector_partition_journal_for_test(&partition)
            .unwrap(),
        pending
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
    assert_eq!(db.execute(query, &HashMap::new()).unwrap().rows, old_rows);
    let fixture_operation_reader_2 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    context
        .retry_one_vector_journal_truncation(
            &persistence,
            &catalog,
            &partition,
            true,
            fixture_operation_reader_2.clone(),
        )
        .unwrap();
    let installed = context
        .vector
        .partition_graph_generation_status(&partition)
        .unwrap();
    assert!(
        [installed.base, installed.change]
            .into_iter()
            .flatten()
            .any(|generation| generation.generation_id == selected)
    );
    assert!(
        persistence
            .debug_vector_partition_journal_for_test(&partition)
            .unwrap()
            .1
            .is_empty()
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
    assert_eq!(db.execute(query, &HashMap::new()).unwrap().rows.len(), 3);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
    drop(context);
    drop(persistence);
    db.close().unwrap();
    drop(db);
    assert_eq!(
        accountant.usage().used,
        0,
        "final retry owner releases every credit"
    );
    assert_eq!(accountant.underflow_count_for_test(), 0);
}
