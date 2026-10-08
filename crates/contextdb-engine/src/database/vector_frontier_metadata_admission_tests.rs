use super::*;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::sync::atomic::AtomicUsize;

const CATALOGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_generation_catalog");
const MEMBERSHIPS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_membership");
const JOURNAL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_partition_journal");
const MALFORMED_METADATA_BYTES: usize = 9 * 1024 * 1024;

#[derive(Clone, Copy)]
enum ColdMetadata {
    Catalog,
    Membership,
    Journal,
}

fn cold_oversized_metadata_refuses_before_decode(metadata: ColdMetadata) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata-owner.db");
    let copy = directory.path().join("metadata-cold.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE metadata_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO metadata_items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("metadata_items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let serving = db
        .vector_store
        .partition_graph_generation_status(&partition)
        .unwrap();
    let original_catalog = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    // A separate snapshot retains the original serving authority. Reopen its
    // metadata with no cache so the physical extent is deterministically cold.
    db.export_snapshot(&copy).unwrap();
    let persistence = RedbPersistence::open_with_cache(&copy, 0).unwrap();
    let (catalog_key, catalog_bytes, membership_key) = db
        .persistence
        .as_ref()
        .unwrap()
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            let catalogs = read.open_table(CATALOGS).unwrap();
            let (key, value) = catalogs.iter().unwrap().next().unwrap().unwrap();
            let memberships = read.open_table(MEMBERSHIPS).unwrap();
            let (membership_key, _) = memberships.iter().unwrap().next().unwrap().unwrap();
            Ok((
                key.value().to_vec(),
                value.value().to_vec(),
                membership_key.value().to_vec(),
            ))
        })
        .unwrap();
    let mut journal_key = catalog_key.clone();
    journal_key.extend_from_slice(&1u64.to_be_bytes());
    let (definition, malformed_key) = match metadata {
        ColdMetadata::Catalog => (CATALOGS, catalog_key.as_slice()),
        ColdMetadata::Membership => (MEMBERSHIPS, membership_key.as_slice()),
        ColdMetadata::Journal => (JOURNAL, journal_key.as_slice()),
    };
    persistence
        .with_db(|destination| {
            let write = destination.begin_write().unwrap();
            {
                let mut catalogs = write.open_table(CATALOGS).unwrap();
                catalogs
                    .insert(catalog_key.as_slice(), catalog_bytes.as_slice())
                    .unwrap();
            }
            // Invalid bytes have no bounded logical descriptor to price. The
            // physical owner must be refused before metadata validation sees it.
            let malformed = vec![0xff; MALFORMED_METADATA_BYTES];
            write
                .open_table(definition)
                .unwrap()
                .insert(malformed_key, malformed.as_slice())
                .unwrap();
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    persistence.close();
    drop(persistence);
    let persistence = Arc::new(RedbPersistence::open_with_cache(&copy, 0).unwrap());
    persistence.attach_memory_accountant(Arc::clone(&db.accountant));
    let mut context = db.maintenance_context();
    context.persistence = Some(Arc::clone(&persistence));
    let baseline = db.accountant.usage().used;
    let snapshot_holds = db.snapshot_registry.state.lock().active.len();
    let headroom = 512 * 1024;
    let limit = baseline + headroom;
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    db.accountant.set_budget(Some(limit)).unwrap();
    let fixture_operation_reader_1 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");
    let error = match metadata {
        ColdMetadata::Membership => {
            // Exercise the durable fallback frontier loader with the same
            // preparation guard used by persist_built_vector_partition.
            vector_source_page_admission::with_preparation_source_page_admission(
                Arc::clone(&db.accountant),
                || {
                    persistence.vector_partition_maintenance_entry_counts(
                        &partition,
                        db.tx_mgr.current_tx_max(),
                        None,
                    )
                },
            )
            .unwrap_err()
        }
        ColdMetadata::Catalog | ColdMetadata::Journal => context
            .persist_built_vector_partition(&partition, fixture_operation_reader_1.clone())
            .unwrap_err(),
    };
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded {
            subsystem, operation, requested_bytes, available_bytes, budget_limit_bytes, hint,
        } if subsystem == "vector_maintenance"
            && operation == "prepare_vector_generation"
            && *requested_bytes > MALFORMED_METADATA_BYTES
            && *available_bytes <= headroom
            && *budget_limit_bytes == limit
            && hint == "Raise MEMORY_LIMIT before inspecting this maintained route."),
        "{error:?}"
    );
    assert!(largest_read.load(Ordering::SeqCst) < MALFORMED_METADATA_BYTES);
    drop(observation);
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
    assert_eq!(
        db.vector_store
            .partition_graph_generation_status(&partition),
        Some(serving)
    );
    assert!(
        db.vector_store
            .partition_route_quarantine(&partition)
            .is_none()
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    db.accountant.set_budget(None).unwrap();
    // With adequate source admission the unchanged malformed bytes reach the
    // bounded decoder and remain corruption, rather than a hidden short count.
    let decode = vector_source_page_admission::with_preparation_source_page_admission(
        Arc::clone(&db.accountant),
        || match metadata {
            ColdMetadata::Catalog => persistence
                .load_vector_partition_generation_descriptors_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .map(|_| ()),
            ColdMetadata::Membership => persistence
                .vector_partition_maintenance_entry_counts(
                    &partition,
                    db.tx_mgr.current_tx_max(),
                    None,
                )
                .map(|_| ()),
            ColdMetadata::Journal => persistence
                .vector_partition_uncovered_tombstone_entry_count(
                    &partition,
                    original_catalog.base.covered_lsn,
                    db.tx_mgr.current_lsn(),
                    |_| Ok(()),
                )
                .map(|_| ()),
        },
    );
    assert!(matches!(decode, Err(Error::StoreCorrupted { .. })));
    assert_eq!(db.accountant.usage().used, baseline);
    assert_eq!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap(),
        Some(original_catalog)
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM metadata_items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3",
            &HashMap::new(),
        )
        .unwrap()
        .rows
        .len(),
        3
    );
    drop(context);
    persistence.close();
    drop(persistence);
    db.close().unwrap();
}

#[test]
fn cold_oversized_catalog_extent_refuses_during_preparation_before_decode() {
    cold_oversized_metadata_refuses_before_decode(ColdMetadata::Catalog);
}

#[test]
fn cold_oversized_journal_extent_refuses_during_frontier_count_before_decode() {
    cold_oversized_metadata_refuses_before_decode(ColdMetadata::Journal);
}

#[test]
fn cold_oversized_membership_extent_refuses_before_fallback_count_decode() {
    cold_oversized_metadata_refuses_before_decode(ColdMetadata::Membership);
}

#[test]
fn cold_oversized_covered_journal_retry_probe_refuses_before_read_and_keeps_serving() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("covered-retry-owner.db");
    let copy = directory.path().join("covered-retry-cold.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE covered_retry_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO covered_retry_items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("covered_retry_items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let serving = db
        .vector_store
        .partition_graph_generation_status(&partition)
        .unwrap();
    let catalog = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    assert!(catalog.base.covered_lsn.0 >= 1);
    let (catalog_key, catalog_bytes) = db
        .persistence
        .as_ref()
        .unwrap()
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            let table = read.open_table(CATALOGS).unwrap();
            let (key, value) = table.iter().unwrap().next().unwrap().unwrap();
            Ok((key.value().to_vec(), value.value().to_vec()))
        })
        .unwrap();
    let mut journal_key = catalog_key.clone();
    journal_key.extend_from_slice(&1u64.to_be_bytes());
    journal_key.extend_from_slice(&0u64.to_be_bytes());

    // This installed route takes the identity-only retry probe. Its first
    // physical journal identity is covered, while invalid oversized bytes
    // make any accidental cleanup decode distinguishable from admission.
    db.export_snapshot(&copy).unwrap();
    let persistence = RedbPersistence::open_with_cache(&copy, 0).unwrap();
    persistence
        .with_db(|destination| {
            let write = destination.begin_write().unwrap();
            write
                .open_table(CATALOGS)
                .unwrap()
                .insert(catalog_key.as_slice(), catalog_bytes.as_slice())
                .unwrap();
            let malformed = vec![0xff; MALFORMED_METADATA_BYTES];
            write
                .open_table(JOURNAL)
                .unwrap()
                .insert(journal_key.as_slice(), malformed.as_slice())
                .unwrap();
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    persistence.close();
    drop(persistence);
    // No cached or write-buffer page can mask the physical source extent.
    let persistence = Arc::new(RedbPersistence::open_with_cache(&copy, 0).unwrap());
    persistence.attach_memory_accountant(Arc::clone(&db.accountant));
    let mut context = db.maintenance_context();
    context.persistence = Some(Arc::clone(&persistence));
    let baseline = db.accountant.usage().used;
    let snapshot_holds = db.snapshot_registry.state.lock().active.len();
    let headroom = 512 * 1024;
    let limit = baseline + headroom;
    db.accountant.set_budget(Some(limit)).unwrap();
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    let fixture_operation_reader_2 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let outcome = context
        .retry_vector_journal_truncations(fixture_operation_reader_2.clone())
        .unwrap();
    let details = outcome
        .first_failure_details
        .expect("a refused physical retry probe retains an actionable partition failure");
    assert_eq!(
        outcome.failed_partitions,
        HashSet::from([partition.clone()])
    );
    drop(outcome);
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    assert_eq!(details.operation, "prepare_vector_generation");
    assert!(details.requested_bytes.unwrap() > MALFORMED_METADATA_BYTES as u64);
    assert!(details.available_bytes.unwrap() <= headroom as u64);
    assert_eq!(details.budget_limit_bytes, Some(limit as u64));
    assert!(details.message.contains("graph is durable"));
    let vector_fixture_reader_1 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_1 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

    assert_eq!(
        db.vector_store
            .partition_info(
                &partition.index,
                &partition.partition_key,
                &mut vector_fixture_copy_1
            )
            .unwrap()
            .unwrap()
            .maintenance_failure_details,
        Some(details)
    );
    assert!(largest_read.load(Ordering::SeqCst) < MALFORMED_METADATA_BYTES);
    drop(observation);
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
    assert_eq!(
        db.vector_store
            .partition_graph_generation_status(&partition),
        Some(serving)
    );
    assert!(
        db.vector_store
            .partition_route_quarantine(&partition)
            .is_none()
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);

    db.accountant.set_budget(None).unwrap();
    assert_eq!(
        persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap(),
        Some(catalog.clone())
    );
    assert!(
        vector_source_page_admission::with_preparation_source_page_admission(
            Arc::clone(&db.accountant),
            || persistence.vector_partition_has_covered_journal_records(&catalog),
        )
        .unwrap()
    );
    let decode = vector_source_page_admission::with_preparation_source_page_admission(
        Arc::clone(&db.accountant),
        || {
            persistence.vector_partition_uncovered_tombstone_entry_count(
                &partition,
                Lsn(0),
                catalog.base.covered_lsn,
                |_| Ok(()),
            )
        },
    );
    assert!(matches!(decode, Err(Error::StoreCorrupted { .. })));
    assert_eq!(db.accountant.usage().used, baseline);
    assert_eq!(
        db.execute(
            "SELECT id FROM covered_retry_items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3",
            &HashMap::new(),
        )
        .unwrap()
        .rows
        .len(),
        3
    );
    assert_eq!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap(),
        Some(catalog)
    );
    drop(context);
    persistence.close();
    drop(persistence);
    db.close().unwrap();
    let accountant = Arc::clone(&db.accountant);
    drop(db);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
}

#[test]
fn wide_text_partition_metadata_refuses_before_cold_catalog_lookup() {
    const CATALOGS: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new("vector_partition_generation_catalog");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("frontier-owner.db");
    let copy = directory.path().join("frontier-cold.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE frontier_items (id INT PRIMARY KEY, scope TEXT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .unwrap();
    let scope = "wide-frontier-route".repeat(1024);
    for id in 0..16 {
        db.execute(
            "INSERT INTO frontier_items VALUES ($id, $scope, '[1,0,0]')",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("scope".into(), Value::Text(scope.clone())),
            ]),
        )
        .unwrap();
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("frontier_items", "v"),
        VectorPartitionKey::from_values(&[Value::Text(scope)]).unwrap(),
    );
    let serving = db
        .vector_store
        .partition_graph_generation_status(&partition)
        .unwrap();
    let original_catalog = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();

    // A logical copy has independent writer ownership. Copy the catalog too,
    // then reopen without a cache so descriptor lookup has observable I/O.
    db.export_snapshot(&copy).unwrap();
    let persistence = RedbPersistence::open_with_cache(&copy, 0).unwrap();
    db.persistence
        .as_ref()
        .unwrap()
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            let source_table = read.open_table(CATALOGS).unwrap();
            persistence.with_db(|destination| {
                let write = destination.begin_write().unwrap();
                {
                    let mut destination_table = write.open_table(CATALOGS).unwrap();
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
    let persistence = Arc::new(RedbPersistence::open_with_cache(&copy, 0).unwrap());
    persistence.attach_memory_accountant(Arc::clone(&db.accountant));
    let mut context = db.maintenance_context();
    context.persistence = Some(Arc::clone(&persistence));

    let baseline = db.accountant.usage().used;
    let headroom = 4096;
    let metadata_bytes = partition
        .partition_key
        .estimated_bytes()
        .saturating_add(partition.index.table.len())
        .saturating_add(partition.index.column.len())
        .saturating_add(1024)
        .saturating_mul(64);
    assert!(metadata_bytes > headroom);
    let backend_reads = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&backend_reads);
    let observation = redb::observe_backend_reads(Arc::new(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
    }));
    db.accountant.set_budget(Some(baseline + headroom)).unwrap();
    let fixture_operation_reader_3 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let error = context
        .persist_built_vector_partition(&partition, fixture_operation_reader_3.clone())
        .unwrap_err();
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded {
            operation, requested_bytes, available_bytes, ..
        } if operation == "prepare_vector_generation"
            && *requested_bytes == metadata_bytes
            && *available_bytes == headroom),
        "{error:?}"
    );
    assert_eq!(backend_reads.load(Ordering::SeqCst), 0);
    drop(observation);
    assert_eq!(db.accountant.usage().used, baseline);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(
        db.vector_store
            .partition_graph_generation_status(&partition),
        Some(serving)
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    db.accountant.set_budget(None).unwrap();
    assert_eq!(
        persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap(),
        Some(original_catalog)
    );
    drop(context);
    persistence.close();
    drop(persistence);
    db.close().unwrap();
}
