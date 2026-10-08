use super::*;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::sync::atomic::AtomicUsize;

const BASES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_base_generations");
const CATALOGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_generation_catalog");
const GRAPH_BYTES: usize = 9 * 1024 * 1024;

fn quarantined_inventory_refuses_cold_oversized_source(missing_catalog: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("inventory-owner.db");
    let copy = directory.path().join("inventory-cold.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute(
        "CREATE TABLE inventory_items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO inventory_items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("inventory_items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let serving = db
        .vector_store
        .partition_graph_generation_status(&partition)
        .unwrap();
    // The logical copy has its own writer identity. Copy rebuildable vector
    // records into it, then reopen with no cache so the inventory source is
    // deterministically cold without evicting or timing any production cache.
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
                {
                    let source_table = read.open_table(BASES).unwrap();
                    let (key, value) = source_table.iter().unwrap().next().unwrap().unwrap();
                    let mut stale_key = key.value().to_vec();
                    let generation_offset = stale_key.len() - 8;
                    stale_key[generation_offset..].copy_from_slice(&0u64.to_be_bytes());
                    let encoded = value.value();
                    let header_len = b"contextdb-vector-partition".len() + 2 + 1 + 8;
                    let (mut stale, _): (VectorPartitionBaseGenerationRecord, usize) =
                        bincode::serde::decode_from_slice(
                            &encoded[header_len..encoded.len() - 32],
                            bincode::config::standard(),
                        )
                        .unwrap();
                    stale.generation_id = 0;
                    stale.graph = VectorPartitionGraphBytes::verified(vec![7; GRAPH_BYTES]);
                    let payload =
                        bincode::serde::encode_to_vec(stale, bincode::config::standard()).unwrap();
                    let mut envelope = encoded[..header_len].to_vec();
                    envelope[header_len - 8..]
                        .copy_from_slice(&(payload.len() as u64).to_be_bytes());
                    envelope.extend_from_slice(&payload);
                    let checksum = blake3::hash(&envelope);
                    envelope.extend_from_slice(checksum.as_bytes());
                    write
                        .open_table(BASES)
                        .unwrap()
                        .insert(stale_key.as_slice(), envelope.as_slice())
                        .unwrap();
                }
                {
                    let mut table = write.open_table(CATALOGS).unwrap();
                    let (catalog_key, value) = table.iter().unwrap().next().unwrap().unwrap();
                    let key = catalog_key.value().to_vec();
                    let mut encoded = value.value().to_vec();
                    drop(value);
                    drop(catalog_key);
                    if missing_catalog {
                        table.remove(key.as_slice()).unwrap();
                    } else {
                        *encoded.last_mut().unwrap() ^= 0xff;
                        table.insert(key.as_slice(), encoded.as_slice()).unwrap();
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
    assert!(
        db.vector_store
            .quarantine_partition_route(&partition, VectorRouteQuarantineReason::CorruptBase)
    );
    let mut context = db.maintenance_context();
    context.persistence = Some(Arc::clone(&persistence));
    let baseline = db.accountant.usage().used;
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));

    db.accountant.set_budget(Some(baseline + 4096)).unwrap();
    let fixture_operation_reader_1 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let early = context
        .persist_built_vector_partition(&partition, fixture_operation_reader_1.clone())
        .unwrap_err();
    assert!(
        matches!(early, Error::MemoryBudgetExceeded { operation, .. }
        if operation == "prepare_vector_generation")
    );
    assert_eq!(largest_read.load(Ordering::SeqCst), 0);
    assert_eq!(db.accountant.usage().used, baseline);
    let vector_fixture_reader_1 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_1 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

    // Construction fits. The retained generation's borrowed payload owns a
    // separate oversized extent and must refuse before its backend read.
    let layout = db
        .vector_store
        .index_layout(&partition.index, &mut vector_fixture_copy_1)
        .unwrap();
    let plan = VectorGenerationWorkspacePlan::new(
        16,
        16,
        &layout,
        &partition,
        layout.resolve_policy(16, 1),
        0,
    )
    .unwrap();
    let headroom = plan.build_phase_bytes() + 128 * 1024;
    assert!(headroom < GRAPH_BYTES);
    let limit = baseline + headroom;
    db.accountant.set_budget(Some(limit)).unwrap();
    let fixture_operation_reader_2 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let error = context
        .persist_built_vector_partition(&partition, fixture_operation_reader_2.clone())
        .unwrap_err();
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes, available_bytes, budget_limit_bytes, ..
    } if subsystem == "vector_maintenance"
        && operation == "read_vector_generation_source_page"
        && *requested_bytes > GRAPH_BYTES
        && *available_bytes < GRAPH_BYTES
        && *budget_limit_bytes == limit),
        "{error:?}"
    );
    assert!(largest_read.load(Ordering::SeqCst) < GRAPH_BYTES);
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
    assert!(
        db.vector_store
            .partition_route_quarantine(&partition)
            .is_some()
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    db.accountant.set_budget(None).unwrap();
    let catalog = persistence
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key);
    if missing_catalog {
        assert!(catalog.unwrap().is_none());
    } else {
        assert!(matches!(catalog, Err(Error::StoreCorrupted { .. })));
    }
    drop(context);
    drop(persistence);
    db.close().unwrap();
}

#[test]
fn missing_catalog_inventory_admits_cold_oversized_source_after_construction() {
    quarantined_inventory_refuses_cold_oversized_source(true);
}

#[test]
fn corrupt_catalog_inventory_admits_cold_oversized_source_after_construction() {
    quarantined_inventory_refuses_cold_oversized_source(false);
}
