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
    Journal,
    CoveredJournal,
}

fn public_cold_metadata_refusal(metadata: ColdMetadata) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata-owner.db");
    let copy = directory.path().join("metadata-cold.db");
    let mut db = Database::open_with_options(
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
    let (catalog_key, catalog_bytes, _membership_key) = db
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
    let journal_lsn = if matches!(metadata, ColdMetadata::CoveredJournal) {
        original_catalog.base.covered_lsn.0
    } else {
        original_catalog.base.covered_lsn.0 + 1
    };
    journal_key.extend_from_slice(&journal_lsn.to_be_bytes());
    journal_key.extend_from_slice(&0u64.to_be_bytes());
    let (definition, malformed_key) = match metadata {
        ColdMetadata::Catalog => (CATALOGS, catalog_key.as_slice()),
        ColdMetadata::CoveredJournal => (JOURNAL, journal_key.as_slice()),
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
    let original_persistence = db.persistence.replace(Arc::clone(&persistence)).unwrap();
    let baseline = db.accountant.usage().used;
    let snapshot_holds = db.snapshot_registry.state.lock().active.len();
    let headroom = 512 * 1024;
    let limit = baseline + headroom;
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    db.clear_maintenance_cycle_stamp_for_test();
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    db.accountant.set_budget(Some(limit)).unwrap();
    let report = db.run_maintenance_cycle().unwrap();
    let details = report
        .vector
        .first_failure_details
        .expect("the public cycle exposes the deferred physical owner");
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    assert_eq!(details.operation, "prepare_vector_generation");
    assert!(details.requested_bytes.unwrap() > MALFORMED_METADATA_BYTES as u64);
    assert!(details.available_bytes.unwrap() <= headroom as u64);
    assert_eq!(details.budget_limit_bytes, Some(limit as u64));
    assert_eq!(report.vector.built_partitions, 0);
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
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
    persistence
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            let table = read.open_table(definition).unwrap();
            let value = table.get(malformed_key).unwrap().unwrap();
            assert_eq!(value.value().len(), MALFORMED_METADATA_BYTES);
            assert!(
                value.value().iter().all(|byte| *byte == 0xff),
                "a resource refusal does not erase or rewrite unknown metadata"
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(
        original_persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key
            )
            .unwrap(),
        Some(original_catalog)
    );
    assert_eq!(
        db.execute(
            "SELECT id FROM metadata_items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3",
            &HashMap::new()
        )
        .unwrap()
        .rows
        .len(),
        3
    );
    db.close().unwrap();
}

#[test]
fn public_cycle_cold_catalog_refusal_keeps_serving_and_closes() {
    public_cold_metadata_refusal(ColdMetadata::Catalog);
}
#[test]
fn public_cycle_cold_journal_refusal_keeps_serving_and_closes() {
    public_cold_metadata_refusal(ColdMetadata::Journal);
}
#[test]
fn public_cycle_cold_covered_cleanup_refusal_keeps_serving_and_closes() {
    public_cold_metadata_refusal(ColdMetadata::CoveredJournal);
}

#[derive(Clone, Copy)]
enum UnknownCatalog {
    Giant,
    Invalid,
    Miskey,
    PossibleMatch,
    DurableOnly,
    Incomplete,
}

fn public_unknown_catalog_preserves_healthy_work(before: bool, kind: UnknownCatalog) {
    use contextdb_core::VectorPartitionComponent;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner.db");
    let copy = directory.path().join("cold.db");
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 3 SEARCH_MODE INDEXED HNSW (M = 4, EF_CONSTRUCTION = 64))", &HashMap::new()).unwrap();
    for scope in 0..2 {
        for row in 0..16 {
            db.execute(
                "INSERT INTO items VALUES ($id, $scope, '[1,0,0]')",
                &HashMap::from([
                    ("id".into(), Value::Int64(scope * 16 + row)),
                    ("scope".into(), Value::Int64(scope)),
                ]),
            )
            .unwrap();
        }
        if scope == 0 {
            assert_eq!(
                db.run_maintenance_cycle().unwrap().vector.built_partitions,
                1
            );
        }
    }
    let route = |scope| {
        VectorPartitionRef::new(
            VectorIndexRef::new("items", "v"),
            VectorPartitionKey::from_components(vec![VectorPartitionComponent::Integer(scope)]),
        )
    };
    let serving = db
        .vector_store
        .partition_graph_generation_status(&route(0))
        .unwrap();
    if matches!(kind, UnknownCatalog::DurableOnly) {
        db.execute(
            "UPDATE items SET v = '[0,1,0]' WHERE id = 0",
            &HashMap::new(),
        )
        .unwrap();
    }
    if matches!(kind, UnknownCatalog::Incomplete) {
        db.execute(
            "CREATE TABLE cold_catalog_pressure (id INT PRIMARY KEY, body TEXT) SYNC OFF",
            &HashMap::new(),
        )
        .unwrap();
        for id in 0..128 {
            db.execute(
                "INSERT INTO cold_catalog_pressure VALUES ($id, $body)",
                &HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    (
                        "body".into(),
                        Value::Text(format!("{id}:{}", "p".repeat(64 * 1024))),
                    ),
                ]),
            )
            .unwrap();
        }
    }
    db.export_snapshot(&copy).unwrap();
    let persistence = RedbPersistence::open_with_cache(&copy, 0).unwrap();
    let definitions = [
        CATALOGS,
        JOURNAL,
        TableDefinition::new("vector_partition_base_generations"),
        TableDefinition::new("vector_partition_change_generations"),
        TableDefinition::new("vector_partition_graph_pages"),
        MEMBERSHIPS,
        TableDefinition::new("vector_entries"),
        TableDefinition::new("vector_open_directory"),
        TableDefinition::new("vector_membership_summary_pages"),
        TableDefinition::new("vector_membership_source_pages"),
        TableDefinition::new("vector_membership_reverse_pages"),
    ];
    let relations = db
        .persistence
        .as_ref()
        .unwrap()
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            Ok(definitions
                .into_iter()
                .map(|definition| {
                    let table = read.open_table(definition).unwrap();
                    let records = table
                        .iter()
                        .unwrap()
                        .map(|record| {
                            let (key, value) = record.unwrap();
                            (key.value().to_vec(), value.value().to_vec())
                        })
                        .collect::<Vec<_>>();
                    (definition, records)
                })
                .collect::<Vec<_>>())
        })
        .unwrap();
    let catalog_bytes = relations[0].1[0].1.clone();
    persistence
        .with_db(|destination| {
            let write = destination.begin_write().unwrap();
            for (definition, records) in &relations {
                let mut table = write.open_table(*definition).unwrap();
                // Replace every coupled relation in one publication, including
                // lifecycle keys introduced only by the snapshot exporter.
                let previous = table
                    .iter()
                    .unwrap()
                    .map(|record| record.unwrap().0.value().to_vec())
                    .collect::<Vec<_>>();
                for key in previous {
                    table.remove(key.as_slice()).unwrap();
                }
                for (key, value) in records {
                    table.insert(key.as_slice(), value.as_slice()).unwrap();
                }
            }
            for (definition, key) in [
                (
                    TableDefinition::<&[u8], &[u8]>::new("vector_membership_source_pages"),
                    "vector_membership_source_pages_root_v1",
                ),
                (
                    TableDefinition::<&[u8], &[u8]>::new("vector_membership_reverse_pages"),
                    "vector_membership_reverse_pages_root_v1",
                ),
                (
                    TableDefinition::<&[u8], &[u8]>::new("vector_membership_summary_pages"),
                    "vector_membership_summary_pages_root_v1",
                ),
            ] {
                let fingerprint = write
                    .table_root_fingerprint(definition)
                    .unwrap()
                    .unwrap_or([0; 32]);
                write
                    .open_table(TableDefinition::<&str, &[u8]>::new(
                        "__content_identity_metadata",
                    ))
                    .unwrap()
                    .insert(key, fingerprint.as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    drop(relations);
    let unknown_key: &[u8] = if before { b"" } else { b"\xff" };
    let unknown_bytes = match kind {
        UnknownCatalog::Giant | UnknownCatalog::DurableOnly => vec![0xff; MALFORMED_METADATA_BYTES],
        UnknownCatalog::Invalid | UnknownCatalog::Incomplete => vec![0xff; 32],
        UnknownCatalog::Miskey => catalog_bytes,
        UnknownCatalog::PossibleMatch => {
            let mut catalog = original_catalog_for_route(&db, &route(0));
            catalog.partition_key = route(1).partition_key;
            let mut bytes = RedbPersistence::encode_maintenance_catalog_for_test(&catalog);
            bytes.resize(MALFORMED_METADATA_BYTES, 0xff);
            bytes
        }
    };
    // Miskey tests introduce corruption after restoring the healthy serving
    // state; reopening an already miskeyed store can quarantine it first.
    if !matches!(kind, UnknownCatalog::Miskey) {
        persistence
            .with_db(|destination| {
                let write = destination.begin_write().unwrap();
                write
                    .open_table(CATALOGS)
                    .unwrap()
                    .insert(unknown_key, unknown_bytes.as_slice())
                    .unwrap();
                write.commit().unwrap();
                Ok(())
            })
            .unwrap();
    }
    persistence.close();
    drop(persistence);
    // Reopen one writer so its publisher, catalog and all lazy sources own
    // the same prepared store. The declared share is smaller than the malformed
    // extent, preserving a cold physical admission on the observed cycle.
    db.close().unwrap();
    drop(db);
    let accountant = Arc::new(MemoryAccountant::no_limit());
    accountant.set_budget(Some(64 * 1024 * 1024)).unwrap();
    let db = Database::open_with_config(&copy, Arc::new(CorePlugin), accountant).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    if !matches!(kind, UnknownCatalog::DurableOnly) {
        assert!(
            db.vector_store
                .preload_dormant_partition_generation(&route(0), true)
                .unwrap(),
            "the serving base is resident before the admission cycle"
        );
        assert_eq!(
            db.vector_store.partition_graph_generation_status(&route(0)),
            Some(serving),
            "normal reopen and preload restore the original serving premise"
        );
    }
    assert!(
        crate::persistence::redb_writer_cache_bytes_for_limit(Some(64 * 1024 * 1024))
            < MALFORMED_METADATA_BYTES
    );
    let persistence = Arc::clone(db.persistence.as_ref().unwrap());
    if matches!(kind, UnknownCatalog::Miskey) {
        persistence
            .with_db(|destination| {
                let write = destination.begin_write().unwrap();
                write
                    .open_table(CATALOGS)
                    .unwrap()
                    .insert(unknown_key, unknown_bytes.as_slice())
                    .unwrap();
                write.commit().unwrap();
                Ok(())
            })
            .unwrap();
    }
    if matches!(kind, UnknownCatalog::Incomplete) {
        let before = db.__writer_cache_stats_for_test().unwrap();
        let declared_share =
            crate::persistence::redb_writer_cache_bytes_for_limit(Some(64 * 1024 * 1024));
        assert!(128 * 64 * 1024 > declared_share);
        for id in 0..128 {
            assert_eq!(
                db.execute(
                    &format!("SELECT body FROM cold_catalog_pressure WHERE id = {id}"),
                    &HashMap::new()
                )
                .unwrap()
                .rows,
                vec![vec![Value::Text(format!("{id}:{}", "p".repeat(64 * 1024)))]]
            );
        }
        let after = db.__writer_cache_stats_for_test().unwrap();
        assert!(
            after.1 > before.1 && after.2 > before.2,
            "the real engine path displaces catalog pages before the incomplete inventory: {before:?} -> {after:?}"
        );
    }
    let headroom = if matches!(kind, UnknownCatalog::Incomplete) {
        4096
    } else {
        2 * 1024 * 1024
    };
    db.accountant
        .set_budget(Some(db.accountant.usage().used + headroom))
        .unwrap();
    if matches!(kind, UnknownCatalog::Incomplete) {
        let inventory = db
            .maintenance_context()
            .maintenance_catalog_inventory()
            .unwrap();
        assert!(!inventory.complete);
        assert_eq!(
            inventory.failures.first.unwrap().failure,
            VectorMaintenanceFailure::MemoryLimit
        );
    }
    if matches!(kind, UnknownCatalog::DurableOnly) {
        db.vector_store
            .replace_with_dormant_raw_partitions(Vec::new())
            .unwrap();
        assert!(
            db.vector_store
                .partition_info(&route(0).index, &route(0).partition_key)
                .is_none()
        );
        assert_eq!(
            vector_source_page_admission::with_preparation_source_page_admission(
                Arc::clone(&db.accountant),
                || persistence.vector_partition_maintenance_entry_counts(
                    &route(0),
                    db.tx_mgr.current_tx_max(),
                    None,
                ),
            )
            .unwrap(),
            (17, 16),
            "the cold frontier retains history and selects all live points"
        );
        let candidates = db
            .maintenance_context()
            .maintenance_build_candidates()
            .unwrap();
        assert!(
            candidates
                .candidates
                .iter()
                .any(|candidate| candidate.partition == route(0))
        );
        assert_eq!(
            candidates.failures.first.unwrap().failure,
            VectorMaintenanceFailure::MemoryLimit
        );
    }
    if matches!(kind, UnknownCatalog::Miskey) {
        let inventory = db
            .maintenance_context()
            .maintenance_catalog_inventory()
            .unwrap();
        assert!(inventory.complete);
        assert!(
            inventory
                .catalogs
                .iter()
                .all(|entry| entry.catalog.partition_key != route(0).partition_key)
        );
        assert!(
            db.vector_store
                .partition_route_quarantine(&route(0))
                .is_some()
        );
    }
    let largest = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest);
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    db.clear_maintenance_cycle_stamp_for_test();
    let report = db.run_maintenance_cycle().unwrap();
    if matches!(
        kind,
        UnknownCatalog::PossibleMatch | UnknownCatalog::Incomplete
    ) {
        assert_eq!(report.vector.built_partitions, 0);
        assert!(
            db.vector_store
                .partition_graph_generation_status(&route(1))
                .unwrap()
                .base
                .is_none()
        );
        assert!(
            db.vector_store
                .partition_route_quarantine(&route(1))
                .is_none()
        );
    } else {
        assert!(
            report.vector.built_partitions >= 1,
            "the independent healthy route publishes; report={report:?}"
        );
        if !matches!(kind, UnknownCatalog::DurableOnly) {
            assert!(
                db.vector_store
                    .partition_graph_generation_status(&route(1))
                    .unwrap()
                    .base
                    .is_some()
            );
        }
    }
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    if matches!(
        kind,
        UnknownCatalog::Giant
            | UnknownCatalog::PossibleMatch
            | UnknownCatalog::DurableOnly
            | UnknownCatalog::Incomplete
    ) {
        let details = report.vector.first_failure_details.unwrap();
        assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
        assert_eq!(details.operation, "prepare_vector_generation");
        assert!(largest.load(Ordering::SeqCst) < MALFORMED_METADATA_BYTES);
        assert!(
            db.vector_store
                .partition_route_quarantine(&route(0))
                .is_none()
        );
        if !matches!(kind, UnknownCatalog::DurableOnly) {
            assert_eq!(
                db.vector_store.partition_graph_generation_status(&route(0)),
                Some(serving)
            );
        }
    } else if matches!(kind, UnknownCatalog::Invalid) {
        assert!(report.vector.first_failure.is_none());
        assert!(
            db.vector_store
                .partition_route_quarantine(&route(0))
                .is_none()
        );
    }
    drop(observation);
    db.accountant.set_budget(None).unwrap();
    if matches!(kind, UnknownCatalog::DurableOnly) {
        assert!(
            db.vector_store
                .partition_info(&route(0).index, &route(0).partition_key)
                .is_none(),
            "durable publication does not fabricate an empty resident state"
        );
        let catalog = persistence
            .load_vector_partition_generation_catalog_for(&route(0).index, &route(0).partition_key)
            .unwrap()
            .unwrap();
        assert_eq!(catalog.base.covered_tx, db.tx_mgr.current_tx_max());
        assert!(catalog.base.generation_id > serving.base.unwrap().generation_id);
        let base = persistence
            .load_vector_partition_base_generation_with_expected_graph_bytes(
                &route(0).index,
                &route(0).partition_key,
                catalog.base.generation_id,
                catalog.base.durable_bytes,
            )
            .unwrap();
        assert_eq!(
            HnswIndex::decode_durable_generation(
                base.graph.bytes.as_slice(),
                3,
                VectorQuantization::F32
            )
            .unwrap()
            .len(),
            16,
            "the cold body roundtrip publishes the live graph rather than an empty snapshot"
        );
    }
    persistence
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            let table = read.open_table(CATALOGS).unwrap();
            if !matches!(kind, UnknownCatalog::Miskey) {
                assert_eq!(
                    table.get(unknown_key).unwrap().unwrap().value(),
                    unknown_bytes.as_slice()
                );
            }
            Ok(())
        })
        .unwrap();
    if matches!(kind, UnknownCatalog::PossibleMatch) {
        let largest = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&largest);
        let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
            observed.fetch_max(bytes, Ordering::SeqCst);
        }));
        db.clear_maintenance_cycle_stamp_for_test();
        let retried = db.run_maintenance_cycle().unwrap();
        assert!(retried.vector.built_partitions >= 1);
        assert_ne!(
            retried.vector.first_failure,
            Some(VectorMaintenanceFailure::MemoryLimit)
        );
        assert!(largest.load(Ordering::SeqCst) >= MALFORMED_METADATA_BYTES);
        assert!(db.maintenance_cycle_stamp_is_set_for_test());
        assert_eq!(
            db.vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            0
        );
        drop(observation);
    }
    db.close().unwrap();
}

#[test]
fn public_cycle_giant_before_healthy_catalog_continues() {
    public_unknown_catalog_preserves_healthy_work(true, UnknownCatalog::Giant);
}
#[test]
fn public_cycle_giant_after_healthy_catalog_continues() {
    public_unknown_catalog_preserves_healthy_work(false, UnknownCatalog::Giant);
}
#[test]
fn public_cycle_small_unknown_invalid_catalog_does_not_quarantine() {
    public_unknown_catalog_preserves_healthy_work(true, UnknownCatalog::Invalid);
}
#[test]
fn public_cycle_miskey_before_valid_catalog_quarantines_canonical_route() {
    public_unknown_catalog_preserves_healthy_work(true, UnknownCatalog::Miskey);
}
#[test]
fn public_cycle_miskey_after_valid_catalog_quarantines_canonical_route() {
    public_unknown_catalog_preserves_healthy_work(false, UnknownCatalog::Miskey);
}

fn original_catalog_for_route(
    db: &Database,
    route: &VectorPartitionRef,
) -> VectorPartitionGenerationCatalogRecord {
    db.persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&route.index, &route.partition_key)
        .unwrap()
        .unwrap()
}
#[test]
fn public_cycle_possible_match_blocks_until_raised_limit_retries_fresh() {
    public_unknown_catalog_preserves_healthy_work(true, UnknownCatalog::PossibleMatch);
}

#[test]
fn public_cycle_valid_durable_only_catalog_after_refused_giant_is_discovered() {
    public_unknown_catalog_preserves_healthy_work(true, UnknownCatalog::DurableOnly);
}

#[test]
fn public_cycle_incomplete_container_sample_preserves_serving_and_closes() {
    public_unknown_catalog_preserves_healthy_work(true, UnknownCatalog::Incomplete);
}

thread_local! {
    static INVENTORY_SLOT_REFUSAL_FOR_TEST: std::cell::RefCell<Option<(usize, &'static str, usize)>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) struct InventorySlotBudgetForTest {
    accountant: Arc<MemoryAccountant>,
    limit: Option<usize>,
}

impl Drop for InventorySlotBudgetForTest {
    fn drop(&mut self) {
        self.accountant.set_budget(self.limit).unwrap();
    }
}

/// Narrow the real accountant only at the requested slot growth, after its
/// route workspace has succeeded. The real try_grow_for supplies the error.
#[doc(hidden)]
pub(super) fn refuse_inventory_slot_for_test(
    accountant: &Arc<MemoryAccountant>,
    slot: &'static str,
) -> Option<InventorySlotBudgetForTest> {
    let refuse = INVENTORY_SLOT_REFUSAL_FOR_TEST.with(|active| {
        let mut active = active.borrow_mut();
        let (owner, target, attempts) = active.as_mut()?;
        if *owner != Arc::as_ptr(accountant) as usize || *target != slot {
            return None;
        }
        *attempts += 1;
        // Allow one slot to retain partial work before refusing the next.
        (*attempts >= 2).then_some(())
    });
    refuse?;
    let usage = accountant.usage();
    accountant.set_budget(Some(usage.used)).unwrap();
    Some(InventorySlotBudgetForTest {
        accountant: Arc::clone(accountant),
        limit: usage.limit,
    })
}

struct InventorySlotRefusalScopeForTest;

impl Drop for InventorySlotRefusalScopeForTest {
    fn drop(&mut self) {
        INVENTORY_SLOT_REFUSAL_FOR_TEST.with(|active| *active.borrow_mut() = None);
    }
}

fn public_later_inventory_slot_refusal(slot: &'static str) {
    use contextdb_core::VectorPartitionComponent;
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(directory.path().join("slot-refusal.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE slot_items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 3 SEARCH_MODE INDEXED HNSW (M = 4, EF_CONSTRUCTION = 64))", &HashMap::new()).unwrap();
    for scope in 0..3 {
        for row in 0..16 {
            db.execute(
                "INSERT INTO slot_items VALUES ($id, $scope, '[1,0,0]')",
                &HashMap::from([
                    ("id".into(), Value::Int64(scope * 16 + row)),
                    ("scope".into(), Value::Int64(scope)),
                ]),
            )
            .unwrap();
        }
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        3
    );
    let route = |scope| {
        VectorPartitionRef::new(
            VectorIndexRef::new("slot_items", "v"),
            VectorPartitionKey::from_components(vec![VectorPartitionComponent::Integer(scope)]),
        )
    };
    let mut retirement_snapshot = None;
    if slot == "closing_routes" {
        let held = db.pin_snapshot(db.snapshot());
        // Retained raw history keeps the later delete below the declared
        // consolidation threshold, leaving retirement to the closing pass.
        for step in 1u16..=10 {
            db.execute(
                "UPDATE slot_items SET v = $v WHERE scope = 2",
                &HashMap::from([("v".into(), Value::Vector(vec![1.0, f32::from(step), 0.0]))]),
            )
            .unwrap();
        }
        db.execute(
            "ALTER TABLE slot_items ALTER COLUMN v SET HNSW (M = 8, EF_CONSTRUCTION = 64)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "UPDATE slot_items SET v = '[0,1,0]' WHERE scope = 0",
            &HashMap::new(),
        )
        .unwrap();
        assert!(db.run_maintenance_cycle().unwrap().vector.built_partitions >= 2);
        assert!(
            db.vector_store
                .partition_graph_generation_status(&route(0))
                .unwrap()
                .retained_generations
                > 0
        );
        assert_eq!(
            db.execute("SHOW VECTOR_PARTITIONS FOR slot_items.v", &HashMap::new())
                .unwrap()
                .rows
                .len(),
            3,
            "the replacement setup leaves every route active before staging retirement"
        );
        db.execute("DELETE FROM slot_items WHERE scope = 2", &HashMap::new())
            .unwrap();
        retirement_snapshot = Some(held);
    }
    // Two fresh routes exercise a partially populated sampled/candidate Vec.
    db.execute(
        "UPDATE slot_items SET v = '[0,0,1]' WHERE scope = 1",
        &HashMap::new(),
    )
    .unwrap();
    if slot != "closing_routes" {
        db.execute(
            "UPDATE slot_items SET v = '[0,1,0]' WHERE scope = 2",
            &HashMap::new(),
        )
        .unwrap();
    }
    drop(retirement_snapshot);
    let assert_serving_answers = || {
        let rows = db
            .execute(
                "SELECT id FROM slot_items WHERE scope = 0 ORDER BY v <=> [0,1,0] USE VECTOR INDEXED LIMIT 16",
                &HashMap::new(),
            )
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 16);
        for id in 0..16 {
            assert!(rows.contains(&vec![Value::Int64(id)]));
        }
    };
    if slot == "closing_routes" {
        assert_serving_answers();
    }
    let serving = db
        .vector_store
        .partition_graph_generation_status(&route(0))
        .unwrap();
    let catalog = original_catalog_for_route(&db, &route(0));
    let state = db
        .vector_store
        .try_partition_state(&route(0).index, &route(0).partition_key)
        .unwrap();
    let pins = Arc::strong_count(&state);
    let snapshot_holds = db.snapshot_registry.state.lock().active.len();
    let used = db.accountant.usage().used;
    if slot == "closing_routes" {
        assert_eq!(
            db.execute("SHOW VECTOR_PARTITIONS FOR slot_items.v", &HashMap::new())
                .unwrap()
                .rows
                .len(),
            3,
            "the deleted route remains active before the closing-slot refusal"
        );
        let retirement_candidate = db
            .vector_store
            .partition_info(&route(2).index, &route(2).partition_key)
            .expect("scope 2 is an active retirement candidate before refusal");
        assert_eq!(retirement_candidate.live_rows, 0);
        assert!(retirement_candidate.retained_rows >= 176);
        assert!(
            db.vector_store
                .index_layout(&route(2).index)
                .unwrap()
                .maintenance_need(&retirement_candidate)
                .is_none(),
            "the deleted route needs closing retirement rather than another publication"
        );
    }
    INVENTORY_SLOT_REFUSAL_FOR_TEST.with(|active| {
        *active.borrow_mut() = Some((Arc::as_ptr(&db.accountant) as usize, slot, 0));
    });
    let refusal = InventorySlotRefusalScopeForTest;
    db.clear_maintenance_cycle_stamp_for_test();
    let report = db
        .run_maintenance_cycle()
        .expect("late container refusal still returns the completed cycle report");
    let attempts =
        INVENTORY_SLOT_REFUSAL_FOR_TEST.with(|active| active.borrow().as_ref().unwrap().2);
    assert!(
        attempts >= 2,
        "the route workspace and one earlier slot succeeded before real slot refusal"
    );
    let details = report.vector.first_failure_details.unwrap();
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    assert_eq!(details.operation, "prepare_vector_generation");
    assert_eq!(details.available_bytes, Some(0));
    assert!(details.requested_bytes.unwrap() > 0);
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(
        Arc::strong_count(&state),
        pins,
        "all sampled source pins are released"
    );
    assert_eq!(
        db.snapshot_registry.state.lock().active.len(),
        snapshot_holds
    );
    assert_eq!(
        db.vector_store.partition_graph_generation_status(&route(0)),
        Some(serving)
    );
    assert_eq!(
        original_catalog_for_route(&db, &route(0)),
        catalog,
        "an incomplete closing sample cannot reclaim the unchanged catalog"
    );
    if slot == "closing_routes" {
        assert_eq!(
            report.vector.built_partitions, 1,
            "closing refusal preserves the successful publication report"
        );
        assert_eq!(
            db.execute("SHOW VECTOR_PARTITIONS FOR slot_items.v", &HashMap::new())
                .unwrap()
                .rows
                .len(),
            3,
            "the incomplete closing sample grants no retirement authority"
        );
        assert_serving_answers();
    } else {
        assert_eq!(
            report.vector.built_partitions, 0,
            "all partial candidates are discarded"
        );
        assert_eq!(
            db.accountant.usage().used,
            used,
            "all partial collection and workspace owners are released"
        );
    }
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    drop(refusal);
    assert!(db.accountant.usage().limit.is_none());
    db.clear_maintenance_cycle_stamp_for_test();
    let retry = db.run_maintenance_cycle().unwrap();
    assert!(retry.vector.first_failure.is_none());
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    if slot == "closing_routes" {
        assert_eq!(
            db.vector_store
                .partition_graph_generation_status(&route(0))
                .unwrap()
                .retained_generations,
            0
        );
        assert_eq!(
            db.execute("SHOW VECTOR_PARTITIONS FOR slot_items.v", &HashMap::new())
                .unwrap()
                .rows
                .len(),
            2,
            "the complete retry retires the deleted route while retaining historical raw state"
        );
    } else {
        assert_eq!(
            retry.vector.built_partitions, 2,
            "a new complete sample retries both discarded candidates"
        );
    }
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    db.close().unwrap();
}

#[test]
fn public_cycle_sampled_route_slot_refusal_drops_partial_work_and_closes() {
    public_later_inventory_slot_refusal("sampled_routes");
}

#[test]
fn public_cycle_candidate_slot_refusal_drops_partial_work_and_closes() {
    public_later_inventory_slot_refusal("candidates");
}

#[test]
fn public_cycle_closing_route_slot_refusal_preserves_publication_and_skips_removal() {
    public_later_inventory_slot_refusal("closing_routes");
}

#[derive(Default)]
struct CandidateSortObservation {
    owner: usize,
    refuse: bool,
    attempts: usize,
    count: usize,
    bytes: usize,
    used_before: usize,
    used_during: usize,
    before: Vec<(VectorPartitionRef, bool)>,
    after: Vec<(VectorPartitionRef, bool)>,
}

thread_local! {
    static CANDIDATE_SORT_FOR_TEST: std::cell::RefCell<CandidateSortObservation> =
        std::cell::RefCell::new(CandidateSortObservation::default());
}

pub(super) fn candidate_sort_budget_for_test(
    accountant: &Arc<MemoryAccountant>,
    count: usize,
    bytes: usize,
) -> Option<InventorySlotBudgetForTest> {
    let refuse = CANDIDATE_SORT_FOR_TEST.with(|active| {
        let mut active = active.borrow_mut();
        if active.owner != Arc::as_ptr(accountant) as usize {
            return false;
        }
        active.attempts += 1;
        active.count = count;
        active.bytes = bytes;
        active.used_before = accountant.usage().used;
        active.refuse
    });
    if !refuse {
        return None;
    }
    let usage = accountant.usage();
    accountant.set_budget(Some(usage.used)).unwrap();
    Some(InventorySlotBudgetForTest {
        accountant: Arc::clone(accountant),
        limit: usage.limit,
    })
}

pub(super) fn observe_candidate_sort_for_test<'a>(
    accountant: &Arc<MemoryAccountant>,
    candidates: impl Iterator<Item = (&'a VectorPartitionRef, bool)>,
    after: bool,
) {
    CANDIDATE_SORT_FOR_TEST.with(|active| {
        let mut active = active.borrow_mut();
        if active.owner != Arc::as_ptr(accountant) as usize {
            return;
        }
        active.used_during = accountant.usage().used;
        let observed = candidates
            .map(|(route, cold)| (route.clone(), cold))
            .collect();
        if after {
            active.after = observed;
        } else {
            active.before = observed;
        }
    });
}

struct CandidateSortScopeForTest;

impl Drop for CandidateSortScopeForTest {
    fn drop(&mut self) {
        CANDIDATE_SORT_FOR_TEST
            .with(|active| *active.borrow_mut() = CandidateSortObservation::default());
    }
}

#[test]
fn public_cycle_candidate_sort_heap_refusal_releases_and_retries_stably() {
    use super::vector_maintenance_inventory::candidate_sort_heap_bytes;
    use contextdb_core::VectorPartitionComponent;
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open_with_options(
        directory.path().join("sort-owner.db"),
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    db.execute("CREATE TABLE sort_items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 22 SEARCH_MODE INDEXED HNSW (M = 4, EF_CONSTRUCTION = 64))", &HashMap::new()).unwrap();
    for scope in 0..22 {
        for row in 0..16 {
            db.execute(
                "INSERT INTO sort_items VALUES ($id, $scope, '[1,0,0]')",
                &HashMap::from([
                    ("id".into(), Value::Int64(scope * 16 + row)),
                    ("scope".into(), Value::Int64(scope)),
                ]),
            )
            .unwrap();
        }
        if scope == 0 {
            assert_eq!(
                db.run_maintenance_cycle().unwrap().vector.built_partitions,
                1
            );
        }
    }
    db.execute(
        "UPDATE sort_items SET v = '[0,1,0]' WHERE scope = 0",
        &HashMap::new(),
    )
    .unwrap();
    let route = VectorPartitionRef::new(
        VectorIndexRef::new("sort_items", "v"),
        VectorPartitionKey::from_components(vec![VectorPartitionComponent::Integer(0)]),
    );
    let state = db
        .vector_store
        .try_partition_state(&route.index, &route.partition_key)
        .unwrap();
    let pins = Arc::strong_count(&state);
    let holds = db.snapshot_registry.state.lock().active.len();
    let serving = db
        .vector_store
        .partition_graph_generation_status(&route)
        .unwrap();
    let catalog = original_catalog_for_route(&db, &route);
    let used = db.accountant.usage().used;
    assert_eq!(candidate_sort_heap_bytes(0), 0);
    assert_eq!(
        candidate_sort_heap_bytes(20),
        0,
        "insertion sort receives no extra heap charge"
    );
    assert!(
        candidate_sort_heap_bytes(22) > 4096,
        "this candidate count forces the real standard-library heap buffer"
    );
    CANDIDATE_SORT_FOR_TEST.with(|active| {
        *active.borrow_mut() = CandidateSortObservation {
            owner: Arc::as_ptr(&db.accountant) as usize,
            refuse: true,
            ..CandidateSortObservation::default()
        }
    });
    let scope = CandidateSortScopeForTest;
    crate::memory_accounting::watch_budget_ledger_for_test();
    db.clear_maintenance_cycle_stamp_for_test();
    let report = db.run_maintenance_cycle().unwrap();
    let ledger = crate::memory_accounting::take_budget_ledger_for_test();
    let details = report.vector.first_failure_details.unwrap();
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    assert_eq!(details.operation, "prepare_vector_generation");
    assert_eq!(details.available_bytes, Some(0));
    assert_eq!(
        details.requested_bytes,
        Some(candidate_sort_heap_bytes(22) as u64)
    );
    CANDIDATE_SORT_FOR_TEST.with(|active| {
        let active = active.borrow();
        assert_eq!(active.attempts, 1);
        assert_eq!(
            active.count, 22,
            "all earlier candidate admissions succeeded"
        );
        assert!(
            active.used_before > used,
            "phase and candidate owners coexist before scratch admission"
        );
        assert!(active.before.is_empty(), "refusal precedes the actual sort");
    });
    assert!(
        !ledger
            .iter()
            .any(|entry| entry.what.contains("read_vector_generation_source_page")),
        "generation-body reading never begins"
    );
    assert_eq!(report.vector.built_partitions, 0);
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(Arc::strong_count(&state), pins);
    assert_eq!(db.snapshot_registry.state.lock().active.len(), holds);
    assert_eq!(
        db.vector_store.partition_graph_generation_status(&route),
        Some(serving)
    );
    assert_eq!(original_catalog_for_route(&db, &route), catalog);
    assert_eq!(db.accountant.underflow_count_for_test(), 0);

    CANDIDATE_SORT_FOR_TEST.with(|active| active.borrow_mut().refuse = false);
    let candidates = db
        .maintenance_context()
        .maintenance_build_candidates()
        .unwrap();
    assert_eq!(candidates.candidates.len(), 22);
    assert!(candidates.failures.first.is_none());
    CANDIDATE_SORT_FOR_TEST.with(|active| {
        let active = active.borrow();
        assert_eq!(
            active.used_during,
            active.used_before + active.bytes,
            "the actual sort holds its full heap owner"
        );
        assert!(active.before.iter().any(|(_, cold)| *cold));
        assert!(active.before.iter().any(|(_, cold)| !*cold));
        let expected: Vec<_> = active
            .before
            .iter()
            .filter(|(_, cold)| *cold)
            .chain(active.before.iter().filter(|(_, cold)| !*cold))
            .cloned()
            .collect();
        assert_eq!(
            active.after, expected,
            "cold-first ordering preserves every within-class tie"
        );
    });
    drop(candidates);
    assert_eq!(
        db.accountant.usage().used,
        used,
        "scratch and candidate owners both release"
    );
    assert_eq!(Arc::strong_count(&state), pins);
    drop(scope);
    db.clear_maintenance_cycle_stamp_for_test();
    let retry = db.run_maintenance_cycle().unwrap();
    assert_eq!(retry.vector.built_partitions, 22);
    assert!(retry.vector.first_failure.is_none());
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    db.close().unwrap();
}
