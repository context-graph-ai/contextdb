use super::*;
use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
};

fn candidate(
    index: VectorIndexRef,
    partition_key: VectorPartitionKey,
) -> VectorPartitionGenerationCandidate {
    let base = VectorPartitionBaseGenerationRecord {
        index: index.clone(),
        partition_key: partition_key.clone(),
        generation_id: 1,
        base_tx: TxId(4),
        base_lsn: Lsn(5),
        highest_included_tx: TxId(6),
        highest_included_lsn: Lsn(8),
        dimension: 3,
        quantization: VectorQuantization::F32,
        algorithm_version: 1,
        graph_format_version: 1,
        graph: VectorPartitionGraphBytes::verified(vec![13; 17]),
        state_digest: [17; 32],
    };
    let change = VectorPartitionChangeGenerationRecord {
        index: index.clone(),
        partition_key: partition_key.clone(),
        base_generation_id: 1,
        change_generation_id: 2,
        covered_through_tx: TxId(7),
        covered_through_lsn: Lsn(9),
        fresh_tail_from_tx: TxId(7),
        fresh_tail_from_lsn: Lsn(9),
        dimension: 3,
        quantization: VectorQuantization::F32,
        algorithm_version: 1,
        graph_format_version: 1,
        graph: VectorPartitionGraphBytes::verified(vec![19; 23]),
    };
    let descriptor = |generation_id, covered_tx, covered_lsn, durable_bytes| {
        VectorPartitionGenerationDescriptor {
            generation_id,
            covered_tx,
            covered_lsn,
            resident_bytes: 1,
            durable_bytes,
            policy_revision: 1,
            hnsw_m: 16,
            hnsw_ef_construction: 64,
        }
    };
    let catalog = VectorPartitionGenerationCatalogRecord {
        index,
        partition_key,
        base: descriptor(1, TxId(6), Lsn(8), 17),
        change: Some(descriptor(2, TxId(7), Lsn(9), 23)),
        published_tx: TxId(7),
        published_lsn: Lsn(9),
    };
    VectorPartitionGenerationCandidate {
        base,
        change: Some(change),
        catalog,
    }
}

fn journal(candidate: &VectorPartitionGenerationCandidate) -> VectorPartitionJournalRecord {
    VectorPartitionJournalRecord {
        index: candidate.catalog.index.clone(),
        partition_key: candidate.catalog.partition_key.clone(),
        lsn: Lsn(9),
        ordinal: 0,
        change: VectorPartitionJournalChange::Upsert(VectorPartitionMembershipRecord {
            index: candidate.catalog.index.clone(),
            partition_key: candidate.catalog.partition_key.clone(),
            row_id: RowId(1),
            row_created_tx: TxId(4),
            row_lsn: Lsn(5),
            vector_created_tx: TxId(4),
            vector_lsn: Lsn(5),
            visible_from: TxId(4),
            deleted_tx: None,
        }),
    }
}

fn insert_journal(persistence: &RedbPersistence, record: &VectorPartitionJournalRecord) {
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                persistence.append_vector_partition_journal(&mut table, record)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
}

#[test]
fn cold_publication_and_cleanup_exclude_unrelated_singleton_extents() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cold-catalog.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    drop(database);
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    let unrelated = candidate(
        VectorIndexRef::new("cleanup_items", "v"),
        VectorPartitionKey::from_components(vec![contextdb_core::VectorPartitionComponent::Text(
            "x".repeat(2 * 1024 * 1024),
        )]),
    );
    let prepared = persistence
        .prepare_vector_partition_generation_candidate(&unrelated)
        .unwrap();
    persistence
        .publish_prepared_vector_partition_generation_candidate(&prepared)
        .unwrap();
    insert_journal(&persistence, &journal(&unrelated));
    persistence.close();
    drop(persistence);

    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    let selected = candidate(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let largest = Arc::new(AtomicUsize::new(0));
    let observed = largest.clone();
    let _observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    redb::with_read_memory_admission(
        Arc::new(|bytes| {
            if bytes > 512 * 1024 {
                Err(io::Error::from(io::ErrorKind::QuotaExceeded))
            } else {
                Ok(())
            }
        }),
        || {
            assert!(
                persistence
                    .load_vector_partition_generation_descriptors_for(
                        &selected.catalog.index,
                        &selected.catalog.partition_key
                    )
                    .unwrap()
                    .is_none()
            );
            let prepared = persistence
                .prepare_vector_partition_generation_candidate(&selected)
                .unwrap();
            assert!(prepared.attributable_stale_catalog_keys.is_empty());
            persistence
                .publish_prepared_vector_partition_generation_candidate(&prepared)
                .unwrap();
            // Immutable existence checks and catalog replacement also work on retry.
            persistence
                .publish_prepared_vector_partition_generation_candidate(&prepared)
                .unwrap();
            assert_eq!(
                persistence
                    .load_vector_partition_generation_catalog_for(
                        &selected.catalog.index,
                        &selected.catalog.partition_key
                    )
                    .unwrap(),
                Some(selected.catalog.clone())
            );
            assert!(
                !persistence
                    .vector_partition_has_covered_journal_records(&selected.catalog)
                    .unwrap()
            );
            insert_journal(&persistence, &journal(&selected));
            assert!(
                persistence
                    .vector_partition_has_covered_journal_records(&selected.catalog)
                    .unwrap()
            );
            let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
            let store = contextdb_vector::VectorStore::default();
            let mut workspace = store
                .reserve_workspace(accountant.clone(), 0, "test_cleanup", "test")
                .unwrap();
            assert!(
                persistence
                    .truncate_vector_partition_journal_through_catalog(
                        &selected.catalog,
                        &mut workspace
                    )
                    .unwrap()
            );
            assert!(
                !persistence
                    .vector_partition_has_covered_journal_records(&selected.catalog)
                    .unwrap()
            );
            assert!(
                persistence
                    .truncate_vector_partition_journal_through_catalog(
                        &selected.catalog,
                        &mut workspace
                    )
                    .unwrap()
            );
            drop(workspace);
            assert_eq!(accountant.usage().used, 0);
        },
    );
    assert!(largest.load(Ordering::SeqCst) <= 512 * 1024);
    // Bounded exclusion does not alter the canonical unrelated records.
    assert_eq!(
        persistence
            .load_vector_partition_generation_catalog_for(
                &unrelated.catalog.index,
                &unrelated.catalog.partition_key
            )
            .unwrap(),
        Some(unrelated.catalog.clone())
    );
    assert!(
        persistence
            .vector_partition_has_covered_journal_records(&unrelated.catalog)
            .unwrap()
    );
    persistence.close();
}

#[test]
fn miskeyed_catalog_attribution_still_authenticates_the_complete_record() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("miskeyed-catalog.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    drop(database);
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    let selected = candidate(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let bytes = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::GenerationCatalog,
        &selected.catalog,
    )
    .unwrap();
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(b"miskeyed".as_slice(), bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
                let mut malformed = bytes.clone();
                *malformed.last_mut().unwrap() ^= 1;
                table
                    .insert(b"malformed".as_slice(), malformed.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    let prepared = persistence
        .prepare_vector_partition_generation_candidate(&selected)
        .unwrap();
    assert_eq!(
        prepared.attributable_stale_catalog_keys,
        vec![b"miskeyed".to_vec()]
    );
    persistence
        .publish_prepared_vector_partition_generation_candidate(&prepared)
        .unwrap();
    persistence
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let table = read
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            assert!(table.get(b"miskeyed".as_slice()).unwrap().is_none());
            assert!(table.get(b"malformed".as_slice()).unwrap().is_some());
            Ok(())
        })
        .unwrap();
    persistence.close();
}

#[test]
fn journal_cleanup_scan_cancellation_aborts_validation_collection_and_deletion() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cancel-long-journal.db");
    let database = crate::Database::open_with_options(
        &path,
        crate::DatabaseOpenOptions {
            maintenance_policy: crate::MaintenancePolicy::CallerDriven,
            ..crate::DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    database.close().unwrap();
    drop(database);
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    let selected = candidate(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let prepared = persistence
        .prepare_vector_partition_generation_candidate(&selected)
        .unwrap();
    persistence
        .publish_prepared_vector_partition_generation_candidate(&prepared)
        .unwrap();
    // Three complete 64-record windows in each pass. Every key is covered;
    // stopping in deletion therefore follows real uncommitted removals.
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                    .map_err(RedbPersistence::storage_error)?;
                for ordinal in 0..192 {
                    let mut record = journal(&selected);
                    record.ordinal = ordinal;
                    persistence.append_vector_partition_journal(&mut table, &record)?;
                }
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    let partition = contextdb_vector::VectorPartitionRef::new(
        selected.catalog.index.clone(),
        selected.catalog.partition_key.clone(),
    );
    let before = persistence
        .debug_vector_partition_journal_for_test(&partition)
        .unwrap();
    assert_eq!(before.1.len(), 192);
    let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let store = contextdb_vector::VectorStore::default();
    // T2 removes the covered prefix in committed steps of
    // VECTOR_JOURNAL_CLEANUP_STEP_RECORDS records. One step checks once on
    // entry, once before each of its records and once before its commit, and
    // the steps are separated by one more check, so a full step spans 67
    // checks. A stop inside a step publishes none of that step's removals;
    // the steps that committed before it stand.
    const STEP: usize = VECTOR_JOURNAL_CLEANUP_STEP_RECORDS;
    let mut remaining = before.1.len();
    for (pass, stop_at, committed_steps) in [
        ("first step", 3, 0),
        ("second step", 70, 1),
        ("second step again", 70, 1),
    ] {
        let mut workspace = store
            .reserve_workspace(accountant.clone(), 0, "test_cleanup", "test")
            .unwrap();
        let mut checks = 0;
        let result = persistence.truncate_vector_partition_journal_through_catalog_with_checkpoint(
            &selected.catalog,
            &mut workspace,
            || {
                checks += 1;
                if checks == stop_at {
                    Err(Error::ReadCancelled)
                } else {
                    Ok(())
                }
            },
        );
        assert!(
            matches!(result, Err(Error::ReadCancelled)),
            "{pass}: {result:?}"
        );
        assert_eq!(
            checks, stop_at,
            "{pass} stops at its first cancelled checkpoint"
        );
        remaining -= committed_steps * STEP;
        let after = persistence
            .debug_vector_partition_journal_for_test(&partition)
            .unwrap();
        assert_eq!(
            (after.0, after.1.len()),
            (before.0, remaining),
            "{pass} publishes exactly the steps that committed before the stop"
        );
        assert!(
            accountant.usage().used > 0,
            "the test holds the cleanup workspace until drop"
        );
        drop(workspace);
        assert_eq!(
            accountant.usage().used,
            0,
            "{pass} releases its whole temporary reservation"
        );
        assert_eq!(accountant.underflow_count_for_test(), 0);
    }
    persistence.close();
    drop(persistence);
    let restarted = RedbPersistence::open_with_cache(&path, 0).unwrap();
    assert_eq!(
        restarted
            .debug_vector_partition_journal_for_test(&partition)
            .unwrap()
            .1
            .len(),
        remaining
    );
    let mut workspace = store
        .reserve_workspace(accountant.clone(), 0, "test_cleanup", "test")
        .unwrap();
    assert!(
        restarted
            .truncate_vector_partition_journal_through_catalog_with_checkpoint(
                &selected.catalog,
                &mut workspace,
                || Ok(()),
            )
            .unwrap()
    );
    assert!(
        restarted
            .debug_vector_partition_journal_for_test(&partition)
            .unwrap()
            .1
            .is_empty()
    );
    drop(workspace);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
    restarted.close();
}
