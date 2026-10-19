//! A refused covered-journal retry defers only its own build. A later search
//! must still prove every partition's frontier before publishing an answer.

use super::*;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::sync::atomic::AtomicUsize;

const CATALOGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_generation_catalog");
const JOURNAL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_partition_journal");
const REFUSING_EXTENT_BYTES: usize = 9 * 1024 * 1024;
const INITIAL_ROWS: i64 = 256;
const SIBLING_START: i64 = 1000;
const VISIBLE_ROWS: usize = (INITIAL_ROWS * 2 + 3) as usize;
const FRESH_MUTATION_COUNT: usize = 4 + 1;

fn route(scope: i64) -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("retry_frontier_items", "v"),
        VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
    )
}

fn insert(db: &Database, id: i64, scope: i64) {
    db.execute(
        "INSERT INTO retry_frontier_items VALUES ($id, $scope, $v)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("scope".into(), Value::Int64(scope)),
            ("v".into(), Value::Vector(vec![1.0, id as f32 + 1.0, 0.0])),
        ]),
    )
    .unwrap();
}

#[test]
fn cleanup_refusal_skips_its_build_while_sibling_and_complete_query_frontiers_progress() {
    use contextdb_vector::mem::VectorPartitionSearchRoute;

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
        "CREATE TABLE retry_frontier_items (id INT PRIMARY KEY, scope INT NOT NULL, \
         v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED \
         AUTO_INDEX_AT 1 HNSW (M = 4, EF_CONSTRUCTION = 64, EF_SEARCH = 64) \
         CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1))",
        &HashMap::new(),
    )
    .unwrap();
    // Keep the admitted scope above AUTO's resident comparison crossover,
    // so the proof observes a mixed graph/exact route after a shortfall.
    for id in 0..INITIAL_ROWS {
        insert(&db, id, 0);
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let refusing = route(0);
    let sibling = route(1);
    let mut serving = db
        .vector_store
        .partition_graph_generation_status(&refusing)
        .unwrap();
    let catalog = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&refusing.index, &refusing.partition_key)
        .unwrap()
        .unwrap();
    for id in INITIAL_ROWS..INITIAL_ROWS + 4 {
        insert(&db, id, 0);
    }
    db.execute(
        "UPDATE retry_frontier_items SET v = '[1,0,0]' WHERE id = 0",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "DELETE FROM retry_frontier_items WHERE id = 1",
        &HashMap::new(),
    )
    .unwrap();
    serving.fresh_tail_entries = FRESH_MUTATION_COUNT;
    for id in SIBLING_START..SIBLING_START + INITIAL_ROWS {
        insert(&db, id, 1);
    }

    // A task-owned logical copy has independent writer ownership. Carry the
    // existing serving generation too, then reopen without a page cache so
    // the covered identity's physical leaf is deterministically cold.
    db.export_snapshot(&copy).unwrap();
    let persistence = RedbPersistence::open_with_cache(&copy, 0).unwrap();
    let mut journal_key = db
        .persistence
        .as_ref()
        .unwrap()
        .with_db(|source| {
            let read = source.begin_read().unwrap();
            let catalogs = read.open_table(CATALOGS).unwrap();
            let (key, _) = catalogs.iter().unwrap().next().unwrap().unwrap();
            let catalog_key = key.value().to_vec();
            persistence.with_db(|destination| {
                let write = destination.begin_write().unwrap();
                for name in [
                    "vector_partition_base_generations",
                    "vector_partition_change_generations",
                    "vector_partition_graph_pages",
                    "vector_partition_generation_catalog",
                    "vector_partition_journal",
                    "vector_partition_membership",
                    "vector_entries",
                    "vector_open_directory",
                    "vector_membership_summary_pages",
                    "vector_membership_source_pages",
                    "vector_membership_reverse_pages",
                    "vector_paged_generation_pages",
                    "vector_paged_builds",
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
                let counters = TableDefinition::<&str, u64>::new("vector_paged_counters");
                write.delete_table(counters).unwrap();
                if let Ok(source_table) = read.open_table(counters) {
                    let mut destination_table = write.open_table(counters).unwrap();
                    for entry in source_table.iter().unwrap() {
                        let (key, value) = entry.unwrap();
                        destination_table
                            .insert(key.value(), value.value())
                            .unwrap();
                    }
                }
                let cleanup = TableDefinition::<u64, &[u8]>::new("vector_paged_cleanup");
                write.delete_table(cleanup).unwrap();
                if let Ok(source_table) = read.open_table(cleanup) {
                    let mut destination_table = write.open_table(cleanup).unwrap();
                    for entry in source_table.iter().unwrap() {
                        let (key, value) = entry.unwrap();
                        destination_table
                            .insert(key.value(), value.value())
                            .unwrap();
                    }
                }
                let floors = TableDefinition::<&[u8], u64>::new("vector_build_floors");
                write.delete_table(floors).unwrap();
                if let Ok(source_table) = read.open_table(floors) {
                    let mut destination_table = write.open_table(floors).unwrap();
                    for entry in source_table.iter().unwrap() {
                        let (key, value) = entry.unwrap();
                        destination_table
                            .insert(key.value(), value.value())
                            .unwrap();
                    }
                }
                for (name, binding) in [
                    (
                        "vector_membership_source_pages",
                        "vector_membership_source_pages_root_v1",
                    ),
                    (
                        "vector_membership_reverse_pages",
                        "vector_membership_reverse_pages_root_v1",
                    ),
                    (
                        "vector_membership_summary_pages",
                        "vector_membership_summary_pages_root_v1",
                    ),
                    (
                        "vector_partition_membership",
                        "vector_membership_authority_pages_root_v1",
                    ),
                    (
                        "vector_open_directory",
                        "vector_open_directory_pages_root_v1",
                    ),
                ] {
                    let fingerprint = write
                        .table_root_fingerprint(TableDefinition::<&[u8], &[u8]>::new(name))
                        .unwrap()
                        .unwrap_or([0; 32]);
                    write
                        .open_table(TableDefinition::<&str, &[u8]>::new(
                            "__content_identity_metadata",
                        ))
                        .unwrap()
                        .insert(binding, fingerprint.as_slice())
                        .unwrap();
                }
                write.commit().unwrap();
                Ok(())
            })?;
            Ok(catalog_key)
        })
        .unwrap();
    journal_key.extend_from_slice(&catalog.base.covered_lsn.0.to_be_bytes());
    journal_key.extend_from_slice(&0u64.to_be_bytes());
    persistence.close();
    drop(persistence);
    // A paged query's published root and its maintenance source must belong
    // to this same writer. Restore the serving premise through normal preload.
    db.close().unwrap();
    drop(db);
    let accountant = Arc::new(MemoryAccountant::no_limit());
    accountant.set_budget(Some(64 * 1024 * 1024)).unwrap();
    let db = Database::open_with_config(&copy, Arc::new(CorePlugin), accountant).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(
        db.vector_store
            .preload_dormant_partition_generation(&refusing, true)
            .unwrap()
    );
    assert_eq!(
        db.vector_store.partition_graph_generation_status(&refusing),
        Some(serving)
    );
    assert!(
        crate::persistence::redb_writer_cache_bytes_for_limit(Some(64 * 1024 * 1024))
            < REFUSING_EXTENT_BYTES
    );
    let persistence = Arc::clone(db.persistence.as_ref().unwrap());
    persistence
        .with_db(|destination| {
            let write = destination.begin_write().unwrap();
            write
                .open_table(JOURNAL)
                .unwrap()
                .insert(
                    journal_key.as_slice(),
                    vec![0xff; REFUSING_EXTENT_BYTES].as_slice(),
                )
                .unwrap();
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    let headroom = 4 * 1024 * 1024;
    let limit = db.accountant.usage().used + headroom;
    db.accountant.set_budget(Some(limit)).unwrap();
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    db.clear_maintenance_cycle_stamp_for_test();
    let report = db.run_maintenance_cycle().unwrap();
    drop(observation);
    let failure = report.vector.first_failure_details.unwrap();
    assert_eq!(failure.failure, VectorMaintenanceFailure::MemoryLimit);
    assert_eq!(failure.operation, "prepare_vector_generation");
    assert!(failure.requested_bytes.unwrap() > REFUSING_EXTENT_BYTES as u64);
    assert_eq!(failure.budget_limit_bytes, Some(limit as u64));
    assert!(largest_read.load(Ordering::SeqCst) < REFUSING_EXTENT_BYTES);
    assert_eq!(report.vector.built_partitions, 1, "{report:?}");
    assert!(db.maintenance_cycle_stamp_is_set_for_test());
    assert_eq!(
        db.vector_store.partition_graph_generation_status(&refusing),
        Some(serving)
    );
    assert!(
        db.vector_store
            .partition_graph_generation_status(&sibling)
            .unwrap()
            .base
            .is_some()
    );
    assert_eq!(
        persistence
            .load_vector_partition_generation_catalog_for(&refusing.index, &refusing.partition_key)
            .unwrap(),
        Some(catalog)
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);

    let query = |mode: &str| {
        format!(
            "SELECT id, scope FROM retry_frontier_items ORDER BY v <=> [1,0,0] \
             USE VECTOR {mode} LIMIT {VISIBLE_ROWS}"
        )
    };
    let exact = db.execute(&query("EXACT"), &HashMap::new()).unwrap();
    assert_eq!(exact.rows.len(), VISIBLE_ROWS);
    let visible_ids: HashSet<_> = exact
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(id) => *id,
            other => panic!("unexpected row identity: {other:?}"),
        })
        .collect();
    assert_eq!(
        visible_ids,
        (0..INITIAL_ROWS + 4)
            .filter(|id| *id != 1)
            .chain(SIBLING_START..SIBLING_START + INITIAL_ROWS)
            .collect()
    );
    // Force an incomplete candidate frontier only on the retry-refusing
    // partition. Sibling candidates must never become a partial answer.
    let partial = db.vector_store.cap_partition_graph_candidates_for_test(
        &refusing.index,
        &refusing.partition_key,
        0,
    );
    let indexed_error = db.execute(&query("INDEXED"), &HashMap::new()).unwrap_err();
    assert!(
        matches!(&indexed_error, Error::VectorIndexedRouteUnavailable { index }
        if *index == refusing.index),
        "{indexed_error:?}"
    );
    db.__reset_last_query_vector_trace_for_test();
    let auto = db.execute(&query("AUTO"), &HashMap::new()).unwrap();
    let fixture_operation_reader_1 = db
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    let trace = db
        .__take_last_query_vector_trace_for_test(fixture_operation_reader_1.clone())
        .expect("read the admitted trace observation")
        .unwrap();
    assert_eq!(auto.rows, exact.rows);
    assert!(
        !trace.used_hnsw,
        "AUTO follows its costing decision: {trace:?}"
    );
    assert_eq!(trace.fallback_reason, Some("graph_work_not_below_exact"));
    assert_eq!(trace.supplemented_row_count, 0);
    assert_eq!(trace.partition_routes.len(), 2);
    assert!(trace.partition_routes.iter().all(|route| {
        route.route == VectorPartitionSearchRoute::Exact
            && route.reason == "maintained_graph_incomplete"
    }));
    drop(partial);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);

    db.accountant.set_budget(None).unwrap();
    persistence
        .with_db(|destination| {
            let write = destination.begin_write().unwrap();
            write
                .open_table(JOURNAL)
                .unwrap()
                .remove(journal_key.as_slice())
                .unwrap();
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    db.clear_maintenance_cycle_stamp_for_test();
    let retry = db.run_maintenance_cycle().unwrap();
    assert_eq!(retry.vector.built_partitions, 1, "{retry:?}");
    assert!(retry.vector.first_failure.is_none(), "{retry:?}");
    assert_eq!(
        db.execute(&query("INDEXED"), &HashMap::new()).unwrap().rows,
        exact.rows
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    let accountant = db.accountant.clone();
    drop(persistence);
    db.close().unwrap();
    drop(db);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
}
