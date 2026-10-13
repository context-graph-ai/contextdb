use super::*;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::sync::atomic::{AtomicUsize, Ordering};

mod change_typed_open;
mod prior_base_typed_open;

const GENERATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_base_generations");
const CACHE_BYTES: usize = 8 * 1024 * 1024;
const GRAPH_BYTES: usize = 9 * 1024 * 1024;

fn fixture(bytes: usize) -> (tempfile::TempDir, redb::Database) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.db");
    let mut builder = redb::Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    let db = builder.create(&path).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(GENERATIONS).unwrap();
        let graph = vec![7u8; bytes];
        table
            .insert(b"old-authoritative".as_slice(), graph.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    // Reopen so the generation source does not come from the write buffer.
    drop(db);
    let db = builder.open(&path).unwrap();
    (directory, db)
}

fn storage_error(error: impl Into<redb::Error>) -> Error {
    crate::persistence::RedbPersistence::storage_error(error)
}

#[test]
fn oversized_generation_source_is_refused_before_backend_read_with_exact_error() {
    let (_directory, db) = fixture(GRAPH_BYTES);
    let read = db.begin_read().unwrap();
    // Prime bounded table metadata, then release its uncached root page.
    drop(read.open_table(GENERATIONS).unwrap());
    let accountant = Arc::new(MemoryAccountant::with_budget(CACHE_BYTES + 1024));
    accountant.try_allocate(CACHE_BYTES).unwrap();
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    let _observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    let error = match with_source_page_admission(accountant.clone().into(), || {
        read.open_table(GENERATIONS)
            .map_err(storage_error)?
            .get(b"old-authoritative".as_slice())
            .map_err(storage_error)
    }) {
        Err(error) => error,
        Ok(_) => panic!("oversized source unexpectedly admitted"),
    };
    assert!(matches!(error, Error::MemoryBudgetExceeded {
        subsystem, operation, requested_bytes, available_bytes: 1024,
        budget_limit_bytes, ..
    } if subsystem == "vector_maintenance"
        && operation == "read_vector_generation_source_page"
        && requested_bytes > GRAPH_BYTES
        && budget_limit_bytes == CACHE_BYTES + 1024));
    assert!(largest_read.load(Ordering::SeqCst) < GRAPH_BYTES);
    assert_eq!(accountant.usage().used, CACHE_BYTES);
    // Refusal leaves the old durable generation addressable and intact.
    let value = with_source_page_admission(Arc::new(MemoryAccountant::no_limit()).into(), || {
        read.open_table(GENERATIONS)
            .map_err(storage_error)?
            .get(b"old-authoritative".as_slice())
            .map_err(storage_error)
    })
    .unwrap()
    .unwrap();
    assert_eq!(value.value().len(), GRAPH_BYTES);
    assert!(value.value().iter().all(|byte| *byte == 7));
    accountant.release(CACHE_BYTES);
}

#[test]
fn oversized_source_charge_outlives_scope_and_releases_with_last_page_handle() {
    let (_directory, db) = fixture(GRAPH_BYTES);
    let read = db.begin_read().unwrap();
    // Prime bounded table metadata, then release its uncached root page.
    drop(read.open_table(GENERATIONS).unwrap());
    let accountant = Arc::new(MemoryAccountant::with_budget(128 * 1024 * 1024));
    accountant.try_allocate(CACHE_BYTES).unwrap();
    let (key, value) = with_source_page_admission(accountant.clone().into(), || {
        let table = read.open_table(GENERATIONS).map_err(storage_error)?;
        let mut range = table.range::<&[u8]>(..).map_err(storage_error)?;
        range.next().unwrap().map_err(storage_error)
    })
    .unwrap();
    let held = accountant.usage().used;
    assert!(held > CACHE_BYTES + GRAPH_BYTES);
    drop(value);
    assert_eq!(accountant.usage().used, held, "the key still owns the page");
    assert_eq!(key.value(), b"old-authoritative");
    drop(key);
    assert_eq!(accountant.usage().used, CACHE_BYTES);
    accountant.release(CACHE_BYTES);
}

#[test]
fn cached_source_hands_charge_to_fixed_cache_and_nested_scope_restores_callback() {
    let (_directory, db) = fixture(16 * 1024);
    let read = db.begin_read().unwrap();
    // Prime bounded table metadata, then release its uncached root page.
    drop(read.open_table(GENERATIONS).unwrap());
    let accountant = Arc::new(MemoryAccountant::with_budget(2 * CACHE_BYTES));
    accountant.try_allocate(CACHE_BYTES).unwrap();
    let outer_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&outer_calls);
    redb::with_read_memory_admission(
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }),
        || {
            let value = with_source_page_admission(accountant.clone().into(), || {
                read.open_table(GENERATIONS)
                    .map_err(storage_error)?
                    .get(b"old-authoritative".as_slice())
                    .map_err(storage_error)
            })
            .unwrap()
            .unwrap();
            assert_eq!(accountant.usage().used, CACHE_BYTES);
            assert_eq!(outer_calls.load(Ordering::SeqCst), 0);
            drop(value);
            // A separate cold oversized extent ensures a callback after the inner
            // scope is an admission, rather than a cache hit.
            let (_other_directory, other_db) = fixture(GRAPH_BYTES);
            let other_read = other_db.begin_read().unwrap();
            let other_table = other_read.open_table(GENERATIONS).unwrap();
            let value = other_table
                .get(b"old-authoritative".as_slice())
                .unwrap()
                .unwrap();
            assert_eq!(value.value().len(), GRAPH_BYTES);
            assert!(outer_calls.load(Ordering::SeqCst) > 0);
        },
    );
    accountant.release(CACHE_BYTES);
}

#[test]
fn concurrent_source_reads_share_writer_budget_without_releasing_held_page() {
    let (_directory, db) = fixture(GRAPH_BYTES);
    let read = db.begin_read().unwrap();
    // Prime bounded table metadata, then release its uncached root page.
    drop(read.open_table(GENERATIONS).unwrap());
    let accountant = Arc::new(MemoryAccountant::with_budget(128 * 1024 * 1024));
    accountant.try_allocate(CACHE_BYTES).unwrap();
    let value = with_source_page_admission(accountant.clone().into(), || {
        read.open_table(GENERATIONS)
            .map_err(storage_error)?
            .get(b"old-authoritative".as_slice())
            .map_err(storage_error)
    })
    .unwrap()
    .unwrap();
    let held = accountant.usage().used;
    assert!(held > CACHE_BYTES + GRAPH_BYTES);
    accountant.set_budget(Some(held)).unwrap();
    std::thread::scope(|scope| {
        let accountant = accountant.clone();
        let db = &db;
        scope
            .spawn(move || {
                let other_read = db.begin_read().unwrap();
                let result = with_source_page_admission(accountant.clone().into(), || {
                    other_read
                        .open_table(GENERATIONS)
                        .map_err(storage_error)?
                        .get(b"old-authoritative".as_slice())
                        .map_err(storage_error)
                });
                assert!(matches!(result, Err(Error::MemoryBudgetExceeded {
                operation, available_bytes: 0, ..
            }) if operation == "read_vector_generation_source_page"));
                assert_eq!(accountant.usage().used, held);
            })
            .join()
            .unwrap();
    });
    assert_eq!(value.value().len(), GRAPH_BYTES);
    drop(value);
    assert_eq!(accountant.usage().used, CACHE_BYTES);
    accountant.release(CACHE_BYTES);
}

#[test]
fn caught_source_refusal_preserves_success_and_unrelated_errors() {
    let (_directory, db) = fixture(GRAPH_BYTES);
    let read = db.begin_read().unwrap();
    // Prime bounded table metadata, then release its uncached root page.
    drop(read.open_table(GENERATIONS).unwrap());
    let accountant = Arc::new(MemoryAccountant::with_budget(1));
    let published = with_source_page_admission(accountant.clone().into(), || {
        let error = read.open_table(GENERATIONS).err().unwrap();
        let typed = crate::persistence::RedbPersistence::storage_error(error);
        assert!(matches!(typed, Error::MemoryBudgetExceeded { .. }));
        Ok(super::super::VectorPartitionPublicationOutcome::published_with_cleanup_failure(&typed))
    })
    .unwrap();
    assert!(published.published);
    assert_eq!(
        published.cleanup_failure.unwrap().operation,
        "read_vector_generation_source_page"
    );
    let classified = with_source_page_admission(accountant.clone().into(), || {
        assert!(read.open_table(GENERATIONS).is_err());
        Ok(17)
    })
    .unwrap();
    assert_eq!(
        classified, 17,
        "successful classification survives an unconsumed refusal"
    );
    let unrelated = with_source_page_admission(accountant.into(), || -> Result<()> {
        assert!(read.open_table(GENERATIONS).is_err());
        Err(crate::persistence::RedbPersistence::storage_error(
            std::io::Error::other("independent storage failure"),
        ))
    })
    .unwrap_err();
    assert!(
        matches!(unrelated, Error::Other(message) if message.contains("independent storage failure"))
    );
}

#[test]
fn nested_refusals_recover_only_the_current_scope_and_restore_after_unwind() {
    let (_directory, db) = fixture(GRAPH_BYTES);
    let read = db.begin_read().unwrap();
    // Prime bounded table metadata, then release its uncached root page.
    drop(read.open_table(GENERATIONS).unwrap());
    with_source_page_admission(Arc::new(MemoryAccountant::with_budget(3)).into(), || {
        let outer = read.open_table(GENERATIONS).err().unwrap();
        assert!(matches!(
            &outer,
            redb::TableError::Storage(redb::StorageError::AdmissionRefused(_))
        ));
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<()> = with_source_page_admission(
                Arc::new(MemoryAccountant::with_budget(7)).into(),
                || {
                    let inner = read.open_table(GENERATIONS).err().unwrap();
                    assert!(matches!(
                        &inner,
                        redb::TableError::Storage(redb::StorageError::AdmissionRefused(_))
                    ));
                    let typed = crate::persistence::RedbPersistence::storage_error(inner);
                    assert!(matches!(
                        typed,
                        Error::MemoryBudgetExceeded {
                            available_bytes: 7,
                            ..
                        }
                    ));
                    panic!("exercise scope restoration");
                },
            );
        }));
        assert!(unwind.is_err());
        let independent = crate::persistence::RedbPersistence::storage_error(
            std::io::Error::other("independent failure after nested unwind"),
        );
        assert!(matches!(independent, Error::Other(_)));
        let typed = crate::persistence::RedbPersistence::storage_error(outer);
        assert!(matches!(
            typed,
            Error::MemoryBudgetExceeded {
                available_bytes: 3,
                ..
            }
        ));
        assert!(read.open_table(GENERATIONS).is_err());
        Ok(())
    })
    .unwrap();
}

#[test]
#[serial_test::serial(vector_journal_cleanup_pause)]
fn source_refusal_after_durable_publication_keeps_built_count_and_cleanup_detail() {
    // The existing cleanup pause is process-wide. Isolate this case so a
    // parallel sibling maintenance fixture cannot reach its cut first.
    const CHILD: &str = "CONTEXTDB_E9_SOURCE_PAGE_CLEANUP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "database::vector_source_page_admission::tests::source_refusal_after_durable_publication_keeps_built_count_and_cleanup_detail",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success(), "isolated post-publication proof failed");
        return;
    }
    use super::super::*;
    use crate::persistence::{VectorPartitionBaseGenerationRecord, VectorPartitionGraphBytes};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("published-source-refusal.db");
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
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let old = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    db.close().unwrap();
    drop(db);
    // A superseded record remains a genuine uncached source owner.
    // Its authenticated graph bytes need not be a serving graph: repair only
    // validates identity/checksums before removing this unselected evidence.
    {
        let file = redb::Database::open(&path).unwrap();
        let write = file.begin_write().unwrap();
        {
            let mut table = write.open_table(GENERATIONS).unwrap();
            let (key, value) = table.iter().unwrap().next().unwrap().unwrap();
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
            let mut header = encoded[..header_len].to_vec();
            drop(value);
            drop(key);
            stale.generation_id = 0;
            stale.graph = VectorPartitionGraphBytes::verified(vec![7; GRAPH_BYTES]);
            let payload =
                bincode::serde::encode_to_vec(stale, bincode::config::standard()).unwrap();
            header[header_len - 8..].copy_from_slice(&(payload.len() as u64).to_be_bytes());
            header.extend_from_slice(&payload);
            let checksum = blake3::hash(&header);
            header.extend_from_slice(checksum.as_bytes());
            table
                .insert(stale_key.as_slice(), header.as_slice())
                .unwrap();
        }
        write.commit().unwrap();
    }
    let db = Arc::new(Database::open(&path).unwrap());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key
            )
            .unwrap()
            .unwrap(),
        old
    );
    let query = "SELECT id FROM items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3";
    assert_eq!(db.execute(query, &HashMap::new()).unwrap().rows.len(), 3);
    // This uncovered journal entry is ignored by the initial retry, then
    // covered by T1 and selected by repaired-generation cleanup.
    db.execute("INSERT INTO items VALUES (16, '[1,0,0]')", &HashMap::new())
        .unwrap();
    assert!(
        db.vector_store_for_test()
            .quarantine_partition_route(&partition, VectorRouteQuarantineReason::CorruptBase)
    );
    let pause = db.__arm_one_shot_vector_journal_cleanup_pause_for_test();
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(pause.wait_until_reached(Duration::from_secs(20)));
    let published = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    assert!(published.base.generation_id > old.base.generation_id);
    // This cut is after T1 and the prepared-owner transition, immediately
    // before journal removal and the stale-generation scan. Tiny metadata
    // fits; the genuine uncached oversized source extent does not.
    let limit = db.accountant().usage().used + 1024 * 1024;
    db.accountant().set_budget(Some(limit)).unwrap();
    pause.release();
    let report = worker.join().unwrap().unwrap();
    assert_eq!(report.vector.built_partitions, 1, "{report:?}");
    let details = report.vector.first_failure_details.unwrap();
    assert_eq!(details.operation, "read_vector_generation_source_page");
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    assert!(details.message.contains("durable"));
    assert!(details.requested_bytes.unwrap() > GRAPH_BYTES as u64);
    assert_eq!(details.budget_limit_bytes, Some(limit as u64));
    assert!(
        db.accountant().usage().used <= limit,
        "no successful earlier source extent remains charged"
    );
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant().underflow_count_for_test(), 0);
    assert_eq!(
        db.persistence
            .as_ref()
            .unwrap()
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key
            )
            .unwrap()
            .unwrap(),
        published
    );
    // Failed repair cleanup keeps the old route quarantined. Recovery uses
    // the already-published replacement rather than reverting the catalog.
    assert!(
        db.vector_store_for_test()
            .partition_route_quarantine(&partition)
            .is_some()
    );
    db.accountant().set_budget(None).unwrap();
    let retry = db.run_maintenance_cycle().unwrap();
    assert!(retry.vector.first_failure.is_none(), "{retry:?}");
    assert!(
        db.vector_store_for_test()
            .partition_route_quarantine(&partition)
            .is_none()
    );
    assert_eq!(db.execute(query, &HashMap::new()).unwrap().rows.len(), 3);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant().underflow_count_for_test(), 0);
    let ready = db
        .persistence
        .as_ref()
        .unwrap()
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    assert!(ready.base.generation_id >= published.base.generation_id);
    db.close().unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        reopened
            .persistence
            .as_ref()
            .unwrap()
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key
            )
            .unwrap()
            .unwrap(),
        ready
    );
    assert_eq!(
        reopened.execute(query, &HashMap::new()).unwrap().rows.len(),
        3
    );
    reopened.close().unwrap();
}

/// Foreground projection uses an exact selected-stream count through execute.
/// Its maintained-count lookup occurs during query preparation, before the
/// bounded row reader installs its own page callback. r17 only restores an
/// image-page refusal after its enclosing visitor returns, so the public
/// query result observed inside that visitor loses the typed payload.
#[test]
fn foreground_projection_query_preserves_page_refusal_before_the_admission_scope_returns() {
    use crate::{Database, DatabaseOpenOptions, MaintenancePolicy};
    use contextdb_core::Value;
    use std::collections::HashMap;
    const BODY_BYTES: usize = 9 * 1024 * 1024;
    const PAGE_BUDGET: usize = 1024 * 1024;
    const QUERY: &str = "SELECT COUNT(*) AS event_count FROM event_search_source_events WHERE context_id = $context_id AND generation_id = $generation_id AND connector_id = $connector AND session_ref = $session AND source_instance_ref = $source";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("projection-page-refusal.db");
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(512 * 1024 * 1024),
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options.clone()).unwrap();
    db.execute(
        "CREATE TABLE admission_driver (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO admission_driver VALUES (1)", &HashMap::new())
        .unwrap();
    // The nonunique prefix is the projection's selected-stream count key.
    // A long session makes its physical extent exceed the fixed page cache.
    db.execute(
        "CREATE TABLE event_search_source_events (id INT PRIMARY KEY, context_id INT, generation_id INT, connector_id TEXT, source_instance_ref TEXT, session_ref TEXT, UNIQUE (context_id, generation_id, connector_id, source_instance_ref, session_ref, id)) MAINTAIN COUNT (context_id, generation_id, connector_id, source_instance_ref, session_ref)",
        &HashMap::new(),
    )
    .unwrap();
    let params = HashMap::from([
        ("context_id".into(), Value::Int64(1)),
        ("generation_id".into(), Value::Int64(1)),
        ("connector".into(), Value::Text("connector".into())),
        ("source".into(), Value::Text("source".into())),
        ("session".into(), Value::Text("p".repeat(BODY_BYTES))),
    ]);
    db.execute(
        "INSERT INTO event_search_source_events VALUES (1, $context_id, $generation_id, $connector, $source, $session)",
        &params,
    )
    .unwrap();
    db.close().unwrap();
    drop(db);
    // Reopen to remove writer buffers. A complete maintained-count extent
    // larger than the cache must still be admitted before it is faulted.
    let db = Database::open_with_options(&path, options).unwrap();
    let persistence = db.persistence.as_ref().unwrap();
    let pages = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        PAGE_BUDGET,
    ));
    let mut observed = None;
    let mut available_at_refusal = None;
    let outer = persistence.visit_relational_table_with_admission(
        "admission_driver",
        &crate::memory_accounting::AccountantHandle::from_host(pages.clone()),
        |_| {
            // Same public API and statement shape as selected_events_through
            // in cg foreground event-search projection. Assert its result
            // before r17's outer side-channel repair can substitute it.
            observed = Some(db.execute(QUERY, &params).unwrap_err());
            available_at_refusal = Some(PAGE_BUDGET - pages.usage().used);
            Ok(())
        },
    );
    let error = observed.expect("the admitted driver calls the foreground query");
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded {
            subsystem, operation, requested_bytes, available_bytes,
            budget_limit_bytes, ..
        } if subsystem == "sync"
            && operation == "stage_received_unit_row_image"
            && *requested_bytes > BODY_BYTES
            && *available_bytes == available_at_refusal.unwrap()
            && *budget_limit_bytes == PAGE_BUDGET),
        "{error:?}"
    );
    outer.unwrap();
    assert_eq!(
        pages.usage().used,
        0,
        "the enclosing image page owner releases its charge"
    );
    let retry = db.execute(QUERY, &params).unwrap();
    assert_eq!(retry.rows[0][0], Value::Int64(1));
    db.close().unwrap();
}

#[test]
fn source_page_callback_returns_original_refusal_in_storage_carrier() {
    let (_directory, db) = fixture(GRAPH_BYTES);
    let read = db.begin_read().unwrap();
    drop(read.open_table(GENERATIONS).unwrap());
    let accountant = Arc::new(MemoryAccountant::with_budget(CACHE_BYTES + 1024));
    accountant.try_allocate(CACHE_BYTES).unwrap();
    let result = with_source_page_admission(accountant.clone().into(), || {
        let table = read.open_table(GENERATIONS).unwrap();
        let error = table.get(b"old-authoritative".as_slice()).err().unwrap();
        let redb::StorageError::AdmissionRefused(payload) = error else {
            panic!("source-page admission crossed storage as {error:?}");
        };
        let error = payload.downcast::<Error>().unwrap();
        assert!(matches!(*error, Error::MemoryBudgetExceeded {
            subsystem, operation, requested_bytes, available_bytes: 1024,
            budget_limit_bytes, ..
        } if subsystem == "vector_maintenance"
            && operation == "read_vector_generation_source_page"
            && requested_bytes > GRAPH_BYTES
            && budget_limit_bytes == CACHE_BYTES + 1024));
        Ok(())
    });
    result.unwrap();
    assert_eq!(accountant.usage().used, CACHE_BYTES);
    accountant.release(CACHE_BYTES);
}
