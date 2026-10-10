//! Deterministic stops at the captured inventory and committed retry boundaries.
use super::*;
use std::cell::RefCell;

// The synchronous maintenance driver stays on the test's thread. No sibling
// fixture can consume this hook, and the guard restores it on return or panic.
type BoundaryHook = Box<dyn FnMut(Option<&VectorPartitionGenerationCatalogRecord>)>;
thread_local! {
    static BOUNDARY_HOOK: RefCell<Option<BoundaryHook>> = const { RefCell::new(None) };
}

struct BoundaryHookGuard(Option<BoundaryHook>);

impl Drop for BoundaryHookGuard {
    fn drop(&mut self) {
        BOUNDARY_HOOK.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

fn on_boundary(
    hook: impl FnMut(Option<&VectorPartitionGenerationCatalogRecord>) + 'static,
) -> BoundaryHookGuard {
    BoundaryHookGuard(BOUNDARY_HOOK.with(|slot| slot.replace(Some(Box::new(hook)))))
}

pub(super) fn after_inventory_capture_for_test() {
    BOUNDARY_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(None);
        }
    });
}

pub(super) fn after_retry_for_test(catalog: &VectorPartitionGenerationCatalogRecord) {
    BOUNDARY_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(Some(catalog));
        }
    });
}

fn pending_cleanup_fixture() -> (
    tempfile::TempDir,
    PathBuf,
    Arc<Database>,
    Vec<VectorPartitionRef>,
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal-retry-stop.db");
    let db = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..DatabaseOpenOptions::default()
            },
        )
        .unwrap(),
    );
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 3 SEARCH_MODE INDEXED HNSW (M = 4, EF_CONSTRUCTION = 64))",
        &HashMap::new(),
    ).unwrap();
    let partitions = (0..3)
        .map(|scope| {
            VectorPartitionRef::new(
                VectorIndexRef::new("items", "v"),
                VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    for scope in 0..3 {
        for point in 0..16 {
            db.execute(
                "INSERT INTO items VALUES ($id, $scope, '[1,0,0]')",
                &HashMap::from([
                    ("id".into(), Value::Int64(scope * 16 + point)),
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
    db.execute("UPDATE items SET v = '[0,1,0]'", &HashMap::new())
        .unwrap();
    let crashes = partitions
        .iter()
        .map(|partition| {
            db.__arm_vector_journal_truncation_crash_for_test(
                partition,
                VectorJournalTruncationPhaseForTest::CatalogDurableBeforeCoveredJournalDelete,
            )
        })
        .collect::<Vec<_>>();
    let published = db.run_maintenance_cycle().unwrap();
    assert_eq!(published.vector.built_partitions, 3, "{published:?}");
    assert!(published.vector.first_failure.is_some());
    drop(crashes);
    for partition in &partitions {
        assert!(
            !db.persistence
                .as_ref()
                .unwrap()
                .debug_vector_partition_journal_for_test(partition)
                .unwrap()
                .1
                .is_empty()
        );
    }
    (directory, path, db, partitions)
}

#[test]
fn cancellation_after_inventory_capture_skips_all_cleanup_and_releases_reservations() {
    let (_directory, _path, db, partitions) = pending_cleanup_fixture();
    let persistence = db.persistence.as_ref().unwrap();
    let before = partitions
        .iter()
        .map(|partition| {
            persistence
                .debug_vector_partition_journal_for_test(partition)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let baseline = db.vector_memory_ownership_receipt_for_test();
    let stop = Arc::new(AtomicBool::new(false));
    let mut context = db.maintenance_context();
    context.worker_stop = Some(Arc::clone(&stop));
    let captured_workspace = Arc::new(AtomicUsize::new(0));
    let retry_count = Arc::new(AtomicUsize::new(0));
    let observed_db = Arc::clone(&db);
    let observed_workspace = Arc::clone(&captured_workspace);
    let observed_retries = Arc::clone(&retry_count);
    let observed_stop = Arc::clone(&stop);
    let hook = on_boundary(move |catalog| {
        if catalog.is_some() {
            observed_retries.fetch_add(1, Ordering::SeqCst);
        } else {
            observed_workspace.store(
                observed_db
                    .vector_memory_ownership_receipt_for_test()
                    .temporary_workspace,
                Ordering::SeqCst,
            );
            observed_stop.store(true, Ordering::SeqCst);
        }
    });
    *db.last_maintenance_cycle_at.lock() = None;
    let fixture_operation_reader_1 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let result = context.run_cycle(CurrencyGate::Always, fixture_operation_reader_1.clone());
    drop(hook);
    assert!(matches!(result, Err(Error::ReadCancelled)), "{result:?}");
    assert!(captured_workspace.load(Ordering::SeqCst) > 0);
    assert_eq!(retry_count.load(Ordering::SeqCst), 0);
    assert!(db.last_maintenance_cycle_at.lock().is_none());
    for (partition, expected) in partitions.iter().zip(before) {
        assert_eq!(
            persistence
                .debug_vector_partition_journal_for_test(partition)
                .unwrap(),
            expected
        );
    }
    let released = db.vector_memory_ownership_receipt_for_test();
    assert_eq!(released.temporary_workspace, 0);
    assert_eq!(
        released.non_vector_total, baseline.non_vector_total,
        "catalog inventory and route reservations also release"
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    drop(context);
    db.close().unwrap();
}

#[test]
fn cancellation_after_committed_journal_retry_leaves_later_partitions_untouched_on_restart() {
    let (_directory, path, db, partitions) = pending_cleanup_fixture();
    let persistence = db.persistence.as_ref().unwrap();
    let catalogs = partitions
        .iter()
        .map(|partition| {
            persistence
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let before = partitions
        .iter()
        .map(|partition| {
            persistence
                .debug_vector_partition_journal_for_test(partition)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let serving_before = partitions
        .iter()
        .map(|partition| db.vector_store.partition_graph_generation_status(partition))
        .collect::<Vec<_>>();
    let baseline = db.vector_memory_ownership_receipt_for_test();
    let baseline_writer = db.writer_memory_ownership_receipt_for_test();
    let stop = Arc::new(AtomicBool::new(false));
    let mut context = db.maintenance_context();
    context.worker_stop = Some(Arc::clone(&stop));
    let completed = Arc::new(Mutex::new(Vec::new()));
    let captured_workspace = Arc::new(AtomicUsize::new(0));
    let observed_db = Arc::clone(&db);
    let observed_completed = Arc::clone(&completed);
    let observed_workspace = Arc::clone(&captured_workspace);
    let hook = on_boundary(move |catalog| {
        if let Some(catalog) = catalog {
            observed_completed.lock().push(VectorPartitionRef::new(
                catalog.index.clone(),
                catalog.partition_key.clone(),
            ));
            stop.store(true, Ordering::SeqCst);
        } else {
            observed_workspace.store(
                observed_db
                    .vector_memory_ownership_receipt_for_test()
                    .temporary_workspace,
                Ordering::SeqCst,
            );
        }
    });
    *db.last_maintenance_cycle_at.lock() = None;
    let fixture_operation_reader_2 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let result = context.run_cycle(CurrencyGate::Always, fixture_operation_reader_2.clone());
    drop(hook);
    assert!(matches!(result, Err(Error::ReadCancelled)), "{result:?}");
    assert!(captured_workspace.load(Ordering::SeqCst) > 0);
    assert!(db.last_maintenance_cycle_at.lock().is_none());
    let completed = completed.lock().clone();
    assert_eq!(
        completed.len(),
        1,
        "the stop follows exactly one committed retry"
    );
    let cleaned = &completed[0];
    for (position, partition) in partitions.iter().enumerate() {
        let journal = persistence
            .debug_vector_partition_journal_for_test(partition)
            .unwrap();
        if partition == cleaned {
            assert!(
                journal.1.is_empty(),
                "the preceding cleanup remains committed"
            );
        } else {
            assert_eq!(
                journal, before[position],
                "later journal partitions are untouched"
            );
            assert_eq!(
                db.vector_store.partition_graph_generation_status(partition),
                serving_before[position]
            );
        }
        assert_eq!(
            persistence
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
                .unwrap(),
            catalogs[position]
        );
    }
    let released = db.vector_memory_ownership_receipt_for_test();
    assert_eq!(released.temporary_workspace, 0);
    assert_eq!(
        released.non_vector_total,
        baseline.non_vector_total
            + 2 * MemoryAccountant::writer_vector_adapter_type_layout_for_test().size(),
        "only the newly installed paged change's standing adapters remain"
    );
    let released_writer = db.writer_memory_ownership_receipt_for_test();
    assert_eq!(released_writer.reconciliation_gap, 0);
    assert_eq!(
        released_writer.writer_adapter_controls - baseline_writer.writer_adapter_controls,
        2 * MemoryAccountant::writer_vector_adapter_type_layout_for_test().size()
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    drop(context);
    db.close().unwrap();
    drop(db);

    // Inspect durable state before writer startup's own idempotent retry can
    // advance the remaining routes. The completed prefix survived the stop.
    let restarted = RedbPersistence::open_with_cache(&path, 0).unwrap();
    for (position, partition) in partitions.iter().enumerate() {
        let journal = restarted
            .debug_vector_partition_journal_for_test(partition)
            .unwrap();
        if partition == cleaned {
            assert!(journal.1.is_empty());
        } else {
            assert_eq!(journal, before[position]);
        }
    }
    restarted.close();
    drop(restarted);
    let reopened = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    for (position, partition) in partitions.iter().enumerate() {
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
            catalogs[position]
        );
        let rows = reopened.execute(
            "SELECT id FROM items WHERE scope = $scope ORDER BY v <=> [0,1,0] USE VECTOR INDEXED LIMIT 3",
            &HashMap::from([("scope".into(), Value::Int64(position as i64))]),
        ).unwrap().rows;
        assert_eq!(
            rows.len(),
            3,
            "the published route remains usable after restart"
        );
        assert!(
            reopened
                .persistence
                .as_ref()
                .unwrap()
                .debug_vector_partition_journal_for_test(partition)
                .unwrap()
                .1
                .is_empty()
        );
    }
    assert_eq!(
        reopened
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(reopened.accountant.underflow_count_for_test(), 0);
    reopened.close().unwrap();
}

// The cut is immediately before normal T2's first checkpoint, after T1 has
// made the complete replacement catalog durable. It is separate from retry
// hooks so the fixture cannot stop during the cycle's initial cleanup pass.
type PublishedCleanupHook = Box<dyn FnMut(&VectorPartitionGenerationCatalogRecord)>;
thread_local! {
    static PUBLISHED_CLEANUP_HOOK: RefCell<Option<PublishedCleanupHook>> = const { RefCell::new(None) };
}

struct PublishedCleanupHookGuard(Option<PublishedCleanupHook>);

impl Drop for PublishedCleanupHookGuard {
    fn drop(&mut self) {
        PUBLISHED_CLEANUP_HOOK.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}

fn on_published_cleanup_boundary(
    hook: impl FnMut(&VectorPartitionGenerationCatalogRecord) + 'static,
) -> PublishedCleanupHookGuard {
    PublishedCleanupHookGuard(
        PUBLISHED_CLEANUP_HOOK.with(|slot| slot.replace(Some(Box::new(hook)))),
    )
}

pub(super) fn before_published_cleanup_for_test(catalog: &VectorPartitionGenerationCatalogRecord) {
    PUBLISHED_CLEANUP_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(catalog);
        }
    });
}

#[cfg(feature = "test-seams")]
#[test]
fn publication_interruption_at_normal_t2_is_terminal_and_preserves_the_restart_frontier() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("published-cleanup-interruption.db");
    let db = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..DatabaseOpenOptions::default()
            },
        )
        .unwrap(),
    );
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(3) PARTITION_KEY (scope) MAX_PARTITIONS 3 SEARCH_MODE INDEXED HNSW (M = 4, EF_CONSTRUCTION = 64))",
        &HashMap::new(),
    ).unwrap();
    let partitions = (0..3)
        .map(|scope| {
            VectorPartitionRef::new(
                VectorIndexRef::new("items", "v"),
                VectorPartitionKey::from_values(&[Value::Int64(scope)]).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    for scope in 0..3 {
        for point in 0..16 {
            db.execute(
                "INSERT INTO items VALUES ($id, $scope, '[1,0,0]')",
                &HashMap::from([
                    ("id".into(), Value::Int64(scope * 16 + point)),
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
    db.execute("UPDATE items SET v = '[0,1,0]'", &HashMap::new())
        .unwrap();
    let persistence = db.persistence.as_ref().unwrap();
    let catalogs_before = partitions
        .iter()
        .map(|partition| {
            persistence
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let journals_before = partitions
        .iter()
        .map(|partition| {
            persistence
                .debug_vector_partition_journal_for_test(partition)
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(journals_before.iter().all(|journal| !journal.1.is_empty()));
    let serving_before = partitions
        .iter()
        .map(|partition| db.vector_store.partition_graph_generation_status(partition))
        .collect::<Vec<_>>();
    let baseline = db.vector_memory_ownership_receipt_for_test();
    let retirement_passes = db.vector_partition_retirement_pass_count_for_test();
    let publication = Arc::new(Mutex::new(Vec::new()));
    let captured_workspace = Arc::new(AtomicUsize::new(0));
    let observed_db = Arc::clone(&db);
    let observed_publication = Arc::clone(&publication);
    let observed_workspace = Arc::clone(&captured_workspace);
    let hook = on_published_cleanup_boundary(move |catalog| {
        assert_eq!(
            observed_db
                .persistence
                .as_ref()
                .unwrap()
                .load_vector_partition_generation_catalog_for(
                    &catalog.index,
                    &catalog.partition_key,
                )
                .unwrap()
                .as_ref(),
            Some(catalog),
            "the interruption follows the actual durable T1 publication"
        );
        observed_publication.lock().push(catalog.clone());
        observed_workspace.store(
            observed_db
                .vector_memory_ownership_receipt_for_test()
                .temporary_workspace,
            Ordering::SeqCst,
        );
        // Set the real handle/context stop state; normal T2's own checkpoint
        // must produce the typed terminal error consumed by run_cycle.
        observed_db
            .publication_interrupted
            .store(true, Ordering::SeqCst);
    });
    *db.last_maintenance_cycle_at.lock() = None;
    let context = db.maintenance_context();
    let fixture_operation_reader_3 = context
        .read_request_provider
        .request_budget()
        .expect("admit the actual fixture reader");

    let result = context.run_cycle(CurrencyGate::Always, fixture_operation_reader_3.clone());
    drop(hook);
    assert!(
        matches!(result, Err(Error::PublicationInterrupted)),
        "{result:?}"
    );
    assert!(db.publication_interrupted.load(Ordering::SeqCst));
    assert!(captured_workspace.load(Ordering::SeqCst) > 0);
    assert!(db.last_maintenance_cycle_at.lock().is_none());
    assert_eq!(
        db.vector_partition_retirement_pass_count_for_test(),
        retirement_passes,
        "terminal publication interruption skips the closing retirement pass"
    );
    let publication = publication.lock().clone();
    assert_eq!(
        publication.len(),
        1,
        "no later candidate reaches durable publication"
    );
    let published = &publication[0];
    let published_partition =
        VectorPartitionRef::new(published.index.clone(), published.partition_key.clone());
    let published_position = partitions
        .iter()
        .position(|partition| partition == &published_partition)
        .unwrap();
    assert!(
        published.change.unwrap().generation_id
            > catalogs_before[published_position].base.generation_id,
        "the fixture publishes a new change generation after its existing base"
    );
    for (position, partition) in partitions.iter().enumerate() {
        assert_eq!(
            persistence
                .debug_vector_partition_journal_for_test(partition)
                .unwrap()
                .1,
            journals_before[position].1,
            "T2's first checkpoint refuses before deleting any journal prefix"
        );
        let expected_catalog = if partition == &published_partition {
            published
        } else {
            assert_eq!(
                db.vector_store.partition_graph_generation_status(partition),
                serving_before[position],
                "later candidates retain their serving state"
            );
            &catalogs_before[position]
        };
        assert_eq!(
            persistence
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
                .as_ref(),
            Some(expected_catalog),
            "only the preceding durable publication changes its catalog"
        );
    }
    let released = db.vector_memory_ownership_receipt_for_test();
    assert_eq!(released.temporary_workspace, 0);
    assert_eq!(
        released.non_vector_total, baseline.non_vector_total,
        "candidate inventory and route reservations also release"
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    assert!(matches!(
        db.execute("SELECT id FROM items", &HashMap::new()),
        Err(Error::PublicationInterrupted)
    ));
    drop(context);
    db.close().unwrap();
    drop(db);

    // Inspect the stopped frontier before startup's idempotent T2 retry.
    let restarted = RedbPersistence::open_with_cache(&path, 0).unwrap();
    for (position, partition) in partitions.iter().enumerate() {
        let expected_catalog = if partition == &published_partition {
            published
        } else {
            &catalogs_before[position]
        };
        assert_eq!(
            restarted
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
                .as_ref(),
            Some(expected_catalog),
            "the preceding publication survives restart"
        );
        assert_eq!(
            restarted
                .debug_vector_partition_journal_for_test(partition)
                .unwrap()
                .1,
            journals_before[position].1,
            "later candidates and every uncommitted T2 prefix remain untouched"
        );
    }
    restarted.close();
    drop(restarted);
    let reopened = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap();
    assert!(!reopened.publication_interrupted.load(Ordering::SeqCst));
    for (position, partition) in partitions.iter().enumerate() {
        let expected_catalog = if partition == &published_partition {
            published
        } else {
            &catalogs_before[position]
        };
        assert_eq!(
            reopened
                .persistence
                .as_ref()
                .unwrap()
                .load_vector_partition_generation_catalog_for(
                    &partition.index,
                    &partition.partition_key,
                )
                .unwrap()
                .as_ref(),
            Some(expected_catalog),
            "startup installs the durable frontier without rebuilding candidates"
        );
        let rows = reopened.execute(
            "SELECT id FROM items WHERE scope = $scope ORDER BY v <=> [0,1,0] USE VECTOR INDEXED LIMIT 3",
            &HashMap::from([("scope".into(), Value::Int64(position as i64))]),
        ).unwrap().rows;
        assert_eq!(
            rows.len(),
            3,
            "every restart route still answers indexed queries"
        );
        if partition == &published_partition {
            assert!(
                reopened
                    .persistence
                    .as_ref()
                    .unwrap()
                    .debug_vector_partition_journal_for_test(partition)
                    .unwrap()
                    .1
                    .is_empty(),
                "startup retries the preceding publication's covered journal"
            );
        }
    }
    assert_eq!(
        reopened
            .vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(reopened.accountant.underflow_count_for_test(), 0);
    reopened.close().unwrap();
}
