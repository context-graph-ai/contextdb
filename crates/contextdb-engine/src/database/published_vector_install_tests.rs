//! Genuine installation admission refusal after successful T1 and T2.
use super::*;

// Target one accountant so parallel maintenance fixtures cannot consume the cut.
static BEFORE_INSTALL: OnceLock<Mutex<Option<(usize, Arc<ApplyPhasePause>)>>> = OnceLock::new();

fn arm_before_install(accountant: &Arc<MemoryAccountant>) -> ApplyPhasePauseGuard {
    let pause = Arc::new(ApplyPhasePause::new());
    let guard = ApplyPhasePauseGuard::arm_for_test(Arc::clone(&pause));
    *BEFORE_INSTALL.get_or_init(|| Mutex::new(None)).lock() =
        Some((Arc::as_ptr(accountant) as usize, pause));
    guard
}

pub(super) fn maybe_pause_before_install(accountant: &Arc<MemoryAccountant>) {
    let pause = BEFORE_INSTALL.get_or_init(|| Mutex::new(None));
    let selected = {
        let mut slot = pause.lock();
        if slot
            .as_ref()
            .is_some_and(|(target, _)| *target == Arc::as_ptr(accountant) as usize)
        {
            slot.take().map(|(_, pause)| pause)
        } else {
            None
        }
    };
    if let Some(pause) = selected {
        pause.maybe_pause();
    }
}

#[test]
fn install_refusal_after_successful_t2_keeps_publication_and_retries_without_journal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("published-install-refusal.db");
    let db = Arc::new(Database::open(&path).unwrap());
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED HNSW (M = 8, EF_CONSTRUCTION = 64))",
        &HashMap::new(),
    )
    .unwrap();
    let insert = |id| {
        db.execute(
            "INSERT INTO items VALUES ($id, '[1,0,0]')",
            &HashMap::from([("id".into(), Value::Int64(id))]),
        )
        .unwrap();
    };
    for id in 0..16 {
        insert(id);
    }
    assert_eq!(
        db.run_maintenance_cycle().unwrap().vector.built_partitions,
        1
    );
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("items", "v"),
        VectorPartitionKey::unpartitioned(),
    );
    let persistence = db.persistence.as_ref().unwrap();
    let old = persistence
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    assert_eq!(
        db.vector_store
            .partition_graph_generation_status(&partition)
            .unwrap()
            .base
            .unwrap()
            .generation_id,
        old.base.generation_id
    );
    // A declared topology change selects a replacement base, retaining the
    // old complete resident chain until the new load is admitted and verified.
    for id in 16..80 {
        insert(id);
    }
    db.execute(
        "ALTER TABLE items ALTER COLUMN v SET HNSW (M = 12)",
        &HashMap::new(),
    )
    .unwrap();
    let pause = arm_before_install(&db.accountant);
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || worker_db.run_maintenance_cycle());
    assert!(pause.wait_until_reached(Duration::from_secs(20)));
    let published = persistence
        .load_vector_partition_generation_catalog_for(&partition.index, &partition.partition_key)
        .unwrap()
        .unwrap();
    assert!(published.base.generation_id > old.base.generation_id);
    assert!(
        published.change.is_none(),
        "fixture requires a replacement base"
    );
    let (covered, journal) = persistence
        .debug_vector_partition_journal_for_test(&partition)
        .unwrap();
    assert_eq!(covered, published.base.covered_lsn);
    assert!(journal.is_empty(), "T2 must finish before the install cut");
    // At this exact cut every surviving owner is charged. Zero additional
    // headroom makes the real generation loader's next admission refuse.
    let limit = db.accountant.usage().used;
    db.accountant.set_budget(Some(limit)).unwrap();
    pause.release();
    let report = worker.join().unwrap().unwrap();
    assert_eq!(report.vector.built_partitions, 1, "{report:?}");
    assert_eq!(report.vector.built_indexes, 1, "{report:?}");
    let details = report.vector.first_failure_details.unwrap();
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    // The durable publication and old serving route below establish ordering;
    // an internal admission stage name is not the contract.
    assert!(details.requested_bytes.unwrap() > details.available_bytes.unwrap());
    assert_eq!(details.budget_limit_bytes, Some(limit as u64));
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    assert_eq!(
        persistence
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )
            .unwrap()
            .unwrap(),
        published
    );
    let status = db
        .vector_store
        .partition_graph_generation_status(&partition)
        .unwrap();
    assert_eq!(status.base.unwrap().generation_id, old.base.generation_id);
    assert!(
        status.base_resident,
        "old serving graph survives refused install"
    );
    assert!(
        published.base.generation_id > status.base.unwrap().generation_id,
        "the durable catalog retains the refused installation for retry"
    );
    assert!(
        db.vector_store
            .partition_route_quarantine(&partition)
            .is_none()
    );
    assert!(
        persistence
            .debug_vector_partition_journal_for_test(&partition)
            .unwrap()
            .1
            .is_empty()
    );
    db.accountant.set_budget(None).unwrap();
    let retry = db.run_maintenance_cycle().unwrap();
    assert!(retry.vector.first_failure.is_none(), "{retry:?}");
    assert_eq!(
        retry.vector.built_partitions, 0,
        "install retry must not rebuild"
    );
    assert_eq!(
        db.vector_store
            .partition_graph_generation_status(&partition)
            .unwrap()
            .base
            .unwrap()
            .generation_id,
        published.base.generation_id
    );
    let query = "SELECT id FROM items ORDER BY v <=> [1,0,0] USE VECTOR INDEXED LIMIT 3";
    assert_eq!(db.execute(query, &HashMap::new()).unwrap().rows.len(), 3);
    assert_eq!(
        db.vector_memory_ownership_receipt_for_test()
            .temporary_workspace,
        0
    );
    assert_eq!(db.accountant.underflow_count_for_test(), 0);
    db.close().unwrap();
    drop(db);
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(
        reopened.execute(query, &HashMap::new()).unwrap().rows.len(),
        3
    );
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
        published
    );
    reopened.close().unwrap();
}
