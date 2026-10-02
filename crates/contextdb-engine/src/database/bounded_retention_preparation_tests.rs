use super::*;
include!("durable_test_observations.rs");
use crate::memory_accounting::{take_budget_ledger_for_test, watch_budget_ledger_for_test};

fn history_fixture(db: &Database) {
    db.execute(
        "CREATE TABLE history (id INT PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    let payload = "captured-event-body".repeat(65536);
    assert!(payload.len() * 128 >= 100 * 1024 * 1024);
    eprintln!("history fixture payload bytes: {}", payload.len() * 128);
    for id in 0..128 {
        db.execute(
            "INSERT INTO history VALUES ($id, $payload)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("payload".into(), Value::Text(payload.clone())),
            ]),
        )
        .unwrap();
    }
}

#[test]
fn empty_and_not_due_retention_do_not_prepare_unrelated_history() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    history_fixture(&db);
    crate::work_ledger::install_work_ledger_schema(&db).unwrap();
    assert_eq!(
        db.relational_store
            .table_meta
            .read()
            .values()
            .filter(|meta| meta.default_ttl_seconds.is_some())
            .count(),
        1
    );
    assert_eq!(
        db.table_meta("work_inputs").unwrap().default_ttl_seconds,
        Some(7 * 24 * 3600)
    );
    for not_due in [false, true] {
        if not_due {
            db.execute("INSERT INTO work_inputs (input_key, job_id, seq, payload) VALUES ('input', 'job', 0, 'not due')", &HashMap::new()).unwrap();
        }
        let used = db.accountant.usage().used;
        db.set_memory_limit(Some(used + 32 * 1024)).unwrap();
        db.accountant.reset_high_water_for_test();
        watch_budget_ledger_for_test();
        let result = db.run_pruning_cycle_checked();
        let ledger = take_budget_ledger_for_test();
        let report = result.unwrap();
        assert_eq!(report.pruned_rows, 0);
        assert_eq!(
            ledger
                .iter()
                .filter(|e| e.what == "retention/prepare_expiry")
                .map(|e| e.bytes)
                .max()
                .unwrap_or(0),
            0
        );
        assert!(db.accountant.high_water_for_test() - used < 32 * 1024);
        assert_eq!(db.relational_store.tables.read()["history"].len(), 128);
        db.set_memory_limit(None).unwrap();
    }
}

#[test]
fn due_retention_next_to_history_is_charged_atomic_and_snapshot_safe() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("retention.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    history_fixture(&db);
    db.execute("CREATE TABLE retained (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    for id in 0..3 {
        db.execute(
            "INSERT INTO retained VALUES ($id, $expires)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("expires".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
    }
    let snapshot = db.snapshot();
    let pin = db.pin_snapshot(snapshot);
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 256 * 1024)).unwrap();
    db.accountant.reset_high_water_for_test();
    watch_budget_ledger_for_test();
    let report = db.run_pruning_cycle_checked().unwrap();
    let ledger = take_budget_ledger_for_test();
    assert_eq!(report.pruned_rows, 0);
    assert_eq!(report.rows_deferred_for_readers, 3);
    let expiry_bytes = ledger
        .iter()
        .filter(|e| e.what == "retention/prepare_expiry")
        .map(|e| e.bytes)
        .sum::<usize>();
    assert!(
        expiry_bytes > 0 && expiry_bytes < 16 * 1024,
        "expiry workspace: {expiry_bytes}"
    );
    assert!(db.accountant.high_water_for_test() - used < 256 * 1024);
    assert!(
        durable_observation::rows(&db, "retained")
            .iter()
            .all(|row| row.deleted_tx.is_some())
    );
    assert_eq!(
        durable_observation::rows(&db, "retained")
            .iter()
            .filter(|row| row.visible_at(snapshot))
            .count(),
        3
    );
    drop(pin);
    let report = db.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 3);
    assert!(durable_observation::version_count(&db, "retained") == 0);
    assert_eq!(durable_observation::version_count(&db, "history"), 128);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(durable_observation::version_count(&reopened, "retained") == 0);
    assert_eq!(
        durable_observation::version_count(&reopened, "history"),
        128
    );
    reopened.close().unwrap();
}

#[test]
fn impossible_expiry_limit_preserves_old_state_with_typed_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("impossible-retention.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options.clone()).unwrap();
    db.execute("CREATE TABLE retained (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    for id in 0..3 {
        db.execute(
            "INSERT INTO retained VALUES ($id, $expires)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("expires".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
    }
    let snapshot = db.snapshot();
    let used = db.accountant.usage().used;
    // Refuse before publication after part of the candidate set is prepared.
    db.accountant.set_budget(Some(used + 800)).unwrap();
    let failure = db
        .maintenance_context()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap_err();
    match failure {
        Error::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        } => {
            let current_bytes = budget_limit_bytes - available_bytes;
            assert!(current_bytes >= used);
            assert!(!hint.is_empty());
            eprintln!(
                "typed refusal: requested={requested_bytes} available={available_bytes} current={current_bytes} limit={budget_limit_bytes}"
            );
            assert_eq!(subsystem, "retention");
            assert_eq!(operation, "prepare_expiry");
            assert!(requested_bytes > available_bytes);
            assert!(available_bytes < 800);
            assert!(budget_limit_bytes - available_bytes > used);
            assert_eq!(budget_limit_bytes, used + 800);
        }
        other => panic!("expected typed memory refusal: {other:?}"),
    }
    assert_eq!(db.accountant.usage().used, used);
    assert!(
        db.relational_store.tables.read()["retained"]
            .iter()
            .all(|row| row.deleted_tx.is_none())
    );
    assert!(
        db.relational_store.tables.read()["retained"]
            .iter()
            .all(|row| !db.relational_store.is_retention_expired(
                "retained",
                row.row_id,
                row.created_tx
            ))
    );
    assert_eq!(db.snapshot(), snapshot);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options).unwrap();
    let tables = reopened.relational_store.tables.read();
    assert_eq!(tables["retained"].len(), 3);
    assert!(
        tables["retained"]
            .iter()
            .all(|row| row.deleted_tx.is_none())
    );
    drop(tables);
    reopened.close().unwrap();
}

#[test]
fn borrowed_foreign_key_frontier_preserves_protected_fixed_point_and_sync_delivery() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE parents (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    db.execute("CREATE TABLE children (id INT PRIMARY KEY, parent INT REFERENCES parents(id), expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC SAFE", &HashMap::new()).unwrap();
    db.execute("CREATE TABLE blockers (id INT PRIMARY KEY, child INT REFERENCES children(id), payload TEXT)", &HashMap::new()).unwrap();
    for id in 0..2 {
        let params = HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("expires".into(), Value::Timestamp(100)),
        ]);
        db.execute("INSERT INTO parents VALUES ($id, $expires)", &params)
            .unwrap();
        db.execute("INSERT INTO children VALUES ($id, $id, $expires)", &params)
            .unwrap();
    }
    db.execute(
        "INSERT INTO blockers VALUES (0, 0, $payload)",
        &HashMap::from([(
            "payload".into(),
            Value::Text("unrelated-child-payload".repeat(4096)),
        )]),
    )
    .unwrap();
    // Undelivered children block both parents. After delivery only the first
    // child remains protected by the non-retained grandchild, so protection
    // must propagate to its parent through the existing fixed point.
    db.set_sync_watermark(Lsn(0));
    assert_eq!(db.run_pruning_cycle_checked().unwrap().pruned_rows, 0);
    db.set_sync_watermark(Lsn(u64::MAX));
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 256 * 1024)).unwrap();
    let report = db.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 2);
    assert_eq!(report.blocked_count, 2);
    for table in ["parents", "children"] {
        let tables = db.relational_store.tables.read();
        assert_eq!(tables[table].len(), 1);
        assert_eq!(tables[table][0].values["id"], Value::Int64(0));
        assert!(tables[table][0].deleted_tx.is_none());
    }
    assert_eq!(db.relational_store.tables.read()["blockers"].len(), 1);
}

#[test]
fn reclamation_admits_vector_and_edge_identities_without_payload_copies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wide-auxiliary.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let db = Database::open(&path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE retained (id UUID PRIMARY KEY, expires TIMESTAMP EXPIRES, v VECTOR(4096)) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    let node = uuid::Uuid::from_u128(100);
    db.execute(
        "INSERT INTO retained VALUES ($id, $expires, $v)",
        &HashMap::from([
            ("id".into(), Value::Uuid(node)),
            ("expires".into(), Value::Timestamp(100)),
            ("v".into(), Value::Vector(vec![0.1; 4096])),
        ]),
    )
    .unwrap();
    let tx = db.begin().unwrap();
    for target in 0..64 {
        db.insert_edge(
            tx,
            node,
            uuid::Uuid::from_u128(1000 + target),
            "owns".into(),
            HashMap::from([(
                "body".into(),
                Value::Text("wide-edge-property".repeat(4096)),
            )]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.maintenance_context()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 8 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { ref subsystem, ref operation, .. }
        if subsystem == "retention" && operation == "prepare_prune")
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(db.relational_store.tables.read()["retained"].len(), 1);
    assert_eq!(
        db.graph_store.forward_adj.read().values().flatten().count(),
        64
    );
    let row = db.relational_store.tables.read()["retained"][0].row_id;
    let index = VectorIndexRef::new("retained", "v");
    assert_eq!(
        db.vector_store
            .directory_versions_for_rows(&index, &HashSet::from([row]))
            .len(),
        1
    );
    db.accountant.set_budget(Some(used + 256 * 1024)).unwrap();
    db.accountant.reset_high_water_for_test();
    let activity = db.__vector_passive_activity_counters_for_test();
    let report = db.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 1);
    assert!(db.accountant.high_water_for_test().saturating_sub(used) < 256 * 1024);
    assert_eq!(
        db.__vector_passive_activity_counters_for_test()
            .raw_partition_loads,
        activity.raw_partition_loads
    );
    assert!(db.relational_store.tables.read()["retained"].is_empty());
    assert!(db.graph_store.forward_adj.read().is_empty());
    assert!(
        db.vector_store
            .directory_versions_for_rows(&index, &HashSet::from([row]))
            .is_empty()
    );
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(reopened.relational_store.tables.read()["retained"].is_empty());
    assert!(reopened.graph_store.forward_adj.read().is_empty());
    assert!(
        reopened
            .vector_store
            .directory_versions_for_rows(&index, &HashSet::from([row]))
            .is_empty()
    );
    reopened.close().unwrap();
}

#[test]
fn pulled_sync_safe_and_null_composite_references_keep_existing_expiry_rules() {
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE parents (id INT PRIMARY KEY, a INT, b INT, expires TIMESTAMP EXPIRES, UNIQUE(a, b)) RETAIN 1 HOURS SYNC SAFE", &HashMap::new()).unwrap();
    db.execute("CREATE TABLE children (id INT PRIMARY KEY, a INT, b INT, FOREIGN KEY (a, b) REFERENCES parents(a, b))", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO parents VALUES (1, 7, NULL, $expires), (2, 8, 9, $expires)",
        &HashMap::from([("expires".into(), Value::Timestamp(100))]),
    )
    .unwrap();
    db.execute("INSERT INTO children VALUES (1, 7, NULL)", &HashMap::new())
        .unwrap();
    let pulled = db.relational_store.tables.read()["parents"]
        .iter()
        .find(|row| row.values["id"] == Value::Int64(1))
        .unwrap()
        .row_id;
    db.relational_store.set_sync_source_kinds([(
        "parents".into(),
        pulled,
        contextdb_relational::store::SyncSourceKind::Pulled,
    )]);
    db.relational_store
        .set_sync_source_lsns([("parents".into(), pulled, Lsn(100))]);
    db.set_sync_watermark(Lsn(0));
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 256 * 1024)).unwrap();
    let report = db.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 1);
    assert_eq!(report.blocked_count, 0);
    let tables = db.relational_store.tables.read();
    assert_eq!(tables["parents"].len(), 1);
    assert_eq!(tables["parents"][0].values["id"], Value::Int64(2));
    assert_eq!(tables["children"].len(), 1);
}

#[test]
fn reader_deferred_edges_remain_charged_and_carry_only_retries_are_admitted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deferred.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let options = || DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options()).unwrap();
    db.execute("CREATE TABLE retained (id UUID PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    let tx = db.begin().unwrap();
    for id in 0..64 {
        db.insert_edge(
            tx,
            uuid::Uuid::from_u128(1000 + id),
            uuid::Uuid::from_u128(2000 + id),
            "older".into(),
            HashMap::new(),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    // This snapshot sees the edges, while the subsequently inserted row is
    // newer. Row reclamation must leave the older adjacency owed to this pin.
    let pin = db.pin_snapshot(db.snapshot());
    for id in 0..64 {
        db.execute(
            "INSERT INTO retained VALUES ($id, $expires)",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(1000 + id))),
                ("expires".into(), Value::Timestamp(100)),
            ]),
        )
        .unwrap();
    }
    assert_eq!(db.run_pruning_cycle_checked().unwrap().pruned_rows, 64);
    assert!(db.relational_store.tables.read()["retained"].is_empty());
    {
        let carry = db.retention_deferred_edge_nodes.lock();
        assert_eq!(carry.nodes.len(), 64);
        assert_eq!(carry._memory.as_ref().unwrap().bytes(), 64 * 128);
    }
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 100)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { ref subsystem, ref operation, available_bytes: 100, .. }
        if subsystem == "retention" && operation == "prepare_prune")
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(db.retention_deferred_edge_nodes.lock().nodes.len(), 64);
    assert_eq!(
        db.graph_store.forward_adj.read().values().flatten().count(),
        64
    );
    db.accountant.set_budget(Some(used + 64 * 1024)).unwrap();
    assert_eq!(db.run_pruning_cycle_checked().unwrap().pruned_rows, 0);
    assert_eq!(db.retention_deferred_edge_nodes.lock().nodes.len(), 64);
    drop(pin);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options()).unwrap();
    let used = reopened.accountant.usage().used;
    reopened.accountant.set_budget(Some(used + 100)).unwrap();
    let failure = reopened.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { ref subsystem, ref operation, .. }
        if subsystem == "retention" && operation == "prepare_prune")
    );
    assert_eq!(reopened.accountant.usage().used, used);
    assert_eq!(
        reopened
            .graph_store
            .forward_adj
            .read()
            .values()
            .flatten()
            .count(),
        64
    );
    reopened
        .accountant
        .set_budget(Some(used + 256 * 1024))
        .unwrap();
    assert_eq!(reopened.run_pruning_cycle_checked().unwrap().pruned_rows, 0);
    assert!(reopened.graph_store.forward_adj.read().is_empty());
    assert!(
        reopened
            .retention_deferred_edge_nodes
            .lock()
            .nodes
            .is_empty()
    );
    assert_eq!(
        reopened
            .retention_deferred_edge_nodes
            .lock()
            ._memory
            .as_ref()
            .unwrap()
            .bytes(),
        0
    );
    reopened.accountant.set_budget(None).unwrap();
    reopened.close().unwrap();
}

#[test]
fn surviving_wide_index_rebuild_is_admitted_before_durable_reclamation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("indexed-retention.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options.clone()).unwrap();
    db.execute("CREATE TABLE retained (id INT PRIMARY KEY, wide TEXT, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    db.execute(
        "CREATE INDEX retained_wide ON retained (wide)",
        &HashMap::new(),
    )
    .unwrap();
    for id in 0..129 {
        db.execute(
            "INSERT INTO retained VALUES ($id, $wide, $expires)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                (
                    "wide".into(),
                    Value::Text(format!("{id}:{}", "surviving-key".repeat(4096))),
                ),
                (
                    "expires".into(),
                    Value::Timestamp(if id == 0 { 100 } else { 2_000_000 }),
                ),
            ]),
        )
        .unwrap();
    }
    db.maintenance_context()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 64 * 1024)).unwrap();
    let error = db.run_pruning_cycle_checked().unwrap_err();
    assert!(matches!(
        error,
        Error::MemoryBudgetExceeded { ref subsystem, ref operation, .. }
            if subsystem == "retention" && operation == "prepare_prune"
    ));
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(db.relational_store.tables.read()["retained"].len(), 129);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options).unwrap();
    assert_eq!(
        reopened.relational_store.tables.read()["retained"].len(),
        129
    );
    let used = reopened.accountant.usage().used;
    reopened
        .accountant
        .set_budget(Some(used + 64 * 1024 * 1024))
        .unwrap();
    let report = reopened.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 1);
    assert_eq!(
        reopened.relational_store.tables.read()["retained"].len(),
        128
    );
    assert_eq!(
        reopened
            .execute(
                "SELECT id FROM retained WHERE wide = $wide",
                &HashMap::from([(
                    "wide".into(),
                    Value::Text(format!("128:{}", "surviving-key".repeat(4096)))
                )])
            )
            .unwrap()
            .rows
            .len(),
        1
    );
    reopened.close().unwrap();
}

#[test]
fn affected_lsn_siblings_are_admitted_before_reclamation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("siblings.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options.clone()).unwrap();
    db.execute(
        "CREATE TABLE history (id INT PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("CREATE TABLE retained (id INT PRIMARY KEY, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO retained VALUES (0, $expires)",
        &HashMap::from([("expires".into(), Value::Timestamp(100))]),
    )
    .unwrap();
    for id in 0..512 {
        db.execute_in_tx(
            tx,
            "INSERT INTO history VALUES ($id, $payload)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("payload".into(), Value::Text("history".repeat(1024))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.maintenance_context()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 128 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { ref subsystem, ref operation, ref hint, .. } if subsystem == "retention" && operation == "prepare_prune" && hint.contains("change-log group"))
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(durable_observation::version_count(&db, "history"), 512);
    assert_eq!(durable_observation::version_count(&db, "retained"), 1);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options).unwrap();
    assert_eq!(durable_observation::version_count(&reopened, "retained"), 1);
    let used = reopened.accountant.usage().used;
    reopened
        .accountant
        .set_budget(Some(used + 2 * 1024 * 1024))
        .unwrap();
    assert_eq!(reopened.run_pruning_cycle_checked().unwrap().pruned_rows, 1);
    assert_eq!(
        durable_observation::version_count(&reopened, "history"),
        512
    );
    reopened.close().unwrap();
}

#[test]
fn wide_partition_inventory_is_admitted_before_reclamation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("partitions.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options.clone()).unwrap();
    db.execute("CREATE TABLE retained (id INT PRIMARY KEY, scope TEXT NOT NULL, expires TIMESTAMP EXPIRES, embedding VECTOR(3) PARTITION_KEY(scope) MAX_PARTITIONS 64 SEARCH_MODE EXACT) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    for id in 0..16 {
        db.execute(
            "INSERT INTO retained VALUES ($id, $scope, $expires, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                (
                    "scope".into(),
                    Value::Text(format!("{id}:{}", "wide-scope".repeat(4096))),
                ),
                (
                    "expires".into(),
                    Value::Timestamp(if id == 0 { 100 } else { 2_000_000 }),
                ),
                ("vector".into(), Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    }
    db.maintenance_context()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 128 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { ref subsystem, ref operation, ref hint, .. } if subsystem == "retention" && operation == "prepare_prune" && hint.contains("partition identities"))
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(durable_observation::version_count(&db, "retained"), 16);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options).unwrap();
    assert_eq!(
        durable_observation::version_count(&reopened, "retained"),
        16
    );
    let used = reopened.accountant.usage().used;
    reopened
        .accountant
        .set_budget(Some(used + 16 * 1024 * 1024))
        .unwrap();
    assert_eq!(reopened.run_pruning_cycle_checked().unwrap().pruned_rows, 1);
    assert_eq!(
        durable_observation::version_count(&reopened, "retained"),
        15
    );
    let index = VectorIndexRef::new("retained", "embedding");
    assert_eq!(
        reopened.vector_store.partition_keys(&index).unwrap().len(),
        15
    );
    reopened.close().unwrap();
}

#[test]
fn repeated_wide_membership_copies_are_admitted_before_reclamation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memberships.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options.clone()).unwrap();
    db.execute("CREATE TABLE retained (id INT PRIMARY KEY, scope TEXT NOT NULL, expires TIMESTAMP EXPIRES, embedding VECTOR(3) PARTITION_KEY(scope) MAX_PARTITIONS 64 SEARCH_MODE EXACT) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    let scope = "one-wide-partition".repeat(4096);
    for id in 0..16 {
        db.execute(
            "INSERT INTO retained VALUES ($id, $scope, $expires, $vector)",
            &HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("scope".into(), Value::Text(scope.clone())),
                ("expires".into(), Value::Timestamp(100)),
                ("vector".into(), Value::Vector(vec![1.0, 0.0, 0.0])),
            ]),
        )
        .unwrap();
    }
    db.maintenance_context()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 1024 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    eprintln!("repeated wide membership refusal: {failure:?}");
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { ref subsystem, ref operation, ref hint, .. } if subsystem == "retention" && operation == "prepare_prune" && (hint.contains("membership records") || hint.contains("journal keys")))
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(durable_observation::version_count(&db, "retained"), 16);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options).unwrap();
    assert_eq!(
        durable_observation::version_count(&reopened, "retained"),
        16
    );
    let used = reopened.accountant.usage().used;
    reopened
        .accountant
        .set_budget(Some(used + 16 * 1024 * 1024))
        .unwrap();
    assert_eq!(
        reopened.run_pruning_cycle_checked().unwrap().pruned_rows,
        16
    );
    assert!(durable_observation::version_count(&reopened, "retained") == 0);
    reopened.close().unwrap();
}

#[test]
fn current_row_identity_body_is_admitted_for_selected_lifetime_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("current-body.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    let db = Database::open_with_options(&path, options.clone()).unwrap();
    let body = "point-read-body".repeat(32768);
    // This unindexed body is omitted from the committed resident directory.
    db.execute("CREATE TABLE retained (id INT PRIMARY KEY, payload TEXT, expires TIMESTAMP EXPIRES) RETAIN 1 HOURS SYNC OFF", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO retained VALUES (0, 'old', $expires)",
        &HashMap::from([("expires".into(), Value::Timestamp(100))]),
    )
    .unwrap();
    // Preserve the physical row identity across the two lifetimes.
    let tx = db.begin().unwrap();
    db.upsert_row(
        tx,
        "retained",
        "id",
        HashMap::from([
            ("id".into(), Value::Int64(0)),
            ("payload".into(), Value::Text(body)),
            ("expires".into(), Value::Timestamp(2_000_000)),
        ]),
    )
    .unwrap();
    db.commit(tx).unwrap();
    {
        let tables = db.relational_store.tables.read();
        assert_eq!(tables["retained"].len(), 2);
        assert_eq!(tables["retained"][0].row_id, tables["retained"][1].row_id);
        assert!(!tables["retained"][1].values.contains_key("payload"));
    }
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 128 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { ref subsystem, ref operation, .. } if subsystem == "retention" && operation == "prepare_prune")
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(db.relational_store.tables.read()["retained"].len(), 2);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options).unwrap();
    assert_eq!(reopened.relational_store.tables.read()["retained"].len(), 2);
    let used = reopened.accountant.usage().used;
    reopened
        .accountant
        .set_budget(Some(used + 8 * 1024 * 1024))
        .unwrap();
    assert_eq!(reopened.run_pruning_cycle_checked().unwrap().pruned_rows, 1);
    assert_eq!(reopened.relational_store.tables.read()["retained"].len(), 1);
    let rows = reopened
        .execute("SELECT payload FROM retained", &HashMap::new())
        .unwrap();
    assert!(matches!(&rows.rows[0][0], Value::Text(body) if body.len() > 128 * 1024));
    reopened.close().unwrap();
}
