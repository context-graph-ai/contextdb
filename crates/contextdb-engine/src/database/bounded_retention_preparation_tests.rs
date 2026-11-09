use super::*;
include!("durable_test_observations.rs");
use crate::memory_accounting::{take_budget_ledger_for_test, watch_budget_ledger_for_test};

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn durable_edge_count(db: &Database) -> usize {
    use crate::persistence::writer_source::{SourceWindowOwner, WriterCursorFamily};
    use std::ops::Bound;
    let root = durable_observation::root(db);
    let owner = SourceWindowOwner::Read(db.declared_reader_memory().unwrap());
    let mut cursor = root
        .cursor(
            owner,
            "graph_fwd",
            WriterCursorFamily::Bytes,
            Bound::Unbounded,
            Bound::Unbounded,
            false,
        )
        .unwrap();
    let mut count = 0;
    while cursor.next(|_, _| Ok(())).unwrap().is_some() {
        count += 1;
    }
    count
}

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
    // The same schema without unrelated history pays the required durable
    // continuation bookkeeping too. Compare that live cost rather than
    // treating every prepare_expiry charge as a copied history payload.
    let control = Database::open_memory();
    control.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    control
        .execute(
            "CREATE TABLE history (id INT PRIMARY KEY, payload TEXT)",
            &HashMap::new(),
        )
        .unwrap();
    crate::work_ledger::install_work_ledger_schema(&control).unwrap();
    let expiry_bytes = |ledger: &[crate::memory_accounting::BudgetLedgerEntry]| {
        ledger
            .iter()
            .filter(|entry| entry.what == "retention/prepare_expiry")
            .map(|entry| entry.bytes)
            .sum::<usize>()
    };
    let row_work = |work: resident_history_test_seams::Work| {
        (
            work.retention_keys_examined,
            work.cleanup_versions_examined,
            work.log_keys_examined,
        )
    };
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
            for target in [&db, &control] {
                target.execute("INSERT INTO work_inputs (input_key, job_id, seq, payload) VALUES ('input', 'job', 0, 'not due')", &HashMap::new()).unwrap();
            }
        }
        // Calibrate the newly maintained committed proof on this same no-op
        // frontier. The unrelated-history allowance remains 32 KiB.
        watch_budget_ledger_for_test();
        let (control_result, control_work) =
            resident_history_test_seams::observe(|| control.run_pruning_cycle_checked().unwrap());
        assert_eq!(control_result.pruned_rows, 0);
        let control_calibration = take_budget_ledger_for_test();
        watch_budget_ledger_for_test();
        let (calibration_result, calibration_work) =
            resident_history_test_seams::observe(|| db.run_pruning_cycle_checked().unwrap());
        assert_eq!(calibration_result.pruned_rows, 0);
        let calibration = take_budget_ledger_for_test();
        assert_eq!(
            expiry_bytes(&calibration),
            expiry_bytes(&control_calibration)
        );
        assert_eq!(row_work(calibration_work), row_work(control_work));
        let proof_bytes = calibration
            .iter()
            .filter(|entry| entry.what == "content_identity/maintain_committed_proof")
            .map(|entry| entry.bytes)
            .sum::<usize>();
        let used = db.accountant.usage().used;
        let headroom = 32 * 1024 + proof_bytes;
        db.accountant.set_budget(Some(used + headroom)).unwrap();
        db.accountant.reset_high_water_for_test();
        watch_budget_ledger_for_test();
        let (control_result, control_work) =
            resident_history_test_seams::observe(|| control.run_pruning_cycle_checked().unwrap());
        assert_eq!(control_result.pruned_rows, 0);
        let control_ledger = take_budget_ledger_for_test();
        watch_budget_ledger_for_test();
        let (result, work) =
            resident_history_test_seams::observe(|| db.run_pruning_cycle_checked().unwrap());
        let ledger = take_budget_ledger_for_test();
        let report = result;
        assert_eq!(report.pruned_rows, 0);
        assert_eq!(expiry_bytes(&ledger), expiry_bytes(&control_ledger));
        assert_eq!(row_work(work), row_work(control_work));
        assert!(db.accountant.high_water_for_test() - used < headroom);
        assert_eq!(durable_observation::version_count(&db, "history"), 128);
        db.accountant.set_budget(None).unwrap();
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
    let pin = db.pin_snapshot(snapshot).unwrap();
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
        .unwrap()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap_err();
    match failure {
        Error::MemoryBudgetExceeded {
            subsystem,
            operation: _,
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
            assert!(requested_bytes > available_bytes);
            assert!(available_bytes < 800);
            assert!(budget_limit_bytes - available_bytes > used);
            assert_eq!(budget_limit_bytes, used + 800);
        }
        other => panic!("expected typed memory refusal: {other:?}"),
    }
    assert_eq!(db.accountant.usage().used, used);
    db.accountant.set_budget(None).unwrap();
    assert!(
        durable_observation::rows(&db, "retained")
            .iter()
            .all(|row| row.deleted_tx.is_none())
    );
    assert!(
        durable_observation::rows(&db, "retained")
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
    let tables = durable_observation::rows(&reopened, "retained");
    assert_eq!(tables.len(), 3);
    assert!(tables.iter().all(|row| row.deleted_tx.is_none()));
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
        let rows = db
            .execute(&format!("SELECT id FROM {table}"), &HashMap::new())
            .unwrap();
        assert_eq!(rows.rows(), &[vec![Value::Int64(0)]]);
        assert!(
            durable_observation::rows(&db, table)[0]
                .deleted_tx
                .is_none()
        );
    }
    assert_eq!(
        db.execute("SELECT id FROM blockers", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(0)]]
    );
}

/// The highest the accountant reaches above what the store held while one
/// prune cycle reclaims an expired row with a wide vector and 64 edges whose
/// property carries `property` -- the same fixture whatever the property is.
/// The cycle runs with no limit; only the accountant's own high water is read.
fn reclamation_peak_above_used(property: &str) -> usize {
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("peak-control.db");
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
            HashMap::from([("body".into(), Value::Text(property.to_owned()))]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.maintenance_context()
        .unwrap()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let used = db.accountant.usage().used;
    db.accountant.reset_high_water_for_test();
    assert_eq!(db.run_pruning_cycle_checked().unwrap().pruned_rows, 1);
    let peak = db.accountant.high_water_for_test().saturating_sub(used);
    db.close().unwrap();
    peak
}

#[test]
#[ignore = "Reclaiming an expired row with wide edge properties asks the accountant for more than the same reclamation with a short property plus one page, so the pruning pass still holds more than its pages and identities need."]
fn reclamation_admits_vector_and_edge_identities_without_payload_copies() {
    // The raw codec fixture owns this explicitly declared operation.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    // SETUP: what the same reclamation asks the accountant for when the edge
    // property is one short word, measured on a control store: the identities it
    // keeps and the small pages it reads. BOUND: the real property is 73 KB wide,
    // so its pages are full ones, and the real reclamation may ask for the
    // control's cost plus one page admission (the size of which the engine
    // reports in the refusal below). That is less than the control plus a page
    // and a copied property map, so a single copy of one map fails it.
    let property = "wide-edge-property".repeat(4096);
    let control_peak = reclamation_peak_above_used("p");

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
            HashMap::from([("body".into(), Value::Text(property.clone()))]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.maintenance_context()
        .unwrap()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let row = durable_observation::rows(&db, "retained")[0].row_id;
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 8 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { requested_bytes, available_bytes, .. }
        if requested_bytes > available_bytes)
    );
    let page_quote = match &failure {
        Error::MemoryBudgetExceeded {
            requested_bytes, ..
        } => *requested_bytes,
        other => panic!("the refusal is the typed memory one: {other:?}"),
    };
    let allowance = control_peak + page_quote;
    assert!(
        page_quote < property.len() * 2,
        "a page admission ({page_quote} B) is far smaller than the pages of payload a copy would hold"
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(durable_observation::version_count(&db, "retained"), 1);
    assert_eq!(durable_edge_count(&db), 64);
    let index = VectorIndexRef::new("retained", "v");
    let vector_fixture_reader_1 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_1 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_1.clone());

    assert_eq!(
        db.vector_store
            .directory_versions_for_rows(&index, &HashSet::from([row]), &mut vector_fixture_copy_1)
            .expect("read or publish the admitted vector fixture")
            .len(),
        1
    );
    db.accountant.set_budget(Some(used + allowance)).unwrap();
    db.accountant.reset_high_water_for_test();
    let activity = db.__vector_passive_activity_counters_for_test();
    let report = db.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 1);
    assert!(
        db.accountant.high_water_for_test().saturating_sub(used) <= allowance,
        "reclaiming a {} B-wide property map must not cost more than the control's {control_peak} B \
         plus one page admission of {page_quote} B",
        property.len()
    );
    assert_eq!(
        db.__vector_passive_activity_counters_for_test()
            .raw_partition_loads,
        activity.raw_partition_loads
    );
    assert!((durable_observation::version_count(&db, "retained") == 0));
    assert!(durable_edge_count(&db) == 0);
    let vector_fixture_reader_2 = db
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_2 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_2.clone());

    assert!(
        db.vector_store
            .directory_versions_for_rows(&index, &HashSet::from([row]), &mut vector_fixture_copy_2)
            .expect("read or publish the admitted vector fixture")
            .is_empty()
    );
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!((durable_observation::version_count(&reopened, "retained") == 0));
    assert!(durable_edge_count(&reopened) == 0);
    let vector_fixture_reader_3 = reopened
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_3 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_3.clone());

    assert!(
        reopened
            .vector_store
            .directory_versions_for_rows(&index, &HashSet::from([row]), &mut vector_fixture_copy_3)
            .expect("read or publish the admitted vector fixture")
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
    let sender = Database::open_memory();
    sender.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    sender.execute("CREATE TABLE parents (id INT PRIMARY KEY, a INT, b INT, expires TIMESTAMP EXPIRES, UNIQUE(a, b)) RETAIN 1 HOURS SYNC SAFE", &HashMap::new()).unwrap();
    sender
        .execute(
            "INSERT INTO parents VALUES (1, 7, NULL, $expires)",
            &HashMap::from([("expires".into(), Value::Timestamp(100))]),
        )
        .unwrap();
    // Pull through durable apply so the captured retention frontier sees the
    // same provenance as an actual receiver, including an undelivered LSN.
    db.apply_synced_changes(
        sender.changes_since(Lsn(0)).unwrap(),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        &HashMap::new(),
        SyncAdoption::Continuing,
    )
    .unwrap();
    db.execute(
        "INSERT INTO parents VALUES (2, 8, 9, $expires)",
        &HashMap::from([("expires".into(), Value::Timestamp(100))]),
    )
    .unwrap();
    db.execute("INSERT INTO children VALUES (1, 7, NULL)", &HashMap::new())
        .unwrap();
    let pulled = durable_observation::rows(&db, "parents")
        .iter()
        .find(|row| row.values["id"] == Value::Int64(1))
        .unwrap()
        .row_id;
    let (source_lsn, source_kind) = db
        .relational_sync_source_at_read_root(
            "parents",
            pulled,
            db.declared_reader_memory().unwrap(),
        )
        .unwrap();
    assert!(source_lsn.is_some_and(|lsn| lsn > Lsn(0)));
    assert_eq!(
        source_kind,
        Some(contextdb_relational::store::SyncSourceKind::Pulled)
    );
    db.set_sync_watermark(Lsn(0));
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 256 * 1024)).unwrap();
    let report = db.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 1);
    assert_eq!(report.blocked_count, 0);
    assert_eq!(
        db.execute("SELECT id FROM parents", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(2)]]
    );
    assert_eq!(
        db.execute("SELECT id FROM children", &HashMap::new())
            .unwrap()
            .rows(),
        &[vec![Value::Int64(1)]]
    );
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
    let pin = db.pin_snapshot(db.snapshot()).unwrap();
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
    let mut pruned = 0;
    for _ in 0..4 {
        let report = db.run_pruning_cycle_checked().unwrap();
        assert!(
            report.pruned_rows <= 32,
            "one finite prune selection stays within its batch"
        );
        pruned += report.pruned_rows;
        let carry = db.retention_deferred_edge_nodes.lock();
        assert_eq!(carry.nodes.len() as u64, pruned);
        assert_eq!(
            carry._memory.as_ref().unwrap().bytes(),
            carry.nodes.len() * 128
        );
        if pruned == 64 {
            break;
        }
    }
    assert_eq!(pruned, 64);
    assert!((durable_observation::version_count(&db, "retained") == 0));
    {
        let carry = db.retention_deferred_edge_nodes.lock();
        assert_eq!(carry.nodes.len(), 64);
        assert_eq!(carry._memory.as_ref().unwrap().bytes(), 64 * 128);
    }
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 100)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { requested_bytes, available_bytes: 100, .. }
        if requested_bytes > 100)
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(db.retention_deferred_edge_nodes.lock().nodes.len(), 64);
    assert_eq!(durable_edge_count(&db), 64);
    // A carry-only pass cannot reclaim pinned adjacency. Measure its actual
    // admitted work, including committed-proof maintenance, before retrying
    // with exactly that disclosed envelope rather than guessed slack.
    db.accountant.set_budget(None).unwrap();
    watch_budget_ledger_for_test();
    assert_eq!(db.run_pruning_cycle_checked().unwrap().pruned_rows, 0);
    let retry_bytes = take_budget_ledger_for_test()
        .iter()
        .filter(|entry| entry.what != "release")
        .map(|entry| entry.bytes)
        .sum::<usize>();
    assert!(retry_bytes > 0);
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(db.retention_deferred_edge_nodes.lock().nodes.len(), 64);
    assert_eq!(durable_edge_count(&db), 64);
    db.accountant.set_budget(Some(used + retry_bytes)).unwrap();
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
        matches!(failure, Error::MemoryBudgetExceeded { requested_bytes, available_bytes, .. }
        if requested_bytes > available_bytes)
    );
    assert_eq!(reopened.accountant.usage().used, used);
    assert_eq!(durable_edge_count(&reopened), 64);
    reopened
        .accountant
        .set_budget(Some(used + 256 * 1024))
        .unwrap();
    for _ in 0..4 {
        assert_eq!(reopened.run_pruning_cycle_checked().unwrap().pruned_rows, 0);
        let carry = reopened.retention_deferred_edge_nodes.lock();
        assert_eq!(
            carry._memory.as_ref().unwrap().bytes(),
            carry.nodes.len() * 128
        );
        if carry.nodes.is_empty() {
            break;
        }
    }
    assert_eq!(durable_edge_count(&reopened), 0);
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
    // The raw codec fixture owns this explicitly declared operation.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("indexed-retention.db");
    let _clock = Wallclock::test_clock_guard(|| 1_000_000);
    let options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    let populate = |path: &std::path::Path| {
        let db = Database::open_with_options(path, options.clone()).unwrap();
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
            .unwrap()
            .stamp_expired_rows(Wallclock(1_000_000))
            .unwrap();
        db
    };
    let db = populate(&path);
    // Build the same durable frontier through the same SQL setup, then
    // calibrate its disclosed admissions, including proof and surviving index
    // work. The target keeps its expired version until admitted publication.
    let control_path = dir.path().join("preparation-control.db");
    let control = populate(&control_path);
    watch_budget_ledger_for_test();
    assert_eq!(control.run_pruning_cycle_checked().unwrap().pruned_rows, 1);
    let preparation_bytes = take_budget_ledger_for_test()
        .iter()
        .filter(|entry| entry.what != "release")
        .map(|entry| entry.bytes)
        .sum::<usize>();
    assert!(preparation_bytes > 0);
    control.close().unwrap();
    let used = db.accountant.usage().used;
    let snapshot = db.snapshot();
    db.accountant.set_budget(Some(used + 100)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { requested_bytes, available_bytes, .. }
        if requested_bytes > available_bytes)
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(db.snapshot(), snapshot);
    assert_eq!(durable_observation::version_count(&db, "retained"), 129);
    db.accountant
        .set_budget(Some(used + preparation_bytes))
        .unwrap();
    db.accountant.reset_high_water_for_test();
    let report = db.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 1);
    assert!(db.accountant.high_water_for_test().saturating_sub(used) <= preparation_bytes);
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(durable_observation::version_count(&db, "retained"), 128);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options).unwrap();
    assert_eq!(
        durable_observation::version_count(&reopened, "retained"),
        128
    );
    let used = reopened.accountant.usage().used;
    reopened
        .accountant
        .set_budget(Some(used + 64 * 1024 * 1024))
        .unwrap();
    let report = reopened.run_pruning_cycle_checked().unwrap();
    assert_eq!(report.pruned_rows, 0);
    assert_eq!(
        durable_observation::version_count(&reopened, "retained"),
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
            .rows()
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
        .unwrap()
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
        .unwrap()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 128 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { requested_bytes, available_bytes, .. } if requested_bytes > available_bytes)
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
    let vector_fixture_reader_4 = reopened
        .declared_reader_memory()
        .expect("admit the vector fixture reader");
    let mut vector_fixture_copy_4 =
        contextdb_engine::vector_read_memory::admission(vector_fixture_reader_4.clone());

    assert_eq!(
        reopened
            .vector_store
            .partition_keys(&index, &mut vector_fixture_copy_4)
            .unwrap()
            .len(),
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
        .unwrap()
        .stamp_expired_rows(Wallclock(1_000_000))
        .unwrap();
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 1024 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    eprintln!("repeated wide membership refusal: {failure:?}");
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { requested_bytes, available_bytes, .. } if requested_bytes > available_bytes)
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
        let rows = durable_observation::rows(&db, "retained");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].row_id, rows[1].row_id);
        assert!(
            matches!(rows[1].values.get("payload"), Some(Value::Text(body)) if body.len() > 128 * 1024)
        );
    }
    let used = db.accountant.usage().used;
    db.accountant.set_budget(Some(used + 128 * 1024)).unwrap();
    let failure = db.run_pruning_cycle_checked().unwrap_err();
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { requested_bytes, available_bytes, .. } if requested_bytes > available_bytes)
    );
    assert_eq!(db.accountant.usage().used, used);
    assert_eq!(durable_observation::version_count(&db, "retained"), 2);
    db.accountant.set_budget(None).unwrap();
    db.close().unwrap();
    let reopened = Database::open_with_options(&path, options).unwrap();
    assert_eq!(durable_observation::version_count(&reopened, "retained"), 2);
    let used = reopened.accountant.usage().used;
    reopened
        .accountant
        .set_budget(Some(used + 8 * 1024 * 1024))
        .unwrap();
    assert_eq!(reopened.run_pruning_cycle_checked().unwrap().pruned_rows, 1);
    assert_eq!(durable_observation::version_count(&reopened, "retained"), 1);
    let rows = reopened
        .execute("SELECT payload FROM retained", &HashMap::new())
        .unwrap();
    assert!(matches!(&rows.rows()[0][0], Value::Text(body) if body.len() > 128 * 1024));
    reopened.close().unwrap();
}
