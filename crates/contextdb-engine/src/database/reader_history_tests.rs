use super::*;

fn typed_memory_refusal(result: Result<impl Sized>, operation: &str) {
    match result {
        Err(Error::MemoryBudgetExceeded {
            operation: actual, ..
        }) => assert_eq!(actual, operation),
        Err(error) => panic!("expected typed memory refusal for {operation}, got {error:?}"),
        Ok(_) => panic!("expected typed memory refusal for {operation}"),
    }
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "callback signature returns the engine Error by contract"
)]
fn trigger_audit_history_refuses_when_reader_budget_cannot_hold_one_entry() {
    let mut db = Database::open_memory();
    db.execute(
        "CREATE TABLE audit_rows (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER long_trigger_name ON audit_rows WHEN INSERT",
        &HashMap::new(),
    )
    .unwrap();
    db.register_trigger_callback("long_trigger_name", |_db, _context| Ok(()))
        .unwrap();
    db.execute("INSERT INTO audit_rows VALUES (1)", &HashMap::new())
        .unwrap();
    assert_eq!(
        db.trigger_audit_history(TriggerAuditFilter::default())
            .unwrap()
            .len(),
        1
    );
    let (root, child) = crate::vector_read_memory::native_control_layouts_for_test();
    // The API and persistence each hold one decoder; the entry cannot fit.
    let controls = root.size()
        + child.size()
        + 2 * crate::read_image_memory::ImageMemory::admitted_control_bytes();
    let entry = std::mem::size_of::<TriggerAuditEntry>() + 96;
    let mut limit = controls + entry - 1;
    loop {
        db.declared_read_memory =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(limit as u64).unwrap();
        match db.trigger_audit_history(TriggerAuditFilter::default()) {
            Err(error) => {
                let Error::MemoryBudgetExceeded {
                    operation,
                    requested_bytes,
                    available_bytes,
                    ..
                } = &error
                else {
                    panic!("expected audit-history refusal, got {error:?}");
                };
                if *operation == "trigger_audit_history" {
                    typed_memory_refusal(Err::<(), _>(error), "trigger_audit_history");
                    break;
                }
                let missing = requested_bytes.checked_sub(*available_bytes).unwrap();
                assert!(missing > 0, "setup refusal must disclose a shortfall");
                limit = limit.checked_add(missing).unwrap();
            }
            Ok(_) => panic!("funding audit controls must leave the first entry unaffordable"),
        }
    }
}

#[test]
fn changes_since_history_projection_refuses_before_returning_unbounded_history() {
    let fixture = || {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE rows (id INTEGER PRIMARY KEY, value TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO rows VALUES (1, 'history payload')",
            &HashMap::new(),
        )
        .unwrap();
        db
    };
    let assert_complete = |(changes, arrivals): (ChangeSet, HashMap<Lsn, Option<Lsn>>)| {
        assert_eq!(changes.rows.len(), 1);
        assert_eq!(changes.rows[0].values.get("id"), Some(&Value::Int64(1)));
        assert_eq!(
            changes.rows[0].values.get("value"),
            Some(&Value::Text("history payload".into()))
        );
        assert_eq!(changes.ddl.len(), 1);
        assert_eq!(arrivals.len(), 1);
        assert!(arrivals.contains_key(&changes.rows[0].lsn));
    };
    let mut control = fixture();
    control.declared_read_memory =
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
            crate::OwnerReadConfig::default().limits.limits.memory,
        )
        .unwrap();
    let (result, charge) = control
        .declared_read_memory
        .observe_high_water_for_test(|| control.changes_since_with_arrivals(Lsn(0)).unwrap());
    assert_complete(result);
    let peak = charge.peak;
    // Parsed by the focused verification controller into the receipt.
    println!(
        "history_boundary peak={peak} low_budget={} high_budget={peak}",
        peak - 1
    );

    // Fresh, identically populated stores keep both boundary calls as cold
    // as the measured control; calibration cannot warm either tested read.
    let mut below = fixture();
    below.declared_read_memory =
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(peak - 1).unwrap();
    let below_result = below.changes_since_with_arrivals(Lsn(0));
    let mut exact = fixture();
    exact.declared_read_memory =
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(peak).unwrap();
    assert_complete(exact.changes_since_with_arrivals(Lsn(0)).unwrap());
    println!(
        "history_boundary_result below_refused={} exact_complete=true",
        matches!(below_result, Err(Error::MemoryBudgetExceeded { .. }))
    );
    match below_result {
        Err(Error::MemoryBudgetExceeded { .. }) => {}
        Err(error) => panic!("expected typed history-memory refusal at P-1, got {error:?}"),
        Ok(_) => panic!(
            "history must refuse at P-1 ({}) rather than return an answer",
            peak - 1
        ),
    }
}
