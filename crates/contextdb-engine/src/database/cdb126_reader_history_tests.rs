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
fn trigger_audit_history_refuses_when_reader_budget_cannot_hold_one_entry() {
    let mut db = Database::open_memory();
    db.trigger.volatile_audit_history.lock().push((
        Wallclock(1),
        TriggerAuditEntry {
            trigger_name: "long_trigger_name".into(),
            firing_tx: TxId(1),
            firing_lsn: Lsn(1),
            depth: 0,
            cascade_row_count: 1,
            status: TriggerAuditStatus::Fired,
        },
    ));
    db.declared_read_memory = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(128)
        .expect("original 128-byte operation fixture must admit its charged root");
    let (root, child) = crate::vector_read_memory::native_control_layouts_for_test();
    assert!(
        root.size() + child.size() <= 128,
        "BLOCKED: original 128-byte total must reach the intended history operation after root and child admission"
    );
    typed_memory_refusal(
        db.trigger_audit_history(TriggerAuditFilter::default()),
        "trigger_audit_history",
    );
}

#[test]
fn changes_since_history_projection_refuses_before_returning_unbounded_history() {
    let mut db = Database::open_memory();
    db.execute(
        "CREATE TABLE cdb126_rows (id INTEGER PRIMARY KEY, value TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO cdb126_rows VALUES (1, 'history payload')",
        &HashMap::new(),
    )
    .unwrap();
    db.declared_read_memory = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(128)
        .expect("original 128-byte operation fixture must admit its charged root");
    let (root, child) = crate::vector_read_memory::native_control_layouts_for_test();
    assert!(
        root.size() + child.size() <= 128,
        "BLOCKED: original 128-byte total must reach the intended history operation after root and child admission"
    );
    typed_memory_refusal(
        db.changes_since_with_arrivals(Lsn(0)),
        "changes_since_with_arrivals",
    );
}
