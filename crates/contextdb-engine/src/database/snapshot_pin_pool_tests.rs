use super::*;

fn open_db() -> (Database, Arc<MemoryAccountant>, usize) {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(accountant.clone());
    let open_time_used = accountant.usage().used;
    (db, accountant, open_time_used)
}

#[test]
fn same_snapshot_pins_take_and_return_exactly_the_open_funded_slots() {
    let (db, accountant, open_time_used) = open_db();
    let snapshot = db.snapshot();
    let pool = db
        .snapshot_registry
        .pin_slots
        .lock()
        .as_ref()
        .unwrap()
        .clone();
    {
        let pins = (0..contextdb_core::read_contract::ReadLimits::SHIPPED_SNAPSHOT_PIN_SLOTS)
            .map(|_| db.pin_snapshot(snapshot).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(accountant.usage().used, open_time_used);
        assert_eq!(pool.available.load(Ordering::SeqCst), 0);
        assert_eq!(db.snapshot_registry.state.lock().active.len(), pins.len());
    }
    assert_eq!(accountant.usage().used, open_time_used);
    assert_eq!(pool.available.load(Ordering::SeqCst), pool.slots);
    assert!(db.snapshot_registry.state.lock().active.is_empty());
}

#[test]
fn distinct_snapshot_pins_refuse_one_past_capacity_without_disturbing_live_pins() {
    let (mut db, accountant, open_time_used) = open_db();
    let pool = db
        .snapshot_registry
        .pin_slots
        .lock()
        .as_ref()
        .unwrap()
        .clone();
    // Exhaust the reader budget as well as the prepaid slots. The pool is
    // a fast path; only a memory shortage may refuse an overflow pin.
    db.declared_read_memory =
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(pool.slot_bytes as u64);
    let _held_reader = contextdb_core::read_memory::ReadCredit::try_new(
        db.declared_read_memory.clone(),
        pool.slot_bytes,
        "test",
        "occupy_reader",
        "fixture",
    )
    .unwrap();
    {
        let pins = (0..contextdb_core::read_contract::ReadLimits::SHIPPED_SNAPSHOT_PIN_SLOTS)
            .map(|n| db.pin_snapshot(SnapshotId(n as u64)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(accountant.usage().used, open_time_used);
        let error = match db.pin_snapshot(db.snapshot()) {
            Ok(_) => panic!("an unfunded overflow pin must refuse"),
            Err(error) => error,
        };
        assert!(matches!(error,
            Error::MemoryBudgetExceeded { ref subsystem, ref operation,
                requested_bytes, available_bytes: 0, budget_limit_bytes, .. }
                if subsystem == "snapshot_pin" && operation == "admit_pin"
                    && requested_bytes > 0 && budget_limit_bytes >= requested_bytes));
        assert_eq!(db.snapshot_registry.state.lock().active.len(), pins.len());
        assert_eq!(pool.available.load(Ordering::SeqCst), 0);
        assert_eq!(accountant.usage().used, open_time_used);
        drop(pins);
    }
    assert_eq!(accountant.usage().used, open_time_used);
    assert_eq!(pool.available.load(Ordering::SeqCst), pool.slots);
    assert!(db.snapshot_registry.state.lock().active.is_empty());
}

#[test]
fn overflow_pins_spend_reader_memory_and_refuse_without_changing_committed_state() {
    let (mut db, accountant, open_time_used) = open_db();
    db.execute(
        "CREATE TABLE pin_state (id INT PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("INSERT INTO pin_state VALUES (7)", &HashMap::new())
        .unwrap();
    let snapshot = db.snapshot();
    let pool = db
        .snapshot_registry
        .pin_slots
        .lock()
        .as_ref()
        .unwrap()
        .clone();
    let writer_used = accountant.usage().used;
    assert!(writer_used >= open_time_used);
    // Exactly two overflow registrations fit; there is no count ceiling.
    db.declared_read_memory = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
        (2 * pool.slot_bytes) as u64,
    );
    let pins = (0..pool.slots)
        .map(|_| db.pin_snapshot(snapshot).unwrap())
        .collect::<Vec<_>>();
    let first = db.pin_snapshot(snapshot).unwrap();
    let second = db.pin_snapshot(snapshot).unwrap();
    assert_eq!(
        db.declared_read_memory.charged_bytes(),
        (2 * pool.slot_bytes) as u64
    );
    let held = db.snapshot_registry.state.lock().active.len();
    assert_eq!(held, pool.slots + 2);
    assert!(
        matches!(db.pin_snapshot(snapshot), Err(Error::MemoryBudgetExceeded {
        ref subsystem, ref operation, requested_bytes, available_bytes: 0, ..
    }) if subsystem == "snapshot_pin" && operation == "admit_pin"
        && requested_bytes == pool.slot_bytes)
    );
    assert_eq!(db.snapshot(), snapshot);
    assert_eq!(db.snapshot_registry.state.lock().active.len(), held);
    assert_eq!(accountant.usage().used, writer_used);
    assert_eq!(
        db.relational_store
            .row_by_id("pin_state", RowId(1), snapshot)
            .unwrap()
            .values
            .get("id"),
        Some(&Value::Int64(7))
    );
    drop(first);
    assert_eq!(
        db.declared_read_memory.charged_bytes(),
        pool.slot_bytes as u64
    );
    let retry = db.pin_snapshot(snapshot).unwrap();
    drop(second);
    drop(retry);
    assert_eq!(db.declared_read_memory.charged_bytes(), 0);
    drop(pins);
    assert_eq!(pool.available.load(Ordering::SeqCst), pool.slots);
    assert!(db.snapshot_registry.state.lock().active.is_empty());
}
