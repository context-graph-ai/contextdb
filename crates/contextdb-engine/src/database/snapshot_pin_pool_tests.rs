use super::*;

fn open_db() -> (Database, crate::memory_accounting::AccountantHandle, usize) {
    let db = Database::open_with_options(":memory:", Default::default()).unwrap();
    let accountant = db.accountant.clone();
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
        crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(pool.slot_bytes as u64)
            .expect("admit the declared fixture root");
    let _held_reader = contextdb_core::read_memory::ReadCredit::try_new(
        db.declared_read_memory.original_owner(),
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
    let root_bytes = crate::vector_read_memory::native_control_layouts_for_test()
        .0
        .size();
    // Exactly two overflow registrations fit; there is no count ceiling.
    db.declared_read_memory = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
        (root_bytes + 2 * pool.slot_bytes) as u64,
    )
    .expect("admit the declared fixture root");
    let pins = (0..pool.slots)
        .map(|_| db.pin_snapshot(snapshot).unwrap())
        .collect::<Vec<_>>();
    let first = db.pin_snapshot(snapshot).unwrap();
    let second = db.pin_snapshot(snapshot).unwrap();
    assert_eq!(
        pin_memory_ownership_snapshot(
            &db.declared_read_memory,
            &[("first", &first), ("second", &second)]
        ),
        (
            vec![
                ("declared_native_root", root_bytes),
                ("first", pool.slot_bytes),
                ("second", pool.slot_bytes)
            ],
            0
        )
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
        pin_memory_ownership_snapshot(&db.declared_read_memory, &[("second", &second)]),
        (
            vec![
                ("declared_native_root", root_bytes),
                ("second", pool.slot_bytes)
            ],
            0
        )
    );
    let retry = db.pin_snapshot(snapshot).unwrap();
    drop(second);
    drop(retry);
    db.declared_read_memory
        .native_memory_ownership_for_test()
        .assert_only_original_root();
    drop(pins);
    assert_eq!(pool.available.load(Ordering::SeqCst), pool.slots);
    assert!(db.snapshot_registry.state.lock().active.is_empty());
    let original_root = db.declared_read_memory.clone();
    drop(db);
    original_root.assert_only_original_root_and_free_for_test();
}

fn pin_memory_ownership_snapshot(
    root: &crate::vector_read_memory::DeclaredReadRoot,
    pins: &[(&'static str, &SnapshotPin<'_>)],
) -> (Vec<(&'static str, usize)>, u64) {
    let (native, mut unnamed) = root
        .native_memory_ownership_for_test()
        .memory_ownership_snapshot();
    let mut named = vec![native.expect("live original root")];
    for &(name, pin) in pins {
        let SnapshotPinSlot::Reader { _credit: credit } = &pin._slot else {
            panic!("the named overflow pin has a real reader invoice");
        };
        assert_eq!(
            credit.owner().accounting_identity(),
            root.original_owner().accounting_identity(),
            "the pin invoice belongs to this original root"
        );
        named.push((name, credit.bytes()));
        unnamed = unnamed
            .checked_sub(credit.bytes() as u64)
            .expect("named pin remains funded");
    }
    (named, unnamed)
}
