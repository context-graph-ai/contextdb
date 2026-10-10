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
    let (db, accountant, open_time_used) = open_db();
    let pool = db
        .snapshot_registry
        .pin_slots
        .lock()
        .as_ref()
        .unwrap()
        .clone();
    {
        let pins = (0..contextdb_core::read_contract::ReadLimits::SHIPPED_SNAPSHOT_PIN_SLOTS)
            .map(|n| db.pin_snapshot(SnapshotId(n as u64)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(accountant.usage().used, open_time_used);
        let error = match db.pin_snapshot(db.snapshot()) {
            Ok(_) => panic!("pin pool must refuse one past its declared capacity"),
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
