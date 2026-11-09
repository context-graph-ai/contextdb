use super::*;

#[test]
fn dropping_an_unreached_pause_allows_maintenance_to_continue() {
    let slot = Arc::new(PauseSlot::new());
    let generation = slot.arm();
    drop(MaintenancePauseHandle {
        slot: Arc::clone(&slot),
        generation,
    });
    {
        let state = slot.state.lock();
        assert!(
            !state.armed || state.released,
            "dropping the pause must release maintenance even before arrival"
        );
    }
    slot.maybe_pause();
}

#[test]
fn blocking_arrival_and_drop_release_the_real_paused_maintenance_call() {
    let slot = Arc::new(PauseSlot::new());
    let handle = MaintenancePauseHandle {
        generation: slot.arm(),
        slot: Arc::clone(&slot),
    };
    let maintenance = std::thread::spawn(move || slot.maybe_pause());
    assert!(handle.wait_until_reached_blocking());
    drop(handle);
    maintenance.join().unwrap();
}

#[test]
fn release_before_arrival_wakes_a_blocking_waiter_with_cancellation() {
    let slot = Arc::new(PauseSlot::new());
    let handle = MaintenancePauseHandle {
        generation: slot.arm(),
        slot: Arc::clone(&slot),
    };
    std::thread::scope(|scope| {
        let (started, started_rx) = std::sync::mpsc::channel();
        let (finished, finished_rx) = std::sync::mpsc::channel();
        let waiting = &handle;
        scope.spawn(move || {
            started.send(()).unwrap();
            finished
                .send(waiting.wait_until_reached_blocking())
                .unwrap();
        });
        started_rx.recv().unwrap();
        handle.release();
        assert!(!finished_rx.recv().unwrap());
    });
    slot.maybe_pause();
}

#[test]
fn replacing_a_reached_generation_releases_it_without_consuming_the_new_pause() {
    let slot = Arc::new(PauseSlot::new());
    let first = MaintenancePauseHandle {
        generation: slot.arm(),
        slot: Arc::clone(&slot),
    };
    let arriving = Arc::clone(&slot);
    let first_call = std::thread::spawn(move || arriving.maybe_pause());
    assert!(first.wait_until_reached_blocking());
    let second = MaintenancePauseHandle {
        generation: slot.arm(),
        slot: Arc::clone(&slot),
    };
    assert!(!first.wait_until_reached_blocking());
    first_call.join().unwrap();
    drop(first);
    let second_call = std::thread::spawn(move || slot.maybe_pause());
    assert!(second.wait_until_reached_blocking());
    second.release();
    second_call.join().unwrap();
}
