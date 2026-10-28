use super::*;

#[derive(Default)]
struct ProgressState {
    progress_generation: u64,
    progress_watchers: usize,
}

#[derive(Default)]
pub(super) struct SinkProgressControlForTest {
    state: Mutex<ProgressState>,
    changed: Condvar,
    active: AtomicBool,
}

pub(super) struct SinkProgressIterationForTest {
    control: Arc<SinkProgressControlForTest>,
}

impl Drop for SinkProgressIterationForTest {
    fn drop(&mut self) {
        let mut state = self.control.state.lock();
        if state.progress_watchers != 0 {
            state.progress_generation = state.progress_generation.wrapping_add(1);
            self.control.changed.notify_all();
        }
    }
}

impl SinkProgressControlForTest {
    pub(super) fn begin_iteration(self: &Arc<Self>) -> Option<SinkProgressIterationForTest> {
        if !self.active.load(Ordering::SeqCst) {
            return None;
        }
        Some(SinkProgressIterationForTest {
            control: self.clone(),
        })
    }

    pub(super) fn progress_waiting(&self) -> bool {
        if !self.active.load(Ordering::SeqCst) {
            return false;
        }
        let mut state = self.state.lock();
        let watched = state.progress_watchers != 0;
        if watched {
            state.progress_generation = state.progress_generation.wrapping_add(1);
            self.changed.notify_all();
        }
        watched
    }
}

/// Observes automatic dispatcher progress and advances actual pacing waits.
/// This is only compiled for unit tests or with the test-seams feature.
#[doc(hidden)]
struct SinkProgressWatchForTest {
    event_bus: Arc<EventBusState>,
    expired: Arc<AtomicBool>,
    cancel_watchdog: std::sync::mpsc::Sender<()>,
    watchdog: Option<std::thread::JoinHandle<()>>,
}

impl SinkProgressWatchForTest {
    #[doc(hidden)]
    pub fn generation_for_test(&self) -> u64 {
        self.event_bus
            .progress_for_test
            .state
            .lock()
            .progress_generation
    }

    #[doc(hidden)]
    pub fn advance_and_wait_for_test(&self, generation: u64) -> bool {
        {
            let _wait_lock = self.event_bus.wait_lock.lock();
            self.event_bus.waiters.notify_all();
        }
        let control = &self.event_bus.progress_for_test;
        let mut state = control.state.lock();
        while state.progress_generation == generation && !self.expired.load(Ordering::SeqCst) {
            control.changed.wait(&mut state);
        }
        !self.expired.load(Ordering::SeqCst)
    }
}

impl Drop for SinkProgressWatchForTest {
    fn drop(&mut self) {
        let _ = self.cancel_watchdog.send(());
        if let Some(watchdog) = self.watchdog.take() {
            let _ = watchdog.join();
        }
        {
            let mut state = self.event_bus.progress_for_test.state.lock();
            state.progress_watchers -= 1;
            self.event_bus
                .progress_for_test
                .active
                .store(state.progress_watchers != 0, Ordering::SeqCst);
        }
        let _wait_lock = self.event_bus.wait_lock.lock();
        self.event_bus.waiters.notify_all();
    }
}

impl Database {
    #[doc(hidden)]
    pub fn wait_for_sink_progress_for_test(
        &self,
        liveness_bound: Duration,
        mut ready: impl FnMut() -> bool,
    ) -> bool {
        {
            let mut state = self.event_bus.progress_for_test.state.lock();
            state.progress_watchers += 1;
            self.event_bus
                .progress_for_test
                .active
                .store(true, Ordering::SeqCst);
        }
        {
            let _wait_lock = self.event_bus.wait_lock.lock();
            self.event_bus.waiters.notify_all();
        }
        let expired = Arc::new(AtomicBool::new(false));
        let watchdog_expired = expired.clone();
        let watchdog_control = self.event_bus.progress_for_test.clone();
        let (cancel_watchdog, cancel_rx) = std::sync::mpsc::channel();
        let watchdog = std::thread::spawn(move || {
            if matches!(
                cancel_rx.recv_timeout(liveness_bound),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                let _state = watchdog_control.state.lock();
                watchdog_expired.store(true, Ordering::SeqCst);
                watchdog_control.changed.notify_all();
            }
        });
        let watch = SinkProgressWatchForTest {
            event_bus: self.event_bus.clone(),
            expired,
            cancel_watchdog,
            watchdog: Some(watchdog),
        };
        loop {
            let generation = watch.generation_for_test();
            if ready() {
                return true;
            }
            if !watch.advance_and_wait_for_test(generation) {
                return false;
            }
        }
    }
}
