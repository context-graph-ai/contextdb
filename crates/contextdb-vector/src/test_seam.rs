//! Test-only maintenance-pause seam. Feature-gated; production builds do not link this module.
//! Mirrors the shape of `contextdb-engine::ApplyPhasePause`.

use crate::store::VectorStore;
use contextdb_core::VectorIndexRef;
use parking_lot::{Condvar, Mutex};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[cfg(test)]
mod pause_lifecycle_tests;

static DORMANT_DECODE_FAILURE_ARMED: AtomicBool = AtomicBool::new(false);
static DORMANT_DECODE_FAILURE_CONSUMED: AtomicBool = AtomicBool::new(false);
static POST_LOAD_RECONCILIATION_FAILURE_ARMED: AtomicBool = AtomicBool::new(false);
static POST_LOAD_RECONCILIATION_FAILURE_CONSUMED: AtomicBool = AtomicBool::new(false);
thread_local! {
    static STALE_DORMANT_LOAD_ARMED: Cell<bool> = const { Cell::new(false) };
    static STALE_DORMANT_LOAD_CONSUMED: Cell<bool> = const { Cell::new(false) };
}

fn arm(armed: &AtomicBool, consumed: &AtomicBool) {
    consumed.store(false, Ordering::SeqCst);
    armed.store(true, Ordering::SeqCst);
}

fn take(armed: &AtomicBool, consumed: &AtomicBool) -> bool {
    if armed.swap(false, Ordering::SeqCst) {
        consumed.store(true, Ordering::SeqCst);
        true
    } else {
        false
    }
}

#[doc(hidden)]
pub fn arm_dormant_vector_decode_failure_for_test() {
    arm(
        &DORMANT_DECODE_FAILURE_ARMED,
        &DORMANT_DECODE_FAILURE_CONSUMED,
    );
}

#[doc(hidden)]
pub fn take_dormant_vector_decode_failure_for_test() -> bool {
    take(
        &DORMANT_DECODE_FAILURE_ARMED,
        &DORMANT_DECODE_FAILURE_CONSUMED,
    )
}

#[doc(hidden)]
pub fn dormant_vector_decode_failure_consumed_for_test() -> bool {
    DORMANT_DECODE_FAILURE_CONSUMED.load(Ordering::SeqCst)
}

#[doc(hidden)]
pub fn arm_vector_post_load_reconciliation_failure_for_test() {
    arm(
        &POST_LOAD_RECONCILIATION_FAILURE_ARMED,
        &POST_LOAD_RECONCILIATION_FAILURE_CONSUMED,
    );
}

#[doc(hidden)]
pub fn take_vector_post_load_reconciliation_failure_for_test() -> bool {
    take(
        &POST_LOAD_RECONCILIATION_FAILURE_ARMED,
        &POST_LOAD_RECONCILIATION_FAILURE_CONSUMED,
    )
}

#[doc(hidden)]
pub fn vector_post_load_reconciliation_failure_consumed_for_test() -> bool {
    POST_LOAD_RECONCILIATION_FAILURE_CONSUMED.load(Ordering::SeqCst)
}

/// The next dormant generation load finds, once it has loaded, that the fresh
/// tail moved while it was loading -- as when a writer publishes over it -- so
/// it is discarded and loaded again.
#[doc(hidden)]
pub fn arm_vector_stale_dormant_load_for_test() {
    STALE_DORMANT_LOAD_CONSUMED.set(false);
    STALE_DORMANT_LOAD_ARMED.set(true);
}

#[doc(hidden)]
pub fn take_vector_stale_dormant_load_for_test() -> bool {
    if STALE_DORMANT_LOAD_ARMED.replace(false) {
        STALE_DORMANT_LOAD_CONSUMED.set(true);
        true
    } else {
        false
    }
}

#[doc(hidden)]
pub fn vector_stale_dormant_load_consumed_for_test() -> bool {
    STALE_DORMANT_LOAD_CONSUMED.get()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PauseWindow {
    Apply,
    Build,
    Search,
    Ddl,
    Bulk,
}

#[derive(Debug, Default)]
struct PauseSlotState {
    generation: u64,
    armed: bool,
    reached: bool,
    released: bool,
}

#[derive(Debug)]
pub(crate) struct PauseSlot {
    state: Mutex<PauseSlotState>,
    waiters: Condvar,
}

impl PauseSlot {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(PauseSlotState::default()),
            waiters: Condvar::new(),
        }
    }

    pub(crate) fn arm(&self) -> u64 {
        let mut state = self.state.lock();
        state.generation = state.generation.saturating_add(1);
        state.armed = true;
        state.reached = false;
        state.released = false;
        self.waiters.notify_all();
        state.generation
    }

    pub(crate) fn wait_until_reached(&self, generation: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock();
        while state.generation == generation && state.armed && !state.reached && !state.released {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            self.waiters
                .wait_for(&mut state, deadline.saturating_duration_since(now));
        }
        state.generation == generation && state.reached && !state.released
    }

    fn wait_until_reached_blocking(&self, generation: u64) -> bool {
        let mut state = self.state.lock();
        while state.generation == generation && state.armed && !state.reached && !state.released {
            self.waiters.wait(&mut state);
        }
        state.generation == generation && state.reached && !state.released
    }

    pub(crate) fn release(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.generation == generation && state.armed {
            state.released = true;
            state.armed = false;
            self.waiters.notify_all();
        }
    }

    pub(crate) fn maybe_pause(&self) {
        let mut state = self.state.lock();
        if !state.armed || state.released {
            return;
        }
        state.reached = true;
        let generation = state.generation;
        self.waiters.notify_all();
        while state.generation == generation && state.armed && !state.released {
            self.waiters.wait(&mut state);
        }
        if state.generation == generation {
            state.armed = false;
            state.reached = false;
            state.released = false;
        }
        self.waiters.notify_all();
    }
}

pub struct MaintenancePauseHandle {
    slot: Arc<PauseSlot>,
    generation: u64,
}

impl MaintenancePauseHandle {
    #[doc(hidden)]
    pub fn wait_until_reached_blocking(&self) -> bool {
        self.slot.wait_until_reached_blocking(self.generation)
    }

    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        self.slot.wait_until_reached(self.generation, timeout)
    }

    pub fn release(&self) {
        self.slot.release(self.generation);
    }
}

impl Drop for MaintenancePauseHandle {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Default)]
pub(crate) struct PauseRegistry {
    slots: Mutex<HashMap<(VectorIndexRef, PauseWindow), Arc<PauseSlot>>>,
}

impl PauseRegistry {
    pub(crate) fn maybe_pause(&self, index: &VectorIndexRef, window: PauseWindow) {
        let slot = {
            let slots = self.slots.lock();
            slots.get(&(index.clone(), window)).cloned()
        };
        if let Some(slot) = slot {
            slot.maybe_pause();
        }
    }
}

impl VectorStore {
    pub fn arm_maintenance_pause_for_test(
        &self,
        index: &VectorIndexRef,
        window: PauseWindow,
    ) -> MaintenancePauseHandle {
        let slot = self
            .pause_registry()
            .slots
            .lock()
            .entry((index.clone(), window))
            .or_insert_with(|| Arc::new(PauseSlot::new()))
            .clone();
        let generation = slot.arm();
        MaintenancePauseHandle { slot, generation }
    }

    pub fn maybe_pause_ddl_for_test(&self, index: &VectorIndexRef) {
        self.pause_registry().maybe_pause(index, PauseWindow::Ddl);
    }
}
