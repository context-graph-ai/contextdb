//! Test-only maintenance-pause seam. Feature-gated; production builds do not link this module.
//! Mirrors the shape of `contextdb-engine::ApplyPhasePause`.

use crate::store::VectorStore;
use contextdb_core::{RowId, VectorIndexRef, VectorQuantization};
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
        while state.generation == generation && state.armed && !state.reached {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            self.waiters
                .wait_for(&mut state, deadline.saturating_duration_since(now));
        }
        state.generation == generation && state.reached
    }

    pub(crate) fn release(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.generation == generation && state.armed {
            state.released = true;
            self.waiters.notify_all();
        }
    }

    pub(crate) fn maybe_pause(&self) {
        let mut state = self.state.lock();
        if !state.armed || state.released {
            return;
        }
        state.reached = true;
        self.waiters.notify_all();
        while state.armed && !state.released {
            self.waiters.wait(&mut state);
        }
        state.armed = false;
        state.reached = false;
        state.released = false;
        self.waiters.notify_all();
    }
}

pub struct MaintenancePauseHandle {
    slot: Arc<PauseSlot>,
    generation: u64,
}

impl MaintenancePauseHandle {
    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        self.slot.wait_until_reached(self.generation, timeout)
    }

    pub fn release(&self) {
        self.slot.release(self.generation);
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

pub fn estimate_hnsw_final_bytes_for_test(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
) -> usize {
    let entry_bytes = match quantization {
        VectorQuantization::F32 => quantization.storage_bytes(dimension),
        VectorQuantization::SQ8 => dimension.saturating_add(12),
        VectorQuantization::SQ4 => dimension.div_ceil(2).saturating_add(12),
    };
    let exact_key_bytes = entry_bytes
        .saturating_add(std::mem::size_of::<RowId>())
        .saturating_add(64);
    entry_count.saturating_mul(
        entry_bytes
            .saturating_mul(3)
            .saturating_add(exact_key_bytes),
    )
}

pub fn estimate_hnsw_build_reservation_for_test(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
) -> usize {
    let final_bytes = estimate_hnsw_final_bytes_for_test(entry_count, dimension, quantization);
    let (m, ef_construction, max_level_bound) = match quantization {
        VectorQuantization::F32 => match entry_count {
            0..=5000 => (16usize, 200usize, 16usize),
            5001..=50000 => (24, 400, 16),
            _ => (16, 200, 16),
        },
        _ => match entry_count {
            0..=5000 => (8usize, 32usize, 16usize),
            5001..=50000 => (12, 64, 16),
            _ => (12, 64, 16),
        },
    };
    let stored_vector_bytes = quantization.storage_bytes(dimension);
    let word = std::mem::size_of::<usize>();
    let sorted_entry_refs = entry_count.saturating_mul(word);
    let cloned_vectors_and_refs = entry_count.saturating_mul(
        stored_vector_bytes
            .saturating_add(word.saturating_mul(5))
            .saturating_add(std::mem::size_of::<RowId>()),
    );
    let map_and_exact_key_overhead = entry_count.saturating_mul(
        std::mem::size_of::<RowId>()
            .saturating_add(word.saturating_mul(3))
            .saturating_add(64),
    );
    let graph_link_upper_bound = entry_count
        .saturating_mul(m)
        .saturating_mul(max_level_bound)
        .saturating_mul(word.saturating_add(std::mem::size_of::<f32>()));
    let construction_scratch = entry_count.min(ef_construction).saturating_mul(
        word.saturating_mul(6)
            .saturating_add(std::mem::size_of::<f32>().saturating_mul(2)),
    );
    final_bytes
        .saturating_add(sorted_entry_refs)
        .saturating_add(cloned_vectors_and_refs)
        .saturating_add(map_and_exact_key_overhead)
        .saturating_add(graph_link_upper_bound)
        .saturating_add(construction_scratch)
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
}
