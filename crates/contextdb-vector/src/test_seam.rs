//! Test-only maintenance-pause seam. Feature-gated; production builds do not link this module.
//! Mirrors the shape of `contextdb-engine::ApplyPhasePause`.

use crate::store::VectorStore;
use contextdb_core::{VectorIndexRef, VectorPartitionKey, VectorQuantization};
use parking_lot::{Condvar, Mutex};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

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

/// How many candidates an already-built HNSW graph hands one search back.
///
/// A live index whose graph returns fewer usable candidates than the caller
/// asked for is an ordinary regime: on a large graph the search reaches only
/// part of it, and rows retired since the graph was built drop out of what it
/// returns. Reaching that regime for real needs tens of thousands of vectors
/// or a writer racing the reader, neither of which a test can pin. This cap
/// reproduces it on a small index by handing the search a shortened candidate
/// list.
///
/// The cap changes nothing else: the graph, the stored vectors, the scoring
/// and the ranking are untouched, and it applies to every search of the capped
/// index while armed, so both read routes see the identical candidate list and
/// neither is told what the right answer is.
#[derive(Debug, Default)]
pub(crate) struct GraphCandidateCapSlot {
    cap: Mutex<Option<usize>>,
}

type GraphCandidateCapKey = (VectorIndexRef, Option<VectorPartitionKey>);
type GraphCandidateCapSlots = HashMap<GraphCandidateCapKey, Arc<GraphCandidateCapSlot>>;

impl GraphCandidateCapSlot {
    fn cap(&self) -> Option<usize> {
        *self.cap.lock()
    }

    fn set(&self, cap: Option<usize>) {
        *self.cap.lock() = cap;
    }
}

#[derive(Default)]
pub(crate) struct GraphCandidateCapRegistry {
    slots: Mutex<GraphCandidateCapSlots>,
}

impl GraphCandidateCapRegistry {
    /// How many graph candidates an armed index's partition may keep.
    pub(crate) fn graph_candidate_cap(
        &self,
        index: &VectorIndexRef,
        partition: Option<&VectorPartitionKey>,
    ) -> Option<usize> {
        let slot = {
            let slots = self.slots.lock();
            partition
                .and_then(|partition| {
                    slots
                        .get(&(index.clone(), Some(partition.clone())))
                        .cloned()
                })
                .or_else(|| slots.get(&(index.clone(), None)).cloned())
        };
        slot.and_then(|slot| slot.cap())
    }

    fn slot(
        &self,
        index: &VectorIndexRef,
        partition: Option<&VectorPartitionKey>,
    ) -> Arc<GraphCandidateCapSlot> {
        self.slots
            .lock()
            .entry((index.clone(), partition.cloned()))
            .or_insert_with(|| Arc::new(GraphCandidateCapSlot::default()))
            .clone()
    }
}

/// Restores the index's unshortened graph candidates when dropped, so a capped
/// index cannot leak its cap into a later search.
pub struct GraphCandidateCapHandle {
    slot: Arc<GraphCandidateCapSlot>,
}

impl Drop for GraphCandidateCapHandle {
    fn drop(&mut self) {
        self.slot.set(None);
    }
}

pub fn estimate_hnsw_final_bytes_for_test(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
) -> usize {
    crate::mem::estimate_hnsw_bytes(entry_count, dimension, quantization)
}

pub fn estimate_hnsw_build_reservation_for_test(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
) -> usize {
    let policy = crate::store::VectorIndexLayout::unpartitioned(dimension, quantization)
        .resolve_policy(entry_count, 1);
    crate::mem::estimate_hnsw_build_reservation(entry_count, dimension, quantization, policy)
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

    /// Cap how many candidates the index's graph hands each search back,
    /// until the returned handle is dropped.
    pub fn cap_graph_candidates_for_test(
        &self,
        index: &VectorIndexRef,
        cap: usize,
    ) -> GraphCandidateCapHandle {
        let slot = self.graph_candidate_caps().slot(index, None);
        slot.set(Some(cap));
        GraphCandidateCapHandle { slot }
    }

    /// Shorten one partition's graph result without changing sibling routes.
    pub fn cap_partition_graph_candidates_for_test(
        &self,
        index: &VectorIndexRef,
        partition: &VectorPartitionKey,
        cap: usize,
    ) -> GraphCandidateCapHandle {
        let slot = self.graph_candidate_caps().slot(index, Some(partition));
        slot.set(Some(cap));
        GraphCandidateCapHandle { slot }
    }

    pub fn maybe_pause_ddl_for_test(&self, index: &VectorIndexRef) {
        self.pause_registry().maybe_pause(index, PauseWindow::Ddl);
    }
}

/// Actual immutable input and maximum single-record export bounds. This reports
/// the producer's bytes and controls; it selects no fixture size or policy.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct SavedTopologyBoundaryForTest {
    pub source_bytes: usize,
    pub retained_control_bytes: usize,
    pub points: usize,
    pub dimension: usize,
    pub quantization: VectorQuantization,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub max_point_bytes: usize,
    pub max_native_encoded_point_bytes: usize,
    pub max_workspace_bytes: usize,
    pub max_exact_key_bytes: usize,
}
#[doc(hidden)]
pub fn saved_topology_boundary_for_test(
    bytes: &[u8], dimension: usize, quantization: VectorQuantization,
    checkpoint: &mut dyn FnMut() -> contextdb_core::Result<()>,
) -> contextdb_core::Result<SavedTopologyBoundaryForTest> {
    use crate::hnsw::saved_topology::{SavedPointCursor, SavedTopology};
    let topology = SavedTopology::inspect(bytes, dimension, quantization, checkpoint)?;
    let mut cursor = SavedPointCursor::default();
    let mut workspace = 0;
    let mut exact = 0;
    let mut encoded = 0;
    while let Some(point) = topology.next_point(&mut cursor)? {
        checkpoint()?;
        workspace = workspace.max(point.workspace_bytes()?);
        exact = exact.max(point.exact_key().len());
        // Use the same point codec the exporter writes, with this exact
        // immutable point. This measures emitted bytes rather than a bound.
        let mut bytes = Vec::new();
        let result = match point.decode()? {
            crate::hnsw::paged_builder::PagedVectorPoint::F32(point) =>
                crate::hnsw::paged_builder::encode_paged_point_into(&point, &mut bytes),
            crate::hnsw::paged_builder::PagedVectorPoint::Quantized(point) =>
                crate::hnsw::paged_builder::encode_paged_point_into(&point, &mut bytes),
        };
        result.map_err(|error| contextdb_core::Error::Other(format!("saved point boundary encoding failed: {error}")))?;
        encoded = encoded.max(bytes.len());
    }
    Ok(SavedTopologyBoundaryForTest {
        source_bytes: bytes.len(), retained_control_bytes: std::mem::size_of_val(&topology),
        points: topology.state.points, dimension, quantization,
        m: topology.m, ef_construction: topology.ef_construction, ef_search: topology.ef_search,
        max_point_bytes: topology.max_point_bytes, max_native_encoded_point_bytes: encoded,
        max_workspace_bytes: workspace,
        max_exact_key_bytes: exact,
    })
}
