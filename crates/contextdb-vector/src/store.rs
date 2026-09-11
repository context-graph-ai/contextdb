use crate::memory_budget::{MemoryBudget, VectorWorkspaceReservation};
use crate::{HnswIndex, quantized::StoredVectorEntry};
use contextdb_core::{
    Error, Lsn, Result, RowId, SnapshotId, TxId, VectorEntry, VectorIndexRef, VectorPartitionKey,
    VectorQuantization, VectorSearchMode,
};
#[cfg(any(test, feature = "test-seams"))]
use parking_lot::Condvar;
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

// The test build forces collisions in the actual admission sets. The ordered
// partition registry still owns identity through complete typed keys.
#[cfg(not(test))]
type PartitionKeySet = HashSet<VectorPartitionKey>;
#[cfg(test)]
type PartitionKeySet =
    HashSet<VectorPartitionKey, std::hash::BuildHasherDefault<tests::PartitionCollisionHasher>>;

type SnapshotCompatibleHnswLayersResult<R, E> = std::result::Result<
    (
        VectorGraphLayerAvailability,
        Vec<(Option<VectorGraphGeneration>, R)>,
    ),
    E,
>;

const MAINTENANCE_SAMPLE_NO_PRIOR_TAIL: u64 = 1;
const MAINTENANCE_SAMPLE_MOVED_BASE: u64 = 2;
const MAINTENANCE_SAMPLE_MOVED_CHANGE: u64 = 3;
const MAINTENANCE_SAMPLE_REPLACED_EMPTY_TAIL: u64 = 4;
const MAINTENANCE_SAMPLE_ADDED_TO_BASE: u64 = 5;

/// Receipt of vector lifecycle work. These counters record completed events,
/// never a derived view of the store's current resident state.
#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorPassiveActivityCounters {
    pub raw_partition_loads: u64,
    /// Requests for the whole entry slice, including exact scan sources.
    pub whole_entry_slice_requests: u64,
    /// Slice lengths exposed by those requests, not a distance-evaluation count.
    pub whole_entry_slice_entries_exposed: u64,
    pub raw_candidate_point_loads: u64,
    pub raw_candidate_point_body_bytes: u64,
    pub sealed_generation_loads: u64,
    pub hnsw_builds: u64,
    pub hnsw_repairs_or_compactions: u64,
    pub raw_partition_evictions: u64,
    pub sealed_generation_evictions: u64,
}

/// Exact retained-owner categories sampled without changing residency.
#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorMemoryOwnershipSnapshot {
    pub raw_vector_bodies: usize,
    pub raw_reverse_directory: usize,
    pub partition_vectors: usize,
    pub base_graph: usize,
    pub change_graph: usize,
    pub mutable_tail: usize,
    pub typed_partition_keys: usize,
    pub retired_pinned_graph_bytes: usize,
    pub temporary_workspace: usize,
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorMemoryWorkspacePhaseForTest {
    CompactionPreparedBeforePublication,
}

#[cfg(any(test, feature = "test-seams"))]
#[derive(Default)]
struct VectorPassiveActivity {
    raw_partition_loads: AtomicU64,
    whole_entry_slice_requests: AtomicU64,
    whole_entry_slice_entries_exposed: AtomicU64,
    raw_candidate_point_loads: AtomicU64,
    raw_candidate_point_body_bytes: AtomicU64,
    sealed_generation_loads: AtomicU64,
    hnsw_builds: AtomicU64,
    hnsw_repairs_or_compactions: AtomicU64,
    raw_partition_evictions: AtomicU64,
    sealed_generation_evictions: AtomicU64,
}

#[cfg(any(test, feature = "test-seams"))]
impl VectorPassiveActivity {
    fn snapshot(&self) -> VectorPassiveActivityCounters {
        VectorPassiveActivityCounters {
            raw_partition_loads: self.raw_partition_loads.load(Ordering::SeqCst),
            whole_entry_slice_requests: self.whole_entry_slice_requests.load(Ordering::SeqCst),
            whole_entry_slice_entries_exposed: self
                .whole_entry_slice_entries_exposed
                .load(Ordering::SeqCst),
            raw_candidate_point_loads: self.raw_candidate_point_loads.load(Ordering::SeqCst),
            raw_candidate_point_body_bytes: self
                .raw_candidate_point_body_bytes
                .load(Ordering::SeqCst),
            sealed_generation_loads: self.sealed_generation_loads.load(Ordering::SeqCst),
            hnsw_builds: self.hnsw_builds.load(Ordering::SeqCst),
            hnsw_repairs_or_compactions: self.hnsw_repairs_or_compactions.load(Ordering::SeqCst),
            raw_partition_evictions: self.raw_partition_evictions.load(Ordering::SeqCst),
            sealed_generation_evictions: self.sealed_generation_evictions.load(Ordering::SeqCst),
        }
    }
}

/// A one-shot maintenance-build pause. Dropping it releases a reached worker
/// and disarms an unreached request.
#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
pub struct VectorMaintenanceProgressPauseHandle {
    slot: Arc<MaintenanceProgressPauseSlot>,
    generation: u64,
}

/// Real preparation checkpoints used by the durable-generation lock-scope
/// proof. Each checkpoint is immediately before the named production work.
#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorMaintenancePreparationPhaseForTest {
    LoadAuthoritativeRawVectors,
    EncodeGraph,
    HashStateDigest,
    PrepareDurableGeneration,
}

/// The callback boundary at which an HNSW layer is handed to query code.
#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorGraphCallbackPhaseForTest {
    BeforeSearchCallback,
}

/// Durable boundary after Redb T1 and before the verified journal-prefix T2.
#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorJournalTruncationPhaseForTest {
    CatalogDurableBeforeCoveredJournalDelete,
}

#[cfg(any(test, feature = "test-seams"))]
#[derive(Default)]
struct OneShotFaultState {
    generation: u64,
    armed: bool,
}

#[cfg(any(test, feature = "test-seams"))]
struct OneShotFaultSlot {
    state: Mutex<OneShotFaultState>,
}

#[cfg(any(test, feature = "test-seams"))]
impl OneShotFaultSlot {
    fn arm(&self) -> u64 {
        let mut state = self.state.lock();
        state.generation = state.generation.saturating_add(1);
        state.armed = true;
        state.generation
    }

    fn take(&self) -> bool {
        let mut state = self.state.lock();
        std::mem::replace(&mut state.armed, false)
    }

    fn disarm(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.generation == generation {
            state.armed = false;
        }
    }
}

/// One-shot crash injection guard. Dropping an unreached guard disarms it.
#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
pub struct VectorJournalTruncationFaultHandle {
    slot: Arc<OneShotFaultSlot>,
    generation: u64,
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for VectorJournalTruncationFaultHandle {
    fn drop(&mut self) {
        self.slot.disarm(self.generation);
    }
}

/// A one-shot lifecycle pause. Dropping it releases a reached worker and
/// disarms an unreached request.
#[doc(hidden)]
#[cfg(any(test, feature = "test-seams"))]
pub struct VectorLifecyclePauseHandle {
    slot: Arc<MaintenanceProgressPauseSlot>,
    generation: u64,
}

#[cfg(any(test, feature = "test-seams"))]
impl VectorLifecyclePauseHandle {
    pub fn wait_until_reached_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> std::result::Result<(), &'static str> {
        self.slot
            .wait_until_reached(self.generation, timeout)
            .then_some(())
            .ok_or("one-shot vector lifecycle pause was not reached before the timeout")
    }

    pub fn release(&self) {
        self.slot.release(self.generation);
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for VectorLifecyclePauseHandle {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl VectorMaintenanceProgressPauseHandle {
    pub fn wait_until_reached_blocking(&self) -> bool {
        self.slot.wait_until_reached_blocking(self.generation)
    }

    pub fn wait_until_reached(&self, timeout: std::time::Duration) -> bool {
        self.slot.wait_until_reached(self.generation, timeout)
    }

    pub fn release(&self) {
        self.slot.release(self.generation);
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for VectorMaintenanceProgressPauseHandle {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(any(test, feature = "test-seams"))]
#[derive(Default)]
struct MaintenanceProgressPauseState {
    generation: u64,
    after_vectors: usize,
    armed: bool,
    reached: bool,
    released: bool,
}

#[cfg(any(test, feature = "test-seams"))]
struct MaintenanceProgressPauseSlot {
    state: Mutex<MaintenanceProgressPauseState>,
    changed: Condvar,
}

#[cfg(any(test, feature = "test-seams"))]
impl MaintenanceProgressPauseSlot {
    fn new() -> Self {
        Self {
            state: Mutex::new(MaintenanceProgressPauseState::default()),
            changed: Condvar::new(),
        }
    }

    fn arm(&self, after_vectors: usize) -> u64 {
        let mut state = self.state.lock();
        state.generation = state.generation.saturating_add(1);
        state.after_vectors = after_vectors;
        state.armed = true;
        state.reached = false;
        state.released = false;
        self.changed.notify_all();
        state.generation
    }

    fn requested_vectors(&self) -> Option<usize> {
        let mut state = self.state.lock();
        if state.armed && state.released {
            state.armed = false;
            state.reached = false;
            state.released = false;
            self.changed.notify_all();
            return None;
        }
        state.armed.then_some(state.after_vectors)
    }

    fn pause_after(&self, processed_vectors: usize) {
        let mut state = self.state.lock();
        if !state.armed
            || state.reached
            || state.released
            || processed_vectors < state.after_vectors
        {
            return;
        }
        state.reached = true;
        self.changed.notify_all();
        while state.armed && !state.released {
            self.changed.wait(&mut state);
        }
        state.armed = false;
        state.reached = false;
        state.released = false;
        self.changed.notify_all();
    }

    fn wait_until_reached(&self, generation: u64, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self.state.lock();
        while state.generation == generation && state.armed && !state.reached {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            self.changed
                .wait_for(&mut state, deadline.saturating_duration_since(now));
        }
        state.generation == generation && state.reached
    }

    fn wait_until_reached_blocking(&self, generation: u64) -> bool {
        let mut state = self.state.lock();
        while state.generation == generation && state.armed && !state.reached {
            self.changed.wait(&mut state);
        }
        state.generation == generation && state.reached
    }

    fn release(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.generation == generation && state.armed {
            if state.reached {
                state.released = true;
            } else {
                state.armed = false;
                state.released = false;
            }
            self.changed.notify_all();
        }
    }
}

thread_local! {
    static HELD_MAINTENANCE_LOCKS: RefCell<Vec<(StoreId, VectorIndexRef, VectorPartitionKey)>> = const { RefCell::new(Vec::new()) };
    static HELD_BULK_READ_LOCKS: RefCell<Vec<StoreId>> = const { RefCell::new(Vec::new()) };
    static HELD_BULK_WRITE_LOCKS: RefCell<Vec<StoreId>> = const { RefCell::new(Vec::new()) };
}

struct MaintenanceStackGuard {
    store_id: StoreId,
    index: VectorIndexRef,
    partition_key: VectorPartitionKey,
}

impl MaintenanceStackGuard {
    fn new(store_id: StoreId, index: &VectorIndexRef, partition_key: &VectorPartitionKey) -> Self {
        HELD_MAINTENANCE_LOCKS.with(|held| {
            held.borrow_mut()
                .push((store_id, index.clone(), partition_key.clone()))
        });
        Self {
            store_id,
            index: index.clone(),
            partition_key: partition_key.clone(),
        }
    }
}

impl Drop for MaintenanceStackGuard {
    fn drop(&mut self) {
        HELD_MAINTENANCE_LOCKS.with(|held| {
            let popped = held.borrow_mut().pop();
            debug_assert_eq!(
                popped,
                Some((
                    self.store_id,
                    self.index.clone(),
                    self.partition_key.clone()
                ))
            );
        });
    }
}

struct BulkReadStackGuard {
    store_id: StoreId,
}

impl BulkReadStackGuard {
    fn new(store_id: StoreId) -> Self {
        HELD_BULK_READ_LOCKS.with(|held| held.borrow_mut().push(store_id));
        Self { store_id }
    }
}

impl Drop for BulkReadStackGuard {
    fn drop(&mut self) {
        HELD_BULK_READ_LOCKS.with(|held| {
            let popped = held.borrow_mut().pop();
            debug_assert_eq!(popped, Some(self.store_id));
        });
    }
}

struct BulkWriteStackGuard {
    store_id: StoreId,
}

impl BulkWriteStackGuard {
    fn new(store_id: StoreId) -> Self {
        HELD_BULK_WRITE_LOCKS.with(|held| held.borrow_mut().push(store_id));
        Self { store_id }
    }
}

impl Drop for BulkWriteStackGuard {
    fn drop(&mut self) {
        HELD_BULK_WRITE_LOCKS.with(|held| {
            let popped = held.borrow_mut().pop();
            debug_assert_eq!(popped, Some(self.store_id));
        });
    }
}

/// What the bounded HNSW gate needs to know about one partition's stored
/// entries. `live_count` is evaluated at the reading snapshot; later entries
/// do not invalidate an older compatible graph generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundedIndexEligibility {
    pub entry_count: usize,
    pub live_count: usize,
}

#[derive(Default)]
struct PartitionVectors {
    entries: Vec<StoredVectorEntry>,
    by_row: HashMap<RowId, Vec<usize>>,
}

impl PartitionVectors {
    fn push(&mut self, entry: StoredVectorEntry) {
        let position = self.entries.len();
        self.by_row.entry(entry.row_id).or_default().push(position);
        self.entries.push(entry);
    }

    fn rebuild_row_positions(&mut self) {
        self.by_row.clear();
        for (position, entry) in self.entries.iter().enumerate() {
            self.by_row.entry(entry.row_id).or_default().push(position);
        }
    }

    fn retained_map_bytes(&self) -> usize {
        self.by_row
            .capacity()
            .saturating_mul(std::mem::size_of::<(RowId, Vec<usize>)>())
            .saturating_add(self.by_row.values().fold(0usize, |bytes, positions| {
                bytes.saturating_add(
                    positions
                        .capacity()
                        .saturating_mul(std::mem::size_of::<usize>()),
                )
            }))
    }
}

/// Identity and durable coverage of a sealed graph generation.  The bytes of
/// the graph deliberately stay outside this value: the codec owns their
/// representation and hands this layer an already-verified `HnswIndex`.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorGraphGeneration {
    pub generation_id: u64,
    pub covered_tx: TxId,
    pub covered_lsn: Lsn,
    pub durable_bytes: u64,
    pub policy_revision: u64,
    pub hnsw_m: u32,
    pub hnsw_ef_construction: u32,
}

/// What a store consumer may inspect without borrowing a graph.  In
/// particular, inspection never forces a lazy graph to load or decode.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorGraphGenerationStatus {
    pub base: Option<VectorGraphGeneration>,
    pub change: Option<VectorGraphGeneration>,
    pub dormant_base: Option<VectorGraphGeneration>,
    pub dormant_change: Option<VectorGraphGeneration>,
    pub base_resident: bool,
    pub change_resident: bool,
    pub fresh_tail_entries: usize,
    pub retained_generations: usize,
    pub pending_generation_count: usize,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorGraphLayerAvailability {
    Ready,
    Dormant,
    Unavailable,
}

/// A passive, route-local reason that forbids indexed use until maintenance
/// has published a replacement from authoritative raw vectors.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorRouteQuarantineReason {
    CorruptBase,
    CorruptChanges,
    IncompatibleFormat,
}

impl VectorRouteQuarantineReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CorruptBase => "corrupt_base",
            Self::CorruptChanges => "corrupt_changes",
            Self::IncompatibleFormat => "incompatible_format",
        }
    }
}

/// Stable reason retained after a maintenance attempt cannot be admitted or
/// completed. Inspection keeps reporting this mark until a later successful
/// publication clears it; a transient worker return must not erase the only
/// actionable explanation of a stalled route.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMaintenanceFailure {
    MemoryLimit,
    DiskLimit,
    BuildFailure,
}

/// Safe typed context retained with a partition-local maintenance failure.
///
/// The fields deliberately exclude caller values, paths, and free-form lower
/// layer errors. Budget failures retain the numbers an operator needs to size
/// the existing limits; other failures retain a stable operation and message
/// that can be reported without disclosing store contents.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMaintenanceFailureDetails {
    pub failure: VectorMaintenanceFailure,
    pub operation: &'static str,
    pub requested_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub current_bytes: Option<u64>,
    pub budget_limit_bytes: Option<u64>,
    pub message: &'static str,
    pub recovery_instruction: &'static str,
}

impl VectorMaintenanceFailure {
    pub fn reason(self) -> &'static str {
        match self {
            Self::MemoryLimit => "memory_limit",
            Self::DiskLimit => "disk_limit",
            Self::BuildFailure => "build_failure",
        }
    }

    pub fn recovery_action(self) -> &'static str {
        match self {
            Self::MemoryLimit => "raise_memory_limit",
            Self::DiskLimit => "raise_disk_limit_or_free_space",
            Self::BuildFailure => "inspect_build_failure",
        }
    }

    #[doc(hidden)]
    pub fn from_error(error: &Error) -> Self {
        match error {
            Error::MemoryBudgetExceeded { .. } => Self::MemoryLimit,
            Error::DiskBudgetExceeded { .. } => Self::DiskLimit,
            _ => Self::BuildFailure,
        }
    }
}

impl VectorMaintenanceFailureDetails {
    const MEMORY_RECOVERY: &'static str = "raise the limit with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>), then run another maintenance cycle";
    const DISK_RECOVERY: &'static str = "raise the limit with SET DISK_LIMIT <SIZE> (or reopen with --disk-limit <SIZE>), or free disk space, then run another maintenance cycle";
    const BUILD_RECOVERY: &'static str = "inspect the reported operation and message, repair the underlying store failure, then run another maintenance cycle";

    fn safe_operation(operation: &str) -> &'static str {
        if operation.starts_with("build_hnsw@") {
            return "build_hnsw";
        }
        if operation.starts_with("prepare_authoritative_purge_hnsw@") {
            return "prepare_authoritative_purge_hnsw";
        }
        match operation {
            "prepare_vector_generation" => "prepare_vector_generation",
            "finalize_repaired_vector_partition" => "finalize_repaired_vector_partition",
            "truncate_vector_partition_journal" => "truncate_vector_partition_journal",
            "load_durable_vector_generation" => "load_durable_vector_generation",
            "prepare_hnsw_tail" => "prepare_hnsw_tail",
            _ => "vector_maintenance",
        }
    }

    fn usize_as_u64(value: usize) -> u64 {
        u64::try_from(value).unwrap_or(u64::MAX)
    }

    #[doc(hidden)]
    pub fn from_failure(failure: VectorMaintenanceFailure) -> Self {
        match failure {
            VectorMaintenanceFailure::MemoryLimit => Self {
                failure,
                operation: "vector_maintenance",
                requested_bytes: None,
                available_bytes: None,
                current_bytes: None,
                budget_limit_bytes: None,
                message: "vector maintenance exceeded the declared memory limit",
                recovery_instruction: Self::MEMORY_RECOVERY,
            },
            VectorMaintenanceFailure::DiskLimit => Self {
                failure,
                operation: "vector_maintenance",
                requested_bytes: None,
                available_bytes: None,
                current_bytes: None,
                budget_limit_bytes: None,
                message: "vector maintenance exceeded the declared disk limit",
                recovery_instruction: Self::DISK_RECOVERY,
            },
            VectorMaintenanceFailure::BuildFailure => Self {
                failure,
                operation: "build_or_publish_vector_generation",
                requested_bytes: None,
                available_bytes: None,
                current_bytes: None,
                budget_limit_bytes: None,
                message: "maintained vector generation construction or publication failed",
                recovery_instruction: Self::BUILD_RECOVERY,
            },
        }
    }

    #[doc(hidden)]
    pub fn from_error(error: &Error) -> Self {
        match error {
            Error::MemoryBudgetExceeded {
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                ..
            } => Self {
                failure: VectorMaintenanceFailure::MemoryLimit,
                operation: Self::safe_operation(operation),
                requested_bytes: Some(Self::usize_as_u64(*requested_bytes)),
                available_bytes: Some(Self::usize_as_u64(*available_bytes)),
                current_bytes: Some(Self::usize_as_u64(
                    (*budget_limit_bytes).saturating_sub(*available_bytes),
                )),
                budget_limit_bytes: Some(Self::usize_as_u64(*budget_limit_bytes)),
                message: "vector maintenance could not reserve its required memory",
                recovery_instruction: Self::MEMORY_RECOVERY,
            },
            Error::DiskBudgetExceeded {
                operation,
                current_bytes,
                budget_limit_bytes,
                ..
            } => Self {
                failure: VectorMaintenanceFailure::DiskLimit,
                operation: Self::safe_operation(operation),
                requested_bytes: None,
                available_bytes: Some((*budget_limit_bytes).saturating_sub(*current_bytes)),
                current_bytes: Some(*current_bytes),
                budget_limit_bytes: Some(*budget_limit_bytes),
                message: "vector maintenance could not admit its durable generation",
                recovery_instruction: Self::DISK_RECOVERY,
            },
            Error::UnknownVectorIndex { .. } => Self {
                failure: VectorMaintenanceFailure::BuildFailure,
                operation: "locate_vector_partition",
                requested_bytes: None,
                available_bytes: None,
                current_bytes: None,
                budget_limit_bytes: None,
                message: "the sampled vector partition was no longer present in memory",
                recovery_instruction: "run another maintenance cycle; an absent route needs no in-memory failure mark",
            },
            Error::StoreCorrupted { .. } => Self {
                failure: VectorMaintenanceFailure::BuildFailure,
                operation: "verify_vector_maintenance_state",
                requested_bytes: None,
                available_bytes: None,
                current_bytes: None,
                budget_limit_bytes: None,
                message: "durable vector maintenance state failed integrity validation",
                recovery_instruction: "restore the store from a healthy backup or sync peer, then run another maintenance cycle",
            },
            Error::Other(message)
                if message.contains("journal truncation")
                    && message.contains("durable catalog T1") =>
            {
                Self {
                    failure: VectorMaintenanceFailure::BuildFailure,
                    operation: "truncate_vector_partition_journal",
                    requested_bytes: None,
                    available_bytes: None,
                    current_bytes: None,
                    budget_limit_bytes: None,
                    message: "the maintained graph is durable and covered journal cleanup is pending",
                    recovery_instruction: "run another maintenance cycle; cleanup resumes from the durable catalog",
                }
            }
            Error::Other(message) if message.contains("encode vector generation") => Self {
                failure: VectorMaintenanceFailure::BuildFailure,
                operation: "encode_vector_generation",
                requested_bytes: None,
                available_bytes: None,
                current_bytes: None,
                budget_limit_bytes: None,
                message: "the maintained vector generation could not be encoded",
                recovery_instruction: Self::BUILD_RECOVERY,
            },
            Error::Other(message)
                if message.contains("durable HNSW") || message.contains("HNSW build") =>
            {
                Self {
                    failure: VectorMaintenanceFailure::BuildFailure,
                    operation: "build_or_validate_hnsw_generation",
                    requested_bytes: None,
                    available_bytes: None,
                    current_bytes: None,
                    budget_limit_bytes: None,
                    message: "maintained HNSW construction or durable validation failed",
                    recovery_instruction: Self::BUILD_RECOVERY,
                }
            }
            Error::Other(message) if message.starts_with("storage error:") => Self {
                failure: VectorMaintenanceFailure::BuildFailure,
                operation: "read_or_publish_vector_storage",
                requested_bytes: None,
                available_bytes: None,
                current_bytes: None,
                budget_limit_bytes: None,
                message: "the durable vector storage operation failed",
                recovery_instruction: "verify filesystem health and store integrity, then run another maintenance cycle",
            },
            Error::Other(message) if message.contains("catalog points to a missing") => Self {
                failure: VectorMaintenanceFailure::BuildFailure,
                operation: "verify_vector_generation_catalog",
                requested_bytes: None,
                available_bytes: None,
                current_bytes: None,
                budget_limit_bytes: None,
                message: "the durable vector catalog references a missing generation",
                recovery_instruction: "restore the store from a healthy backup or sync peer, then run another maintenance cycle",
            },
            _ => Self::from_failure(VectorMaintenanceFailure::BuildFailure),
        }
    }

    #[doc(hidden)]
    pub fn from_published_cleanup_error(error: &Error) -> Self {
        let mut details = Self::from_error(error);
        details.message = match details.failure {
            VectorMaintenanceFailure::MemoryLimit => {
                "the maintained graph is durable and journal cleanup could not reserve its required memory"
            }
            VectorMaintenanceFailure::DiskLimit => {
                "the maintained graph is durable and journal cleanup was refused by the disk limit"
            }
            VectorMaintenanceFailure::BuildFailure => {
                "the maintained graph is durable and covered journal cleanup is pending"
            }
        };
        details
    }

    pub fn reason(self) -> &'static str {
        self.failure.reason()
    }

    pub fn recovery_action(self) -> &'static str {
        self.failure.recovery_action()
    }
}

struct SealedHnswGeneration {
    identity: VectorGraphGeneration,
    graph: HnswIndex,
    bytes: usize,
    accountant: Option<Arc<dyn MemoryBudget>>,
}

/// One retained-memory reservation whose release belongs to the object that
/// holds the corresponding allocation. Keeping the reservation beside the
/// retained state prevents DDL cleanup from having to reconstruct who paid
/// for it from vector values after the index identity is gone.
struct RetainedMemoryCharge {
    bytes: usize,
    accountant: Arc<dyn MemoryBudget>,
}

impl Drop for RetainedMemoryCharge {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
        self.bytes = 0;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct VectorDirectoryOwnership {
    raw_reverse_directory: usize,
    partition_vectors: usize,
    typed_partition_keys: usize,
}

impl VectorDirectoryOwnership {
    fn total(self) -> usize {
        self.raw_reverse_directory
            .saturating_add(self.partition_vectors)
            .saturating_add(self.typed_partition_keys)
    }
}

/// A durable catalog entry that is known to be valid but whose graph pages are
/// not resident in this process.  The vector crate intentionally knows no
/// file format or persistence implementation; the owner of the descriptor
/// supplies the loader.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DormantVectorGraphGeneration {
    pub identity: VectorGraphGeneration,
    /// Bytes that become resident if this generation is loaded.  This is an
    /// admission fact, not a claim about durable file size.
    pub resident_bytes: usize,
    /// Resident graph plus the persisted graph buffer that coexists with it
    /// during decode. A bounded caller reserves this known minimum before the
    /// loader performs any persistence I/O, then retains only owned residency.
    pub load_bytes: usize,
}

/// An owned, verified graph returned by a persistence-owned loader. The
/// loader either reserves `resident_bytes` itself or adopts the caller's
/// already-admitted reservation before returning, then transfers that one
/// owner to the store through the supplied accountant.
#[doc(hidden)]
pub struct LoadedVectorGraphGeneration {
    pub graph: HnswIndex,
    pub resident_bytes: usize,
    pub accountant: Arc<dyn MemoryBudget>,
    /// Restart may replay the bounded journal suffix while it loads the base.
    /// This graph was created fresh in this process and is therefore the one
    /// mutable layer later commits may extend.
    pub fresh_tail: Option<LoadedVectorFreshTail>,
}

#[doc(hidden)]
pub struct LoadedVectorFreshTail {
    pub graph: HnswIndex,
    pub resident_bytes: usize,
    pub accountant: Arc<dyn MemoryBudget>,
    /// Highest durable journal LSN represented by this replay image. Hot
    /// commits at or below it were persisted before their in-memory apply and
    /// must not be inserted a second time after this image is published.
    pub replayed_through_lsn: Lsn,
}

/// A persistence-owned loader asks its caller either to check cancellation or
/// to admit an additional load allocation learned after lightweight metadata
/// I/O and before that allocation is materialized.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DormantVectorLoadRequest {
    Checkpoint,
    Reserve(usize),
    Progress { done: usize, total: usize },
}

/// The persistence boundary for lazy graph residency.  It deliberately has
/// no byte-format methods: codecs remain outside the vector crate.  A loader
/// either returns one complete, verified owned graph or an error; it never
/// exposes partial graph state to search.
#[doc(hidden)]
pub trait DormantVectorGraphLoader: Send + Sync {
    fn load(
        &self,
        generation: DormantVectorGraphGeneration,
        caller_reserved_load_bytes: usize,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedVectorGraphGeneration>;
}

/// Lightweight identity retained at open for one durable raw vector. It is
/// sufficient for counts, snapshot visibility, row membership, and a later
/// point load, but deliberately contains no vector body.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawVectorDirectoryEntry {
    pub row_id: RowId,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
    pub lsn: Lsn,
}

/// The identity of one stored vector version: the `(row_id, created_tx, lsn)`
/// triple that also names its relational row version, qualified by the index
/// that holds it. It carries no body and no partition, so naming a version --
/// to prune it durably, or to match it against a directory -- never requires
/// faulting a dormant partition in.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VectorVersionIdentity {
    pub index: VectorIndexRef,
    pub row_id: RowId,
    pub created_tx: TxId,
    pub lsn: Lsn,
}

impl VectorVersionIdentity {
    pub fn from_entry(entry: &VectorEntry) -> Self {
        Self {
            index: entry.index.clone(),
            row_id: entry.row_id,
            created_tx: entry.created_tx,
            lsn: entry.lsn,
        }
    }

    fn version(&self) -> (RowId, TxId, Lsn) {
        (self.row_id, self.created_tx, self.lsn)
    }
}

/// A loader returns one complete partition and the final shared-memory charge
/// it reserved. `workspace_bytes` covers the decoded F32 bodies carried across
/// this crate boundary; the store releases that part only after it has built
/// the declared stored representation. The store takes ownership of
/// `resident_bytes` only after it has validated every entry against the
/// directory.
#[doc(hidden)]
pub struct LoadedRawVectorPartition {
    pub entries: Vec<VectorEntry>,
    pub resident_bytes: usize,
    pub workspace_bytes: usize,
    pub accountant: Arc<dyn MemoryBudget>,
}

/// One decoded raw-vector body held only for the duration of candidate
/// scoring. Loader-owned reservations are returned on drop. A bounded caller
/// instead owns `transient_bytes` through its request reservation and releases
/// that charge after dropping this value.
#[doc(hidden)]
pub struct LoadedRawVectorCandidate {
    entry: VectorEntry,
    transient_bytes: usize,
    loader_accountant: Option<Arc<dyn MemoryBudget>>,
}

impl LoadedRawVectorCandidate {
    #[doc(hidden)]
    pub fn loader_charged(
        entry: VectorEntry,
        transient_bytes: usize,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Self {
        Self {
            entry,
            transient_bytes,
            loader_accountant: Some(accountant),
        }
    }

    #[doc(hidden)]
    pub fn caller_charged(entry: VectorEntry, transient_bytes: usize) -> Self {
        Self {
            entry,
            transient_bytes,
            loader_accountant: None,
        }
    }

    fn entry(&self) -> &VectorEntry {
        &self.entry
    }

    fn transient_bytes(&self) -> usize {
        self.transient_bytes
    }
}

impl Drop for LoadedRawVectorCandidate {
    fn drop(&mut self) {
        if let Some(accountant) = self.loader_accountant.take() {
            accountant.release(self.transient_bytes);
        }
    }
}

#[doc(hidden)]
pub trait DormantRawVectorLoader: Send + Sync {
    fn load(&self, directory: &[RawVectorDirectoryEntry]) -> Result<LoadedRawVectorPartition>;

    fn load_candidate(
        &self,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate>;
}

#[doc(hidden)]
pub struct DormantRawVectorPartition {
    pub partition: VectorPartitionRef,
    pub directory: Vec<RawVectorDirectoryEntry>,
    pub loader: Arc<dyn DormantRawVectorLoader>,
}

#[derive(Clone)]
struct DormantSealedGeneration {
    descriptor: DormantVectorGraphGeneration,
    loader: Arc<dyn DormantVectorGraphLoader>,
}

#[derive(Default)]
struct RetiredSealedHnswChain {
    base: Option<SealedHnswGeneration>,
    change: Option<SealedHnswGeneration>,
    dormant_base: Option<DormantSealedGeneration>,
    dormant_change: Option<DormantSealedGeneration>,
}

#[derive(Clone, Copy)]
enum SealedChainLocation {
    Current,
    Retired { base_generation_id: u64 },
}

/// Sealed graphs are never mutated after installation. `retired` preserves
/// each old base and its optional bounded change as one serving chain until
/// the engine confirms that no registered snapshot can still ask for it. We
/// retain a chain only while a registered snapshot selects that chain.
#[derive(Default)]
struct SealedHnswGenerations {
    base: Option<SealedHnswGeneration>,
    change: Option<SealedHnswGeneration>,
    dormant_base: Option<DormantSealedGeneration>,
    dormant_change: Option<DormantSealedGeneration>,
    pending_base: Option<DormantSealedGeneration>,
    retired: Vec<RetiredSealedHnswChain>,
    pending_generation_count: usize,
    quarantine: Option<VectorRouteQuarantineReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorServingPolicy {
    pub hnsw_m: usize,
    pub hnsw_ef_construction: usize,
    pub policy_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMaintenanceProgress {
    pub state: &'static str,
    pub reason: &'static str,
    pub vectors_total: usize,
    pub vectors_done: usize,
    pub checkpoint_tx: TxId,
}

/// Sorted visibility boundaries answer old and current snapshot counts without
/// visiting raw history. Mutations update these under the same publication gate
/// as the directory; physical pruning rebuilds them from the surviving intervals.
#[derive(Default)]
struct VectorVisibilityCounts {
    intervals: Vec<(u64, u64)>,
    created: Vec<TxId>,
    deleted: Vec<TxId>,
}

impl VectorVisibilityCounts {
    fn insert(boundaries: &mut Vec<TxId>, tx: TxId) {
        if boundaries.capacity() == 0 {
            boundaries.reserve_exact(1);
        }
        let position = boundaries.partition_point(|existing| *existing <= tx);
        boundaries.insert(position, tx);
    }

    fn add(&mut self, entry: &RawVectorDirectoryEntry) {
        Self::insert(&mut self.created, entry.created_tx);
        if let Some(tx) = entry.deleted_tx {
            Self::insert(&mut self.deleted, tx);
        }
    }

    // A segment summary skips a block when every version ended before the
    // snapshot or starts after it. Directory positions stay stable until the
    // existing rebuild boundary (load or physical reclamation).
    fn set_interval(&mut self, position: usize, entry: &RawVectorDirectoryEntry) {
        let mut leaves = self.intervals.len() / 2;
        if position >= leaves {
            let next = (position + 1).next_power_of_two();
            let mut grown = vec![(u64::MAX, 0); next * 2];
            if leaves != 0 {
                grown[next..next + leaves].copy_from_slice(&self.intervals[leaves..]);
            }
            for node in (1..next).rev() {
                grown[node] = (
                    grown[node * 2].0.min(grown[node * 2 + 1].0),
                    grown[node * 2].1.max(grown[node * 2 + 1].1),
                );
            }
            self.intervals = grown;
            leaves = next;
        }
        let mut node = leaves + position;
        self.intervals[node] = (
            entry.created_tx.0,
            entry.deleted_tx.map_or(u64::MAX, |tx| tx.0),
        );
        while node > 1 {
            node /= 2;
            self.intervals[node] = (
                self.intervals[node * 2]
                    .0
                    .min(self.intervals[node * 2 + 1].0),
                self.intervals[node * 2]
                    .1
                    .max(self.intervals[node * 2 + 1].1),
            );
        }
    }

    fn count(&self, snapshot: SnapshotId) -> usize {
        self.created.partition_point(|tx| tx.0 <= snapshot.0)
            - self.deleted.partition_point(|tx| tx.0 <= snapshot.0)
    }

    fn max_tx(&self) -> TxId {
        self.created
            .last()
            .into_iter()
            .chain(self.deleted.last())
            .copied()
            .max()
            .unwrap_or_default()
    }

    fn retained_bytes(&self) -> usize {
        // Includes simultaneous old/new trees during geometric growth.
        self.created.len()
            * (4 * std::mem::size_of::<TxId>() + 8 * std::mem::size_of::<(u64, u64)>())
    }
}

pub struct IndexState {
    dimension: usize,
    quantization: VectorQuantization,
    maintenance: Mutex<()>,
    /// Serializes builders for this partition without making foreground
    /// publication wait for graph construction.
    maintenance_build: Mutex<()>,
    vectors: RwLock<PartitionVectors>,
    raw_directory: RwLock<Vec<RawVectorDirectoryEntry>>,
    #[cfg(test)]
    raw_directory_entries_inspected: AtomicU64,
    raw_directory_by_row: RwLock<HashMap<RowId, Vec<usize>>>,
    visibility_counts: RwLock<VectorVisibilityCounts>,
    raw_loader: RwLock<Option<Arc<dyn DormantRawVectorLoader>>>,
    raw_residency_gate: RwLock<()>,
    raw_resident: AtomicBool,
    raw_resident_bytes: AtomicUsize,
    raw_accountant: RwLock<Option<Arc<dyn MemoryBudget>>>,
    /// This state still owns the map key and raw historical directory.  The
    /// flag retires only its maintained graph/count identity, without
    /// allocating a second copy of that key in a side registry.
    historical_only: AtomicBool,
    hnsw: OnceLock<RwLock<Option<HnswIndex>>>,
    hnsw_bytes: AtomicUsize,
    hnsw_accountant: RwLock<Option<Arc<dyn MemoryBudget>>>,
    serving_policy: RwLock<Option<VectorServingPolicy>>,
    inspection_generation_id: AtomicU64,
    inspection_base_tx: AtomicU64,
    /// Monotonic store-local sequence of the most recent search that used
    /// this partition's sealed graph chain.
    sealed_graph_last_used: AtomicU64,
    #[cfg(feature = "test-seams")]
    base_backlog: Mutex<crate::observations::BaseBacklog>,
    maintenance_progress: RwLock<Option<VectorMaintenanceProgress>>,
    #[cfg(any(test, feature = "test-seams"))]
    maintenance_step_pause: Mutex<Option<Arc<MaintenanceProgressPauseSlot>>>,
    maintenance_failure: RwLock<Option<VectorMaintenanceFailureDetails>>,
    maintenance_policy_revision: AtomicU64,
    /// Makes tail extension/replacement and a generation pointer switch one
    /// observation. Durable decode stays outside this short boundary; its
    /// candidate is admitted only if `fresh_tail_revision` is unchanged.
    generation_publication: Mutex<()>,
    fresh_tail_revision: AtomicU64,
    fresh_tail_preparations: AtomicUsize,
    fresh_tail_replay_frontier_active: AtomicBool,
    fresh_tail_replayed_through_lsn: AtomicU64,
    fresh_tail_mutated_through_lsn: AtomicU64,
    /// In-memory maintenance builds from this fixed frontier while commits
    /// continue into the fresh tail. A failed build retains the same frontier
    /// for the next bounded attempt instead of folding later writes back into
    /// the candidate.
    maintenance_sample_active: AtomicBool,
    maintenance_sample_tx: AtomicU64,
    maintenance_sample_lsn: AtomicU64,
    maintenance_sample_origin: AtomicU64,
    maintenance_sample_prior_replay_frontier_active: AtomicBool,
    maintenance_sample_prior_replayed_through_lsn: AtomicU64,
    maintenance_sample_prior_mutated_through_lsn: AtomicU64,
    #[cfg(any(test, feature = "test-seams"))]
    graph_callback_pause: Mutex<Option<Arc<MaintenanceProgressPauseSlot>>>,
    /// The old `hnsw` field above is deliberately the fresh, process-created
    /// mutable tail.  Keeping it separate makes it impossible for an insert
    /// to reach a sealed graph loaded from durable storage.
    sealed_hnsw: RwLock<SealedHnswGenerations>,
    #[cfg(any(test, feature = "test-seams"))]
    passive_activity: OnceLock<Arc<VectorPassiveActivity>>,
}

#[doc(hidden)]
pub struct VectorMaintenanceProgressGuard {
    state: Arc<IndexState>,
}

struct SampledMaintenanceRollback {
    state: Arc<IndexState>,
    armed: bool,
}

impl Drop for SampledMaintenanceRollback {
    fn drop(&mut self) {
        if self.armed {
            self.state.abort_sampled_hnsw_maintenance_if_uncontended();
        }
    }
}

impl VectorMaintenanceProgressGuard {
    pub fn advance(&self, done: usize) {
        self.state.set_maintenance_vectors_done(done);
    }
}

impl Drop for VectorMaintenanceProgressGuard {
    fn drop(&mut self) {
        *self.state.maintenance_progress.write() = None;
    }
}

/// Borrowed, allocation-free view of one vector column's complete ordered
/// raw-source registry. The enclosing store call keeps structural publication
/// out until the visitor returns; callers clone only the state handles and
/// typed keys they have already admitted.
#[doc(hidden)]
pub struct VectorPartitionSourceView<'a> {
    partitions: parking_lot::RwLockReadGuard<'a, BTreeMap<VectorPartitionKey, Arc<IndexState>>>,
}

impl VectorPartitionSourceView<'_> {
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn visit<E>(
        &self,
        mut visit: impl FnMut(&VectorPartitionKey, &Arc<IndexState>) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        for (key, state) in self.partitions.iter() {
            visit(key, state)?;
        }
        Ok(())
    }
}

impl IndexState {
    fn new(dimension: usize, quantization: VectorQuantization) -> Self {
        Self {
            dimension,
            quantization,
            maintenance: Mutex::new(()),
            maintenance_build: Mutex::new(()),
            vectors: RwLock::new(PartitionVectors::default()),
            raw_directory: RwLock::new(Vec::new()),
            #[cfg(test)]
            raw_directory_entries_inspected: AtomicU64::new(0),
            raw_directory_by_row: RwLock::new(HashMap::new()),
            visibility_counts: RwLock::new(VectorVisibilityCounts::default()),
            raw_loader: RwLock::new(None),
            raw_residency_gate: RwLock::new(()),
            raw_resident: AtomicBool::new(true),
            raw_resident_bytes: AtomicUsize::new(0),
            raw_accountant: RwLock::new(None),
            historical_only: AtomicBool::new(false),
            hnsw: OnceLock::new(),
            hnsw_bytes: AtomicUsize::new(0),
            hnsw_accountant: RwLock::new(None),
            serving_policy: RwLock::new(None),
            inspection_generation_id: AtomicU64::new(0),
            inspection_base_tx: AtomicU64::new(0),
            sealed_graph_last_used: AtomicU64::new(0),
            #[cfg(feature = "test-seams")]
            base_backlog: Mutex::new(crate::observations::BaseBacklog::default()),
            maintenance_progress: RwLock::new(None),
            #[cfg(any(test, feature = "test-seams"))]
            maintenance_step_pause: Mutex::new(None),
            maintenance_failure: RwLock::new(None),
            maintenance_policy_revision: AtomicU64::new(0),
            generation_publication: Mutex::new(()),
            fresh_tail_revision: AtomicU64::new(0),
            fresh_tail_preparations: AtomicUsize::new(0),
            fresh_tail_replay_frontier_active: AtomicBool::new(false),
            fresh_tail_replayed_through_lsn: AtomicU64::new(0),
            fresh_tail_mutated_through_lsn: AtomicU64::new(0),
            maintenance_sample_active: AtomicBool::new(false),
            maintenance_sample_tx: AtomicU64::new(0),
            maintenance_sample_lsn: AtomicU64::new(0),
            maintenance_sample_origin: AtomicU64::new(0),
            maintenance_sample_prior_replay_frontier_active: AtomicBool::new(false),
            maintenance_sample_prior_replayed_through_lsn: AtomicU64::new(0),
            maintenance_sample_prior_mutated_through_lsn: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            graph_callback_pause: Mutex::new(None),
            sealed_hnsw: RwLock::new(SealedHnswGenerations::default()),
            #[cfg(any(test, feature = "test-seams"))]
            passive_activity: OnceLock::new(),
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn attach_passive_activity(&self, activity: Arc<VectorPassiveActivity>) {
        let _ = self.passive_activity.set(activity);
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn note_passive_activity(&self, event: impl FnOnce(&VectorPassiveActivity)) {
        if let Some(activity) = self.passive_activity.get() {
            event(activity);
        }
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    pub fn quantization(&self) -> VectorQuantization {
        self.quantization
    }

    fn serving_policy(&self) -> Option<VectorServingPolicy> {
        *self.serving_policy.read()
    }

    fn is_historical_only(&self) -> bool {
        self.historical_only.load(Ordering::SeqCst)
    }

    fn mark_historical_only(&self) {
        self.historical_only.store(true, Ordering::SeqCst);
    }

    fn reactivate_maintained_state(&self) {
        self.historical_only.store(false, Ordering::SeqCst);
    }

    fn set_serving_policy(&self, policy: ResolvedVectorPolicy) {
        *self.serving_policy.write() = Some(VectorServingPolicy {
            hnsw_m: policy.hnsw_m,
            hnsw_ef_construction: policy.hnsw_ef_construction,
            policy_revision: policy.policy_revision,
        });
        self.inspection_generation_id.fetch_add(1, Ordering::SeqCst);
        self.inspection_base_tx
            .store(self.max_tx().0, Ordering::SeqCst);
        self.clear_maintenance_failure();
    }

    fn set_serving_policy_from_generation(&self, generation: VectorGraphGeneration) {
        *self.serving_policy.write() = Some(VectorServingPolicy {
            hnsw_m: generation.hnsw_m as usize,
            hnsw_ef_construction: generation.hnsw_ef_construction as usize,
            policy_revision: generation.policy_revision,
        });
        self.inspection_generation_id
            .store(generation.generation_id, Ordering::SeqCst);
        self.inspection_base_tx
            .store(generation.covered_tx.0, Ordering::SeqCst);
        self.clear_maintenance_failure();
    }

    fn maintenance_progress(&self) -> Option<VectorMaintenanceProgress> {
        *self.maintenance_progress.read()
    }

    fn maintenance_failure_details(&self) -> Option<VectorMaintenanceFailureDetails> {
        *self.maintenance_failure.read()
    }

    fn record_maintenance_failure(&self, details: VectorMaintenanceFailureDetails) {
        *self.maintenance_failure.write() = Some(details);
    }

    fn clear_maintenance_failure(&self) {
        *self.maintenance_failure.write() = None;
    }

    fn clear_maintenance_failure_if(&self, expected: VectorMaintenanceFailureDetails) {
        let mut failure = self.maintenance_failure.write();
        if *failure == Some(expected) {
            *failure = None;
        }
    }

    fn begin_maintenance_progress_at(
        self: &Arc<Self>,
        state: &'static str,
        reason: &'static str,
        vectors_total: usize,
        checkpoint_tx: TxId,
    ) -> VectorMaintenanceProgressGuard {
        *self.maintenance_progress.write() = Some(VectorMaintenanceProgress {
            state,
            reason,
            vectors_total,
            vectors_done: 0,
            checkpoint_tx,
        });
        VectorMaintenanceProgressGuard {
            state: Arc::clone(self),
        }
    }

    fn set_maintenance_vectors_done(&self, vectors_done: usize) {
        if let Some(progress) = self.maintenance_progress.write().as_mut() {
            progress.vectors_done = vectors_done.min(progress.vectors_total);
        }
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(slot) = self.maintenance_step_pause.lock().clone()
            && slot
                .requested_vectors()
                .is_some_and(|requested| vectors_done >= requested)
        {
            slot.pause_after(vectors_done);
        }
    }

    fn maintenance_policy_revision(&self) -> Option<u64> {
        match self.maintenance_policy_revision.load(Ordering::SeqCst) {
            0 => None,
            revision => Some(revision),
        }
    }

    fn directory_entry(entry: &StoredVectorEntry) -> RawVectorDirectoryEntry {
        RawVectorDirectoryEntry {
            row_id: entry.row_id,
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        }
    }

    fn install_dormant_raw_directory(
        &self,
        entries: Vec<RawVectorDirectoryEntry>,
        loader: Arc<dyn DormantRawVectorLoader>,
    ) -> Result<()> {
        let _gate = self.raw_residency_gate.write();
        if !self.vectors.read().entries.is_empty() || !self.raw_directory.read().is_empty() {
            return Err(Error::Other(
                "raw vector directory can only be installed into an empty partition".to_string(),
            ));
        }
        *self.raw_directory.write() = entries;
        self.rebuild_raw_directory_positions();
        *self.raw_loader.write() = Some(loader);
        self.raw_resident.store(false, Ordering::SeqCst);
        self.raw_resident_bytes.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn attach_durable_raw_loader(
        &self,
        expected: &[RawVectorDirectoryEntry],
        loader: Arc<dyn DormantRawVectorLoader>,
    ) -> Result<()> {
        let _gate = self.raw_residency_gate.write();
        if self.raw_directory.read().as_slice() != expected {
            return Err(Error::Other(
                "durable raw-vector membership changed outside publication".to_string(),
            ));
        }
        *self.raw_loader.write() = Some(loader);
        Ok(())
    }

    pub(crate) fn ensure_raw_vectors_loaded(&self) -> Result<()> {
        if self.raw_resident.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _gate = self.raw_residency_gate.write();
        if self.raw_resident.load(Ordering::SeqCst) {
            return Ok(());
        }
        let loader = self.raw_loader.read().clone().ok_or_else(|| {
            Error::Other("dormant raw vector partition has no loader".to_string())
        })?;
        let expected = self.raw_directory.read().clone();
        let loaded = loader.load(&expected)?;
        let LoadedRawVectorPartition {
            entries,
            resident_bytes: reserved_resident_bytes,
            workspace_bytes,
            accountant,
        } = loaded;
        let reserved_bytes = reserved_resident_bytes.saturating_add(workspace_bytes);
        if entries.len() != expected.len() {
            accountant.release(reserved_bytes);
            return Err(Error::Other(
                "loaded raw vector partition does not match its directory count".to_string(),
            ));
        }
        let mut vectors = PartitionVectors::default();
        let mut resident_bytes = 0usize;
        for (entry, identity) in entries.into_iter().zip(&expected) {
            if entry.row_id != identity.row_id
                || entry.created_tx != identity.created_tx
                || entry.deleted_tx != identity.deleted_tx
                || entry.lsn != identity.lsn
            {
                accountant.release(reserved_bytes);
                return Err(Error::Other(
                    "loaded raw vector partition disagrees with its durable directory".to_string(),
                ));
            }
            let stored = self.stored_entry(entry);
            resident_bytes = resident_bytes.saturating_add(stored.estimated_bytes());
            vectors.push(stored);
        }
        if resident_bytes != reserved_resident_bytes {
            accountant.release(reserved_bytes);
            return Err(Error::Other(format!(
                "loaded raw vector charge mismatch: directory owns {} bytes but decoded vectors require {resident_bytes}",
                reserved_resident_bytes
            )));
        }
        *self.vectors.write() = vectors;
        if workspace_bytes != 0 {
            accountant.release(workspace_bytes);
        }
        *self.raw_accountant.write() = Some(accountant);
        self.raw_resident_bytes
            .store(resident_bytes, Ordering::SeqCst);
        self.raw_resident.store(true, Ordering::SeqCst);
        #[cfg(any(test, feature = "test-seams"))]
        self.note_passive_activity(|activity| {
            activity.raw_partition_loads.fetch_add(1, Ordering::SeqCst);
        });
        Ok(())
    }

    #[cfg(feature = "test-seams")]
    fn raw_vectors_resident(&self) -> bool {
        self.raw_resident.load(Ordering::SeqCst)
    }

    fn raw_residency_is_accounted(&self) -> bool {
        self.raw_accountant.read().is_some()
    }

    fn attach_raw_accounting(&self, accountant: Arc<dyn MemoryBudget>) {
        let mut owner = self.raw_accountant.write();
        if owner.is_none() {
            *owner = Some(accountant);
        }
    }

    /// Release a raw cache reservation owned by this state before the state
    /// is removed from the registry. `false` means the bodies, if any, were
    /// admitted by the engine's older per-insert path and the caller must
    /// settle those entries instead.
    fn release_accounted_raw_residency_for_removal(&self) -> bool {
        let _gate = self.raw_residency_gate.write();
        let Some(accountant) = self.raw_accountant.write().take() else {
            return false;
        };
        let bytes = self.raw_resident_bytes.swap(0, Ordering::SeqCst);
        let mut vectors = self.vectors.write();
        vectors.entries.clear();
        vectors.by_row.clear();
        drop(vectors);
        if bytes != 0 {
            accountant.release(bytes);
        }
        self.raw_resident.store(false, Ordering::SeqCst);
        #[cfg(any(test, feature = "test-seams"))]
        self.note_passive_activity(|activity| {
            activity
                .raw_partition_evictions
                .fetch_add(1, Ordering::SeqCst);
        });
        true
    }

    fn raw_payload_bytes(&self) -> usize {
        if !self.raw_resident.load(Ordering::SeqCst) {
            return 0;
        }
        self.raw_resident_bytes.load(Ordering::SeqCst).max(
            self.vectors
                .read()
                .entries
                .iter()
                .map(StoredVectorEntry::estimated_bytes)
                .sum(),
        )
    }

    fn directory_byte_count(&self) -> usize {
        let directory_bytes = self
            .raw_directory
            .read()
            .len()
            .saturating_mul(std::mem::size_of::<RawVectorDirectoryEntry>());
        let by_row = self.raw_directory_by_row.read();
        directory_bytes
            .saturating_add(self.visibility_counts.read().retained_bytes())
            .saturating_add(
                by_row
                    .len()
                    .saturating_mul(std::mem::size_of::<(RowId, Vec<usize>)>()),
            )
            .saturating_add(by_row.values().fold(0usize, |bytes, positions| {
                bytes.saturating_add(positions.len().saturating_mul(std::mem::size_of::<usize>()))
            }))
    }

    fn rebuild_raw_directory_positions(&self) {
        let directory = self.raw_directory.read();
        let mut by_row = HashMap::<RowId, Vec<usize>>::new();
        for (position, entry) in directory.iter().enumerate() {
            by_row.entry(entry.row_id).or_default().push(position);
        }
        for positions in by_row.values_mut() {
            positions.sort_by_key(|position| directory[*position].created_tx);
        }
        let mut counts = VectorVisibilityCounts {
            intervals: Vec::new(),
            created: directory.iter().map(|entry| entry.created_tx).collect(),
            deleted: Vec::with_capacity(directory.len()),
        };
        counts
            .deleted
            .extend(directory.iter().filter_map(|entry| entry.deleted_tx));
        for (position, entry) in directory.iter().enumerate() {
            counts.set_interval(position, entry);
        }
        counts.created.sort_unstable();
        counts.deleted.sort_unstable();
        *self.visibility_counts.write() = counts;
        *self.raw_directory_by_row.write() = by_row;
    }

    pub(crate) fn directory_has_visible_row(&self, row_id: RowId, snapshot: SnapshotId) -> bool {
        self.visible_directory_entry_by_row(row_id, snapshot)
            .is_some()
    }

    fn visible_directory_entry_by_row(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Option<RawVectorDirectoryEntry> {
        let directory = self.raw_directory.read();
        let by_row = self.raw_directory_by_row.read();
        let positions = by_row.get(&row_id)?;
        let end =
            positions.partition_point(|position| directory[*position].created_tx.0 <= snapshot.0);
        // Raw-store callers can publish IDs out of order; Database commits
        // restamp them first. A deleted directory head must not conceal the
        // visible version behind it.
        positions[..end].iter().rev().find_map(|position| {
            directory
                .get(*position)
                .filter(|entry| entry.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0))
                .cloned()
        })
    }

    fn directory_has_row_lsn(&self, row_id: RowId, lsn: Lsn) -> bool {
        let directory = self.raw_directory.read();
        self.raw_directory_by_row
            .read()
            .get(&row_id)
            .is_some_and(|positions| {
                positions.iter().any(|position| {
                    directory
                        .get(*position)
                        .is_some_and(|entry| entry.lsn == lsn)
                })
            })
    }

    pub(crate) fn directory_visible_entry_count(
        &self,
        snapshot: SnapshotId,
        candidates: Option<&roaring::RoaringTreemap>,
    ) -> usize {
        match candidates {
            Some(ids) => ids
                .iter()
                .filter(|id| self.directory_has_visible_row(RowId(*id), snapshot))
                .count(),
            None => self.visibility_counts.read().count(snapshot),
        }
    }

    pub(crate) fn bounded_directory_visible_entry_count<E>(
        &self,
        snapshot: SnapshotId,
        candidates: Option<&[u64]>,
        mut before_entry: impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<usize, E> {
        before_entry()?;
        let Some(ids) = candidates else {
            return Ok(self.visibility_counts.read().count(snapshot));
        };
        let mut count = 0;
        for id in ids {
            before_entry()?;
            count += usize::from(self.directory_has_visible_row(RowId(*id), snapshot));
        }
        Ok(count)
    }

    pub(crate) fn bounded_visit_visible_ids<E>(
        &self,
        snapshot: SnapshotId,
        mut before_entry: impl FnMut() -> std::result::Result<(), E>,
        mut visit: impl FnMut(RowId) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        let directory = self.raw_directory.read();
        let counts = self.visibility_counts.read();
        if counts.intervals.is_empty() {
            return Ok(());
        }
        // Fixed stack: at most one sibling per bit in a native directory index.
        let mut stack = [0usize; usize::BITS as usize + 1];
        let mut pending = 1;
        stack[0] = 1;
        let leaves = counts.intervals.len() / 2;
        while pending != 0 {
            pending -= 1;
            let node = stack[pending];
            before_entry()?;
            let (created, deleted) = counts.intervals[node];
            if created > snapshot.0 || (deleted != u64::MAX && deleted <= snapshot.0) {
                continue;
            }
            if node >= leaves {
                if let Some(entry) = directory.get(node - leaves)
                    && entry.created_tx.0 <= snapshot.0
                    && entry.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
                {
                    visit(entry.row_id)?;
                }
            } else {
                stack[pending] = node * 2 + 1;
                stack[pending + 1] = node * 2;
                pending += 2;
            }
        }
        Ok(())
    }

    fn evict_raw_vectors(&self) -> bool {
        let _gate = self.raw_residency_gate.write();
        if !self.raw_resident.load(Ordering::SeqCst) || self.raw_loader.read().is_none() {
            return false;
        }
        let mut vectors = self.vectors.write();
        *vectors = PartitionVectors::default();
        drop(vectors);
        let bytes = self.raw_resident_bytes.swap(0, Ordering::SeqCst);
        if bytes != 0
            && let Some(accountant) = self.raw_accountant.write().take()
        {
            accountant.release(bytes);
        }
        self.raw_resident.store(false, Ordering::SeqCst);
        #[cfg(any(test, feature = "test-seams"))]
        self.note_passive_activity(|activity| {
            activity
                .raw_partition_evictions
                .fetch_add(1, Ordering::SeqCst);
        });
        true
    }

    pub fn vector_count(&self) -> usize {
        self.visibility_counts.read().count(SnapshotId(u64::MAX))
    }

    pub fn max_tx(&self) -> TxId {
        self.visibility_counts.read().max_tx()
    }

    /// Eligibility shares the snapshot boundary index used by ordinary counts.
    /// One checkpoint admits the lookup; excluded history is never enumerated.
    pub(crate) fn bounded_hnsw_eligibility<E>(
        &self,
        snapshot_tx: TxId,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<BoundedIndexEligibility, E>
    where
        E: From<Error>,
    {
        before_source_entry()?;
        let counts = self.visibility_counts.read();
        Ok(BoundedIndexEligibility {
            entry_count: counts.created.len(),
            live_count: counts.count(SnapshotId(snapshot_tx.0)),
        })
    }

    pub fn byte_count(&self) -> usize {
        let vectors = self.vectors.read();
        let raw_bytes = if self.raw_resident.load(Ordering::SeqCst) {
            self.raw_resident_bytes.load(Ordering::SeqCst).max(
                vectors
                    .entries
                    .iter()
                    .map(StoredVectorEntry::estimated_bytes)
                    .sum::<usize>(),
            )
        } else {
            0
        };
        raw_bytes
            .saturating_add(
                vectors
                    .entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StoredVectorEntry>()),
            )
            .saturating_add(vectors.retained_map_bytes())
            .saturating_add(self.directory_byte_count())
            .saturating_add(self.hnsw_bytes.load(Ordering::SeqCst))
            .saturating_add(self.sealed_hnsw_bytes())
    }

    fn live_payload_bytes(&self) -> usize {
        self.vector_count()
            .saturating_mul(crate::quantized::stored_vector_resident_bytes(
                self.dimension,
                self.quantization,
            ))
    }

    fn sealed_hnsw_bytes(&self) -> usize {
        let sealed = self.sealed_hnsw.read();
        sealed
            .base
            .iter()
            .chain(sealed.change.iter())
            .fold(0usize, |bytes, generation| {
                bytes.saturating_add(generation.bytes)
            })
            .saturating_add(sealed.retired.iter().fold(0usize, |bytes, chain| {
                bytes
                    .saturating_add(
                        chain
                            .base
                            .as_ref()
                            .map(|generation| generation.bytes)
                            .unwrap_or(0),
                    )
                    .saturating_add(
                        chain
                            .change
                            .as_ref()
                            .map(|generation| generation.bytes)
                            .unwrap_or(0),
                    )
            }))
    }

    pub fn all_entries(&self, index: &VectorIndexRef) -> Vec<VectorEntry> {
        self.vectors
            .read()
            .entries
            .iter()
            .map(|entry| entry.to_vector_entry(index.clone()))
            .collect()
    }

    pub fn find_by_row_id(&self, index: &VectorIndexRef, row_id: RowId) -> Option<VectorEntry> {
        let vectors = self.vectors.read();
        vectors.by_row.get(&row_id).and_then(|positions| {
            positions.iter().rev().find_map(|position| {
                vectors
                    .entries
                    .get(*position)
                    .map(|entry| entry.to_vector_entry(index.clone()))
            })
        })
    }

    fn stored_by_row_id(&self, row_id: RowId) -> Option<StoredVectorEntry> {
        let vectors = self.vectors.read();
        vectors.by_row.get(&row_id).and_then(|positions| {
            positions
                .iter()
                .rev()
                .find_map(|position| vectors.entries.get(*position).cloned())
        })
    }

    fn visible_entry_by_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Option<VectorEntry> {
        self.with_visible_stored_entry_by_row(row_id, snapshot, |entry| {
            entry.to_vector_entry(index.clone())
        })
    }

    pub(crate) fn with_visible_stored_entry_by_row<R>(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        f: impl FnOnce(&StoredVectorEntry) -> R,
    ) -> Option<R> {
        let vectors = self.vectors.read();
        let positions = vectors.by_row.get(&row_id)?;
        for position in positions.iter().rev() {
            let Some(entry) = vectors.entries.get(*position) else {
                continue;
            };
            if entry.visible_at(snapshot) {
                return Some(f(entry));
            }
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    fn score_visible_candidate_inner<E>(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        caller_charged: bool,
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
    {
        before_checkpoint()?;
        if let Some(score) = self
            .with_visible_stored_entry_by_row(row_id, snapshot, |entry| {
                before_distance().map(|()| entry.vector.cosine_similarity(query))
            })
            .transpose()?
        {
            return Ok(Some(score));
        }

        let Some(identity) = self.visible_directory_entry_by_row(row_id, snapshot) else {
            return Ok(None);
        };
        let loader = self.raw_loader.read().clone().ok_or_else(|| {
            E::from(Error::Other(
                "dormant raw vector candidate has no loader".to_string(),
            ))
        })?;
        let mut interrupted = None;
        let mut caller_reserved = 0usize;
        let loaded = {
            let mut request = |request| {
                if interrupted.is_some() {
                    return false;
                }
                let result = match request {
                    DormantVectorLoadRequest::Checkpoint
                    | DormantVectorLoadRequest::Progress { .. } => before_checkpoint(),
                    DormantVectorLoadRequest::Reserve(bytes) => {
                        if let Err(error) = before_checkpoint() {
                            Err(error)
                        } else if let Some(total) = caller_reserved.checked_add(bytes) {
                            match before_retain(bytes) {
                                Ok(()) => {
                                    caller_reserved = total;
                                    Ok(())
                                }
                                Err(error) => Err(error),
                            }
                        } else {
                            Err(E::from(Error::Other(
                                "raw vector candidate reservation overflow".to_string(),
                            )))
                        }
                    }
                };
                match result {
                    Ok(()) => true,
                    Err(error) => {
                        interrupted = Some(error);
                        false
                    }
                }
            };
            loader.load_candidate(&identity, caller_charged, &mut request)
        };
        let loaded = match loaded {
            Ok(loaded) => loaded,
            Err(error) => {
                if caller_charged {
                    release_retained(caller_reserved);
                }
                return Err(interrupted.unwrap_or_else(|| E::from(error)));
            }
        };
        if let Some(error) = interrupted {
            drop(loaded);
            if caller_charged {
                release_retained(caller_reserved);
            }
            return Err(error);
        }

        let scored = (|| -> std::result::Result<(f32, usize), E> {
            if caller_charged && loaded.transient_bytes() != caller_reserved {
                return Err(E::from(Error::Other(
                    "raw vector candidate charge disagrees with its caller reservation".to_string(),
                )));
            }
            before_checkpoint()?;
            let entry = loaded.entry();
            if entry.index != *index
                || entry.row_id != identity.row_id
                || entry.created_tx != identity.created_tx
                || entry.deleted_tx != identity.deleted_tx
                || entry.lsn != identity.lsn
                || entry.vector.len() != self.dimension
            {
                return Err(E::from(Error::Other(
                    "loaded raw vector candidate disagrees with its durable directory".to_string(),
                )));
            }
            let stored = self.stored_entry_ref(entry);
            let body_bytes = stored.estimated_bytes();
            before_distance()?;
            Ok((stored.vector.cosine_similarity(query), body_bytes))
        })();
        drop(loaded);
        if caller_charged {
            release_retained(caller_reserved);
        }
        let (score, body_bytes) = scored?;
        #[cfg(any(test, feature = "test-seams"))]
        self.note_passive_activity(|activity| {
            activity
                .raw_candidate_point_loads
                .fetch_add(1, Ordering::SeqCst);
            let body_bytes = u64::try_from(body_bytes).unwrap_or(u64::MAX);
            let _ = activity.raw_candidate_point_body_bytes.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |bytes| Some(bytes.saturating_add(body_bytes)),
            );
        });
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = body_bytes;
        Ok(Some(score))
    }

    pub(crate) fn score_visible_candidate(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
    ) -> Result<Option<f32>> {
        self.score_visible_candidate_inner(
            index,
            row_id,
            snapshot,
            query,
            false,
            &mut || Ok::<(), Error>(()),
            &mut || Ok::<(), Error>(()),
            &mut |_bytes| Ok::<(), Error>(()),
            &mut |_bytes| {},
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_score_visible_candidate<E>(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
    {
        self.score_visible_candidate_inner(
            index,
            row_id,
            snapshot,
            query,
            true,
            before_checkpoint,
            before_distance,
            before_retain,
            release_retained,
        )
    }

    fn vector_for_row_lsn(&self, row_id: RowId, lsn: Lsn) -> Option<Vec<f32>> {
        let vectors = self.vectors.read();
        vectors.by_row.get(&row_id).and_then(|positions| {
            positions.iter().find_map(|position| {
                vectors
                    .entries
                    .get(*position)
                    .filter(|entry| entry.lsn == lsn)
                    .map(|entry| entry.vector.to_f32())
            })
        })
    }

    pub(crate) fn with_entries<R>(&self, f: impl FnOnce(&[StoredVectorEntry]) -> R) -> R {
        let vectors = self.vectors.read();
        #[cfg(any(test, feature = "test-seams"))]
        self.note_passive_activity(|activity| {
            activity
                .whole_entry_slice_requests
                .fetch_add(1, Ordering::SeqCst);
            activity
                .whole_entry_slice_entries_exposed
                .fetch_add(vectors.entries.len() as u64, Ordering::SeqCst);
        });
        f(&vectors.entries)
    }

    pub fn entry_count(&self) -> usize {
        self.raw_directory.read().len()
    }

    fn stored_entry(&self, entry: VectorEntry) -> StoredVectorEntry {
        StoredVectorEntry::from_vector_entry(entry, self.quantization)
    }

    fn stored_entry_ref(&self, entry: &VectorEntry) -> StoredVectorEntry {
        StoredVectorEntry::from_vector_entry_ref(entry, self.quantization)
    }

    fn push_entry(&self, entry: StoredVectorEntry) {
        debug_assert!(self.raw_resident.load(Ordering::SeqCst));
        let mut directory = self.raw_directory.write();
        let position = directory.len();
        let identity = Self::directory_entry(&entry);
        {
            let mut counts = self.visibility_counts.write();
            counts.add(&identity);
            counts.set_interval(position, &identity);
        }
        directory.push(identity);
        #[cfg(feature = "test-seams")]
        {
            let mut backlog = self.base_backlog.lock();
            if entry.deleted_tx.is_none() && entry.created_tx.0 > backlog.covered_tx {
                backlog.pending.inserts += 1;
                backlog.record_high_water();
            }
        }
        let mut by_row = self.raw_directory_by_row.write();
        let positions = by_row.entry(entry.row_id).or_default();
        let at = positions
            .partition_point(|position| directory[*position].created_tx <= entry.created_tx);
        positions.insert(at, position);
        drop(by_row);
        drop(directory);
        self.raw_resident_bytes
            .fetch_add(entry.estimated_bytes(), Ordering::SeqCst);
        // A durable loader resolves the partition's current checksummed
        // membership at load time, so it remains valid after this already-
        // durable publication and makes the updated partition evictable.
        self.vectors.write().push(entry);
    }

    /// Extend a graph that was built fresh in this process. Loaded durable
    /// generations use a separate sealed type and never reach this method.
    ///
    /// Memory is reserved before the vendor insertion. If that reservation or
    /// insertion cannot complete, the old graph is retired so no query can
    /// mistake a stale route for a complete one; the authoritative vectors
    /// remain available to exact search and maintenance can build a new graph.
    fn insert_into_materialized_hnsw(
        &self,
        index: &VectorIndexRef,
        entry: &StoredVectorEntry,
        fallback_accountant: Option<&dyn MemoryBudget>,
    ) {
        let Some(lock) = self.hnsw.get() else {
            return;
        };
        let _publication = self.generation_publication.lock();
        if self
            .fresh_tail_replay_frontier_active
            .load(Ordering::SeqCst)
            && entry.lsn.0 <= self.fresh_tail_replayed_through_lsn.load(Ordering::SeqCst)
        {
            return;
        }
        let guard = lock.read();
        let Some(hnsw) = guard.as_ref() else {
            return;
        };
        let old_bytes = self.hnsw_bytes.load(Ordering::SeqCst);
        let new_bytes = HnswIndex::estimated_resident_bytes_with_m(
            hnsw.len().saturating_add(1),
            self.dimension,
            self.quantization,
            hnsw.policy_values().0,
        );
        let additional_bytes = new_bytes.saturating_sub(old_bytes);
        let recorded_accountant = self.hnsw_accountant.read().clone();
        let allocation = recorded_accountant
            .as_deref()
            .or(fallback_accountant)
            .map(|accountant| {
                accountant.try_allocate_for(
                    additional_bytes,
                    "vector_index",
                    &format!("extend_hnsw@{}.{}", index.table, index.column),
                    "Reduce vector volume or raise MEMORY_LIMIT so committed vectors remain on the maintained indexed route.",
                )
            })
            .transpose();
        if allocation.is_err() {
            drop(guard);
            self.clear_hnsw_under_generation_publication(fallback_accountant);
            return;
        }
        if let Err(_error) = hnsw.insert(entry) {
            if additional_bytes != 0
                && let Some(accountant) = recorded_accountant.as_deref().or(fallback_accountant)
            {
                accountant.release(additional_bytes);
            }
            drop(guard);
            self.clear_hnsw_under_generation_publication(fallback_accountant);
            return;
        }
        self.hnsw_bytes.store(new_bytes, Ordering::SeqCst);
        self.fresh_tail_mutated_through_lsn
            .fetch_max(entry.lsn.0, Ordering::SeqCst);
        self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
    }

    fn tombstone_row(&self, row_id: RowId, deleted_tx: TxId) -> usize {
        debug_assert!(self.raw_resident.load(Ordering::SeqCst));
        let mut released = 0usize;
        let mut vectors = self.vectors.write();
        let positions = vectors.by_row.get(&row_id).cloned().unwrap_or_default();
        for position in positions {
            if let Some(entry) = vectors.entries.get_mut(position) {
                // A raw publication with an earlier ID closes every prior
                // interval at that boundary, including already retired ones.
                // Never create an interval whose end precedes its start.
                let boundary = deleted_tx.max(entry.created_tx);
                if entry.deleted_tx.is_none_or(|previous| previous > boundary) {
                    if entry.deleted_tx.is_none() {
                        released = released.saturating_add(entry.estimated_bytes());
                    }
                    entry.deleted_tx = Some(boundary);
                }
            }
        }
        for (directory_position, entry) in self
            .raw_directory
            .write()
            .iter_mut()
            .enumerate()
            .filter(|(_, entry)| entry.row_id == row_id)
        {
            let boundary = deleted_tx.max(entry.created_tx);
            let previous = entry.deleted_tx;
            if previous.is_some_and(|previous| previous <= boundary) {
                continue;
            }
            #[cfg(feature = "test-seams")]
            {
                let mut backlog = self.base_backlog.lock();
                if previous.is_none() && entry.created_tx.0 > backlog.covered_tx {
                    backlog.pending.inserts = backlog.pending.inserts.saturating_sub(1);
                }
                if previous.is_some_and(|previous| previous.0 > backlog.covered_tx) {
                    backlog.pending.tombstones = backlog.pending.tombstones.saturating_sub(1);
                }
                if boundary.0 > backlog.covered_tx {
                    backlog.pending.tombstones += 1;
                }
                backlog.record_high_water();
            }
            entry.deleted_tx = Some(boundary);
            let mut counts = self.visibility_counts.write();
            if let Some(previous) = previous {
                let position = counts
                    .deleted
                    .binary_search(&previous)
                    .expect("every directory tombstone has a visibility boundary");
                counts.deleted.remove(position);
            }
            VectorVisibilityCounts::insert(&mut counts.deleted, boundary);
            counts.set_interval(directory_position, entry);
        }
        released
    }

    fn clear_entries(&self) {
        let _gate = self.raw_residency_gate.write();
        let mut vectors = self.vectors.write();
        vectors.entries.clear();
        vectors.by_row.clear();
        self.raw_directory.write().clear();
        self.raw_directory_by_row.write().clear();
        *self.visibility_counts.write() = VectorVisibilityCounts::default();
        self.raw_loader.write().take();
        let bytes = self.raw_resident_bytes.swap(0, Ordering::SeqCst);
        let accountant = self.raw_accountant.write().take();
        if bytes != 0
            && let Some(accountant) = accountant
        {
            accountant.release(bytes);
        }
        self.raw_resident.store(true, Ordering::SeqCst);
    }

    /// Whether this partition's durable directory names any version of one of
    /// `row_ids`. A directory-only answer: nothing is faulted in.
    fn directory_names_any_row<'a>(&self, row_ids: impl IntoIterator<Item = &'a RowId>) -> bool {
        let by_row = self.raw_directory_by_row.read();
        row_ids
            .into_iter()
            .any(|row_id| by_row.contains_key(row_id))
    }

    /// Physically drop every version `keep` rejects, whether or not this
    /// partition's bodies are resident, and return the bytes the caller still
    /// owns (see `retain_entries`).
    ///
    /// A partition whose directory names nothing to drop is left untouched.
    /// A resident partition drops the bodies through `retain_entries`, which
    /// returns them from this state's own record. A dormant partition holds no
    /// body and no charge -- its bodies were never loaded, so nothing was ever
    /// charged for them -- so only its directory shrinks: nothing is faulted
    /// in and nothing is released. The caller has already removed the same
    /// versions from durable storage, so the partition's next load reads the
    /// pruned durable membership and matches this trimmed directory exactly.
    fn prune_directory_versions(
        &self,
        mut keep: impl FnMut(&RawVectorDirectoryEntry) -> bool,
    ) -> usize {
        let _gate = self.raw_residency_gate.write();
        if self.raw_directory.read().iter().all(&mut keep) {
            return 0;
        }
        if self.raw_resident.load(Ordering::SeqCst) {
            return self.retain_entries(|entry| keep(&Self::directory_entry(entry)));
        }
        self.raw_directory.write().retain(|entry| keep(entry));
        self.rebuild_raw_directory_positions();
        0
    }

    /// Physically drop the entries `keep` rejects. When this state carries
    /// the accountant it owns every resident body, tombstoned or not, so the
    /// dropped bytes are returned from its own record right here and the
    /// caller receives `0`. A state without an accountant instead hands back
    /// only the bytes the caller's per-insert admission still holds: a body
    /// that was tombstoned had that admission returned at the commit that
    /// superseded it, so only a still-current body is reported.
    ///
    /// Precondition: the bodies are resident. `prune_directory_versions` is
    /// the one caller and reaches here only on that branch; a dormant
    /// partition takes its directory-only path instead.
    fn retain_entries(&self, mut keep: impl FnMut(&StoredVectorEntry) -> bool) -> usize {
        debug_assert!(self.raw_resident.load(Ordering::SeqCst));
        let state_owned = self.raw_accountant.read().is_some();
        let mut vectors = self.vectors.write();
        let mut released = 0usize;
        vectors.entries.retain(|entry| {
            if keep(entry) {
                true
            } else {
                if state_owned || entry.deleted_tx.is_none() {
                    released = released.saturating_add(entry.estimated_bytes());
                }
                false
            }
        });
        vectors.rebuild_row_positions();
        *self.raw_directory.write() = vectors.entries.iter().map(Self::directory_entry).collect();
        self.rebuild_raw_directory_positions();
        self.raw_resident_bytes.store(
            vectors
                .entries
                .iter()
                .map(StoredVectorEntry::estimated_bytes)
                .sum(),
            Ordering::SeqCst,
        );
        drop(vectors);
        #[cfg(any(test, feature = "test-seams"))]
        if released != 0 {
            self.note_passive_activity(|activity| {
                activity
                    .hnsw_repairs_or_compactions
                    .fetch_add(1, Ordering::SeqCst);
            });
        }
        if state_owned
            && released != 0
            && let Some(accountant) = self.raw_accountant.read().clone()
        {
            accountant.release(released);
            return 0;
        }
        released
    }

    pub fn clear_hnsw(&self, accountant: &dyn MemoryBudget) {
        self.clear_hnsw_with_optional_accountant(Some(accountant));
    }

    fn drop_hnsw_without_accounting(&self) {
        self.clear_hnsw_with_optional_accountant(None);
    }

    fn clear_hnsw_with_optional_accountant(&self, accountant: Option<&dyn MemoryBudget>) {
        let _publication = self.generation_publication.lock();
        self.clear_hnsw_under_generation_publication(accountant);
    }

    fn clear_hnsw_under_generation_publication(&self, accountant: Option<&dyn MemoryBudget>) {
        let bytes = self.remove_hnsw_graph();
        let recorded_accountant = self.hnsw_accountant.write().take();
        if bytes != 0 {
            if let Some(accountant) = recorded_accountant {
                accountant.release(bytes);
            } else if let Some(accountant) = accountant {
                accountant.release(bytes);
            }
        }
        self.clear_sealed_hnsw();
        *self.serving_policy.write() = None;
        self.inspection_generation_id.store(0, Ordering::SeqCst);
        self.inspection_base_tx.store(0, Ordering::SeqCst);
        self.maintenance_sample_active
            .store(false, Ordering::SeqCst);
        self.maintenance_sample_tx.store(0, Ordering::SeqCst);
        self.maintenance_sample_lsn.store(0, Ordering::SeqCst);
        self.maintenance_sample_origin.store(0, Ordering::SeqCst);
        self.maintenance_sample_prior_replay_frontier_active
            .store(false, Ordering::SeqCst);
        self.maintenance_sample_prior_replayed_through_lsn
            .store(0, Ordering::SeqCst);
        self.maintenance_sample_prior_mutated_through_lsn
            .store(0, Ordering::SeqCst);
    }

    fn release_sealed_generation(generation: SealedHnswGeneration) {
        if generation.bytes != 0
            && let Some(accountant) = generation.accountant
        {
            accountant.release(generation.bytes);
        }
    }

    /// A loader transfers both reservations to this store only on successful
    /// installation.  Every rejection path must hand them back here: dropping
    /// the Rust values alone cannot return a budget reservation.
    fn release_uninstalled_loaded_generation(loaded: LoadedVectorGraphGeneration) {
        if loaded.resident_bytes != 0 {
            loaded.accountant.release(loaded.resident_bytes);
        }
        if let Some(tail) = loaded.fresh_tail
            && tail.resident_bytes != 0
        {
            tail.accountant.release(tail.resident_bytes);
        }
    }

    fn layer_identity(
        resident: Option<&SealedHnswGeneration>,
        dormant: Option<&DormantSealedGeneration>,
    ) -> Option<VectorGraphGeneration> {
        dormant
            .map(|generation| generation.descriptor.identity)
            .or_else(|| resident.map(|generation| generation.identity))
    }

    fn retired_chain_base_identity(
        chain: &RetiredSealedHnswChain,
    ) -> Option<VectorGraphGeneration> {
        Self::layer_identity(chain.base.as_ref(), chain.dormant_base.as_ref())
    }

    fn retired_chain_change_identity(
        chain: &RetiredSealedHnswChain,
    ) -> Option<VectorGraphGeneration> {
        Self::layer_identity(chain.change.as_ref(), chain.dormant_change.as_ref())
    }

    fn retire_current_chain(sealed: &mut SealedHnswGenerations) {
        if sealed.base.is_none()
            && sealed.change.is_none()
            && sealed.dormant_base.is_none()
            && sealed.dormant_change.is_none()
        {
            return;
        }
        sealed.retired.push(RetiredSealedHnswChain {
            base: sealed.base.take(),
            change: sealed.change.take(),
            dormant_base: sealed.dormant_base.take(),
            dormant_change: sealed.dormant_change.take(),
        });
    }

    fn selected_chain_location(
        sealed: &SealedHnswGenerations,
        snapshot_tx: TxId,
    ) -> Option<SealedChainLocation> {
        let current = Self::layer_identity(sealed.base.as_ref(), sealed.dormant_base.as_ref())
            .filter(|identity| identity.covered_tx <= snapshot_tx)
            .map(|identity| (identity, SealedChainLocation::Current));
        sealed
            .retired
            .iter()
            .filter_map(|chain| {
                let identity = Self::retired_chain_base_identity(chain)?;
                (identity.covered_tx <= snapshot_tx).then_some((
                    identity,
                    SealedChainLocation::Retired {
                        base_generation_id: identity.generation_id,
                    },
                ))
            })
            .chain(current)
            .max_by_key(|(identity, _)| (identity.covered_tx, identity.generation_id))
            .map(|(_, location)| location)
    }

    /// Topology of the published chain selected for this snapshot, without
    /// loading any graph. Pending replacements never describe serving work.
    pub fn snapshot_serving_policy(&self, snapshot: SnapshotId) -> Option<VectorServingPolicy> {
        let snapshot_tx = TxId::from_snapshot(snapshot);
        let sealed = self.sealed_hnsw.read();
        let generation = match Self::selected_chain_location(&sealed, snapshot_tx) {
            Some(SealedChainLocation::Current) => {
                Self::layer_identity(sealed.base.as_ref(), sealed.dormant_base.as_ref())
            }
            Some(SealedChainLocation::Retired { base_generation_id }) => sealed
                .retired
                .iter()
                .filter_map(Self::retired_chain_base_identity)
                .find(|identity| identity.generation_id == base_generation_id),
            None => {
                return (self.max_tx() <= snapshot_tx)
                    .then(|| self.serving_policy())
                    .flatten();
            }
        }?;
        Some(VectorServingPolicy {
            hnsw_m: generation.hnsw_m as usize,
            hnsw_ef_construction: generation.hnsw_ef_construction as usize,
            policy_revision: generation.policy_revision,
        })
    }

    pub(crate) fn dormant_load_bytes_for_snapshot(&self, snapshot: SnapshotId) -> usize {
        fn missing_bytes(
            resident: Option<&SealedHnswGeneration>,
            dormant: Option<&DormantSealedGeneration>,
        ) -> usize {
            dormant
                .filter(|dormant| {
                    !resident
                        .is_some_and(|resident| resident.identity == dormant.descriptor.identity)
                })
                .map_or(0, |dormant| dormant.descriptor.load_bytes)
        }

        let sealed = self.sealed_hnsw.read();
        match Self::selected_chain_location(&sealed, TxId::from_snapshot(snapshot)) {
            Some(SealedChainLocation::Current) => {
                missing_bytes(sealed.base.as_ref(), sealed.dormant_base.as_ref()).saturating_add(
                    missing_bytes(sealed.change.as_ref(), sealed.dormant_change.as_ref()),
                )
            }
            Some(SealedChainLocation::Retired { base_generation_id }) => sealed
                .retired
                .iter()
                .find(|chain| {
                    Self::retired_chain_base_identity(chain)
                        .is_some_and(|base| base.generation_id == base_generation_id)
                })
                .map_or(0, |chain| {
                    missing_bytes(chain.base.as_ref(), chain.dormant_base.as_ref()).saturating_add(
                        missing_bytes(chain.change.as_ref(), chain.dormant_change.as_ref()),
                    )
                }),
            None => 0,
        }
    }

    fn clear_sealed_hnsw(&self) {
        let mut sealed = self.sealed_hnsw.write();
        let base = sealed.base.take();
        let change = sealed.change.take();
        let retired = std::mem::take(&mut sealed.retired);
        sealed.dormant_base = None;
        sealed.dormant_change = None;
        sealed.pending_base = None;
        sealed.pending_generation_count = 0;
        sealed.quarantine = None;
        for generation in base.into_iter().chain(change) {
            Self::release_sealed_generation(generation);
        }
        for chain in retired {
            for generation in chain.base.into_iter().chain(chain.change) {
                Self::release_sealed_generation(generation);
            }
        }
    }

    fn remove_hnsw_graph(&self) -> usize {
        if let Some(lock) = self.hnsw.get() {
            *lock.write() = None;
        }
        self.fresh_tail_replayed_through_lsn
            .store(0, Ordering::SeqCst);
        self.fresh_tail_replay_frontier_active
            .store(false, Ordering::SeqCst);
        self.fresh_tail_mutated_through_lsn
            .store(0, Ordering::SeqCst);
        self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
        self.hnsw_bytes.swap(0, Ordering::SeqCst)
    }

    pub fn hnsw_len(&self) -> Option<usize> {
        self.hnsw_len_after_tail(|| {})
    }

    fn hnsw_len_after_tail(&self, after_tail: impl FnOnce()) -> Option<usize> {
        // Base publication also retires the old tail. Sample both owners
        // under that same boundary so one node cannot be counted twice.
        let _publication = self.generation_publication.lock();
        let tail = self
            .hnsw
            .get()
            .and_then(|lock| lock.read().as_ref().map(|hnsw| hnsw.len()));
        after_tail();
        let sealed = self.sealed_hnsw.read();
        let sealed_len = sealed
            .base
            .as_ref()
            .map(|generation| generation.graph.len())
            .unwrap_or(0)
            .saturating_add(
                sealed
                    .change
                    .as_ref()
                    .map(|generation| generation.graph.len())
                    .unwrap_or(0),
            );
        let total = sealed_len.saturating_add(tail.unwrap_or(0));
        (total != 0).then_some(total)
    }

    pub(crate) fn has_complete_hnsw_route(&self) -> bool {
        let tail_ready = self
            .hnsw
            .get()
            .and_then(|tail| tail.read().as_ref().map(|graph| !graph.is_empty()))
            .unwrap_or(false)
            && self.serving_policy().is_some()
            && !self.maintenance_sample_active.load(Ordering::SeqCst);
        let sealed = self.sealed_hnsw.read();
        if sealed.quarantine.is_some() {
            return false;
        }
        let base_known = sealed.base.is_some() || sealed.dormant_base.is_some();
        let change_known = sealed.change.is_some() || sealed.dormant_change.is_some();
        base_known || (!change_known && tail_ready)
    }

    pub fn hnsw_stats(&self) -> Option<crate::HnswGraphStats> {
        self.hnsw
            .get()
            .and_then(|lock| lock.read().as_ref().map(|hnsw| hnsw.graph_stats()))
    }

    /// Record a verified catalog entry without reading its graph pages.  The
    /// replacement is only metadata, so it never affects a currently serving
    /// resident graph.  The engine chooses when the catalog pointer itself is
    /// durable; this store only makes the descriptor visible atomically.
    #[doc(hidden)]
    pub fn register_dormant_sealed_generation(
        &self,
        is_base: bool,
        descriptor: DormantVectorGraphGeneration,
        loader: Arc<dyn DormantVectorGraphLoader>,
    ) -> Result<()> {
        if descriptor.load_bytes < descriptor.resident_bytes {
            return Err(Error::Other(
                "dormant vector load admission is smaller than resident ownership".to_string(),
            ));
        }
        let mut sealed = self.sealed_hnsw.write();
        let current_identity = if is_base {
            Self::layer_identity(sealed.base.as_ref(), sealed.dormant_base.as_ref())
        } else {
            Self::layer_identity(sealed.change.as_ref(), sealed.dormant_change.as_ref())
        };
        if current_identity
            .is_some_and(|current| current.generation_id > descriptor.identity.generation_id)
        {
            return Err(Error::Other(
                "refusing to replace a newer dormant vector generation with an older one"
                    .to_string(),
            ));
        }
        if !is_base
            && current_identity
                .is_some_and(|current| current.generation_id != descriptor.identity.generation_id)
        {
            return Err(Error::Other(
                "a vector change generation can only be superseded by a replacement base"
                    .to_string(),
            ));
        }
        let has_current_chain = sealed.base.is_some()
            || sealed.change.is_some()
            || sealed.dormant_base.is_some()
            || sealed.dormant_change.is_some();
        if is_base
            && has_current_chain
            && current_identity
                .is_none_or(|current| current.generation_id != descriptor.identity.generation_id)
        {
            if sealed.pending_base.as_ref().is_some_and(|pending| {
                pending.descriptor.identity.generation_id > descriptor.identity.generation_id
            }) {
                return Err(Error::Other(
                    "refusing to replace a newer pending vector base with an older one".to_string(),
                ));
            }
            let was_absent = sealed.pending_base.is_none();
            sealed.pending_base = Some(DormantSealedGeneration { descriptor, loader });
            if was_absent {
                sealed.pending_generation_count = sealed.pending_generation_count.saturating_add(1);
            }
            return Ok(());
        }
        let resident_matches = if is_base {
            sealed
                .base
                .as_ref()
                .is_some_and(|generation| generation.identity == descriptor.identity)
        } else {
            sealed
                .change
                .as_ref()
                .is_some_and(|generation| generation.identity == descriptor.identity)
        };
        let target = if is_base {
            &mut sealed.dormant_base
        } else {
            &mut sealed.dormant_change
        };
        let was_absent = target.is_none();
        *target = Some(DormantSealedGeneration { descriptor, loader });
        if was_absent && !resident_matches {
            sealed.pending_generation_count = sealed.pending_generation_count.saturating_add(1);
        }
        drop(sealed);
        self.set_serving_policy_from_generation(descriptor.identity);
        Ok(())
    }

    /// Install an owned sealed graph after a codec has fully verified it and
    /// reserved its resident bytes.  A loaded graph is never inserted into:
    /// future commits extend only `hnsw`, the fresh process-created tail.
    #[doc(hidden)]
    pub fn install_loaded_sealed_generation(
        &self,
        is_base: bool,
        identity: VectorGraphGeneration,
        loaded: LoadedVectorGraphGeneration,
    ) -> Result<()> {
        let _publication = self.generation_publication.lock();
        self.install_loaded_sealed_generation_inner(is_base, identity, loaded, false)
    }

    fn install_loaded_sealed_generation_inner(
        &self,
        is_base: bool,
        identity: VectorGraphGeneration,
        loaded: LoadedVectorGraphGeneration,
        preserve_dormant_descriptor: bool,
    ) -> Result<()> {
        if loaded.resident_bytes == 0 && !loaded.graph.is_empty() {
            Self::release_uninstalled_loaded_generation(loaded);
            return Err(Error::Other(
                "loaded vector generation has graph entries but no reserved resident bytes"
                    .to_string(),
            ));
        }
        if loaded
            .fresh_tail
            .as_ref()
            .is_some_and(|tail| tail.resident_bytes == 0 && !tail.graph.is_empty())
        {
            Self::release_uninstalled_loaded_generation(loaded);
            return Err(Error::Other(
                "loaded vector tail has graph entries but no reserved resident bytes".to_string(),
            ));
        }
        let LoadedVectorGraphGeneration {
            graph,
            resident_bytes,
            accountant,
            mut fresh_tail,
        } = loaded;
        // A row commit already admitted and validated insertion against this
        // fresh graph. A concurrent sealed-page eviction/reload may refresh the
        // base, but must not replace that insertion target before publication.
        if self.fresh_tail_preparations.load(Ordering::SeqCst) != 0
            && let Some(tail) = fresh_tail.take()
        {
            tail.accountant.release(tail.resident_bytes);
        }
        let mut sealed = self.sealed_hnsw.write();
        let current_generation_id = if is_base {
            Self::layer_identity(sealed.base.as_ref(), sealed.dormant_base.as_ref())
                .map(|identity| identity.generation_id)
        } else {
            Self::layer_identity(sealed.change.as_ref(), sealed.dormant_change.as_ref())
                .map(|identity| identity.generation_id)
        };
        if current_generation_id.is_some_and(|current| current > identity.generation_id) {
            Self::release_uninstalled_loaded_generation(LoadedVectorGraphGeneration {
                graph,
                resident_bytes,
                accountant,
                fresh_tail,
            });
            return Err(Error::Other(
                "refusing to install an older sealed vector generation".to_string(),
            ));
        }
        if !is_base
            && current_generation_id.is_some_and(|current| current != identity.generation_id)
        {
            Self::release_uninstalled_loaded_generation(LoadedVectorGraphGeneration {
                graph,
                resident_bytes,
                accountant,
                fresh_tail,
            });
            return Err(Error::Other(
                "a sealed vector change can only be superseded by a replacement base".to_string(),
            ));
        }
        let incoming = SealedHnswGeneration {
            identity,
            graph,
            bytes: resident_bytes,
            accountant: Some(accountant),
        };
        let installing_pending_base = is_base
            && sealed
                .pending_base
                .as_ref()
                .is_some_and(|pending| pending.descriptor.identity == identity);
        if is_base
            && (installing_pending_base
                || current_generation_id.is_some_and(|current| current != identity.generation_id))
        {
            Self::retire_current_chain(&mut sealed);
        }
        if installing_pending_base {
            sealed.dormant_base = sealed.pending_base.take();
        }
        let old = if is_base {
            sealed.base.replace(incoming)
        } else {
            sealed.change.replace(incoming)
        };
        if let Some(old) = old {
            Self::release_sealed_generation(old);
        }
        if !preserve_dormant_descriptor {
            if is_base {
                sealed.dormant_base = None;
            } else {
                sealed.dormant_change = None;
            }
        }
        sealed.pending_generation_count = sealed.pending_generation_count.saturating_sub(1);
        drop(sealed);
        self.set_serving_policy_from_generation(identity);
        if let Some(tail) = fresh_tail {
            let LoadedVectorFreshTail {
                graph,
                resident_bytes,
                accountant,
                replayed_through_lsn,
            } = tail;
            let prior_bytes = self.remove_hnsw_graph();
            if prior_bytes != 0
                && let Some(prior_accountant) = self.hnsw_accountant.write().take()
            {
                prior_accountant.release(prior_bytes);
            }
            self.set_hnsw(Some(graph), resident_bytes);
            self.set_hnsw_bytes_with_accountant(resident_bytes, accountant);
            self.fresh_tail_replayed_through_lsn
                .store(replayed_through_lsn.0, Ordering::SeqCst);
            self.fresh_tail_replay_frontier_active
                .store(true, Ordering::SeqCst);
            self.fresh_tail_mutated_through_lsn
                .store(replayed_through_lsn.0, Ordering::SeqCst);
        }
        #[cfg(any(test, feature = "test-seams"))]
        self.note_passive_activity(|activity| {
            activity
                .sealed_generation_loads
                .fetch_add(1, Ordering::SeqCst);
        });
        Ok(())
    }

    /// Build preparation completes before this call.  Publication swaps the
    /// complete replacement base under one lock, then starts with no mutable
    /// tail; callers may create and populate that tail only after the commit
    /// that made its change journal durable.
    #[doc(hidden)]
    pub fn publish_replacement_base_generation(
        &self,
        identity: VectorGraphGeneration,
        loaded: LoadedVectorGraphGeneration,
    ) -> Result<()> {
        let _publication = self.generation_publication.lock();
        self.install_loaded_sealed_generation_inner(true, identity, loaded, false)?;
        let old_tail_bytes = self.remove_hnsw_graph();
        if old_tail_bytes != 0
            && let Some(accountant) = self.hnsw_accountant.write().take()
        {
            accountant.release(old_tail_bytes);
        }
        Ok(())
    }

    /// The engine calls this only while holding its snapshot-removal guard.
    /// Retain exactly the retired chains selected by registered snapshots.
    /// Newer readers do not pin older chains that they cannot select.
    #[doc(hidden)]
    pub fn reclaim_snapshot_free_graph_generations(
        &self,
        registered_snapshots: &[SnapshotId],
    ) -> usize {
        let mut sealed = self.sealed_hnsw.write();
        let needed = registered_snapshots
            .iter()
            .filter_map(|snapshot| {
                match Self::selected_chain_location(&sealed, TxId::from_snapshot(*snapshot)) {
                    Some(SealedChainLocation::Retired { base_generation_id }) => {
                        Some(base_generation_id)
                    }
                    _ => None,
                }
            })
            .collect::<HashSet<_>>();
        let retired = std::mem::take(&mut sealed.retired);
        let mut count = 0usize;
        for chain in retired {
            if Self::retired_chain_base_identity(&chain)
                .is_some_and(|base| needed.contains(&base.generation_id))
            {
                sealed.retired.push(chain);
                continue;
            }
            if chain.base.is_none() && chain.dormant_base.is_some() {
                sealed.pending_generation_count = sealed.pending_generation_count.saturating_sub(1);
            }
            if chain.change.is_none() && chain.dormant_change.is_some() {
                sealed.pending_generation_count = sealed.pending_generation_count.saturating_sub(1);
            }
            count = count
                .saturating_add(usize::from(
                    chain.base.is_some() || chain.dormant_base.is_some(),
                ))
                .saturating_add(usize::from(
                    chain.change.is_some() || chain.dormant_change.is_some(),
                ));
            for generation in chain.base.into_iter().chain(chain.change) {
                Self::release_sealed_generation(generation);
            }
        }
        count
    }

    fn snapshot_retained_chain_ids(&self, snapshots: &[SnapshotId]) -> Vec<(u64, Option<u64>)> {
        let sealed = self.sealed_hnsw.read();
        snapshots
            .iter()
            .filter_map(|snapshot| {
                let SealedChainLocation::Retired { base_generation_id } =
                    Self::selected_chain_location(&sealed, TxId::from_snapshot(*snapshot))?
                else {
                    return None;
                };
                let chain = sealed.retired.iter().find(|chain| {
                    Self::retired_chain_base_identity(chain)
                        .is_some_and(|base| base.generation_id == base_generation_id)
                })?;
                Some((
                    base_generation_id,
                    Self::retired_chain_change_identity(chain).map(|change| change.generation_id),
                ))
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn retained_chain_ids(&self) -> Vec<(u64, Option<u64>)> {
        self.sealed_hnsw
            .read()
            .retired
            .iter()
            .filter_map(|chain| {
                let base = Self::retired_chain_base_identity(chain)?;
                Some((
                    base.generation_id,
                    Self::retired_chain_change_identity(chain).map(|change| change.generation_id),
                ))
            })
            .collect()
    }

    /// Cheap read-only peek at whether
    /// [`Self::reclaim_snapshot_free_graph_generations`] would have anything
    /// to release right now: a non-empty `retired` queue. Never drains or
    /// mutates the queue, and does not consult registered snapshots -- a
    /// caller uses this only to decide whether beginning a guarded removal
    /// pass is worth attempting this cycle.
    #[doc(hidden)]
    pub fn has_retired_sealed_generations(&self) -> bool {
        !self.sealed_hnsw.read().retired.is_empty()
    }

    #[doc(hidden)]
    pub fn graph_generation_status(&self) -> VectorGraphGenerationStatus {
        let sealed = self.sealed_hnsw.read();
        VectorGraphGenerationStatus {
            base: sealed.base.as_ref().map(|generation| generation.identity),
            change: sealed.change.as_ref().map(|generation| generation.identity),
            dormant_base: sealed
                .dormant_base
                .as_ref()
                .map(|generation| generation.descriptor.identity),
            dormant_change: sealed
                .dormant_change
                .as_ref()
                .map(|generation| generation.descriptor.identity),
            base_resident: sealed.base.is_some(),
            change_resident: sealed.change.is_some(),
            fresh_tail_entries: self
                .hnsw
                .get()
                .and_then(|tail| tail.read().as_ref().map(HnswIndex::len))
                .unwrap_or(0),
            retained_generations: sealed.retired.iter().fold(0usize, |count, chain| {
                count
                    .saturating_add(usize::from(
                        chain.base.is_some() || chain.dormant_base.is_some(),
                    ))
                    .saturating_add(usize::from(
                        chain.change.is_some() || chain.dormant_change.is_some(),
                    ))
            }),
            pending_generation_count: sealed.pending_generation_count,
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn superseded_generation_retention_for_test(&self) -> (usize, usize) {
        let sealed = self.sealed_hnsw.read();
        let superseded_base = if sealed.change.is_some() || sealed.dormant_change.is_some() {
            sealed.base.as_ref()
        } else {
            None
        };
        (
            sealed
                .retired
                .iter()
                .fold(0usize, |count, chain| {
                    count
                        .saturating_add(usize::from(chain.base.is_some()))
                        .saturating_add(usize::from(chain.change.is_some()))
                })
                .saturating_add(usize::from(superseded_base.is_some())),
            sealed
                .retired
                .iter()
                .fold(0usize, |bytes, chain| {
                    bytes
                        .saturating_add(
                            chain
                                .base
                                .as_ref()
                                .map(|generation| generation.bytes)
                                .unwrap_or(0),
                        )
                        .saturating_add(
                            chain
                                .change
                                .as_ref()
                                .map(|generation| generation.bytes)
                                .unwrap_or(0),
                        )
                })
                .saturating_add(
                    superseded_base
                        .map(|generation| generation.bytes)
                        .unwrap_or(0),
                ),
        )
    }

    fn set_route_quarantine(&self, reason: VectorRouteQuarantineReason) {
        self.sealed_hnsw.write().quarantine = Some(reason);
    }

    fn clear_route_quarantine(&self) {
        self.sealed_hnsw.write().quarantine = None;
    }

    pub(crate) fn route_quarantine(&self) -> Option<VectorRouteQuarantineReason> {
        self.sealed_hnsw.read().quarantine
    }

    /// Borrow a resident sealed graph for a codec-owned export.  No clone or
    /// byte encoding occurs here; the caller's closure is the only place a
    /// codec may serialize it, so accounting cannot be bypassed by a hidden
    /// graph copy.
    #[doc(hidden)]
    pub fn with_resident_sealed_generation<R>(
        &self,
        is_base: bool,
        f: impl FnOnce(VectorGraphGeneration, &HnswIndex) -> R,
    ) -> Option<R> {
        let sealed = self.sealed_hnsw.read();
        let generation = if is_base {
            sealed.base.as_ref()
        } else {
            sealed.change.as_ref()
        }?;
        Some(f(generation.identity, &generation.graph))
    }

    /// Load one cataloged graph without holding a store lock across I/O or
    /// decode.  The loader transfers a single pre-reserved resident allocation
    /// with the owned graph.  A racing successful load releases its own
    /// reservation and keeps the first complete publication.
    #[doc(hidden)]
    pub fn preload_dormant_sealed_generation(&self, is_base: bool) -> Result<bool> {
        let mut checkpoint = || Ok::<(), Error>(());
        let mut reserve = |_bytes| Ok::<(), Error>(());
        let mut release = |_bytes| {};
        self.preload_dormant_sealed_generation_with(
            is_base,
            false,
            &mut checkpoint,
            &mut reserve,
            &mut release,
        )
    }

    fn load_dormant_generation<E>(
        &self,
        dormant: &DormantSealedGeneration,
        corruption_reason: VectorRouteQuarantineReason,
        caller_charged: bool,
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<LoadedVectorGraphGeneration, E>
    where
        E: From<Error>,
    {
        let initial_caller_reservation = if caller_charged {
            dormant.descriptor.load_bytes
        } else {
            0
        };
        if caller_charged {
            before_checkpoint()?;
            before_retain(initial_caller_reservation)?;
        }
        struct ReplayProgress<'a> {
            state: &'a IndexState,
            owned: bool,
        }
        impl Drop for ReplayProgress<'_> {
            fn drop(&mut self) {
                if self.owned {
                    *self.state.maintenance_progress.write() = None;
                }
            }
        }
        let mut progress_owner = ReplayProgress {
            state: self,
            owned: false,
        };
        let mut interrupted = None;
        let mut caller_reserved = initial_caller_reservation;
        let loaded = {
            let mut request = |request| {
                if interrupted.is_some() {
                    return false;
                }
                let result = match request {
                    DormantVectorLoadRequest::Checkpoint => before_checkpoint(),
                    DormantVectorLoadRequest::Progress { done, total } => {
                        {
                            let mut slot = self.maintenance_progress.write();
                            if slot.is_none() {
                                *slot = Some(VectorMaintenanceProgress {
                                    state: "replaying",
                                    reason: "new_changes",
                                    vectors_done: 0,
                                    vectors_total: total,
                                    checkpoint_tx: dormant.descriptor.identity.covered_tx,
                                });
                                progress_owner.owned = true;
                            }
                        }
                        if progress_owner.owned {
                            self.set_maintenance_vectors_done(done);
                        }
                        before_checkpoint()
                    }
                    DormantVectorLoadRequest::Reserve(bytes) => {
                        if let Err(error) = before_checkpoint() {
                            Err(error)
                        } else if let Some(total) = caller_reserved.checked_add(bytes) {
                            match before_retain(bytes) {
                                Ok(()) => {
                                    caller_reserved = total;
                                    Ok(())
                                }
                                Err(error) => Err(error),
                            }
                        } else {
                            Err(E::from(Error::Other(
                                "bounded vector load reservation overflow".to_string(),
                            )))
                        }
                    }
                };
                match result {
                    Ok(()) => true,
                    Err(error) => {
                        interrupted = Some(error);
                        false
                    }
                }
            };
            dormant
                .loader
                .load(dormant.descriptor, initial_caller_reservation, &mut request)
        };
        match loaded {
            Err(error) => {
                if caller_charged {
                    release_retained(caller_reserved);
                }
                match interrupted {
                    Some(error) => Err(error),
                    None => {
                        if matches!(error, Error::StoreCorrupted { .. }) {
                            self.set_route_quarantine(corruption_reason);
                        }
                        Err(E::from(error))
                    }
                }
            }
            Ok(loaded) => {
                if caller_charged {
                    let tail_resident = loaded
                        .fresh_tail
                        .as_ref()
                        .map(|tail| tail.resident_bytes)
                        .unwrap_or(0);
                    let Some(adopted) = loaded.resident_bytes.checked_add(tail_resident) else {
                        Self::release_uninstalled_loaded_generation(loaded);
                        return Err(E::from(Error::Other(
                            "loaded vector residency overflow".to_string(),
                        )));
                    };
                    let Some(transient) = caller_reserved.checked_sub(adopted) else {
                        Self::release_uninstalled_loaded_generation(loaded);
                        return Err(E::from(Error::Other(
                            "loaded vector residency exceeds its caller reservation".to_string(),
                        )));
                    };
                    release_retained(transient);
                }
                if let Some(error) = interrupted {
                    Self::release_uninstalled_loaded_generation(loaded);
                    return Err(error);
                }
                if let Err(error) = before_checkpoint() {
                    Self::release_uninstalled_loaded_generation(loaded);
                    return Err(error);
                }
                Ok(loaded)
            }
        }
    }

    fn preload_dormant_sealed_generation_with<E>(
        &self,
        is_base: bool,
        caller_charged: bool,
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        for _ in 0..4 {
            let dormant = {
                let sealed = self.sealed_hnsw.read();
                let dormant = if is_base {
                    sealed
                        .pending_base
                        .clone()
                        .or_else(|| sealed.dormant_base.clone())
                } else {
                    sealed.dormant_change.clone()
                };
                if dormant.as_ref().is_some_and(|dormant| {
                    let resident = if is_base {
                        sealed.base.as_ref()
                    } else {
                        sealed.change.as_ref()
                    };
                    resident
                        .is_some_and(|resident| resident.identity == dormant.descriptor.identity)
                }) {
                    return Ok(false);
                }
                dormant
            };
            let Some(dormant) = dormant else {
                return Ok(false);
            };
            let expected_tail_revision = {
                let _publication = self.generation_publication.lock();
                self.fresh_tail_revision.load(Ordering::SeqCst)
            };
            let loaded = self.load_dormant_generation(
                &dormant,
                if is_base {
                    VectorRouteQuarantineReason::CorruptBase
                } else {
                    VectorRouteQuarantineReason::CorruptChanges
                },
                caller_charged,
                before_checkpoint,
                before_retain,
                release_retained,
            )?;
            #[cfg(feature = "test-seams")]
            if crate::test_seam::take_vector_post_load_reconciliation_failure_for_test() {
                Self::release_uninstalled_loaded_generation(loaded);
                return Err(E::from(Error::Other(
                    "injected vector post-load reconciliation failure".to_string(),
                )));
            }
            let identity = dormant.descriptor.identity;
            let _publication = self.generation_publication.lock();
            if loaded.fresh_tail.as_ref().is_some_and(|tail| {
                self.fresh_tail_revision.load(Ordering::SeqCst) != expected_tail_revision
                    || tail.replayed_through_lsn.0
                        < self.fresh_tail_mutated_through_lsn.load(Ordering::SeqCst)
            }) {
                Self::release_uninstalled_loaded_generation(loaded);
                continue;
            }
            let still_selected = {
                let sealed = self.sealed_hnsw.read();
                let selected = if is_base {
                    sealed
                        .pending_base
                        .as_ref()
                        .or(sealed.dormant_base.as_ref())
                } else {
                    sealed.dormant_change.as_ref()
                };
                selected.is_some_and(|selected| selected.descriptor.identity == identity)
            };
            if !still_selected {
                Self::release_uninstalled_loaded_generation(loaded);
                return Ok(false);
            }
            let already_resident = {
                let sealed = self.sealed_hnsw.read();
                if is_base {
                    sealed
                        .base
                        .as_ref()
                        .is_some_and(|generation| generation.identity == identity)
                } else {
                    sealed
                        .change
                        .as_ref()
                        .is_some_and(|generation| generation.identity == identity)
                }
            };
            if already_resident {
                Self::release_uninstalled_loaded_generation(loaded);
                return Ok(false);
            }
            self.install_loaded_sealed_generation_inner(is_base, identity, loaded, true)
                .map_err(E::from)?;
            return Ok(true);
        }
        Err(E::from(Error::Other(
            "vector fresh tail changed repeatedly while a dormant generation was loading"
                .to_string(),
        )))
    }

    fn preload_retired_sealed_generation(
        &self,
        base_generation_id: u64,
        is_base: bool,
    ) -> Result<bool> {
        let mut checkpoint = || Ok::<(), Error>(());
        let mut reserve = |_bytes| Ok::<(), Error>(());
        let mut release = |_bytes| {};
        self.preload_retired_sealed_generation_with(
            base_generation_id,
            is_base,
            false,
            &mut checkpoint,
            &mut reserve,
            &mut release,
        )
    }

    fn preload_retired_sealed_generation_with<E>(
        &self,
        base_generation_id: u64,
        is_base: bool,
        caller_charged: bool,
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        let dormant = {
            let sealed = self.sealed_hnsw.read();
            let Some(chain) = sealed.retired.iter().find(|chain| {
                Self::retired_chain_base_identity(chain)
                    .is_some_and(|identity| identity.generation_id == base_generation_id)
            }) else {
                return Ok(false);
            };
            let dormant = if is_base {
                chain.dormant_base.clone()
            } else {
                chain.dormant_change.clone()
            };
            let resident = if is_base {
                chain.base.as_ref()
            } else {
                chain.change.as_ref()
            };
            if dormant.as_ref().is_some_and(|dormant| {
                resident.is_some_and(|resident| resident.identity == dormant.descriptor.identity)
            }) {
                return Ok(false);
            }
            dormant
        };
        let Some(dormant) = dormant else {
            return Ok(false);
        };
        let loaded = self.load_dormant_generation(
            &dormant,
            if is_base {
                VectorRouteQuarantineReason::CorruptBase
            } else {
                VectorRouteQuarantineReason::CorruptChanges
            },
            caller_charged,
            before_checkpoint,
            before_retain,
            release_retained,
        )?;
        if loaded.resident_bytes == 0 && !loaded.graph.is_empty() {
            Self::release_uninstalled_loaded_generation(loaded);
            return Err(E::from(Error::Other(
                "loaded retired vector generation has graph entries but no reserved resident bytes"
                    .to_string(),
            )));
        }
        let identity = dormant.descriptor.identity;
        let _publication = self.generation_publication.lock();
        let mut sealed = self.sealed_hnsw.write();
        let Some(chain) = sealed.retired.iter_mut().find(|chain| {
            Self::retired_chain_base_identity(chain)
                .is_some_and(|base| base.generation_id == base_generation_id)
        }) else {
            drop(sealed);
            Self::release_uninstalled_loaded_generation(loaded);
            return Ok(false);
        };
        let selected = if is_base {
            chain.dormant_base.as_ref()
        } else {
            chain.dormant_change.as_ref()
        };
        if !selected.is_some_and(|selected| selected.descriptor.identity == identity) {
            drop(sealed);
            Self::release_uninstalled_loaded_generation(loaded);
            return Ok(false);
        }
        let resident = if is_base {
            &mut chain.base
        } else {
            &mut chain.change
        };
        if resident
            .as_ref()
            .is_some_and(|generation| generation.identity == identity)
        {
            drop(sealed);
            Self::release_uninstalled_loaded_generation(loaded);
            return Ok(false);
        }
        let LoadedVectorGraphGeneration {
            graph,
            resident_bytes,
            accountant,
            fresh_tail,
        } = loaded;
        if let Some(tail) = fresh_tail
            && tail.resident_bytes != 0
        {
            tail.accountant.release(tail.resident_bytes);
        }
        let old = resident.replace(SealedHnswGeneration {
            identity,
            graph,
            bytes: resident_bytes,
            accountant: Some(accountant),
        });
        sealed.pending_generation_count = sealed.pending_generation_count.saturating_sub(1);
        drop(sealed);
        if let Some(old) = old {
            Self::release_sealed_generation(old);
        }
        #[cfg(any(test, feature = "test-seams"))]
        self.note_passive_activity(|activity| {
            activity
                .sealed_generation_loads
                .fetch_add(1, Ordering::SeqCst);
        });
        Ok(true)
    }

    /// Eviction drops only a resident graph that still has a durable catalog
    /// descriptor. Raw vectors, metadata, and the descriptor remain, so any
    /// later selected read can load the same verified generation. A bounded
    /// read does so through its charged, cancellable request preloader.
    #[doc(hidden)]
    pub fn evict_resident_sealed_generation(&self, is_base: bool) -> bool {
        let Some(_publication) = self.generation_publication.try_lock() else {
            return false;
        };
        let Some(mut sealed) = self.sealed_hnsw.try_write() else {
            return false;
        };
        let descriptor_present = if is_base {
            sealed.dormant_base.is_some()
        } else {
            sealed.dormant_change.is_some()
        };
        if !descriptor_present {
            return false;
        }
        let removed = if is_base {
            sealed.base.take()
        } else {
            sealed.change.take()
        };
        if removed.is_some() {
            sealed.pending_generation_count = sealed.pending_generation_count.saturating_add(1);
        }
        drop(sealed);
        if let Some(generation) = removed {
            Self::release_sealed_generation(generation);
            // An empty replay tail owns memory too. It can be recreated from
            // the durable frontier, unless a prepared commit owns its target.
            if self.fresh_tail_preparations.load(Ordering::SeqCst) == 0
                && let Some(lock) = self.hnsw.get()
                && let Some(mut tail) = lock.try_write()
                && tail.as_ref().is_some_and(HnswIndex::is_empty)
            {
                *tail = None;
                let bytes = self.hnsw_bytes.swap(0, Ordering::SeqCst);
                if let Some(accountant) = self.hnsw_accountant.write().take() {
                    accountant.release(bytes);
                }
                self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
            }
            #[cfg(any(test, feature = "test-seams"))]
            self.note_passive_activity(|activity| {
                activity
                    .sealed_generation_evictions
                    .fetch_add(1, Ordering::SeqCst);
            });
            true
        } else {
            false
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn make_retired_sealed_generations_dormant_for_test(&self) -> usize {
        let _publication = self.generation_publication.lock();
        let mut sealed = self.sealed_hnsw.write();
        let mut removed = Vec::new();
        let mut pending = 0usize;
        for chain in &mut sealed.retired {
            let evict_base = chain.dormant_base.is_some() && chain.base.is_some();
            if evict_base && let Some(generation) = chain.base.take() {
                removed.push(generation);
                pending = pending.saturating_add(1);
            }
            let evict_change = chain.dormant_change.is_some() && chain.change.is_some();
            if evict_change && let Some(generation) = chain.change.take() {
                removed.push(generation);
                pending = pending.saturating_add(1);
            }
        }
        sealed.pending_generation_count = sealed.pending_generation_count.saturating_add(pending);
        let dormant = sealed.retired.iter().fold(0usize, |count, chain| {
            count
                .saturating_add(usize::from(chain.dormant_base.is_some()))
                .saturating_add(usize::from(chain.dormant_change.is_some()))
        });
        drop(sealed);
        for generation in removed {
            Self::release_sealed_generation(generation);
        }
        dormant
    }

    /// Remove a graph/catalog entry that the codec has proved corrupt or
    /// incompatible.  This is deliberately a quarantine operation, not a
    /// rebuild: callers receive the normal unavailable route until their
    /// maintenance path prepares a replacement from authoritative vectors.
    #[doc(hidden)]
    pub fn quarantine_sealed_generation(&self, is_base: bool, generation_id: u64) -> bool {
        let mut sealed = self.sealed_hnsw.write();
        let removed_pending = is_base
            && sealed.pending_base.as_ref().is_some_and(|generation| {
                generation.descriptor.identity.generation_id == generation_id
            });
        if removed_pending {
            sealed.pending_base = None;
            sealed.pending_generation_count = sealed.pending_generation_count.saturating_sub(1);
        }
        let resident = if is_base {
            if sealed
                .base
                .as_ref()
                .is_some_and(|generation| generation.identity.generation_id == generation_id)
            {
                sealed.base.take()
            } else {
                None
            }
        } else if sealed
            .change
            .as_ref()
            .is_some_and(|generation| generation.identity.generation_id == generation_id)
        {
            sealed.change.take()
        } else {
            None
        };
        let dormant = if is_base {
            &mut sealed.dormant_base
        } else {
            &mut sealed.dormant_change
        };
        let removed_descriptor = dormant.as_ref().is_some_and(|generation| {
            generation.descriptor.identity.generation_id == generation_id
        });
        if removed_descriptor {
            *dormant = None;
            sealed.pending_generation_count = sealed.pending_generation_count.saturating_sub(1);
        }
        drop(sealed);
        if let Some(generation) = resident {
            Self::release_sealed_generation(generation);
            true
        } else {
            removed_descriptor || removed_pending
        }
    }

    /// Preload every durable layer selected for this snapshot under the
    /// request's own cancellable memory charge. Each successful load moves
    /// that charge to the resident graph owner; every other exit returns it.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn preload_snapshot_compatible_hnsw_layers_for_request<E>(
        &self,
        snapshot: SnapshotId,
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let snapshot_tx = TxId::from_snapshot(snapshot);
        for _ in 0..4 {
            before_checkpoint()?;
            let load = {
                let sealed = self.sealed_hnsw.read();
                if sealed.quarantine.is_some() {
                    return Ok(());
                }
                Self::selected_chain_location(&sealed, snapshot_tx).and_then(|location| {
                    match location {
                        SealedChainLocation::Current => {
                            let base_identity = Self::layer_identity(
                                sealed.base.as_ref(),
                                sealed.dormant_base.as_ref(),
                            )?;
                            if !sealed
                                .base
                                .as_ref()
                                .is_some_and(|base| base.identity == base_identity)
                                && sealed.dormant_base.is_some()
                            {
                                return Some((location, true));
                            }
                            let change_identity = Self::layer_identity(
                                sealed.change.as_ref(),
                                sealed.dormant_change.as_ref(),
                            );
                            if change_identity.is_some_and(|identity| {
                                !sealed
                                    .change
                                    .as_ref()
                                    .is_some_and(|change| change.identity == identity)
                            }) && sealed.dormant_change.is_some()
                            {
                                return Some((location, false));
                            }
                            None
                        }
                        SealedChainLocation::Retired { base_generation_id } => {
                            let chain = sealed.retired.iter().find(|chain| {
                                Self::retired_chain_base_identity(chain).is_some_and(|identity| {
                                    identity.generation_id == base_generation_id
                                })
                            })?;
                            let base_identity = Self::retired_chain_base_identity(chain)?;
                            if !chain
                                .base
                                .as_ref()
                                .is_some_and(|base| base.identity == base_identity)
                                && chain.dormant_base.is_some()
                            {
                                return Some((location, true));
                            }
                            let change_identity = Self::retired_chain_change_identity(chain);
                            if change_identity.is_some_and(|identity| {
                                !chain
                                    .change
                                    .as_ref()
                                    .is_some_and(|change| change.identity == identity)
                            }) && chain.dormant_change.is_some()
                            {
                                return Some((location, false));
                            }
                            None
                        }
                    }
                })
            };
            let Some((location, is_base)) = load else {
                return Ok(());
            };
            match location {
                SealedChainLocation::Current => {
                    self.preload_dormant_sealed_generation_with(
                        is_base,
                        true,
                        before_checkpoint,
                        before_retain,
                        release_retained,
                    )?;
                }
                SealedChainLocation::Retired { base_generation_id } => {
                    self.preload_retired_sealed_generation_with(
                        base_generation_id,
                        is_base,
                        true,
                        before_checkpoint,
                        before_retain,
                        release_retained,
                    )?;
                }
            }
        }
        Err(E::from(Error::Other(
            "snapshot-compatible vector layers changed repeatedly while loading".to_string(),
        )))
    }

    /// Visit the complete graph layers compatible with `snapshot`. Ordinary
    /// execution may load a dormant layer here. A bounded caller first uses
    /// the request-owned preloader above, then visits without further I/O.
    /// The callback sees only immutable sealed graphs plus the fresh tail.
    #[doc(hidden)]
    pub(crate) fn with_snapshot_compatible_hnsw_layers<E, R>(
        &self,
        snapshot: SnapshotId,
        allow_lazy_load: bool,
        mut visit: impl FnMut(&HnswIndex) -> std::result::Result<R, E>,
    ) -> SnapshotCompatibleHnswLayersResult<R, E>
    where
        E: From<Error>,
    {
        let snapshot_tx = TxId::from_snapshot(snapshot);
        if allow_lazy_load {
            // A chain has at most two durable layers. A few bounded retries
            // cover a publication racing either load without permitting an
            // unbounded read-side repair loop.
            for _ in 0..4 {
                let load = {
                    let sealed = self.sealed_hnsw.read();
                    if sealed.quarantine.is_some() {
                        return Ok((VectorGraphLayerAvailability::Unavailable, Vec::new()));
                    }
                    Self::selected_chain_location(&sealed, snapshot_tx).and_then(|location| {
                        match location {
                            SealedChainLocation::Current => {
                                let base_identity = Self::layer_identity(
                                    sealed.base.as_ref(),
                                    sealed.dormant_base.as_ref(),
                                )?;
                                if !sealed
                                    .base
                                    .as_ref()
                                    .is_some_and(|base| base.identity == base_identity)
                                    && sealed.dormant_base.is_some()
                                {
                                    return Some((location, true));
                                }
                                let change_identity = Self::layer_identity(
                                    sealed.change.as_ref(),
                                    sealed.dormant_change.as_ref(),
                                );
                                if change_identity.is_some_and(|identity| {
                                    !sealed
                                        .change
                                        .as_ref()
                                        .is_some_and(|change| change.identity == identity)
                                }) && sealed.dormant_change.is_some()
                                {
                                    return Some((location, false));
                                }
                                None
                            }
                            SealedChainLocation::Retired { base_generation_id } => {
                                let chain = sealed.retired.iter().find(|chain| {
                                    Self::retired_chain_base_identity(chain).is_some_and(
                                        |identity| identity.generation_id == base_generation_id,
                                    )
                                })?;
                                let base_identity = Self::retired_chain_base_identity(chain)?;
                                if !chain
                                    .base
                                    .as_ref()
                                    .is_some_and(|base| base.identity == base_identity)
                                    && chain.dormant_base.is_some()
                                {
                                    return Some((location, true));
                                }
                                let change_identity = Self::retired_chain_change_identity(chain);
                                if change_identity.is_some_and(|identity| {
                                    !chain
                                        .change
                                        .as_ref()
                                        .is_some_and(|change| change.identity == identity)
                                }) && chain.dormant_change.is_some()
                                {
                                    return Some((location, false));
                                }
                                None
                            }
                        }
                    })
                };
                let Some((location, is_base)) = load else {
                    break;
                };
                match location {
                    SealedChainLocation::Current => {
                        self.preload_dormant_sealed_generation(is_base)
                            .map_err(E::from)?;
                    }
                    SealedChainLocation::Retired { base_generation_id } => {
                        self.preload_retired_sealed_generation(base_generation_id, is_base)
                            .map_err(E::from)?;
                    }
                }
            }
        }
        let publication = self.generation_publication.lock();
        let sealed = self.sealed_hnsw.read();
        if sealed.quarantine.is_some() {
            return Ok((VectorGraphLayerAvailability::Unavailable, Vec::new()));
        }
        let location = Self::selected_chain_location(&sealed, snapshot_tx);
        let tail = self.hnsw.get().map(|tail| tail.read());
        let Some(location) = location else {
            let tail_only = sealed.base.is_none()
                && sealed.dormant_base.is_none()
                && sealed.change.is_none()
                && sealed.dormant_change.is_none()
                && self.serving_policy().is_some()
                && !self.maintenance_sample_active.load(Ordering::SeqCst)
                && self.max_tx() <= snapshot_tx;
            let Some(tail_graph) = tail
                .as_ref()
                .and_then(|tail| tail.as_ref())
                .filter(|tail| tail_only && !tail.is_empty())
            else {
                return Ok((VectorGraphLayerAvailability::Unavailable, Vec::new()));
            };
            drop(publication);
            #[cfg(any(test, feature = "test-seams"))]
            if let Some(slot) = self.graph_callback_pause.lock().clone() {
                slot.pause_after(0);
            }
            return Ok((
                VectorGraphLayerAvailability::Ready,
                vec![(None, visit(tail_graph)?)],
            ));
        };
        let (base, change, include_tail) = match location {
            SealedChainLocation::Current => {
                let base_identity =
                    Self::layer_identity(sealed.base.as_ref(), sealed.dormant_base.as_ref())
                        .expect("a selected current vector chain has a base identity");
                let Some(base) = sealed
                    .base
                    .as_ref()
                    .filter(|base| base.identity == base_identity)
                else {
                    let availability = if sealed.dormant_base.is_some() {
                        VectorGraphLayerAvailability::Dormant
                    } else {
                        VectorGraphLayerAvailability::Unavailable
                    };
                    return Ok((availability, Vec::new()));
                };
                let change_identity =
                    Self::layer_identity(sealed.change.as_ref(), sealed.dormant_change.as_ref());
                let change = if let Some(identity) = change_identity {
                    let Some(change) = sealed
                        .change
                        .as_ref()
                        .filter(|change| change.identity == identity)
                    else {
                        let availability = if sealed.dormant_change.is_some() {
                            VectorGraphLayerAvailability::Dormant
                        } else {
                            VectorGraphLayerAvailability::Unavailable
                        };
                        return Ok((availability, Vec::new()));
                    };
                    Some(change)
                } else {
                    None
                };
                // A later commit cannot suppress an older reader's eligible
                // tail. Search it exactly when the sealed frontier predates
                // the snapshot; per-row visibility drops any newer points.
                let active_frontier = change_identity.unwrap_or(base_identity);
                (base, change, active_frontier.covered_tx < snapshot_tx)
            }
            SealedChainLocation::Retired { base_generation_id } => {
                let chain = sealed
                    .retired
                    .iter()
                    .find(|chain| {
                        Self::retired_chain_base_identity(chain)
                            .is_some_and(|identity| identity.generation_id == base_generation_id)
                    })
                    .expect("a selected retired vector chain remains registered");
                let base_identity = Self::retired_chain_base_identity(chain)
                    .expect("a selected retired vector chain has a base identity");
                let Some(base) = chain
                    .base
                    .as_ref()
                    .filter(|base| base.identity == base_identity)
                else {
                    let availability = if chain.dormant_base.is_some() {
                        VectorGraphLayerAvailability::Dormant
                    } else {
                        VectorGraphLayerAvailability::Unavailable
                    };
                    return Ok((availability, Vec::new()));
                };
                let change_identity = Self::retired_chain_change_identity(chain);
                let change = if let Some(identity) = change_identity {
                    let Some(change) = chain
                        .change
                        .as_ref()
                        .filter(|change| change.identity == identity)
                    else {
                        let availability = if chain.dormant_change.is_some() {
                            VectorGraphLayerAvailability::Dormant
                        } else {
                            VectorGraphLayerAvailability::Unavailable
                        };
                        return Ok((availability, Vec::new()));
                    };
                    Some(change)
                } else {
                    None
                };
                (base, change, false)
            }
        };
        // The guards above pin the selected graph objects. Publication may
        // wait on those guards, but the publication mutex itself must not be
        // held while arbitrary query callbacks execute.
        drop(publication);
        let mut results = Vec::with_capacity(
            1usize
                .saturating_add(usize::from(change.is_some()))
                .saturating_add(usize::from(
                    include_tail && tail.as_ref().is_some_and(|tail| tail.is_some()),
                )),
        );
        #[cfg(any(test, feature = "test-seams"))]
        if let Some(slot) = self.graph_callback_pause.lock().clone() {
            slot.pause_after(0);
        }
        results.push((Some(base.identity), visit(&base.graph)?));
        if let Some(change) = change {
            results.push((Some(change.identity), visit(&change.graph)?));
        }
        if include_tail && let Some(tail) = tail.as_ref().and_then(|tail| tail.as_ref()) {
            results.push((None, visit(tail)?));
        }
        Ok((VectorGraphLayerAvailability::Ready, results))
    }

    pub fn raw_hnsw_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Option<Result<Vec<(RowId, f32)>>> {
        let searched =
            self.with_snapshot_compatible_hnsw_layers(SnapshotId(u64::MAX), false, |hnsw| {
                hnsw.search(index, query, k)
            });
        match searched {
            Err(error) => Some(Err(error)),
            Ok((VectorGraphLayerAvailability::Ready, layers)) => {
                let mut best = HashMap::<RowId, f32>::new();
                for (_, rows) in layers {
                    for (row_id, distance) in rows {
                        best.entry(row_id)
                            .and_modify(|current| *current = current.max(distance))
                            .or_insert(distance);
                    }
                }
                let mut rows = best.into_iter().collect::<Vec<_>>();
                rows.sort_by(|(left_row, left_distance), (right_row, right_distance)| {
                    right_distance
                        .total_cmp(left_distance)
                        .then_with(|| left_row.0.cmp(&right_row.0))
                });
                rows.truncate(k);
                Some(Ok(rows))
            }
            Ok((_, _)) => None,
        }
    }

    pub fn raw_hnsw_entry_count_for_row(&self, row_id: RowId) -> Option<usize> {
        self.hnsw.get().and_then(|lock| {
            lock.read()
                .as_ref()
                .map(|hnsw| hnsw.raw_entry_count_for_row(row_id))
        })
    }

    pub fn raw_hnsw_topology_digest_for_test(&self) -> Option<u64> {
        self.hnsw.get().and_then(|lock| {
            lock.read()
                .as_ref()
                .map(|hnsw| hnsw.graph_topology_digest_for_test())
        })
    }

    pub fn raw_hnsw_build_serial_for_test(&self) -> Option<u64> {
        let sealed = self.sealed_hnsw.read();
        let durable_generation = sealed
            .base
            .as_ref()
            .map(|generation| generation.identity.generation_id)
            .or_else(|| {
                sealed
                    .dormant_base
                    .as_ref()
                    .map(|generation| generation.descriptor.identity.generation_id)
            });
        drop(sealed);
        durable_generation.or_else(|| {
            let generation_id = self.inspection_generation_id.load(Ordering::SeqCst);
            self.hnsw.get().and_then(|lock| {
                lock.read().as_ref().map(|hnsw| {
                    if generation_id == 0 {
                        hnsw.build_serial_for_test()
                    } else {
                        generation_id
                    }
                })
            })
        })
    }

    pub fn set_hnsw(&self, hnsw: Option<HnswIndex>, bytes: usize) {
        self.fresh_tail_replayed_through_lsn
            .store(0, Ordering::SeqCst);
        self.fresh_tail_replay_frontier_active
            .store(false, Ordering::SeqCst);
        self.fresh_tail_mutated_through_lsn
            .store(0, Ordering::SeqCst);
        self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
        if hnsw.is_some() {
            self.hnsw_bytes.store(bytes, Ordering::SeqCst);
        } else {
            self.hnsw_bytes.store(0, Ordering::SeqCst);
            self.hnsw_accountant.write().take();
        }
        let lock = self.hnsw.get_or_init(|| RwLock::new(None));
        *lock.write() = hnsw;
    }

    fn sampled_maintenance_frontier(&self) -> Option<(TxId, Lsn)> {
        self.maintenance_sample_active
            .load(Ordering::SeqCst)
            .then(|| {
                (
                    TxId(self.maintenance_sample_tx.load(Ordering::SeqCst)),
                    Lsn(self.maintenance_sample_lsn.load(Ordering::SeqCst)),
                )
            })
    }

    /// Freeze one in-memory build frontier and move subsequent commits onto a
    /// new fresh tail. The existing serving graph becomes the current sealed
    /// base/change chain, so reads keep using it while construction runs.
    fn begin_sampled_hnsw_maintenance(
        &self,
        accountant: Arc<dyn MemoryBudget>,
        policy: ResolvedVectorPolicy,
    ) -> Result<Option<(TxId, Lsn)>> {
        if let Some(frontier) = self.sampled_maintenance_frontier() {
            return Ok(Some(frontier));
        }
        if self.fresh_tail_preparations.load(Ordering::SeqCst) != 0 {
            return Ok(None);
        }

        let sample_tx = self.max_tx();
        let sample_lsn = self
            .raw_directory
            .read()
            .iter()
            .filter(|entry| entry.created_tx <= sample_tx)
            .map(|entry| entry.lsn)
            .max()
            .unwrap_or_default();
        let empty_bytes = HnswIndex::estimated_resident_bytes_with_m(
            0,
            self.dimension,
            self.quantization,
            policy.hnsw_m,
        );
        accountant.try_allocate_for(
            empty_bytes,
            "vector_index",
            "begin_sampled_hnsw_maintenance",
            "Raise MEMORY_LIMIT to retain the fresh vector tail during maintenance.",
        )?;
        let mut empty_charge = RetainedMemoryCharge {
            bytes: empty_bytes,
            accountant: accountant.clone(),
        };
        let empty_tail = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            HnswIndex::from_vector_entries_with_policy(
                &[],
                self.dimension,
                self.quantization,
                policy,
            )
        }))
        .map_err(|_| Error::Other("preparing an empty maintenance tail panicked".to_string()))?;

        let _publication = self.generation_publication.lock();
        if let Some(frontier) = self.sampled_maintenance_frontier() {
            return Ok(Some(frontier));
        }
        if self.fresh_tail_preparations.load(Ordering::SeqCst) != 0 {
            return Ok(None);
        }

        let mut sealed = self.sealed_hnsw.write();
        let tail_lock = self.hnsw.get_or_init(|| RwLock::new(None));
        let mut tail = tail_lock.write();
        let serving = self.serving_policy();
        let tail_has_entries = tail.as_ref().is_some_and(|graph| !graph.is_empty());
        let base_known = sealed.base.is_some() || sealed.dormant_base.is_some();
        if tail_has_entries
            && serving.is_some()
            && base_known
            && (sealed.change.is_some() || sealed.dormant_change.is_some())
        {
            return Err(Error::Other(
                "sampled maintenance found an occupied sealed change layer".to_string(),
            ));
        }

        let old_graph = tail.take();
        let old_bytes = self.hnsw_bytes.swap(0, Ordering::SeqCst);
        let old_accountant = self.hnsw_accountant.write().take();
        let moves_serving_graph =
            old_graph.as_ref().is_some_and(|graph| !graph.is_empty()) && serving.is_some();
        let sample_origin = match (base_known, moves_serving_graph, old_graph.is_some()) {
            (false, true, _) => MAINTENANCE_SAMPLE_MOVED_BASE,
            (true, true, _) => MAINTENANCE_SAMPLE_MOVED_CHANGE,
            (true, false, true) => MAINTENANCE_SAMPLE_REPLACED_EMPTY_TAIL,
            (true, false, false) => MAINTENANCE_SAMPLE_ADDED_TO_BASE,
            (false, false, _) => MAINTENANCE_SAMPLE_NO_PRIOR_TAIL,
        };
        self.maintenance_sample_prior_replay_frontier_active.store(
            self.fresh_tail_replay_frontier_active
                .load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.maintenance_sample_prior_replayed_through_lsn.store(
            self.fresh_tail_replayed_through_lsn.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.maintenance_sample_prior_mutated_through_lsn.store(
            self.fresh_tail_mutated_through_lsn.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        let mut discarded_charge = None;
        if let Some(graph) = old_graph {
            if !graph.is_empty()
                && let Some(serving) = serving
            {
                let (hnsw_m, hnsw_ef_construction, _) = graph.policy_values();
                let identity = if let Some(base) =
                    Self::layer_identity(sealed.base.as_ref(), sealed.dormant_base.as_ref())
                {
                    VectorGraphGeneration {
                        generation_id: base.generation_id,
                        covered_tx: sample_tx,
                        covered_lsn: sample_lsn,
                        durable_bytes: 0,
                        policy_revision: serving.policy_revision,
                        hnsw_m: hnsw_m as u32,
                        hnsw_ef_construction: hnsw_ef_construction as u32,
                    }
                } else {
                    VectorGraphGeneration {
                        generation_id: self.inspection_generation_id.load(Ordering::SeqCst).max(1),
                        covered_tx: TxId(
                            self.inspection_base_tx
                                .load(Ordering::SeqCst)
                                .min(sample_tx.0),
                        ),
                        covered_lsn: sample_lsn,
                        durable_bytes: 0,
                        policy_revision: serving.policy_revision,
                        hnsw_m: hnsw_m as u32,
                        hnsw_ef_construction: hnsw_ef_construction as u32,
                    }
                };
                let generation = SealedHnswGeneration {
                    identity,
                    graph,
                    bytes: old_bytes,
                    accountant: old_accountant,
                };
                if base_known {
                    sealed.change = Some(generation);
                } else {
                    sealed.base = Some(generation);
                }
            } else {
                discarded_charge = old_accountant.map(|accountant| RetainedMemoryCharge {
                    bytes: old_bytes,
                    accountant,
                });
            }
        }

        *tail = Some(empty_tail);
        self.hnsw_bytes.store(empty_bytes, Ordering::SeqCst);
        *self.hnsw_accountant.write() = Some(accountant);
        empty_charge.bytes = 0;
        self.fresh_tail_replayed_through_lsn
            .store(sample_lsn.0, Ordering::SeqCst);
        self.fresh_tail_replay_frontier_active
            .store(true, Ordering::SeqCst);
        self.fresh_tail_mutated_through_lsn
            .store(0, Ordering::SeqCst);
        self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
        self.maintenance_sample_tx
            .store(sample_tx.0, Ordering::SeqCst);
        self.maintenance_sample_lsn
            .store(sample_lsn.0, Ordering::SeqCst);
        self.maintenance_sample_origin
            .store(sample_origin, Ordering::SeqCst);
        self.maintenance_sample_active.store(true, Ordering::SeqCst);
        drop(sealed);
        drop(tail);
        drop(discarded_charge);
        Ok(Some((sample_tx, sample_lsn)))
    }

    /// Roll back a failed sample split only while no commit has reached its
    /// fresh tail. Once a write crosses, the fixed frontier stays installed
    /// for a later retry so aborting maintenance cannot discard that write.
    fn abort_sampled_hnsw_maintenance_if_uncontended(&self) -> bool {
        if !self.maintenance_sample_active.load(Ordering::SeqCst) {
            return false;
        }
        let _publication = self.generation_publication.lock();
        if !self.maintenance_sample_active.load(Ordering::SeqCst)
            || self.fresh_tail_preparations.load(Ordering::SeqCst) != 0
        {
            return false;
        }
        let mut sealed = self.sealed_hnsw.write();
        let tail_lock = self.hnsw.get_or_init(|| RwLock::new(None));
        let mut tail = tail_lock.write();
        if tail.as_ref().is_some_and(|graph| !graph.is_empty()) {
            return false;
        }

        let origin = self.maintenance_sample_origin.load(Ordering::SeqCst);
        if origin == MAINTENANCE_SAMPLE_REPLACED_EMPTY_TAIL {
            self.fresh_tail_replayed_through_lsn.store(
                self.maintenance_sample_prior_replayed_through_lsn
                    .load(Ordering::SeqCst),
                Ordering::SeqCst,
            );
            self.fresh_tail_replay_frontier_active.store(
                self.maintenance_sample_prior_replay_frontier_active
                    .load(Ordering::SeqCst),
                Ordering::SeqCst,
            );
            self.fresh_tail_mutated_through_lsn.store(
                self.maintenance_sample_prior_mutated_through_lsn
                    .load(Ordering::SeqCst),
                Ordering::SeqCst,
            );
            self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
            self.maintenance_sample_active
                .store(false, Ordering::SeqCst);
            self.maintenance_sample_tx.store(0, Ordering::SeqCst);
            self.maintenance_sample_lsn.store(0, Ordering::SeqCst);
            self.maintenance_sample_origin.store(0, Ordering::SeqCst);
            self.maintenance_sample_prior_replay_frontier_active
                .store(false, Ordering::SeqCst);
            self.maintenance_sample_prior_replayed_through_lsn
                .store(0, Ordering::SeqCst);
            self.maintenance_sample_prior_mutated_through_lsn
                .store(0, Ordering::SeqCst);
            return true;
        }

        let restored = match origin {
            MAINTENANCE_SAMPLE_MOVED_BASE => sealed.base.take(),
            MAINTENANCE_SAMPLE_MOVED_CHANGE => sealed.change.take(),
            MAINTENANCE_SAMPLE_NO_PRIOR_TAIL | MAINTENANCE_SAMPLE_ADDED_TO_BASE => None,
            _ => return false,
        };
        if matches!(
            origin,
            MAINTENANCE_SAMPLE_MOVED_BASE | MAINTENANCE_SAMPLE_MOVED_CHANGE
        ) && restored.is_none()
        {
            return false;
        }

        let discarded_tail = tail.take();
        let discarded_bytes = self.hnsw_bytes.swap(0, Ordering::SeqCst);
        let discarded_accountant = self.hnsw_accountant.write().take();
        let restored_old_tail = restored.is_some();
        if let Some(restored) = restored {
            *tail = Some(restored.graph);
            self.hnsw_bytes.store(restored.bytes, Ordering::SeqCst);
            *self.hnsw_accountant.write() = restored.accountant;
        }
        self.fresh_tail_replayed_through_lsn.store(
            if restored_old_tail {
                self.maintenance_sample_prior_replayed_through_lsn
                    .load(Ordering::SeqCst)
            } else {
                0
            },
            Ordering::SeqCst,
        );
        self.fresh_tail_replay_frontier_active.store(
            restored_old_tail
                && self
                    .maintenance_sample_prior_replay_frontier_active
                    .load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.fresh_tail_mutated_through_lsn.store(
            if restored_old_tail {
                self.maintenance_sample_prior_mutated_through_lsn
                    .load(Ordering::SeqCst)
            } else {
                0
            },
            Ordering::SeqCst,
        );
        self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
        self.maintenance_sample_active
            .store(false, Ordering::SeqCst);
        self.maintenance_sample_tx.store(0, Ordering::SeqCst);
        self.maintenance_sample_lsn.store(0, Ordering::SeqCst);
        self.maintenance_sample_origin.store(0, Ordering::SeqCst);
        self.maintenance_sample_prior_replay_frontier_active
            .store(false, Ordering::SeqCst);
        self.maintenance_sample_prior_replayed_through_lsn
            .store(0, Ordering::SeqCst);
        self.maintenance_sample_prior_mutated_through_lsn
            .store(0, Ordering::SeqCst);
        drop(sealed);
        drop(tail);
        drop(discarded_tail);
        if discarded_bytes != 0
            && let Some(accountant) = discarded_accountant
        {
            accountant.release(discarded_bytes);
        }
        true
    }

    /// Publish a graph built from the fixed in-memory maintenance frontier.
    /// The current base/change pair moves to the retired snapshot chain while
    /// commits that crossed during construction remain in the fresh tail.
    fn publish_sampled_hnsw(
        &self,
        hnsw: HnswIndex,
        bytes: usize,
        accountant: Arc<dyn MemoryBudget>,
        policy: ResolvedVectorPolicy,
        sample_tx: TxId,
        sample_lsn: Lsn,
    ) {
        let _publication = self.generation_publication.lock();
        let mut sealed = self.sealed_hnsw.write();
        let mut greatest_generation = self.inspection_generation_id.load(Ordering::SeqCst);
        for identity in [
            Self::layer_identity(sealed.base.as_ref(), sealed.dormant_base.as_ref()),
            Self::layer_identity(sealed.change.as_ref(), sealed.dormant_change.as_ref()),
            sealed
                .pending_base
                .as_ref()
                .map(|generation| generation.descriptor.identity),
        ]
        .into_iter()
        .flatten()
        {
            greatest_generation = greatest_generation.max(identity.generation_id);
        }
        for chain in &sealed.retired {
            for identity in [
                Self::retired_chain_base_identity(chain),
                Self::retired_chain_change_identity(chain),
            ]
            .into_iter()
            .flatten()
            {
                greatest_generation = greatest_generation.max(identity.generation_id);
            }
        }
        let identity = VectorGraphGeneration {
            generation_id: greatest_generation.saturating_add(1).max(1),
            covered_tx: sample_tx,
            covered_lsn: sample_lsn,
            durable_bytes: 0,
            policy_revision: policy.policy_revision,
            hnsw_m: policy.hnsw_m as u32,
            hnsw_ef_construction: policy.hnsw_ef_construction as u32,
        };
        let tail_lock = self.hnsw.get_or_init(|| RwLock::new(None));
        let mut tail = tail_lock.write();
        let first_build_without_overlap = sealed.base.is_none()
            && sealed.change.is_none()
            && sealed.dormant_base.is_none()
            && sealed.dormant_change.is_none()
            && self.fresh_tail_preparations.load(Ordering::SeqCst) == 0
            && tail.as_ref().is_some_and(HnswIndex::is_empty);
        if first_build_without_overlap {
            let old_tail = tail.replace(hnsw);
            let old_bytes = self.hnsw_bytes.swap(bytes, Ordering::SeqCst);
            let old_accountant = self.hnsw_accountant.write().replace(accountant);
            self.fresh_tail_replayed_through_lsn
                .store(0, Ordering::SeqCst);
            self.fresh_tail_replay_frontier_active
                .store(false, Ordering::SeqCst);
            self.fresh_tail_mutated_through_lsn
                .store(0, Ordering::SeqCst);
            self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
            self.maintenance_sample_active
                .store(false, Ordering::SeqCst);
            self.maintenance_sample_tx.store(0, Ordering::SeqCst);
            self.maintenance_sample_lsn.store(0, Ordering::SeqCst);
            self.maintenance_sample_origin.store(0, Ordering::SeqCst);
            self.maintenance_sample_prior_replay_frontier_active
                .store(false, Ordering::SeqCst);
            self.maintenance_sample_prior_replayed_through_lsn
                .store(0, Ordering::SeqCst);
            self.maintenance_sample_prior_mutated_through_lsn
                .store(0, Ordering::SeqCst);
            drop(sealed);
            drop(tail);
            self.set_serving_policy_from_generation(identity);
            self.clear_route_quarantine();
            drop(old_tail);
            if old_bytes != 0
                && let Some(old_accountant) = old_accountant
            {
                old_accountant.release(old_bytes);
            }
            return;
        }
        Self::retire_current_chain(&mut sealed);
        sealed.base = Some(SealedHnswGeneration {
            identity,
            graph: hnsw,
            bytes,
            accountant: Some(accountant),
        });
        sealed.change = None;
        sealed.dormant_base = None;
        sealed.dormant_change = None;
        drop(sealed);
        drop(tail);
        self.fresh_tail_replayed_through_lsn
            .store(sample_lsn.0, Ordering::SeqCst);
        self.fresh_tail_replay_frontier_active
            .store(true, Ordering::SeqCst);
        self.maintenance_sample_active
            .store(false, Ordering::SeqCst);
        self.maintenance_sample_tx.store(0, Ordering::SeqCst);
        self.maintenance_sample_lsn.store(0, Ordering::SeqCst);
        self.maintenance_sample_origin.store(0, Ordering::SeqCst);
        self.maintenance_sample_prior_replay_frontier_active
            .store(false, Ordering::SeqCst);
        self.maintenance_sample_prior_replayed_through_lsn
            .store(0, Ordering::SeqCst);
        self.maintenance_sample_prior_mutated_through_lsn
            .store(0, Ordering::SeqCst);
        self.set_serving_policy_from_generation(identity);
        self.clear_route_quarantine();
    }

    pub fn set_hnsw_bytes(&self, bytes: usize) {
        self.hnsw_bytes.store(bytes, Ordering::SeqCst);
        if bytes == 0 {
            self.hnsw_accountant.write().take();
        }
    }

    pub(crate) fn set_hnsw_bytes_with_accountant(
        &self,
        bytes: usize,
        accountant: Arc<dyn MemoryBudget>,
    ) {
        self.hnsw_bytes.store(bytes, Ordering::SeqCst);
        *self.hnsw_accountant.write() = Some(accountant);
    }

    pub fn hnsw(&self) -> &OnceLock<RwLock<Option<HnswIndex>>> {
        &self.hnsw
    }

    #[doc(hidden)]
    pub(crate) fn with_fresh_tail<R>(&self, f: impl FnOnce(&HnswIndex) -> R) -> Option<R> {
        self.hnsw.get().and_then(|tail| tail.read().as_ref().map(f))
    }

    /// Turn a graph built in an unpublished purge image into that image's
    /// sealed base generation. Every fallible step completes before the graph
    /// moves: encoding, charge reconciliation, and construction of the empty
    /// mutable tail that later commits will extend.
    fn prepare_fresh_tail_as_base_generation(
        &self,
        identity: VectorGraphGeneration,
        policy: ResolvedVectorPolicy,
    ) -> Result<(Vec<u8>, usize)> {
        if identity.generation_id == 0 {
            return Err(Error::Other(
                "a prepared vector base generation requires a nonzero identity".to_string(),
            ));
        }
        let _publication = self.generation_publication.lock();
        let tail = self.hnsw.get().ok_or_else(|| {
            Error::Other("prepared vector generation has no built graph".to_string())
        })?;
        let (encoded, resident_bytes) = {
            let tail = tail.read();
            let graph = tail.as_ref().ok_or_else(|| {
                Error::Other("prepared vector generation has no built graph".to_string())
            })?;
            (
                graph.encode_durable_generation()?,
                graph.estimated_resident_bytes(),
            )
        };
        let identity = VectorGraphGeneration {
            durable_bytes: encoded.len() as u64,
            ..identity
        };
        let charged_bytes = self.hnsw_bytes.load(Ordering::SeqCst);
        if charged_bytes != resident_bytes {
            return Err(Error::Other(format!(
                "prepared vector generation charge mismatch: graph requires {resident_bytes} bytes but owns {charged_bytes}"
            )));
        }
        let accountant = self.hnsw_accountant.read().clone().ok_or_else(|| {
            Error::Other("prepared vector generation has no memory owner".to_string())
        })?;
        {
            let sealed = self.sealed_hnsw.read();
            if sealed.base.is_some()
                || sealed.change.is_some()
                || sealed.dormant_base.is_some()
                || sealed.dormant_change.is_some()
                || sealed.pending_base.is_some()
                || !sealed.retired.is_empty()
            {
                return Err(Error::Other(
                    "prepared purge state already owns a sealed vector generation".to_string(),
                ));
            }
        }
        let empty_bytes = HnswIndex::estimated_resident_bytes_with_m(
            0,
            self.dimension,
            self.quantization,
            policy.hnsw_m,
        );
        accountant.try_allocate_for(
            empty_bytes,
            "vector_index",
            "prepare_vector_generation",
            "Raise MEMORY_LIMIT to retain the fresh vector tail.",
        )?;
        let mut empty_charge = RetainedMemoryCharge {
            bytes: empty_bytes,
            accountant: accountant.clone(),
        };
        let empty_tail = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            HnswIndex::from_vector_entries_with_policy(
                &[],
                self.dimension,
                self.quantization,
                policy,
            )
        }))
        .map_err(|_| {
            Error::Other("preparing an empty post-purge vector tail panicked".to_string())
        })?;

        let graph = {
            let mut tail = tail.write();
            let graph = tail
                .take()
                .expect("prepared vector graph was validated under the publication lock");
            *tail = Some(empty_tail);
            graph
        };
        self.fresh_tail_replayed_through_lsn
            .store(identity.covered_lsn.0, Ordering::SeqCst);
        self.fresh_tail_replay_frontier_active
            .store(true, Ordering::SeqCst);
        self.fresh_tail_mutated_through_lsn
            .store(0, Ordering::SeqCst);
        self.fresh_tail_revision.fetch_add(1, Ordering::SeqCst);
        self.hnsw_bytes.store(empty_bytes, Ordering::SeqCst);
        empty_charge.bytes = 0;
        *self.hnsw_accountant.write() = Some(accountant.clone());
        self.sealed_hnsw.write().base = Some(SealedHnswGeneration {
            identity,
            graph,
            bytes: resident_bytes,
            accountant: Some(accountant),
        });
        Ok((encoded, resident_bytes))
    }

    #[doc(hidden)]
    pub(crate) fn initialize_empty_fresh_tail(
        &self,
        accountant: Arc<dyn MemoryBudget>,
        policy: ResolvedVectorPolicy,
    ) -> Result<()> {
        let _publication = self.generation_publication.lock();
        if self.hnsw.get().is_some_and(|tail| tail.read().is_some()) {
            return Ok(());
        }
        let bytes = HnswIndex::estimated_resident_bytes_with_m(
            0,
            self.dimension,
            self.quantization,
            policy.hnsw_m,
        );
        accountant.try_allocate_for(
            bytes,
            "vector_index",
            "initialize_vector_tail",
            "Raise MEMORY_LIMIT to retain the fresh vector tail.",
        )?;
        let mut charge = RetainedMemoryCharge {
            bytes,
            accountant: accountant.clone(),
        };
        let graph = HnswIndex::from_vector_entries_with_policy(
            &[],
            self.dimension,
            self.quantization,
            policy,
        );
        self.set_hnsw(Some(graph), bytes);
        self.set_hnsw_bytes_with_accountant(bytes, accountant);
        charge.bytes = 0;
        Ok(())
    }

    pub fn storage_bytes_per_entry(&self) -> Vec<usize> {
        self.vectors
            .read()
            .entries
            .iter()
            .map(StoredVectorEntry::estimated_bytes)
            .collect()
    }
}

impl Drop for IndexState {
    fn drop(&mut self) {
        let raw_bytes = self.raw_resident_bytes.swap(0, Ordering::SeqCst);
        if let Some(accountant) = self.raw_accountant.write().take()
            && raw_bytes != 0
        {
            accountant.release(raw_bytes);
        }
        self.drop_hnsw_without_accounting();
    }
}

/// Complete in-memory declaration for one vector column. Partition columns
/// remain in declaration order; the cap is `None` only for an unpartitioned
/// column and is the already-resolved effective cap otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexLayout {
    pub dimension: usize,
    pub quantization: VectorQuantization,
    pub partition_key_columns: Vec<String>,
    pub max_partitions: Option<u32>,
    pub search_mode: VectorSearchMode,
    pub auto_index_at: Option<u32>,
    pub hnsw_m: Option<u32>,
    pub hnsw_ef_construction: Option<u32>,
    pub hnsw_ef_search: Option<u32>,
    pub policy_revision: u64,
    pub consolidation_change_percent: Option<u32>,
    pub consolidation_tombstone_percent: Option<u32>,
    pub consolidation_disabled: bool,
}

/// One authoritative policy resolution used by routing, graph construction,
/// search admission, inspection, and publication validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedVectorPolicy {
    pub auto_index_at: usize,
    pub hnsw_m: usize,
    pub hnsw_ef_construction: usize,
    pub hnsw_ef_search: usize,
    pub policy_revision: u64,
    pub auto_index_at_source: &'static str,
    pub ef_search_source: &'static str,
}

/// One shared answer for maintenance wake selection and passive lifecycle
/// inspection. Persistent journal-only work is added by the engine, but the
/// represented partition state is classified here exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorMaintenanceNeed {
    InitialBuild,
    Quarantine(VectorRouteQuarantineReason),
    PolicyReplacement,
    NewChanges,
    Tombstones,
}

impl VectorMaintenanceNeed {
    pub fn reason(self) -> &'static str {
        match self {
            Self::InitialBuild => "initial_build",
            Self::Quarantine(reason) => reason.as_str(),
            Self::PolicyReplacement | Self::NewChanges => "new_changes",
            Self::Tombstones => "tombstones",
        }
    }

    fn build_reason(self) -> &'static str {
        match self {
            Self::PolicyReplacement => "policy_replacement",
            _ => self.reason(),
        }
    }
}

impl VectorIndexLayout {
    pub fn new(
        dimension: usize,
        quantization: VectorQuantization,
        partition_key_columns: Vec<String>,
        max_partitions: Option<u32>,
        search_mode: VectorSearchMode,
    ) -> Self {
        Self {
            dimension,
            quantization,
            partition_key_columns,
            max_partitions,
            search_mode,
            auto_index_at: None,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
            policy_revision: contextdb_core::DEFAULT_VECTOR_POLICY_REVISION,
            consolidation_change_percent: None,
            consolidation_tombstone_percent: None,
            consolidation_disabled: false,
        }
    }

    pub fn with_policy(
        mut self,
        auto_index_at: Option<u32>,
        hnsw_m: Option<u32>,
        hnsw_ef_construction: Option<u32>,
        hnsw_ef_search: Option<u32>,
        policy_revision: u64,
    ) -> Self {
        self.auto_index_at = auto_index_at;
        self.hnsw_m = hnsw_m;
        self.hnsw_ef_construction = hnsw_ef_construction;
        self.hnsw_ef_search = hnsw_ef_search;
        self.policy_revision = policy_revision.max(contextdb_core::DEFAULT_VECTOR_POLICY_REVISION);
        self
    }

    pub fn with_consolidation_policy(
        mut self,
        change_percent: Option<u32>,
        tombstone_percent: Option<u32>,
    ) -> Self {
        self.consolidation_change_percent = change_percent;
        self.consolidation_tombstone_percent = tombstone_percent;
        self.consolidation_disabled = false;
        self
    }

    pub fn with_consolidation_disabled(mut self, disabled: bool) -> Self {
        self.consolidation_disabled = disabled;
        if disabled {
            self.consolidation_change_percent = None;
            self.consolidation_tombstone_percent = None;
        }
        self
    }

    pub fn unpartitioned(dimension: usize, quantization: VectorQuantization) -> Self {
        Self {
            dimension,
            quantization,
            partition_key_columns: Vec::new(),
            max_partitions: None,
            search_mode: VectorSearchMode::Auto,
            auto_index_at: None,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
            policy_revision: contextdb_core::DEFAULT_VECTOR_POLICY_REVISION,
            consolidation_change_percent: None,
            consolidation_tombstone_percent: None,
            consolidation_disabled: false,
        }
    }

    pub fn effective_consolidation_change_percent(&self) -> u32 {
        self.consolidation_change_percent
            .unwrap_or(contextdb_core::DEFAULT_VECTOR_CONSOLIDATION_CHANGE_PERCENT)
    }

    pub fn effective_consolidation_tombstone_percent(&self) -> u32 {
        self.consolidation_tombstone_percent
            .unwrap_or(contextdb_core::DEFAULT_VECTOR_CONSOLIDATION_TOMBSTONE_PERCENT)
    }

    pub fn consolidation_due(
        &self,
        live_rows: usize,
        retained_rows: usize,
        pending_inserts: usize,
        tombstones: usize,
    ) -> bool {
        if self.consolidation_disabled {
            return false;
        }
        let population = live_rows.saturating_add(retained_rows).max(1) as u128;
        let changed = pending_inserts.saturating_add(tombstones) as u128;
        let tombstones = tombstones as u128;
        changed.saturating_mul(100)
            >= population.saturating_mul(self.effective_consolidation_change_percent() as u128)
            || tombstones.saturating_mul(100)
                >= population
                    .saturating_mul(self.effective_consolidation_tombstone_percent() as u128)
    }

    pub fn maintenance_need(
        &self,
        partition: &VectorPartitionInfo,
    ) -> Option<VectorMaintenanceNeed> {
        if let Some(reason) = partition.quarantine_reason {
            return Some(VectorMaintenanceNeed::Quarantine(reason));
        }
        if partition.live_rows == 0
            && partition.retained_rows == 0
            && partition.pending_inserts == 0
            && partition.tombstones == 0
        {
            return None;
        }
        if !partition.graph_available {
            return Some(VectorMaintenanceNeed::InitialBuild);
        }
        if partition.serving_policy.is_none_or(|serving| {
            serving.hnsw_m != partition.desired_policy.hnsw_m
                || serving.hnsw_ef_construction != partition.desired_policy.hnsw_ef_construction
        }) {
            return Some(VectorMaintenanceNeed::PolicyReplacement);
        }
        if !self.consolidation_due(
            partition.live_rows,
            partition.retained_rows,
            partition.pending_inserts,
            partition.tombstones,
        ) {
            return None;
        }
        Some(if partition.tombstones != 0 {
            VectorMaintenanceNeed::Tombstones
        } else {
            VectorMaintenanceNeed::NewChanges
        })
    }

    pub fn effective_auto_index_at(&self) -> usize {
        self.auto_index_at.map_or_else(
            || match self.quantization {
                VectorQuantization::F32 => 1_000,
                VectorQuantization::SQ8 | VectorQuantization::SQ4 => 5_001,
            },
            |value| value as usize,
        )
    }

    /// Resolve the policy for one partition size and one requested top-k.
    /// The topology members use the partition's live count; AUTO routing uses
    /// `effective_auto_index_at` against the separately aggregated allowed
    /// count at the caller.
    pub fn resolve_policy(&self, live_vectors: usize, k: usize) -> ResolvedVectorPolicy {
        let (profile_m, profile_ef_construction, profile_ef_search) =
            compatibility_hnsw_profile(live_vectors, self.quantization);
        let (hnsw_ef_search, ef_search_source) = match self.hnsw_ef_search {
            Some(declared) if (declared as usize) < k => (k, "declared_raised_to_k"),
            Some(declared) => (declared as usize, "declared"),
            None => (
                profile_ef_search.max(k.saturating_mul(10)).max(1),
                "compatibility_profile",
            ),
        };
        ResolvedVectorPolicy {
            auto_index_at: self.effective_auto_index_at(),
            hnsw_m: self.hnsw_m.map_or(profile_m, |value| value as usize),
            hnsw_ef_construction: self
                .hnsw_ef_construction
                .map_or(profile_ef_construction, |value| value as usize),
            hnsw_ef_search,
            policy_revision: self
                .policy_revision
                .max(contextdb_core::DEFAULT_VECTOR_POLICY_REVISION),
            auto_index_at_source: if self.auto_index_at.is_some() {
                "declared"
            } else {
                "compatibility_profile"
            },
            ef_search_source,
        }
    }

    /// Validate every compatibility size band so a partial declaration can
    /// never become invalid merely because a partition crosses a threshold.
    pub fn hnsw_policy_is_valid(&self) -> bool {
        [0, HNSW_MEDIUM_PROFILE_START, HNSW_LARGE_PROFILE_START]
            .into_iter()
            .all(|count| {
                let resolved = self.resolve_policy(count, 1);
                resolved.hnsw_ef_construction >= resolved.hnsw_m
            })
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_key_columns.is_empty()
    }

    fn has_same_storage_shape(&self, other: &Self) -> bool {
        self.dimension == other.dimension
            && self.quantization == other.quantization
            && self.partition_key_columns == other.partition_key_columns
    }

    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.partition_key_columns
                    .capacity()
                    .saturating_mul(std::mem::size_of::<String>()),
            )
            .saturating_add(
                self.partition_key_columns
                    .iter()
                    .map(|column| column.capacity())
                    .sum::<usize>(),
            )
    }
}

const HNSW_MEDIUM_PROFILE_START: usize = 5_001;
const HNSW_LARGE_PROFILE_START: usize = 50_001;

fn compatibility_hnsw_profile(
    count: usize,
    quantization: VectorQuantization,
) -> (usize, usize, usize) {
    match quantization {
        VectorQuantization::F32 => match count {
            n if n < HNSW_MEDIUM_PROFILE_START => (16, 200, count.max(200)),
            n if n < HNSW_LARGE_PROFILE_START => (24, 400, 400),
            _ => (16, 200, 200),
        },
        VectorQuantization::SQ8 | VectorQuantization::SQ4 => match count {
            n if n < HNSW_MEDIUM_PROFILE_START => (8, 32, count.clamp(32, 96)),
            _ => (12, 64, 128),
        },
    }
}

/// One vector entry plus the exact typed partition that owns it. `VectorEntry`
/// remains unchanged for wire and compatibility callers.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedVectorEntry {
    pub partition_key: VectorPartitionKey,
    pub entry: VectorEntry,
}

impl PartitionedVectorEntry {
    pub fn new(partition_key: VectorPartitionKey, entry: VectorEntry) -> Self {
        Self {
            partition_key,
            entry,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VectorPartitionRef {
    pub index: VectorIndexRef,
    pub partition_key: VectorPartitionKey,
}

impl VectorPartitionRef {
    pub fn new(index: VectorIndexRef, partition_key: VectorPartitionKey) -> Self {
        Self {
            index,
            partition_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedVectorDelete {
    pub partition: VectorPartitionRef,
    pub row_id: RowId,
    pub deleted_tx: TxId,
}

impl PartitionedVectorDelete {
    pub fn new(
        index: VectorIndexRef,
        partition_key: VectorPartitionKey,
        row_id: RowId,
        deleted_tx: TxId,
    ) -> Self {
        Self {
            partition: VectorPartitionRef::new(index, partition_key),
            row_id,
            deleted_tx,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedVectorMove {
    replace_row_version: bool,
    pub index: VectorIndexRef,
    pub source_partition_key: VectorPartitionKey,
    pub target_partition_key: VectorPartitionKey,
    pub old_row_id: RowId,
    pub new_row_id: RowId,
    pub tx: TxId,
}

impl PartitionedVectorMove {
    pub fn new(
        index: VectorIndexRef,
        source_partition_key: VectorPartitionKey,
        target_partition_key: VectorPartitionKey,
        old_row_id: RowId,
        new_row_id: RowId,
        tx: TxId,
    ) -> Self {
        Self {
            replace_row_version: false,
            index,
            source_partition_key,
            target_partition_key,
            old_row_id,
            new_row_id,
            tx,
        }
    }
    /// A scalar row replacement still advances the durable owner binding of
    /// its unchanged vector, including when its identity and partition stay.
    pub fn replacing_row_version(mut self) -> Self {
        self.replace_row_version = true;
        self
    }
}

/// Read-only validation result for one atomic keyed publication. The engine
/// prepares this before durability while holding its commit/snapshot gates,
/// then consumes it at publication. `reclaimable_partitions` means the engine
/// has proved that no live row and no active snapshot still needs the source's
/// maintained graph/count identity after this batch. Raw vector history is
/// retained until the row-version retention pass authorizes matching row and
/// vector reclamation together.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedPartitionedVectorBatch {
    deletes: Vec<PartitionedVectorDelete>,
    inserts: Vec<PartitionedVectorEntry>,
    moves: Vec<PartitionedVectorMove>,
    reclaimable_partitions: HashSet<VectorPartitionRef>,
    touched: HashSet<VectorPartitionRef>,
    requested: HashMap<VectorIndexRef, PartitionKeySet>,
    projected_live_or_retained_partitions: HashMap<VectorIndexRef, usize>,
    move_raw_body_bytes: usize,
    directory_reservation: PreparedDirectoryReservation,
    tail_insertions: PreparedTailInsertions,
}

struct PreparedTailInsertion {
    state: Arc<IndexState>,
    row_id: RowId,
    insertion: Option<crate::hnsw::PreparedHnswInsertion>,
    charge: RetainedMemoryCharge,
    resident_bytes: usize,
}

impl PreparedTailInsertion {
    fn publish(mut self, lsn: Lsn) {
        let _publication = self.state.generation_publication.lock();
        let tail = self
            .state
            .hnsw
            .get()
            .expect("prepared fresh tail exists")
            .read();
        let graph = tail
            .as_ref()
            .expect("prepared fresh tail stays resident through commit");
        graph.publish_insert(
            self.insertion
                .take()
                .expect("prepared insertion publishes once"),
        );
        self.state
            .hnsw_bytes
            .fetch_add(self.resident_bytes, Ordering::SeqCst);
        *self.state.hnsw_accountant.write() = Some(self.charge.accountant.clone());
        self.charge.bytes -= self.resident_bytes;
        self.state
            .fresh_tail_mutated_through_lsn
            .fetch_max(lsn.0, Ordering::SeqCst);
        self.state
            .fresh_tail_revision
            .fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for PreparedTailInsertion {
    fn drop(&mut self) {
        let previous = self
            .state
            .fresh_tail_preparations
            .fetch_sub(1, Ordering::SeqCst);
        debug_assert!(
            previous != 0,
            "a prepared tail owns its insertion target pin"
        );
    }
}

#[derive(Clone)]
struct PreparedTailInsertions(Arc<Mutex<VecDeque<PreparedTailInsertion>>>);

impl std::fmt::Debug for PreparedTailInsertions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PreparedTailInsertions")
            .field(&self.0.lock().len())
            .finish()
    }
}

impl PartialEq for PreparedTailInsertions {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl PreparedTailInsertions {
    fn publish(&self, state: &Arc<IndexState>, row_id: RowId, lsn: Lsn) {
        let mut pending = self.0.lock();
        if let Some(at) = pending
            .iter()
            .position(|item| Arc::ptr_eq(&item.state, state) && item.row_id == row_id)
        {
            pending
                .remove(at)
                .expect("prepared insertion position exists")
                .publish(lsn);
        }
    }
}

#[derive(Clone)]
struct PreparedDirectoryReservation {
    charge: Arc<Mutex<Option<RetainedMemoryCharge>>>,
}

impl PreparedDirectoryReservation {
    fn empty() -> Self {
        Self {
            charge: Arc::new(Mutex::new(None)),
        }
    }

    fn reserve(accountant: Arc<dyn MemoryBudget>, bytes: usize) -> Result<Self> {
        accountant.try_allocate_for(
            bytes,
            "vector_index",
            "prepare_vector_directory_growth",
            "Reduce partition-key or vector-history growth, or raise MEMORY_LIMIT before committing the row.",
        )?;
        Ok(Self {
            charge: Arc::new(Mutex::new(Some(RetainedMemoryCharge { bytes, accountant }))),
        })
    }

    fn take(&self) -> Option<RetainedMemoryCharge> {
        self.charge.lock().take()
    }
}

impl std::fmt::Debug for PreparedDirectoryReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedDirectoryReservation")
            .field(
                "bytes",
                &self.charge.lock().as_ref().map_or(0, |charge| charge.bytes),
            )
            .finish()
    }
}

impl PartialEq for PreparedDirectoryReservation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.charge, &other.charge)
    }
}

/// Marker returned by infallible prepared publication. It deliberately is not
/// a `Result`: Redb has already committed by this point. `unwrap` remains as
/// a no-op compatibility aid for older internal call sites while they migrate
/// to the direct infallible contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedPartitionedVectorPublication;

impl PreparedPartitionedVectorPublication {
    pub fn unwrap(self) {}
}

impl PreparedPartitionedVectorBatch {
    pub fn valid_move_count(&self) -> usize {
        self.moves.len()
    }

    pub fn projected_live_or_retained_partition_count(
        &self,
        index: &VectorIndexRef,
    ) -> Option<usize> {
        self.projected_live_or_retained_partitions
            .get(index)
            .copied()
    }
}

fn prepared_directory_growth_upper_bound(
    inserts: &[PartitionedVectorEntry],
    moves: &[PartitionedVectorMove],
    reclaimable_partitions: &HashSet<VectorPartitionRef>,
    requested: &HashMap<VectorIndexRef, PartitionKeySet>,
) -> usize {
    let directory_entry = std::mem::size_of::<RawVectorDirectoryEntry>()
        .saturating_add(4 * std::mem::size_of::<TxId>() + 8 * std::mem::size_of::<(u64, u64)>())
        .saturating_add(std::mem::size_of::<(RowId, Vec<usize>)>())
        .saturating_add(std::mem::size_of::<usize>())
        .saturating_add(std::mem::size_of::<(RowId, VectorPartitionKey)>())
        .saturating_sub(std::mem::size_of::<VectorPartitionKey>());
    let mut bytes = inserts
        .len()
        .saturating_add(moves.len())
        .saturating_mul(directory_entry);
    for insert in inserts {
        bytes = bytes.saturating_add(insert.partition_key.estimated_bytes());
    }
    for row_move in moves {
        bytes = bytes.saturating_add(row_move.target_partition_key.estimated_bytes());
    }
    for keys in requested.values() {
        for key in keys {
            bytes = bytes
                .saturating_add(std::mem::size_of::<Arc<IndexState>>())
                .saturating_add(std::mem::size_of::<IndexState>())
                .saturating_add(std::mem::size_of::<usize>().saturating_mul(3))
                .saturating_add(key.estimated_bytes());
        }
    }
    for partition in reclaimable_partitions {
        bytes = bytes.saturating_add(partition.partition_key.estimated_bytes());
    }
    bytes
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPartitionInfo {
    pub partition: VectorPartitionRef,
    pub live_rows: usize,
    pub retained_rows: usize,
    pub bytes: usize,
    pub graph_available: bool,
    pub base_generation: Option<u64>,
    pub base_tx: Option<TxId>,
    pub pending_inserts: usize,
    pub tombstones: usize,
    pub durable_vector_bytes: usize,
    pub charged_vector_bytes: usize,
    pub durable_index_bytes: usize,
    pub charged_index_bytes: usize,
    pub quarantine_reason: Option<VectorRouteQuarantineReason>,
    pub maintenance_progress: Option<VectorMaintenanceProgress>,
    pub maintenance_failure: Option<VectorMaintenanceFailure>,
    pub maintenance_failure_details: Option<VectorMaintenanceFailureDetails>,
    pub desired_policy: ResolvedVectorPolicy,
    pub serving_policy: Option<VectorServingPolicy>,
    pub maintenance_policy_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexLayoutInfo {
    pub index: VectorIndexRef,
    pub layout: VectorIndexLayout,
    pub live_partitions: usize,
    pub retained_partitions: usize,
    pub live_or_retained_partitions: usize,
    pub partition_states: usize,
    pub bytes: usize,
    pub graph_available: bool,
}

/// Marks one bounded replacement attempt for passive inspection. Dropping
/// the guard clears only the attempt it installed, so a newer retry cannot be
/// erased by an older worker finishing late.
pub struct VectorPolicyBuildGuard {
    state: Arc<IndexState>,
    revision: u64,
}

impl Drop for VectorPolicyBuildGuard {
    fn drop(&mut self) {
        let _ = self.state.maintenance_policy_revision.compare_exchange(
            self.revision,
            0,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }
}

struct VectorColumnState {
    layout: RwLock<VectorIndexLayout>,
    partitions: RwLock<BTreeMap<VectorPartitionKey, Arc<IndexState>>>,
    row_to_partition: RwLock<HashMap<RowId, VectorPartitionKey>>,
    /// File-backed open reserves the lightweight registry/directory image as
    /// one retained category. The column keeps that exact reservation so a
    /// later DROP COLUMN/TABLE returns it once, independent of cache state.
    directory_accounting: Mutex<Option<RetainedMemoryCharge>>,
}

impl Drop for VectorColumnState {
    fn drop(&mut self) {
        self.release_directory_accounting();
    }
}

impl VectorColumnState {
    fn new(layout: VectorIndexLayout) -> Self {
        let mut partitions = BTreeMap::new();
        if !layout.is_partitioned() {
            partitions.insert(
                VectorPartitionKey::unpartitioned(),
                Arc::new(IndexState::new(layout.dimension, layout.quantization)),
            );
        }
        Self {
            layout: RwLock::new(layout),
            partitions: RwLock::new(partitions),
            row_to_partition: RwLock::new(HashMap::new()),
            directory_accounting: Mutex::new(None),
        }
    }

    fn layout(&self) -> VectorIndexLayout {
        self.layout.read().clone()
    }

    fn partition_state(&self, key: &VectorPartitionKey) -> Option<Arc<IndexState>> {
        self.partitions.read().get(key).cloned()
    }

    fn partition_snapshots(&self) -> Vec<(VectorPartitionKey, Arc<IndexState>)> {
        self.partitions
            .read()
            .iter()
            .map(|(key, state)| (key.clone(), state.clone()))
            .collect()
    }

    fn partition_keys(&self) -> Vec<VectorPartitionKey> {
        self.maintained_partition_snapshots()
            .into_iter()
            .map(|(key, _)| key)
            .collect()
    }

    fn partition_keys_including_historical(&self) -> Vec<VectorPartitionKey> {
        self.partitions.read().keys().cloned().collect()
    }

    fn partition_is_historical_only(&self, key: &VectorPartitionKey) -> bool {
        self.partitions
            .read()
            .get(key)
            .is_some_and(|state| state.is_historical_only())
    }

    fn maintained_partition_snapshots(&self) -> Vec<(VectorPartitionKey, Arc<IndexState>)> {
        self.partitions
            .read()
            .iter()
            .filter(|(_, state)| !state.is_historical_only())
            .map(|(key, state)| (key.clone(), state.clone()))
            .collect()
    }

    fn reactivate_partitions(&self, keys: &PartitionKeySet) {
        if keys.is_empty() {
            return;
        }
        let partitions = self.partitions.read();
        for key in keys {
            if let Some(state) = partitions.get(key) {
                state.reactivate_maintained_state();
            }
        }
    }

    fn mark_historical_only(&self, key: &VectorPartitionKey) {
        if let Some(state) = self.partitions.read().get(key) {
            state.mark_historical_only();
        }
    }

    fn live_partition_count(&self) -> usize {
        self.maintained_partition_snapshots()
            .into_iter()
            .map(|(_, state)| state)
            .filter(|state| state.vector_count() != 0)
            .count()
    }

    fn retained_partition_count(&self) -> usize {
        self.maintained_partition_snapshots()
            .into_iter()
            .map(|(_, state)| state)
            .filter(|state| state.entry_count().saturating_sub(state.vector_count()) != 0)
            .count()
    }

    fn live_or_retained_partition_count(&self) -> usize {
        self.maintained_partition_snapshots()
            .into_iter()
            .map(|(_, state)| state)
            .filter(|state| state.entry_count() != 0)
            .count()
    }

    fn maintained_partition_state_count(&self) -> usize {
        self.partitions
            .read()
            .values()
            .filter(|state| !state.is_historical_only())
            .count()
    }

    fn live_vector_count(&self) -> usize {
        self.partitions
            .read()
            .values()
            .map(|state| state.vector_count())
            .sum()
    }

    fn total_entry_count(&self) -> usize {
        self.partitions
            .read()
            .values()
            .map(|state| state.entry_count())
            .sum()
    }

    fn has_hnsw(&self) -> bool {
        self.partitions
            .read()
            .values()
            .any(|state| state.hnsw_len().is_some())
    }

    fn has_complete_hnsw_route(&self) -> bool {
        let partitions = self.partitions.read();
        let mut nonempty = partitions
            .values()
            .filter(|state| state.vector_count() != 0);
        let Some(first) = nonempty.next() else {
            return false;
        };
        first.has_complete_hnsw_route() && nonempty.all(|state| state.has_complete_hnsw_route())
    }

    fn current_partition(&self, row_id: RowId) -> Option<VectorPartitionKey> {
        self.row_to_partition.read().get(&row_id).cloned()
    }

    fn note_live_insert(&self, key: &VectorPartitionKey, entry: &StoredVectorEntry) {
        if entry.deleted_tx.is_none() {
            self.row_to_partition
                .write()
                .insert(entry.row_id, key.clone());
        }
    }

    fn note_tombstone(&self, key: &VectorPartitionKey, row_id: RowId) {
        let mut rows = self.row_to_partition.write();
        if rows.get(&row_id).is_some_and(|current| current == key) {
            rows.remove(&row_id);
        }
    }

    fn note_move(
        &self,
        source_key: &VectorPartitionKey,
        target_key: &VectorPartitionKey,
        old_row_id: RowId,
        new_row_id: RowId,
    ) {
        let mut rows = self.row_to_partition.write();
        if rows
            .get(&old_row_id)
            .is_some_and(|current| current == source_key)
        {
            rows.remove(&old_row_id);
        }
        rows.insert(new_row_id, target_key.clone());
    }

    fn rebuild_current_rows(&self) {
        let partitions = self.partition_snapshots();
        let mut latest = HashMap::<RowId, (TxId, Lsn, VectorPartitionKey)>::new();
        for (key, state) in partitions {
            for entry in state
                .raw_directory
                .read()
                .iter()
                .filter(|entry| entry.deleted_tx.is_none())
            {
                let replace = latest
                    .get(&entry.row_id)
                    .is_none_or(|(tx, lsn, _)| (entry.created_tx, entry.lsn) > (*tx, *lsn));
                if replace {
                    latest.insert(entry.row_id, (entry.created_tx, entry.lsn, key.clone()));
                }
            }
        }
        *self.row_to_partition.write() = latest
            .into_iter()
            .map(|(row_id, (_, _, key))| (row_id, key))
            .collect();
    }

    fn remove_empty_partitioned_states(&self) -> bool {
        if !self.layout.read().is_partitioned() {
            return false;
        }
        let removed = {
            let mut partitions = self.partitions.write();
            let removed = partitions
                .iter()
                .filter(|(_, state)| state.entry_count() == 0)
                .map(|(key, _)| key.clone())
                .collect::<HashSet<_>>();
            partitions.retain(|key, _| !removed.contains(key));
            removed
        };
        !removed.is_empty()
    }

    fn clear_hnsw(&self, accountant: &dyn MemoryBudget) {
        for state in self.partitions.read().values() {
            state.clear_hnsw(accountant);
        }
    }

    fn drop_hnsw_without_accounting_except(&self, preserved_states: &HashSet<usize>) {
        for state in self.partitions.read().values() {
            if !preserved_states.contains(&(Arc::as_ptr(state) as usize)) {
                state.drop_hnsw_without_accounting();
            }
        }
    }

    fn clear_hnsw_except(&self, accountant: &dyn MemoryBudget, preserved_states: &HashSet<usize>) {
        for state in self.partitions.read().values() {
            if !preserved_states.contains(&(Arc::as_ptr(state) as usize)) {
                state.clear_hnsw(accountant);
            }
        }
    }

    fn retained_bytes(&self, index: &VectorIndexRef) -> usize {
        let state_bytes = self
            .partitions
            .read()
            .values()
            .fold(0usize, |bytes, state| {
                bytes
                    .saturating_add(state.raw_payload_bytes())
                    .saturating_add(state.hnsw_bytes.load(Ordering::SeqCst))
                    .saturating_add(state.sealed_hnsw_bytes())
            });
        self.directory_ownership(index)
            .total()
            .saturating_add(state_bytes)
    }

    /// Exact disjoint owners in the lightweight registry image. These are
    /// logical retained-byte charges, so growth is deterministic and can be
    /// admitted before publication instead of inferred from allocator
    /// capacity after mutation.
    fn directory_ownership(&self, index: &VectorIndexRef) -> VectorDirectoryOwnership {
        let layout_bytes = self.layout.read().estimated_bytes();
        let partitions = self.partitions.read();
        let mut ownership = VectorDirectoryOwnership {
            partition_vectors: std::mem::size_of::<Self>()
                .saturating_add(index.table.capacity())
                .saturating_add(index.column.capacity())
                .saturating_add(layout_bytes),
            ..VectorDirectoryOwnership::default()
        };
        for (key, state) in partitions.iter() {
            ownership.typed_partition_keys = ownership
                .typed_partition_keys
                .saturating_add(key.estimated_bytes());
            ownership.partition_vectors = ownership
                .partition_vectors
                .saturating_add(std::mem::size_of::<Arc<IndexState>>())
                .saturating_add(std::mem::size_of::<IndexState>())
                .saturating_add(std::mem::size_of::<usize>().saturating_mul(3));
            ownership.raw_reverse_directory = ownership
                .raw_reverse_directory
                .saturating_add(state.directory_byte_count());
        }
        drop(partitions);
        let rows = self.row_to_partition.read();
        let key_header = std::mem::size_of::<VectorPartitionKey>();
        ownership.partition_vectors = ownership.partition_vectors.saturating_add(
            rows.len()
                .saturating_mul(std::mem::size_of::<(RowId, VectorPartitionKey)>())
                .saturating_sub(rows.len().saturating_mul(key_header)),
        );
        for key in rows.values() {
            ownership.typed_partition_keys = ownership
                .typed_partition_keys
                .saturating_add(key.estimated_bytes());
        }
        ownership
    }

    /// Retained registry identity without raw bodies or graph pages. This is
    /// the category writable open keeps resident for every idle partition.
    fn directory_retained_bytes(&self, index: &VectorIndexRef) -> usize {
        self.directory_ownership(index).total()
    }

    fn install_directory_accounting(&self, bytes: usize, accountant: Arc<dyn MemoryBudget>) {
        debug_assert!(self.directory_accounting.lock().is_none());
        *self.directory_accounting.lock() = Some(RetainedMemoryCharge { bytes, accountant });
    }

    fn settle_directory_accounting(
        &self,
        index: &VectorIndexRef,
        reservation: &mut RetainedMemoryCharge,
    ) {
        let new_bytes = self.directory_retained_bytes(index);
        let mut slot = self.directory_accounting.lock();
        if let Some(charge) = slot.as_mut() {
            if new_bytes > charge.bytes {
                let growth = new_bytes - charge.bytes;
                assert!(
                    growth <= reservation.bytes,
                    "prepared vector-directory reservation covers publication growth"
                );
                reservation.bytes -= growth;
            } else if charge.bytes > new_bytes {
                charge.accountant.release(charge.bytes - new_bytes);
            }
            charge.bytes = new_bytes;
            return;
        }
        assert!(
            new_bytes <= reservation.bytes,
            "prepared vector-directory reservation covers initial publication"
        );
        reservation.bytes -= new_bytes;
        *slot = Some(RetainedMemoryCharge {
            bytes: new_bytes,
            accountant: reservation.accountant.clone(),
        });
    }

    /// Return exactly the directory bytes one reclamation pass physically
    /// removed from this index. `bytes_before` is the directory figure the
    /// pass sampled before it removed anything, under the same bulk
    /// maintenance guard, so the difference is this pass's own shrinkage and
    /// nothing else's: growth is charged by publication as it happens
    /// (`settle_directory_accounting`), and a pass that runs after a commit
    /// which both applied rows and left versions to reclaim -- a received
    /// image does exactly that -- sees the grown directory already charged.
    /// The two assertions keep the owner honest: a reclamation never grows
    /// the directory it reclaims from, and it never returns more than the
    /// owner still holds.
    fn reconcile_directory_accounting_after_reclamation(
        &self,
        index: &VectorIndexRef,
        bytes_before: usize,
    ) {
        let bytes_after = self.directory_retained_bytes(index);
        assert!(
            bytes_after <= bytes_before,
            "vector-directory reclamation cannot grow the directory it reclaims from: \
             {index_table}.{index_column} held {bytes_before} bytes before the pass and \
             {bytes_after} bytes after it",
            index_table = index.table,
            index_column = index.column,
        );
        let reclaimed = bytes_before - bytes_after;
        if reclaimed == 0 {
            return;
        }
        let mut slot = self.directory_accounting.lock();
        let Some(charge) = slot.as_mut() else {
            return;
        };
        assert!(
            reclaimed <= charge.bytes,
            "vector-directory reclamation cannot return more than the owner holds: \
             {index_table}.{index_column} reclaimed {reclaimed} bytes but the retained \
             charge is {} bytes",
            charge.bytes,
            index_table = index.table,
            index_column = index.column,
        );
        charge.accountant.release(reclaimed);
        charge.bytes -= reclaimed;
    }

    fn release_directory_accounting(&self) {
        drop(self.directory_accounting.lock().take());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexInfo {
    pub index: VectorIndexRef,
    pub dimension: usize,
    pub quantization: VectorQuantization,
    pub vector_count: usize,
    pub bytes: usize,
}

/// One finite maintained-index cycle. Every needy partition in the wake's
/// fixed sample is attempted; successful work and the first local refusal are
/// reported together so cycle-closing work is never skipped. The stable
/// failure class remains available for simple callers, while
/// `first_failure_details` carries safe operation, budget, and recovery data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorMaintenanceReport {
    pub nonempty_indexes: usize,
    pub ready_indexes: usize,
    pub built_indexes: usize,
    pub remaining_indexes: usize,
    pub nonempty_partitions: usize,
    pub ready_partitions: usize,
    pub built_partitions: usize,
    pub remaining_partitions: usize,
    pub first_failure: Option<VectorMaintenanceFailure>,
    pub first_failure_details: Option<VectorMaintenanceFailureDetails>,
}

#[derive(Default)]
struct PendingVectorChanges {
    deletes: Vec<(VectorIndexRef, RowId, TxId)>,
    inserts: Vec<VectorEntry>,
    moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
}

#[derive(Default)]
struct PendingVectorChangesRef<'a> {
    deletes: Vec<&'a (VectorIndexRef, RowId, TxId)>,
    inserts: Vec<&'a VectorEntry>,
    moves: Vec<&'a (VectorIndexRef, RowId, RowId, TxId)>,
}

#[derive(Default)]
struct ProjectedPartitionState {
    total_entries: usize,
    live_by_row: HashMap<RowId, usize>,
    latest_live_rows: HashSet<RowId>,
    counts_toward_partition_limit: bool,
}

impl ProjectedPartitionState {
    fn from_entry_count(total_entries: usize, counts_toward_partition_limit: bool) -> Self {
        Self {
            total_entries,
            counts_toward_partition_limit,
            ..Self::default()
        }
    }

    fn from_state(state: &IndexState, counts_toward_partition_limit: bool) -> Self {
        {
            let entries = state.raw_directory.read();
            let mut projected = Self {
                total_entries: entries.len(),
                live_by_row: HashMap::new(),
                latest_live_rows: HashSet::new(),
                counts_toward_partition_limit,
            };
            for entry in entries.iter() {
                if entry.deleted_tx.is_none() {
                    *projected.live_by_row.entry(entry.row_id).or_default() += 1;
                    projected.latest_live_rows.insert(entry.row_id);
                } else {
                    projected.latest_live_rows.remove(&entry.row_id);
                }
            }
            projected
        }
    }

    fn delete_live_row(&mut self, row_id: RowId) -> usize {
        self.latest_live_rows.remove(&row_id);
        self.live_by_row.remove(&row_id).unwrap_or(0)
    }

    fn has_movable_row(&self, row_id: RowId) -> bool {
        self.latest_live_rows.contains(&row_id)
    }

    fn insert(&mut self, entry: &VectorEntry) {
        self.total_entries = self.total_entries.saturating_add(1);
        self.counts_toward_partition_limit = true;
        if entry.deleted_tx.is_none() {
            *self.live_by_row.entry(entry.row_id).or_default() += 1;
            self.latest_live_rows.insert(entry.row_id);
        } else {
            self.latest_live_rows.remove(&entry.row_id);
        }
    }

    fn insert_moved(&mut self, row_id: RowId) {
        self.total_entries = self.total_entries.saturating_add(1);
        self.counts_toward_partition_limit = true;
        *self.live_by_row.entry(row_id).or_default() += 1;
        self.latest_live_rows.insert(row_id);
    }

    fn live_count(&self) -> usize {
        self.live_by_row.values().copied().sum()
    }

    fn retire_maintained_state(&mut self) {
        self.counts_toward_partition_limit = false;
    }
}

/// Identity of one vector store, handed out at construction and never reused.
///
/// The re-entrancy guards below ask "is this store's lock already held on this
/// thread?", and the answer has to be about a STORE. A raw address is not an
/// identity: a dropped store frees its address for the next one, so two stores
/// that never met can carry the same number and one can be mistaken for the
/// other -- which decides that a lock is held when it is not.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct StoreId(u64);

static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

impl StoreId {
    fn next() -> Self {
        Self(NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed))
    }
}

pub struct VectorStore {
    last_maintenance_partition: Mutex<Option<VectorPartitionRef>>,
    graph_use_sequence: AtomicU64,
    id: StoreId,
    registry: RwLock<HashMap<VectorIndexRef, Arc<VectorColumnState>>>,
    build_mutex: Mutex<()>,
    /// Lets destructive all-index graph operations wait for builders without
    /// putting foreground structural publication behind the bulk gate.
    hnsw_build_gate: RwLock<()>,
    bulk_gate: RwLock<()>,
    #[cfg(any(test, feature = "test-seams"))]
    full_index_entries_touched: AtomicU64,
    #[cfg(feature = "test-seams")]
    pause_registry: crate::test_seam::PauseRegistry,
    #[cfg(feature = "test-seams")]
    graph_candidate_caps: crate::test_seam::GraphCandidateCapRegistry,
    #[cfg(any(test, feature = "test-seams"))]
    passive_activity: Arc<VectorPassiveActivity>,
    #[cfg(any(test, feature = "test-seams"))]
    maintenance_progress_pauses:
        Mutex<HashMap<VectorPartitionRef, Arc<MaintenanceProgressPauseSlot>>>,
    #[cfg(any(test, feature = "test-seams"))]
    maintenance_preparation_pauses: Mutex<
        HashMap<
            (VectorPartitionRef, VectorMaintenancePreparationPhaseForTest),
            Arc<MaintenanceProgressPauseSlot>,
        >,
    >,
    #[cfg(any(test, feature = "test-seams"))]
    journal_truncation_faults: Mutex<
        HashMap<(VectorPartitionRef, VectorJournalTruncationPhaseForTest), Arc<OneShotFaultSlot>>,
    >,
    workspace_bytes: Arc<AtomicUsize>,
    memory_accountant: RwLock<Option<Arc<dyn MemoryBudget>>>,
    #[cfg(any(test, feature = "test-seams"))]
    memory_workspace_pause: Mutex<Option<Arc<MaintenanceProgressPauseSlot>>>,
}

/// A fully constructed vector registry.  Received-schema staging creates this
/// before durability so publishing it cannot reconfigure an index, quantize a
/// vector, or allocate registry entries after Redb commits.
pub struct PreparedVectorPublication {
    registry: HashMap<VectorIndexRef, Arc<VectorColumnState>>,
    /// State handles borrowed from the live registry because authoritative
    /// purge did not change their partition. Rollback must not clear these
    /// graphs, and successful publication must not clear them through the
    /// outgoing registry: the same `Arc` becomes part of the new image.
    preserved_states: HashSet<usize>,
}

impl PreparedVectorPublication {
    fn new(registry: HashMap<VectorIndexRef, Arc<VectorColumnState>>) -> Self {
        Self {
            registry,
            preserved_states: HashSet::new(),
        }
    }

    fn into_parts(
        mut self,
    ) -> (
        HashMap<VectorIndexRef, Arc<VectorColumnState>>,
        HashSet<usize>,
    ) {
        (
            std::mem::take(&mut self.registry),
            std::mem::take(&mut self.preserved_states),
        )
    }

    /// Bytes whose ownership is transferred by the engine's enclosing image
    /// swap. Newly built graphs already carry their own reservations and are
    /// deliberately excluded; preserved state handles retain their existing
    /// raw/graph reservations and are included only to neutralize the same
    /// continuing bytes in the outgoing image.
    #[doc(hidden)]
    pub fn memory_swap_vector_bytes(&self) -> usize {
        self.registry.iter().fold(0usize, |total, (index, column)| {
            let mut bytes = total.saturating_add(column.directory_retained_bytes(index));
            for (_, state) in column.partition_snapshots() {
                bytes = bytes.saturating_add(state.raw_payload_bytes());
                if self
                    .preserved_states
                    .contains(&(Arc::as_ptr(&state) as usize))
                {
                    bytes = bytes
                        .saturating_add(state.hnsw_bytes.load(Ordering::SeqCst))
                        .saturating_add(state.sealed_hnsw_bytes());
                }
            }
            bytes
        })
    }

    #[doc(hidden)]
    pub fn preserved_state_bytes(&self) -> usize {
        self.registry.values().fold(0usize, |total, column| {
            column
                .partition_snapshots()
                .into_iter()
                .filter(|(_, state)| {
                    self.preserved_states
                        .contains(&(Arc::as_ptr(state) as usize))
                })
                .fold(total, |bytes, (_, state)| {
                    bytes
                        .saturating_add(state.raw_payload_bytes())
                        .saturating_add(state.hnsw_bytes.load(Ordering::SeqCst))
                        .saturating_add(state.sealed_hnsw_bytes())
                })
        })
    }

    /// Encode and seal one affected survivor partition before the owning
    /// purge transaction starts. The returned bytes are immutable input to
    /// persistence; the same prepared graph becomes visible in memory only
    /// if that transaction commits and consumes this publication.
    #[doc(hidden)]
    pub fn prepare_partition_base_generation(
        &self,
        partition: &VectorPartitionRef,
        identity: VectorGraphGeneration,
    ) -> Result<(Vec<u8>, usize)> {
        let column =
            self.registry
                .get(&partition.index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: partition.index.clone(),
                })?;
        let state = column
            .partition_state(&partition.partition_key)
            .ok_or_else(|| {
                Error::Other(format!(
                    "prepared vector partition is missing for {}.{}",
                    partition.index.table, partition.index.column
                ))
            })?;
        let policy = column.layout().resolve_policy(state.vector_count(), 1);
        state.prepare_fresh_tail_as_base_generation(identity, policy)
    }
}

/// A prepared purge publication can own HNSW graphs before the durable commit.
/// If that commit never happens, release those precharged graphs with the
/// publication rather than leaking their final allocation.
impl Drop for PreparedVectorPublication {
    fn drop(&mut self) {
        for column in self.registry.values() {
            column.drop_hnsw_without_accounting_except(&self.preserved_states);
        }
    }
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new(Arc::new(OnceLock::new()))
    }
}

impl VectorStore {
    /// This store's identity. Two stores never share one, including a store
    /// constructed at an address a dropped store used to occupy -- which is
    /// the whole reason the identity is not the address.
    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub fn store_identity_for_test(&self) -> u64 {
        self.id.0
    }

    pub fn new(_legacy_hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>) -> Self {
        Self {
            id: StoreId::next(),
            registry: RwLock::new(HashMap::new()),
            build_mutex: Mutex::new(()),
            hnsw_build_gate: RwLock::new(()),
            bulk_gate: RwLock::new(()),
            #[cfg(any(test, feature = "test-seams"))]
            full_index_entries_touched: AtomicU64::new(0),
            #[cfg(feature = "test-seams")]
            pause_registry: crate::test_seam::PauseRegistry::default(),
            #[cfg(feature = "test-seams")]
            graph_candidate_caps: crate::test_seam::GraphCandidateCapRegistry::default(),
            #[cfg(any(test, feature = "test-seams"))]
            passive_activity: Arc::new(VectorPassiveActivity::default()),
            #[cfg(any(test, feature = "test-seams"))]
            maintenance_progress_pauses: Mutex::new(HashMap::new()),
            #[cfg(any(test, feature = "test-seams"))]
            maintenance_preparation_pauses: Mutex::new(HashMap::new()),
            #[cfg(any(test, feature = "test-seams"))]
            journal_truncation_faults: Mutex::new(HashMap::new()),
            last_maintenance_partition: Mutex::new(None),
            graph_use_sequence: AtomicU64::new(0),
            workspace_bytes: Arc::new(AtomicUsize::new(0)),
            memory_accountant: RwLock::new(None),
            #[cfg(any(test, feature = "test-seams"))]
            memory_workspace_pause: Mutex::new(None),
        }
    }

    /// Admit a real temporary owner before vector generation publication.
    #[doc(hidden)]
    pub fn reserve_workspace(
        &self,
        accountant: Arc<dyn MemoryBudget>,
        bytes: usize,
        operation: &str,
        hint: &str,
    ) -> Result<VectorWorkspaceReservation> {
        VectorWorkspaceReservation::try_new(
            accountant,
            self.workspace_bytes.clone(),
            bytes,
            operation,
            hint,
        )
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_memory_workspace_pause_for_test(&self) -> VectorLifecyclePauseHandle {
        let slot = Arc::new(MaintenanceProgressPauseSlot::new());
        let generation = slot.arm(0);
        *self.memory_workspace_pause.lock() = Some(slot.clone());
        VectorLifecyclePauseHandle { slot, generation }
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn maybe_pause_memory_workspace_for_test(&self) {
        if let Some(slot) = self.memory_workspace_pause.lock().take() {
            slot.pause_after(0);
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn attach_passive_activity_to_registry(&self) {
        for column in self.registry.read().values() {
            for (_, state) in column.partition_snapshots() {
                state.attach_passive_activity(self.passive_activity.clone());
            }
        }
    }

    /// Snapshot real lifecycle events without loading or evicting anything.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn passive_activity_counters_for_test(&self) -> VectorPassiveActivityCounters {
        self.passive_activity.snapshot()
    }

    /// Arm one partition's next maintenance build to pause after the requested
    /// number of actual stored vectors has been traversed.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn arm_maintenance_progress_pause_for_test(
        &self,
        partition: &VectorPartitionRef,
        after_vectors: usize,
    ) -> VectorMaintenanceProgressPauseHandle {
        let slot = self
            .maintenance_progress_pauses
            .lock()
            .entry(partition.clone())
            .or_insert_with(|| Arc::new(MaintenanceProgressPauseSlot::new()))
            .clone();
        if let Ok(state) = self.partition_state(&partition.index, &partition.partition_key) {
            *state.maintenance_step_pause.lock() = Some(slot.clone());
        }
        let generation = slot.arm(after_vectors);
        VectorMaintenanceProgressPauseHandle { slot, generation }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn arm_maintenance_preparation_pause_for_test(
        &self,
        partition: &VectorPartitionRef,
        phase: VectorMaintenancePreparationPhaseForTest,
    ) -> VectorLifecyclePauseHandle {
        let slot = self
            .maintenance_preparation_pauses
            .lock()
            .entry((partition.clone(), phase))
            .or_insert_with(|| Arc::new(MaintenanceProgressPauseSlot::new()))
            .clone();
        let generation = slot.arm(0);
        VectorLifecyclePauseHandle { slot, generation }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn maybe_pause_maintenance_preparation_for_test(
        &self,
        partition: &VectorPartitionRef,
        phase: VectorMaintenancePreparationPhaseForTest,
    ) {
        if let Some(slot) = self
            .maintenance_preparation_pauses
            .lock()
            .get(&(partition.clone(), phase))
            .cloned()
        {
            slot.pause_after(0);
        }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn arm_graph_callback_pause_for_test(
        &self,
        partition: &VectorPartitionRef,
        _phase: VectorGraphCallbackPhaseForTest,
    ) -> VectorLifecyclePauseHandle {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            let state = self
                .partition_state(&partition.index, &partition.partition_key)
                .expect("an armed graph callback pause names an existing partition");
            let slot = state
                .graph_callback_pause
                .lock()
                .get_or_insert_with(|| Arc::new(MaintenanceProgressPauseSlot::new()))
                .clone();
            let generation = slot.arm(0);
            VectorLifecyclePauseHandle { slot, generation }
        })
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn try_generation_publication_lock_for_test(&self, partition: &VectorPartitionRef) -> bool {
        self.try_partition_state(&partition.index, &partition.partition_key)
            .is_some_and(|state| state.generation_publication.try_lock().is_some())
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn try_partition_maintenance_lock_for_test(&self, partition: &VectorPartitionRef) -> bool {
        self.try_partition_state(&partition.index, &partition.partition_key)
            .is_some_and(|state| state.maintenance.try_lock().is_some())
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn try_bulk_maintenance_lock_for_test(&self) -> bool {
        self.bulk_gate.try_write().is_some()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn arm_journal_truncation_fault_for_test(
        &self,
        partition: &VectorPartitionRef,
        phase: VectorJournalTruncationPhaseForTest,
    ) -> VectorJournalTruncationFaultHandle {
        let slot = self
            .journal_truncation_faults
            .lock()
            .entry((partition.clone(), phase))
            .or_insert_with(|| {
                Arc::new(OneShotFaultSlot {
                    state: Mutex::new(OneShotFaultState::default()),
                })
            })
            .clone();
        let generation = slot.arm();
        VectorJournalTruncationFaultHandle { slot, generation }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn take_journal_truncation_fault_for_test(
        &self,
        partition: &VectorPartitionRef,
        phase: VectorJournalTruncationPhaseForTest,
    ) -> bool {
        self.journal_truncation_faults
            .lock()
            .get(&(partition.clone(), phase))
            .is_some_and(|slot| slot.take())
    }

    pub fn prepare_received_schema_publication(
        schemas: Vec<(VectorIndexRef, usize, VectorQuantization)>,
        entries: Vec<VectorEntry>,
    ) -> PreparedVectorPublication {
        let mut registry = HashMap::<VectorIndexRef, Arc<VectorColumnState>>::new();
        for (index, dimension, quantization) in schemas {
            registry.insert(
                index,
                Arc::new(VectorColumnState::new(VectorIndexLayout::unpartitioned(
                    dimension,
                    quantization,
                ))),
            );
        }
        for entry in entries {
            let index = entry.index.clone();
            let column = registry.entry(index).or_insert_with(|| {
                Arc::new(VectorColumnState::new(VectorIndexLayout::unpartitioned(
                    entry.vector.len(),
                    VectorQuantization::F32,
                )))
            });
            let key = VectorPartitionKey::unpartitioned();
            let state = column
                .partition_state(&key)
                .expect("unpartitioned declaration always owns its empty-key state");
            let stored = state.stored_entry(entry);
            state.push_entry(stored.clone());
            column.note_live_insert(&key, &stored);
        }
        PreparedVectorPublication::new(registry)
    }

    /// Key-aware received-schema staging. Unlike the legacy wire-shaped
    /// method above, declarations carry their full layout and entries carry
    /// their already-derived typed key.
    pub fn prepare_partitioned_received_schema_publication(
        schemas: Vec<(VectorIndexRef, VectorIndexLayout)>,
        entries: Vec<PartitionedVectorEntry>,
    ) -> Result<PreparedVectorPublication> {
        let mut registry = HashMap::<VectorIndexRef, Arc<VectorColumnState>>::new();
        for (index, layout) in schemas {
            Self::validate_layout(&index, &layout)?;
            registry.insert(index, Arc::new(VectorColumnState::new(layout)));
        }

        let mut requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
        let mut live_requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
        for entry in &entries {
            let index = &entry.entry.index;
            let column = registry
                .get(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            Self::validate_partition_key(index, &column.layout(), &entry.partition_key)?;
            requested
                .entry(index.clone())
                .or_default()
                .insert(entry.partition_key.clone());
            if entry.entry.deleted_tx.is_none() {
                live_requested
                    .entry(index.clone())
                    .or_default()
                    .insert(entry.partition_key.clone());
            }
        }
        for (index, keys) in &live_requested {
            let column = registry
                .get(index)
                .expect("requested prepared keys were validated against the registry");
            Self::validate_partition_cap(index, column, keys)?;
        }
        for (index, keys) in requested {
            let column = registry
                .get(&index)
                .expect("requested prepared keys were validated against the registry");
            let layout = column.layout();
            let mut partitions = column.partitions.write();
            for key in keys {
                partitions.entry(key).or_insert_with(|| {
                    Arc::new(IndexState::new(layout.dimension, layout.quantization))
                });
            }
        }
        for entry in entries {
            let index = entry.entry.index.clone();
            let column = registry
                .get(&index)
                .expect("prepared entry index was validated");
            let state = column
                .partition_state(&entry.partition_key)
                .expect("prepared partition was admitted");
            let stored = state.stored_entry(entry.entry);
            state.push_entry(stored.clone());
            column.note_live_insert(&entry.partition_key, &stored);
        }
        for column in registry.values() {
            if !column.layout().is_partitioned() {
                continue;
            }
            for (partition_key, state) in column.partition_snapshots() {
                if state.vector_count() == 0 && state.entry_count() != 0 {
                    column.mark_historical_only(&partition_key);
                }
            }
        }
        Ok(PreparedVectorPublication::new(registry))
    }

    /// Build the exact replacement registry for authoritative purge.  Unlike
    /// normal lazy search construction, this preserves a graph only for an
    /// index that already had one, and builds it before durability even when
    /// the survivor count has crossed below the ordinary lazy threshold.
    pub fn prepare_authoritative_purge_publication(
        schemas: Vec<(VectorIndexRef, usize, VectorQuantization)>,
        entries: Vec<VectorEntry>,
        materialized_indexes: Vec<VectorIndexRef>,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Result<PreparedVectorPublication> {
        let publication = Self::prepare_received_schema_publication(schemas, entries);
        for index in materialized_indexes {
            let Some(column) = publication.registry.get(&index) else {
                continue;
            };
            let Some(state) = column.partition_state(&VectorPartitionKey::unpartitioned()) else {
                continue;
            };
            let policy = column.layout().resolve_policy(state.vector_count(), 1);
            Self::build_prepared_hnsw(
                &index,
                &state,
                policy,
                accountant.clone(),
                || {},
                &mut || Ok(()),
            )?;
        }
        Ok(publication)
    }

    pub fn prepare_partitioned_authoritative_purge_publication(
        &self,
        schemas: Vec<(VectorIndexRef, VectorIndexLayout)>,
        entries: Vec<PartitionedVectorEntry>,
        affected_partitions: Vec<VectorPartitionRef>,
        materialized_partitions: Vec<VectorPartitionRef>,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Result<PreparedVectorPublication> {
        let affected_partitions = affected_partitions.into_iter().collect::<HashSet<_>>();
        let mut registry = HashMap::<VectorIndexRef, Arc<VectorColumnState>>::new();
        let mut requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
        let mut live_requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
        let mut survivor_counts = HashMap::<VectorPartitionRef, usize>::new();

        for (index, layout) in schemas {
            Self::validate_layout(&index, &layout)?;
            if !layout.is_partitioned() {
                requested
                    .entry(index.clone())
                    .or_default()
                    .insert(VectorPartitionKey::unpartitioned());
            }
            registry.insert(index, Arc::new(VectorColumnState::new(layout)));
        }

        for entry in &entries {
            let index = &entry.entry.index;
            let column = registry
                .get(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            Self::validate_partition_key(index, &column.layout(), &entry.partition_key)?;
            requested
                .entry(index.clone())
                .or_default()
                .insert(entry.partition_key.clone());
            let partition = VectorPartitionRef::new(index.clone(), entry.partition_key.clone());
            *survivor_counts.entry(partition).or_default() += 1;
            if entry.entry.deleted_tx.is_none() {
                live_requested
                    .entry(index.clone())
                    .or_default()
                    .insert(entry.partition_key.clone());
            }
        }
        // `entries` contains bodies that are resident for this operation.
        // An unrelated dormant partition intentionally contributes no body,
        // but it still belongs in the replacement registry unchanged. Carry
        // every unaffected state by handle and use its lightweight directory
        // for the exact survivor count and cap calculation.
        self.with_bulk_read(|| -> Result<()> {
            let current_registry = self.registry.read();
            for (index, current_column) in current_registry.iter() {
                let Some(replacement_column) = registry.get(index) else {
                    continue;
                };
                if current_column.layout() != replacement_column.layout() {
                    return Err(Error::Other(format!(
                        "authoritative purge vector layout changed while preparing {}.{}",
                        index.table, index.column
                    )));
                }
                for (partition_key, state) in current_column.partition_snapshots() {
                    let partition = VectorPartitionRef::new(index.clone(), partition_key.clone());
                    if affected_partitions.contains(&partition) {
                        continue;
                    }
                    requested
                        .entry(index.clone())
                        .or_default()
                        .insert(partition_key.clone());
                    survivor_counts.insert(partition, state.entry_count());
                    if state.vector_count() != 0 {
                        live_requested
                            .entry(index.clone())
                            .or_default()
                            .insert(partition_key);
                    }
                }
            }
            Ok(())
        })?;
        for (index, keys) in &live_requested {
            let column = registry
                .get(index)
                .expect("requested prepared keys were validated against the registry");
            Self::validate_partition_cap(index, column, keys)?;
        }

        let mut preserved_partitions = HashSet::<VectorPartitionRef>::new();
        let mut preserved_states = HashSet::<usize>::new();
        self.with_bulk_read(|| -> Result<()> {
            let current_registry = self.registry.read();
            for (index, keys) in &requested {
                let column = registry
                    .get(index)
                    .expect("requested purge keys were validated against the new registry");
                let current_column = current_registry.get(index);
                if let Some(current_column) = current_column
                    && current_column.layout() != column.layout()
                {
                    return Err(Error::Other(format!(
                        "authoritative purge vector layout changed while preparing {}.{}",
                        index.table, index.column
                    )));
                }
                let layout = column.layout();
                let mut partitions = column.partitions.write();
                for key in keys {
                    let partition = VectorPartitionRef::new(index.clone(), key.clone());
                    let survivor_count = survivor_counts.get(&partition).copied().unwrap_or(0);
                    let preserved = (!affected_partitions.contains(&partition))
                        .then(|| current_column.and_then(|column| column.partition_state(key)))
                        .flatten();
                    if let Some(state) = preserved {
                        if state.entry_count() != survivor_count {
                            return Err(Error::Other(format!(
                                "authoritative purge affected-partition set is incomplete for {}.{}",
                                index.table, index.column
                            )));
                        }
                        partitions.insert(key.clone(), state.clone());
                        if current_column
                            .is_some_and(|column| column.partition_is_historical_only(key))
                        {
                            column.mark_historical_only(key);
                        }
                        preserved_states.insert(Arc::as_ptr(&state) as usize);
                        preserved_partitions.insert(partition);
                    } else {
                        partitions.entry(key.clone()).or_insert_with(|| {
                            Arc::new(IndexState::new(layout.dimension, layout.quantization))
                        });
                    }
                }
            }
            Ok(())
        })?;

        for entry in entries {
            let index = entry.entry.index.clone();
            let partition = VectorPartitionRef::new(index.clone(), entry.partition_key.clone());
            if preserved_partitions.contains(&partition) {
                continue;
            }
            let column = registry
                .get(&index)
                .expect("prepared purge entry index was validated");
            let state = column
                .partition_state(&entry.partition_key)
                .expect("prepared purge partition was admitted");
            state.push_entry(state.stored_entry(entry.entry));
        }
        for (index, column) in &registry {
            column.rebuild_current_rows();
            if !column.layout().is_partitioned() {
                continue;
            }
            for (partition_key, state) in column.partition_snapshots() {
                let partition = VectorPartitionRef::new(index.clone(), partition_key.clone());
                if !preserved_partitions.contains(&partition)
                    && state.vector_count() == 0
                    && state.entry_count() != 0
                {
                    column.mark_historical_only(&partition_key);
                }
            }
        }

        let publication = PreparedVectorPublication {
            registry,
            preserved_states,
        };
        for partition in materialized_partitions {
            if !affected_partitions.contains(&partition) {
                continue;
            }
            let Some(column) = publication.registry.get(&partition.index) else {
                continue;
            };
            let Some(state) = column.partition_state(&partition.partition_key) else {
                continue;
            };
            let policy = column.layout().resolve_policy(state.vector_count(), 1);
            Self::build_prepared_hnsw(
                &partition.index,
                &state,
                policy,
                accountant.clone(),
                || {},
                &mut || Ok(()),
            )?;
        }
        Ok(publication)
    }

    fn build_prepared_hnsw(
        index: &VectorIndexRef,
        state: &Arc<IndexState>,
        policy: ResolvedVectorPolicy,
        accountant: Arc<dyn MemoryBudget>,
        after_reservation: impl FnOnce(),
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        let entry_count = state.vector_count();
        let final_bytes = HnswIndex::estimated_resident_bytes_with_m(
            entry_count,
            state.dimension(),
            state.quantization(),
            policy.hnsw_m,
        );
        let reservation_bytes = crate::mem::estimate_hnsw_build_reservation(
            entry_count,
            state.dimension(),
            state.quantization(),
            policy,
        );
        accountant.try_allocate_for(
            reservation_bytes,
            "vector_index",
            &format!("prepare_authoritative_purge_hnsw@{}.{}", index.table, index.column),
            "Reduce vector volume or raise MEMORY_LIMIT before authoritative purge can rebuild its materialized HNSW index.",
        )?;
        let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            after_reservation();
            state.with_entries(|entries| {
                HnswIndex::new_with_policy_and_progress(
                    entries,
                    state.dimension(),
                    state.quantization(),
                    policy,
                    |done| {
                        checkpoint()?;
                        state.set_maintenance_vectors_done(done);
                        checkpoint()
                    },
                )
            })
        }));
        let hnsw = match built {
            Ok(Ok(hnsw)) => hnsw,
            Ok(Err(error)) => {
                accountant.release(reservation_bytes);
                return Err(error);
            }
            Err(_) => {
                accountant.release(reservation_bytes);
                return Err(Error::Other(format!(
                    "authoritative purge HNSW build panicked for {}.{}",
                    index.table, index.column
                )));
            }
        };
        accountant.release(reservation_bytes.saturating_sub(final_bytes));
        state.set_hnsw(Some(hnsw), final_bytes);
        state.set_hnsw_bytes_with_accountant(final_bytes, accountant);
        state.set_serving_policy(policy);
        state.clear_route_quarantine();
        #[cfg(any(test, feature = "test-seams"))]
        state.note_passive_activity(|activity| {
            activity.hnsw_builds.fetch_add(1, Ordering::SeqCst);
        });
        Ok(())
    }

    fn in_memory_maintenance_request(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        state: &Arc<IndexState>,
        layout: &VectorIndexLayout,
    ) -> Option<(ResolvedVectorPolicy, &'static str)> {
        let info = Self::partition_info_snapshot(index, partition_key, state, layout);
        let need = layout.maintenance_need(&info);
        if let Some((sample_tx, _)) = state.sampled_maintenance_frontier() {
            let need = need?;
            let sampled_count = state
                .visibility_counts
                .read()
                .count(SnapshotId(sample_tx.0));
            let policy = layout.resolve_policy(sampled_count, 1);
            return Some((policy, need.build_reason()));
        }
        if state.vector_count() == 0 {
            return None;
        }
        need.map(|need| (info.desired_policy, need.build_reason()))
    }

    #[allow(clippy::too_many_arguments)]
    fn run_sampled_hnsw_maintenance(
        &self,
        partition: &VectorPartitionRef,
        accountant: Arc<dyn MemoryBudget>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
        mut after_reservation: impl FnMut(),
    ) -> Result<bool> {
        // Short partition gates freeze and later revalidate one sampled
        // frontier. Construction itself holds only the detached candidate and
        // graph-lifecycle locks, so unrelated structural commits can publish
        // while foreground writes extend this partition's fresh tail.
        let _build_lifecycle = self.hnsw_build_gate.read();
        {
            let Some(column) = self.try_column(&partition.index) else {
                return Ok(false);
            };
            let Some(state) = column.partition_state(&partition.partition_key) else {
                return Ok(false);
            };
            let _builder = state.maintenance_build.lock();

            let prepared = self.with_partition_maintenance(
                &partition.index,
                &partition.partition_key,
                || -> Result<_> {
                    let Some(current_column) = self.try_column(&partition.index) else {
                        return Ok(None);
                    };
                    let Some(current_state) =
                        current_column.partition_state(&partition.partition_key)
                    else {
                        return Ok(None);
                    };
                    if !Arc::ptr_eq(&current_state, &state) {
                        return Ok(None);
                    }
                    let layout = current_column.layout();
                    let Some((policy, reason)) = Self::in_memory_maintenance_request(
                        &partition.index,
                        &partition.partition_key,
                        &state,
                        &layout,
                    ) else {
                        return Ok(None);
                    };
                    checkpoint()?;
                    let Some((sample_tx, sample_lsn)) = state
                        .begin_sampled_hnsw_maintenance(accountant.clone(), policy)?
                    else {
                        return Ok(None);
                    };
                    let rollback = SampledMaintenanceRollback {
                        state: state.clone(),
                        armed: true,
                    };
                    let vector_count = state
                        .visibility_counts
                        .read()
                        .count(SnapshotId(sample_tx.0));
                    let final_bytes = HnswIndex::estimated_resident_bytes_with_m(
                        vector_count,
                        state.dimension(),
                        state.quantization(),
                        policy.hnsw_m,
                    );
                    let reservation_bytes = crate::mem::estimate_hnsw_build_reservation(
                        vector_count,
                        state.dimension(),
                        state.quantization(),
                        policy,
                    );
                    let workspace_bytes = reservation_bytes.checked_sub(final_bytes).ok_or_else(|| {
                        Error::Other(format!(
                            "maintenance build reservation is smaller than its retained graph for {}.{}",
                            partition.index.table, partition.index.column
                        ))
                    })?;
                    // The fresh tail and detached candidate coexist during
                    // construction, so each owner keeps its complete charge.
                    accountant.try_allocate_for(
                        reservation_bytes,
                        "vector_index",
                        &format!(
                            "build_hnsw@{}.{}",
                            partition.index.table, partition.index.column
                        ),
                        "Reduce vector volume or raise MEMORY_LIMIT before maintenance rebuilds this materialized HNSW index.",
                    )?;
                    let reservation = RetainedMemoryCharge {
                        bytes: reservation_bytes,
                        accountant: accountant.clone(),
                    };
                    let mut entries = Vec::new();
                    entries.try_reserve_exact(vector_count).map_err(|_| {
                        Error::Other(format!(
                            "maintenance vector snapshot allocation failed for {}.{}",
                            partition.index.table, partition.index.column
                        ))
                    })?;
                    state.with_entries(|source| {
                        entries.extend(
                            source
                                .iter()
                                .filter(|entry| {
                                    entry.created_tx <= sample_tx
                                        && entry
                                            .deleted_tx
                                            .is_none_or(|deleted| deleted > sample_tx)
                                })
                                .cloned(),
                        );
                    });
                    if entries.len() != vector_count {
                        return Err(Error::Other(format!(
                            "maintenance vector snapshot disagrees with live count for {}.{}",
                            partition.index.table, partition.index.column
                        )));
                    }
                    checkpoint()?;
                    let progress = state.begin_maintenance_progress_at(
                        match reason {
                            "initial_build" | "policy_replacement" => "building",
                            "new_changes" => "replaying",
                            _ => "compacting",
                        },
                        if reason == "policy_replacement" {
                            "new_changes"
                        } else {
                            reason
                        },
                        vector_count,
                        sample_tx,
                    );
                    Ok(Some((
                        policy,
                        entries,
                        sample_tx,
                        sample_lsn,
                        final_bytes,
                        workspace_bytes,
                        reservation,
                        progress,
                        rollback,
                    )))
                },
            )?;
            let Some((
                policy,
                entries,
                sample_tx,
                sample_lsn,
                final_bytes,
                workspace_bytes,
                mut reservation,
                progress,
                mut rollback,
            )) = prepared
            else {
                return Ok(false);
            };

            let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                after_reservation();
                HnswIndex::new_with_policy_and_progress(
                    &entries,
                    state.dimension(),
                    state.quantization(),
                    policy,
                    |done| {
                        checkpoint()?;
                        progress.advance(done);
                        checkpoint()
                    },
                )
            }));
            let hnsw = match built {
                Ok(Ok(hnsw)) => hnsw,
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Err(Error::Other(format!(
                        "maintenance HNSW build panicked for {}.{}",
                        partition.index.table, partition.index.column
                    )));
                }
            };

            let published =
                self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
                    let Some(current_column) = self.try_column(&partition.index) else {
                        return false;
                    };
                    let Some(current_state) =
                        current_column.partition_state(&partition.partition_key)
                    else {
                        return false;
                    };
                    if !Arc::ptr_eq(&current_state, &state) {
                        return false;
                    }
                    let layout = current_column.layout();
                    let current_policy = layout.resolve_policy(entries.len(), 1);
                    if current_policy != policy {
                        return false;
                    }
                    if state.sampled_maintenance_frontier() != Some((sample_tx, sample_lsn)) {
                        return false;
                    }
                    state.publish_sampled_hnsw(
                        hnsw,
                        final_bytes,
                        accountant.clone(),
                        policy,
                        sample_tx,
                        sample_lsn,
                    );
                    rollback.armed = false;
                    reservation.bytes = workspace_bytes;
                    #[cfg(any(test, feature = "test-seams"))]
                    state.note_passive_activity(|activity| {
                        activity.hnsw_builds.fetch_add(1, Ordering::SeqCst);
                    });
                    true
                });
            Ok(published)
        }
    }

    /// Advance every needy partition in one finite sampled cycle. Queries do
    /// not call this method; caller-driven and engine-owned maintenance share
    /// the same cycle.
    ///
    /// The engine calls `retire_snapshot_free_partition_states` first while
    /// holding its commit and snapshot-removal guards. Keeping that separate
    /// makes the proof boundary explicit: this graph-build loop has no
    /// authority to decide that an old snapshot has released a state.
    pub fn run_hnsw_maintenance_cycle(
        &self,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Result<VectorMaintenanceReport> {
        self.run_hnsw_maintenance_cycle_with(accountant, &mut || Ok(()))
    }

    #[doc(hidden)]
    pub fn run_hnsw_maintenance_cycle_with(
        &self,
        accountant: Arc<dyn MemoryBudget>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<VectorMaintenanceReport> {
        let indexes = Self::sorted_refs(self.registry.read().keys().cloned());
        let mut pending = Vec::new();
        for index in &indexes {
            if let Some(column) = self.try_column(index) {
                for (key, state) in column.partition_snapshots() {
                    let layout = column.layout();
                    if Self::in_memory_maintenance_request(index, &key, &state, &layout).is_some() {
                        pending.push(VectorPartitionRef::new(index.clone(), key));
                    }
                }
            }
        }
        // A wake advances the whole finite sampled backlog. A partition with
        // no complete route goes first so a large replacement backlog cannot
        // leave a cold partition unavailable for another full sweep.
        pending.sort_by_key(|partition| {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .is_some_and(|state| {
                    state.serving_policy().is_some() || state.route_quarantine().is_some()
                })
        });

        let mut built_partitions = 0usize;
        let mut built_indexes = HashSet::new();
        let mut first_failure_details = None;
        for partition in pending {
            let did_build = self.run_sampled_hnsw_maintenance(
                &partition,
                accountant.clone(),
                checkpoint,
                || {
                    #[cfg(feature = "test-seams")]
                    self.pause_registry
                        .maybe_pause(&partition.index, crate::test_seam::PauseWindow::Build);
                },
            );
            let did_build = match did_build {
                Ok(did_build) => did_build,
                Err(error @ Error::ReadCancelled) => return Err(error),
                Err(error) => {
                    let details = VectorMaintenanceFailureDetails::from_error(&error);
                    if let Some(state) =
                        self.try_partition_state(&partition.index, &partition.partition_key)
                    {
                        state.record_maintenance_failure(details);
                    }
                    first_failure_details.get_or_insert(details);
                    continue;
                }
            };
            if did_build {
                built_partitions = built_partitions.saturating_add(1);
                built_indexes.insert(partition.index.clone());
            }
        }

        let mut report = self.hnsw_maintenance_status();
        report.built_partitions = built_partitions;
        report.built_indexes = built_indexes.len();
        report.first_failure = first_failure_details.map(|details| details.failure);
        report.first_failure_details = first_failure_details;
        Ok(report)
    }

    /// Drive the production maintenance body for one index without making a
    /// query the owner of construction. This is test-only because production
    /// scheduling remains the finite store-wide maintenance cycle above.
    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub fn run_hnsw_maintenance_for_index_for_test(
        &self,
        index: &VectorIndexRef,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Result<bool> {
        self.try_column(index)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let mut built = false;
        let partitions = self
            .try_column(index)
            .expect("index existence was checked")
            .partition_snapshots();
        for (partition_key, _) in partitions {
            let partition = VectorPartitionRef::new(index.clone(), partition_key);
            let did_build = self.run_sampled_hnsw_maintenance(
                &partition,
                accountant.clone(),
                &mut || Ok(()),
                || {
                    self.pause_registry
                        .maybe_pause(index, crate::test_seam::PauseWindow::Build);
                },
            )?;
            built |= did_build;
        }
        Ok(built)
    }

    /// Observe maintained-route readiness without constructing, loading, or
    /// mutating a graph. File-backed maintenance uses this after its durable
    /// catalog publication so the report and every reader share one
    /// publication point.
    pub fn hnsw_maintenance_status(&self) -> VectorMaintenanceReport {
        self.with_bulk_read(|| {
            let mut report = VectorMaintenanceReport::default();
            for column in self.registry.read().values() {
                let mut index_nonempty = false;
                let mut index_remaining = false;
                for (_, state) in column.maintained_partition_snapshots() {
                    if state.vector_count() == 0 {
                        continue;
                    }
                    index_nonempty = true;
                    report.nonempty_partitions = report.nonempty_partitions.saturating_add(1);
                    if state.has_complete_hnsw_route() {
                        report.ready_partitions = report.ready_partitions.saturating_add(1);
                    } else {
                        index_remaining = true;
                        report.remaining_partitions = report.remaining_partitions.saturating_add(1);
                    }
                }
                if index_nonempty {
                    report.nonempty_indexes = report.nonempty_indexes.saturating_add(1);
                    if index_remaining {
                        report.remaining_indexes = report.remaining_indexes.saturating_add(1);
                    } else {
                        report.ready_indexes = report.ready_indexes.saturating_add(1);
                    }
                }
            }
            report
        })
    }

    /// Cheap read-only pre-check for
    /// [`Self::retire_snapshot_free_partition_states`] and
    /// [`IndexState::reclaim_snapshot_free_graph_generations`]: whether any
    /// partitioned column currently holds state either call could act on --
    /// an empty state, a partition with no live vector (a candidate,
    /// regardless of whether a registered snapshot ultimately still needs
    /// it), or a sealed graph generation already queued for release. This
    /// deliberately answers the same question those calls answer, minus the
    /// registered-snapshot "needed" filter, which only the guarded pass may
    /// evaluate (the sample must be taken atomically with beginning the
    /// pass). A conservative superset: `false` here means neither call would
    /// do anything, so the caller may skip beginning the guarded removal
    /// pass and the read-parking it causes; `true` does not guarantee either
    /// call will actually retire something.
    #[doc(hidden)]
    pub fn has_snapshot_free_retirement_candidate(&self) -> bool {
        let indexes = Self::sorted_refs(self.registry.read().keys().cloned());
        for index in &indexes {
            let Some(column) = self.try_column(index) else {
                continue;
            };
            if !column.layout().is_partitioned() {
                continue;
            }
            for (partition_key, state) in column.partition_snapshots() {
                if state.entry_count() == 0 {
                    return true;
                }
                if !column.partition_is_historical_only(&partition_key) && state.vector_count() == 0
                {
                    return true;
                }
                if state.has_retired_sealed_generations() {
                    return true;
                }
            }
        }
        false
    }

    /// Retired graphs can be reclaimed independently of route retirement.
    #[doc(hidden)]
    pub fn has_retired_graph_generations(&self) -> bool {
        self.registry.read().values().any(|column| {
            column
                .partitions
                .read()
                .values()
                .any(|state| state.has_retired_sealed_generations())
        })
    }

    /// Retire maintained state for partitioned sources that have no live
    /// vector and are invisible to every snapshot captured by the engine's
    /// active removal guard. Raw versions stay stored for later historical
    /// EXACT reads; only the graph and live-plus-retained partition identity
    /// are released.
    ///
    /// The caller must hold the transaction commit lock and the matching
    /// snapshot-removal guard for this entire call.
    #[doc(hidden)]
    pub fn retire_snapshot_free_partition_states(
        &self,
        registered_snapshots: &[SnapshotId],
        accountant: &dyn MemoryBudget,
    ) -> usize {
        self.with_bulk_maintenance(|| {
            let mut retired = 0usize;
            let indexes = Self::sorted_refs(self.registry.read().keys().cloned());
            let sampled = self.directory_bytes_before_reclamation(&indexes);
            for index in &indexes {
                let Some(column) = self.try_column(index) else {
                    continue;
                };
                if !column.layout().is_partitioned() {
                    continue;
                }
                let mut has_empty_state = false;
                for (partition_key, state) in column.partition_snapshots() {
                    if state.entry_count() == 0 {
                        // A state with no raw versions cannot answer any
                        // historical read. Do not leave its empty active
                        // identity consuming an inspection slot after the
                        // source history has already been pruned.
                        state.clear_hnsw(accountant);
                        has_empty_state = true;
                        continue;
                    }
                    if column.partition_is_historical_only(&partition_key)
                        || state.vector_count() != 0
                    {
                        continue;
                    }
                    let needed = registered_snapshots
                        .iter()
                        .any(|snapshot| state.directory_visible_entry_count(*snapshot, None) != 0);
                    if needed {
                        continue;
                    }
                    state.clear_hnsw(accountant);
                    column.mark_historical_only(&partition_key);
                    retired = retired.saturating_add(1);
                }
                if has_empty_state {
                    column.remove_empty_partitioned_states();
                }
            }
            self.reconcile_directory_accounting_for_indexes(&sampled);
            retired
        })
    }

    /// Replace an already prepared registry after durable received-schema
    /// publication.  Any outgoing HNSW graph is retired here, after the
    /// replacement is visible; its accountant release is paired with the
    /// engine's private staged-memory settlement.
    pub fn publish_prepared_received_schema(
        &self,
        publication: PreparedVectorPublication,
        accountant: Arc<dyn MemoryBudget>,
    ) {
        let _bulk = self.bulk_gate.write();
        let (registry, preserved_states) = publication.into_parts();
        for (index, column) in &registry {
            if column.directory_accounting.lock().is_none() {
                let bytes = column.directory_retained_bytes(index);
                column.install_directory_accounting(bytes, accountant.clone());
            }
            for (_, state) in column.partition_snapshots() {
                if state.raw_payload_bytes() != 0 {
                    state.attach_raw_accounting(accountant.clone());
                }
            }
        }
        let old_registry = std::mem::replace(&mut *self.registry.write(), registry);
        for column in old_registry.values() {
            column.clear_hnsw_except(&*accountant, &preserved_states);
        }
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn pause_registry(&self) -> &crate::test_seam::PauseRegistry {
        &self.pause_registry
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn graph_candidate_caps(&self) -> &crate::test_seam::GraphCandidateCapRegistry {
        &self.graph_candidate_caps
    }

    pub fn register_index(
        &self,
        index: VectorIndexRef,
        dimension: usize,
        quantization: VectorQuantization,
    ) {
        self.with_bulk_read(|| {
            let column = {
                let mut registry = self.registry.write();
                registry
                    .entry(index.clone())
                    .or_insert_with(|| {
                        Arc::new(VectorColumnState::new(VectorIndexLayout::unpartitioned(
                            dimension,
                            quantization,
                        )))
                    })
                    .clone()
            };
            let key = VectorPartitionKey::unpartitioned();
            if let Some(state) = column.partition_state(&key) {
                let _index_guard = state.maintenance.lock();
                let _stack_guard = MaintenanceStackGuard::new(self.id, &index, &key);
            }
        });
    }

    /// Register or update a manager-resolved declaration. Policy-only changes
    /// retain every partition; a physical-layout change is accepted only when
    /// the column has no retained vector entry.
    pub fn register_index_with_layout(
        &self,
        index: VectorIndexRef,
        layout: VectorIndexLayout,
    ) -> Result<()> {
        Self::validate_layout(&index, &layout)?;
        let changed_column = self.with_bulk_maintenance(|| {
            let mut registry = self.registry.write();
            let Some(existing) = registry.get(&index).cloned() else {
                let column = Arc::new(VectorColumnState::new(layout.clone()));
                if let Some(accountant) = self.memory_accountant.read().clone() {
                    let bytes = column.directory_retained_bytes(&index);
                    accountant.try_allocate_for(
                        bytes,
                        "vector_index",
                        "register_vector_directory",
                        "Reduce vector declaration metadata or raise MEMORY_LIMIT before creating the index.",
                    )?;
                    column.install_directory_accounting(bytes, accountant);
                }
                registry.insert(index.clone(), column);
                return Ok(None);
            };
            let current = existing.layout();
            if current == layout {
                return Ok(None);
            }
            if current.has_same_storage_shape(&layout) {
                if let Some(max_partitions) = layout.max_partitions
                    && existing.live_or_retained_partition_count() > max_partitions as usize
                {
                    return Err(Error::VectorPartitionLimitExceeded {
                        index: index.clone(),
                        max_partitions,
                    });
                }
                *existing.layout.write() = layout.clone();
                return Ok(Some(existing));
            }
            if existing.total_entry_count() != 0 {
                return Err(Error::Other(format!(
                    "cannot reconfigure nonempty vector index {}.{}",
                    index.table, index.column
                )));
            }
            let column = Arc::new(VectorColumnState::new(layout.clone()));
            if let Some(accountant) = self.memory_accountant.read().clone() {
                let bytes = column.directory_retained_bytes(&index);
                accountant.try_allocate_for(
                    bytes,
                    "vector_index",
                    "replace_vector_directory",
                    "Reduce vector declaration metadata or raise MEMORY_LIMIT before replacing the index.",
                )?;
                column.install_directory_accounting(bytes, accountant);
            }
            registry.insert(index.clone(), column);
            Ok(None)
        })?;

        // The declaration itself is already visible and the exclusive store
        // gate is released. Foreground writes share the read side used below.
        // Only the few partitions carrying an old failure need a
        // directory-derived recheck, and a concurrent retry cannot have its
        // newer mark cleared by this one.
        let Some(changed_column) = changed_column else {
            return Ok(());
        };
        self.with_bulk_read(|| {
            let still_current = self.registry.read().get(&index).is_some_and(|column| {
                Arc::ptr_eq(column, &changed_column) && column.layout() == layout
            });
            if !still_current {
                return;
            }
            for (partition_key, state) in changed_column.maintained_partition_snapshots() {
                self.with_partition_maintenance(&index, &partition_key, || {
                    let Some(failure) = state.maintenance_failure_details() else {
                        return;
                    };
                    debug_assert!(
                        !HELD_BULK_WRITE_LOCKS
                            .with(|held| held.borrow().contains(&self.id)),
                        "declaration failure recheck cannot inspect raw vector directories under the store-wide write gate"
                    );
                    let info =
                        Self::partition_info_snapshot(&index, &partition_key, &state, &layout);
                    if layout.maintenance_need(&info).is_none() {
                        state.clear_maintenance_failure_if(failure);
                    }
                });
            }
        });
        Ok(())
    }

    fn validate_layout(index: &VectorIndexRef, layout: &VectorIndexLayout) -> Result<()> {
        match (layout.is_partitioned(), layout.max_partitions) {
            (false, None) => Ok(()),
            (true, Some(max_partitions)) => {
                if max_partitions == 0 {
                    Err(Error::Other(format!(
                        "partitioned vector index {}.{} requires a positive effective MAX_PARTITIONS",
                        index.table, index.column
                    )))
                } else {
                    Ok(())
                }
            }
            (false, Some(_)) => Err(Error::Other(format!(
                "unpartitioned vector index {}.{} cannot carry MAX_PARTITIONS",
                index.table, index.column
            ))),
            (true, None) => Err(Error::Other(format!(
                "partitioned vector index {}.{} requires a positive effective MAX_PARTITIONS",
                index.table, index.column
            ))),
        }
    }

    fn validate_partition_key(
        index: &VectorIndexRef,
        layout: &VectorIndexLayout,
        key: &VectorPartitionKey,
    ) -> Result<()> {
        if key.components().len() != layout.partition_key_columns.len() {
            return Err(Error::Other(format!(
                "vector partition key shape does not match declaration for {}.{}",
                index.table, index.column
            )));
        }
        Ok(())
    }

    fn validate_partition_cap(
        index: &VectorIndexRef,
        column: &VectorColumnState,
        requested: &PartitionKeySet,
    ) -> Result<()> {
        let layout = column.layout();
        let Some(max_partitions) = layout.max_partitions else {
            return Ok(());
        };
        let partitions = column.partitions.read();
        let needs_admission = requested.iter().any(|key| {
            partitions
                .get(key)
                .is_none_or(|state| state.entry_count() == 0)
                || column.partition_is_historical_only(key)
        });
        if !needs_admission {
            return Ok(());
        }
        let occupied_count = partitions
            .iter()
            .filter(|(key, state)| {
                state.entry_count() != 0 && !column.partition_is_historical_only(key)
            })
            .count();
        let new_count = requested
            .iter()
            .filter(|key| {
                partitions
                    .get(*key)
                    .is_none_or(|state| state.entry_count() == 0)
                    || column.partition_is_historical_only(key)
            })
            .count();
        if occupied_count.saturating_add(new_count) > max_partitions as usize {
            return Err(Error::VectorPartitionLimitExceeded {
                index: index.clone(),
                max_partitions,
            });
        }
        Ok(())
    }

    fn validate_partition_batch(
        &self,
        touched: &HashSet<VectorPartitionRef>,
        requested: &HashMap<VectorIndexRef, PartitionKeySet>,
    ) -> Result<bool> {
        for partition in touched {
            let column =
                self.try_column(&partition.index)
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: partition.index.clone(),
                    })?;
            Self::validate_partition_key(
                &partition.index,
                &column.layout(),
                &partition.partition_key,
            )?;
        }
        let mut all_present = true;
        for (index, keys) in requested {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            Self::validate_partition_cap(index, &column, keys)?;
            if keys.iter().any(|key| {
                column.partition_state(key).is_none() || column.partition_is_historical_only(key)
            }) {
                all_present = false;
            }
        }
        Ok(all_present)
    }

    fn project_partitioned_batch(
        &self,
        deletes: &[PartitionedVectorDelete],
        inserts: &[PartitionedVectorEntry],
        moves: &[PartitionedVectorMove],
        reclaimable_partitions: &HashSet<VectorPartitionRef>,
        registered_snapshots: Option<&[SnapshotId]>,
        enforce_partition_cap: bool,
    ) -> Result<PreparedPartitionedVectorBatch> {
        let mut candidate_partitions = HashSet::<VectorPartitionRef>::new();
        for delete in deletes {
            candidate_partitions.insert(delete.partition.clone());
        }
        for insert in inserts {
            candidate_partitions.insert(VectorPartitionRef::new(
                insert.entry.index.clone(),
                insert.partition_key.clone(),
            ));
        }
        for row_move in moves {
            candidate_partitions.insert(VectorPartitionRef::new(
                row_move.index.clone(),
                row_move.source_partition_key.clone(),
            ));
            candidate_partitions.insert(VectorPartitionRef::new(
                row_move.index.clone(),
                row_move.target_partition_key.clone(),
            ));
        }
        candidate_partitions.extend(reclaimable_partitions.iter().cloned());

        for partition in &candidate_partitions {
            let column =
                self.try_column(&partition.index)
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: partition.index.clone(),
                    })?;
            Self::validate_partition_key(
                &partition.index,
                &column.layout(),
                &partition.partition_key,
            )?;
        }

        let candidate_indexes = Self::sorted_refs(
            candidate_partitions
                .iter()
                .map(|partition| partition.index.clone()),
        );
        let mut projected =
            HashMap::<VectorIndexRef, HashMap<VectorPartitionKey, ProjectedPartitionState>>::new();
        for index in candidate_indexes {
            let column = self
                .try_column(&index)
                .expect("candidate vector index was validated");
            let states = projected.entry(index.clone()).or_default();
            if enforce_partition_cap {
                states.extend(
                    column
                        .partition_snapshots()
                        .into_iter()
                        .map(|(key, state)| {
                            let counts_toward_partition_limit =
                                !column.partition_is_historical_only(&key);
                            (
                                key,
                                ProjectedPartitionState::from_entry_count(
                                    state.entry_count(),
                                    counts_toward_partition_limit,
                                ),
                            )
                        }),
                );
            }
            for partition in candidate_partitions
                .iter()
                .filter(|partition| partition.index == index)
            {
                if let Some(state) = column.partition_state(&partition.partition_key) {
                    let counts_toward_partition_limit =
                        !column.partition_is_historical_only(&partition.partition_key);
                    states.insert(
                        partition.partition_key.clone(),
                        ProjectedPartitionState::from_state(&state, counts_toward_partition_limit),
                    );
                }
            }
        }

        for delete in deletes {
            if let Some(state) = projected
                .get_mut(&delete.partition.index)
                .and_then(|states| states.get_mut(&delete.partition.partition_key))
            {
                state.delete_live_row(delete.row_id);
            }
        }
        for insert in inserts {
            projected
                .get_mut(&insert.entry.index)
                .expect("insert index was validated")
                .entry(insert.partition_key.clone())
                .or_default()
                .insert(&insert.entry);
        }

        let mut valid_moves = Vec::with_capacity(moves.len());
        for row_move in moves {
            if !row_move.replace_row_version
                && row_move.source_partition_key == row_move.target_partition_key
                && row_move.old_row_id == row_move.new_row_id
            {
                continue;
            }
            let source_is_live = projected
                .get(&row_move.index)
                .and_then(|states| states.get(&row_move.source_partition_key))
                .is_some_and(|state| state.has_movable_row(row_move.old_row_id));
            if !source_is_live {
                continue;
            }
            projected
                .get_mut(&row_move.index)
                .expect("move index was validated")
                .get_mut(&row_move.source_partition_key)
                .expect("live move source has a projected state")
                .delete_live_row(row_move.old_row_id);
            projected
                .get_mut(&row_move.index)
                .expect("move index was validated")
                .entry(row_move.target_partition_key.clone())
                .or_default()
                .insert_moved(row_move.new_row_id);
            valid_moves.push(row_move.clone());
        }

        let mut effective_reclaimable_partitions = reclaimable_partitions.clone();
        if let Some(registered_snapshots) = registered_snapshots {
            // A write may arrive on a different partition from an old empty
            // one. Sweep every projected slot in each touched index before
            // enforcing the cap, so a continuous tail cannot strand a slot
            // after its last snapshot disappears.
            let mut source_candidates = projected
                .iter()
                .flat_map(|(index, states)| {
                    states
                        .keys()
                        .cloned()
                        .map(|key| VectorPartitionRef::new(index.clone(), key))
                })
                .collect::<HashSet<_>>();
            source_candidates.retain(|partition| {
                let Some(column) = self.try_column(&partition.index) else {
                    return false;
                };
                if !column.layout().is_partitioned() {
                    return false;
                }
                let Some(state) = column.partition_state(&partition.partition_key) else {
                    return false;
                };
                let changed_by_batch = candidate_partitions.contains(partition);
                let projected_is_empty = if changed_by_batch {
                    projected
                        .get(&partition.index)
                        .and_then(|states| states.get(&partition.partition_key))
                        .is_some_and(|state| state.live_count() == 0)
                } else {
                    state.vector_count() == 0
                };
                if !projected_is_empty {
                    return false;
                }
                if !changed_by_batch {
                    let layout = column.layout();
                    let info = Self::partition_info_snapshot(
                        &partition.index,
                        &partition.partition_key,
                        &state,
                        &layout,
                    );
                    if info.pending_inserts != 0
                        || info.tombstones != 0
                        || info.quarantine_reason.is_some()
                    {
                        // The cap-time sweep has no authority to discard an
                        // untouched route's pending durability or repair work.
                        return false;
                    }
                }
                !state.raw_directory.read().iter().any(|entry| {
                    registered_snapshots.iter().any(|snapshot| {
                        entry.created_tx.0 <= snapshot.0
                            && entry.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
                    })
                })
            });
            effective_reclaimable_partitions.extend(source_candidates);
        }

        for partition in &effective_reclaimable_partitions {
            let column = self
                .try_column(&partition.index)
                .expect("reclaimable vector index was validated");
            if !column.layout().is_partitioned() {
                return Err(Error::Other(format!(
                    "cannot reclaim the declaration state of unpartitioned vector index {}.{}",
                    partition.index.table, partition.index.column
                )));
            }
            let Some(state) = projected
                .get_mut(&partition.index)
                .and_then(|states| states.get_mut(&partition.partition_key))
            else {
                return Err(Error::Other(format!(
                    "cannot reclaim an absent partition of vector index {}.{}",
                    partition.index.table, partition.index.column
                )));
            };
            if state.live_count() != 0 {
                return Err(Error::Other(format!(
                    "cannot reclaim a partition with live rows in vector index {}.{}",
                    partition.index.table, partition.index.column
                )));
            }
            state.retire_maintained_state();
        }

        let mut touched = HashSet::<VectorPartitionRef>::new();
        let mut requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
        for delete in deletes {
            touched.insert(delete.partition.clone());
        }
        for insert in inserts {
            let partition =
                VectorPartitionRef::new(insert.entry.index.clone(), insert.partition_key.clone());
            touched.insert(partition.clone());
            requested
                .entry(partition.index)
                .or_default()
                .insert(partition.partition_key);
        }
        for row_move in &valid_moves {
            let source = VectorPartitionRef::new(
                row_move.index.clone(),
                row_move.source_partition_key.clone(),
            );
            let target = VectorPartitionRef::new(
                row_move.index.clone(),
                row_move.target_partition_key.clone(),
            );
            touched.insert(source);
            touched.insert(target.clone());
            requested
                .entry(target.index)
                .or_default()
                .insert(target.partition_key);
        }
        touched.extend(effective_reclaimable_partitions.iter().cloned());

        let mut projected_counts = HashMap::<VectorIndexRef, usize>::new();
        for index in Self::sorted_refs(touched.iter().map(|partition| partition.index.clone())) {
            let states = projected
                .get(&index)
                .expect("touched index has projected partition states");
            let count = states
                .values()
                .filter(|state| state.counts_toward_partition_limit && state.total_entries != 0)
                .count();
            let column = self
                .try_column(&index)
                .expect("touched vector index was validated");
            if enforce_partition_cap
                && let Some(max_partitions) = column.layout().max_partitions
                && count > max_partitions as usize
            {
                return Err(Error::VectorPartitionLimitExceeded {
                    index,
                    max_partitions,
                });
            }
            projected_counts.insert(index, count);
        }

        let move_raw_body_bytes = valid_moves.iter().fold(0usize, |bytes, row_move| {
            bytes.saturating_add(
                self.try_partition_state(&row_move.index, &row_move.source_partition_key)
                    .and_then(|state| state.stored_by_row_id(row_move.old_row_id))
                    .map_or(0, |entry| entry.estimated_bytes()),
            )
        });
        let directory_reservation = if let Some(accountant) = self.memory_accountant.read().clone()
        {
            PreparedDirectoryReservation::reserve(
                accountant,
                prepared_directory_growth_upper_bound(
                    inserts,
                    &valid_moves,
                    &effective_reclaimable_partitions,
                    &requested,
                )
                .saturating_add(move_raw_body_bytes),
            )?
        } else {
            PreparedDirectoryReservation::empty()
        };

        let tail_insertions = self.prepare_tail_insertions(inserts, &valid_moves)?;

        Ok(PreparedPartitionedVectorBatch {
            deletes: deletes.to_vec(),
            inserts: inserts.to_vec(),
            moves: valid_moves,
            reclaimable_partitions: effective_reclaimable_partitions,
            touched,
            requested,
            projected_live_or_retained_partitions: projected_counts,
            move_raw_body_bytes,
            directory_reservation,
            tail_insertions,
        })
    }

    fn prepare_tail_insertion(
        &self,
        pending: &mut Vec<PreparedTailInsertion>,
        state: Arc<IndexState>,
        lsn: Lsn,
        prepare_entry: impl FnOnce() -> Result<StoredVectorEntry>,
    ) -> Result<()> {
        let _publication = state.generation_publication.lock();
        if state
            .fresh_tail_replay_frontier_active
            .load(Ordering::SeqCst)
            && lsn.0 != 0
            && lsn.0 <= state.fresh_tail_replayed_through_lsn.load(Ordering::SeqCst)
        {
            return Ok(());
        }
        let Some(lock) = state.hnsw.get() else {
            return Ok(());
        };
        let tail = lock.read();
        let Some(graph) = tail.as_ref() else {
            return Ok(());
        };
        let additions = state
            .fresh_tail_preparations
            .load(Ordering::SeqCst)
            .checked_add(1)
            .ok_or_else(|| Error::Other("HNSW prepared insertion count overflow".to_string()))?;
        let prior_count = graph
            .len()
            .checked_add(additions - 1)
            .ok_or_else(|| Error::Other("HNSW data-id space exhausted".to_string()))?;
        let next_count = prior_count
            .checked_add(1)
            .ok_or_else(|| Error::Other("HNSW data-id space exhausted".to_string()))?;
        let resident_bytes = HnswIndex::estimated_resident_bytes_with_m(
            next_count,
            state.dimension,
            state.quantization,
            graph.policy_values().0,
        )
        .saturating_sub(HnswIndex::estimated_resident_bytes_with_m(
            prior_count,
            state.dimension,
            state.quantization,
            graph.policy_values().0,
        ));
        let (_, ef_construction, _) = graph.policy_values();
        let scratch_bytes =
            crate::quantized::stored_vector_resident_bytes(state.dimension, state.quantization)
                .saturating_mul(3)
                .saturating_add(2 * std::mem::size_of::<PreparedTailInsertion>())
                .saturating_add(ef_construction.saturating_mul(8 * std::mem::size_of::<usize>()));
        let accountant = state
            .hnsw_accountant
            .read()
            .clone()
            .or_else(|| self.memory_accountant.read().clone())
            .unwrap_or_else(crate::memory_budget::unlimited_memory_budget);
        let bytes = resident_bytes.saturating_add(scratch_bytes);
        self.reclaim_idle_graphs_for(bytes, Some(&state));
        accountant.try_allocate_for(bytes, "vector_index", "prepare_hnsw_tail",
            "Reduce vector volume or raise MEMORY_LIMIT before committing another searchable vector.")?;
        let charge = RetainedMemoryCharge { bytes, accountant };
        // Conversion, encoding and id-space validation precede durability and
        // happen only after admission. No vendor insert is attempted here.
        let entry = prepare_entry()?;
        let insertion = graph.prepare_insert(&entry, additions)?;
        if pending.capacity() == 0 {
            pending.reserve_exact(1);
        }
        state.fresh_tail_preparations.fetch_add(1, Ordering::SeqCst);
        pending.push(PreparedTailInsertion {
            state: state.clone(),
            row_id: entry.row_id,
            insertion: Some(insertion),
            charge,
            resident_bytes,
        });
        Ok(())
    }

    fn prepare_tail_insertions(
        &self,
        inserts: &[PartitionedVectorEntry],
        moves: &[PartitionedVectorMove],
    ) -> Result<PreparedTailInsertions> {
        let mut pending = Vec::<PreparedTailInsertion>::new();
        for insert in inserts {
            if insert.entry.deleted_tx.is_none()
                && let Some(state) =
                    self.try_partition_state(&insert.entry.index, &insert.partition_key)
            {
                self.prepare_tail_insertion(&mut pending, state.clone(), insert.entry.lsn, || {
                    Ok(state.stored_entry_ref(&insert.entry))
                })?;
            }
        }
        for row_move in moves {
            if let Some(source) =
                self.try_partition_state(&row_move.index, &row_move.source_partition_key)
                && let Some(target) =
                    self.try_partition_state(&row_move.index, &row_move.target_partition_key)
            {
                self.prepare_tail_insertion(&mut pending, target, Lsn(0), || {
                    let mut entry =
                        source
                            .stored_by_row_id(row_move.old_row_id)
                            .ok_or_else(|| {
                                Error::Other("prepared vector move lost its source".to_string())
                            })?;
                    entry.row_id = row_move.new_row_id;
                    entry.created_tx = row_move.tx;
                    entry.deleted_tx = None;
                    entry.lsn = Lsn(0);
                    Ok(entry)
                })?;
            }
        }
        Ok(PreparedTailInsertions(Arc::new(Mutex::new(pending.into()))))
    }

    /// Validate one keyed batch without mutating the store. The engine calls
    /// this before durability and names only source partitions for which its
    /// snapshot gate proves that no active snapshot needs the maintained
    /// source state. Raw historical vector versions are never discarded here.
    pub fn prepare_partitioned_batch(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        reclaimable_partitions: HashSet<VectorPartitionRef>,
    ) -> Result<PreparedPartitionedVectorBatch> {
        self.with_bulk_read(|| {
            self.ensure_existing_raw_partitions_for_batch(&deletes, &inserts, &moves)?;
            self.project_partitioned_batch(
                &deletes,
                &inserts,
                &moves,
                &reclaimable_partitions,
                None,
                true,
            )
        })
    }

    /// Validate one keyed batch while the engine holds its snapshot-removal
    /// guard. A source state is retired only when the final batch leaves it
    /// with no live vector and none of the snapshots captured by that guard
    /// can see one of its retained versions. Raw historical vectors remain
    /// available for later EXACT reads.
    #[doc(hidden)]
    pub fn prepare_partitioned_batch_with_registered_snapshots(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        registered_snapshots: &[SnapshotId],
    ) -> Result<PreparedPartitionedVectorBatch> {
        self.with_bulk_read(|| {
            self.ensure_existing_raw_partitions_for_batch(&deletes, &inserts, &moves)?;
            self.project_partitioned_batch(
                &deletes,
                &inserts,
                &moves,
                &HashSet::new(),
                Some(registered_snapshots),
                true,
            )
        })
    }

    fn ensure_existing_raw_partitions_for_batch(
        &self,
        deletes: &[PartitionedVectorDelete],
        inserts: &[PartitionedVectorEntry],
        moves: &[PartitionedVectorMove],
    ) -> Result<()> {
        let mut partitions = HashSet::<VectorPartitionRef>::new();
        partitions.extend(deletes.iter().map(|delete| delete.partition.clone()));
        partitions.extend(inserts.iter().map(|insert| {
            VectorPartitionRef::new(insert.entry.index.clone(), insert.partition_key.clone())
        }));
        for row_move in moves {
            partitions.insert(VectorPartitionRef::new(
                row_move.index.clone(),
                row_move.source_partition_key.clone(),
            ));
            partitions.insert(VectorPartitionRef::new(
                row_move.index.clone(),
                row_move.target_partition_key.clone(),
            ));
        }
        for partition in Self::sorted_partition_refs(partitions) {
            if let Some(state) =
                self.try_partition_state(&partition.index, &partition.partition_key)
            {
                state.ensure_raw_vectors_loaded()?;
                if state.hnsw.get().is_none_or(|tail| tail.read().is_none()) {
                    state.preload_dormant_sealed_generation(true)?;
                    state.preload_dormant_sealed_generation(false)?;
                }
            }
        }
        Ok(())
    }

    /// Publish a previously validated keyed batch. Preparation is the last
    /// fallible point: the engine holds its commit gate from preparation
    /// through durable apply and this consumption, so durable success cannot
    /// become a second user-visible vector error.
    pub fn publish_prepared_partitioned_batch(
        &self,
        prepared: PreparedPartitionedVectorBatch,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) -> PreparedPartitionedVectorPublication {
        let mut directory_reservation = prepared.directory_reservation.take();
        if let Some(reservation) = directory_reservation.as_mut() {
            assert!(
                prepared.move_raw_body_bytes <= reservation.bytes,
                "prepared vector reservation covers moved raw bodies"
            );
            reservation.bytes -= prepared.move_raw_body_bytes;
        }
        let touched_indexes = Self::sorted_refs(
            prepared
                .touched
                .iter()
                .map(|partition| partition.index.clone()),
        );
        let mut pending = Some(prepared);
        let published_without_structural_change = self.with_bulk_read(|| {
            let prepared = pending
                .as_ref()
                .expect("prepared vector batch publishes once");
            let all_requested_occupied = prepared.requested.iter().all(|(index, keys)| {
                self.try_column(index).is_some_and(|column| {
                    keys.iter().all(|key| {
                        column
                            .partition_state(key)
                            .is_some_and(|state| state.entry_count() != 0)
                            && !column.partition_is_historical_only(key)
                    })
                })
            });
            if !prepared.reclaimable_partitions.is_empty() || !all_requested_occupied {
                return false;
            }

            let ordered = Self::sorted_partition_refs(prepared.touched.iter().cloned());
            self.with_partition_set_maintenance(&ordered, || {
                let PreparedPartitionedVectorBatch {
                    deletes,
                    inserts,
                    moves,
                    reclaimable_partitions: _,
                    touched: _,
                    requested: _,
                    projected_live_or_retained_partitions: _,
                    move_raw_body_bytes: _,
                    directory_reservation: _,
                    tail_insertions,
                } = pending
                    .take()
                    .expect("prepared vector batch publishes once");
                self.apply_admitted_partitioned_changes(
                    deletes,
                    inserts,
                    moves,
                    lsn,
                    accountant,
                    &tail_insertions,
                );
            });
            // Publish the matching retained-directory charge before releasing
            // the bulk read guard. Reclamation takes the write side of this
            // gate and must never observe the grown directory with its old
            // charge.
            self.settle_directory_accounting_for_indexes(
                &touched_indexes,
                directory_reservation.as_mut(),
            );
            true
        });
        if published_without_structural_change {
            return PreparedPartitionedVectorPublication;
        }

        let prepared = pending.expect("structural vector publication retains its batch");
        self.with_bulk_maintenance(|| {
            let PreparedPartitionedVectorBatch {
                deletes,
                inserts,
                moves,
                reclaimable_partitions,
                touched,
                requested,
                projected_live_or_retained_partitions: _,
                move_raw_body_bytes: _,
                directory_reservation: _,
                tail_insertions,
            } = prepared;
            self.create_partition_states(&requested);
            let ordered = Self::sorted_partition_refs(touched);
            self.with_partition_set_maintenance(&ordered, || {
                self.apply_admitted_partitioned_changes(
                    deletes,
                    inserts,
                    moves,
                    lsn,
                    accountant,
                    &tail_insertions,
                );

                let mut rebuild_indexes = HashSet::<VectorIndexRef>::new();
                for partition in &reclaimable_partitions {
                    let Some(column) = self.try_column(&partition.index) else {
                        continue;
                    };
                    if let Some(state) = column.partition_state(&partition.partition_key) {
                        debug_assert_eq!(state.vector_count(), 0);
                        state.clear_hnsw_with_optional_accountant(accountant);
                        column.mark_historical_only(&partition.partition_key);
                    }
                    rebuild_indexes.insert(partition.index.clone());
                }
                for index in Self::sorted_refs(rebuild_indexes) {
                    if let Some(column) = self.try_column(&index) {
                        column.remove_empty_partitioned_states();
                        column.rebuild_current_rows();
                    }
                }
            });
            self.settle_directory_accounting_for_indexes(
                &touched_indexes,
                directory_reservation.as_mut(),
            );
        });
        PreparedPartitionedVectorPublication
    }

    fn settle_directory_accounting_for_indexes(
        &self,
        indexes: &[VectorIndexRef],
        reservation: Option<&mut RetainedMemoryCharge>,
    ) {
        let Some(reservation) = reservation else {
            return;
        };
        for index in indexes {
            if let Some(column) = self.try_column(index) {
                column.settle_directory_accounting(index, reservation);
            }
        }
    }

    /// The directory figure of every registered index, sampled by a
    /// reclamation pass before it removes anything; paired with
    /// `reconcile_directory_accounting_for_indexes` after the removal.
    fn directory_bytes_before_reclamation(
        &self,
        indexes: &[VectorIndexRef],
    ) -> Vec<(VectorIndexRef, usize)> {
        indexes
            .iter()
            .filter_map(|index| {
                self.try_column(index)
                    .map(|column| (index.clone(), column.directory_retained_bytes(index)))
            })
            .collect()
    }

    fn reconcile_directory_accounting_for_indexes(&self, sampled: &[(VectorIndexRef, usize)]) {
        for (index, bytes_before) in sampled {
            if let Some(column) = self.try_column(index) {
                column.reconcile_directory_accounting_after_reclamation(index, *bytes_before);
            }
        }
    }

    fn create_partition_states(&self, requested: &HashMap<VectorIndexRef, PartitionKeySet>) {
        for (index, keys) in requested {
            let Some(column) = self.try_column(index) else {
                continue;
            };
            let layout = column.layout();
            let mut partitions = column.partitions.write();
            for key in keys {
                partitions.entry(key.clone()).or_insert_with(|| {
                    Arc::new(IndexState::new(layout.dimension, layout.quantization))
                });
            }
            drop(partitions);
            column.reactivate_partitions(keys);
        }
    }

    fn with_admitted_partition_states<R>(
        &self,
        touched: &HashSet<VectorPartitionRef>,
        requested: &HashMap<VectorIndexRef, PartitionKeySet>,
        f: impl FnOnce() -> R,
    ) -> Result<R> {
        let mut f = Some(f);
        let fast = self.with_bulk_read(|| {
            if self.validate_partition_batch(touched, requested)? {
                Ok::<Option<R>, Error>(Some(f
                    .take()
                    .expect("partition operation runs once after validation")(
                )))
            } else {
                Ok::<Option<R>, Error>(None)
            }
        })?;
        if let Some(result) = fast {
            return Ok(result);
        }
        self.with_bulk_maintenance(|| {
            self.validate_partition_batch(touched, requested)?;
            self.create_partition_states(requested);
            Ok(f.take().expect(
                "partition operation runs once after state admission",
            )())
        })
    }

    fn sorted_refs(refs: impl IntoIterator<Item = VectorIndexRef>) -> Vec<VectorIndexRef> {
        let mut refs = refs.into_iter().collect::<Vec<_>>();
        refs.sort_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
        refs.dedup();
        refs
    }

    fn sorted_partition_refs(
        refs: impl IntoIterator<Item = VectorPartitionRef>,
    ) -> Vec<VectorPartitionRef> {
        let mut refs = refs.into_iter().collect::<Vec<_>>();
        refs.sort_by(|a, b| {
            a.index
                .table
                .cmp(&b.index.table)
                .then(a.index.column.cmp(&b.index.column))
                .then(a.partition_key.cmp(&b.partition_key))
        });
        refs.dedup();
        refs
    }

    pub(crate) fn with_index_maintenance<R>(
        &self,
        index: &VectorIndexRef,
        f: impl FnOnce() -> R,
    ) -> R {
        self.with_partition_maintenance(index, &VectorPartitionKey::unpartitioned(), f)
    }

    fn with_partition_maintenance<R>(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        f: impl FnOnce() -> R,
    ) -> R {
        let store_id = self.id;
        let already_held = HELD_MAINTENANCE_LOCKS.with(|held| {
            held.borrow()
                .iter()
                .any(|(held_store, held_index, held_key)| {
                    *held_store == store_id && held_index == index && held_key == partition_key
                })
        });
        if already_held {
            return f();
        }

        self.with_bulk_read(|| {
            loop {
                let Some(column) = self.try_column(index) else {
                    return f();
                };
                let Some(state) = column.partition_state(partition_key) else {
                    return f();
                };
                let _index_guard = state.maintenance.lock();
                let still_current = self.registry.read().get(index).is_some_and(|current| {
                    Arc::ptr_eq(current, &column)
                        && current
                            .partition_state(partition_key)
                            .is_some_and(|candidate| Arc::ptr_eq(&candidate, &state))
                });
                if !still_current {
                    continue;
                }
                let _stack_guard = MaintenanceStackGuard::new(store_id, index, partition_key);
                return f();
            }
        })
    }

    fn with_partition_set_maintenance<R, F>(&self, partitions: &[VectorPartitionRef], f: F) -> R
    where
        F: FnOnce() -> R,
    {
        fn lock_next<R, F>(
            store: &VectorStore,
            partitions: &[VectorPartitionRef],
            f: &mut Option<F>,
        ) -> R
        where
            F: FnOnce() -> R,
        {
            let Some((partition, remaining)) = partitions.split_first() else {
                return f.take().expect("partition operation runs once")();
            };
            store.with_partition_maintenance(&partition.index, &partition.partition_key, || {
                lock_next(store, remaining, f)
            })
        }

        let mut f = Some(f);
        lock_next(self, partitions, &mut f)
    }

    pub(crate) fn with_bulk_read<R>(&self, f: impl FnOnce() -> R) -> R {
        let store_id = self.id;
        let already_held = HELD_BULK_READ_LOCKS.with(|held| held.borrow().contains(&store_id))
            || HELD_BULK_WRITE_LOCKS.with(|held| held.borrow().contains(&store_id));
        if already_held {
            return f();
        }
        #[cfg(any(test, feature = "test-seams"))]
        self.attach_passive_activity_to_registry();

        let _bulk_read = self.bulk_gate.read();
        let _stack_guard = BulkReadStackGuard::new(store_id);
        f()
    }

    fn with_bulk_maintenance<R>(&self, f: impl FnOnce() -> R) -> R {
        let store_id = self.id;
        let already_held = HELD_BULK_WRITE_LOCKS.with(|held| held.borrow().contains(&store_id));
        if already_held {
            return f();
        }
        #[cfg(any(test, feature = "test-seams"))]
        self.attach_passive_activity_to_registry();

        let _bulk_write = self.bulk_gate.write();
        let _stack_guard = BulkWriteStackGuard::new(store_id);
        f()
    }

    pub fn register_or_reconfigure_empty_index(
        &self,
        index: VectorIndexRef,
        dimension: usize,
        quantization: VectorQuantization,
    ) {
        let lock_index = index.clone();
        self.with_index_maintenance(&lock_index, || {
            let mut registry = self.registry.write();
            match registry.get(&index) {
                Some(column) if column.total_entry_count() != 0 => {}
                Some(column) if column.layout().dimension == dimension => {}
                Some(_) | None => {
                    registry.insert(
                        index,
                        Arc::new(VectorColumnState::new(VectorIndexLayout::unpartitioned(
                            dimension,
                            quantization,
                        ))),
                    );
                }
            }
        });
    }

    /// Remove one vector identity and return every raw entry that was owned by
    /// it. The store releases graph reservations here. The engine owns the
    /// separate admission charge for raw vector copies, and must settle that
    /// charge from these entries only after durable DDL publication succeeds.
    pub fn deregister_index(
        &self,
        index: &VectorIndexRef,
        accountant: &dyn MemoryBudget,
    ) -> Vec<VectorEntry> {
        self.with_bulk_maintenance(|| {
            if let Some(column) = self.registry.write().remove(index) {
                let mut entries = Vec::new();
                for (_, state) in column.partition_snapshots() {
                    if !state.release_accounted_raw_residency_for_removal() {
                        entries.extend(state.all_entries(index));
                    }
                }
                column.release_directory_accounting();
                column.clear_hnsw(accountant);
                entries
            } else {
                Vec::new()
            }
        })
    }

    /// Table-level counterpart of [`Self::deregister_index`]. Returning the
    /// original index identity with each entry lets the engine release exactly
    /// the charge it admitted, without treating graph bytes as raw-vector
    /// bytes a second time.
    pub fn deregister_table(&self, table: &str, accountant: &dyn MemoryBudget) -> Vec<VectorEntry> {
        self.with_bulk_read(|| {
            let keys = Self::sorted_refs(
                self.registry
                    .read()
                    .keys()
                    .filter(|index| index.table == table)
                    .cloned(),
            );
            let partitions = {
                let registry = self.registry.read();
                Self::sorted_partition_refs(keys.iter().flat_map(|index| {
                    registry
                        .get(index)
                        .into_iter()
                        .flat_map(|column| column.partition_snapshots())
                        .map(|(partition_key, _)| {
                            VectorPartitionRef::new(index.clone(), partition_key)
                        })
                }))
            };
            self.with_partition_set_maintenance(&partitions, || {
                let mut removed = Vec::new();
                let mut registry = self.registry.write();
                for index in keys {
                    if let Some(column) = registry.remove(&index) {
                        removed.push((index, column));
                    }
                }
                drop(registry);
                let mut entries = Vec::new();
                for (index, column) in removed {
                    for (_, state) in column.partition_snapshots() {
                        if !state.release_accounted_raw_residency_for_removal() {
                            entries.extend(state.all_entries(&index));
                        }
                    }
                    column.release_directory_accounting();
                    column.clear_hnsw(accountant);
                }
                entries
            })
        })
    }

    pub fn rename_index(&self, old: &VectorIndexRef, new: VectorIndexRef) -> Result<()> {
        self.with_bulk_maintenance(|| {
            let mut registry = self.registry.write();
            if registry.contains_key(&new) {
                return Err(Error::Other(format!(
                    "vector index already exists: {}.{}",
                    new.table, new.column
                )));
            }
            let column = registry
                .remove(old)
                .ok_or_else(|| Error::UnknownVectorIndex { index: old.clone() })?;
            registry.insert(new.clone(), column);
            Ok(())
        })
    }

    pub fn state(&self, index: &VectorIndexRef) -> Result<Arc<IndexState>> {
        self.partition_state(index, &VectorPartitionKey::unpartitioned())
    }

    pub fn partition_state(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<Arc<IndexState>> {
        self.try_partition_state(index, partition_key)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })
    }

    pub fn try_state(&self, index: &VectorIndexRef) -> Option<Arc<IndexState>> {
        self.try_partition_state(index, &VectorPartitionKey::unpartitioned())
    }

    pub fn try_partition_state(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Option<Arc<IndexState>> {
        self.try_column(index)
            .and_then(|column| column.partition_state(partition_key))
    }

    fn try_column(&self, index: &VectorIndexRef) -> Option<Arc<VectorColumnState>> {
        self.registry.read().get(index).cloned()
    }

    pub fn with_registered_partition_state<R>(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        f: impl FnOnce(Arc<IndexState>) -> R,
    ) -> Result<R> {
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            Ok(f(state))
        })
    }

    pub fn contains_index(&self, index: &VectorIndexRef) -> bool {
        self.with_bulk_read(|| self.registry.read().contains_key(index))
    }

    pub fn index_layout(&self, index: &VectorIndexRef) -> Result<VectorIndexLayout> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.layout())
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    pub fn partition_keys(&self, index: &VectorIndexRef) -> Result<Vec<VectorPartitionKey>> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.partition_keys())
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    /// Every stored typed key, including retired graph states that retain raw
    /// vectors solely for historical EXACT reads. Ordinary inspection and
    /// live partition counts use `partition_keys` instead.
    #[doc(hidden)]
    pub fn partition_keys_including_historical(
        &self,
        index: &VectorIndexRef,
    ) -> Result<Vec<VectorPartitionKey>> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.partition_keys_including_historical())
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    /// Visit a complete, stable snapshot of this column's active and
    /// historical-only raw-vector sources without first allocating an
    /// uncharged key list. The borrowed view exposes the exact source count
    /// and then visits each typed key in stable order; callers can admit
    /// capacity once, charge each key, cancel between sources, and clone only
    /// state handles they will pin after this call returns.
    #[doc(hidden)]
    pub fn with_partition_sources<E, T>(
        &self,
        index: &VectorIndexRef,
        use_sources: impl FnOnce(VectorPartitionSourceView<'_>) -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E>
    where
        E: From<Error>,
    {
        self.with_bulk_read(|| {
            let column = self.try_column(index).ok_or_else(|| {
                E::from(Error::UnknownVectorIndex {
                    index: index.clone(),
                })
            })?;
            let partitions = column.partitions.read();
            use_sources(VectorPartitionSourceView { partitions })
        })
    }

    /// Whether this key retains raw historical vectors but no longer owns a
    /// maintained indexed state. Exact readers still inspect it when their
    /// snapshot can see one of those versions; indexed readers must treat it
    /// as unavailable, not as a missing tuple.
    #[doc(hidden)]
    pub fn partition_is_historical_only(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> bool {
        self.with_bulk_read(|| {
            self.try_column(index)
                .is_some_and(|column| column.partition_is_historical_only(partition_key))
        })
    }

    pub fn current_partition_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
    ) -> Option<VectorPartitionKey> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .and_then(|column| column.current_partition(row_id))
        })
    }

    pub fn live_entry_for_row_in_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Option<VectorEntry> {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .and_then(|state| state.visible_entry_by_row(index, row_id, snapshot))
        })
    }

    pub fn partition_info(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Option<VectorPartitionInfo> {
        self.with_bulk_read(|| {
            let column = self.try_column(index)?;
            if column.partition_is_historical_only(partition_key) {
                return None;
            }
            column.partition_state(partition_key).map(|state| {
                let layout = column.layout();
                Self::partition_info_snapshot(index, partition_key, &state, &layout)
            })
        })
    }

    fn partition_info_snapshot(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        state: &Arc<IndexState>,
        layout: &VectorIndexLayout,
    ) -> VectorPartitionInfo {
        let live_rows = state.vector_count();
        let retained_rows = state.entry_count().saturating_sub(live_rows);
        let bytes = state
            .byte_count()
            .saturating_add(partition_key.estimated_bytes());
        let charged_vector_bytes = state.raw_payload_bytes();
        let charged_index_bytes = bytes.saturating_sub(charged_vector_bytes);
        let generations = state.graph_generation_status();
        let base = generations.base.or(generations.dormant_base);
        let change = generations.change.or(generations.dormant_change);
        let local_generation_id = state.inspection_generation_id.load(Ordering::SeqCst);
        let local_base_tx = state.inspection_base_tx.load(Ordering::SeqCst);
        let base_generation = base
            .map(|generation| generation.generation_id)
            .or((local_generation_id != 0).then_some(local_generation_id));
        let base_tx = base
            .map(|generation| generation.covered_tx)
            .or((local_generation_id != 0).then_some(TxId(local_base_tx)));
        let covered_tx = change
            .or(base)
            .map(|generation| generation.covered_tx)
            .or((local_generation_id != 0).then_some(TxId(local_base_tx)));
        let directory = state.raw_directory.read();
        #[cfg(test)]
        state.raw_directory_entries_inspected.fetch_add(
            u64::try_from(directory.len()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
        let pending_inserts = directory
            .iter()
            .filter(|entry| {
                entry.deleted_tx.is_none()
                    && covered_tx.is_none_or(|covered| entry.created_tx > covered)
            })
            .count();
        let tombstones = directory
            .iter()
            .filter(|entry| {
                entry
                    .deleted_tx
                    .is_some_and(|deleted| covered_tx.is_none_or(|covered| deleted > covered))
            })
            .count();
        let durable_vector_bytes = if state.raw_loader.read().is_some() {
            directory
                .len()
                .saturating_mul(crate::quantized::stored_vector_resident_bytes(
                    state.dimension,
                    state.quantization,
                ))
        } else {
            0
        };
        drop(directory);
        let durable_index_bytes =
            base.into_iter()
                .chain(change)
                .fold(0usize, |total, generation| {
                    total.saturating_add(
                        usize::try_from(generation.durable_bytes).unwrap_or(usize::MAX),
                    )
                });
        let maintenance_failure_details = state.maintenance_failure_details();
        VectorPartitionInfo {
            partition: VectorPartitionRef::new(index.clone(), partition_key.clone()),
            live_rows,
            retained_rows,
            bytes,
            graph_available: state.has_complete_hnsw_route(),
            base_generation,
            base_tx,
            pending_inserts,
            tombstones,
            durable_vector_bytes,
            charged_vector_bytes,
            durable_index_bytes,
            charged_index_bytes,
            quarantine_reason: state.route_quarantine(),
            maintenance_progress: state.maintenance_progress(),
            maintenance_failure: maintenance_failure_details.map(|details| details.failure),
            maintenance_failure_details,
            desired_policy: layout.resolve_policy(live_rows, 1),
            serving_policy: state.serving_policy(),
            maintenance_policy_revision: state.maintenance_policy_revision(),
        }
    }

    pub fn partition_infos(&self, index: &VectorIndexRef) -> Result<Vec<VectorPartitionInfo>> {
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            Ok(column
                .maintained_partition_snapshots()
                .into_iter()
                .map(|(key, state)| {
                    let layout = column.layout();
                    Self::partition_info_snapshot(index, &key, &state, &layout)
                })
                .collect())
        })
    }

    #[doc(hidden)]
    pub fn begin_partition_maintenance_progress(
        &self,
        partition: &VectorPartitionRef,
        reason: &'static str,
        total: usize,
        checkpoint_tx: TxId,
        phase: &'static str,
    ) -> Result<VectorMaintenanceProgressGuard> {
        let state = self.partition_state(&partition.index, &partition.partition_key)?;
        Ok(state.begin_maintenance_progress_at(phase, reason, total, checkpoint_tx))
    }

    #[doc(hidden)]
    pub fn select_maintenance_partition(
        &self,
        candidates: &[VectorPartitionRef],
    ) -> Option<VectorPartitionRef> {
        let mut last = self.last_maintenance_partition.lock();
        let next = last
            .as_ref()
            .and_then(|last| candidates.iter().position(|p| p == last))
            .map_or(0, |position| (position + 1) % candidates.len().max(1));
        let selected = candidates.get(next)?.clone();
        *last = Some(selected.clone());
        Some(selected)
    }

    /// Reclaim only reloadable, unborrowed graphs. Query layer read guards
    /// prevent eviction while a snapshot is actively using those pages.
    #[doc(hidden)]
    pub fn evict_idle_graphs(&self) -> usize {
        self.evict_idle_graphs_except(None)
    }

    pub(crate) fn evict_idle_graphs_except(&self, selected: Option<&IndexState>) -> usize {
        self.registry
            .read()
            .values()
            .flat_map(|column| column.partition_snapshots())
            .filter(|(_, state)| {
                selected.is_none_or(|selected| !std::ptr::eq(state.as_ref(), selected))
            })
            .map(|(_, state)| {
                usize::from(state.evict_resident_sealed_generation(true))
                    + usize::from(state.evict_resident_sealed_generation(false))
            })
            .sum()
    }

    pub(crate) fn mark_sealed_graph_used(&self, state: &IndexState) {
        let sequence = self
            .graph_use_sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        state
            .sealed_graph_last_used
            .store(sequence, Ordering::Relaxed);
    }

    /// Release reloadable graphs only when a finite shared budget cannot
    /// admit the next allocation. With room available, alternating
    /// partitions remain warm.
    pub(crate) fn reclaim_idle_graphs_for(
        &self,
        required_bytes: usize,
        selected: Option<&IndexState>,
    ) -> usize {
        let Some(accountant) = self.memory_accountant.read().clone() else {
            return 0;
        };
        if accountant
            .available_bytes()
            .is_none_or(|available| required_bytes <= available)
        {
            return 0;
        }
        let Some(mut available) = accountant.available_bytes() else {
            return 0;
        };
        let mut candidates = self
            .registry
            .read()
            .iter()
            .flat_map(|(index, column)| {
                column.partition_snapshots().into_iter().filter_map(
                    move |(partition_key, state)| {
                        selected
                            .is_none_or(|selected| !std::ptr::eq(state.as_ref(), selected))
                            .then(|| (VectorPartitionRef::new(index.clone(), partition_key), state))
                    },
                )
            })
            .collect::<Vec<_>>();
        candidates.sort_by(
            |(left_partition, left_state), (right_partition, right_state)| {
                left_state
                    .sealed_graph_last_used
                    .load(Ordering::Relaxed)
                    .cmp(&right_state.sealed_graph_last_used.load(Ordering::Relaxed))
                    .then_with(|| left_partition.index.table.cmp(&right_partition.index.table))
                    .then_with(|| {
                        left_partition
                            .index
                            .column
                            .cmp(&right_partition.index.column)
                    })
                    .then_with(|| {
                        left_partition
                            .partition_key
                            .cmp(&right_partition.partition_key)
                    })
            },
        );

        let mut evicted = 0usize;
        for (_, state) in candidates {
            if required_bytes <= available {
                break;
            }
            evicted += usize::from(state.evict_resident_sealed_generation(true));
            evicted += usize::from(state.evict_resident_sealed_generation(false));
            available = accountant.available_bytes().unwrap_or(usize::MAX);
        }
        evicted
    }

    #[doc(hidden)]
    pub fn begin_partition_policy_build(
        &self,
        partition: &VectorPartitionRef,
        revision: u64,
    ) -> Result<VectorPolicyBuildGuard> {
        self.with_bulk_read(|| {
            let state = self.partition_state(&partition.index, &partition.partition_key)?;
            state
                .maintenance_policy_revision
                .store(revision.max(1), Ordering::SeqCst);
            Ok(VectorPolicyBuildGuard {
                state,
                revision: revision.max(1),
            })
        })
    }

    pub fn index_layout_info(&self, index: &VectorIndexRef) -> Result<VectorIndexLayoutInfo> {
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            Ok(VectorIndexLayoutInfo {
                index: index.clone(),
                layout: column.layout(),
                live_partitions: column.live_partition_count(),
                retained_partitions: column.retained_partition_count(),
                live_or_retained_partitions: column.live_or_retained_partition_count(),
                partition_states: column.maintained_partition_state_count(),
                bytes: column.retained_bytes(index),
                graph_available: column.has_complete_hnsw_route(),
            })
        })
    }

    pub fn is_empty(&self) -> bool {
        self.with_bulk_read(|| self.registry.read().is_empty())
    }

    pub fn index_count(&self) -> usize {
        self.with_bulk_read(|| self.registry.read().len())
    }

    pub fn validate_vector(&self, index: &VectorIndexRef, actual: usize) -> Result<()> {
        self.with_bulk_read(|| {
            let expected = self.index_layout(index)?.dimension;
            if expected != actual {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: index.clone(),
                    expected,
                    actual,
                });
            }
            Ok(())
        })
    }

    pub fn apply_inserts(&self, inserts: Vec<VectorEntry>) {
        self.apply_inserts_with_accountant(inserts, None);
    }

    pub fn apply_inserts_with_accountant(
        &self,
        inserts: Vec<VectorEntry>,
        accountant: Option<&dyn MemoryBudget>,
    ) {
        let mut by_index = HashMap::<VectorIndexRef, Vec<VectorEntry>>::new();
        for entry in inserts {
            by_index.entry(entry.index.clone()).or_default().push(entry);
        }
        for index in Self::sorted_refs(by_index.keys().cloned()) {
            if let Some(inserts) = by_index.remove(&index) {
                self.with_index_maintenance(&index, || {
                    self.apply_inserts_unlocked(inserts, accountant);
                });
            }
        }
    }

    fn apply_inserts_unlocked(
        &self,
        inserts: Vec<VectorEntry>,
        accountant: Option<&dyn MemoryBudget>,
    ) {
        for entry in inserts {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&entry.index, crate::test_seam::PauseWindow::Apply);
            if let Some(column) = self.try_column(&entry.index)
                && let Some(state) = column.partition_state(&VectorPartitionKey::unpartitioned())
            {
                let index = entry.index.clone();
                let stored_entry = state.stored_entry(entry);
                state.push_entry(stored_entry.clone());
                column.note_live_insert(&VectorPartitionKey::unpartitioned(), &stored_entry);
                state.insert_into_materialized_hnsw(&index, &stored_entry, accountant);
            }
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(VectorIndexRef, RowId, TxId)>) {
        self.apply_deletes_with_accountant(deletes, None);
    }

    pub fn apply_deletes_with_accountant(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId)>,
        accountant: Option<&dyn MemoryBudget>,
    ) {
        let mut by_index = HashMap::<VectorIndexRef, Vec<(VectorIndexRef, RowId, TxId)>>::new();
        for delete in deletes {
            by_index.entry(delete.0.clone()).or_default().push(delete);
        }
        for index in Self::sorted_refs(by_index.keys().cloned()) {
            if let Some(deletes) = by_index.remove(&index) {
                self.with_index_maintenance(&index, || {
                    self.apply_deletes_unlocked(deletes, accountant);
                });
            }
        }
    }

    fn apply_deletes_unlocked(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId)>,
        _accountant: Option<&dyn MemoryBudget>,
    ) {
        for (index, row_id, deleted_tx) in deletes {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&index, crate::test_seam::PauseWindow::Apply);
            let key = VectorPartitionKey::unpartitioned();
            if let Some(column) = self.try_column(&index)
                && let Some(state) = column.partition_state(&key)
                && state.tombstone_row(row_id, deleted_tx) != 0
            {
                column.note_tombstone(&key, row_id);
            }
        }
    }

    pub fn apply_moves(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
    ) {
        self.apply_moves_with_accountant(moves, lsn, None);
    }

    pub fn apply_moves_with_accountant(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) {
        let mut by_index =
            HashMap::<VectorIndexRef, Vec<(VectorIndexRef, RowId, RowId, TxId)>>::new();
        for row_move in moves {
            by_index
                .entry(row_move.0.clone())
                .or_default()
                .push(row_move);
        }
        for index in Self::sorted_refs(by_index.keys().cloned()) {
            if let Some(moves) = by_index.remove(&index) {
                self.with_index_maintenance(&index, || {
                    self.apply_moves_unlocked(moves, lsn, accountant);
                });
            }
        }
    }

    fn apply_moves_unlocked(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) {
        for (index, old_row_id, new_row_id, tx) in moves {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&index, crate::test_seam::PauseWindow::Apply);
            let key = VectorPartitionKey::unpartitioned();
            if let Some(column) = self.try_column(&index)
                && let Some(state) = column.partition_state(&key)
                && let Some(old) = state.stored_by_row_id(old_row_id)
                && old.deleted_tx.is_none()
            {
                state.tombstone_row(old_row_id, tx);
                let mut moved = old;
                moved.row_id = new_row_id;
                moved.created_tx = tx;
                moved.deleted_tx = None;
                moved.lsn = lsn;
                state.push_entry(moved.clone());
                column.note_move(&key, &key, old_row_id, new_row_id);
                state.insert_into_materialized_hnsw(&index, &moved, accountant);
            }
        }
    }

    pub fn apply_changes_with_accountant(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId)>,
        inserts: Vec<VectorEntry>,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId)>,
        lsn: contextdb_core::Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) {
        let mut by_index = HashMap::<VectorIndexRef, PendingVectorChanges>::new();
        for delete in deletes {
            by_index
                .entry(delete.0.clone())
                .or_default()
                .deletes
                .push(delete);
        }
        for insert in inserts {
            by_index
                .entry(insert.index.clone())
                .or_default()
                .inserts
                .push(insert);
        }
        for row_move in moves {
            by_index
                .entry(row_move.0.clone())
                .or_default()
                .moves
                .push(row_move);
        }
        for index in Self::sorted_refs(by_index.keys().cloned()) {
            if let Some(changes) = by_index.remove(&index) {
                self.with_index_maintenance(&index, || {
                    self.apply_deletes_unlocked(changes.deletes, accountant);
                    self.apply_inserts_unlocked(changes.inserts, accountant);
                    self.apply_moves_unlocked(changes.moves, lsn, accountant);
                });
            }
        }
    }

    pub fn apply_changes_with_accountant_ref(
        &self,
        deletes: &[(VectorIndexRef, RowId, TxId)],
        inserts: &[VectorEntry],
        moves: &[(VectorIndexRef, RowId, RowId, TxId)],
        lsn: contextdb_core::Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) {
        let mut by_index = HashMap::<&VectorIndexRef, PendingVectorChangesRef<'_>>::new();
        for delete in deletes {
            by_index.entry(&delete.0).or_default().deletes.push(delete);
        }
        for insert in inserts {
            by_index
                .entry(&insert.index)
                .or_default()
                .inserts
                .push(insert);
        }
        for row_move in moves {
            by_index
                .entry(&row_move.0)
                .or_default()
                .moves
                .push(row_move);
        }

        let mut refs = by_index.keys().copied().collect::<Vec<_>>();
        refs.sort_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
        for index in refs {
            if let Some(changes) = by_index.get(index) {
                self.with_index_maintenance(index, || {
                    self.apply_deletes_unlocked_ref(&changes.deletes, accountant);
                    self.apply_inserts_unlocked_ref(&changes.inserts, accountant);
                    self.apply_moves_unlocked_ref(&changes.moves, lsn, accountant);
                });
            }
        }
    }

    fn apply_deletes_unlocked_ref(
        &self,
        deletes: &[&(VectorIndexRef, RowId, TxId)],
        _accountant: Option<&dyn MemoryBudget>,
    ) {
        for (index, row_id, deleted_tx) in deletes.iter().copied() {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(index, crate::test_seam::PauseWindow::Apply);
            let key = VectorPartitionKey::unpartitioned();
            if let Some(column) = self.try_column(index)
                && let Some(state) = column.partition_state(&key)
                && state.tombstone_row(*row_id, *deleted_tx) != 0
            {
                column.note_tombstone(&key, *row_id);
            }
        }
    }

    fn apply_inserts_unlocked_ref(
        &self,
        inserts: &[&VectorEntry],
        accountant: Option<&dyn MemoryBudget>,
    ) {
        for entry in inserts {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&entry.index, crate::test_seam::PauseWindow::Apply);
            if let Some(column) = self.try_column(&entry.index)
                && let Some(state) = column.partition_state(&VectorPartitionKey::unpartitioned())
            {
                let stored_entry = state.stored_entry_ref(entry);
                state.push_entry(stored_entry.clone());
                column.note_live_insert(&VectorPartitionKey::unpartitioned(), &stored_entry);
                state.insert_into_materialized_hnsw(&entry.index, &stored_entry, accountant);
            }
        }
    }

    fn apply_moves_unlocked_ref(
        &self,
        moves: &[&(VectorIndexRef, RowId, RowId, TxId)],
        lsn: contextdb_core::Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) {
        for (index, old_row_id, new_row_id, tx) in moves.iter().copied() {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(index, crate::test_seam::PauseWindow::Apply);
            let key = VectorPartitionKey::unpartitioned();
            if let Some(column) = self.try_column(index)
                && let Some(state) = column.partition_state(&key)
                && let Some(old) = state.stored_by_row_id(*old_row_id)
                && old.deleted_tx.is_none()
            {
                state.tombstone_row(*old_row_id, *tx);
                let mut moved = old;
                moved.row_id = *new_row_id;
                moved.created_tx = *tx;
                moved.deleted_tx = None;
                moved.lsn = lsn;
                state.push_entry(moved.clone());
                column.note_move(&key, &key, *old_row_id, *new_row_id);
                state.insert_into_materialized_hnsw(index, &moved, accountant);
            }
        }
    }

    pub fn apply_partitioned_inserts(&self, inserts: Vec<PartitionedVectorEntry>) -> Result<()> {
        self.apply_partitioned_inserts_with_accountant(inserts, None)
    }

    pub fn apply_partitioned_inserts_with_accountant(
        &self,
        inserts: Vec<PartitionedVectorEntry>,
        accountant: Option<&dyn MemoryBudget>,
    ) -> Result<()> {
        self.apply_partitioned_changes_with_accountant(
            Vec::new(),
            inserts,
            Vec::new(),
            Lsn::default(),
            accountant,
        )
    }

    pub fn apply_partitioned_deletes(&self, deletes: Vec<PartitionedVectorDelete>) -> Result<()> {
        self.apply_partitioned_changes_with_accountant(
            deletes,
            Vec::new(),
            Vec::new(),
            Lsn::default(),
            None,
        )
    }

    pub fn apply_partitioned_moves(
        &self,
        moves: Vec<PartitionedVectorMove>,
        lsn: Lsn,
    ) -> Result<()> {
        self.apply_partitioned_moves_with_accountant(moves, lsn, None)
    }

    pub fn apply_partitioned_moves_with_accountant(
        &self,
        moves: Vec<PartitionedVectorMove>,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) -> Result<()> {
        self.apply_partitioned_changes_with_accountant(
            Vec::new(),
            Vec::new(),
            moves,
            lsn,
            accountant,
        )
    }

    pub fn apply_partitioned_changes_with_accountant(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) -> Result<()> {
        let prepared = self.prepare_partitioned_batch(deletes, inserts, moves, HashSet::new())?;
        self.publish_prepared_partitioned_batch(prepared, lsn, accountant);
        Ok(())
    }

    fn apply_admitted_partitioned_changes(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
        tail_insertions: &PreparedTailInsertions,
    ) {
        let deleted_rows = deletes
            .iter()
            .map(|delete| (delete.partition.index.clone(), delete.row_id))
            .collect::<HashSet<_>>();
        for delete in deletes {
            #[cfg(feature = "test-seams")]
            self.pause_registry.maybe_pause(
                &delete.partition.index,
                crate::test_seam::PauseWindow::Apply,
            );
            let Some(column) = self.try_column(&delete.partition.index) else {
                continue;
            };
            let Some(state) = column.partition_state(&delete.partition.partition_key) else {
                continue;
            };
            if state.tombstone_row(delete.row_id, delete.deleted_tx) != 0 {
                column.note_tombstone(&delete.partition.partition_key, delete.row_id);
            }
        }
        for insert in inserts {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&insert.entry.index, crate::test_seam::PauseWindow::Apply);
            let Some(column) = self.try_column(&insert.entry.index) else {
                continue;
            };
            let Some(state) = column.partition_state(&insert.partition_key) else {
                continue;
            };
            if let Some(owner) = self.memory_accountant.read().clone() {
                state.attach_raw_accounting(owner);
            }
            let index = insert.entry.index.clone();
            let stored = state.stored_entry(insert.entry);
            let already_present = state.with_entries(|entries| {
                entries.iter().any(|existing| {
                    existing.row_id == stored.row_id
                        && existing.created_tx == stored.created_tx
                        && existing.deleted_tx == stored.deleted_tx
                        && existing.lsn == stored.lsn
                })
            });
            let live_without_replacement = !deleted_rows.contains(&(index.clone(), stored.row_id))
                && state.with_entries(|entries| {
                    entries.iter().any(|existing| {
                        existing.row_id == stored.row_id && existing.deleted_tx.is_none()
                    })
                });
            if already_present || live_without_replacement {
                if let Some(accountant) = accountant {
                    accountant.release(stored.estimated_bytes());
                }
                continue;
            }
            state.push_entry(stored.clone());
            column.note_live_insert(&insert.partition_key, &stored);
            tail_insertions.publish(&state, stored.row_id, stored.lsn);
        }
        for row_move in moves {
            #[cfg(feature = "test-seams")]
            self.pause_registry
                .maybe_pause(&row_move.index, crate::test_seam::PauseWindow::Apply);
            let Some(column) = self.try_column(&row_move.index) else {
                continue;
            };
            let Some(source) = column.partition_state(&row_move.source_partition_key) else {
                continue;
            };
            let Some(target) = column.partition_state(&row_move.target_partition_key) else {
                continue;
            };
            if let Some(owner) = self.memory_accountant.read().clone() {
                target.attach_raw_accounting(owner);
            }
            let Some(old) = source.stored_by_row_id(row_move.old_row_id) else {
                continue;
            };
            if old.deleted_tx.is_some() {
                continue;
            }
            source.tombstone_row(row_move.old_row_id, row_move.tx);
            let mut moved = old;
            moved.row_id = row_move.new_row_id;
            moved.created_tx = row_move.tx;
            moved.deleted_tx = None;
            moved.lsn = lsn;
            target.push_entry(moved.clone());
            column.note_move(
                &row_move.source_partition_key,
                &row_move.target_partition_key,
                row_move.old_row_id,
                row_move.new_row_id,
            );
            tail_insertions.publish(&target, moved.row_id, lsn);
        }
    }

    pub fn insert_loaded_vector(&self, entry: VectorEntry) {
        let index = entry.index.clone();
        self.with_index_maintenance(&index, || {
            let quantization = self
                .try_state(&index)
                .map(|state| state.quantization())
                .unwrap_or(VectorQuantization::F32);
            self.register_or_reconfigure_empty_index(
                index.clone(),
                entry.vector.len(),
                quantization,
            );
            if let Some(column) = self.try_column(&index)
                && let Some(state) = column.partition_state(&VectorPartitionKey::unpartitioned())
            {
                let stored_entry = state.stored_entry(entry);
                state.push_entry(stored_entry.clone());
                column.note_live_insert(&VectorPartitionKey::unpartitioned(), &stored_entry);
            }
        });
    }

    pub fn insert_loaded_partitioned_vector(&self, entry: PartitionedVectorEntry) -> Result<()> {
        let partition =
            VectorPartitionRef::new(entry.entry.index.clone(), entry.partition_key.clone());
        let touched = HashSet::from([partition.clone()]);
        let requested = HashMap::from([(
            partition.index.clone(),
            [partition.partition_key.clone()]
                .into_iter()
                .collect::<PartitionKeySet>(),
        )]);
        self.with_admitted_partition_states(&touched, &requested, || {
            self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
                let Some(column) = self.try_column(&partition.index) else {
                    return;
                };
                let Some(state) = column.partition_state(&partition.partition_key) else {
                    return;
                };
                let stored = state.stored_entry(entry.entry);
                state.push_entry(stored.clone());
                column.note_live_insert(&partition.partition_key, &stored);
            });
        })
    }

    /// Install only durable identities for a file-backed open. Vector bodies
    /// remain behind each partition's loader until a read or mutation selects
    /// that partition.
    #[doc(hidden)]
    pub fn replace_with_dormant_raw_partitions(
        &self,
        dormant: Vec<DormantRawVectorPartition>,
    ) -> Result<()> {
        self.with_bulk_maintenance(|| {
            let mut live_requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
            let mut seen = HashSet::<VectorPartitionRef>::new();
            for partition in &dormant {
                if !seen.insert(partition.partition.clone()) {
                    return Err(Error::Other(format!(
                        "duplicate dormant raw vector partition for {}.{}",
                        partition.partition.index.table, partition.partition.index.column
                    )));
                }
                let column = self.try_column(&partition.partition.index).ok_or_else(|| {
                    Error::UnknownVectorIndex {
                        index: partition.partition.index.clone(),
                    }
                })?;
                Self::validate_partition_key(
                    &partition.partition.index,
                    &column.layout(),
                    &partition.partition.partition_key,
                )?;
                if partition
                    .directory
                    .iter()
                    .any(|entry| entry.deleted_tx.is_none())
                {
                    live_requested
                        .entry(partition.partition.index.clone())
                        .or_default()
                        .insert(partition.partition.partition_key.clone());
                }
            }
            for (index, keys) in &live_requested {
                let column = self
                    .try_column(index)
                    .expect("dormant raw partition index was validated");
                if let Some(max_partitions) = column.layout().max_partitions
                    && keys.len() > max_partitions as usize
                {
                    return Err(Error::VectorPartitionLimitExceeded {
                        index: index.clone(),
                        max_partitions,
                    });
                }
            }

            for column in self.registry.read().values() {
                let layout = column.layout();
                for (_, state) in column.partition_snapshots() {
                    state.clear_entries();
                    state.drop_hnsw_without_accounting();
                }
                if layout.is_partitioned() {
                    column.partitions.write().clear();
                }
                column.row_to_partition.write().clear();
            }
            for partition in dormant {
                let column = self
                    .try_column(&partition.partition.index)
                    .expect("dormant raw partition index was validated");
                let layout = column.layout();
                let state = {
                    let mut partitions = column.partitions.write();
                    partitions
                        .entry(partition.partition.partition_key.clone())
                        .or_insert_with(|| {
                            Arc::new(IndexState::new(layout.dimension, layout.quantization))
                        })
                        .clone()
                };
                state.install_dormant_raw_directory(partition.directory, partition.loader)?;
                if state.vector_count() == 0 && state.entry_count() != 0 {
                    column.mark_historical_only(&partition.partition.partition_key);
                }
            }
            for column in self.registry.read().values() {
                column.rebuild_current_rows();
            }
            Ok(())
        })
    }

    /// Attach current durable loaders to an already-published registry. A
    /// terminal-only partition absent from the durable current-membership
    /// image has no reloadable body, so its stale resident body is retired at
    /// this compaction boundary. Graph routes remain untouched.
    #[doc(hidden)]
    pub fn attach_durable_raw_partition_loaders(
        &self,
        dormant: Vec<DormantRawVectorPartition>,
    ) -> Result<()> {
        self.with_bulk_maintenance(|| {
            let durable_partitions = dormant
                .iter()
                .map(|partition| partition.partition.clone())
                .collect::<HashSet<_>>();
            for partition in dormant {
                let state = self
                    .try_partition_state(
                        &partition.partition.index,
                        &partition.partition.partition_key,
                    )
                    .ok_or_else(|| {
                        Error::Other(
                            "durable raw-vector partition is absent from the resident registry"
                                .to_string(),
                        )
                    })?;
                state.attach_durable_raw_loader(&partition.directory, partition.loader)?;
            }
            let indexes = Self::sorted_refs(self.registry.read().keys().cloned());
            let sampled = self.directory_bytes_before_reclamation(&indexes);
            for (index, column) in self.registry.read().iter() {
                for (partition_key, state) in column.partition_snapshots() {
                    let partition = VectorPartitionRef::new(index.clone(), partition_key);
                    if durable_partitions.contains(&partition) || state.vector_count() != 0 {
                        continue;
                    }
                    state.release_accounted_raw_residency_for_removal();
                    state.clear_entries();
                }
                column.rebuild_current_rows();
            }
            // The directory owner returns exactly what the clears above
            // emptied.
            self.reconcile_directory_accounting_for_indexes(&sampled);
            Ok(())
        })
    }

    pub fn replace_loaded_vectors(&self, entries: Vec<VectorEntry>) {
        self.with_bulk_maintenance(|| {
            #[cfg(feature = "test-seams")]
            self.pause_registry.maybe_pause(
                &contextdb_core::VectorIndexRef::default(),
                crate::test_seam::PauseWindow::Bulk,
            );
            for column in self.registry.read().values() {
                let layout = column.layout();
                for (_, state) in column.partition_snapshots() {
                    state.clear_entries();
                    state.drop_hnsw_without_accounting();
                }
                if layout.is_partitioned() {
                    column.partitions.write().clear();
                }
                column.row_to_partition.write().clear();
            }
            for entry in entries {
                self.insert_loaded_vector(entry);
            }
        });
    }

    pub fn replace_loaded_partitioned_vectors(
        &self,
        entries: Vec<PartitionedVectorEntry>,
    ) -> Result<()> {
        self.with_bulk_maintenance(|| {
            let mut requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
            let mut live_requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
            for entry in &entries {
                let column = self.try_column(&entry.entry.index).ok_or_else(|| {
                    Error::UnknownVectorIndex {
                        index: entry.entry.index.clone(),
                    }
                })?;
                let layout = column.layout();
                Self::validate_partition_key(&entry.entry.index, &layout, &entry.partition_key)?;
                requested
                    .entry(entry.entry.index.clone())
                    .or_default()
                    .insert(entry.partition_key.clone());
                if entry.entry.deleted_tx.is_none() {
                    live_requested
                        .entry(entry.entry.index.clone())
                        .or_default()
                        .insert(entry.partition_key.clone());
                }
            }
            for (index, keys) in &live_requested {
                let column = self
                    .try_column(index)
                    .expect("loaded entry index was validated");
                if let Some(max_partitions) = column.layout().max_partitions
                    && keys.len() > max_partitions as usize
                {
                    return Err(Error::VectorPartitionLimitExceeded {
                        index: index.clone(),
                        max_partitions,
                    });
                }
            }

            for column in self.registry.read().values() {
                let layout = column.layout();
                for (_, state) in column.partition_snapshots() {
                    state.clear_entries();
                    state.drop_hnsw_without_accounting();
                }
                if layout.is_partitioned() {
                    column.partitions.write().clear();
                }
                column.row_to_partition.write().clear();
            }
            self.create_partition_states(&requested);
            for entry in entries {
                let column = self
                    .try_column(&entry.entry.index)
                    .expect("loaded entry index was validated");
                let state = column
                    .partition_state(&entry.partition_key)
                    .expect("loaded partition was admitted");
                let stored = state.stored_entry(entry.entry);
                state.push_entry(stored.clone());
                column.note_live_insert(&entry.partition_key, &stored);
            }
            for column in self.registry.read().values() {
                if !column.layout().is_partitioned() {
                    continue;
                }
                for (partition_key, state) in column.partition_snapshots() {
                    if state.vector_count() == 0 && state.entry_count() != 0 {
                        column.mark_historical_only(&partition_key);
                    }
                }
            }
            Ok(())
        })
    }

    pub fn all_entries(&self) -> Vec<VectorEntry> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.partition_snapshots() {
                    entries.extend(state.all_entries(&index));
                }
            }
            entries
        })
    }

    /// Materialize every raw partition before returning its entries. Durable
    /// callers use this for operations such as export and schema projection
    /// that genuinely need the complete body set and can propagate a load or
    /// admission refusal.
    #[doc(hidden)]
    pub fn try_all_entries(&self) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.partition_snapshots() {
                    state.ensure_raw_vectors_loaded()?;
                    entries.extend(state.all_entries(&index));
                }
            }
            Ok(entries)
        })
    }

    /// Lightweight durable identities used by startup bookkeeping. This does
    /// not fault a vector body into memory.
    #[doc(hidden)]
    pub fn raw_directory_entries(&self) -> Vec<(VectorIndexRef, RawVectorDirectoryEntry)> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.partition_snapshots() {
                    entries.extend(
                        state
                            .raw_directory
                            .read()
                            .iter()
                            .cloned()
                            .map(|entry| (index.clone(), entry)),
                    );
                }
            }
            entries
        })
    }

    #[doc(hidden)]
    pub fn ensure_all_raw_vectors_loaded(&self) -> Result<()> {
        self.with_bulk_read(|| {
            for column in self.registry.read().values() {
                for (_, state) in column.partition_snapshots() {
                    state.ensure_raw_vectors_loaded()?;
                }
            }
            Ok(())
        })
    }

    #[doc(hidden)]
    pub fn ensure_raw_partition_loaded(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<()> {
        self.with_bulk_read(|| {
            self.partition_state(index, partition_key)?
                .ensure_raw_vectors_loaded()
        })
    }

    /// Admit the raw-image copies and affected graph encodings before purge
    /// loads its selected values or constructs a replacement registry.
    #[doc(hidden)]
    pub fn purge_workspace_bounds(&self, rows: &[(String, RowId)]) -> (usize, usize) {
        let mut workspace = 0usize;
        let mut disk = 0usize;
        for (index, column) in self.registry.read().iter() {
            let layout = column.layout();
            for (_, state) in column.partition_snapshots() {
                // Existing resident values are cloned into the staged image;
                // dormant unrelated partitions retain only their descriptors.
                workspace = workspace.saturating_add(state.raw_payload_bytes().saturating_mul(4));
                let contains_selected_row = {
                    let directory = state.raw_directory_by_row.read();
                    rows.iter()
                        .any(|(table, row)| table == &index.table && directory.contains_key(row))
                };
                if !contains_selected_row {
                    continue;
                }
                let count = state.entry_count();
                let values = count.saturating_mul(
                    std::mem::size_of::<VectorEntry>()
                        .saturating_add(layout.dimension.saturating_mul(4))
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len())
                        .saturating_add(128),
                );
                workspace = workspace.saturating_add(values.saturating_mul(4));
                if state.has_complete_hnsw_route() {
                    let graph = HnswIndex::estimated_build_reservation(
                        count,
                        layout.dimension,
                        layout.quantization,
                        layout.resolve_policy(count, 1),
                    );
                    workspace = workspace.saturating_add(graph.saturating_mul(6));
                    disk = disk.saturating_add(graph.saturating_mul(2).saturating_add(64 * 1024));
                }
            }
        }
        (workspace, disk)
    }

    /// Load exactly the raw partitions that contain any version of one of
    /// the named rows. Authoritative purge uses this before constructing its
    /// replacement image: every version of the selected lineage must be
    /// available, while unrelated partitions remain dormant.
    #[doc(hidden)]
    pub fn ensure_raw_partitions_containing_rows(
        &self,
        table: &str,
        row_ids: &HashSet<RowId>,
    ) -> Result<Vec<VectorPartitionRef>> {
        self.with_bulk_read(|| {
            let mut loaded = Vec::new();
            for index in Self::sorted_refs(
                self.registry
                    .read()
                    .keys()
                    .filter(|index| index.table == table)
                    .cloned(),
            ) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.partition_snapshots() {
                    // Probe the existing row directory: a single-row public
                    // read must not enumerate every other vector on each call.
                    // All versions still select their owning partitions.
                    let contains_selected_row = {
                        let by_row = state.raw_directory_by_row.read();
                        row_ids.iter().any(|row_id| by_row.contains_key(row_id))
                    };
                    if !contains_selected_row {
                        continue;
                    }
                    state.ensure_raw_vectors_loaded()?;
                    loaded.push(VectorPartitionRef::new(index.clone(), partition_key));
                }
            }
            Ok(loaded)
        })
    }

    pub fn all_partitioned_entries(&self) -> Vec<PartitionedVectorEntry> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.partition_snapshots() {
                    entries.extend(
                        state
                            .all_entries(&index)
                            .into_iter()
                            .map(|entry| PartitionedVectorEntry::new(partition_key.clone(), entry)),
                    );
                }
            }
            entries
        })
    }

    #[doc(hidden)]
    pub fn try_all_partitioned_entries(&self) -> Result<Vec<PartitionedVectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.partition_snapshots() {
                    state.ensure_raw_vectors_loaded()?;
                    entries.extend(
                        state
                            .all_entries(&index)
                            .into_iter()
                            .map(|entry| PartitionedVectorEntry::new(partition_key.clone(), entry)),
                    );
                }
            }
            Ok(entries)
        })
    }

    pub fn prune_row_ids(&self, row_ids: &HashSet<RowId>, accountant: &dyn MemoryBudget) -> usize {
        self.with_bulk_maintenance(|| {
            #[cfg(feature = "test-seams")]
            self.pause_registry.maybe_pause(
                &contextdb_core::VectorIndexRef::default(),
                crate::test_seam::PauseWindow::Bulk,
            );
            let mut released = 0usize;
            let indexes = Self::sorted_refs(self.registry.read().keys().cloned());
            // Sampled before anything is removed: the directory owner returns
            // exactly the identity this pass physically removes (entries and
            // emptied partition states), once, and never estimates it
            // elsewhere.
            let sampled = self.directory_bytes_before_reclamation(&indexes);
            for index in &indexes {
                let Some(column) = self.try_column(index) else {
                    continue;
                };
                for (_, state) in column.partition_snapshots() {
                    // Only a partition whose directory names one of the rows
                    // is pruned; a dormant partition prunes its directory
                    // alone and stays dormant (see
                    // `IndexState::prune_directory_versions`).
                    if state.directory_names_any_row(row_ids) {
                        released = released
                            .saturating_add(state.prune_directory_versions(|entry| {
                                !row_ids.contains(&entry.row_id)
                            }));
                    }
                    state.clear_hnsw(accountant);
                }
                column.remove_empty_partitioned_states();
                column.rebuild_current_rows();
            }
            self.reconcile_directory_accounting_for_indexes(&sampled);
            released
        })
    }

    /// Remove exactly the named SUPERSEDED vector-entry versions -- the
    /// vector copy attached to a relational row VERSION that version cleanup
    /// released, identified by `(row_id, created_tx, lsn)` (the same triple
    /// that names the row version), so the row's CURRENT vector entry (a
    /// different `created_tx`/`lsn`) is never touched. This is the
    /// counterpart of `prune_row_ids`, which removes EVERY entry for a row
    /// whose live version is entirely gone (retention); this removes only
    /// specific superseded copies while the row stays live, so it never
    /// mutates an immutable HNSW graph. Old graph points remain until generation
    /// replacement; candidate merge checks snapshot visibility and resolves each
    /// logical row's native score from its visible body before ranking. A single
    /// remaining directory version does not make an old graph score current.
    ///
    /// Only the indexes the identities name are visited, and within an index
    /// only the partitions whose directory names one of the rows; a dormant
    /// partition holding a named version prunes its directory alone and stays
    /// dormant (see `IndexState::prune_directory_versions`). The caller has
    /// already removed the same versions durably, so a version pruned here is
    /// gone on the partition's next load and after any restart.
    ///
    /// Returns the bytes the caller's own admission still holds for the
    /// dropped bodies (a state carrying the accountant returns its bodies from
    /// its own record inside the prune and reports `0`); matching
    /// `prune_row_ids`'s contract, the CALLER releases them on the shared
    /// accountant (this never releases the reported bytes internally, so a
    /// caller summing several populations into one `accountant.release(...)`
    /// call never double-releases).
    pub fn prune_superseded_versions(&self, versions: &[VectorVersionIdentity]) -> usize {
        if versions.is_empty() {
            return 0;
        }
        let mut by_index: HashMap<VectorIndexRef, HashSet<(RowId, TxId, Lsn)>> = HashMap::new();
        for identity in versions {
            by_index
                .entry(identity.index.clone())
                .or_default()
                .insert(identity.version());
        }
        self.with_bulk_maintenance(|| {
            let mut released = 0usize;
            let indexes = Self::sorted_refs(by_index.keys().cloned());
            // See `prune_row_ids`: sampled before removal so the directory
            // owner returns exactly this pass's own shrinkage.
            let sampled = self.directory_bytes_before_reclamation(&indexes);
            for index in &indexes {
                let Some(column) = self.try_column(index) else {
                    continue;
                };
                let Some(versions) = by_index.get(index) else {
                    continue;
                };
                let row_ids = versions
                    .iter()
                    .map(|(row_id, _, _)| *row_id)
                    .collect::<HashSet<_>>();
                for (_, state) in column.partition_snapshots() {
                    if !state.directory_names_any_row(&row_ids) {
                        continue;
                    }
                    released = released.saturating_add(state.prune_directory_versions(|entry| {
                        !versions.contains(&(entry.row_id, entry.created_tx, entry.lsn))
                    }));
                }
                column.remove_empty_partitioned_states();
                column.rebuild_current_rows();
            }
            self.reconcile_directory_accounting_for_indexes(&sampled);
            released
        })
    }

    /// Name every version of `index` that `versions` selects, read from the
    /// partitions' durable directories alone: no partition is faulted in, so
    /// an idle retained partition stays dormant while a superseded version it
    /// holds is still found and can be pruned durably.
    pub fn directory_versions_matching(
        &self,
        index: &VectorIndexRef,
        versions: &HashSet<(RowId, TxId, Lsn)>,
    ) -> Vec<VectorVersionIdentity> {
        if versions.is_empty() {
            return Vec::new();
        }
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Vec::new();
            };
            let row_ids = versions
                .iter()
                .map(|(row_id, _, _)| *row_id)
                .collect::<HashSet<_>>();
            let mut matched = Vec::new();
            for (_, state) in column.partition_snapshots() {
                if !state.directory_names_any_row(&row_ids) {
                    continue;
                }
                matched.extend(
                    state
                        .raw_directory
                        .read()
                        .iter()
                        .filter(|entry| {
                            versions.contains(&(entry.row_id, entry.created_tx, entry.lsn))
                        })
                        .map(|entry| VectorVersionIdentity {
                            index: index.clone(),
                            row_id: entry.row_id,
                            created_tx: entry.created_tx,
                            lsn: entry.lsn,
                        }),
                );
            }
            matched
        })
    }

    pub fn entries_for_index(&self, index: &VectorIndexRef) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            let mut entries = Vec::new();
            for (_, state) in column.partition_snapshots() {
                state.ensure_raw_vectors_loaded()?;
                let state_entries = state.all_entries(index);
                #[cfg(any(test, feature = "test-seams"))]
                self.full_index_entries_touched
                    .fetch_add(state_entries.len() as u64, Ordering::SeqCst);
                entries.extend(state_entries);
            }
            Ok(entries)
        })
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn reset_full_index_entries_touched_for_test(&self) {
        self.full_index_entries_touched.store(0, Ordering::SeqCst);
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn full_index_entries_touched_for_test(&self) -> u64 {
        self.full_index_entries_touched.load(Ordering::SeqCst)
    }

    /// Capture the finite entry prefix visible at a committed frontier. Writers
    /// only append or tombstone entries; each payload copy takes one short read
    /// lock, and a later tombstone cannot erase visibility at this snapshot.
    #[doc(hidden)]
    pub fn entries_for_partition_at_snapshot(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        snapshot: SnapshotId,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            state.ensure_raw_vectors_loaded()?;
            let count = state.vectors.read().entries.len();
            let mut entries = Vec::new();
            for position in 0..count {
                let entry = {
                    let vectors = state.vectors.read();
                    vectors
                        .entries
                        .get(position)
                        .filter(|entry| entry.created_tx.0 <= snapshot.0)
                        .map(|entry| entry.to_vector_entry(index.clone()))
                };
                if let Some(mut entry) = entry {
                    if entry.deleted_tx.is_some_and(|tx| tx.0 > snapshot.0) {
                        entry.deleted_tx = None;
                    }
                    entries.push(entry);
                }
            }
            Ok(entries)
        })
    }

    pub fn entries_for_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            state.ensure_raw_vectors_loaded()?;
            Ok(state.all_entries(index))
        })
    }

    /// Count one column at the caller's snapshot without materializing inspection
    /// rows or traversing retained vector bodies and reverse directories.
    #[doc(hidden)]
    pub fn visible_entry_count(&self, index: &VectorIndexRef, snapshot: SnapshotId) -> usize {
        self.with_bulk_read(|| {
            self.try_column(index).map_or(0, |column| {
                column
                    .partitions
                    .read()
                    .values()
                    .map(|state| state.directory_visible_entry_count(snapshot, None))
                    .sum()
            })
        })
    }

    pub fn vector_count(&self) -> usize {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .values()
                .map(|column| column.live_vector_count())
                .sum()
        })
    }

    pub fn has_hnsw_index(&self) -> bool {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .values()
                .any(|column| column.has_hnsw())
        })
    }

    pub fn has_hnsw_index_for(&self, index: &VectorIndexRef) -> bool {
        self.with_bulk_read(|| {
            self.try_state(index)
                .and_then(|state| state.hnsw_len())
                .is_some()
        })
    }

    pub fn has_hnsw_index_for_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> bool {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .and_then(|state| state.hnsw_len())
                .is_some()
        })
    }

    /// The purge planner uses this to preserve the materialization contract
    /// without causing previously lazy indexes to allocate a graph.
    pub fn materialized_hnsw_indexes(&self) -> Vec<VectorIndexRef> {
        self.with_bulk_read(|| {
            let indexes = self
                .registry
                .read()
                .iter()
                .filter(|(_, column)| column.has_hnsw())
                .map(|(index, _)| index.clone())
                .collect::<Vec<_>>();
            Self::sorted_refs(indexes)
        })
    }

    pub fn materialized_hnsw_partitions(&self) -> Vec<VectorPartitionRef> {
        self.with_bulk_read(|| {
            let mut materialized = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                materialized.extend(
                    column
                        .partition_snapshots()
                        .into_iter()
                        .filter(|(_, state)| state.hnsw_len().is_some())
                        .map(|(partition_key, _)| {
                            VectorPartitionRef::new(index.clone(), partition_key)
                        }),
                );
            }
            materialized
        })
    }

    /// Passive raw-vector residency observation for deterministic working-set
    /// proofs. Reading this bit never causes a load.
    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub fn raw_partition_resident_for_test(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> bool {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .is_some_and(|state| state.raw_vectors_resident())
        })
    }

    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub fn resident_raw_partition_count_for_test(&self) -> usize {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .values()
                .map(|column| {
                    column
                        .partition_snapshots()
                        .into_iter()
                        .filter(|(_, state)| state.raw_vectors_resident())
                        .count()
                })
                .sum()
        })
    }

    /// Drop every reloadable raw body after an explicit handle-recycle
    /// compaction. Durable directories and graph generations remain resident;
    /// a later exact or indexed read reloads only its selected partition.
    #[doc(hidden)]
    pub fn evict_reloadable_raw_partitions(&self) -> usize {
        self.with_bulk_maintenance(|| {
            self.registry
                .read()
                .values()
                .flat_map(|column| column.partition_snapshots())
                .filter(|(_, state)| state.evict_raw_vectors())
                .count()
        })
    }

    /// Evict one idle raw body while retaining its durable directory and
    /// loader. Graph generations are independent and remain untouched.
    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub fn evict_raw_partition_for_test(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> bool {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .is_some_and(|state| state.evict_raw_vectors())
        })
    }

    pub fn clear_hnsw(&self, accountant: &dyn MemoryBudget) {
        let _build_lifecycle = self.hnsw_build_gate.write();
        self.with_bulk_maintenance(|| {
            #[cfg(feature = "test-seams")]
            self.pause_registry.maybe_pause(
                &contextdb_core::VectorIndexRef::default(),
                crate::test_seam::PauseWindow::Bulk,
            );
            for column in self.registry.read().values() {
                column.clear_hnsw(accountant);
            }
        });
    }

    pub fn clear_hnsw_for(&self, index: &VectorIndexRef, accountant: &dyn MemoryBudget) {
        self.with_index_maintenance(index, || {
            if let Some(state) = self.try_state(index) {
                state.clear_hnsw(accountant);
            }
        });
    }

    pub fn clear_hnsw_for_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        accountant: &dyn MemoryBudget,
    ) {
        self.with_partition_maintenance(index, partition_key, || {
            if let Some(state) = self.try_partition_state(index, partition_key) {
                state.clear_hnsw(accountant);
            }
        });
    }

    /// Register durable graph metadata for one partition without loading it.
    #[doc(hidden)]
    pub fn register_dormant_partition_generation(
        &self,
        partition: &VectorPartitionRef,
        is_base: bool,
        descriptor: DormantVectorGraphGeneration,
        loader: Arc<dyn DormantVectorGraphLoader>,
    ) -> Result<()> {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            let state = self.partition_state(&partition.index, &partition.partition_key)?;
            #[cfg(feature = "test-seams")]
            if is_base {
                // A reopened process starts observation at its real durable
                // base. Historical high-water before this process is unknown.
                let directory = state.raw_directory.read();
                let mut backlog = state.base_backlog.lock();
                if backlog.generation == 0 {
                    backlog.generation = descriptor.identity.generation_id;
                    backlog.covered_tx = descriptor.identity.covered_tx.0;
                    backlog.pending = crate::observations::VectorBacklog {
                        inserts: directory
                            .iter()
                            .filter(|entry| {
                                entry.deleted_tx.is_none()
                                    && entry.created_tx.0 > backlog.covered_tx
                            })
                            .count(),
                        tombstones: directory
                            .iter()
                            .filter(|entry| {
                                entry.deleted_tx.is_some_and(|tx| tx.0 > backlog.covered_tx)
                            })
                            .count(),
                    };
                    backlog.record_high_water();
                }
            }
            state.register_dormant_sealed_generation(is_base, descriptor, loader)
        })
    }

    /// Install one already decoded, checksummed graph.  The codec/persistence
    /// owner reserves its resident bytes before this call; this method only
    /// transfers ownership at one atomic partition publication point.
    #[doc(hidden)]
    pub fn install_loaded_partition_generation(
        &self,
        partition: &VectorPartitionRef,
        is_base: bool,
        identity: VectorGraphGeneration,
        loaded: LoadedVectorGraphGeneration,
    ) -> Result<()> {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            let state = self.partition_state(&partition.index, &partition.partition_key)?;
            state.install_loaded_sealed_generation(is_base, identity, loaded)
        })
    }

    /// Atomically publish a prepared full base and retire the old mutable
    /// tail. Superseded sealed graphs remain pinned until the engine calls the
    /// reclaim method with no registered old snapshots.
    #[doc(hidden)]
    pub fn publish_partition_base_generation(
        &self,
        partition: &VectorPartitionRef,
        identity: VectorGraphGeneration,
        loaded: LoadedVectorGraphGeneration,
    ) -> Result<()> {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            let state = self.partition_state(&partition.index, &partition.partition_key)?;
            state.publish_replacement_base_generation(identity, loaded)
        })
    }

    /// Borrow the process-created tail graph for durable checkpoint encoding.
    /// The callback runs under this partition's maintenance lock, so a commit
    /// cannot extend the graph midway through serialization.
    #[doc(hidden)]
    pub fn with_partition_fresh_tail<R>(
        &self,
        partition: &VectorPartitionRef,
        f: impl FnOnce(&HnswIndex) -> R,
    ) -> Option<R> {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .and_then(|state| state.with_fresh_tail(f))
        })
    }

    /// Serialize one short durable publication with every publisher and
    /// maintenance mutation for the same route. Callers must preserve the
    /// engine lock order by taking the global commit lock first.
    #[doc(hidden)]
    pub fn with_partition_generation_publication<R>(
        &self,
        partition: &VectorPartitionRef,
        publish: impl FnOnce() -> R,
    ) -> R {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, publish)
    }

    /// Called after the durable catalog commit while the engine commit lock
    /// and this partition's maintenance boundary still exclude publishers.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn observe_durable_base_publication(
        &self,
        partition: &VectorPartitionRef,
        generation: u64,
        covered_tx: TxId,
    ) -> Result<()> {
        let state = self.partition_state(&partition.index, &partition.partition_key)?;
        let directory = state.raw_directory.read();
        let mut backlog = state.base_backlog.lock();
        if backlog.generation == generation {
            return Ok(());
        }
        let count = |covered: u64| crate::observations::VectorBacklog {
            inserts: directory
                .iter()
                .filter(|entry| entry.deleted_tx.is_none() && entry.created_tx.0 > covered)
                .count(),
            tombstones: directory
                .iter()
                .filter(|entry| entry.deleted_tx.is_some_and(|tx| tx.0 > covered))
                .count(),
        };
        backlog.pending = count(backlog.covered_tx);
        backlog.record_high_water();
        let after = count(covered_tx.0);
        let event = crate::observations::VectorBasePublicationEvent {
            partition: partition.clone(),
            previous_generation: backlog.generation,
            generation,
            covered_tx,
            pending_before: backlog.pending,
            pending_after: after,
            previous_generation_high_water: backlog.high_water,
            previous_generation_total_high_water: backlog.total_high_water,
            generation_high_water: after,
        };
        *backlog = crate::observations::BaseBacklog {
            generation,
            covered_tx: covered_tx.0,
            pending: after,
            high_water: after,
            total_high_water: after.inserts.saturating_add(after.tombstones),
        };
        drop(backlog);
        drop(directory);
        crate::observations::emit(&event);
        Ok(())
    }

    /// Start the one mutable post-generation tail. An empty graph owns no
    /// entry bytes but gives later commits the fresh in-process insertion
    /// target that sealed graphs deliberately refuse.
    #[doc(hidden)]
    pub fn initialize_partition_fresh_tail(
        &self,
        partition: &VectorPartitionRef,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Result<()> {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            let state = self.partition_state(&partition.index, &partition.partition_key)?;
            let policy = self
                .index_layout(&partition.index)?
                .resolve_policy(state.vector_count(), 1);
            state.initialize_empty_fresh_tail(accountant, policy)
        })
    }

    #[doc(hidden)]
    pub fn preload_dormant_partition_generation(
        &self,
        partition: &VectorPartitionRef,
        is_base: bool,
    ) -> Result<bool> {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            let state = self.partition_state(&partition.index, &partition.partition_key)?;
            state.preload_dormant_sealed_generation(is_base)
        })
    }

    #[doc(hidden)]
    pub fn evict_resident_partition_generation(
        &self,
        partition: &VectorPartitionRef,
        is_base: bool,
    ) -> bool {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .is_some_and(|state| state.evict_resident_sealed_generation(is_base))
        })
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn make_retired_partition_generations_dormant_for_test(
        &self,
        partition: &VectorPartitionRef,
    ) -> usize {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .map_or(0, |state| {
                    state.make_retired_sealed_generations_dormant_for_test()
                })
        })
    }

    #[doc(hidden)]
    pub fn partition_graph_generation_status(
        &self,
        partition: &VectorPartitionRef,
    ) -> Option<VectorGraphGenerationStatus> {
        self.with_bulk_read(|| {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .map(|state| state.graph_generation_status())
        })
    }

    /// Retain an actionable maintenance failure for passive inspection. The
    /// engine calls this when durable generation work fails outside the vector
    /// crate (for example disk admission); only a later successful generation
    /// publication clears the mark.
    #[doc(hidden)]
    pub fn record_partition_maintenance_failure(
        &self,
        partition: &VectorPartitionRef,
        failure: VectorMaintenanceFailure,
    ) -> Result<()> {
        self.record_partition_maintenance_failure_details(
            partition,
            VectorMaintenanceFailureDetails::from_failure(failure),
        )
    }

    #[doc(hidden)]
    pub fn record_partition_maintenance_failure_details(
        &self,
        partition: &VectorPartitionRef,
        details: VectorMaintenanceFailureDetails,
    ) -> Result<()> {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            let Some(state) = self.try_partition_state(&partition.index, &partition.partition_key)
            else {
                // A cold catalog-only route has no in-memory mark to update,
                // and DDL may remove a sampled route while maintenance runs.
                // The cycle report still owns the failure and closing work.
                return Ok(());
            };
            state.record_maintenance_failure(details);
            Ok(())
        })
    }

    /// Classify and retain a maintenance error raised by an engine-owned
    /// durable preparation or publication step outside this crate.
    #[doc(hidden)]
    pub fn record_partition_maintenance_error(
        &self,
        partition: &VectorPartitionRef,
        error: &Error,
    ) -> Result<()> {
        // An intentional stop leaves pending work and any earlier real failure intact.
        if matches!(error, Error::ReadCancelled) {
            return Ok(());
        }
        self.record_partition_maintenance_failure_details(
            partition,
            VectorMaintenanceFailureDetails::from_error(error),
        )
    }

    /// Clear a retained failure only after the caller has completed the
    /// matching durable publication successfully.
    #[doc(hidden)]
    pub fn clear_partition_maintenance_failure(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<()> {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            let state = self.partition_state(&partition.index, &partition.partition_key)?;
            state.clear_maintenance_failure();
            Ok(())
        })
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn dormant_partition_generation_load_bytes_for_test(
        &self,
        partition: &VectorPartitionRef,
        is_base: bool,
    ) -> Option<usize> {
        self.with_bulk_read(|| {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .and_then(|state| {
                    let sealed = state.sealed_hnsw.read();
                    if is_base {
                        sealed
                            .pending_base
                            .as_ref()
                            .or(sealed.dormant_base.as_ref())
                            .map(|generation| generation.descriptor.load_bytes)
                    } else {
                        sealed
                            .dormant_change
                            .as_ref()
                            .map(|generation| generation.descriptor.load_bytes)
                    }
                })
        })
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn partition_superseded_generation_retention_for_test(
        &self,
        partition: &VectorPartitionRef,
    ) -> (usize, usize) {
        self.with_bulk_read(|| {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .map(|state| state.superseded_generation_retention_for_test())
                .unwrap_or((0, 0))
        })
    }

    #[doc(hidden)]
    pub fn with_resident_partition_generation<R>(
        &self,
        partition: &VectorPartitionRef,
        is_base: bool,
        f: impl FnOnce(VectorGraphGeneration, &HnswIndex) -> R,
    ) -> Option<R> {
        self.with_bulk_read(|| {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .and_then(|state| state.with_resident_sealed_generation(is_base, f))
        })
    }

    /// Durable chains still referenced after a guarded generation reclamation.
    #[doc(hidden)]
    pub fn snapshot_retained_partition_chain_ids(
        &self,
        partition: &VectorPartitionRef,
        snapshots: &[SnapshotId],
    ) -> Vec<(u64, Option<u64>)> {
        self.try_partition_state(&partition.index, &partition.partition_key)
            .map_or_else(Vec::new, |state| {
                state.snapshot_retained_chain_ids(snapshots)
            })
    }

    #[doc(hidden)]
    pub fn retained_partition_chain_ids(
        &self,
        partition: &VectorPartitionRef,
    ) -> Vec<(u64, Option<u64>)> {
        self.with_bulk_read(|| {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .map_or_else(Vec::new, |state| state.retained_chain_ids())
        })
    }

    /// The caller holds its snapshot-removal guard. Return the number of released graphs.
    #[doc(hidden)]
    pub fn reclaim_snapshot_free_partition_graph_generations(
        &self,
        partition: &VectorPartitionRef,
        registered_snapshots: &[SnapshotId],
    ) -> usize {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .map(|state| state.reclaim_snapshot_free_graph_generations(registered_snapshots))
                .unwrap_or(0)
        })
    }

    #[doc(hidden)]
    pub fn quarantine_partition_generation(
        &self,
        partition: &VectorPartitionRef,
        is_base: bool,
        generation_id: u64,
    ) -> bool {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .is_some_and(|state| state.quarantine_sealed_generation(is_base, generation_id))
        })
    }

    /// Mark one live raw-vector route unavailable without inventing a graph
    /// descriptor or touching any authoritative vector body.
    #[doc(hidden)]
    pub fn quarantine_partition_route(
        &self,
        partition: &VectorPartitionRef,
        reason: VectorRouteQuarantineReason,
    ) -> bool {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .is_some_and(|state| {
                    state.set_route_quarantine(reason);
                    true
                })
        })
    }

    #[doc(hidden)]
    pub fn clear_partition_route_quarantine(&self, partition: &VectorPartitionRef) -> bool {
        self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .is_some_and(|state| {
                    state.clear_route_quarantine();
                    true
                })
        })
    }

    #[doc(hidden)]
    pub fn partition_route_quarantine(
        &self,
        partition: &VectorPartitionRef,
    ) -> Option<VectorRouteQuarantineReason> {
        self.with_bulk_read(|| {
            self.try_partition_state(&partition.index, &partition.partition_key)
                .and_then(|state| state.route_quarantine())
        })
    }

    #[doc(hidden)]
    pub fn partition_route_quarantines(
        &self,
    ) -> Vec<(VectorPartitionRef, VectorRouteQuarantineReason)> {
        self.with_bulk_read(|| {
            let mut quarantines = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.maintained_partition_snapshots() {
                    if let Some(reason) = state.route_quarantine() {
                        quarantines.push((
                            VectorPartitionRef::new(index.clone(), partition_key),
                            reason,
                        ));
                    }
                }
            }
            quarantines
        })
    }

    pub fn raw_hnsw_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Option<Result<Vec<(RowId, f32)>>> {
        self.with_bulk_read(|| {
            self.try_state(index)
                .and_then(|state| state.raw_hnsw_search(index, query, k))
        })
    }

    pub fn raw_hnsw_search_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        query: &[f32],
        k: usize,
    ) -> Option<Result<Vec<(RowId, f32)>>> {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .and_then(|state| state.raw_hnsw_search(index, query, k))
        })
    }

    pub fn raw_hnsw_entry_count_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
    ) -> Option<usize> {
        self.with_bulk_read(|| {
            self.try_state(index)
                .and_then(|state| state.raw_hnsw_entry_count_for_row(row_id))
        })
    }

    pub fn raw_hnsw_entry_count_for_row_in_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
    ) -> Option<usize> {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .and_then(|state| state.raw_hnsw_entry_count_for_row(row_id))
        })
    }

    pub fn raw_hnsw_topology_digest_for_test(&self, index: &VectorIndexRef) -> Option<u64> {
        self.with_bulk_read(|| {
            self.try_column(index).and_then(|column| {
                column
                    .maintained_partition_snapshots()
                    .into_iter()
                    .find_map(|(_, state)| state.raw_hnsw_topology_digest_for_test())
            })
        })
    }

    pub fn raw_hnsw_topology_digest_for_partition_for_test(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Option<u64> {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .and_then(|state| state.raw_hnsw_topology_digest_for_test())
        })
    }

    pub fn raw_hnsw_build_serial_for_test(&self, index: &VectorIndexRef) -> Option<u64> {
        self.with_bulk_read(|| {
            self.try_column(index).and_then(|column| {
                column
                    .maintained_partition_snapshots()
                    .into_iter()
                    .find_map(|(_, state)| state.raw_hnsw_build_serial_for_test())
            })
        })
    }

    pub fn raw_hnsw_build_serial_for_partition_for_test(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Option<u64> {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .and_then(|state| state.raw_hnsw_build_serial_for_test())
        })
    }

    pub fn find_by_row_id(&self, row_id: RowId) -> Option<VectorEntry> {
        self.with_bulk_read(|| {
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.partition_snapshots() {
                    if let Some(entry) = state.find_by_row_id(&index, row_id) {
                        return Some(entry);
                    }
                }
            }
            None
        })
    }

    pub fn live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Option<VectorEntry> {
        self.with_bulk_read(|| {
            self.try_state(index)
                .and_then(|state| state.visible_entry_by_row(index, row_id, snapshot))
        })
    }

    /// Load only the partition whose durable directory says it can answer
    /// this row at the requested snapshot, then return that stored value.
    /// Score one logical identity without hydrating a partition or converting
    /// its stored quantization into a different arithmetic representation.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_score_live_row<E: From<Error>>(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        mut before_checkpoint: impl FnMut() -> std::result::Result<(), E>,
        mut before_distance: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<Option<f32>, E> {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(None);
            };
            let score = |state: &IndexState,
                         checkpoint: &mut _,
                         distance: &mut _,
                         retain: &mut _,
                         release: &mut _| {
                state.bounded_score_visible_candidate(
                    index, row_id, snapshot, query, checkpoint, distance, retain, release,
                )
            };
            let current = column.current_partition(row_id);
            if let Some(key) = current.as_ref()
                && let Some(state) = column.partition_state(key)
                && let Some(value) = score(
                    &state,
                    &mut before_checkpoint,
                    &mut before_distance,
                    &mut before_retain,
                    &mut release_retained,
                )?
            {
                return Ok(Some(value));
            }
            // Historical placement is metadata-only; every inspected partition
            // is cancellable and charged before its row directory is touched.
            for (key, state) in column.partitions.read().iter() {
                if current.as_ref() == Some(key) {
                    continue;
                }
                before_checkpoint()?;
                if !state.directory_has_visible_row(row_id, snapshot) {
                    continue;
                }
                if let Some(value) = score(
                    state,
                    &mut before_checkpoint,
                    &mut before_distance,
                    &mut before_retain,
                    &mut release_retained,
                )? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        })
    }

    pub fn load_live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Result<Option<VectorEntry>> {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(None);
            };
            // The live row-to-partition directory is the primary source for
            // current reads. Consult it before the historical directory walk:
            // newly committed partitioned rows already have this canonical
            // ownership even when a persisted-directory image has not yet
            // been rebuilt by maintenance.
            if let Some(partition_key) = column.current_partition(row_id)
                && let Some(state) = column.partition_state(&partition_key)
            {
                state.ensure_raw_vectors_loaded()?;
                if let Some(entry) = state.visible_entry_by_row(index, row_id, snapshot) {
                    return Ok(Some(entry));
                }
            }
            for (_, state) in column.partition_snapshots() {
                if !state.directory_has_visible_row(row_id, snapshot) {
                    continue;
                }
                state.ensure_raw_vectors_loaded()?;
                if let Some(entry) = state.visible_entry_by_row(index, row_id, snapshot) {
                    return Ok(Some(entry));
                }
            }
            Ok(None)
        })
    }

    pub fn live_entries_for_row(
        &self,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Vec<VectorEntry> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                entries.extend(
                    column
                        .partition_snapshots()
                        .into_iter()
                        .filter_map(|(_, state)| {
                            state.visible_entry_by_row(&index, row_id, snapshot)
                        }),
                );
            }
            entries
        })
    }

    /// Whether a row has a visible vector in this column. This consults only
    /// the lightweight durable directory and never faults a body into memory.
    pub fn has_live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> bool {
        self.with_bulk_read(|| {
            self.try_column(index).is_some_and(|column| {
                let current = column.current_partition(row_id);
                let partitions = column.partitions.read();
                if current
                    .as_ref()
                    .and_then(|key| partitions.get(key))
                    .is_some_and(|state| state.directory_has_visible_row(row_id, snapshot))
                {
                    return true;
                }
                partitions.iter().any(|(key, state)| {
                    current.as_ref() != Some(key)
                        && state.directory_has_visible_row(row_id, snapshot)
                })
            })
        })
    }

    /// Whether a row has any visible vector column. This is the body-free
    /// existence check used by row delete and replacement planning.
    pub fn has_any_live_entry_for_row(
        &self,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> bool {
        self.with_bulk_read(|| {
            self.registry.read().values().any(|column| {
                column
                    .partition_snapshots()
                    .into_iter()
                    .any(|(_, state)| state.directory_has_visible_row(row_id, snapshot))
            })
        })
    }

    /// Load only partitions that contain a visible value for this row, then
    /// return every vector column owned by that row.
    pub fn load_live_entries_for_row(
        &self,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.partition_snapshots() {
                    if !state.directory_has_visible_row(row_id, snapshot) {
                        continue;
                    }
                    state.ensure_raw_vectors_loaded()?;
                    if let Some(entry) = state.visible_entry_by_row(&index, row_id, snapshot) {
                        entries.push(entry);
                    }
                }
            }
            Ok(entries)
        })
    }

    /// Materialize only partitions containing one of the selected row ids and
    /// return every retained vector version for those rows.
    pub fn load_entries_for_rows(&self, row_ids: &HashSet<RowId>) -> Result<Vec<VectorEntry>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in Self::sorted_refs(self.registry.read().keys().cloned()) {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.partition_snapshots() {
                    let contains_selected_row = state
                        .raw_directory
                        .read()
                        .iter()
                        .any(|entry| row_ids.contains(&entry.row_id));
                    if !contains_selected_row {
                        continue;
                    }
                    state.ensure_raw_vectors_loaded()?;
                    entries.extend(
                        state
                            .all_entries(&index)
                            .into_iter()
                            .filter(|entry| row_ids.contains(&entry.row_id)),
                    );
                }
            }
            Ok(entries)
        })
    }

    /// Stored body bytes represented by the lightweight directories. This is
    /// independent of cache residency and therefore remains exact after open
    /// and eviction.
    pub fn live_vector_payload_bytes(&self) -> usize {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .values()
                .flat_map(|column| column.partition_snapshots())
                .fold(0usize, |bytes, (_, state)| {
                    bytes.saturating_add(state.live_payload_bytes())
                })
        })
    }

    pub fn vector_for_row_lsn(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        lsn: contextdb_core::Lsn,
    ) -> Option<Vec<f32>> {
        self.with_bulk_read(|| {
            self.try_state(index)
                .and_then(|state| state.vector_for_row_lsn(row_id, lsn))
        })
    }

    pub fn vector_for_row_lsn_in_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
        lsn: Lsn,
    ) -> Option<Vec<f32>> {
        self.with_bulk_read(|| {
            self.try_partition_state(index, partition_key)
                .and_then(|state| state.vector_for_row_lsn(row_id, lsn))
        })
    }

    /// Resolve an exact row version through the lightweight directories and
    /// load only the partition that owns that version.
    pub fn load_vector_for_row_lsn(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        lsn: Lsn,
    ) -> Result<Option<Vec<f32>>> {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(None);
            };
            for (_, state) in column.partition_snapshots() {
                if !state.directory_has_row_lsn(row_id, lsn) {
                    continue;
                }
                state.ensure_raw_vectors_loaded()?;
                if let Some(vector) = state.vector_for_row_lsn(row_id, lsn) {
                    return Ok(Some(vector));
                }
            }
            Ok(None)
        })
    }

    pub fn storage_bytes_per_entry(&self, index: &VectorIndexRef) -> Result<Vec<usize>> {
        self.with_bulk_read(|| {
            let state = self.state(index)?;
            state.ensure_raw_vectors_loaded()?;
            Ok(state.storage_bytes_per_entry())
        })
    }

    pub fn storage_bytes_per_entry_in_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<Vec<usize>> {
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            state.ensure_raw_vectors_loaded()?;
            Ok(state.storage_bytes_per_entry())
        })
    }

    pub fn live_partition_count(&self, index: &VectorIndexRef) -> Result<usize> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.live_partition_count())
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    pub fn retained_partition_count(&self, index: &VectorIndexRef) -> Result<usize> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.retained_partition_count())
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    /// Number of partition states that count against MAX_PARTITIONS: each
    /// state with at least one live row or one retained historical row.
    pub fn live_or_retained_partition_count(&self, index: &VectorIndexRef) -> Result<usize> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.live_or_retained_partition_count())
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    pub fn retained_bytes_for_index(&self, index: &VectorIndexRef) -> Result<usize> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.retained_bytes(index))
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    /// Allocation-free receipt over the owners already resident in this
    /// store. It deliberately does not call a loader, eviction, or accounting
    /// reconciliation path.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn memory_ownership_snapshot_for_test(&self) -> VectorMemoryOwnershipSnapshot {
        let registry = self.registry.read();
        let mut receipt = VectorMemoryOwnershipSnapshot {
            temporary_workspace: self.workspace_bytes.load(Ordering::SeqCst),
            ..VectorMemoryOwnershipSnapshot::default()
        };
        for (index, column) in registry.iter() {
            let directory = column.directory_ownership(index);
            receipt.raw_reverse_directory = receipt
                .raw_reverse_directory
                .saturating_add(directory.raw_reverse_directory);
            receipt.partition_vectors = receipt
                .partition_vectors
                .saturating_add(directory.partition_vectors);
            receipt.typed_partition_keys = receipt
                .typed_partition_keys
                .saturating_add(directory.typed_partition_keys);
            let partitions = column.partitions.read();
            for state in partitions.values() {
                if state.raw_resident.load(Ordering::SeqCst) {
                    receipt.raw_vector_bodies = receipt
                        .raw_vector_bodies
                        .saturating_add(state.raw_resident_bytes.load(Ordering::SeqCst));
                }
                receipt.mutable_tail = receipt
                    .mutable_tail
                    .saturating_add(state.hnsw_bytes.load(Ordering::SeqCst));
                let sealed = state.sealed_hnsw.read();
                let current_base = sealed
                    .base
                    .as_ref()
                    .map(|generation| generation.bytes)
                    .unwrap_or(0);
                let current_change = sealed
                    .change
                    .as_ref()
                    .map(|generation| generation.bytes)
                    .unwrap_or(0);
                if state.vector_count() == 0 {
                    receipt.retired_pinned_graph_bytes = receipt
                        .retired_pinned_graph_bytes
                        .saturating_add(current_base)
                        .saturating_add(current_change);
                } else {
                    receipt.base_graph = receipt.base_graph.saturating_add(current_base);
                    receipt.change_graph = receipt.change_graph.saturating_add(current_change);
                }
                receipt.retired_pinned_graph_bytes = receipt
                    .retired_pinned_graph_bytes
                    .saturating_add(sealed.retired.iter().fold(0usize, |bytes, chain| {
                        bytes
                            .saturating_add(
                                chain
                                    .base
                                    .as_ref()
                                    .map(|generation| generation.bytes)
                                    .unwrap_or(0),
                            )
                            .saturating_add(
                                chain
                                    .change
                                    .as_ref()
                                    .map(|generation| generation.bytes)
                                    .unwrap_or(0),
                            )
                    }));
            }
        }
        receipt
    }

    /// Reserve and attach every vector allocation that is already resident
    /// when a database handle finishes opening. Writable file open normally
    /// contributes only lightweight directories here; an owned committed
    /// image may also arrive with raw bodies already resident. Attaching the
    /// exact reservations makes later column/table removal release the same
    /// bytes without guessing from the then-current cache contents.
    #[doc(hidden)]
    pub fn account_loaded_state(&self, accountant: Arc<dyn MemoryBudget>) -> Result<usize> {
        self.with_bulk_maintenance(|| {
            if self.memory_accountant.read().is_some() {
                return Err(Error::Other(
                    "vector memory accounting is already attached".to_string(),
                ));
            }
            let registry = self.registry.read();
            let mut directory = Vec::with_capacity(registry.len());
            let mut raw = Vec::<(Arc<IndexState>, usize)>::new();
            let mut total = 0usize;
            for (index, column) in registry.iter() {
                if column.directory_accounting.lock().is_some() {
                    return Err(Error::Other(format!(
                        "vector directory accounting is already attached for {}.{}",
                        index.table, index.column
                    )));
                }
                let bytes = column.directory_retained_bytes(index);
                total = total.saturating_add(bytes);
                directory.push((column.clone(), bytes));
                for (_, state) in column.partition_snapshots() {
                    if state.raw_residency_is_accounted() {
                        continue;
                    }
                    let bytes = state.raw_payload_bytes();
                    if bytes == 0 {
                        continue;
                    }
                    total = total.saturating_add(bytes);
                    raw.push((state, bytes));
                }
            }
            accountant.try_allocate_for(
                total,
                "open",
                "load_vector_directories",
                "Open the database with a larger MEMORY_LIMIT or reduce retained vector identities.",
            )?;
            for (column, bytes) in directory {
                column.install_directory_accounting(bytes, accountant.clone());
            }
            for (state, bytes) in raw {
                debug_assert!(state.raw_accountant.read().is_none());
                state.raw_resident_bytes.store(bytes, Ordering::SeqCst);
                *state.raw_accountant.write() = Some(accountant.clone());
            }
            *self.memory_accountant.write() = Some(accountant);
            Ok(total)
        })
    }

    /// Whether the state that currently owns this row also owns the raw-body
    /// reservation. A commit must not return that reservation merely because
    /// it tombstoned the body; physical pruning or cache/index removal is the
    /// one release.
    pub fn raw_entry_charge_owned_by_state(&self, index: &VectorIndexRef, row_id: RowId) -> bool {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return false;
            };
            let Some(partition_key) = column.current_partition(row_id) else {
                return false;
            };
            column
                .partition_state(&partition_key)
                .is_some_and(|state| state.raw_residency_is_accounted())
        })
    }

    pub fn index_infos(&self) -> Vec<VectorIndexInfo> {
        self.with_bulk_read(|| {
            let mut infos = self
                .registry
                .read()
                .iter()
                .map(|(index, column)| {
                    let layout = column.layout();
                    VectorIndexInfo {
                        index: index.clone(),
                        dimension: layout.dimension,
                        quantization: layout.quantization,
                        vector_count: column.live_vector_count(),
                        bytes: column.retained_bytes(index),
                    }
                })
                .collect::<Vec<_>>();
            infos.sort_by(|a, b| {
                a.index
                    .table
                    .cmp(&b.index.table)
                    .then(a.index.column.cmp(&b.index.column))
            });
            infos
        })
    }
    pub fn index_layout_infos(&self) -> Vec<VectorIndexLayoutInfo> {
        self.with_bulk_read(|| {
            Self::sorted_refs(self.registry.read().keys().cloned())
                .into_iter()
                .filter_map(|index| self.index_layout_info(&index).ok())
                .collect()
        })
    }

    pub fn build_lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.build_mutex.lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::VectorPartitionComponent;
    use std::hash::BuildHasher;

    #[derive(Debug, Default)]
    pub(super) struct PartitionCollisionHasher;

    impl std::hash::Hasher for PartitionCollisionHasher {
        fn finish(&self) -> u64 {
            0
        }
        fn write(&mut self, _: &[u8]) {}
    }

    #[test]
    fn compatibility_profile_edges_and_partial_policy_validation_agree() {
        for (quantization, expected) in [
            (
                VectorQuantization::F32,
                [
                    (16, 200, 5_000),
                    (24, 400, 400),
                    (24, 400, 400),
                    (16, 200, 200),
                ],
            ),
            (
                VectorQuantization::SQ8,
                [(8, 32, 96), (12, 64, 128), (12, 64, 128), (12, 64, 128)],
            ),
            (
                VectorQuantization::SQ4,
                [(8, 32, 96), (12, 64, 128), (12, 64, 128), (12, 64, 128)],
            ),
        ] {
            let silent = VectorIndexLayout::unpartitioned(3, quantization);
            for (count, expected) in [5_000, 5_001, 50_000, 50_001].into_iter().zip(expected) {
                let resolved = silent.resolve_policy(count, 1);
                assert_eq!(
                    (
                        resolved.hnsw_m,
                        resolved.hnsw_ef_construction,
                        resolved.hnsw_ef_search
                    ),
                    expected,
                    "{quantization:?} at {count}"
                );
            }
            let (too_small, valid) = if quantization == VectorQuantization::F32 {
                (20, 24)
            } else {
                (10, 12)
            };
            let partial = silent
                .clone()
                .with_policy(None, None, Some(too_small), None, 1);
            let at_zero = partial.resolve_policy(0, 1);
            assert!(at_zero.hnsw_ef_construction >= at_zero.hnsw_m);
            assert!(
                !partial.hnsw_policy_is_valid(),
                "validation includes the next profile even on an empty column"
            );
            assert!(
                silent
                    .clone()
                    .with_policy(None, None, Some(valid), None, 1)
                    .hnsw_policy_is_valid()
            );
            assert!(
                silent
                    .with_policy(None, Some(u32::MAX), Some(u32::MAX), Some(u32::MAX), 1)
                    .hnsw_policy_is_valid(),
                "validation adds no private numeric maximum"
            );
        }
    }

    #[test]
    fn visible_id_work_skips_obsolete_directory_intervals() {
        for history in [0, 64, 4096_u64] {
            let state = IndexState::new(2, VectorQuantization::F32);
            let mut directory = Vec::new();
            for version in 0..history {
                directory.push(RawVectorDirectoryEntry {
                    row_id: RowId(1),
                    created_tx: TxId(version + 1),
                    deleted_tx: Some(TxId(version + 2)),
                    lsn: Lsn(version + 1),
                });
            }
            directory.push(RawVectorDirectoryEntry {
                row_id: RowId(1),
                created_tx: TxId(history + 1),
                deleted_tx: None,
                lsn: Lsn(history + 1),
            });
            *state.raw_directory.write() = directory;
            state.rebuild_raw_directory_positions();
            for snapshot in [1, history / 2 + 1, history + 1, u64::MAX] {
                let mut work = 0;
                let mut ids = Vec::new();
                state
                    .bounded_visit_visible_ids(
                        SnapshotId(snapshot),
                        || {
                            work += 1;
                            Ok::<_, Error>(())
                        },
                        |id| {
                            ids.push(id);
                            Ok(())
                        },
                    )
                    .unwrap();
                eprintln!(
                    "snapshot={snapshot} history={history} visible={} directory_work={work}",
                    ids.len()
                );
                assert_eq!(ids, vec![RowId(1)]);
                assert!(
                    work <= 64,
                    "membership work must skip obsolete and future intervals: {work}"
                );
            }
        }
    }

    #[derive(Debug, Default)]
    struct CountingMemoryBudget {
        limit: Option<usize>,
        live_bytes: AtomicUsize,
        build_reservation_bytes: AtomicUsize,
    }

    impl CountingMemoryBudget {
        fn with_limit(limit: usize) -> Self {
            Self {
                limit: Some(limit),
                ..Self::default()
            }
        }

        fn live_bytes(&self) -> usize {
            self.live_bytes.load(Ordering::SeqCst)
        }

        fn build_reservation_bytes(&self) -> usize {
            self.build_reservation_bytes.load(Ordering::SeqCst)
        }
    }

    impl MemoryBudget for CountingMemoryBudget {
        fn try_allocate_for(
            &self,
            bytes: usize,
            subsystem: &str,
            operation: &str,
            hint: &str,
        ) -> Result<()> {
            if operation.starts_with("build_hnsw@") {
                self.build_reservation_bytes.store(bytes, Ordering::SeqCst);
            }
            loop {
                let used = self.live_bytes.load(Ordering::SeqCst);
                let available = self
                    .limit
                    .map_or(usize::MAX, |limit| limit.saturating_sub(used));
                if bytes > available {
                    return Err(Error::MemoryBudgetExceeded {
                        subsystem: subsystem.to_owned(),
                        operation: operation.to_owned(),
                        requested_bytes: bytes,
                        available_bytes: available,
                        budget_limit_bytes: self.limit.unwrap_or(usize::MAX),
                        hint: hint.to_owned(),
                    });
                }
                if self
                    .live_bytes
                    .compare_exchange(
                        used,
                        used.saturating_add(bytes),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    return Ok(());
                }
            }
        }

        fn release(&self, bytes: usize) {
            self.live_bytes.fetch_sub(bytes, Ordering::SeqCst);
        }

        fn available_bytes(&self) -> Option<usize> {
            self.limit
                .map(|limit| limit.saturating_sub(self.live_bytes()))
        }
    }

    #[derive(Debug, Default)]
    struct RefuseSampledBuildMemoryBudget {
        live_bytes: AtomicUsize,
    }

    impl MemoryBudget for RefuseSampledBuildMemoryBudget {
        fn try_allocate_for(
            &self,
            bytes: usize,
            subsystem: &str,
            operation: &str,
            hint: &str,
        ) -> Result<()> {
            if operation.starts_with("build_hnsw@") {
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned(),
                    operation: operation.to_owned(),
                    requested_bytes: bytes,
                    available_bytes: 0,
                    budget_limit_bytes: self.live_bytes.load(Ordering::SeqCst),
                    hint: hint.to_owned(),
                });
            }
            self.live_bytes.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.live_bytes.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    struct CandidateOnlyRawLoader {
        entry: VectorEntry,
        transient_bytes: usize,
        accountant: Arc<CountingMemoryBudget>,
        point_reads: AtomicUsize,
        bulk_reads: AtomicUsize,
    }

    fn raw_identity(entry: &VectorEntry) -> RawVectorDirectoryEntry {
        RawVectorDirectoryEntry {
            row_id: entry.row_id,
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        }
    }

    impl DormantRawVectorLoader for CandidateOnlyRawLoader {
        fn load(&self, _directory: &[RawVectorDirectoryEntry]) -> Result<LoadedRawVectorPartition> {
            self.bulk_reads.fetch_add(1, Ordering::SeqCst);
            Err(Error::Other(
                "candidate-only test loader rejects bulk hydration".to_string(),
            ))
        }

        fn load_candidate(
            &self,
            identity: &RawVectorDirectoryEntry,
            caller_charged: bool,
            request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
        ) -> Result<LoadedRawVectorCandidate> {
            assert_eq!(identity, &raw_identity(&self.entry));
            if !request(DormantVectorLoadRequest::Checkpoint) {
                return Err(Error::ReadCancelled);
            }
            if caller_charged {
                if !request(DormantVectorLoadRequest::Reserve(self.transient_bytes)) {
                    return Err(Error::ReadCancelled);
                }
            } else {
                self.accountant.try_allocate_for(
                    self.transient_bytes,
                    "vector_store",
                    "candidate_test",
                    "test reservation",
                )?;
            }
            if !request(DormantVectorLoadRequest::Checkpoint) {
                if !caller_charged {
                    self.accountant.release(self.transient_bytes);
                }
                return Err(Error::ReadCancelled);
            }
            self.point_reads.fetch_add(1, Ordering::SeqCst);
            Ok(if caller_charged {
                LoadedRawVectorCandidate::caller_charged(self.entry.clone(), self.transient_bytes)
            } else {
                LoadedRawVectorCandidate::loader_charged(
                    self.entry.clone(),
                    self.transient_bytes,
                    self.accountant.clone(),
                )
            })
        }
    }

    fn index() -> VectorIndexRef {
        VectorIndexRef::new("items", "embedding")
    }

    fn partitioned_layout(max_partitions: u32) -> VectorIndexLayout {
        VectorIndexLayout::new(
            2,
            VectorQuantization::F32,
            vec!["scope".to_owned()],
            Some(max_partitions),
            VectorSearchMode::Auto,
        )
    }

    fn text_key(value: impl Into<String>) -> VectorPartitionKey {
        VectorPartitionKey::from_components(vec![VectorPartitionComponent::Text(value.into())])
    }

    fn integer_key(value: i64) -> VectorPartitionKey {
        VectorPartitionKey::from_components(vec![VectorPartitionComponent::Integer(value)])
    }

    fn entry(index: &VectorIndexRef, row: u64, tx: TxId, vector: [f32; 2]) -> VectorEntry {
        VectorEntry {
            index: index.clone(),
            row_id: RowId(row),
            vector: vector.to_vec(),
            created_tx: tx,
            deleted_tx: None,
            lsn: Lsn(tx.0),
        }
    }

    #[test]
    fn dormant_candidate_scoring_charges_releases_and_cancels_without_bulk_hydration() {
        let index = index();
        let entry = entry(&index, 7, TxId(3), [1.0, 0.0]);
        let accountant = Arc::new(CountingMemoryBudget::default());
        let loader = Arc::new(CandidateOnlyRawLoader {
            entry: entry.clone(),
            transient_bytes: 64,
            accountant: accountant.clone(),
            point_reads: AtomicUsize::new(0),
            bulk_reads: AtomicUsize::new(0),
        });
        let state = IndexState::new(2, VectorQuantization::F32);
        state
            .install_dormant_raw_directory(vec![raw_identity(&entry)], loader.clone())
            .unwrap();

        let ordinary = state
            .score_visible_candidate(&index, RowId(7), SnapshotId(3), &[1.0, 0.0])
            .unwrap();
        assert_eq!(ordinary, Some(1.0));
        assert_eq!(accountant.live_bytes(), 0);

        let mut checkpoints = 0usize;
        let mut distances = 0usize;
        let mut reserved = 0usize;
        let mut released = 0usize;
        let bounded = state
            .bounded_score_visible_candidate(
                &index,
                RowId(7),
                SnapshotId(3),
                &[1.0, 0.0],
                &mut || {
                    checkpoints += 1;
                    Ok::<(), Error>(())
                },
                &mut || {
                    distances += 1;
                    Ok::<(), Error>(())
                },
                &mut |bytes| {
                    reserved += bytes;
                    Ok::<(), Error>(())
                },
                &mut |bytes| released += bytes,
            )
            .unwrap();
        assert_eq!(bounded, Some(1.0));
        assert_eq!(checkpoints, 5);
        assert_eq!(distances, 1);
        assert_eq!(reserved, 64);
        assert_eq!(released, 64);
        assert_eq!(accountant.live_bytes(), 0);

        let mut cancellation_checkpoints = 0usize;
        let mut cancellation_reserved = 0usize;
        let mut cancellation_released = 0usize;
        let cancelled = state.bounded_score_visible_candidate(
            &index,
            RowId(7),
            SnapshotId(3),
            &[1.0, 0.0],
            &mut || {
                cancellation_checkpoints += 1;
                if cancellation_checkpoints == 4 {
                    Err(Error::ReadCancelled)
                } else {
                    Ok(())
                }
            },
            &mut || panic!("cancelled point read must not reach distance scoring"),
            &mut |bytes| {
                cancellation_reserved += bytes;
                Ok::<(), Error>(())
            },
            &mut |bytes| cancellation_released += bytes,
        );
        assert!(matches!(cancelled, Err(Error::ReadCancelled)));
        assert_eq!(cancellation_reserved, 64);
        assert_eq!(cancellation_released, 64);
        assert_eq!(loader.point_reads.load(Ordering::SeqCst), 2);
        assert_eq!(loader.bulk_reads.load(Ordering::SeqCst), 0);
        assert!(!state.raw_resident.load(Ordering::SeqCst));
    }

    #[test]
    fn dormant_candidate_scoring_uses_each_declared_stored_representation() {
        let index = index();
        let query = [0.91, 0.41];
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let entry = entry(&index, 9, TxId(5), [0.13, 0.87]);
            let expected = StoredVectorEntry::from_vector_entry_ref(&entry, quantization)
                .vector
                .cosine_similarity(&query);
            let accountant = Arc::new(CountingMemoryBudget::default());
            let loader = Arc::new(CandidateOnlyRawLoader {
                entry: entry.clone(),
                transient_bytes: 64,
                accountant: accountant.clone(),
                point_reads: AtomicUsize::new(0),
                bulk_reads: AtomicUsize::new(0),
            });
            let state = IndexState::new(2, quantization);
            state
                .install_dormant_raw_directory(vec![raw_identity(&entry)], loader.clone())
                .unwrap();

            let actual = state
                .score_visible_candidate(&index, RowId(9), SnapshotId(5), &query)
                .unwrap()
                .expect("the exact dormant identity is visible");
            assert_eq!(actual, expected);
            assert_eq!(loader.point_reads.load(Ordering::SeqCst), 1);
            assert_eq!(loader.bulk_reads.load(Ordering::SeqCst), 0);
            assert_eq!(accountant.live_bytes(), 0);
        }
    }

    fn partitioned_entry(
        index: &VectorIndexRef,
        key: &VectorPartitionKey,
        row: u64,
        tx: TxId,
        vector: [f32; 2],
    ) -> PartitionedVectorEntry {
        PartitionedVectorEntry::new(key.clone(), entry(index, row, tx, vector))
    }

    #[test]
    fn unpartitioned_registration_keeps_one_explicit_empty_state() {
        let store = VectorStore::default();
        let index = index();
        store.register_index(index.clone(), 2, VectorQuantization::F32);

        assert!(store.contains_index(&index));
        assert_eq!(
            store.partition_keys(&index).unwrap(),
            vec![VectorPartitionKey::unpartitioned()]
        );
        assert_eq!(store.state(&index).unwrap().entry_count(), 0);
        let empty = store.index_layout_info(&index).unwrap();
        assert_eq!(empty.partition_states, 1);
        assert_eq!(empty.live_partitions, 0);
        assert_eq!(empty.retained_partitions, 0);
        assert_eq!(empty.live_or_retained_partitions, 0);

        store.apply_inserts(vec![entry(&index, 1, TxId(1), [1.0, 0.0])]);
        assert_eq!(store.entries_for_index(&index).unwrap().len(), 1);
        assert_eq!(
            store.current_partition_for_row(&index, RowId(1)),
            Some(VectorPartitionKey::unpartitioned())
        );
    }

    #[test]
    fn typed_partition_keys_remain_distinct_and_inspection_separates_retention() {
        let store = VectorStore::default();
        let index = index();
        store
            .register_index_with_layout(index.clone(), partitioned_layout(4))
            .unwrap();
        let integer = integer_key(7);
        let text = text_key("7");

        // Observe the real prepared admission set, not a separate toy map.
        let prepared = store
            .prepare_partitioned_batch(
                vec![],
                vec![
                    partitioned_entry(&index, &integer, 1, TxId(1), [1.0, 0.0]),
                    partitioned_entry(&index, &text, 2, TxId(2), [0.0, 1.0]),
                ],
                vec![],
                HashSet::new(),
            )
            .unwrap();
        let admitted = &prepared.requested[&index];
        assert_eq!(
            admitted.hasher().hash_one(&integer),
            admitted.hasher().hash_one(&text)
        );
        assert_eq!(
            admitted.len(),
            2,
            "a hash collision cannot merge admitted identities"
        );
        assert_eq!(
            prepared.projected_live_or_retained_partition_count(&index),
            Some(2)
        );
        drop(prepared);

        store
            .apply_partitioned_inserts(vec![
                partitioned_entry(&index, &integer, 1, TxId(1), [1.0, 0.0]),
                partitioned_entry(&index, &text, 2, TxId(2), [0.0, 1.0]),
            ])
            .unwrap();

        assert_ne!(integer, text);
        let integer_state = store.partition_state(&index, &integer).unwrap();
        let text_state = store.partition_state(&index, &text).unwrap();
        assert!(!Arc::ptr_eq(&integer_state, &text_state));
        for (key, own, other, vector) in [
            (&integer, RowId(1), RowId(2), vec![1.0, 0.0]),
            (&text, RowId(2), RowId(1), vec![0.0, 1.0]),
        ] {
            assert_eq!(
                store.current_partition_for_row(&index, own),
                Some(key.clone())
            );
            assert_eq!(
                store
                    .live_entry_for_row_in_partition(&index, key, own, SnapshotId(2))
                    .unwrap()
                    .vector,
                vector
            );
            assert!(
                store
                    .live_entry_for_row_in_partition(&index, key, other, SnapshotId(2))
                    .is_none()
            );
        }
        assert!(store.try_partition_state(&index, &integer).is_some());
        assert!(store.try_partition_state(&index, &text).is_some());
        let summary = store.index_layout_info(&index).unwrap();
        assert_eq!(summary.live_partitions, 2);
        assert_eq!(summary.retained_partitions, 0);
        assert_eq!(summary.live_or_retained_partitions, 2);
        assert!(
            store
                .partition_infos(&index)
                .unwrap()
                .iter()
                .all(|partition| partition.retained_rows == 0)
        );
        assert!(matches!(
            store
                .register_index_with_layout(index.clone(), partitioned_layout(1))
                .unwrap_err(),
            Error::VectorPartitionLimitExceeded {
                max_partitions: 1,
                ..
            }
        ));
        assert_eq!(store.index_layout(&index).unwrap().max_partitions, Some(4));
    }

    #[test]
    fn partition_cap_rejects_a_whole_multi_key_batch_without_state_creation() {
        let store = VectorStore::default();
        let index = index();
        store
            .register_index_with_layout(index.clone(), partitioned_layout(1))
            .unwrap();
        let first = text_key("private-first");
        let second = text_key("private-second");

        let error = store
            .apply_partitioned_inserts(vec![
                partitioned_entry(&index, &first, 1, TxId(1), [1.0, 0.0]),
                partitioned_entry(&index, &second, 2, TxId(2), [0.0, 1.0]),
            ])
            .unwrap_err();

        assert!(matches!(
            error,
            Error::VectorPartitionLimitExceeded {
                max_partitions: 1,
                ..
            }
        ));
        assert!(store.partition_keys(&index).unwrap().is_empty());
        assert!(store.entries_for_index(&index).unwrap().is_empty());
        assert_eq!(store.live_or_retained_partition_count(&index).unwrap(), 0);
    }

    #[test]
    fn prepared_move_releases_its_last_unretained_source_at_the_cap() {
        let store = VectorStore::default();
        let index = index();
        let source = text_key("source");
        let target = text_key("target");
        store
            .register_index_with_layout(index.clone(), partitioned_layout(1))
            .unwrap();
        store
            .apply_partitioned_inserts(vec![partitioned_entry(
                &index,
                &source,
                7,
                TxId(1),
                [1.0, 0.0],
            )])
            .unwrap();

        let prepared = store
            .prepare_partitioned_batch(
                Vec::new(),
                Vec::new(),
                vec![PartitionedVectorMove::new(
                    index.clone(),
                    source.clone(),
                    target.clone(),
                    RowId(7),
                    RowId(7),
                    TxId(2),
                )],
                HashSet::from([VectorPartitionRef::new(index.clone(), source.clone())]),
            )
            .unwrap();
        assert_eq!(prepared.valid_move_count(), 1);
        assert_eq!(
            prepared.projected_live_or_retained_partition_count(&index),
            Some(1)
        );

        store
            .publish_prepared_partitioned_batch(prepared, Lsn(2), None)
            .unwrap();

        assert_eq!(store.partition_keys(&index).unwrap(), vec![target.clone()]);
        assert_eq!(
            store.partition_keys_including_historical(&index).unwrap(),
            vec![source.clone(), target.clone()]
        );
        assert!(store.partition_is_historical_only(&index, &source));
        assert!(store.partition_info(&index, &source).is_none());
        assert!(
            store
                .live_entry_for_row_in_partition(&index, &source, RowId(7), SnapshotId(1),)
                .is_some(),
            "retiring the maintained source must preserve its raw historical vector"
        );
        assert_eq!(
            store.current_partition_for_row(&index, RowId(7)),
            Some(target)
        );
        assert_eq!(store.live_partition_count(&index).unwrap(), 1);
        assert_eq!(store.retained_partition_count(&index).unwrap(), 0);
        assert_eq!(store.live_or_retained_partition_count(&index).unwrap(), 1);
    }

    #[test]
    fn snapshot_free_partition_retirement_reuses_the_retained_key_owner() {
        let accountant = Arc::new(CountingMemoryBudget::default());
        {
            let store = VectorStore::default();
            let index = index();
            let partition_key = text_key("retained-history");
            store
                .register_index_with_layout(
                    index.clone(),
                    VectorIndexLayout::new(
                        2,
                        VectorQuantization::F32,
                        vec!["scope".to_owned()],
                        Some(1),
                        VectorSearchMode::Exact,
                    ),
                )
                .unwrap();
            store
                .apply_partitioned_inserts(vec![partitioned_entry(
                    &index,
                    &partition_key,
                    7,
                    TxId(1),
                    [1.0, 0.0],
                )])
                .unwrap();
            store.account_loaded_state(accountant.clone()).unwrap();
            let charged_before_retirement = accountant.live_bytes();

            store
                .apply_partitioned_deletes(vec![PartitionedVectorDelete::new(
                    index.clone(),
                    partition_key.clone(),
                    RowId(7),
                    TxId(2),
                )])
                .unwrap();
            assert_eq!(
                store.retire_snapshot_free_partition_states(&[], accountant.as_ref()),
                1
            );
            assert!(store.partition_is_historical_only(&index, &partition_key));
            assert!(
                accountant.live_bytes() <= charged_before_retirement,
                "retiring maintained state must not allocate a duplicate partition key"
            );

            let released = store.prune_row_ids(&HashSet::from([RowId(7)]), accountant.as_ref());
            accountant.release(released);
            assert!(
                accountant.live_bytes() < charged_before_retirement,
                "pruning the last historical version must release its retained owners"
            );
        }
        assert_eq!(
            accountant.live_bytes(),
            0,
            "every retained directory and raw-body charge releases exactly once"
        );
    }

    #[test]
    fn absent_source_move_does_not_create_an_empty_target_state() {
        let store = VectorStore::default();
        let index = index();
        let source = text_key("source");
        let target = text_key("target");
        store
            .register_index_with_layout(index.clone(), partitioned_layout(1))
            .unwrap();
        store
            .apply_partitioned_inserts(vec![partitioned_entry(
                &index,
                &source,
                1,
                TxId(1),
                [1.0, 0.0],
            )])
            .unwrap();

        let prepared = store
            .prepare_partitioned_batch(
                Vec::new(),
                Vec::new(),
                vec![PartitionedVectorMove::new(
                    index.clone(),
                    source.clone(),
                    target.clone(),
                    RowId(99),
                    RowId(99),
                    TxId(2),
                )],
                HashSet::new(),
            )
            .unwrap();
        assert_eq!(prepared.valid_move_count(), 0);

        store
            .publish_prepared_partitioned_batch(prepared, Lsn(2), None)
            .unwrap();

        assert_eq!(store.partition_keys(&index).unwrap(), vec![source.clone()]);
        assert!(store.try_partition_state(&index, &target).is_none());
        assert_eq!(store.live_or_retained_partition_count(&index).unwrap(), 1);

        store
            .apply_partitioned_moves(
                vec![PartitionedVectorMove::new(
                    index.clone(),
                    source.clone(),
                    source,
                    RowId(1),
                    RowId(1),
                    TxId(3),
                )],
                Lsn(3),
            )
            .unwrap();
        assert_eq!(store.entries_for_index(&index).unwrap().len(), 1);
    }

    #[test]
    fn partition_keys_are_returned_in_typed_tree_order() {
        let store = VectorStore::default();
        let index = index();
        store
            .register_index_with_layout(index.clone(), partitioned_layout(4))
            .unwrap();
        let a = text_key("a");
        let m = text_key("m");
        let z = text_key("z");

        store
            .apply_partitioned_inserts(vec![
                partitioned_entry(&index, &z, 3, TxId(3), [1.0, 0.0]),
                partitioned_entry(&index, &a, 1, TxId(1), [1.0, 0.0]),
                partitioned_entry(&index, &m, 2, TxId(2), [1.0, 0.0]),
            ])
            .unwrap();

        assert_eq!(store.partition_keys(&index).unwrap(), vec![a, m, z]);
    }

    #[test]
    fn row_membership_moves_directly_while_history_stays_in_its_source_state() {
        let store = VectorStore::default();
        let index = index();
        let source = text_key("source");
        let target = text_key("target");
        store
            .register_index_with_layout(index.clone(), partitioned_layout(4))
            .unwrap();
        store
            .apply_partitioned_inserts(vec![partitioned_entry(
                &index,
                &source,
                7,
                TxId(1),
                [1.0, 0.0],
            )])
            .unwrap();

        store
            .apply_partitioned_moves(
                vec![PartitionedVectorMove::new(
                    index.clone(),
                    source.clone(),
                    target.clone(),
                    RowId(7),
                    RowId(7),
                    TxId(2),
                )],
                Lsn(2),
            )
            .unwrap();

        assert_eq!(
            store.current_partition_for_row(&index, RowId(7)),
            Some(target.clone())
        );
        assert!(
            store
                .live_entry_for_row_in_partition(&index, &source, RowId(7), SnapshotId(1))
                .is_some()
        );
        assert_eq!(
            store.partition_info(&index, &source).unwrap().retained_rows,
            1
        );
        assert_eq!(
            store.partition_info(&index, &target).unwrap().retained_rows,
            0
        );
        assert_eq!(store.live_or_retained_partition_count(&index).unwrap(), 2);

        store
            .apply_partitioned_deletes(vec![PartitionedVectorDelete::new(
                index.clone(),
                target,
                RowId(7),
                TxId(3),
            )])
            .unwrap();
        assert_eq!(store.current_partition_for_row(&index, RowId(7)), None);
    }

    #[test]
    fn maintenance_and_hot_writes_keep_partition_graphs_independent() {
        let store = VectorStore::default();
        let index = index();
        let first = text_key("a");
        let second = text_key("b");
        store
            .register_index_with_layout(index.clone(), partitioned_layout(4))
            .unwrap();
        store
            .apply_partitioned_inserts(vec![
                partitioned_entry(&index, &first, 1, TxId(1), [1.0, 0.0]),
                partitioned_entry(&index, &second, 2, TxId(2), [0.0, 1.0]),
            ])
            .unwrap();
        let accountant = crate::memory_budget::unlimited_memory_budget();

        let first_cycle = store
            .run_hnsw_maintenance_cycle(accountant.clone())
            .unwrap();
        assert_eq!(first_cycle.built_partitions, 2);
        let first_serial = store
            .raw_hnsw_build_serial_for_partition_for_test(&index, &first)
            .unwrap();
        let second_serial = store
            .raw_hnsw_build_serial_for_partition_for_test(&index, &second)
            .unwrap();
        let second_cycle = store
            .run_hnsw_maintenance_cycle(accountant.clone())
            .unwrap();
        assert_eq!(second_cycle.built_partitions, 0);

        store
            .apply_partitioned_inserts_with_accountant(
                vec![partitioned_entry(&index, &first, 3, TxId(3), [0.9, 0.1])],
                Some(accountant.as_ref()),
            )
            .unwrap();
        store
            .apply_partitioned_deletes(vec![PartitionedVectorDelete::new(
                index.clone(),
                first.clone(),
                RowId(1),
                TxId(4),
            )])
            .unwrap();

        assert_eq!(
            store.raw_hnsw_build_serial_for_partition_for_test(&index, &first),
            Some(first_serial)
        );
        assert_eq!(
            store.raw_hnsw_build_serial_for_partition_for_test(&index, &second),
            Some(second_serial)
        );
        assert!(
            !store
                .raw_hnsw_search_partition(&index, &second, &[0.0, 1.0], 1)
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn sampled_build_accounts_for_the_fresh_tail_and_candidate_separately() {
        let layout = partitioned_layout(1)
            .with_policy(Some(1), Some(2), Some(2), Some(2), 1)
            .with_consolidation_policy(Some(1), Some(1));
        assert!(
            layout.hnsw_policy_is_valid(),
            "M=2 and EF_CONSTRUCTION=2 are a legal small topology"
        );
        let policy = layout.resolve_policy(2, 1);
        let fresh_tail_bytes = HnswIndex::estimated_resident_bytes_with_m(
            0,
            layout.dimension,
            layout.quantization,
            policy.hnsw_m,
        );
        let candidate_bytes = crate::mem::estimate_hnsw_build_reservation(
            2,
            layout.dimension,
            layout.quantization,
            policy,
        );
        let final_bytes = HnswIndex::estimated_resident_bytes_with_m(
            2,
            layout.dimension,
            layout.quantization,
            policy.hnsw_m,
        );
        let one_entry_tail_bytes = HnswIndex::estimated_resident_bytes_with_m(
            1,
            layout.dimension,
            layout.quantization,
            policy.hnsw_m,
        );
        assert!(candidate_bytes >= final_bytes);

        let make_store = || {
            let store = VectorStore::default();
            let index = index();
            let key = text_key("accounted");
            store
                .register_index_with_layout(index.clone(), layout.clone())
                .unwrap();
            store
                .apply_partitioned_inserts(vec![
                    partitioned_entry(&index, &key, 1, TxId(1), [1.0, 0.0]),
                    partitioned_entry(&index, &key, 2, TxId(2), [0.0, 1.0]),
                ])
                .unwrap();
            let partition = VectorPartitionRef::new(index.clone(), key.clone());
            (store, index, key, partition)
        };
        let graph_bytes = |ownership: VectorMemoryOwnershipSnapshot| {
            ownership
                .base_graph
                .saturating_add(ownership.change_graph)
                .saturating_add(ownership.mutable_tail)
                .saturating_add(ownership.retired_pinned_graph_bytes)
        };

        let (store, _index, _key, partition) = make_store();
        let exact_budget = Arc::new(CountingMemoryBudget::with_limit(
            fresh_tail_bytes.saturating_add(candidate_bytes),
        ));
        let mut reached_boundary = false;
        assert!(
            store
                .run_sampled_hnsw_maintenance(
                    &partition,
                    exact_budget.clone(),
                    &mut || Ok(()),
                    || {
                        reached_boundary = true;
                        let ownership = store.memory_ownership_snapshot_for_test();
                        assert_eq!(ownership.mutable_tail, fresh_tail_bytes);
                        assert_eq!(graph_bytes(ownership), fresh_tail_bytes);
                        assert_eq!(
                            exact_budget.live_bytes(),
                            fresh_tail_bytes.saturating_add(candidate_bytes),
                            "the fresh tail and complete detached candidate coexist at the build boundary"
                        );
                    },
                )
                .unwrap()
        );
        assert!(reached_boundary);
        assert_eq!(exact_budget.build_reservation_bytes(), candidate_bytes);
        let published = store.memory_ownership_snapshot_for_test();
        assert_eq!(published.mutable_tail, final_bytes);
        assert_eq!(graph_bytes(published), exact_budget.live_bytes());
        assert_eq!(exact_budget.live_bytes(), final_bytes);
        store.clear_hnsw(exact_budget.as_ref());
        assert_eq!(exact_budget.live_bytes(), 0);

        let (store, index, key, partition) = make_store();
        let overlapping_budget = Arc::new(CountingMemoryBudget::default());
        assert!(
            store
                .run_sampled_hnsw_maintenance(
                    &partition,
                    overlapping_budget.clone(),
                    &mut || Ok(()),
                    || {
                        assert_eq!(
                            overlapping_budget.live_bytes(),
                            fresh_tail_bytes.saturating_add(candidate_bytes)
                        );
                        store
                            .apply_partitioned_inserts_with_accountant(
                                vec![partitioned_entry(&index, &key, 3, TxId(3), [0.7, 0.7])],
                                Some(overlapping_budget.as_ref()),
                            )
                            .expect("the exact budget includes the overlapping fresh-tail growth");
                    },
                )
                .unwrap()
        );
        assert_eq!(
            overlapping_budget.build_reservation_bytes(),
            candidate_bytes
        );
        let published = store.memory_ownership_snapshot_for_test();
        assert_eq!(published.base_graph, final_bytes);
        assert_eq!(published.mutable_tail, one_entry_tail_bytes);
        assert_eq!(graph_bytes(published), overlapping_budget.live_bytes());
        assert_eq!(
            overlapping_budget.live_bytes(),
            final_bytes.saturating_add(one_entry_tail_bytes)
        );
        store.clear_hnsw(overlapping_budget.as_ref());
        assert_eq!(overlapping_budget.live_bytes(), 0);

        let (store, _index, _key, partition) = make_store();
        let short_budget = Arc::new(CountingMemoryBudget::with_limit(
            fresh_tail_bytes
                .saturating_add(candidate_bytes)
                .saturating_sub(1),
        ));
        let refused = store
            .run_sampled_hnsw_maintenance(&partition, short_budget.clone(), &mut || Ok(()), || {
                panic!("a refused candidate never reaches graph construction")
            })
            .expect_err("one byte below complete ownership must refuse the candidate");
        assert!(matches!(refused, Error::MemoryBudgetExceeded { .. }));
        assert_eq!(short_budget.build_reservation_bytes(), candidate_bytes);
        assert_eq!(short_budget.live_bytes(), 0);
        assert_eq!(graph_bytes(store.memory_ownership_snapshot_for_test()), 0);
    }

    #[test]
    fn sampled_maintenance_publishes_while_every_build_attempt_overlaps_a_write() {
        let store = VectorStore::default();
        let index = index();
        let key = text_key("hot");
        store
            .register_index_with_layout(index.clone(), partitioned_layout(4))
            .unwrap();
        store
            .apply_partitioned_inserts(
                (0..16)
                    .map(|row| {
                        partitioned_entry(
                            &index,
                            &key,
                            row + 1,
                            TxId(row + 1),
                            [1.0, row as f32 / 16.0],
                        )
                    })
                    .collect(),
            )
            .unwrap();
        let partition = VectorPartitionRef::new(index.clone(), key.clone());
        let accountant = crate::memory_budget::unlimited_memory_budget();
        let mut attempts = 0_u64;

        let published = store
            .run_sampled_hnsw_maintenance(&partition, accountant.clone(), &mut || Ok(()), || {
                attempts += 1;
                let row = 100 + attempts;
                store
                    .apply_partitioned_inserts_with_accountant(
                        vec![partitioned_entry(&index, &key, row, TxId(row), [0.0, 1.0])],
                        Some(accountant.as_ref()),
                    )
                    .expect("a foreground write overlaps every maintenance build attempt");
            })
            .expect("sampled maintenance returns normally");

        assert!(
            attempts > 0,
            "the test must overlap at least one real build"
        );
        assert!(
            published,
            "ordinary writes overlapping every build attempt cannot prevent maintenance progress"
        );
        let info = store.partition_info(&index, &key).unwrap();
        assert_eq!(info.live_rows, 16 + attempts as usize);
        assert!(
            info.graph_available,
            "the resulting indexed route is complete"
        );
    }

    #[test]
    fn disabled_consolidation_skips_an_interrupted_threshold_retry_but_keeps_required_builds() {
        let store = VectorStore::default();
        let index = index();
        let hot = text_key("hot");
        let layout = partitioned_layout(2)
            .with_policy(None, Some(8), Some(32), Some(16), 1)
            .with_consolidation_policy(Some(1), Some(100));
        store
            .register_index_with_layout(index.clone(), layout.clone())
            .unwrap();
        store
            .apply_partitioned_inserts(
                (0..32)
                    .map(|row| {
                        partitioned_entry(
                            &index,
                            &hot,
                            row + 1,
                            TxId(row + 1),
                            [1.0, row as f32 / 32.0],
                        )
                    })
                    .collect(),
            )
            .unwrap();
        let accountant = crate::memory_budget::unlimited_memory_budget();
        assert_eq!(
            store
                .run_hnsw_maintenance_cycle(accountant.clone())
                .unwrap()
                .built_partitions,
            1,
            "CONSOLIDATION does not suppress initial construction"
        );
        let initial_serial = store
            .raw_hnsw_build_serial_for_partition_for_test(&index, &hot)
            .unwrap();
        store
            .apply_partitioned_inserts_with_accountant(
                vec![partitioned_entry(&index, &hot, 33, TxId(33), [0.0, -1.0])],
                Some(accountant.as_ref()),
            )
            .unwrap();

        let hot_state = store.try_partition_state(&index, &hot).unwrap();
        let mut sampled_checkpoints = 0usize;
        let mut crossed_write = false;
        let interrupted = store.run_hnsw_maintenance_cycle_with(accountant.clone(), &mut || {
            if hot_state.sampled_maintenance_frontier().is_some() {
                sampled_checkpoints += 1;
                if sampled_checkpoints == 2 {
                    store.apply_partitioned_inserts_with_accountant(
                        vec![partitioned_entry(&index, &hot, 34, TxId(34), [-1.0, 0.0])],
                        Some(accountant.as_ref()),
                    )?;
                    crossed_write = true;
                    return Err(Error::ReadCancelled);
                }
            }
            Ok(())
        });
        assert!(matches!(interrupted, Err(Error::ReadCancelled)));
        assert!(
            crossed_write,
            "the interruption follows a real graph-construction checkpoint and fresh-tail write"
        );
        assert!(
            hot_state.sampled_maintenance_frontier().is_some(),
            "the nonempty fresh tail retains the interrupted sample frontier"
        );
        assert!(
            hot_state
                .visible_entry_by_row(&index, RowId(32), SnapshotId(32))
                .is_some()
        );
        assert!(
            hot_state
                .visible_entry_by_row(&index, RowId(33), SnapshotId(32))
                .is_none()
        );
        assert!(
            hot_state
                .visible_entry_by_row(&index, RowId(34), SnapshotId(32))
                .is_none()
        );
        for (row, query) in [(33, [0.0, -1.0]), (34, [-1.0, 0.0])] {
            assert!(
                store
                    .raw_hnsw_search_partition(&index, &hot, &query, 4)
                    .unwrap()
                    .unwrap()
                    .iter()
                    .any(|(candidate, _)| *candidate == RowId(row)),
                "the sealed sample and fresh tail both remain searchable"
            );
        }

        let disabled = layout.clone().with_consolidation_disabled(true);
        store
            .register_index_with_layout(index.clone(), disabled.clone())
            .unwrap();
        let hot_info = store.partition_info(&index, &hot).unwrap();
        assert!(hot_info.graph_available);
        assert_eq!(disabled.maintenance_need(&hot_info), None);
        let disabled_cycle = store
            .run_hnsw_maintenance_cycle(accountant.clone())
            .unwrap();
        assert_eq!(disabled_cycle.built_partitions, 0);
        assert_eq!(disabled_cycle.built_indexes, 0);
        assert_eq!(
            store.raw_hnsw_build_serial_for_partition_for_test(&index, &hot),
            Some(initial_serial),
            "disabling threshold consolidation does not publish the retained sample"
        );

        let cold = text_key("cold");
        store
            .apply_partitioned_inserts_with_accountant(
                vec![partitioned_entry(&index, &cold, 35, TxId(35), [0.0, 1.0])],
                Some(accountant.as_ref()),
            )
            .unwrap();
        let initial = store
            .run_hnsw_maintenance_cycle(accountant.clone())
            .unwrap();
        assert_eq!(initial.built_partitions, 1);
        assert_eq!(initial.built_indexes, 1);
        assert!(store.partition_info(&index, &cold).unwrap().graph_available);

        assert!(store.quarantine_partition_route(
            &VectorPartitionRef::new(index.clone(), cold.clone()),
            VectorRouteQuarantineReason::CorruptChanges,
        ));
        let repaired = store
            .run_hnsw_maintenance_cycle(accountant.clone())
            .unwrap();
        assert_eq!(repaired.built_partitions, 1);
        assert_eq!(repaired.built_indexes, 1);
        assert_eq!(
            store
                .partition_route_quarantine(&VectorPartitionRef::new(index.clone(), cold.clone(),)),
            None
        );

        let replacement = disabled.with_policy(None, Some(12), Some(64), Some(16), 2);
        store
            .register_index_with_layout(index.clone(), replacement)
            .unwrap();
        let replaced = store.run_hnsw_maintenance_cycle(accountant).unwrap();
        assert_eq!(replaced.built_partitions, 2);
        assert_eq!(replaced.built_indexes, 1);
        for key in [&hot, &cold] {
            let serving = store
                .partition_info(&index, key)
                .unwrap()
                .serving_policy
                .unwrap();
            assert_eq!((serving.hnsw_m, serving.hnsw_ef_construction), (12, 64));
        }
    }

    #[test]
    fn refused_sampled_build_restores_the_old_tail_replay_frontier() {
        let store = VectorStore::default();
        let index = index();
        let key = text_key("replayed-tail");
        let layout = partitioned_layout(1)
            .with_policy(Some(1), Some(8), Some(32), Some(16), 1)
            .with_consolidation_policy(Some(1), Some(1));
        store
            .register_index_with_layout(index.clone(), layout)
            .unwrap();
        store
            .apply_partitioned_inserts(vec![
                partitioned_entry(&index, &key, 1, TxId(1), [1.0, 0.0]),
                partitioned_entry(&index, &key, 2, TxId(2), [0.0, 1.0]),
            ])
            .unwrap();
        let partition = VectorPartitionRef::new(index.clone(), key.clone());
        let unlimited = crate::memory_budget::unlimited_memory_budget();
        assert!(
            store
                .run_sampled_hnsw_maintenance(&partition, unlimited.clone(), &mut || Ok(()), || {
                    store
                        .apply_partitioned_inserts_with_accountant(
                            vec![partitioned_entry(&index, &key, 3, TxId(3), [0.7, 0.7])],
                            Some(unlimited.as_ref()),
                        )
                        .unwrap();
                },)
                .unwrap(),
            "the first sampled base publishes with a nonempty fresh tail"
        );
        let state = store.try_partition_state(&index, &key).unwrap();
        assert_eq!(state.hnsw_len(), Some(3));
        // Model the durable replay path after it has established that the
        // existing fresh tail already represents journal LSN 3.
        state
            .fresh_tail_replayed_through_lsn
            .store(3, Ordering::SeqCst);
        state
            .fresh_tail_replay_frontier_active
            .store(true, Ordering::SeqCst);
        state
            .fresh_tail_mutated_through_lsn
            .store(3, Ordering::SeqCst);
        let replayed = state.stored_by_row_id(RowId(3)).unwrap();

        let refused = Arc::new(RefuseSampledBuildMemoryBudget::default());
        let error = store
            .run_sampled_hnsw_maintenance(&partition, refused.clone(), &mut || Ok(()), || {})
            .expect_err("the second sampled build is refused after splitting the old tail");
        assert!(matches!(error, Error::MemoryBudgetExceeded { .. }));
        assert_eq!(
            refused.live_bytes.load(Ordering::SeqCst),
            0,
            "the abandoned candidate releases its empty-tail allocation"
        );

        // The overlapping durable commit publication repeats a record the
        // restored tail already owns. Its restored replay frontier must make
        // this idempotent rather than adding a second graph node.
        state.insert_into_materialized_hnsw(&index, &replayed, Some(unlimited.as_ref()));
        let (_, layer_counts) = state
            .with_snapshot_compatible_hnsw_layers(SnapshotId(3), true, |graph| {
                Ok::<_, Error>(graph.raw_entry_count_for_row(RowId(3)))
            })
            .unwrap();
        assert_eq!(
            layer_counts.iter().map(|(_, count)| *count).sum::<usize>(),
            1,
            "the base/tail chain represents the replayed row exactly once"
        );
        let (_, layer_results) = state
            .with_snapshot_compatible_hnsw_layers(SnapshotId(3), true, |graph| {
                graph.search(&index, &[0.7, 0.7], 8)
            })
            .unwrap();
        assert_eq!(
            layer_results
                .into_iter()
                .flat_map(|(_, rows)| rows)
                .filter(|(row_id, _)| *row_id == RowId(3))
                .count(),
            1,
            "search observes one result for the replayed tail row"
        );
    }

    #[test]
    fn raw_publication_helpers_keep_the_serving_graph_policy_until_replacement() {
        let store = VectorStore::default();
        let index = index();
        let layout = VectorIndexLayout::unpartitioned(2, VectorQuantization::F32).with_policy(
            Some(3),
            Some(24),
            Some(128),
            Some(7),
            9,
        );
        store
            .register_index_with_layout(index.clone(), layout.clone())
            .unwrap();
        store.apply_inserts(vec![entry(&index, 1, TxId(1), [1.0, 0.0])]);
        let accountant = crate::memory_budget::unlimited_memory_budget();
        store
            .run_hnsw_maintenance_cycle(accountant.clone())
            .unwrap();
        let state = store.state(&index).unwrap();
        let serial = state.raw_hnsw_build_serial_for_test().unwrap();
        store
            .register_index_with_layout(
                index.clone(),
                layout.with_policy(Some(3), Some(32), Some(160), Some(9), 10),
            )
            .unwrap();

        for publication in 0..4 {
            match publication {
                0 => store.apply_inserts_with_accountant(
                    vec![entry(&index, 2, TxId(2), [0.0, 1.0])],
                    Some(accountant.as_ref()),
                ),
                1 => store.apply_moves_with_accountant(
                    vec![(index.clone(), RowId(2), RowId(3), TxId(3))],
                    Lsn(3),
                    Some(accountant.as_ref()),
                ),
                2 => store.apply_changes_with_accountant_ref(
                    &[],
                    &[entry(&index, 4, TxId(4), [-1.0, 0.0])],
                    &[],
                    Lsn(4),
                    Some(accountant.as_ref()),
                ),
                _ => store.apply_changes_with_accountant_ref(
                    &[],
                    &[],
                    &[(index.clone(), RowId(4), RowId(5), TxId(5))],
                    Lsn(5),
                    Some(accountant.as_ref()),
                ),
            }
            assert_eq!(
                state.raw_hnsw_build_serial_for_test(),
                Some(serial),
                "publication extends the existing graph without clearing it"
            );
            assert_eq!(
                state
                    .hnsw
                    .get()
                    .unwrap()
                    .read()
                    .as_ref()
                    .unwrap()
                    .policy_values(),
                (24, 128, 7),
                "the serving graph retains its built shape while the newer declaration awaits maintenance"
            );
        }
        for (old, current, vector) in [(2, 3, [0.0, 1.0]), (4, 5, [-1.0, 0.0])] {
            assert!(
                state
                    .visible_entry_by_row(&index, RowId(old), SnapshotId(5))
                    .is_none()
            );
            assert_eq!(
                state
                    .visible_entry_by_row(&index, RowId(current), SnapshotId(5))
                    .unwrap()
                    .vector,
                vector.to_vec()
            );
            assert!(
                store
                    .raw_hnsw_search(&index, &vector, 10)
                    .unwrap()
                    .unwrap()
                    .iter()
                    .any(|(row, _)| *row == RowId(current))
            );
        }
        store.run_hnsw_maintenance_cycle(accountant).unwrap();
        assert_ne!(state.raw_hnsw_build_serial_for_test(), Some(serial));
        assert_eq!(
            state
                .hnsw
                .get()
                .unwrap()
                .read()
                .as_ref()
                .unwrap()
                .policy_values(),
            (32, 160, 9)
        );
    }

    #[test]
    fn retained_bytes_include_large_text_partition_keys() {
        let store = VectorStore::default();
        let index = index();
        store
            .register_index_with_layout(index.clone(), partitioned_layout(4))
            .unwrap();
        let before = store.retained_bytes_for_index(&index).unwrap();
        let large = "x".repeat(128 * 1024);
        let key = text_key(large.clone());

        store
            .apply_partitioned_inserts(vec![partitioned_entry(
                &index,
                &key,
                1,
                TxId(1),
                [1.0, 0.0],
            )])
            .unwrap();

        let after = store.retained_bytes_for_index(&index).unwrap();
        assert!(after.saturating_sub(before) >= large.len());
    }

    #[test]
    fn policy_only_declaration_update_scans_only_failed_routes_after_the_write_gate() {
        let store = VectorStore::default();
        let index = index();
        let layout = partitioned_layout(8).with_policy(Some(1), Some(4), Some(64), Some(16), 1);
        store
            .register_index_with_layout(index.clone(), layout.clone())
            .unwrap();
        store
            .apply_partitioned_inserts(
                (0..4)
                    .map(|row| {
                        partitioned_entry(
                            &index,
                            &text_key(format!("scope-{row}")),
                            row + 1,
                            TxId(row + 1),
                            [1.0, row as f32],
                        )
                    })
                    .collect(),
            )
            .unwrap();

        let inspected_entries = || {
            store
                .try_column(&index)
                .unwrap()
                .partition_snapshots()
                .into_iter()
                .map(|(_, state)| state.raw_directory_entries_inspected.load(Ordering::SeqCst))
                .sum::<u64>()
        };
        let before_inspection = inspected_entries();
        assert_eq!(store.partition_infos(&index).unwrap().len(), 4);
        let before_update = inspected_entries();
        assert!(
            before_update > before_inspection,
            "the counter observes the production raw-directory inspection path"
        );

        store
            .register_index_with_layout(
                index.clone(),
                layout.with_policy(Some(1), Some(4), Some(64), Some(32), 2),
            )
            .unwrap();
        assert_eq!(
            inspected_entries(),
            before_update,
            "an EF_SEARCH declaration swap cannot inspect any vector body or directory entry"
        );

        let failed_partition = VectorPartitionRef::new(index.clone(), text_key("scope-0"));
        store
            .record_partition_maintenance_failure(
                &failed_partition,
                VectorMaintenanceFailure::BuildFailure,
            )
            .unwrap();
        let before_failure_recheck = inspected_entries();
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(8).with_policy(Some(1), Some(4), Some(64), Some(48), 3),
            )
            .unwrap();
        assert_eq!(
            inspected_entries(),
            before_failure_recheck + 1,
            "the shared-gate recheck scans only the one route carrying a retained failure"
        );
    }

    #[test]
    fn recording_a_failure_for_an_absent_partition_is_a_noop() {
        let store = VectorStore::default();
        let index = index();
        store
            .record_partition_maintenance_error(
                &VectorPartitionRef::new(index.clone(), text_key("cold-catalog-only")),
                &Error::Other("safe local test failure".to_owned()),
            )
            .expect("a catalog-only route has no in-memory mark to record");

        store
            .register_index_with_layout(index.clone(), partitioned_layout(2))
            .unwrap();
        store
            .record_partition_maintenance_error(
                &VectorPartitionRef::new(index, text_key("dropped-before-record")),
                &Error::Other("safe local test failure".to_owned()),
            )
            .expect("a dropped sampled partition cannot abort cycle closing");
    }

    #[test]
    fn maintenance_failure_marks_are_stable_until_successful_publication() {
        let store = VectorStore::default();
        let index = index();
        let partition_key = VectorPartitionKey::unpartitioned();
        let partition = VectorPartitionRef::new(index.clone(), partition_key.clone());
        store.register_index(index.clone(), 2, VectorQuantization::F32);
        store.apply_inserts(vec![entry(&index, 1, TxId(1), [1.0, 0.0])]);

        store
            .record_partition_maintenance_error(&partition, &Error::ReadCancelled)
            .unwrap();
        assert_eq!(
            store
                .partition_info(&index, &partition_key)
                .unwrap()
                .maintenance_failure,
            None,
            "stopping before any failure leaves the initial build pending"
        );

        for (error, failure, reason, action) in [
            (
                Error::MemoryBudgetExceeded {
                    subsystem: "vector_index".to_owned(),
                    operation: "prepare_vector_generation".to_owned(),
                    requested_bytes: 2,
                    available_bytes: 1,
                    budget_limit_bytes: 1,
                    hint: "raise memory".to_owned(),
                },
                VectorMaintenanceFailure::MemoryLimit,
                "memory_limit",
                "raise_memory_limit",
            ),
            (
                Error::DiskBudgetExceeded {
                    operation: "prepare_vector_generation".to_owned(),
                    current_bytes: 2,
                    budget_limit_bytes: 1,
                    hint: "raise disk".to_owned(),
                },
                VectorMaintenanceFailure::DiskLimit,
                "disk_limit",
                "raise_disk_limit_or_free_space",
            ),
            (
                Error::Other("codec rejected the candidate".to_owned()),
                VectorMaintenanceFailure::BuildFailure,
                "build_failure",
                "inspect_build_failure",
            ),
        ] {
            store
                .record_partition_maintenance_error(&partition, &error)
                .unwrap();
            // Both the durable engine recorder and the in-memory builder must
            // keep a previous actionable failure when a later attempt is stopped.
            store
                .record_partition_maintenance_error(&partition, &Error::ReadCancelled)
                .unwrap();
            let accountant = Arc::new(CountingMemoryBudget::default());
            let cancelled = store.run_hnsw_maintenance_cycle_with(accountant.clone(), &mut || {
                Err(Error::ReadCancelled)
            });
            assert!(matches!(cancelled, Err(Error::ReadCancelled)));
            assert_eq!(
                accountant.live_bytes(),
                0,
                "cancellation releases build workspace"
            );
            assert!(
                store
                    .partition_info(&index, &partition_key)
                    .unwrap()
                    .maintenance_progress
                    .is_none()
            );
            for _ in 0..2 {
                let info = store
                    .partition_info(&index, &partition_key)
                    .expect("the failed partition remains inspectable");
                let observed = info.maintenance_failure;
                assert_eq!(observed, Some(failure));
                assert_eq!(failure.reason(), reason);
                assert_eq!(failure.recovery_action(), action);
                let details = info
                    .maintenance_failure_details
                    .expect("inspection retains safe failure details");
                assert_eq!(details.failure, failure);
                assert_eq!(details.reason(), reason);
                assert_eq!(details.recovery_action(), action);
                assert!(!details.message.contains("codec rejected"));
                match failure {
                    VectorMaintenanceFailure::MemoryLimit => {
                        assert_eq!(details.operation, "prepare_vector_generation");
                        assert_eq!(details.requested_bytes, Some(2));
                        assert_eq!(details.available_bytes, Some(1));
                        assert_eq!(details.budget_limit_bytes, Some(1));
                        assert!(details.recovery_instruction.contains("SET MEMORY_LIMIT"));
                    }
                    VectorMaintenanceFailure::DiskLimit => {
                        assert_eq!(details.operation, "prepare_vector_generation");
                        assert_eq!(details.current_bytes, Some(2));
                        assert_eq!(details.budget_limit_bytes, Some(1));
                        assert!(details.recovery_instruction.contains("SET DISK_LIMIT"));
                    }
                    VectorMaintenanceFailure::BuildFailure => {
                        assert_eq!(details.operation, "build_or_publish_vector_generation");
                        assert!(details.message.contains("generation"));
                    }
                }
            }
        }

        let report = store
            .run_hnsw_maintenance_cycle(crate::memory_budget::unlimited_memory_budget())
            .unwrap();
        assert_eq!(report.built_partitions, 1);
        assert_eq!(
            store
                .partition_info(&index, &partition_key)
                .and_then(|info| info.maintenance_failure),
            None,
            "only a successful graph publication clears the retained failure"
        );
    }
}

#[cfg(test)]
mod transaction_visibility_tests;
