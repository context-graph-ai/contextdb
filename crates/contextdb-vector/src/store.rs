mod membership;
use membership::with_membership_read;
#[doc(hidden)]
pub use membership::{
    LazyRawVectorMembershipSource, RawVectorInspectionSnapshot, RawVectorMembershipCursor,
    RawVectorMembershipFactory, RawVectorMembershipSelection, RawVectorMembershipSource,
    RawVectorMembershipSummary, RawVectorReverseMembershipCursor, RawVectorReverseMembershipSource,
    RawVectorSuffixChange, canonical_source_key,
};

use crate::memory_budget::{
    MemoryBudget, ScopedVectorCopy, VectorCopyAdmission, VectorWorkspaceReservation,
};
use crate::quantized::StoredVectorEntry;
use contextdb_core::{
    Error, Lsn, Result, RowId, SnapshotId, TxId, VectorEntry, VectorIndexRef, VectorPartitionKey,
    VectorQuantization, VectorSearchMode,
};
#[cfg(any(test, feature = "test-seams"))]
use parking_lot::Condvar;
use parking_lot::{Mutex, RwLock};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(any(test, feature = "test-seams"))]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

// unpartitioned_vector_membership_pages_reload_at_constant_residency observes private directory owners.
#[cfg(feature = "test-seams")]
#[path = "store/resident_directory_test_seams.rs"]
mod resident_directory_test_seams;
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub use resident_directory_test_seams::VectorGenerationSourceVisitsForTest;

// The test build forces collisions in the actual admission sets. The ordered
// partition registry still owns identity through complete typed keys.
#[cfg(not(test))]
type PartitionKeySet = HashSet<VectorPartitionKey>;
#[cfg(test)]
type PartitionKeySet =
    HashSet<VectorPartitionKey, std::hash::BuildHasherDefault<tests::PartitionCollisionHasher>>;

type VectorCopyAdmissionCallback<'a> =
    &'a mut (dyn FnMut(&mut VectorCopyAdmission, usize, &str, &str, &str) -> Result<()> + 'a);

/// Internal route inconsistency; not an embedder-facing refusal type.
enum SnapshotPartitionMismatch {
    AbsentPartition,
    ForwardMembershipAbsent,
}

impl SnapshotPartitionMismatch {
    fn error(self) -> Error {
        Error::Other(
            match self {
                Self::AbsentPartition => {
                    "durable vector reverse directory names an absent partition"
                }
                Self::ForwardMembershipAbsent => {
                    "durable vector reverse and partition memberships disagree"
                }
            }
            .into(),
        )
    }
}

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
    /// Native candidate constructions.
    pub index_builds: u64,
    /// Native coalescing/remap steps.
    pub index_repairs_or_compactions: u64,
    pub raw_partition_evictions: u64,
    pub sealed_generation_evictions: u64,
}

/// Work a partitioned write spends on the declared partition cap.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorCapWorkCountersForTest {
    /// Routes projected by partitioned-batch preparation.
    pub projected_routes: u64,
    /// Counted-empty routes the exact cap edge examined.
    pub cap_edge_route_visits: u64,
    /// Indexed hold lookups made by reclaim selection, one per snapshot.
    pub hold_lookups: u64,
    /// Partition-map lookups the cap edge made to confirm each candidate's
    /// identity: one per examined candidate, carried key included.
    pub cap_edge_identity_probes: u64,
    /// Membership records the hold check streamed because a durable route
    /// had no usable occupancy index (zero on every indexed path).
    pub hold_streamed_records: u64,
}

/// The cap occupancy and directory figure a column maintains, beside the
/// same figures recounted from every partition and row map.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorPartitionLimitAuditForTest {
    pub occupied: usize,
    pub recounted_occupied: usize,
    pub empty_counted: Vec<VectorPartitionKey>,
    pub recounted_empty_counted: Vec<VectorPartitionKey>,
    pub directory_bytes: usize,
    pub recounted_directory_bytes: usize,
}

#[derive(Default)]
struct VectorCapWork {
    projected_routes: AtomicU64,
    cap_edge_route_visits: AtomicU64,
    hold_lookups: AtomicU64,
    cap_edge_identity_probes: AtomicU64,
    hold_streamed_records: AtomicU64,
}

/// Exact retained-owner categories sampled without changing residency.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VectorMemoryOwnershipSnapshot {
    pub raw_vector_bodies: usize,
    pub raw_reverse_directory: usize,
    /// Resident row-placement and historical partition-key directories.
    pub partition_vectors: usize,
    /// Column and index controls, with fixed bytes per live partition.
    /// Partition handles also include obsolete partitions held for old
    /// snapshots until reclamation releases them.
    pub vector_partition_controls: usize,
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
    native_builds: AtomicU64,
    native_rewrites: AtomicU64,
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
            index_builds: self.native_builds.load(Ordering::SeqCst),
            index_repairs_or_compactions: self.native_rewrites.load(Ordering::SeqCst),
            raw_partition_evictions: self.raw_partition_evictions.load(Ordering::SeqCst),
            sealed_generation_evictions: self.sealed_generation_evictions.load(Ordering::SeqCst),
        }
    }
}

#[cfg(feature = "test-seams")]
impl crate::build_observation::NativeIndexActivity for VectorPassiveActivity {
    fn before_build(&self) {
        self.native_builds.fetch_add(1, Ordering::SeqCst);
    }
    fn before_rewrite(&self) {
        self.native_rewrites.fetch_add(1, Ordering::SeqCst);
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
    AdmitEncoding,
    AdmitCandidate,
    CandidateAdmitted,
    AdmitPersistence,
    PersistenceAdmitted,
    EncodeGraph,
    HashStateDigest,
    PrepareDurableGeneration,
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

#[derive(Default)]
struct PartitionVectors {
    entries: Vec<StoredVectorEntry>,
    by_row: HashMap<RowId, Vec<usize>>,
    payload_memberships: HashMap<usize, usize>,
    owned_payload_bytes: usize,
}

impl PartitionVectors {
    fn push(&mut self, entry: StoredVectorEntry) {
        let position = self.entries.len();
        self.by_row.entry(entry.row_id).or_default().push(position);
        if let Some(identity) = entry.vector.payload_identity() {
            self.payload_memberships
                .entry(identity)
                .or_insert(entry.estimated_bytes());
        } else {
            self.owned_payload_bytes = self
                .owned_payload_bytes
                .saturating_add(entry.estimated_bytes());
        }
        self.entries.push(entry);
    }

    fn unique_payload_bytes(&self) -> usize {
        self.owned_payload_bytes
            .saturating_add(self.payload_memberships.values().copied().sum())
    }

    fn rebuild_row_positions(&mut self) {
        self.by_row.clear();
        self.payload_memberships.clear();
        self.owned_payload_bytes = 0;
        for (position, entry) in self.entries.iter().enumerate() {
            if let Some(identity) = entry.vector.payload_identity() {
                self.payload_memberships
                    .entry(identity)
                    .or_insert(entry.estimated_bytes());
            } else {
                self.owned_payload_bytes = self
                    .owned_payload_bytes
                    .saturating_add(entry.estimated_bytes());
            }
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

/// The memory refusal that sent one search to the paged/durable route
/// instead of a retained saved graph.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorPagedRouteRefusal {
    pub requested_bytes: usize,
    pub available_bytes: usize,
    pub budget_limit_bytes: usize,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorGraphLayerAvailability {
    Ready,
    Dormant,
    Unavailable,
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

/// Fixed maintenance metadata from a borrowed active or historical source.
/// Sampling this value retains no key, graph, or vector allocation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorPartitionSourceMaintenanceMetadata {
    pub historical_only: bool,
    pub failure: Option<VectorMaintenanceFailureDetails>,
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
        match operation {
            "prepare_vector_generation" => "prepare_vector_generation",
            "finalize_repaired_vector_partition" => "finalize_repaired_vector_partition",
            "truncate_vector_partition_journal" => "truncate_vector_partition_journal",
            "load_durable_vector_generation" => "load_durable_vector_generation",
            "read_vector_generation_source_page" => "read_vector_generation_source_page",
            "inspect_vector_generation_inventory" => "inspect_vector_generation_inventory",
            "clustered_calibration_capture" => "clustered_calibration_capture",
            "clustered_calibration_dependency" => "clustered_calibration_dependency",
            "clustered_calibration_metadata_control" => "clustered_calibration_metadata_control",
            "clustered_calibration_query_identity" => "clustered_calibration_query_identity",
            "clustered_calibration_query_output" => "clustered_calibration_query_output",
            "clustered_calibration_sample_page" => "clustered_calibration_sample_page",
            "clustered_calibration_source" => "clustered_calibration_source",
            "clustered_calibration_source_range" => "clustered_calibration_source_range",
            "clustered_calibration_source_route" => "clustered_calibration_source_route",
            "clustered_candidate_verification_window" => "clustered_candidate_verification_window",
            "clustered_canonical_visit" => "clustered_canonical_visit",
            "clustered_codebook" => "clustered_codebook",
            "clustered_descriptor_window" => "clustered_descriptor_window",
            "clustered_erasure_controls" => "clustered_erasure_controls",
            "clustered_erasure_slot_input" => "clustered_erasure_slot_input",
            "clustered_layout" => "clustered_layout",
            "clustered_manifest" => "clustered_manifest",
            "clustered_native_seal" => "clustered_native_seal",
            "clustered_outer_write" => "clustered_outer_write",
            "clustered_qualification_record" => "clustered_qualification_record",
            "clustered_qualification_selection" => "clustered_qualification_selection",
            "clustered_query" => "clustered_query",
            "clustered_retained_codebook" => "clustered_retained_codebook",
            "clustered_retained_control" => "clustered_retained_control",
            "clustered_retained_directory" => "clustered_retained_directory",
            "clustered_retained_registry" => "clustered_retained_registry",
            "clustered_retained_stage" => "clustered_retained_stage",
            "clustered_served_depth" => "clustered_served_depth",
            "clustered_source_control" => "clustered_source_control",
            "clustered_source_range" => "clustered_source_range",
            "clustered_writer_workspace" => "clustered_writer_workspace",
            "maintenance_route_output" => "maintenance_route_output",
            "exact_byte_read" => "exact_byte_read",
            "materialize" => "materialize",
            "native_canonical_merge" => "native_canonical_merge",
            "native_explain_scope" => "native_explain_scope",
            "native_partition_hits" => "native_partition_hits",
            "native_public_results" => "native_public_results",
            "native_vector_declaration" => "native_vector_declaration",
            "resolve_clustered_policy" => "resolve_clustered_policy",
            _ => "vector_maintenance",
        }
    }

    fn safe_memory_operation(operation: &contextdb_core::error::DiagnosticLabel) -> &'static str {
        // Native static labels already name the actual refusal site. Preserve
        // them without allocation.
        match operation.static_text() {
            Some(name) => name,
            None => Self::safe_operation(operation),
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
                operation: Self::safe_memory_operation(operation),
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

/// One retained-memory reservation whose release belongs to the object that
/// holds the corresponding allocation. Keeping the reservation beside the
/// retained state prevents DDL cleanup from having to reconstruct who paid
/// for it from vector values after the index identity is gone.
struct RetainedMemoryCharge {
    bytes: usize,
    accountant: crate::memory_budget::MemoryBudgetOwner,
}

impl Drop for RetainedMemoryCharge {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
        self.bytes = 0;
    }
}

/// Original adapter control custody travels outside the budget Arc, including
/// retained suffixes, directories and canonical vector bodies. The inner
/// reservation remains available to the established settlement interface.
struct PaidRetainedMemoryCharge {
    charge: RetainedMemoryCharge,
    _control_invoice:
        Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
}
impl PaidRetainedMemoryCharge {
    fn new(bytes: usize, accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>) -> Self {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        let invoice = accountant.control_invoice();
        Self {
            charge: RetainedMemoryCharge { bytes, accountant },
            _control_invoice: invoice,
        }
    }
}
impl std::ops::Deref for PaidRetainedMemoryCharge {
    type Target = RetainedMemoryCharge;
    fn deref(&self) -> &RetainedMemoryCharge {
        &self.charge
    }
}
impl std::ops::DerefMut for PaidRetainedMemoryCharge {
    fn deref_mut(&mut self) -> &mut RetainedMemoryCharge {
        &mut self.charge
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

/// A partition's standing toward its column's declared partition cap. Counted
/// means retained entries on a route that is not historical-only (the cap
/// predicate); counted-empty additionally has no live vector, which makes it
/// the only kind of route a capacity-expanding write may try to reclaim.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum PartitionLimitClass {
    #[default]
    NotCounted,
    CountedLive,
    CountedEmpty,
}

impl PartitionLimitClass {
    fn counted(self) -> bool {
        self != Self::NotCounted
    }
}

/// The link inside each attached state holds one shared copy of its map key,
/// priced in the directory figure with the key (`partition_entry_terms`) and
/// admitted with each new partition (`prepared_directory_growth_upper_bound`).
/// The counted-empty set holds one weak state handle and one shared key
/// handle per counted-empty route, a fixed per-route term bounded by the
/// declared cap; it never copies a key.
#[derive(Default)]
struct PartitionLimitTally {
    occupied: usize,
    empty_counted: HashMap<usize, (Weak<IndexState>, Arc<VectorPartitionKey>)>,
}

/// The retained bytes of the cap link's own key copy: the shared allocation
/// header plus the key.
fn partition_link_key_bytes(key_bytes: usize) -> usize {
    key_bytes.saturating_add(2 * std::mem::size_of::<usize>())
}

/// Derived in-memory occupancy of one column's partition cap. Every state
/// mutation that can change a partition's class reports only its difference
/// here, so admission reads the count without visiting other partitions.
/// Open, reload and clear rebuild it as states enter or leave the map.
#[derive(Default)]
struct PartitionLimitLedger {
    tally: Mutex<PartitionLimitTally>,
}

impl PartitionLimitLedger {
    fn apply(
        &self,
        state: &Weak<IndexState>,
        key: &Arc<VectorPartitionKey>,
        old: PartitionLimitClass,
        new: PartitionLimitClass,
    ) {
        if old == new {
            return;
        }
        let mut tally = self.tally.lock();
        if old.counted() {
            tally.occupied = tally
                .occupied
                .checked_sub(1)
                .expect("partition cap occupancy never goes below zero");
        }
        if new.counted() {
            tally.occupied += 1;
        }
        if old == PartitionLimitClass::CountedEmpty {
            tally.empty_counted.remove(&(state.as_ptr() as usize));
        }
        if new == PartitionLimitClass::CountedEmpty {
            tally
                .empty_counted
                .insert(state.as_ptr() as usize, (state.clone(), key.clone()));
        }
    }

    fn occupied(&self) -> usize {
        self.tally.lock().occupied
    }

    fn empty_counted_len(&self) -> usize {
        self.tally.lock().empty_counted.len()
    }

    /// Edge path only: at most the declared cap, priced by the caller. Each
    /// candidate carries its own key, so no route is identified by a search.
    fn empty_counted(&self) -> Vec<(Arc<IndexState>, Arc<VectorPartitionKey>)> {
        self.tally
            .lock()
            .empty_counted
            .values()
            .filter_map(|(state, key)| state.upgrade().map(|state| (state, key.clone())))
            .collect()
    }
}

#[derive(Default)]
struct PartitionLimitLink {
    ledger: Weak<PartitionLimitLedger>,
    /// This state, as the ledger's counted-empty set holds it.
    this: Weak<IndexState>,
    /// This state's map key, held once and shared with the counted-empty set.
    key: Option<Arc<VectorPartitionKey>>,
    class: PartitionLimitClass,
    /// Directory figure of this state last reported to the column tally.
    directory_bytes: usize,
    directory: Weak<DirectoryTally>,
}

#[derive(Debug, Default, Clone, Copy)]
struct ResidentPendingMarks {
    newest_open_created: Option<TxId>,
    newest_tombstone: Option<TxId>,
}

impl ResidentPendingMarks {
    fn note(&mut self, entry: &RawVectorDirectoryEntry) {
        match entry.deleted_tx {
            None => {
                self.newest_open_created = self.newest_open_created.max(Some(entry.created_tx));
            }
            Some(deleted) => self.newest_tombstone = self.newest_tombstone.max(Some(deleted)),
        }
    }

    fn of(directory: &[RawVectorDirectoryEntry]) -> Self {
        let mut marks = Self::default();
        for entry in directory {
            marks.note(entry);
        }
        marks
    }
}

/// Reports a state's class and directory differences when a mutator returns,
/// after the mutator's own field guards (declared later) have dropped.
struct ReclassifyOnReturn<'a>(&'a IndexState);

impl Drop for ReclassifyOnReturn<'_> {
    fn drop(&mut self) {
        self.0.reclassify_limit();
    }
}

/// Derived retained-directory figure of one column. Partition states report
/// their own directory differences; row-map mutators report theirs. The
/// figure equals `VectorColumnState::directory_ownership_recount`.
#[derive(Default)]
struct DirectoryTally {
    figures: Mutex<DirectoryFigures>,
}

#[derive(Default, Clone, Copy)]
struct DirectoryFigures {
    raw_reverse_directory: usize,
    partition_vectors: usize,
    typed_partition_keys: usize,
}

impl DirectoryTally {
    fn adjust(&self, apply: impl FnOnce(&mut DirectoryFigures)) {
        apply(&mut self.figures.lock());
    }

    fn figures(&self) -> DirectoryFigures {
        *self.figures.lock()
    }
}

fn add_signed(value: &mut usize, before: usize, after: usize) {
    *value = value
        .checked_sub(before)
        .expect("derived vector-directory figure never goes below zero")
        .saturating_add(after);
}

/// Per-partition retained directory terms outside its raw reverse directory:
/// the map entry and state, the map key, and the cap link's shared key copy.
fn partition_entry_terms(key_bytes: usize) -> (usize, usize) {
    let vectors = std::mem::size_of::<Arc<IndexState>>()
        .saturating_add(IndexState::allocation_bytes())
        .saturating_add(std::mem::size_of::<usize>().saturating_mul(3));
    (
        vectors,
        key_bytes.saturating_add(partition_link_key_bytes(key_bytes)),
    )
}

fn row_entry_terms(key: &VectorPartitionKey) -> (usize, usize) {
    (
        std::mem::size_of::<(RowId, Arc<VectorPartitionKey>)>(),
        key.estimated_bytes()
            .saturating_add(2 * std::mem::size_of::<usize>()),
    )
}

fn history_entry_terms(keys: &[Arc<VectorPartitionKey>], capacity: usize) -> (usize, usize) {
    let vectors = std::mem::size_of::<(RowId, Vec<Arc<VectorPartitionKey>>)>()
        .saturating_add(capacity.saturating_mul(std::mem::size_of::<Arc<VectorPartitionKey>>()));
    let typed = keys.iter().fold(0usize, |bytes, key| {
        bytes
            .saturating_add(key.estimated_bytes())
            .saturating_add(2 * std::mem::size_of::<usize>())
    });
    (vectors, typed)
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

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawVectorTailEntry {
    pub row_id: RowId,
    pub row_created_tx: TxId,
    pub row_lsn: Lsn,
    pub vector_created_tx: TxId,
    pub vector_lsn: Lsn,
    pub visible_from: TxId,
    pub deleted_tx: Option<TxId>,
}

impl RawVectorTailEntry {
    pub fn membership(&self) -> VectorMembershipProvenance {
        VectorMembershipProvenance::new(
            self.row_created_tx,
            self.row_lsn,
            self.visible_from,
            self.deleted_tx,
        )
    }
}

#[doc(hidden)]
pub trait StreamedVectorSuffixHandle: Send + Sync {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn stream_tail(
        &self,
        visit: &mut dyn FnMut(RawVectorTailEntry) -> Result<bool>,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<()>;

    fn covered_lsn(&self) -> Lsn;

    fn through_lsn(&self) -> Option<Lsn>;

    fn saved_inserts(&self) -> usize;

    #[cfg(any(test, feature = "test-seams"))]
    fn policy_for_test(&self) -> Option<(usize, usize, usize)> {
        None
    }

    fn observe_vector_decode(&self, row_id: RowId, created_tx: TxId, lsn: Lsn) {
        let _ = (row_id, created_tx, lsn);
    }
}

/// Private serving delta consumed through the existing bounded raw scorer.
/// Eligibility proves current placement/body identity against saved coverage;
/// a creation timestamp alone is not sufficient.
#[doc(hidden)]
pub trait CapturedVectorStreamedTail: Send + Sync {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn validate(&self) -> Result<()>;
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn includes(
        &self,
        interval: &RawVectorDirectoryEntry,
        body: &RawVectorDirectoryEntry,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<bool>;
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn includes_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<bool>;
}
/// Every clone retains the external control before cloning the Arc body.
#[doc(hidden)]
pub struct CapturedVectorStreamedTailHandle {
    source: Arc<dyn CapturedVectorStreamedTail>,
    control: QuerySourceControl,
}
impl CapturedVectorStreamedTailHandle {
    pub fn new(source: Arc<dyn CapturedVectorStreamedTail>, control: QuerySourceControl) -> Self {
        Self { source, control }
    }
}
impl Clone for CapturedVectorStreamedTailHandle {
    fn clone(&self) -> Self {
        let control = self.control.clone();
        let source = self.source.clone();
        Self { source, control }
    }
}
impl std::ops::Deref for CapturedVectorStreamedTailHandle {
    type Target = dyn CapturedVectorStreamedTail;
    fn deref(&self) -> &Self::Target {
        &*self.source
    }
}

/// Lightweight identity retained at open for one durable raw vector. It is
/// sufficient for counts, snapshot visibility, row membership, and a later
/// point load, but deliberately contains no vector body.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawVectorDirectoryEntry {
    pub membership: VectorMembershipProvenance,
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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn with_copy_admission<T>(
    owner: &mut VectorCopyAdmission,
    action: impl FnOnce(&mut dyn FnMut(DormantVectorLoadRequest) -> bool) -> Result<T>,
) -> Result<T> {
    let mut refusal = None;
    let result = action(&mut |request| match request {
        DormantVectorLoadRequest::Reserve(bytes) => match owner.reserve(bytes) {
            Ok(()) => true,
            Err(error) => {
                refusal = Some(error);
                false
            }
        },
        _ => true,
    });
    match refusal {
        Some(error) => Err(error),
        None => result,
    }
}

#[cfg(not(feature = "test-seams"))]
use self::with_copy_admission as with_membership_copy_admission;

#[cfg(feature = "test-seams")]
#[expect(
    clippy::result_large_err,
    reason = "The source preserves its typed memory refusal."
)]
fn with_membership_copy_admission<T>(
    owner: &mut VectorCopyAdmission,
    action: impl FnOnce(&mut dyn FnMut(DormantVectorLoadRequest) -> bool) -> Result<T>,
) -> Result<T> {
    #[cfg(feature = "test-seams")]
    let _family =
        contextdb_core::family_loads::enter(contextdb_core::family_loads::Family::VectorMembership);
    with_copy_admission(owner, action)
}

/// Reader-funded control for ephemeral sources. The raw Arc never escapes:
/// every clone must free its control allocation before its final admission drops.
#[doc(hidden)]
pub struct QuerySourceControl(Option<Arc<VectorCopyAdmission>>);
impl QuerySourceControl {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn new(mut owner: VectorCopyAdmission) -> Result<Self> {
        owner.reserve(
            contextdb_tx::staged_source::source_arc_layout::<VectorCopyAdmission>().size(),
        )?;
        Ok(Self(Some(Arc::new(owner))))
    }
    pub fn funder(&self) -> crate::memory_budget::MemoryBudgetOwner {
        self.0.as_ref().expect("live query source control").funder()
    }
    pub fn visit_retained_copy_credit(
        &self,
        caller: &dyn MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        VectorCopyAdmission::visit_retained_copy_credit(
            self.0.as_ref().expect("live query source control"),
            caller,
            visit,
        );
    }
}
impl Clone for QuerySourceControl {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl Drop for QuerySourceControl {
    fn drop(&mut self) {
        if let Some(control) = self.0.take() {
            // into_inner deallocates this strong allocation before returning its
            // body. Unlike try_unwrap, concurrent final drops cannot all fail.
            drop(Arc::into_inner(control));
        }
    }
}

enum NativeSourceControl {
    Original(contextdb_tx::SourceCharge),
    Query(QuerySourceControl),
}
impl Clone for NativeSourceControl {
    fn clone(&self) -> Self {
        match self {
            Self::Original(control) => Self::Original(control.clone()),
            Self::Query(control) => Self::Query(control.clone()),
        }
    }
}

/// A native source allocation is priced by an owner outside that allocation.
/// Every internal strong clone carries that owner through Arc deallocation.
pub(crate) struct NativeSourceArc<T: ?Sized> {
    source: Arc<T>,
    control: Option<NativeSourceControl>,
}
impl<T: ?Sized> Clone for NativeSourceArc<T> {
    fn clone(&self) -> Self {
        let control = self.control.clone();
        let source = self.source.clone();
        Self { source, control }
    }
}
impl<T: ?Sized> std::ops::Deref for NativeSourceArc<T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}
impl<T: ?Sized + std::fmt::Debug> std::fmt::Debug for NativeSourceArc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
macro_rules! native_source_arc {
    ($source:ty) => {
        impl From<Arc<$source>> for NativeSourceArc<$source> {
            fn from(source: Arc<$source>) -> Self {
                let control = match source.query_source_control() {
                    Some(control) => Some(NativeSourceControl::Query(control)),
                    None => source
                        .native_source_control()
                        .map(NativeSourceControl::Original),
                };
                Self { source, control }
            }
        }
    };
}
native_source_arc!(dyn RawVectorMembershipSource);
native_source_arc!(dyn RawVectorReverseMembershipSource);
native_source_arc!(dyn DormantRawVectorLoader);
native_source_arc!(dyn DurableRawColumnFactory);
native_source_arc!(dyn DurableRawPartitionFactory);

/// One bounded decoder; output ownership remains with the explicit caller.
struct OwnedVectorEntries<'a> {
    state: &'a IndexState,
    index: &'a VectorIndexRef,
    membership: Option<Box<dyn RawVectorMembershipCursor>>,
    loader: Option<NativeSourceArc<dyn DormantRawVectorLoader>>,
    bodies: RawVectorBodyReads,
    scratch: Option<VectorCopyAdmission>,
    position: usize,
    count: usize,
}

impl OwnedVectorEntries<'_> {
    fn close(&mut self) {
        self.membership = None;
        self.loader = None;
        self.bodies = RawVectorBodyReads::default();
        self.scratch = None;
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn read_next(&mut self) -> Result<Option<VectorEntry>> {
        if self.scratch.is_none() {
            return Ok(None);
        }
        if let Some(cursor) = self.membership.as_mut() {
            let mut refusal = None;
            let scratch = self.scratch.as_mut().unwrap();
            let next = cursor.next_with_body_identity(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => match scratch.reserve(bytes) {
                    Ok(()) => {
                        #[cfg(feature = "test-seams")]
                        contextdb_core::family_loads::loaded(
                            contextdb_core::family_loads::Family::VectorMembership,
                            bytes,
                        );
                        true
                    }
                    Err(error) => {
                        refusal = Some(error);
                        false
                    }
                },
                _ => true,
            });
            if let Some(error) = refusal {
                return Err(error);
            }
            let Some((_, identity)) = next? else {
                self.close();
                return Ok(None);
            };
            if self.position >= self.count {
                return Err(Error::Other(
                    "vector copy grew beyond its admitted summary".into(),
                ));
            }
            let mut control = self.scratch.as_ref().unwrap().scratch();
            let mut transient = self.scratch.as_ref().unwrap().scratch();
            let mut refusal = None;
            let mut request = |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => match transient.reserve(bytes) {
                    Ok(()) => true,
                    Err(error) => {
                        refusal = Some(error);
                        false
                    }
                },
                _ => true,
            };
            let loader = self.loader.as_ref().unwrap();
            if let Some(receipt) = cursor.selected_receipt()
                && let Some(scan) =
                    self.bodies
                        .scan_for_membership(loader.as_ref(), Some(receipt), &mut control)?
            {
                loader.bind_streamed_membership(scan, receipt)?;
            }
            let loaded = match self.bodies.scan_for(loader.as_ref(), &mut control)? {
                Some(scan) => {
                    loader.load_streamed_candidate_with_scan(scan, &identity, true, &mut request)
                }
                None => loader.load_candidate(&identity, true, &mut request),
            };
            if let Some(error) = refusal {
                return Err(error);
            }
            let loaded = loaded?;
            let entry = loaded.entry();
            if entry.index != *self.index
                || entry.row_id != identity.row_id
                || entry.created_tx != identity.created_tx
                || entry.deleted_tx != identity.deleted_tx
                || entry.lsn != identity.lsn
                || entry.vector.len() != self.state.dimension
            {
                return Err(Error::Other(
                    "raw vector body disagrees with admitted copy identity".into(),
                ));
            }
            let entry = entry.clone();
            drop(loaded);
            drop(transient);
            self.position += 1;
            Ok(Some(entry))
        } else {
            let entry = {
                let vectors = self.state.vectors.read();
                if self.position < vectors.entries.len() && self.position >= self.count {
                    return Err(Error::Other(
                        "vector copy grew beyond its admitted summary".into(),
                    ));
                }
                vectors
                    .entries
                    .get(self.position)
                    .map(|entry| entry.to_vector_entry(self.index.clone()))
            };
            self.position += usize::from(entry.is_some());
            if entry.is_none() {
                self.close();
            }
            Ok(entry)
        }
    }
}

impl Iterator for OwnedVectorEntries<'_> {
    type Item = Result<VectorEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(error) => {
                self.close();
                Some(Err(error))
            }
        }
    }
}

impl Drop for OwnedVectorEntries<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

/// One decoded body in a partition whose route is retained by its loader.
#[doc(hidden)]
#[derive(Debug)]
pub struct LoadedRawVectorEntry {
    pub entry: VectorEntry,
    pub membership: VectorMembershipProvenance,
    pub canonical_deleted_tx: Option<TxId>,
}

impl LoadedRawVectorEntry {
    pub fn new(
        entry: VectorEntry,
        membership: VectorMembershipProvenance,
        canonical_deleted_tx: Option<TxId>,
    ) -> Self {
        Self {
            entry,
            membership,
            canonical_deleted_tx,
        }
    }
}

/// A loader returns one complete partition. Permanent stored bodies are
/// reserved with `accountant`; decoded carriers retain their executing read
/// reservation in `workspace_owner` until the last carrier is consumed.
/// An abandoned load frees its carriers before releasing either reservation.
#[doc(hidden)]
pub struct LoadedRawVectorPartition {
    pub entries: Vec<LoadedRawVectorEntry>,
    pub resident_bytes: usize,
    pub workspace_owner: VectorCopyAdmission,
    pub accountant: crate::memory_budget::MemoryBudgetOwner,
}

pub(crate) struct HistoricalRawVectorPartition {
    loaded: LoadedRawVectorPartition,
}
impl std::fmt::Debug for HistoricalRawVectorPartition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HistoricalRawVectorPartition")
            .field("entries", &self.loaded.entries.len())
            .finish()
    }
}
impl HistoricalRawVectorPartition {
    pub(crate) fn len(&self) -> usize {
        self.loaded.entries.len()
    }
    pub(crate) fn entry(&self, position: usize) -> Option<&LoadedRawVectorEntry> {
        self.loaded.entries.get(position)
    }
}

impl LoadedRawVectorPartition {
    fn into_parts(
        mut self,
    ) -> (
        Vec<LoadedRawVectorEntry>,
        PaidRetainedMemoryCharge,
        VectorCopyAdmission,
    ) {
        let entries = std::mem::take(&mut self.entries);
        let resident = PaidRetainedMemoryCharge::new(
            std::mem::take(&mut self.resident_bytes),
            self.accountant.clone(),
        );
        // The empty replacement owns no credit. The returned admission keeps
        // the actual loader workspace owner through consumption of the entries.
        let empty = self.workspace_owner.scratch();
        let workspace = std::mem::replace(&mut self.workspace_owner, empty);
        (entries, resident, workspace)
    }
}

impl Drop for LoadedRawVectorPartition {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.entries));
        self.accountant
            .release(std::mem::take(&mut self.resident_bytes));
        // workspace_owner drops after the carriers have been freed.
    }
}

#[cfg(test)]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn load_co_created_fixture_carriers(
    entries: &[VectorEntry],
    directory: Option<&[RawVectorDirectoryEntry]>,
    accountant: Arc<dyn MemoryBudget>,
    quantization: VectorQuantization,
    executing: &mut VectorCopyAdmission,
) -> Result<LoadedRawVectorPartition> {
    if let Some(directory) = directory {
        assert_eq!(entries.len(), directory.len());
    }
    let resident_bytes = entries
        .iter()
        .map(|entry| {
            crate::quantized::stored_vector_resident_bytes(entry.vector.len(), quantization)
        })
        .sum::<usize>();
    let workspace_bytes = entries.iter().try_fold(
        entries
            .len()
            .checked_mul(std::mem::size_of::<LoadedRawVectorEntry>())
            .ok_or_else(|| Error::Other("fixture carrier capacity overflow".into()))?,
        |bytes, entry| {
            bytes
                .checked_add(entry.index.table.len())
                .and_then(|bytes| bytes.checked_add(entry.index.column.len()))
                .and_then(|bytes| {
                    bytes.checked_add(entry.vector.len().checked_mul(std::mem::size_of::<f32>())?)
                })
                .ok_or_else(|| Error::Other("fixture carrier workspace overflow".into()))
        },
    )?;
    let mut workspace_owner = executing.scratch();
    workspace_owner.reserve(workspace_bytes)?;
    let mut resident_owner = VectorCopyAdmission::new(accountant.clone());
    resident_owner.reserve(resident_bytes)?;
    let mut carriers = Vec::with_capacity(entries.len());
    for (position, entry) in entries.iter().enumerate() {
        let membership = match directory {
            Some(directory) => directory[position].membership,
            // Only the explicitly co-created pruning fixture calls this arm.
            None => VectorMembershipProvenance::new(
                entry.created_tx,
                entry.lsn,
                entry.created_tx,
                entry.deleted_tx,
            ),
        };
        carriers.push(LoadedRawVectorEntry::new(
            entry.clone(),
            membership,
            entry.deleted_tx,
        ));
    }
    resident_owner.transfer();
    Ok(LoadedRawVectorPartition {
        entries: carriers,
        resident_bytes,
        workspace_owner,
        accountant: accountant.into(),
    })
}

/// One decoded raw-vector body held only for the duration of candidate
/// scoring. Loader-owned reservations are returned on drop. A bounded caller
/// instead owns `transient_bytes` through its request reservation and releases
/// that charge after dropping this value.
#[doc(hidden)]
pub struct LoadedRawVectorCandidate {
    entry: VectorEntry,
    transient_bytes: usize,
    loader_accountant: Option<crate::memory_budget::MemoryBudgetOwner>,
}

impl LoadedRawVectorCandidate {
    #[doc(hidden)]
    pub fn loader_charged(
        entry: VectorEntry,
        transient_bytes: usize,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Self {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

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
        if let Some(accountant) = self.loader_accountant.as_ref() {
            // The raw body and both identifier allocations physically retire
            // before their loader-owned debit is returned.
            drop(std::mem::take(&mut self.entry.vector));
            drop(std::mem::take(&mut self.entry.index.table));
            drop(std::mem::take(&mut self.entry.index.column));
            accountant.release(self.transient_bytes);
        }
    }
}

/// Creates the durable membership source and body loader of a partition
/// that a durable-membership column first writes after open. Creation only
/// builds handles; it reads nothing and cannot fail.
/// Creates the durable reverse source and partition factory of a vector
/// column declared after a durable-membership store opened.
/// A loader's state for one statement's one-pass scan, kept across the
/// statement's resumed steps.
#[doc(hidden)]
#[derive(Default)]
pub struct OnePassSession(pub Option<Box<dyn std::any::Any + Send + Sync>>);

impl std::fmt::Debug for OnePassSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OnePassSession")
            .field(&self.0.is_some())
            .finish()
    }
}

#[doc(hidden)]
pub trait DurableRawColumnFactory: Send + Sync {
    /// Internal native assembly credit; source holders keep this outside Arc.
    #[doc(hidden)]
    fn native_source_control(&self) -> Option<contextdb_tx::SourceCharge> {
        None
    }
    /// Independent executing-reader control for a paged query source.
    #[doc(hidden)]
    fn query_source_control(&self) -> Option<QuerySourceControl> {
        None
    }

    /// Borrow retained metadata credits without creating or loading a column.
    /// Callbacks must not call store or source operations, including reads;
    /// visitation holds store guards or the lazy source mutex.
    fn visit_retained_copy_credits(
        &self,
        _caller: &dyn MemoryBudget,
        _visit: &mut dyn FnMut(usize, usize, bool),
    ) {
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn create(
        &self,
        index: &VectorIndexRef,
        layout: &VectorIndexLayout,
        workspace: VectorCopyAdmission,
        retained: VectorCopyAdmission,
    ) -> Result<(
        Arc<dyn RawVectorReverseMembershipSource>,
        Arc<dyn DurableRawPartitionFactory>,
    )>;
}

#[doc(hidden)]
pub trait DurableRawPartitionFactory: Send + Sync {
    /// Internal native assembly credit; source holders keep this outside Arc.
    #[doc(hidden)]
    fn native_source_control(&self) -> Option<contextdb_tx::SourceCharge> {
        None
    }
    /// Independent executing-reader control for a paged query source.
    #[doc(hidden)]
    fn query_source_control(&self) -> Option<QuerySourceControl> {
        None
    }

    /// A working catalog is authoritative even when the resident map is empty.
    /// Capture occurs after store guards are released, with the actual reader.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn capture_catalog(
        &self,
        _index: &VectorIndexRef,
        _owner: &mut VectorCopyAdmission,
    ) -> Result<Option<PagedVectorCatalogSelection>> {
        Ok(None)
    }
    /// Complete physical, live, retained and cap-occupied counts, with no
    /// query-owned partition handles or standing cache changes.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn catalog_scalar_counts(&self, _index: &VectorIndexRef) -> Result<Option<[usize; 6]>> {
        Ok(None)
    }
    /// Prevalidated maintained scalars for existing infallible observers.
    /// Working factories seed these before exposure and refresh on apply/DDL.
    fn published_catalog_counts(&self) -> Option<[usize; 6]> {
        None
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn catalog_scalar_occupied(
        &self,
        _index: &VectorIndexRef,
        _key: &VectorPartitionKey,
    ) -> Result<Option<bool>> {
        Ok(None)
    }

    /// Borrow retained metadata credits without creating or loading a partition.
    /// Callbacks must not call store or source operations, including reads;
    /// visitation holds store guards or the lazy source mutex.
    fn visit_retained_copy_credits(
        &self,
        _caller: &dyn MemoryBudget,
        _visit: &mut dyn FnMut(usize, usize, bool),
    ) {
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn create(
        &self,
        partition: &VectorPartitionRef,
        workspace: VectorCopyAdmission,
        retained: VectorCopyAdmission,
    ) -> Result<(
        Arc<dyn RawVectorMembershipSource>,
        Arc<dyn DormantRawVectorLoader>,
    )>;
}

#[doc(hidden)]
pub trait DormantRawVectorLoader: Send + Sync {
    /// Internal native assembly credit; source holders keep this outside Arc.
    #[doc(hidden)]
    fn native_source_control(&self) -> Option<contextdb_tx::SourceCharge> {
        None
    }
    /// Independent executing-reader control for a paged query source.
    #[doc(hidden)]
    fn query_source_control(&self) -> Option<QuerySourceControl> {
        None
    }

    /// Borrow retained metadata credits without creating or loading a partition.
    /// Callbacks must not call store or source operations, including reads;
    /// visitation holds store guards or the lazy source mutex.
    fn visit_retained_copy_credits(
        &self,
        _caller: &dyn MemoryBudget,
        _visit: &mut dyn FnMut(usize, usize, bool),
    ) {
    }

    /// Rebind a live writer loader after its durable index keys are renamed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn for_renamed_index(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
        retained: VectorCopyAdmission,
    ) -> Result<Arc<dyn DormantRawVectorLoader>> {
        let _ = (index, owner, retained);
        Err(Error::Other(
            "this raw vector loader cannot prepare an index rename".into(),
        ))
    }

    fn supports_bounded_candidate_load(&self) -> bool {
        false
    }

    /// Load every body of a durable-membership partition in durable key
    /// order, admitting the complete load from its count before any body is
    /// read.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_all(&self, owner: &mut VectorCopyAdmission) -> Result<LoadedRawVectorPartition> {
        let _ = owner;
        Err(Error::Other(
            "this raw vector loader cannot load a durable partition".into(),
        ))
    }

    /// A committed image has no raw-vector cache a later find reuses. When the
    /// whole partition does not fit the reader's declaration, exact comparison
    /// reads each stored body. A writable store keeps the whole-partition load,
    /// so a short declaration is a typed refusal rather than a silent scan.
    fn exact_comparison_streams_when_partition_does_not_fit(&self) -> bool {
        false
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load(
        &self,
        directory: &[RawVectorDirectoryEntry],
        owner: &mut VectorCopyAdmission,
    ) -> Result<LoadedRawVectorPartition>;

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_candidate(
        &self,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate>;

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn bind_streamed_membership(
        &self,
        scan: &mut dyn RawVectorBodyScan,
        receipt: &dyn std::any::Any,
    ) -> Result<()> {
        let _ = (scan, receipt);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_selected_candidate(
        &self,
        selected: &dyn RawVectorMembershipSelection,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.load_candidate(&selected.identity(), caller_charged, request)
    }

    /// Which open body read serves this loader. Loaders reporting the same
    /// nonzero value read the same pinned durable image, so one statement's
    /// scan serves all of them; zero means this loader has no such read.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn open_body_scan_for_membership(
        &self,
        receipt: &dyn std::any::Any,
    ) -> Result<Option<Box<dyn RawVectorBodyScan>>> {
        let _ = receipt;
        self.open_body_scan()
    }

    fn body_scan_control_bytes(&self) -> usize {
        0
    }

    fn body_scan_source(&self) -> usize {
        0
    }

    /// Open one read of authoritative bodies that a statement reuses for every
    /// candidate it compares: one pinned transaction, its tables opened once,
    /// and one authentication batch. `None` means this loader has none.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn open_body_scan(&self) -> Result<Option<Box<dyn RawVectorBodyScan>>> {
        Ok(None)
    }

    /// The membership was already authenticated by this statement's source cursor.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_streamed_candidate_with_scan(
        &self,
        scan: &mut dyn RawVectorBodyScan,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.load_candidate_with_scan(scan, identity, caller_charged, request)
    }

    /// Enter one-pass reads on this thread for an exhaustive comparison:
    /// every page it reads is read once and must not fill a shared page
    /// cache. `session` carries the scan's state across resumed steps of one
    /// statement; the returned guard ends the mode when dropped.
    fn one_pass_reads(&self, session: &mut OnePassSession) -> Option<Box<dyn std::any::Any>> {
        let _ = session;
        None
    }

    /// `load_candidate_with_scan` for a one-pass exhaustive scan: a body read
    /// once must not displace the warm working set of a shared page cache.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_candidate_with_scan_once(
        &self,
        scan: &mut dyn RawVectorBodyScan,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.load_candidate_with_scan(scan, identity, caller_charged, request)
    }

    /// `load_candidate`, reading the body through a statement's open scan.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_candidate_with_scan(
        &self,
        _scan: &mut dyn RawVectorBodyScan,
        identity: &RawVectorDirectoryEntry,
        caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedRawVectorCandidate> {
        self.load_candidate(identity, caller_charged, request)
    }
}

/// One statement's open read of stored vector bodies. It reads exactly the
/// directory identity it is handed, authenticates the body against the
/// committed image, and holds nothing of it afterwards: the caller admits the
/// body's bytes before asking and releases them once it has scored it.
#[doc(hidden)]
pub trait RawVectorBodyScan: Send {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn bind_streamed_membership(
        &mut self,
        partition: &VectorPartitionRef,
        receipt: &dyn std::any::Any,
    ) -> Result<()> {
        let _ = (partition, receipt);
        Ok(())
    }

    /// Read a body whose complete identity the source stream has authenticated.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn read_streamed_body(
        &mut self,
        partition: &VectorPartitionRef,
        identity: &RawVectorDirectoryEntry,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        self.read_body(partition, identity, dimension, quantization)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn read_body(
        &mut self,
        partition: &VectorPartitionRef,
        identity: &RawVectorDirectoryEntry,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry>;
}

/// The body reads one statement has open, so every exhaustive comparison it
/// makes -- across every partition it admits -- goes through one pinned
/// transaction instead of beginning one per vector.
#[derive(Default)]
pub struct RawVectorBodyReads {
    open: Option<(usize, Box<dyn RawVectorBodyScan>)>,
    // Declared after the scan: its explicit executing owner outlives all controls.
    control_owner: Option<VectorCopyAdmission>,
    /// Exhaustive comparison reads each body once.
    one_pass: bool,
    #[cfg(any(test, feature = "test-seams"))]
    opened: usize,
}

impl RawVectorBodyReads {
    /// Body reads for a one-pass exhaustive comparison.
    pub(crate) fn one_pass() -> Self {
        Self {
            one_pass: true,
            ..Self::default()
        }
    }

    /// A completed score no longer needs its table/proof handles while the
    /// next scalar row check owns a different source window. The operation's
    /// captured root remains retained by its membership source and receipt.
    pub(crate) fn release_scan(&mut self) {
        drop(self.open.take());
        drop(self.control_owner.take());
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn scan_for(
        &mut self,
        loader: &dyn DormantRawVectorLoader,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<&mut dyn RawVectorBodyScan>> {
        self.scan_for_membership(loader, None, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn scan_for_membership(
        &mut self,
        loader: &dyn DormantRawVectorLoader,
        receipt: Option<&dyn std::any::Any>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<&mut dyn RawVectorBodyScan>> {
        let source = loader.body_scan_source();
        if source == 0 {
            return Ok(None);
        }
        if self.open.as_ref().is_none_or(|(open, _)| *open != source) {
            let mut control_owner = owner.scratch();
            // RawVectorBodyReads is inline in its caller's already-paid
            // continuation (or on the stack). Only the newly boxed scan and
            // any genuinely new root need another allocation invoice.
            control_owner.reserve(loader.body_scan_control_bytes())?;
            let opened = match receipt {
                Some(receipt) => loader.open_body_scan_for_membership(receipt),
                None => loader.open_body_scan(),
            }?;
            let Some(scan) = opened else {
                return Ok(None);
            };
            self.open = Some((source, scan));
            self.control_owner = Some(control_owner);
            contextdb_core::find_attribution::count("raw_body_scans_opened", 1);
            #[cfg(any(test, feature = "test-seams"))]
            {
                self.opened += 1;
            }
        }
        Ok(self
            .open
            .as_mut()
            .map(|(_, scan)| scan.as_mut() as &mut dyn RawVectorBodyScan))
    }

    /// How many body reads this statement opened.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn opened_for_test(&self) -> usize {
        self.opened
    }
}

impl std::fmt::Debug for RawVectorBodyReads {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RawVectorBodyReads")
            .field("open", &self.open.is_some())
            .finish()
    }
}

#[doc(hidden)]
pub struct DormantRawVectorPartition {
    pub partition: VectorPartitionRef,
    pub directory: Vec<RawVectorDirectoryEntry>,
    pub loader: Arc<dyn DormantRawVectorLoader>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMaintenanceProgress {
    pub state: &'static str,
    pub reason: &'static str,
    pub vectors_total: usize,
    pub vectors_done: usize,
    pub checkpoint_tx: TxId,
}

/// The maintenance phase, committed progress and retained failure are copied
/// together for inspection. Writers keep the companion field under the same
/// lock, so retry progress cannot be paired with a pre-failure observation.
#[derive(Clone, Copy, Default)]
struct VectorMaintenanceInspection {
    progress: Option<VectorMaintenanceProgress>,
    failure: Option<VectorMaintenanceFailureDetails>,
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
        Self::insert(&mut self.created, entry.membership.visible_from);
        if let Some(tx) = entry.membership.deleted_tx {
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
            entry.membership.visible_from.0,
            entry.membership.deleted_tx.map_or(u64::MAX, |tx| tx.0),
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
    vectors: RwLock<PartitionVectors>,
    raw_directory: RwLock<Vec<RawVectorDirectoryEntry>>,
    #[cfg(test)]
    raw_directory_entries_inspected: AtomicU64,
    raw_directory_by_row: RwLock<HashMap<RowId, Vec<usize>>>,
    visibility_counts: RwLock<VectorVisibilityCounts>,
    raw_loader: RwLock<Option<NativeSourceArc<dyn DormantRawVectorLoader>>>,
    durable_memberships: RwLock<Option<NativeSourceArc<dyn RawVectorMembershipSource>>>,
    /// Counts of a durable-membership partition at the writer's current
    /// frontier: the registered summary plus every published commit since.
    durable_counts: Mutex<RawVectorMembershipSummary>,
    durable_pending: Mutex<DurablePendingLog>,
    raw_residency_gate: Arc<RwLock<()>>,
    raw_resident: AtomicBool,
    raw_resident_bytes: AtomicUsize,
    raw_accountant: RwLock<Option<crate::memory_budget::MemoryBudgetOwner>>,
    raw_payload_counter: RwLock<Arc<AtomicUsize>>,
    raw_payload_header_counter: RwLock<Arc<AtomicUsize>>,
    /// This state still owns the map key and raw historical directory.  The
    /// flag retires only its maintained graph/count identity, without
    /// allocating a second copy of that key in a side registry.
    historical_only: AtomicBool,
    /// This state's cap class and directory figure as last reported to the
    /// column that holds it. Mutators report differences on return.
    limit_link: Mutex<PartitionLimitLink>,
    /// Resident pending-work marks, kept exact by the directory mutators:
    /// the newest creation among open entries and the newest tombstone, so
    /// the cap edge answers "any pending work after this frontier" without a
    /// walk of the route's history.
    pending_marks: Mutex<ResidentPendingMarks>,
    maintenance_inspection: RwLock<VectorMaintenanceInspection>,
    #[cfg(any(test, feature = "test-seams"))]
    maintenance_step_pause: Mutex<Option<Arc<MaintenanceProgressPauseSlot>>>,
    maintenance_policy_revision: AtomicU64,
    #[cfg(any(test, feature = "test-seams"))]
    passive_activity: OnceLock<Arc<VectorPassiveActivity>>,
}

#[doc(hidden)]
pub struct VectorMaintenanceProgressGuard {
    state: Arc<IndexState>,
}

impl VectorMaintenanceProgressGuard {
    pub fn advance(&self, done: usize) {
        self.state.set_maintenance_vectors_done(done);
    }
}

impl Drop for VectorMaintenanceProgressGuard {
    fn drop(&mut self) {
        self.state.maintenance_inspection.write().progress = None;
    }
}

/// Opaque captured private directory. Each visit owns one admitted identity
/// window and releases native guards before constructing sources or callbacks.
#[doc(hidden)]
pub trait PagedVectorCatalog: Send + Sync {
    fn counts(&self) -> [usize; 6];
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn validate(&self) -> Result<()>;
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[expect(
        clippy::type_complexity,
        reason = "The captured catalog callback keeps route metadata and both original source handles together without an allocating adapter."
    )]
    fn visit_empty(
        &self,
        owner: &mut VectorCopyAdmission,
        visit: &mut dyn FnMut(
            &VectorPartitionKey,
            RawVectorMembershipSummary,
            bool,
            Arc<dyn RawVectorMembershipSource>,
            Arc<dyn DormantRawVectorLoader>,
        ) -> Result<bool>,
    ) -> Result<()>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn probe(
        &self,
        key: &VectorPartitionKey,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<PagedVectorPartitionSources>>;
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[expect(
        clippy::type_complexity,
        reason = "The captured catalog callback keeps route metadata and both original source handles together without an allocating adapter."
    )]
    fn visit(
        &self,
        owner: &mut VectorCopyAdmission,
        before_read: &mut dyn FnMut() -> Result<()>,
        visit: &mut dyn FnMut(
            &VectorPartitionKey,
            RawVectorMembershipSummary,
            bool,
            Arc<dyn RawVectorMembershipSource>,
            Arc<dyn DormantRawVectorLoader>,
        ) -> Result<()>,
    ) -> Result<()>;
}
#[doc(hidden)]
pub struct PagedVectorPartitionSources {
    pub memberships: Arc<dyn RawVectorMembershipSource>,
    pub loader: Arc<dyn DormantRawVectorLoader>,
    pub summary: RawVectorMembershipSummary,
    pub historical_only: bool,
    pub control: QuerySourceControl,
}
#[doc(hidden)]
pub struct PagedVectorCatalogSelection {
    pub catalog: Box<dyn PagedVectorCatalog>,
    pub control: QuerySourceControl,
}

/// Every query-owned state clone carries the reader's actual control invoice.
/// No bare Arc is returned by this handle.
#[doc(hidden)]
pub struct VectorQueryState {
    state: Arc<IndexState>,
    control: Option<QuerySourceControl>,
}
impl From<Arc<IndexState>> for VectorQueryState {
    fn from(state: Arc<IndexState>) -> Self {
        Self {
            state,
            control: None,
        }
    }
}
impl Clone for VectorQueryState {
    fn clone(&self) -> Self {
        let control = self.control.clone();
        let state = self.state.clone();
        Self { state, control }
    }
}
impl std::ops::Deref for VectorQueryState {
    type Target = IndexState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl AsRef<IndexState> for VectorQueryState {
    fn as_ref(&self) -> &IndexState {
        &self.state
    }
}

/// Captured source selection owns no list of all partition identities.
#[doc(hidden)]
pub struct PagedVectorSourceSelection {
    catalog: PagedVectorCatalogSelection,
    dimension: usize,
    quantization: VectorQuantization,
    partitioned: bool,
    payload: Arc<AtomicUsize>,
    payload_headers: Arc<AtomicUsize>,
}
impl PagedVectorSourceSelection {
    pub fn len(&self) -> usize {
        self.catalog.catalog.len()
    }
    pub fn is_empty(&self) -> bool {
        self.catalog.catalog.is_empty()
    }
    pub fn counts(&self) -> [usize; 6] {
        self.catalog.catalog.counts()
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn empty_declared_state(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorQueryState>> {
        self.catalog.catalog.validate()?;
        if self.partitioned || !self.is_empty() {
            return Ok(None);
        }
        let mut control = owner.scratch();
        control.reserve(
            contextdb_tx::staged_source::source_arc_layout::<IndexState>()
                .size()
                .checked_add(contextdb_tx::staged_source::source_arc_layout::<RwLock<()>>().size())
                .and_then(|n| {
                    n.checked_add(
                        2 * contextdb_tx::staged_source::source_arc_layout::<AtomicUsize>().size(),
                    )
                })
                .ok_or_else(|| Error::Other("empty declared vector state size overflow".into()))?,
        )?;
        let control = QuerySourceControl::new(control)?;
        let state = Arc::new(IndexState::new(self.dimension, self.quantization));
        self.catalog.catalog.validate()?;
        Ok(Some(VectorQueryState {
            state,
            control: Some(control),
        }))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn probe(
        &self,
        key: &VectorPartitionKey,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorQueryState>> {
        let Some(source) = self.catalog.catalog.probe(key, owner)? else {
            return Ok(None);
        };
        self.make_state(
            source.memberships.clone(),
            source.loader.clone(),
            source.summary,
            source.historical_only,
            owner,
        )
        .map(Some)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn make_state(
        &self,
        memberships: Arc<dyn RawVectorMembershipSource>,
        loader: Arc<dyn DormantRawVectorLoader>,
        summary: RawVectorMembershipSummary,
        historical_only: bool,
        owner: &mut VectorCopyAdmission,
    ) -> Result<VectorQueryState> {
        let mut control = owner.scratch();
        control.reserve(
            contextdb_tx::staged_source::source_arc_layout::<IndexState>()
                .size()
                .checked_add(contextdb_tx::staged_source::source_arc_layout::<RwLock<()>>().size())
                .ok_or_else(|| Error::Other("query vector state control overflow".into()))?,
        )?;
        let control = QuerySourceControl::new(control)?;
        let mut transient = owner.scratch();
        transient
            .reserve(2 * contextdb_tx::staged_source::source_arc_layout::<AtomicUsize>().size())?;
        let state = Arc::new(IndexState::new(self.dimension, self.quantization));
        state.install_durable_source(
            memberships,
            loader,
            summary,
            self.payload.clone(),
            self.payload_headers.clone(),
        );
        state
            .historical_only
            .store(historical_only, Ordering::SeqCst);
        drop(transient);
        Ok(VectorQueryState {
            state,
            control: Some(control),
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn visit_empty<E: From<Error>>(
        &self,
        owner: &mut VectorCopyAdmission,
        mut visit: impl FnMut(&VectorPartitionKey, VectorQueryState) -> std::result::Result<bool, E>,
    ) -> std::result::Result<(), E> {
        let mut callback_error = None;
        let mut source_owner = owner.scratch();
        let result = self.catalog.catalog.visit_empty(
            &mut source_owner,
            &mut |key, summary, historical, memberships, loader| {
                let state = self.make_state(memberships, loader, summary, historical, owner)?;
                match visit(key, state) {
                    Ok(keep_going) => Ok(keep_going),
                    Err(error) => {
                        callback_error = Some(error);
                        Err(Error::ReadCancelled)
                    }
                }
            },
        );
        match callback_error {
            Some(error) => Err(error),
            None => result.map_err(E::from),
        }
    }
    pub fn visit<E: From<Error>>(
        &self,
        owner: &mut VectorCopyAdmission,
        visit: impl FnMut(&VectorPartitionKey, VectorQueryState) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        self.visit_checked(owner, &mut || Ok(()), visit)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn visit_checked<E: From<Error>>(
        &self,
        owner: &mut VectorCopyAdmission,
        before: &mut impl FnMut() -> std::result::Result<(), E>,
        mut visit: impl FnMut(&VectorPartitionKey, VectorQueryState) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        let callback_error = std::cell::RefCell::new(None);
        let mut source_owner = owner.scratch();
        let result = self.catalog.catalog.visit(
            &mut source_owner,
            &mut || {
                if let Err(error) = before() {
                    *callback_error.borrow_mut() = Some(error);
                    return Err(Error::ReadCancelled);
                }
                Ok(())
            },
            &mut |key, summary, historical_only, memberships, loader| {
                let state =
                    self.make_state(memberships, loader, summary, historical_only, owner)?;
                if let Err(error) = visit(key, state) {
                    *callback_error.borrow_mut() = Some(error);
                    return Err(Error::ReadCancelled);
                }
                Ok(())
            },
        );
        match callback_error.into_inner() {
            Some(error) => Err(error),
            None => result.map_err(E::from),
        }
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
    fn allocation_bytes() -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(std::mem::size_of::<RwLock<()>>())
            .saturating_add(2 * std::mem::size_of::<usize>())
    }

    fn new(dimension: usize, quantization: VectorQuantization) -> Self {
        Self {
            dimension,
            quantization,
            maintenance: Mutex::new(()),
            vectors: RwLock::new(PartitionVectors::default()),
            raw_directory: RwLock::new(Vec::new()),
            #[cfg(test)]
            raw_directory_entries_inspected: AtomicU64::new(0),
            raw_directory_by_row: RwLock::new(HashMap::new()),
            visibility_counts: RwLock::new(VectorVisibilityCounts::default()),
            raw_loader: RwLock::new(None),
            durable_memberships: RwLock::new(None),
            durable_counts: Mutex::new(RawVectorMembershipSummary::default()),
            durable_pending: Mutex::new(DurablePendingLog::default()),
            raw_residency_gate: Arc::new(RwLock::new(())),
            raw_resident: AtomicBool::new(true),
            raw_resident_bytes: AtomicUsize::new(0),
            raw_accountant: RwLock::new(None),
            raw_payload_counter: RwLock::new(Arc::new(AtomicUsize::new(0))),
            raw_payload_header_counter: RwLock::new(Arc::new(AtomicUsize::new(0))),
            historical_only: AtomicBool::new(false),
            limit_link: Mutex::new(PartitionLimitLink::default()),
            pending_marks: Mutex::new(ResidentPendingMarks::default()),
            maintenance_inspection: RwLock::new(VectorMaintenanceInspection::default()),
            #[cfg(any(test, feature = "test-seams"))]
            maintenance_step_pause: Mutex::new(None),
            maintenance_policy_revision: AtomicU64::new(0),
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

    fn is_historical_only(&self) -> bool {
        self.historical_only.load(Ordering::SeqCst)
    }

    /// Sample only fixed scalar metadata, including from a borrowed source
    /// view. This takes the existing field locks but no maintenance mutex.
    #[doc(hidden)]
    pub fn source_maintenance_metadata(&self) -> VectorPartitionSourceMaintenanceMetadata {
        VectorPartitionSourceMaintenanceMetadata {
            historical_only: self.is_historical_only(),
            failure: self.maintenance_failure_details(),
        }
    }

    fn mark_historical_only(&self) {
        self.historical_only.store(true, Ordering::SeqCst);
        self.reclassify_limit();
    }

    fn reactivate_maintained_state(&self) {
        self.historical_only.store(false, Ordering::SeqCst);
        self.reclassify_limit();
    }

    /// Whether this resident partition has pending inserts or tombstones
    /// after `covered`, from its fixed marks.
    fn resident_pending_after(&self, covered: Option<TxId>) -> bool {
        let marks = *self.pending_marks.lock();
        let after =
            |tx: Option<TxId>| tx.is_some_and(|tx| covered.is_none_or(|covered| tx > covered));
        after(marks.newest_open_created) || after(marks.newest_tombstone)
    }

    fn limit_class(&self) -> PartitionLimitClass {
        if self.is_historical_only() || self.entry_count() == 0 {
            PartitionLimitClass::NotCounted
        } else if self.vector_count() == 0 {
            PartitionLimitClass::CountedEmpty
        } else {
            PartitionLimitClass::CountedLive
        }
    }

    /// Report this state's own class and directory differences to the column
    /// holding it. Callers have released every field guard they took; the
    /// work reads only this partition's fixed counts and reverse directory.
    fn reclassify_limit(&self) {
        let mut link = self.limit_link.lock();
        let Some(ledger) = link.ledger.upgrade() else {
            return;
        };
        let class = self.limit_class();
        let key = link
            .key
            .clone()
            .expect("an attached partition state holds its key");
        ledger.apply(&link.this, &key, link.class, class);
        link.class = class;
        if let Some(directory) = link.directory.upgrade() {
            let bytes = self.directory_byte_count();
            let before = link.directory_bytes;
            if bytes != before {
                directory.adjust(|figures| {
                    add_signed(&mut figures.raw_reverse_directory, before, bytes);
                });
                link.directory_bytes = bytes;
            }
        }
    }

    /// Enter one column's map under `key`. Idempotent for the same column and
    /// key; a state moved from another column leaves that column first.
    fn attach_limit(
        this: &Arc<Self>,
        ledger: &Arc<PartitionLimitLedger>,
        directory: &Arc<DirectoryTally>,
        key: &VectorPartitionKey,
    ) {
        {
            let link = this.limit_link.lock();
            if std::ptr::eq(link.ledger.as_ptr(), Arc::as_ptr(ledger)) {
                drop(link);
                this.reclassify_limit();
                return;
            }
        }
        Self::attach_limit_with_prepared_key(this, ledger, directory, Arc::new(key.clone()));
    }

    // Erasure supplies an already admitted key and preallocated ledger buckets.
    fn attach_limit_with_prepared_key(
        this: &Arc<Self>,
        ledger: &Arc<PartitionLimitLedger>,
        directory: &Arc<DirectoryTally>,
        key: Arc<VectorPartitionKey>,
    ) {
        this.detach_limit();
        let mut link = this.limit_link.lock();
        let weak = Arc::downgrade(this);
        let class = this.limit_class();
        ledger.apply(&weak, &key, PartitionLimitClass::NotCounted, class);
        let bytes = this.directory_byte_count();
        let (vectors, keys) = partition_entry_terms(key.estimated_bytes());
        directory.adjust(|figures| {
            figures.raw_reverse_directory = figures.raw_reverse_directory.saturating_add(bytes);
            figures.partition_vectors = figures.partition_vectors.saturating_add(vectors);
            figures.typed_partition_keys = figures.typed_partition_keys.saturating_add(keys);
        });
        *link = PartitionLimitLink {
            ledger: Arc::downgrade(ledger),
            this: weak,
            key: Some(key),
            class,
            directory_bytes: bytes,
            directory: Arc::downgrade(directory),
        };
    }

    /// Leave `ledger`'s column if this state is attached there; a state
    /// shared with a staged image stays with its live column.
    fn detach_limit_from(&self, ledger: &Arc<PartitionLimitLedger>) {
        if std::ptr::eq(self.limit_link.lock().ledger.as_ptr(), Arc::as_ptr(ledger)) {
            self.detach_limit();
        }
    }

    /// Leave the column map this state was attached to, if any.
    fn detach_limit(&self) {
        let mut link = self.limit_link.lock();
        let previous = std::mem::take(&mut *link);
        drop(link);
        let (Some(ledger), Some(key)) = (previous.ledger.upgrade(), previous.key) else {
            return;
        };
        ledger.apply(
            &previous.this,
            &key,
            previous.class,
            PartitionLimitClass::NotCounted,
        );
        if let Some(directory) = previous.directory.upgrade() {
            let (vectors, keys) = partition_entry_terms(key.estimated_bytes());
            directory.adjust(|figures| {
                add_signed(
                    &mut figures.raw_reverse_directory,
                    previous.directory_bytes,
                    0,
                );
                add_signed(&mut figures.partition_vectors, vectors, 0);
                add_signed(&mut figures.typed_partition_keys, keys, 0);
            });
        }
    }

    fn maintenance_inspection(&self) -> VectorMaintenanceInspection {
        *self.maintenance_inspection.read()
    }

    fn maintenance_failure_details(&self) -> Option<VectorMaintenanceFailureDetails> {
        self.maintenance_inspection.read().failure
    }

    fn record_maintenance_failure(&self, details: VectorMaintenanceFailureDetails) {
        self.maintenance_inspection.write().failure = Some(details);
    }

    fn clear_maintenance_failure(&self) {
        self.maintenance_inspection.write().failure = None;
    }

    fn begin_maintenance_progress_at(
        self: &Arc<Self>,
        state: &'static str,
        reason: &'static str,
        vectors_total: usize,
        checkpoint_tx: TxId,
    ) -> VectorMaintenanceProgressGuard {
        self.maintenance_inspection.write().progress = Some(VectorMaintenanceProgress {
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
        if let Some(progress) = self.maintenance_inspection.write().progress.as_mut() {
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
            membership: entry.membership,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn install_dormant_raw_directory(
        &self,
        entries: Vec<RawVectorDirectoryEntry>,
        loader: Arc<dyn DormantRawVectorLoader>,
    ) -> Result<()> {
        let loader: NativeSourceArc<dyn DormantRawVectorLoader> = loader.into();
        let _reclassify = ReclassifyOnReturn(self);
        let _gate = self.raw_residency_gate.write();
        if !self.vectors.read().entries.is_empty() || !self.raw_directory.read().is_empty() {
            return Err(Error::Other(
                ("raw vector directory can only be installed into an empty partition".to_string())
                    .into(),
            ));
        }
        *self.raw_directory.write() = entries;
        self.rebuild_raw_directory_positions();
        *self.raw_loader.write() = Some(loader);
        self.raw_resident.store(false, Ordering::SeqCst);
        self.raw_resident_bytes.store(0, Ordering::SeqCst);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn attach_durable_raw_loader(
        &self,
        mut expected: Vec<RawVectorDirectoryEntry>,
        loader: Arc<dyn DormantRawVectorLoader>,
    ) -> Result<()> {
        let loader: NativeSourceArc<dyn DormantRawVectorLoader> = loader.into();
        let _reclassify = ReclassifyOnReturn(self);
        let _gate = self.raw_residency_gate.write();
        let order = |entry: &RawVectorDirectoryEntry| {
            (
                entry.row_id,
                entry.created_tx,
                entry.lsn,
                entry.deleted_tx,
                entry.membership.owning_row_tx,
                entry.membership.row_lsn,
                entry.membership.visible_from,
                entry.membership.deleted_tx,
            )
        };
        expected.sort_unstable_by_key(order);
        self.raw_directory.write().sort_unstable_by_key(order);
        self.rebuild_raw_directory_positions();
        if self.raw_directory.read().as_slice() != expected {
            return Err(Error::Other(
                ("durable raw-vector membership changed outside publication".to_string()).into(),
            ));
        }
        // Commits append to the resident directory, while Redb enumerates the
        // same durable set by key. Canonicalize both without another O(n)
        // allocation before later eviction makes them positional again.
        *self.raw_loader.write() = Some(loader);
        Ok(())
    }

    pub(crate) fn durable_membership_source(
        &self,
    ) -> Option<NativeSourceArc<dyn RawVectorMembershipSource>> {
        self.durable_memberships.read().clone()
    }

    fn install_durable_source(
        &self,
        memberships: Arc<dyn RawVectorMembershipSource>,
        loader: Arc<dyn DormantRawVectorLoader>,
        summary: RawVectorMembershipSummary,
        payload: Arc<AtomicUsize>,
        payload_headers: Arc<AtomicUsize>,
    ) {
        let memberships: NativeSourceArc<dyn RawVectorMembershipSource> = memberships.into();
        let loader: NativeSourceArc<dyn DormantRawVectorLoader> = loader.into();
        let _reclassify = ReclassifyOnReturn(self);
        // All durable bodies, including new writes, use the declared page
        // cache. Retaining decoded bodies here would create an additional
        // population-sized cache outside that share.
        let resident = false;
        *self.durable_memberships.write() = Some(memberships);
        *self.durable_counts.lock() = summary;
        *self.durable_pending.lock() = DurablePendingLog {
            floor: summary.max_tx,
            events: Vec::new(),
            cached: None,
        };
        *self.raw_loader.write() = Some(loader);
        self.raw_resident.store(resident, Ordering::SeqCst);
        *self.raw_payload_counter.write() = payload;
        *self.raw_payload_header_counter.write() = payload_headers;
    }

    /// Whether any retained membership of this partition is visible to one of
    /// `snapshots`. Resident partitions answer from their maintained
    /// visibility boundaries (two binary searches per snapshot); durable
    /// partitions from their source's occupancy-interval index. Neither reads
    /// a membership record.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn visible_at_any_registered(
        &self,
        snapshots: &[SnapshotId],
        owner: &mut VectorCopyAdmission,
        streamed_records: &mut u64,
    ) -> Result<bool> {
        if snapshots.is_empty() {
            return Ok(false);
        }
        if let Some(source) = self.durable_membership_source() {
            let mut local_copy_admission = owner.local_scratch();
            let owner = local_copy_admission.as_mut().unwrap_or(owner);
            if let Some(visible) = source.occupancy_visible_at_any(snapshots, owner)? {
                return Ok(visible);
            }
            return self.durable_visible_at_any(snapshots, owner, streamed_records);
        }
        let counts = self.visibility_counts.read();
        Ok(snapshots
            .iter()
            .any(|snapshot| counts.count(*snapshot) != 0))
    }

    /// Whether any retained membership of this durable partition is visible
    /// to one of `snapshots`. Streams the durable directory once; an incomplete
    /// read refuses the preparation before any partition is retired.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn durable_visible_at_any(
        &self,
        snapshots: &[SnapshotId],
        owner: &mut VectorCopyAdmission,
        streamed_records: &mut u64,
    ) -> Result<bool> {
        let Some(source) = self.durable_membership_source() else {
            return Ok(true);
        };
        if snapshots.is_empty() {
            return Ok(false);
        }
        let mut scratch = owner.scratch();
        let mut scan = || -> Result<bool> {
            let mut cursor =
                with_membership_copy_admission(&mut scratch, |request| source.open(request))?;
            while let Some(entry) =
                with_membership_copy_admission(&mut scratch, |request| cursor.next(request))?
            {
                *streamed_records += 1;
                if snapshots.iter().any(|snapshot| {
                    entry.membership.visible_from.0 <= snapshot.0
                        && entry
                            .membership
                            .deleted_tx
                            .is_none_or(|tx| tx.0 > snapshot.0)
                }) {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        scan()
    }

    /// Keep the pending-work log and any cached frontier count current with
    /// one published batch.
    fn apply_durable_pending_events(&self, events: &[DurableMembershipEvent]) {
        if self
            .durable_membership_source()
            .is_some_and(|source| source.has_durable_pending_counts())
        {
            // The commit updated fixed durable counters. Retaining a second
            // resident suffix would grow with writes outside the cache share.
            return;
        }
        let mut log = self.durable_pending.lock();
        if let Some(counts) = log.cached.as_mut() {
            Self::count_pending_events(counts, events);
        }
        if log.events.len().saturating_add(events.len()) > DURABLE_PENDING_LOG_CAPACITY {
            // Bounded: forget the log; later frontiers before the new floor
            // are counted by one stream.
            log.events = Vec::new();
            log.floor = self.durable_counts.lock().max_tx;
            return;
        }
        log.events.extend_from_slice(events);
    }

    fn count_pending_events(counts: &mut DurablePendingCounts, events: &[DurableMembershipEvent]) {
        let after = |tx: TxId| counts.covered.is_none_or(|covered| tx > covered);
        let (mut pending, mut tombstones) = (counts.pending_inserts, counts.tombstones);
        for event in events {
            match *event {
                DurableMembershipEvent::Closed { created, deleted } => {
                    if after(created) {
                        pending = pending.saturating_sub(1);
                    }
                    if after(deleted) {
                        tombstones = tombstones.saturating_add(1);
                    }
                }
                DurableMembershipEvent::Opened(created) => {
                    if after(created) {
                        pending = pending.saturating_add(1);
                    }
                }
                DurableMembershipEvent::PublishedClosed(deleted) => {
                    if after(deleted) {
                        tombstones = tombstones.saturating_add(1);
                    }
                }
            }
        }
        counts.pending_inserts = pending;
        counts.tombstones = tombstones;
    }

    /// Pending work is authoritative fixed metadata for writable durable
    /// sources. Storage-neutral sources may answer from their bounded log;
    /// an unavailable frontier refuses rather than walking memberships.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn durable_pending_counts(
        &self,
        covered: Option<TxId>,
        covered_lsn: Option<Lsn>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<(usize, usize)> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        if let Some(source) = self.durable_membership_source()
            && let Some(counts) = source.pending_counts(covered, owner)?
        {
            return Ok(counts);
        }
        let _ = covered_lsn;
        let Some(frontier) = covered else {
            let counts = *self.durable_counts.lock();
            return Ok((
                counts.live_entries,
                counts.entries.saturating_sub(counts.live_entries),
            ));
        };
        {
            let mut log = self.durable_pending.lock();
            if let Some(counts) = log.cached
                && counts.covered == covered
            {
                return Ok((counts.pending_inserts, counts.tombstones));
            }
            if frontier >= log.floor {
                let mut counts = DurablePendingCounts {
                    covered,
                    pending_inserts: 0,
                    tombstones: 0,
                };
                Self::count_pending_events(&mut counts, &log.events);
                // Frontiers only advance: events wholly at or before this
                // one can no longer count.
                log.events.retain(|event| event.relevant_after(frontier));
                log.floor = frontier;
                log.cached = Some(counts);
                return Ok((counts.pending_inserts, counts.tombstones));
            }
        }
        Err(Error::StoreCorrupted {
            path: "vector pending metadata".into(),
            reason: "pending metadata does not represent the serving frontier".into(),
        })
    }

    /// The boolean pending-work fact, without constructing an inspection
    /// record or enumerating any other partition. The durable source keeps
    /// its captured root and uses the same frontier as full inspection.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn has_pending_work_for_read(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<bool> {
        if self.is_durable() {
            let (inserts, tombstones) = self.durable_pending_counts(None, None, owner)?;
            Ok(inserts != 0 || tombstones != 0)
        } else {
            Ok(self.resident_pending_after(None))
        }
    }

    /// Refresh the durable counts after a removal published elsewhere.
    /// A refusal reaches the caller before any directory pruning begins.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn refresh_durable_counts(&self, owner: &mut VectorCopyAdmission) -> Result<()> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        let Some(source) = self.durable_membership_source() else {
            return Ok(());
        };
        let summary = source.current_summary(owner)?;
        self.apply_refreshed_durable_counts(summary);
        Ok(())
    }

    fn apply_refreshed_durable_counts(&self, summary: RawVectorMembershipSummary) {
        let _reclassify = ReclassifyOnReturn(self);
        let previous = std::mem::replace(&mut *self.durable_counts.lock(), summary);
        if previous == summary {
            return;
        }
        self.evict_raw_vectors();
        let mut log = self.durable_pending.lock();
        log.cached = None;
        log.events = Vec::new();
        log.floor = TxId(u64::MAX);
    }

    /// Every visibility interval of a durable partition, streamed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn visit_durable_intervals(
        &self,
        mut visit: impl FnMut(&RawVectorDirectoryEntry) -> Result<()>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        let source = self.durable_membership_source().ok_or_else(|| {
            Error::Other("durable vector partition lost its membership source".into())
        })?;
        let mut scratch = owner.scratch();
        let mut cursor =
            with_membership_copy_admission(&mut scratch, |request| source.open(request))?;
        while let Some(entry) =
            with_membership_copy_admission(&mut scratch, |request| cursor.next(request))?
        {
            visit(&entry)?;
        }
        Ok(())
    }

    /// Copy the bodies of every durable interval `select` accepts, one point
    /// read each; a body is decoded only for a selected interval.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn durable_entries_selected(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        owner: &dyn Fn(usize) -> Result<()>,
        copy_owner: &mut VectorCopyAdmission,
        select: impl Fn(&RawVectorDirectoryEntry) -> bool,
    ) -> Result<Vec<VectorEntry>> {
        let mut metadata = copy_owner.scratch();
        let source = self.durable_membership_source().ok_or_else(|| {
            Error::Other("durable vector partition lost its membership source".into())
        })?;
        let loader = self.raw_loader.read().clone().ok_or_else(|| {
            Error::Other("durable vector membership has no raw-body loader".into())
        })?;
        let mut selected = Vec::new();
        let mut directory_scratch = copy_owner.scratch();
        let mut cursor =
            with_membership_copy_admission(&mut directory_scratch, |request| source.open(request))?;
        while let Some(interval) =
            with_membership_copy_admission(&mut directory_scratch, |request| cursor.next(request))?
        {
            if select(&interval) {
                owner(self.generation_copy_bytes(index, selected.len() + 1))?;
                selected.push((interval.row_id, interval.lsn));
            }
        }
        drop(cursor);
        drop(directory_scratch);
        let output_bytes = self.generation_copy_bytes(index, selected.len());
        owner(output_bytes)?;
        let mut entries = Vec::with_capacity(selected.len());
        for (row_id, lsn) in selected {
            let identity = source
                .identity_by_row_lsn(row_id, lsn, &mut || true, &mut metadata)?
                .ok_or_else(|| Error::StoreCorrupted {
                    path: format!("vector index {}.{}", index.table, index.column).into(),
                    reason: "durable membership interval has no body identity".into(),
                })?;
            let mut scratch = copy_owner.scratch();
            let mut refusal = None;
            let loaded = loader.load_candidate(&identity, true, &mut |request| {
                if let DormantVectorLoadRequest::Reserve(bytes) = request
                    && let Err(error) = scratch.reserve(bytes)
                {
                    refusal = Some(error);
                    return false;
                }
                true
            });
            if let Some(error) = refusal {
                return Err(error);
            }
            let mut loaded = loaded?;
            let mut entry = std::mem::replace(
                &mut loaded.entry,
                VectorEntry {
                    index: index.clone(),
                    row_id,
                    vector: Vec::new(),
                    created_tx: TxId(0),
                    deleted_tx: None,
                    lsn,
                },
            );
            drop(loaded);
            if entry.deleted_tx.is_some_and(|tx| tx.0 > snapshot.0) {
                entry.deleted_tx = None;
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    /// The live bodies at `snapshot` published after `after_lsn`, selected
    /// from the durable change suffix alone; `None` without a suffix.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn durable_suffix_entries(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        after_lsn: Lsn,
        owner: &dyn Fn(usize) -> Result<()>,
        copy_owner: &mut VectorCopyAdmission,
    ) -> Result<Option<Vec<VectorEntry>>> {
        let mut metadata = copy_owner.scratch();
        let source = self.durable_membership_source().ok_or_else(|| {
            Error::Other("durable vector partition lost its membership source".into())
        })?;
        let mut opened = BTreeMap::<(RowId, TxId, Lsn), TxId>::new();
        let mut closed = Vec::new();
        let supported = source.visit_suffix(
            after_lsn,
            &mut |change| {
                match change {
                    RawVectorSuffixChange::Opened {
                        identity,
                        visible_from,
                    } => {
                        owner(self.generation_copy_bytes(index, opened.len() + closed.len() + 1))?;
                        opened.insert(
                            (identity.row_id, identity.created_tx, identity.lsn),
                            visible_from,
                        );
                    }
                    RawVectorSuffixChange::Closed {
                        row_id,
                        created_tx,
                        lsn,
                        deleted_tx,
                    } => {
                        if deleted_tx.0 <= snapshot.0 {
                            owner(
                                self.generation_copy_bytes(index, opened.len() + closed.len() + 1),
                            )?;
                            closed.push((row_id, created_tx, lsn));
                        }
                    }
                }
                Ok(())
            },
            &mut metadata,
        )?;
        if !supported {
            return Ok(None);
        }
        for key in closed {
            opened.remove(&key);
        }
        let loader = self.raw_loader.read().clone().ok_or_else(|| {
            Error::Other("durable vector membership has no raw-body loader".into())
        })?;
        let output_bytes = self.generation_copy_bytes(index, opened.len());
        owner(output_bytes)?;
        let mut entries = Vec::with_capacity(opened.len());
        for ((row_id, _, lsn), visible_from) in opened {
            if visible_from.0 > snapshot.0 || lsn <= after_lsn {
                continue;
            }
            let identity = source
                .identity_by_row_lsn(row_id, lsn, &mut || true, &mut metadata)?
                .ok_or_else(|| Error::StoreCorrupted {
                    path: format!("vector index {}.{}", index.table, index.column).into(),
                    reason: "durable change suffix names a membership the directory lacks".into(),
                })?;
            let mut scratch = copy_owner.scratch();
            let mut refusal = None;
            let loaded = loader.load_candidate(&identity, true, &mut |request| {
                if let DormantVectorLoadRequest::Reserve(bytes) = request
                    && let Err(error) = scratch.reserve(bytes)
                {
                    refusal = Some(error);
                    return false;
                }
                true
            });
            if let Some(error) = refusal {
                return Err(error);
            }
            let mut loaded = loaded?;
            let mut entry = std::mem::replace(
                &mut loaded.entry,
                VectorEntry {
                    index: index.clone(),
                    row_id,
                    vector: Vec::new(),
                    created_tx: TxId(0),
                    deleted_tx: None,
                    lsn,
                },
            );
            drop(loaded);
            if entry.deleted_tx.is_some_and(|tx| tx.0 > snapshot.0) {
                entry.deleted_tx = None;
            }
            entries.push(entry);
        }
        Ok(Some(entries))
    }

    /// Body identities of every retained version of `rows` in a durable
    /// partition, each read from that row's own prefix.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn durable_row_versions(
        &self,
        rows: &HashSet<RowId>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<RawVectorDirectoryEntry>> {
        let source = self.durable_membership_source().ok_or_else(|| {
            Error::Other("durable vector partition lost its membership source".into())
        })?;
        owner.reserve(rows.len().saturating_mul(std::mem::size_of::<RowId>()))?;
        let mut metadata = owner.scratch();
        let mut ordered = rows.iter().copied().collect::<Vec<_>>();
        ordered.sort_unstable();
        let mut versions = Vec::new();
        for row in ordered {
            source.visit_row_versions(
                row,
                &mut || true,
                &mut |identity| {
                    owner.reserve(std::mem::size_of::<RawVectorDirectoryEntry>())?;
                    versions.reserve_exact(1);
                    versions.push(identity);
                    Ok(())
                },
                &mut metadata,
            )?;
        }
        Ok(versions)
    }

    /// Test selected lineages without retaining their version directory.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn durable_contains_rows(
        &self,
        rows: impl IntoIterator<Item = RowId>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<bool> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        let source = self.durable_membership_source().ok_or_else(|| {
            Error::Other("durable vector partition lost its membership source".into())
        })?;
        for row in rows {
            let mut found = false;
            source.visit_row_versions(
                row,
                &mut || true,
                &mut |_| {
                    found = true;
                    Ok(())
                },
                owner,
            )?;
            if found {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Drop a prepared resident image in favour of the durable directory it
    /// mirrors, keeping its counts and any graph built from it.
    fn convert_to_durable(
        &self,
        memberships: Arc<dyn RawVectorMembershipSource>,
        loader: Arc<dyn DormantRawVectorLoader>,
        payload: Arc<AtomicUsize>,
        payload_headers: Arc<AtomicUsize>,
    ) {
        let memberships: NativeSourceArc<dyn RawVectorMembershipSource> = memberships.into();
        let loader: NativeSourceArc<dyn DormantRawVectorLoader> = loader.into();
        let _reclassify = ReclassifyOnReturn(self);
        let summary = RawVectorMembershipSummary {
            entries: self.entry_count(),
            live_entries: self.vector_count(),
            max_tx: self.max_tx(),
        };
        {
            let _gate = self.raw_residency_gate.write();
            let mut vectors = self.vectors.write();
            *vectors = PartitionVectors::default();
            *self.raw_directory.write() = Vec::new();
            *self.raw_directory_by_row.write() = HashMap::new();
            *self.pending_marks.lock() = ResidentPendingMarks::default();
            *self.visibility_counts.write() = VectorVisibilityCounts::default();
            self.raw_resident_bytes.store(0, Ordering::SeqCst);
            self.raw_accountant.write().take();
        }
        self.install_durable_source(
            memberships.source.clone(),
            loader.source.clone(),
            summary,
            payload,
            payload_headers,
        );
    }

    /// One-pass reads for an exhaustive comparison over this partition.
    pub(crate) fn one_pass_reads(
        &self,
        session: &mut OnePassSession,
    ) -> Option<Box<dyn std::any::Any>> {
        let loader = self.raw_loader.read().clone();
        loader.and_then(|loader| loader.one_pass_reads(session))
    }

    /// Whether this partition's bodies are cached in memory.
    pub(crate) fn bodies_resident(&self) -> bool {
        self.raw_resident.load(Ordering::SeqCst)
    }

    /// Cache this durable partition's bodies when they fit. `Ok(false)` when
    /// the memory to hold them is refused; the caller then reads bodies one
    /// at a time through the durable source.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn try_cache_durable_bodies(&self, owner: &mut VectorCopyAdmission) -> Result<bool> {
        if self.is_durable()
            || (owner.historical_durable_reads() && self.durable_membership_source().is_some())
        {
            // Durable bodies and their shared-allocation headers are cached as
            // bytes in the writer's one declared buffer pool. A decoded whole
            // partition would be a second, population-sized cache outside that
            // share. Caller projections use admitted point reads instead.
            return Ok(false);
        }
        match self.ensure_raw_vectors_loaded(owner) {
            Ok(()) => Ok(true),
            Err(Error::MemoryBudgetExceeded { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Append one published body to a durable partition's body cache. The
    /// membership itself is durable; no directory is kept.
    fn push_resident_body(&self, mut entry: StoredVectorEntry) {
        if let Some(accountant) = self.raw_accountant.read().clone() {
            entry.vector.adopt_raw_charge(
                accountant,
                self.raw_payload_counter.read().clone(),
                self.raw_payload_header_counter.read().clone(),
            );
        }
        let mut vectors = self.vectors.write();
        // A scalar publication retains another visibility interval, but its
        // bit-identical stored body shares the prior allocation. Adopt the
        // incoming charge first so replacing that body returns its payload
        // and header reservation to their actual funder.
        if let Some(positions) = vectors.by_row.get(&entry.row_id)
            && let Some(prior) = positions
                .iter()
                .rev()
                .map(|position| &vectors.entries[*position])
                .find(|prior| prior.vector.representation_eq(&entry.vector))
        {
            entry.vector = prior.vector.clone();
        }
        if entry
            .vector
            .payload_identity()
            .is_none_or(|identity| !vectors.payload_memberships.contains_key(&identity))
        {
            self.raw_resident_bytes
                .fetch_add(entry.estimated_bytes(), Ordering::SeqCst);
        }
        vectors.push(entry);
    }

    /// Close a row's cached bodies at `deleted_tx`; returns the live bytes
    /// the closed bodies were costing (see `tombstone_row`).
    fn tombstone_resident_bodies(&self, row_id: RowId, deleted_tx: TxId) -> usize {
        let mut released = 0usize;
        let mut vectors = self.vectors.write();
        let PartitionVectors {
            entries, by_row, ..
        } = &mut *vectors;
        for &position in by_row.get(&row_id).into_iter().flatten() {
            if let Some(entry) = entries.get_mut(position) {
                let boundary = deleted_tx.max(entry.created_tx);
                if entry.deleted_tx.is_none_or(|previous| previous > boundary) {
                    if entry.deleted_tx.is_none() {
                        released = released.saturating_add(entry.estimated_bytes());
                    }
                    entry.deleted_tx = Some(boundary);
                    entry.membership.deleted_tx = Some(boundary.max(entry.membership.visible_from));
                }
            }
        }
        released
    }

    /// Whether this partition's membership lives only in durable storage.
    /// Commits then change only its counts and fresh graph tail.
    pub(crate) fn is_durable(&self) -> bool {
        self.durable_memberships.read().is_some()
    }

    fn durable_counts(&self) -> Option<RawVectorMembershipSummary> {
        self.is_durable().then(|| *self.durable_counts.lock())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn try_directory_has_visible_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<bool> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        Ok(self
            .try_visible_directory_entry_by_row(row, snapshot, owner)?
            .is_some())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn try_directory_visible_entry_count(
        &self,
        snapshot: SnapshotId,
        candidates: Option<&roaring::RoaringTreemap>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<usize> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        let Some(source) = self.durable_membership_source() else {
            return Ok(self.directory_visible_entry_count(snapshot, candidates));
        };
        if let Some(ids) = candidates {
            let mut count = 0usize;
            for id in ids.iter() {
                count +=
                    usize::from(self.try_directory_has_visible_row(RowId(id), snapshot, owner)?);
            }
            return Ok(count);
        }
        source.visible_count(snapshot, &mut || true, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn try_visible_directory_entry_by_row(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        if let Some(source) = self.durable_membership_source() {
            return source.visible_by_row(row_id, snapshot, &mut || true, owner);
        }
        Ok(self.visible_directory_entry_by_row(row_id, snapshot))
    }

    fn bounded_visible_directory_entry_by_row<E: From<Error>>(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        before_read: &mut impl FnMut() -> std::result::Result<(), E>,
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<RawVectorDirectoryEntry>, E> {
        self.bounded_visible_directory_entry_by_row_with_copy_admission(
            row_id,
            snapshot,
            before_read,
            owner,
            None,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_visible_directory_entry_by_row_with_copy_admission<E: From<Error>>(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        before_read: &mut impl FnMut() -> std::result::Result<(), E>,
        owner: &mut VectorCopyAdmission,
        admit: Option<VectorCopyAdmissionCallback<'_>>,
    ) -> std::result::Result<Option<RawVectorDirectoryEntry>, E> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        if let Some(source) = self.durable_membership_source() {
            return with_membership_read(before_read, |before_read| match admit {
                Some(admit) => source.visible_by_row_with_copy_admission(
                    row_id,
                    snapshot,
                    before_read,
                    owner,
                    admit,
                ),
                None => source.visible_by_row(row_id, snapshot, before_read, owner),
            });
        }
        Ok(self.visible_directory_entry_by_row(row_id, snapshot))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bind_streamed_membership(
        &self,
        cursor: &dyn RawVectorMembershipCursor,
        bodies: &mut RawVectorBodyReads,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        let Some(receipt) = cursor.selected_receipt() else {
            return Ok(());
        };
        let loader = self
            .raw_loader
            .read()
            .clone()
            .ok_or_else(|| Error::Other("durable membership has no raw-body loader".into()))?;
        if let Some(scan) = bodies.scan_for_membership(loader.as_ref(), Some(receipt), owner)? {
            loader.bind_streamed_membership(scan, receipt)?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_vector_for_row_lsn(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        lsn: Lsn,
        row_created_tx: TxId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<Vec<f32>>> {
        let mut selection_copy = owner.local_scratch();
        owner.reserve(self.copy_bytes(index, 1))?;
        let Some(source) = self.durable_membership_source() else {
            if !self.directory_has_visible_row(row_id, snapshot) {
                return Ok(None);
            }
            if let Some(partition) = self.historical_partition(owner)? {
                return Ok(partition
                    .loaded
                    .entries
                    .iter()
                    .rev()
                    .find(|carrier| {
                        carrier.entry.row_id == row_id
                            && carrier.membership.row_lsn == lsn
                            && carrier.membership.owning_row_tx == row_created_tx
                            && carrier.membership.visible_from.0 <= snapshot.0
                            && carrier
                                .membership
                                .deleted_tx
                                .is_none_or(|tx| tx.0 > snapshot.0)
                    })
                    .map(|carrier| {
                        crate::quantized::stored_vector_value(
                            &carrier.entry.vector,
                            self.quantization,
                        )
                    }));
            }
            self.ensure_raw_vectors_loaded(owner)?;
            return Ok(self.vector_for_row_lsn(row_id, lsn, row_created_tx, snapshot));
        };
        // Durable caller projections use admitted point reads; an in-memory
        // partition may use its already-resident bodies.
        if self.try_cache_durable_bodies(owner)? {
            return Ok(self.vector_for_row_lsn(row_id, lsn, row_created_tx, snapshot));
        }
        let selected = if source.supports_root_selection() {
            source.select_visible_by_row(
                row_id,
                snapshot,
                &mut || true,
                selection_copy.as_mut().unwrap_or(owner),
            )?
        } else {
            None
        };
        let identity = if source.supports_root_selection() {
            selected.as_ref().map(|selected| selected.identity())
        } else {
            source.visible_by_row(
                row_id,
                snapshot,
                &mut || true,
                selection_copy.as_mut().unwrap_or(owner),
            )?
        };
        let Some(identity) = identity else {
            return Ok(None);
        };
        if identity.row_id != row_id {
            return Err(Error::Other(
                "durable vector membership returned another row version".into(),
            ));
        }
        if identity.membership.owning_row_tx != row_created_tx
            || identity.membership.row_lsn != lsn
            || identity.membership.visible_from.0 > snapshot.0
            || identity
                .membership
                .deleted_tx
                .is_some_and(|deleted| deleted.0 <= snapshot.0)
        {
            return Ok(None);
        }
        let loader = self.raw_loader.read().clone().ok_or_else(|| {
            Error::Other("durable vector membership has no raw-body loader".into())
        })?;
        let mut scratch = owner.scratch();
        let mut refusal = None;
        let mut request = |request| match request {
            DormantVectorLoadRequest::Reserve(bytes) => match scratch.reserve(bytes) {
                Ok(()) => true,
                Err(error) => {
                    refusal = Some(error);
                    false
                }
            },
            _ => true,
        };
        let loaded = match selected.as_deref() {
            Some(selected) => loader.load_selected_candidate(selected, true, &mut request),
            None => loader.load_candidate(&identity, true, &mut request),
        };
        if let Some(error) = refusal {
            return Err(error);
        }
        let mut loaded = loaded?;
        let entry = loaded.entry();
        if entry.index != *index
            || entry.row_id != identity.row_id
            || entry.created_tx != identity.created_tx
            || entry.deleted_tx != identity.deleted_tx
            || entry.lsn != identity.lsn
            || entry.vector.len() != self.dimension
        {
            return Err(Error::Other(
                "raw vector body disagrees with durable membership".into(),
            ));
        }
        // Move the loader-admitted decode into the caller-owned projection;
        // never copy a body or hydrate the owning partition.
        Ok(Some(std::mem::take(&mut loaded.entry.vector)))
    }

    /// `cache` lets a caller-facing read fault in a fitting durable
    /// partition; commit and maintenance reads only use an existing cache.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_visible_entry_by_row_caching(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        cache: bool,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorEntry>> {
        self.load_visible_entry_by_row_selected(index, row_id, snapshot, cache, None, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn load_visible_entry_by_row_selected(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        cache: bool,
        supplied: Option<&dyn RawVectorMembershipSelection>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorEntry>> {
        let mut selection_copy = owner.local_scratch();
        owner.reserve(self.copy_bytes(index, 1))?;
        if let Some(source) = self.durable_membership_source() {
            let cached = if owner.historical_durable_reads() {
                false
            } else if cache {
                self.try_cache_durable_bodies(owner)?
            } else {
                self.bodies_resident()
            };
            if cached {
                let _gate = self.raw_residency_gate.read();
                if self.bodies_resident() {
                    return Ok(self.visible_entry_by_row(index, row_id, snapshot));
                }
            }
            let selected_owned = if supplied.is_none() && source.supports_root_selection() {
                source.select_visible_by_row(
                    row_id,
                    snapshot,
                    &mut || true,
                    selection_copy.as_mut().unwrap_or(owner),
                )?
            } else {
                None
            };
            let selected = supplied.or(selected_owned.as_deref());
            let identity = if source.supports_root_selection() {
                selected.map(|selected| selected.identity())
            } else {
                source.visible_by_row(
                    row_id,
                    snapshot,
                    &mut || true,
                    selection_copy.as_mut().unwrap_or(owner),
                )?
            };
            let Some(identity) = identity else {
                return Ok(None);
            };
            let loader = self.raw_loader.read().clone().ok_or_else(|| {
                Error::Other("durable vector membership has no raw-body loader".into())
            })?;
            let mut scratch = owner.scratch();
            let mut refusal = None;
            let mut request = |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => match scratch.reserve(bytes) {
                    Ok(()) => true,
                    Err(error) => {
                        refusal = Some(error);
                        false
                    }
                },
                _ => true,
            };
            let loaded = match selected {
                Some(selected) => loader.load_selected_candidate(selected, true, &mut request),
                None => loader.load_candidate(&identity, true, &mut request),
            };
            if let Some(error) = refusal {
                return Err(error);
            }
            let mut loaded = loaded?;
            let entry = loaded.entry();
            if entry.index != *index
                || entry.row_id != identity.row_id
                || entry.created_tx != identity.created_tx
                || entry.deleted_tx != identity.deleted_tx
                || entry.lsn != identity.lsn
                || entry.vector.len() != self.dimension
            {
                return Err(Error::Other(
                    "raw vector body disagrees with durable membership".into(),
                ));
            }
            // The loader funds the complete decode/copy window. The returned
            // owned entry follows the existing caller-owned projection contract.
            let output_index = entry.index.clone();
            let output = VectorEntry {
                index: output_index,
                row_id: entry.row_id,
                created_tx: entry.created_tx,
                deleted_tx: entry.deleted_tx,
                lsn: entry.lsn,
                vector: std::mem::take(&mut loaded.entry.vector),
            };
            return Ok(Some(output));
        }
        if let Some(partition) = self.historical_partition(owner)? {
            return Ok(partition
                .loaded
                .entries
                .iter()
                .rev()
                .find(|carrier| {
                    carrier.entry.row_id == row_id
                        && carrier.membership.visible_from.0 <= snapshot.0
                        && carrier
                            .membership
                            .deleted_tx
                            .is_none_or(|tx| tx.0 > snapshot.0)
                })
                .map(|carrier| carrier.entry.clone()));
        }
        self.ensure_raw_vectors_loaded(owner)?;
        Ok(self.visible_entry_by_row(index, row_id, snapshot))
    }

    /// Legacy full-load readers retain an operation-owned authenticated copy.
    /// Their original load refusal remains observable; the shared current
    /// residency and its owning accountant are never changed by this copy.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn historical_partition(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<HistoricalRawVectorPartition>> {
        if !owner.historical_durable_reads() || self.is_durable() {
            return Ok(None);
        }
        let Some(loader) = self.raw_loader.read().clone() else {
            return Ok(None);
        };
        let mut directory_owner = owner.scratch();
        let expected = {
            let directory = self.raw_directory.read();
            directory_owner.reserve_for_static(
                directory
                    .len()
                    .checked_mul(std::mem::size_of::<RawVectorDirectoryEntry>())
                    .ok_or_else(|| {
                        Error::Other("raw hydration directory capacity overflow".into())
                    })?,
                "vector_copy",
                "load_raw_vector_directory",
                "Raise the executing operation memory limit to hold the hydration directory.",
            )?;
            directory.iter().cloned().collect::<Vec<_>>()
        };
        let mut loaded = loader.load(&expected, owner)?;
        if loaded.entries.len() != expected.len()
            || loaded
                .entries
                .iter()
                .zip(&expected)
                .any(|(entry, identity)| {
                    entry.entry.row_id != identity.row_id
                        || entry.entry.created_tx != identity.created_tx
                        || entry.entry.deleted_tx != identity.deleted_tx
                        || entry.entry.lsn != identity.lsn
                        || entry.membership != identity.membership
                })
        {
            return Err(Error::Other(
                "loaded raw vector partition disagrees with its durable directory".into(),
            ));
        }
        drop(expected);
        drop(directory_owner);
        loaded.workspace_owner.reserve_for_static(
            loaded
                .entries
                .len()
                .checked_mul(crate::quantized::shared_payload_header_bytes())
                .ok_or_else(|| Error::Other("raw vector header capacity overflow".into()))?,
            "vector_index",
            "load_raw_vector_headers",
            "Raise MEMORY_LIMIT to hold the shared raw-vector allocation headers.",
        )?;
        Ok(Some(HistoricalRawVectorPartition { loaded }))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn historical_partition_score(
        &self,
        partition: &HistoricalRawVectorPartition,
        position: usize,
        snapshot: SnapshotId,
        query: &[f32],
    ) -> Result<Option<(RowId, f32)>> {
        let carrier = partition.loaded.entries.get(position).ok_or_else(|| {
            Error::Other("historical vector position exceeds its admitted copy".into())
        })?;
        if carrier.membership.visible_from.0 > snapshot.0
            || carrier
                .membership
                .deleted_tx
                .is_some_and(|tx| tx.0 <= snapshot.0)
        {
            return Ok(None);
        }
        // The retained full-load reservation already owns the complete stored
        // representation; one temporary representation uses that headroom.
        let stored = self.stored_entry_ref(&carrier.entry);
        #[cfg(feature = "test-seams")]
        crate::mem::observe_native_vector_score_for_test();
        Ok(Some((
            stored.row_id,
            stored.vector.cosine_similarity(query),
        )))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn ensure_raw_vectors_loaded(&self, owner: &mut VectorCopyAdmission) -> Result<()> {
        self.ensure_raw_vectors_loaded_with_copy_preference(false, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn ensure_raw_vectors_loaded_with_copy_preference(
        &self,
        prefer_unique_owned: bool,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        if self.raw_resident.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _gate = self.raw_residency_gate.write();
        if self.raw_resident.load(Ordering::SeqCst) {
            return Ok(());
        }
        let loader = self.raw_loader.read().clone().ok_or_else(|| {
            Error::Other(("dormant raw vector partition has no loader".to_string()).into())
        })?;
        // A durable partition is loaded in its durable key order, admitted
        // from its count before any body is read; the cached bodies are its
        // only resident copy and its membership directory stays durable.
        let durable = self.is_durable();
        let mut directory_owner = owner.scratch();
        let expected = if durable {
            Vec::new()
        } else {
            let directory = self.raw_directory.read();
            let bytes = directory
                .len()
                .checked_mul(std::mem::size_of::<RawVectorDirectoryEntry>())
                .ok_or_else(|| Error::Other("raw hydration directory capacity overflow".into()))?;
            directory_owner.reserve_for_static(
                bytes,
                "vector_copy",
                "load_raw_vector_directory",
                "Raise the executing operation memory limit to hold the hydration directory.",
            )?;
            let mut expected = Vec::with_capacity(directory.len());
            expected.extend(directory.iter().cloned());
            expected
        };
        let loaded = if durable {
            loader.load_all(owner)?
        } else {
            loader.load(&expected, owner)?
        };
        let (entries, mut resident_reservation, workspace_owner) = loaded.into_parts();
        let reserved_resident_bytes = resident_reservation.bytes;
        let accountant = resident_reservation.accountant.clone();
        if !durable && entries.len() != expected.len() {
            drop(entries);
            return Err(Error::Other(
                ("loaded raw vector partition does not match its directory count".to_string())
                    .into(),
            ));
        }
        if !durable
            && entries.iter().zip(&expected).any(|(entry, identity)| {
                entry.entry.row_id != identity.row_id
                    || entry.entry.created_tx != identity.created_tx
                    || entry.entry.deleted_tx != identity.deleted_tx
                    || entry.entry.lsn != identity.lsn
                    || entry.membership != identity.membership
            })
        {
            drop(entries);
            return Err(Error::Other(
                ("loaded raw vector partition disagrees with its durable directory".to_string())
                    .into(),
            ));
        }
        // Loader callbacks run without the directory lock. Free the copied
        // identities before their executing scratch owner returns the credit.
        drop(expected);
        drop(directory_owner);
        let header_bytes = entries
            .len()
            .saturating_mul(crate::quantized::shared_payload_header_bytes());
        if let Err(error) = accountant.try_allocate_for_static(
            header_bytes,
            "vector_index",
            "load_raw_vector_headers",
            "Raise MEMORY_LIMIT to hold the shared raw-vector allocation headers.",
        ) {
            drop(entries);
            return Err(error);
        }
        let mut header_reservation =
            PaidRetainedMemoryCharge::new(header_bytes, accountant.clone());
        let mut vectors = PartitionVectors::default();
        let mut resident_bytes = 0usize;
        for carrier in entries {
            let mut stored = self.stored_entry(carrier.entry);
            stored.membership = carrier.membership;
            stored.canonical_deleted_tx = carrier.canonical_deleted_tx;
            resident_bytes = resident_bytes.saturating_add(stored.estimated_bytes());
            stored.vector.promote_to_shared();
            if let Some(positions) = vectors.by_row.get(&stored.row_id)
                && let Some(prior) = positions
                    .iter()
                    .rev()
                    .map(|position| &vectors.entries[*position])
                    .find(|prior| prior.vector.representation_eq(&stored.vector))
            {
                stored.vector = prior.vector.clone();
            }
            vectors.push(stored);
        }
        // The consuming iterator has freed the decoded carriers and their
        // container. Only the writer-funded stored representation remains.
        drop(workspace_owner);
        if resident_bytes != reserved_resident_bytes {
            drop(vectors);
            return Err(Error::Other((format!(
                "loaded raw vector charge mismatch: directory owns {} bytes but decoded vectors require {resident_bytes}",
                reserved_resident_bytes
            )).into()));
        }
        if prefer_unique_owned {
            // Detached copies retain a newly hydrated singleton inline. The
            // common load still admits every temporary header before promotion;
            // only a successful unwrap frees one before its surplus is released.
            for entry in &mut vectors.entries {
                entry.vector.move_unique_uncharged_to_owned();
            }
            vectors.rebuild_row_positions();
        }
        let unique_bytes = vectors.unique_payload_bytes();
        header_reservation.bytes -=
            vectors.payload_memberships.len() * crate::quantized::shared_payload_header_bytes();
        *self.vectors.write() = vectors;
        accountant.release(reserved_resident_bytes - unique_bytes);
        resident_reservation.bytes = 0;
        let counter = self.raw_payload_counter.read().clone();
        let header_counter = self.raw_payload_header_counter.read().clone();
        self.attach_raw_accounting(accountant, counter, header_counter);
        self.raw_resident_bytes
            .store(unique_bytes, Ordering::SeqCst);
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

    fn attach_raw_accounting(
        &self,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        counter: Arc<AtomicUsize>,
        header_counter: Arc<AtomicUsize>,
    ) {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        let mut owner = self.raw_accountant.write();
        if owner.is_some()
            && self.raw_resident.load(Ordering::SeqCst)
            && Arc::ptr_eq(&*self.raw_payload_counter.read(), &counter)
            && Arc::ptr_eq(&*self.raw_payload_header_counter.read(), &header_counter)
        {
            *owner = Some(accountant);
            return;
        }
        *self.raw_payload_counter.write() = counter.clone();
        *self.raw_payload_header_counter.write() = header_counter.clone();
        for entry in &self.vectors.read().entries {
            entry.vector.adopt_raw_charge(
                accountant.clone(),
                counter.clone(),
                header_counter.clone(),
            );
        }
        // Existing payload tokens retain their original funders. Future
        // legacy/prepared publication adopts this current caller's owner.
        *owner = Some(accountant);
    }

    /// Release a raw cache reservation owned by this state before the state
    /// is removed from the registry. `false` means the bodies, if any, were
    /// admitted by the engine's older per-insert path and the caller must
    /// settle those entries instead.
    fn release_accounted_raw_residency_for_removal(&self) -> bool {
        let _gate = self.raw_residency_gate.write();
        let Some(_accountant) = self.raw_accountant.write().take() else {
            return false;
        };
        self.raw_resident_bytes.store(0, Ordering::SeqCst);
        let mut vectors = self.vectors.write();
        vectors.entries.clear();
        vectors.by_row.clear();
        vectors.payload_memberships.clear();
        vectors.owned_payload_bytes = 0;
        drop(vectors);
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
        if self.raw_resident.load(Ordering::SeqCst) {
            self.raw_resident_bytes.load(Ordering::SeqCst)
        } else {
            0
        }
    }

    fn directory_byte_count(&self) -> usize {
        let directory_len = self.raw_directory.read().len();
        let directory_bytes = directory_len.saturating_mul(
            std::mem::size_of::<RawVectorDirectoryEntry>()
                + std::mem::size_of::<StoredVectorEntry>()
                + std::mem::size_of::<(usize, usize)>(),
        );
        let rows = self.raw_directory_by_row.read().len();
        // Every directory position is listed under exactly one row identity
        // (`push_entry`, `rebuild_raw_directory_positions`), so the position
        // lists hold exactly `directory_len` slots. Reading the length keeps
        // a per-insert refresh constant instead of a walk of the partition.
        directory_bytes
            .saturating_add(self.visibility_counts.read().retained_bytes())
            .saturating_add(rows.saturating_mul(std::mem::size_of::<(RowId, Vec<usize>)>()))
            .saturating_add(directory_len.saturating_mul(std::mem::size_of::<usize>()))
    }

    fn rebuild_raw_directory_positions(&self) {
        let directory = self.raw_directory.read();
        *self.pending_marks.lock() = ResidentPendingMarks::of(&directory);
        let mut by_row = HashMap::<RowId, Vec<usize>>::new();
        for (position, entry) in directory.iter().enumerate() {
            by_row.entry(entry.row_id).or_default().push(position);
        }
        for positions in by_row.values_mut() {
            positions.sort_by_key(|position| directory[*position].membership.visible_from);
        }
        let mut counts = VectorVisibilityCounts {
            intervals: Vec::new(),
            created: directory
                .iter()
                .map(|entry| entry.membership.visible_from)
                .collect(),
            deleted: Vec::with_capacity(directory.len()),
        };
        counts.deleted.extend(
            directory
                .iter()
                .filter_map(|entry| entry.membership.deleted_tx),
        );
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
        let end = positions.partition_point(|position| {
            directory[*position].membership.visible_from.0 <= snapshot.0
        });
        // Raw-store callers can publish IDs out of order; Database commits
        // restamp them first. A deleted directory head must not conceal the
        // visible version behind it.
        positions[..end].iter().rev().find_map(|position| {
            directory
                .get(*position)
                .filter(|entry| {
                    entry
                        .membership
                        .deleted_tx
                        .is_none_or(|tx| tx.0 > snapshot.0)
                })
                .cloned()
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_directory_visible_entry_count<E>(
        &self,
        snapshot: SnapshotId,
        candidates: Option<&[u64]>,
        mut before_entry: impl FnMut() -> std::result::Result<(), E>,
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<usize, E>
    where
        E: From<Error>,
    {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        before_entry()?;
        let source = self.durable_membership_source();
        let Some(ids) = candidates else {
            return match source {
                Some(source) => with_membership_read(&mut before_entry, |before_read| {
                    source.visible_count(snapshot, before_read, owner)
                }),
                None => Ok(self.visibility_counts.read().count(snapshot)),
            };
        };
        let mut count = 0;
        for id in ids {
            before_entry()?;
            count += usize::from(
                self.bounded_visible_directory_entry_by_row(
                    RowId(*id),
                    snapshot,
                    &mut before_entry,
                    owner,
                )?
                .is_some(),
            );
        }
        Ok(count)
    }

    pub(crate) fn bounded_visit_visible_ids_charged<E>(
        &self,
        snapshot: SnapshotId,
        mut before_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        mut visit: impl FnMut(RowId) -> std::result::Result<(), E>,
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let mut local_copy_admission = owner.local_scratch();
        let _owner = local_copy_admission.as_mut().unwrap_or(owner);
        if let Some(source) = self.durable_membership_source() {
            let mut reserved = 0usize;
            let mut interrupted = None;
            let opened = source.open(&mut |request| {
                if interrupted.is_some() {
                    return false;
                }
                let result = match request {
                    DormantVectorLoadRequest::Reserve(bytes) => match reserved.checked_add(bytes) {
                        Some(total) => before_retain(bytes).map(|()| {
                            #[cfg(feature = "test-seams")]
                            contextdb_core::family_loads::loaded(
                                contextdb_core::family_loads::Family::VectorMembership,
                                bytes,
                            );
                            reserved = total
                        }),
                        None => Err(E::from(Error::Other(
                            "membership window charge overflow".into(),
                        ))),
                    },
                    _ => before_entry(),
                };
                if let Err(error) = result {
                    interrupted = Some(error);
                    false
                } else {
                    true
                }
            });
            let result = (|| {
                if let Some(error) = interrupted.take() {
                    return Err(error);
                }
                let mut cursor = opened.map_err(E::from)?;
                loop {
                    before_entry()?;
                    let mut interrupted = None;
                    let next = cursor.next(&mut |request| {
                        if interrupted.is_some() {
                            return false;
                        }
                        let result = match request {
                            DormantVectorLoadRequest::Reserve(bytes) => {
                                match reserved.checked_add(bytes) {
                                    Some(total) => before_retain(bytes).map(|()| {
                                        #[cfg(feature = "test-seams")]
                                        contextdb_core::family_loads::loaded(
                                            contextdb_core::family_loads::Family::VectorMembership,
                                            bytes,
                                        );
                                        reserved = total
                                    }),
                                    None => Err(E::from(Error::Other(
                                        "membership window charge overflow".into(),
                                    ))),
                                }
                            }
                            _ => before_entry(),
                        };
                        if let Err(error) = result {
                            interrupted = Some(error);
                            false
                        } else {
                            true
                        }
                    });
                    if let Some(error) = interrupted {
                        return Err(error);
                    }
                    let Some(entry) = next.map_err(E::from)? else {
                        break;
                    };
                    if entry.membership.visible_from.0 <= snapshot.0
                        && entry
                            .membership
                            .deleted_tx
                            .is_none_or(|tx| tx.0 > snapshot.0)
                    {
                        visit(entry.row_id)?;
                    }
                }
                Ok(())
            })();
            release_retained(reserved);
            return result;
        }
        self.bounded_visit_visible_ids(snapshot, before_entry, visit)
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
                    && entry.membership.visible_from.0 <= snapshot.0
                    && entry
                        .membership
                        .deleted_tx
                        .is_none_or(|tx| tx.0 > snapshot.0)
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
        self.raw_resident_bytes.store(0, Ordering::SeqCst);
        self.raw_accountant.write().take();
        self.raw_resident.store(false, Ordering::SeqCst);
        #[cfg(any(test, feature = "test-seams"))]
        self.note_passive_activity(|activity| {
            activity
                .raw_partition_evictions
                .fetch_add(1, Ordering::SeqCst);
        });
        true
    }

    pub(crate) fn supports_bounded_raw_point_load(&self) -> bool {
        !self.raw_resident.load(Ordering::SeqCst)
            && self
                .raw_loader
                .read()
                .as_ref()
                .is_some_and(|loader| loader.supports_bounded_candidate_load())
    }

    pub(crate) fn exact_comparison_streams_when_partition_does_not_fit(&self) -> bool {
        self.raw_loader
            .read()
            .as_ref()
            .is_some_and(|loader| loader.exact_comparison_streams_when_partition_does_not_fit())
    }

    pub fn vector_count(&self) -> usize {
        if let Some(counts) = self.durable_counts() {
            return counts.live_entries;
        }
        self.visibility_counts.read().count(SnapshotId(u64::MAX))
    }

    pub fn max_tx(&self) -> TxId {
        if let Some(counts) = self.durable_counts() {
            return counts.max_tx;
        }
        self.visibility_counts.read().max_tx()
    }

    pub fn byte_count(&self) -> usize {
        let vectors = self.vectors.read();
        let raw_bytes = self.raw_payload_bytes();
        raw_bytes
            .saturating_add(
                vectors
                    .payload_memberships
                    .len()
                    .saturating_mul(crate::quantized::shared_payload_header_bytes()),
            )
            .saturating_add(
                vectors
                    .entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StoredVectorEntry>()),
            )
            .saturating_add(vectors.retained_map_bytes())
            .saturating_add(self.directory_byte_count())
    }

    fn live_payload_bytes(&self) -> usize {
        self.vector_count()
            .saturating_mul(crate::quantized::stored_vector_resident_bytes(
                self.dimension,
                self.quantization,
            ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn all_entries(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorEntry>> {
        let vectors = self.vectors.read();
        owner.reserve(self.copy_bytes(index, vectors.entries.len()))?;
        Ok(vectors
            .entries
            .iter()
            .map(|entry| entry.to_vector_entry(index.clone()))
            .collect())
    }

    fn copy_bytes(&self, index: &VectorIndexRef, count: usize) -> usize {
        self.copy_payload_bytes(index, count).saturating_mul(4)
    }

    fn generation_copy_bytes(&self, index: &VectorIndexRef, count: usize) -> usize {
        self.copy_payload_bytes(index, count)
            .saturating_add(count.saturating_mul(128))
    }

    fn copy_payload_bytes(&self, index: &VectorIndexRef, count: usize) -> usize {
        count.saturating_mul(
            std::mem::size_of::<VectorEntry>()
                .saturating_add(self.dimension.saturating_mul(std::mem::size_of::<f32>()))
                .saturating_add(index.table.len())
                .saturating_add(index.column.len()),
        )
    }

    /// Admit the complete output before copying the first body. The cursor
    /// point-loads durable bodies and closes its decode window at exhaustion.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn owned_entries<'a>(
        &'a self,
        index: &'a VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<OwnedVectorEntries<'a>> {
        self.owned_entries_with_copy_preference(index, owner, false)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn owned_entries_with_copy_preference<'a>(
        &'a self,
        index: &'a VectorIndexRef,
        owner: &mut VectorCopyAdmission,
        prefer_unique_owned: bool,
    ) -> Result<OwnedVectorEntries<'a>> {
        let count = self.entry_count();
        let per_entry = std::mem::size_of::<VectorEntry>()
            .saturating_add(self.dimension.saturating_mul(std::mem::size_of::<f32>()))
            .saturating_add(index.table.len())
            .saturating_add(index.column.len());
        owner.reserve(count.saturating_mul(per_entry).saturating_mul(4))?;
        let mut scratch = owner.scratch();
        let membership = if let Some(source) = self.durable_membership_source() {
            let mut refusal = None;
            let opened = source.open(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => match scratch.reserve(bytes) {
                    Ok(()) => {
                        #[cfg(feature = "test-seams")]
                        contextdb_core::family_loads::loaded(
                            contextdb_core::family_loads::Family::VectorMembership,
                            bytes,
                        );
                        true
                    }
                    Err(error) => {
                        refusal = Some(error);
                        false
                    }
                },
                _ => true,
            });
            if let Some(error) = refusal {
                return Err(error);
            }
            Some(opened?)
        } else {
            None
        };
        let loader = if membership.is_some() {
            Some(self.raw_loader.read().clone().ok_or_else(|| {
                Error::Other("durable vector partition lost its raw-body loader".into())
            })?)
        } else {
            self.ensure_raw_vectors_loaded_with_copy_preference(prefer_unique_owned, owner)?;
            None
        };
        Ok(OwnedVectorEntries {
            state: self,
            index,
            membership,
            loader,
            bodies: RawVectorBodyReads::default(),
            scratch: Some(scratch),
            position: 0,
            count,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn find_by_row_id(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorEntry>> {
        let vectors = self.vectors.read();
        owner.reserve(self.copy_bytes(index, 1))?;
        Ok(vectors.by_row.get(&row_id).and_then(|positions| {
            positions.iter().rev().find_map(|position| {
                vectors
                    .entries
                    .get(*position)
                    .map(|entry| entry.to_vector_entry(index.clone()))
            })
        }))
    }

    fn stored_by_row_id(&self, row_id: RowId) -> Option<StoredVectorEntry> {
        self.with_stored_by_row_id(row_id, Clone::clone)
    }

    fn with_stored_by_row_id<R>(
        &self,
        row_id: RowId,
        f: impl FnOnce(&StoredVectorEntry) -> R,
    ) -> Option<R> {
        let vectors = self.vectors.read();
        let entry = vectors.by_row.get(&row_id).and_then(|positions| {
            positions
                .iter()
                .rev()
                .find_map(|position| vectors.entries.get(*position))
        });
        entry.map(f)
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
        streamed_identity: Option<RawVectorDirectoryEntry>,
        snapshot: SnapshotId,
        query: &[f32],
        caller_charged: bool,
        bodies: Option<&mut RawVectorBodyReads>,
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        owner: &mut VectorCopyAdmission,
        admit: Option<VectorCopyAdmissionCallback<'_>>,
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
    {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        before_checkpoint()?;
        if streamed_identity.is_none()
            && !owner.historical_durable_reads()
            && let Some(score) = self
                .with_visible_stored_entry_by_row(row_id, snapshot, |entry| {
                    before_distance().map(|()| {
                        #[cfg(feature = "test-seams")]
                        crate::mem::observe_native_vector_score_for_test();
                        let score = entry.vector.cosine_similarity(query);
                        #[cfg(feature = "test-seams")]
                        crate::mem::observe_native_score_identity_for_test(
                            entry.row_id,
                            entry.created_tx,
                            entry.lsn,
                            snapshot,
                            score,
                            None,
                        );
                        score
                    })
                })
                .transpose()?
        {
            return Ok(Some(score));
        }

        let membership_streamed = streamed_identity.is_some();
        let identity = match streamed_identity {
            Some(identity) => identity,
            None => {
                let Some(identity) = self
                    .bounded_visible_directory_entry_by_row_with_copy_admission(
                        row_id,
                        snapshot,
                        before_checkpoint,
                        owner,
                        admit,
                    )?
                else {
                    return Ok(None);
                };
                identity
            }
        };
        let loader = self.raw_loader.read().clone().ok_or_else(|| {
            E::from(Error::Other(
                ("dormant raw vector candidate has no loader".to_string()).into(),
            ))
        })?;
        // Membership and body reads are native source steps. Only scoring is
        // logical distance work; page admission bounds these physical reads.
        if caller_charged && bodies.is_some() {
            if !membership_streamed {
                before_checkpoint()?;
            }
            before_checkpoint()?;
        }
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
                                ("raw vector candidate reservation overflow".to_string()).into(),
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
            match bodies {
                Some(bodies) => {
                    let one_pass = bodies.one_pass;
                    match bodies.scan_for(loader.as_ref(), owner) {
                        Ok(Some(scan)) if membership_streamed => loader
                            .load_streamed_candidate_with_scan(
                                scan,
                                &identity,
                                caller_charged,
                                &mut request,
                            ),
                        Ok(Some(scan)) if one_pass => loader.load_candidate_with_scan_once(
                            scan,
                            &identity,
                            caller_charged,
                            &mut request,
                        ),
                        Ok(Some(scan)) => loader.load_candidate_with_scan(
                            scan,
                            &identity,
                            caller_charged,
                            &mut request,
                        ),
                        Ok(None) => loader.load_candidate(&identity, caller_charged, &mut request),
                        Err(error) => Err(error),
                    }
                }
                None => loader.load_candidate(&identity, caller_charged, &mut request),
            }
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
                    ("raw vector candidate charge disagrees with its caller reservation"
                        .to_string())
                    .into(),
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
                    ("loaded raw vector candidate disagrees with its durable directory"
                        .to_string())
                    .into(),
                )));
            }
            let stored = self.stored_entry_ref(entry);
            let body_bytes = stored.estimated_bytes();
            before_distance()?;
            #[cfg(feature = "test-seams")]
            crate::mem::observe_native_vector_score_for_test();
            let score = stored.vector.cosine_similarity(query);
            #[cfg(feature = "test-seams")]
            crate::mem::observe_native_score_identity_for_test(
                entry.row_id,
                entry.created_tx,
                entry.lsn,
                snapshot,
                score,
                Some((
                    entry.vector.as_ptr() as usize,
                    entry.vector.capacity() * std::mem::size_of::<f32>(),
                    loaded.transient_bytes(),
                )),
            );
            Ok((score, body_bytes))
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn score_visible_candidate(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<f32>> {
        let mut scratch = owner.scratch();
        self.score_visible_candidate_inner(
            index,
            row_id,
            None,
            snapshot,
            query,
            true,
            None,
            &mut || Ok::<(), Error>(()),
            &mut || Ok::<(), Error>(()),
            &mut |bytes| scratch.reserve(bytes),
            &mut |_bytes| {},
            owner,
            None,
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
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
    {
        self.score_visible_candidate_inner(
            index,
            row_id,
            None,
            snapshot,
            query,
            true,
            None,
            before_checkpoint,
            before_distance,
            before_retain,
            release_retained,
            owner,
            None,
        )
    }

    /// Score one row's own stored body for an exhaustive comparison, reading
    /// it through the statement's open body read when the partition is not
    /// resident. The body's bytes are admitted through `before_retain` before
    /// it is read and released once it is scored.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_score_stored_body<E>(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        query: &[f32],
        bodies: &mut RawVectorBodyReads,
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
    {
        self.score_visible_candidate_inner(
            index,
            row_id,
            None,
            snapshot,
            query,
            true,
            Some(bodies),
            before_checkpoint,
            before_distance,
            before_retain,
            release_retained,
            owner,
            None,
        )
    }

    /// Score a membership already authenticated by the exhaustive source cursor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_score_streamed_body<E>(
        &self,
        index: &VectorIndexRef,
        identity: RawVectorDirectoryEntry,
        snapshot: SnapshotId,
        query: &[f32],
        bodies: &mut RawVectorBodyReads,
        before_checkpoint: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
    {
        self.score_visible_candidate_inner(
            index,
            identity.row_id,
            Some(identity),
            snapshot,
            query,
            true,
            Some(bodies),
            before_checkpoint,
            before_distance,
            before_retain,
            release_retained,
            owner,
            None,
        )
    }

    /// The row at one directory position if it is visible at `snapshot`;
    /// `None` once the position is past the directory's end. Holds the
    /// directory only for the lookup, so a caller can read the body without
    /// keeping it locked.
    pub(crate) fn visible_directory_row_at(
        &self,
        position: usize,
        snapshot: SnapshotId,
    ) -> Option<Option<RowId>> {
        if self.is_durable() {
            // A durable partition's positions are its cached bodies.
            let vectors = self.vectors.read();
            let entry = vectors.entries.get(position)?;
            return Some(entry.visible_at(snapshot).then_some(entry.row_id));
        }
        let directory = self.raw_directory.read();
        let entry = directory.get(position)?;
        Some(
            (entry.membership.visible_from.0 <= snapshot.0
                && entry
                    .membership
                    .deleted_tx
                    .is_none_or(|tx| tx.0 > snapshot.0))
            .then_some(entry.row_id),
        )
    }

    /// Whether this partition's exhaustive comparison can read bodies one at
    /// a time instead of loading the partition's vectors whole: its vectors
    /// are not resident and a loader can read one stored body by its
    /// identity. Every durable loader can -- a committed image through its
    /// authenticated body scan, a writable store through a point read of the
    /// row's own membership.
    pub(crate) fn streams_raw_bodies(&self) -> bool {
        self.supports_bounded_raw_point_load()
    }

    fn vector_for_row_lsn(
        &self,
        row_id: RowId,
        lsn: Lsn,
        row_created_tx: TxId,
        snapshot: SnapshotId,
    ) -> Option<Vec<f32>> {
        let vectors = self.vectors.read();
        vectors.by_row.get(&row_id).and_then(|positions| {
            positions.iter().find_map(|position| {
                vectors
                    .entries
                    .get(*position)
                    .filter(|entry| {
                        entry.membership.row_lsn == lsn
                            && entry.membership.owning_row_tx == row_created_tx
                            && entry.membership.visible_from.0 <= snapshot.0
                            && entry
                                .membership
                                .deleted_tx
                                .is_none_or(|deleted| deleted.0 > snapshot.0)
                    })
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
        if let Some(counts) = self.durable_counts() {
            return counts.entries;
        }
        self.raw_directory.read().len()
    }

    fn stored_entry(&self, entry: VectorEntry) -> StoredVectorEntry {
        StoredVectorEntry::from_vector_entry(entry, self.quantization)
    }

    fn stored_entry_ref(&self, entry: &VectorEntry) -> StoredVectorEntry {
        StoredVectorEntry::from_vector_entry_ref(entry, self.quantization)
    }

    fn push_entry(&self, entry: StoredVectorEntry) {
        let _reclassify = ReclassifyOnReturn(self);
        debug_assert!(self.raw_resident.load(Ordering::SeqCst));
        let mut directory = self.raw_directory.write();
        let position = directory.len();
        let identity = Self::directory_entry(&entry);
        {
            let mut counts = self.visibility_counts.write();
            counts.add(&identity);
            counts.set_interval(position, &identity);
        }
        self.pending_marks.lock().note(&identity);
        directory.push(identity);
        let mut by_row = self.raw_directory_by_row.write();
        let positions = by_row.entry(entry.row_id).or_default();
        let at = positions.partition_point(|position| {
            directory[*position].membership.visible_from <= entry.membership.visible_from
        });
        positions.insert(at, position);
        drop(by_row);
        drop(directory);
        if let Some(accountant) = self.raw_accountant.read().clone() {
            entry.vector.adopt_raw_charge(
                accountant,
                self.raw_payload_counter.read().clone(),
                self.raw_payload_header_counter.read().clone(),
            );
        }
        let mut vectors = self.vectors.write();
        if entry
            .vector
            .payload_identity()
            .is_none_or(|identity| !vectors.payload_memberships.contains_key(&identity))
        {
            self.raw_resident_bytes
                .fetch_add(entry.estimated_bytes(), Ordering::SeqCst);
        }
        // A durable loader resolves the partition's current checksummed
        // membership at load time, so it remains valid after this already-
        // durable publication and makes the updated partition evictable.
        vectors.push(entry);
    }

    fn push_entry_with(&self, entry: StoredVectorEntry, after: impl FnOnce(&StoredVectorEntry)) {
        let _residency = self.raw_residency_gate.read();
        let row_id = entry.row_id;
        self.push_entry(entry);
        let _ = self.with_stored_by_row_id(row_id, after);
    }

    fn tombstone_row(&self, row_id: RowId, deleted_tx: TxId) -> usize {
        let _reclassify = ReclassifyOnReturn(self);
        debug_assert!(self.raw_resident.load(Ordering::SeqCst));
        let mut released = 0usize;
        let mut vectors = self.vectors.write();
        let PartitionVectors {
            entries, by_row, ..
        } = &mut *vectors;
        for &position in by_row.get(&row_id).into_iter().flatten() {
            if let Some(entry) = entries.get_mut(position) {
                // A raw publication with an earlier ID closes every prior
                // interval at that boundary, including already retired ones.
                // Never create an interval whose end precedes its start.
                let boundary = deleted_tx.max(entry.created_tx);
                if entry.deleted_tx.is_none_or(|previous| previous > boundary) {
                    if entry.deleted_tx.is_none() {
                        released = released.saturating_add(entry.estimated_bytes());
                    }
                    entry.deleted_tx = Some(boundary);
                    entry.membership.deleted_tx = Some(boundary.max(entry.membership.visible_from));
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
            entry.deleted_tx = Some(boundary);
            entry.membership.deleted_tx = Some(boundary.max(entry.membership.visible_from));
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
        // The walk above already visited this partition's directory; one more
        // pass over the same entries keeps the pending marks exact.
        *self.pending_marks.lock() = ResidentPendingMarks::of(&self.raw_directory.read());
        released
    }

    fn clear_entries(&self) {
        let _reclassify = ReclassifyOnReturn(self);
        let _gate = self.raw_residency_gate.write();
        let mut vectors = self.vectors.write();
        vectors.entries.clear();
        vectors.by_row.clear();
        vectors.payload_memberships.clear();
        vectors.owned_payload_bytes = 0;
        self.raw_directory.write().clear();
        self.raw_directory_by_row.write().clear();
        *self.pending_marks.lock() = ResidentPendingMarks::default();
        *self.visibility_counts.write() = VectorVisibilityCounts::default();
        self.raw_loader.write().take();
        self.durable_memberships.write().take();
        *self.durable_counts.lock() = RawVectorMembershipSummary::default();
        self.raw_resident_bytes.store(0, Ordering::SeqCst);
        self.raw_accountant.write().take();
        self.raw_resident.store(true, Ordering::SeqCst);
    }

    /// Whether this partition's durable directory names any version of one of
    /// `row_ids`. A directory-only answer: nothing is faulted in.
    fn directory_names_any_row<'a>(&self, row_ids: impl IntoIterator<Item = &'a RowId>) -> bool {
        // A durable partition keeps no resident directory to prune.
        if self.is_durable() {
            return false;
        }
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
        let _reclassify = ReclassifyOnReturn(self);
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

    /// Physically drop the entries `keep` rejects. Accounted immutable bodies
    /// return their reservation only when the final shared holder drops, so
    /// the caller receives `0`. A state without an accountant instead hands back
    /// only the bytes the caller's per-insert admission still holds: a body
    /// that was tombstoned had that admission returned at the commit that
    /// superseded it, so only a still-current body is reported.
    ///
    /// Precondition: the bodies are resident. `prune_directory_versions` is
    /// the one caller and reaches here only on that branch; a dormant
    /// partition takes its directory-only path instead.
    fn retain_entries(&self, mut keep: impl FnMut(&StoredVectorEntry) -> bool) -> usize {
        let _reclassify = ReclassifyOnReturn(self);
        debug_assert!(self.raw_resident.load(Ordering::SeqCst));
        let state_owned = self.raw_accountant.read().is_some();
        let mut vectors = self.vectors.write();
        let mut released = 0usize;
        vectors.entries.retain(|entry| {
            if keep(entry) {
                true
            } else {
                if !state_owned && entry.deleted_tx.is_none() {
                    released = released.saturating_add(entry.estimated_bytes());
                }
                false
            }
        });
        vectors.rebuild_row_positions();
        *self.raw_directory.write() = vectors.entries.iter().map(Self::directory_entry).collect();
        self.rebuild_raw_directory_positions();
        self.raw_resident_bytes
            .store(vectors.unique_payload_bytes(), Ordering::SeqCst);
        drop(vectors);
        released
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn storage_bytes_per_entry(&self, owner: &mut VectorCopyAdmission) -> Result<Vec<usize>> {
        let vectors = self.vectors.read();
        let bytes = vectors
            .entries
            .len()
            .checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(|| Error::Other("vector storage-size copy capacity overflow".into()))?;
        owner.reserve(bytes)?;
        let mut sizes = Vec::with_capacity(vectors.entries.len());
        sizes.extend(
            vectors
                .entries
                .iter()
                .map(StoredVectorEntry::estimated_bytes),
        );
        Ok(sizes)
    }
}

/// A policy-only layout change admitted by
/// [`VectorStore::prepare_policy_layout_update`] and not yet published.
/// Dropping it unpublished returns its admitted growth.
pub struct PreparedVectorPolicyLayout {
    index: VectorIndexRef,
    column: Arc<VectorColumnState>,
    layout: VectorIndexLayout,
    growth: Option<PaidRetainedMemoryCharge>,
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
    pub policy_revision: u64,
    pub consolidation_change_percent: Option<u32>,
    pub consolidation_tombstone_percent: Option<u32>,
    pub consolidation_disabled: bool,
}

/// One authoritative policy resolution used by routing and inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedVectorPolicy {
    pub auto_index_at: usize,
    pub policy_revision: u64,
    pub auto_index_at_source: &'static str,
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
            policy_revision: contextdb_core::DEFAULT_VECTOR_POLICY_REVISION,
            consolidation_change_percent: None,
            consolidation_tombstone_percent: None,
            consolidation_disabled: false,
        }
    }

    pub fn with_policy(mut self, auto_index_at: Option<u32>, policy_revision: u64) -> Self {
        self.auto_index_at = auto_index_at;
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

    pub fn effective_auto_index_at(&self) -> usize {
        self.auto_index_at.map_or_else(
            || match self.quantization {
                VectorQuantization::F32 => 1_000,
                VectorQuantization::SQ8 | VectorQuantization::SQ4 => 5_001,
            },
            |value| value as usize,
        )
    }

    /// Resolve the policy that routing and inspection read. AUTO routing
    /// compares `auto_index_at` against the separately aggregated allowed
    /// count at the caller.
    pub fn resolve_policy(&self) -> ResolvedVectorPolicy {
        ResolvedVectorPolicy {
            auto_index_at: self.effective_auto_index_at(),
            policy_revision: self
                .policy_revision
                .max(contextdb_core::DEFAULT_VECTOR_POLICY_REVISION),
            auto_index_at_source: if self.auto_index_at.is_some() {
                "declared"
            } else {
                "compatibility_profile"
            },
        }
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_key_columns.is_empty()
    }

    fn has_same_storage_shape(&self, other: &Self) -> bool {
        self.dimension == other.dimension
            && self.quantization == other.quantization
            && self.partition_key_columns == other.partition_key_columns
    }

    #[doc(hidden)]
    pub fn estimated_bytes(&self) -> usize {
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

/// One vector entry plus the exact typed partition that owns it. `VectorEntry`
/// remains unchanged for wire and compatibility callers.
/// Exact source row ownership and placement interval, retained independently
/// from the canonical vector body's creation transaction and LSN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMembershipProvenance {
    pub owning_row_tx: TxId,
    pub row_lsn: Lsn,
    pub visible_from: TxId,
    pub deleted_tx: Option<TxId>,
}

impl VectorMembershipProvenance {
    /// Ordinary insertion of a row and its new vector in one operation.
    pub fn new(
        owning_row_tx: TxId,
        row_lsn: Lsn,
        visible_from: TxId,
        deleted_tx: Option<TxId>,
    ) -> Self {
        Self {
            owning_row_tx,
            row_lsn,
            visible_from,
            deleted_tx,
        }
    }
}

/// Whole-resident export. The copied image dies before its executing credit.
#[derive(Debug)]
pub struct OwnedPartitionedVectorEntries {
    entries: Vec<PartitionedVectorEntry>,
    owner: VectorCopyAdmission,
}

impl OwnedPartitionedVectorEntries {
    pub fn entries(&self) -> &[PartitionedVectorEntry] {
        &self.entries
    }
    /// Consume the bridge exactly once; the caller keeps the returned credit
    /// through serialization and destruction of the returned entries.
    pub fn into_parts(self) -> (Vec<PartitionedVectorEntry>, VectorCopyAdmission) {
        (self.entries, self.owner)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedVectorEntry {
    pub membership: VectorMembershipProvenance,
    /// The producer-supplied body stamp, independent of placement visibility.
    pub canonical_deleted_tx: Option<TxId>,
    pub partition_key: VectorPartitionKey,
    pub entry: VectorEntry,
}

impl PartitionedVectorEntry {
    pub fn new(partition_key: VectorPartitionKey, entry: VectorEntry) -> Self {
        // This constructor is for a co-created row/vector operation. Restore,
        // received images and unchanged-body row replacements supply their
        // actual source binding through `with_membership` instead.
        let membership = VectorMembershipProvenance::new(
            entry.created_tx,
            entry.lsn,
            entry.created_tx,
            entry.deleted_tx,
        );
        let canonical_deleted_tx = entry.deleted_tx;
        Self::with_membership(partition_key, entry, membership, canonical_deleted_tx)
    }

    pub fn with_membership(
        partition_key: VectorPartitionKey,
        entry: VectorEntry,
        membership: VectorMembershipProvenance,
        canonical_deleted_tx: Option<TxId>,
    ) -> Self {
        Self {
            canonical_deleted_tx,
            partition_key,
            entry,
            membership,
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
    pub before_created_tx: TxId,
    pub before_lsn: Lsn,
}

impl PartitionedVectorDelete {
    pub fn new(
        index: VectorIndexRef,
        partition_key: VectorPartitionKey,
        row_id: RowId,
        deleted_tx: TxId,
        before_created_tx: TxId,
        before_lsn: Lsn,
    ) -> Self {
        Self {
            partition: VectorPartitionRef::new(index, partition_key),
            row_id,
            deleted_tx,
            before_created_tx,
            before_lsn,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedVectorMove {
    target_membership: VectorMoveMembership,
    replace_row_version: bool,
    pub index: VectorIndexRef,
    pub source_partition_key: VectorPartitionKey,
    pub target_partition_key: VectorPartitionKey,
    pub old_row_id: RowId,
    pub new_row_id: RowId,
    pub tx: TxId,
    pub before_created_tx: TxId,
    pub before_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum VectorMoveMembership {
    Operation,
    Source(VectorMembershipProvenance),
}

impl PartitionedVectorMove {
    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    pub fn new(
        index: VectorIndexRef,
        source_partition_key: VectorPartitionKey,
        target_partition_key: VectorPartitionKey,
        old_row_id: RowId,
        new_row_id: RowId,
        tx: TxId,
        before_created_tx: TxId,
        before_lsn: Lsn,
    ) -> Self {
        Self {
            before_created_tx,
            before_lsn,
            target_membership: VectorMoveMembership::Operation,
            replace_row_version: false,
            index,
            source_partition_key,
            target_partition_key,
            old_row_id,
            new_row_id,
            tx,
        }
    }
    /// Restamp the ordinary destination body while installing the exact target
    /// row binding and placement interval supplied by the engine.
    pub fn with_target_membership(mut self, membership: VectorMembershipProvenance) -> Self {
        self.target_membership = VectorMoveMembership::Source(membership);
        self
    }

    fn stamp_target(&self, entry: &mut StoredVectorEntry, lsn: Lsn) {
        entry.created_tx = self.tx;
        entry.lsn = lsn;
        entry.canonical_deleted_tx = None;
        entry.deleted_tx = None;
        entry.membership = match self.target_membership {
            VectorMoveMembership::Operation => {
                VectorMembershipProvenance::new(self.tx, lsn, self.tx, None)
            }
            VectorMoveMembership::Source(membership) => membership,
        };
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
#[derive(Debug)]
pub struct PreparedPartitionedVectorBatch {
    deletes: Vec<PartitionedVectorDelete>,
    inserts: Vec<PartitionedVectorEntry>,
    moves: Vec<PartitionedVectorMove>,
    reclaimable_partitions: HashSet<VectorPartitionRef>,
    touched: HashSet<VectorPartitionRef>,
    requested: HashMap<VectorIndexRef, PartitionKeySet>,
    projected_live_or_retained_partitions: HashMap<VectorIndexRef, usize>,
    directory_reservation: PreparedDirectoryReservation,
    /// Post-batch counts of each touched durable-membership partition. Their
    /// memberships are published durably with the rows; publication installs
    /// only these counts.
    durable_counts: Vec<PreparedDurableCounts>,
    canonical_closures: Vec<PreparedCanonicalClosure>,
    // Declared last: copied metadata and input buffers die before their credit.
    partition_states: PreparedPartitionStates,
    preparation_owner: Option<VectorCopyAdmission>,
}

impl PartialEq for PreparedPartitionedVectorBatch {
    fn eq(&self, other: &Self) -> bool {
        self.deletes == other.deletes
            && self.inserts == other.inserts
            && self.moves == other.moves
            && self.reclaimable_partitions == other.reclaimable_partitions
            && self.touched == other.touched
            && self.requested == other.requested
            && self.projected_live_or_retained_partitions
                == other.projected_live_or_retained_partitions
            && self.directory_reservation == other.directory_reservation
            && self.durable_counts == other.durable_counts
    }
}

/// Pins every retained candidate's residency through durable publication. Dormant
/// candidates cannot hydrate an old committed body during that window; resident
/// candidates cannot replace their validated cached metadata. No payload is read.
struct PreparedCanonicalClosure {
    state: Arc<IndexState>,
    row_id: RowId,
    created_tx: TxId,
    lsn: Lsn,
    deleted_tx: TxId,
    matches_cached_identity: bool,
    _residency: Option<parking_lot::ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
}

impl std::fmt::Debug for PreparedCanonicalClosure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedCanonicalClosure")
            .field("row_id", &self.row_id)
            .field("created_tx", &self.created_tx)
            .field("lsn", &self.lsn)
            .finish()
    }
}

impl PreparedCanonicalClosure {
    fn publish(&self) {
        if !self.matches_cached_identity {
            return;
        }
        let mut vectors = self.state.vectors.write();
        let PartitionVectors {
            entries, by_row, ..
        } = &mut *vectors;
        for &position in by_row.get(&self.row_id).into_iter().flatten() {
            let entry = &mut entries[position];
            if entry.created_tx == self.created_tx
                && entry.lsn == self.lsn
                && entry
                    .canonical_deleted_tx
                    .is_none_or(|prior| prior > self.deleted_tx)
            {
                entry.canonical_deleted_tx = Some(self.deleted_tx);
            }
        }
    }
}

struct PreparedPartitionStates {
    states: HashMap<VectorIndexRef, BTreeMap<VectorPartitionKey, Arc<IndexState>>>,
}
impl std::fmt::Debug for PreparedPartitionStates {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedPartitionStates")
            .field("columns", &self.states.len())
            .finish()
    }
}

#[derive(Clone)]
struct PreparedDirectoryReservation {
    charge: Arc<Mutex<Option<PaidRetainedMemoryCharge>>>,
    index_charges: Arc<Mutex<HashMap<VectorIndexRef, PaidRetainedMemoryCharge>>>,
}

struct PreparedDirectoryCharges {
    store: Option<PaidRetainedMemoryCharge>,
    indexes: HashMap<VectorIndexRef, PaidRetainedMemoryCharge>,
}

impl PreparedDirectoryCharges {
    fn owner(&self, index: &VectorIndexRef) -> Option<crate::memory_budget::MemoryBudgetOwner> {
        self.store
            .as_ref()
            .or_else(|| self.indexes.get(index))
            .map(|charge| charge.accountant.clone())
    }

    fn get_mut(&mut self, index: &VectorIndexRef) -> Option<&mut PaidRetainedMemoryCharge> {
        self.store.as_mut().or_else(|| self.indexes.get_mut(index))
    }
}

impl PreparedDirectoryReservation {
    fn empty() -> Self {
        Self {
            charge: Arc::new(Mutex::new(None)),
            index_charges: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn reserve(
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        bytes: usize,
    ) -> Result<Self> {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        accountant.try_allocate_for_static(
            bytes,
            "vector_index",
            "prepare_vector_directory_growth",
            "Reduce partition-key or vector-history growth, or raise MEMORY_LIMIT before committing the row.",
        )?;
        Ok(Self {
            charge: Arc::new(Mutex::new(Some(PaidRetainedMemoryCharge::new(
                bytes, accountant,
            )))),
            index_charges: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn reserve_index(
        &self,
        index: VectorIndexRef,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        bytes: usize,
    ) -> Result<()> {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        let reservation = Self::reserve(accountant, bytes)?;
        self.index_charges.lock().insert(
            index,
            reservation
                .charge
                .lock()
                .take()
                .expect("new reservation owns its charge"),
        );
        Ok(())
    }

    fn take(&self) -> PreparedDirectoryCharges {
        PreparedDirectoryCharges {
            store: self.charge.lock().take(),
            indexes: std::mem::take(&mut *self.index_charges.lock()),
        }
    }
}

impl std::fmt::Debug for PreparedDirectoryReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedDirectoryReservation")
            .field(
                "bytes",
                &self
                    .charge
                    .lock()
                    .as_ref()
                    .map_or(0, |charge| charge.bytes)
                    .saturating_add(
                        self.index_charges
                            .lock()
                            .values()
                            .map(|charge| charge.bytes)
                            .sum::<usize>(),
                    ),
            )
            .finish()
    }
}

impl PartialEq for PreparedDirectoryReservation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.charge, &other.charge)
            && Arc::ptr_eq(&self.index_charges, &other.index_charges)
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
    #[doc(hidden)]
    pub fn reclaimable_partitions(&self) -> impl Iterator<Item = &VectorPartitionRef> {
        self.reclaimable_partitions.iter()
    }
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
    prepared_directory_growth_upper_bound_matching(
        inserts,
        moves,
        reclaimable_partitions,
        requested,
        |_| true,
    )
}

fn prepared_directory_growth_upper_bound_matching(
    inserts: &[PartitionedVectorEntry],
    moves: &[PartitionedVectorMove],
    reclaimable_partitions: &HashSet<VectorPartitionRef>,
    requested: &HashMap<VectorIndexRef, PartitionKeySet>,
    matches: impl Fn(&VectorIndexRef) -> bool,
) -> usize {
    let directory_entry = (6 * std::mem::size_of::<usize>())
        .saturating_add(2 * std::mem::size_of::<VectorPartitionKey>())
        .saturating_add(std::mem::size_of::<RawVectorDirectoryEntry>())
        .saturating_add(std::mem::size_of::<StoredVectorEntry>())
        .saturating_add(std::mem::size_of::<(usize, usize)>())
        .saturating_add(crate::quantized::shared_payload_header_bytes())
        .saturating_add(4 * std::mem::size_of::<TxId>() + 8 * std::mem::size_of::<(u64, u64)>())
        .saturating_add(std::mem::size_of::<(RowId, Vec<usize>)>())
        .saturating_add(std::mem::size_of::<usize>())
        .saturating_add(std::mem::size_of::<(RowId, Arc<VectorPartitionKey>)>())
        .saturating_sub(std::mem::size_of::<VectorPartitionKey>())
        .saturating_add(std::mem::size_of::<(RowId, Vec<Arc<VectorPartitionKey>>)>())
        .saturating_add(std::mem::size_of::<VectorPartitionKey>());
    let mut bytes = inserts
        .iter()
        .filter(|insert| matches(&insert.entry.index))
        .count()
        .saturating_add(
            moves
                .iter()
                .filter(|row_move| matches(&row_move.index))
                .count(),
        )
        .saturating_mul(directory_entry);
    for insert in inserts.iter().filter(|insert| matches(&insert.entry.index)) {
        bytes = bytes
            .saturating_add(insert.partition_key.estimated_bytes())
            .saturating_add(
                insert
                    .partition_key
                    .estimated_bytes()
                    .saturating_sub(std::mem::size_of::<VectorPartitionKey>()),
            );
    }
    for row_move in moves.iter().filter(|row_move| matches(&row_move.index)) {
        bytes = bytes
            .saturating_add(row_move.target_partition_key.estimated_bytes())
            .saturating_add(
                row_move
                    .source_partition_key
                    .estimated_bytes()
                    .saturating_add(row_move.target_partition_key.estimated_bytes()),
            );
    }
    for (_, keys) in requested.iter().filter(|(index, _)| matches(index)) {
        for key in keys {
            bytes = bytes
                .saturating_add(std::mem::size_of::<Arc<IndexState>>())
                .saturating_add(IndexState::allocation_bytes())
                .saturating_add(std::mem::size_of::<usize>().saturating_mul(3))
                .saturating_add(key.estimated_bytes())
                .saturating_add(partition_link_key_bytes(key.estimated_bytes()));
        }
    }
    for partition in reclaimable_partitions
        .iter()
        .filter(|partition| matches(&partition.index))
    {
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
    pub pending_inserts: usize,
    pub tombstones: usize,
    pub durable_vector_bytes: usize,
    pub charged_vector_bytes: usize,
    pub charged_index_bytes: usize,
    pub maintenance_progress: Option<VectorMaintenanceProgress>,
    pub maintenance_failure: Option<VectorMaintenanceFailure>,
    pub maintenance_failure_details: Option<VectorMaintenanceFailureDetails>,
    pub desired_policy: ResolvedVectorPolicy,
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

/// Standard HashMap allocation envelope: power-of-two buckets, one control
/// byte per bucket and the trailing control group. Public capacity omits the
/// load-factor spare buckets, so charge those as well as occupied slots.
fn row_placement_map_bytes<K, V>(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = capacity
        .saturating_mul(8)
        .div_ceil(7)
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .max(4);
    buckets
        .saturating_mul(std::mem::size_of::<(K, V)>() + 1)
        .saturating_add(16)
}

type PreparedPruningColumn = (
    VectorIndexRef,
    Arc<VectorColumnState>,
    Vec<(VectorPartitionKey, Arc<IndexState>)>,
    Vec<Option<RawVectorMembershipSummary>>,
);

/// See [`VectorColumnState::failed_partitions`].
#[derive(Default)]
struct FailedPartitions {
    keys: std::collections::BTreeSet<VectorPartitionKey>,
    /// A charge was refused, or the column is a replacement image: some
    /// failing partition may be missing from `keys`.
    incomplete: bool,
    /// Counts the times the set became incomplete, so a full recheck marks
    /// it complete only when no refusal happened while it ran.
    incomplete_since: u64,
}

struct VectorColumnState {
    layout: RwLock<VectorIndexLayout>,
    partitions: RwLock<BTreeMap<VectorPartitionKey, Arc<IndexState>>>,
    row_to_partition: RwLock<HashMap<RowId, Arc<VectorPartitionKey>>>,
    durable_reverse_memberships:
        RwLock<Option<NativeSourceArc<dyn RawVectorReverseMembershipSource>>>,
    /// Present exactly when this column's memberships are durable: each new
    /// partition state is created with its durable source and loader.
    durable_partition_factory: RwLock<Option<NativeSourceArc<dyn DurableRawPartitionFactory>>>,
    /// Every physical partition in which a row identity has retained history.
    /// The current directory above stays the fast path; this one makes an old
    /// snapshot resolve its own placement without probing every partition.
    row_partition_history: RwLock<HashMap<RowId, Vec<Arc<VectorPartitionKey>>>>,
    /// Derived cap occupancy of `partitions`, maintained by state mutators.
    limit_ledger: Arc<PartitionLimitLedger>,
    /// Derived retained-directory figure of the maps above.
    directory_tally: Arc<DirectoryTally>,
    /// A live column keeps the two derived figures above. A staged column
    /// (a replacement image under preparation, which may share states with
    /// the live column it replaces) recounts instead and goes live when it
    /// is installed.
    derived: AtomicBool,
    /// The partitions that have recorded a maintenance failure since they were
    /// last rechecked: a superset of the ones failing now, so a policy recheck
    /// visits only them. Its entries are charged to `directory_accounting`.
    failed_partitions: Mutex<FailedPartitions>,
    /// Registration declares the owner of every resident registry/directory
    /// image, including memory stores. Preparation admits its retained bytes;
    /// DROP COLUMN/TABLE returns them once, independent of cache state.
    // Keep last: final-column drop destroys all directory allocations first.
    // Registry removal may leave other Arc holders; their allocation remains
    // charged until this field drops with the final column holder.
    registry_copy_bytes: AtomicUsize,
    // Native counted-empty capacity prepaid by an erasure publication.
    erasure_ledger_bytes: AtomicUsize,
    directory_accounting: Mutex<PaidRetainedMemoryCharge>,
}

impl VectorColumnState {
    fn new(
        layout: VectorIndexLayout,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Self {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        let mut partitions = BTreeMap::new();
        if !layout.is_partitioned() {
            partitions.insert(
                VectorPartitionKey::unpartitioned(),
                Arc::new(IndexState::new(layout.dimension, layout.quantization)),
            );
        }
        let column = Self {
            layout: RwLock::new(layout),
            partitions: RwLock::new(partitions),
            row_to_partition: RwLock::new(HashMap::new()),
            durable_reverse_memberships: RwLock::new(None),
            durable_partition_factory: RwLock::new(None),
            row_partition_history: RwLock::new(HashMap::new()),
            limit_ledger: Arc::new(PartitionLimitLedger::default()),
            directory_tally: Arc::new(DirectoryTally::default()),
            derived: AtomicBool::new(true),
            failed_partitions: Mutex::new(FailedPartitions::default()),
            registry_copy_bytes: AtomicUsize::new(0),
            erasure_ledger_bytes: AtomicUsize::new(0),
            directory_accounting: Mutex::new(PaidRetainedMemoryCharge::new(0, declared_owner)),
        };
        for (key, state) in column.partitions.read().iter() {
            column.attach_state(key, state);
        }
        column
    }

    /// A replacement image under preparation. It may hold states that the
    /// live column it replaces still owns, so it keeps no derived figures
    /// until `go_live`.
    fn new_staged(
        layout: VectorIndexLayout,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Self {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        let column = Self::new(layout, declared_owner);
        for state in column.partitions.read().values() {
            state.detach_limit_from(&column.limit_ledger);
        }
        column.derived.store(false, Ordering::SeqCst);
        *column.directory_tally.figures.lock() = DirectoryFigures::default();
        // A replacement image can hold states that carry failures recorded
        // on the column it replaces; its first recheck looks at every one.
        {
            let mut failed = column.failed_partitions.lock();
            failed.incomplete = true;
            failed.incomplete_since += 1;
        }
        column
    }

    /// Install a staged image as the live column: one bulk pass that links
    /// every state and recounts the row maps.
    fn go_live(&self) {
        if self.derived.load(Ordering::SeqCst) {
            return;
        }
        *self.directory_tally.figures.lock() = DirectoryFigures::default();
        self.derived.store(true, Ordering::SeqCst);
        for (key, state) in self.partitions.read().iter() {
            self.attach_state(key, state);
        }
        let rows = Self::row_map_terms(&self.row_to_partition.read());
        let history = Self::history_map_terms(&self.row_partition_history.read());
        self.reset_row_map_terms((0, 0), (0, 0), rows, history);
    }

    /// Routes counted toward the declared cap. A staged image keeps no
    /// derived figures and recounts.
    fn occupied_partition_count(&self) -> usize {
        if let Some(factory) = self.durable_partition_factory.read().clone()
            && let Some(counts) = factory.published_catalog_counts()
        {
            return counts[3];
        }
        if self.derived.load(Ordering::SeqCst) {
            return self.limit_ledger.occupied();
        }
        self.partitions
            .read()
            .values()
            .filter(|state| state.entry_count() != 0 && !state.is_historical_only())
            .count()
    }

    /// A state entered this column's map under `key`.
    fn attach_state(&self, key: &VectorPartitionKey, state: &Arc<IndexState>) {
        if self.derived.load(Ordering::SeqCst) {
            IndexState::attach_limit(state, &self.limit_ledger, &self.directory_tally, key);
        }
    }

    /// Remove every state from the map; a bulk boundary that already visits
    /// each partition.
    fn clear_partition_map(&self) {
        let mut partitions = self.partitions.write();
        for state in partitions.values() {
            state.detach_limit_from(&self.limit_ledger);
        }
        partitions.clear();
    }

    fn clear_row_maps(&self) {
        let old_rows = Self::row_map_terms(&std::mem::take(&mut *self.row_to_partition.write()));
        let old_history =
            Self::history_map_terms(&std::mem::take(&mut *self.row_partition_history.write()));
        self.reset_row_map_terms(old_rows, old_history, (0, 0), (0, 0));
    }

    fn adjust_row_terms(
        &self,
        removed: Option<&VectorPartitionKey>,
        added: Option<&VectorPartitionKey>,
    ) {
        let (removed_vectors, removed_keys) = removed.map_or((0, 0), row_entry_terms);
        let (added_vectors, added_keys) = added.map_or((0, 0), row_entry_terms);
        self.directory_tally.adjust(|figures| {
            add_signed(
                &mut figures.partition_vectors,
                removed_vectors,
                added_vectors,
            );
            add_signed(&mut figures.typed_partition_keys, removed_keys, added_keys);
        });
    }

    fn adjust_history_terms(&self, before: (usize, usize), after: (usize, usize)) {
        self.directory_tally.adjust(|figures| {
            add_signed(&mut figures.partition_vectors, before.0, after.0);
            add_signed(&mut figures.typed_partition_keys, before.1, after.1);
        });
    }

    /// Recompute both row-map terms after a bulk rebuild or clear of the maps.
    fn reset_row_map_terms(
        &self,
        old_rows: (usize, usize),
        old_history: (usize, usize),
        new_rows: (usize, usize),
        new_history: (usize, usize),
    ) {
        self.adjust_history_terms(
            (
                old_rows.0.saturating_add(old_history.0),
                old_rows.1.saturating_add(old_history.1),
            ),
            (
                new_rows.0.saturating_add(new_history.0),
                new_rows.1.saturating_add(new_history.1),
            ),
        );
    }

    fn row_map_terms(rows: &HashMap<RowId, Arc<VectorPartitionKey>>) -> (usize, usize) {
        rows.values().fold((0, 0), |(vectors, keys), key| {
            let (v, k) = row_entry_terms(key);
            (vectors.saturating_add(v), keys.saturating_add(k))
        })
    }

    fn history_map_terms(history: &HashMap<RowId, Vec<Arc<VectorPartitionKey>>>) -> (usize, usize) {
        history.values().fold((0, 0), |(vectors, typed), keys| {
            let (v, k) = history_entry_terms(keys, keys.capacity());
            (vectors.saturating_add(v), typed.saturating_add(k))
        })
    }

    fn layout(&self) -> VectorIndexLayout {
        self.layout.read().clone()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn admitted_layout(&self, owner: &mut VectorCopyAdmission) -> Result<VectorIndexLayout> {
        let layout = self.layout.read();
        owner.reserve(layout.estimated_bytes())?;
        Ok(layout.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn scoped_layout(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<ScopedVectorCopy<VectorIndexLayout>> {
        ScopedVectorCopy::new(owner, |scratch| self.admitted_layout(scratch))
    }

    fn partition_state(&self, key: &VectorPartitionKey) -> Option<Arc<IndexState>> {
        self.partitions.read().get(key).cloned()
    }

    /// The partitions, or none if their map cannot be read right now.
    fn partition_snapshots(&self) -> Vec<(VectorPartitionKey, Arc<IndexState>)> {
        self.partitions
            .read()
            .iter()
            .map(|(key, state)| (key.clone(), state.clone()))
            .collect()
    }

    /// Keep route copies under the same caller admission as returned bodies.
    /// Hold the map read guard across pricing and cloning so the quote covers
    /// the exact inventory allocated, including empty partitions and text keys.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn admitted_partition_snapshots(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<(VectorPartitionKey, Arc<IndexState>)>> {
        let partitions = self.partitions.read();
        let bytes = partitions.iter().fold(0usize, |bytes, (key, _)| {
            bytes
                .saturating_add(std::mem::size_of::<(VectorPartitionKey, Arc<IndexState>)>())
                .saturating_add(key.estimated_bytes())
        });
        owner.reserve(bytes)?;
        let mut snapshots = Vec::with_capacity(partitions.len());
        snapshots.extend(
            partitions
                .iter()
                .map(|(key, state)| (key.clone(), state.clone())),
        );
        Ok(snapshots)
    }

    /// Charged size of one failed-partition entry.
    fn failed_partition_entry_bytes(key: &VectorPartitionKey) -> usize {
        key.estimated_bytes()
            .saturating_add(std::mem::size_of::<VectorPartitionKey>())
            .saturating_add(2 * std::mem::size_of::<usize>())
    }

    /// Note that `key`'s partition recorded a maintenance failure. A charge the
    /// column's owner refuses leaves the set incomplete, so the next recheck
    /// looks at every partition instead of trusting it. Keys are noted while
    /// the set is incomplete too: a full recheck rebuilds the set from them.
    fn note_failed_partition(&self, key: &VectorPartitionKey) {
        let mut failed = self.failed_partitions.lock();
        if failed.keys.contains(key) {
            return;
        }
        let bytes = Self::failed_partition_entry_bytes(key);
        let mut retained = self.directory_accounting.lock();
        if retained
            .accountant
            .try_allocate_for_static(
                bytes,
                "vector_index",
                "record_partition_failure",
                "Raise MEMORY_LIMIT to keep this partition's maintenance failure indexed.",
            )
            .is_err()
        {
            failed.incomplete = true;
            failed.incomplete_since += 1;
            return;
        }
        retained.bytes += bytes;
        failed.keys.insert(key.clone());
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn scoped_partition_snapshots(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<ScopedVectorCopy<Vec<PartitionSnapshot>>> {
        ScopedVectorCopy::new(owner, |scratch| self.admitted_partition_snapshots(scratch))
    }

    fn partition_is_historical_only(&self, key: &VectorPartitionKey) -> bool {
        self.partitions
            .read()
            .get(key)
            .is_some_and(|state| state.is_historical_only())
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
        if let Some(factory) = self.durable_partition_factory.read().clone()
            && let Some(counts) = factory.published_catalog_counts()
        {
            return counts[1];
        }
        self.partitions
            .read()
            .values()
            .filter(|state| !state.is_historical_only() && state.vector_count() != 0)
            .count()
    }

    fn retained_partition_count(&self) -> usize {
        if let Some(factory) = self.durable_partition_factory.read().clone()
            && let Some(counts) = factory.published_catalog_counts()
        {
            return counts[2];
        }
        self.partitions
            .read()
            .values()
            .filter(|state| {
                !state.is_historical_only()
                    && state.entry_count().saturating_sub(state.vector_count()) != 0
            })
            .count()
    }

    fn live_or_retained_partition_count(&self) -> usize {
        if let Some(factory) = self.durable_partition_factory.read().clone()
            && let Some(counts) = factory.published_catalog_counts()
        {
            return counts[3];
        }
        self.partitions
            .read()
            .values()
            .filter(|state| !state.is_historical_only() && state.entry_count() != 0)
            .count()
    }

    fn maintained_partition_state_count(&self) -> usize {
        if let Some(factory) = self.durable_partition_factory.read().clone()
            && let Some(counts) = factory.published_catalog_counts()
        {
            return if self.layout.read().is_partitioned() {
                counts[3]
            } else {
                1
            };
        }
        self.partitions
            .read()
            .values()
            .filter(|state| !state.is_historical_only())
            .count()
    }

    fn live_vector_count(&self) -> usize {
        if let Some(factory) = self.durable_partition_factory.read().clone()
            && let Some(counts) = factory.published_catalog_counts()
        {
            return counts[4];
        }
        self.partitions
            .read()
            .values()
            .map(|state| state.vector_count())
            .sum()
    }

    fn total_entry_count(&self) -> usize {
        if let Some(factory) = self.durable_partition_factory.read().clone()
            && let Some(counts) = factory.published_catalog_counts()
        {
            return counts[5];
        }
        self.partitions
            .read()
            .values()
            .map(|state| state.entry_count())
            .sum()
    }

    fn with_partition_candidates_for_row<E, T>(
        &self,
        row_id: RowId,
        mut inspect_partition: impl FnMut(
            &VectorPartitionKey,
            &Arc<IndexState>,
            &mut VectorCopyAdmission,
        ) -> std::result::Result<Option<T>, E>,
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<T>, E>
    where
        E: From<Error>,
    {
        #[cfg(feature = "test-seams")]
        crate::mem::observe_bounded_candidate_grouping_for_test(
            crate::mem::BoundedCandidateGroupingEventForTest::RowDirectoryLookup,
        );
        let reverse = self.durable_reverse_memberships.read().clone();
        if let Some(reverse) = reverse {
            let mut cursor_copy = owner.local_scratch();
            let mut cursor = reverse
                .open_row(row_id, &mut || true, cursor_copy.as_mut().unwrap_or(owner))
                .map_err(E::from)?;
            loop {
                let mut key_copy = owner.local_scratch();
                let Some(key) = cursor
                    .next(&mut || true, key_copy.as_mut().unwrap_or(owner))
                    .map_err(E::from)?
                else {
                    break;
                };
                let state = self.partition_state(&key).ok_or_else(|| {
                    E::from(Error::Other(
                        "durable vector reverse directory names an absent partition".into(),
                    ))
                })?;
                if let Some(value) = inspect_partition(&key, &state, owner)? {
                    return Ok(Some(value));
                }
            }
            return Ok(None);
        }
        // Copy one handle/key at a time, releasing every directory guard before
        // any callback can admit memory, perform storage IO, or reenter the store.
        let current = self.row_to_partition.read().get(&row_id).cloned();
        if let Some(key) = current.as_ref()
            && let Some(state) = self.partition_state(key)
            && let Some(value) = inspect_partition(key, &state, owner)?
        {
            return Ok(Some(value));
        }
        let mut position = 0;
        loop {
            let key = self
                .row_partition_history
                .read()
                .get(&row_id)
                .and_then(|keys| keys.get(position))
                .cloned();
            let Some(key) = key else {
                break;
            };
            position += 1;
            if current.as_ref() == Some(&key) {
                continue;
            }
            if let Some(state) = self.partition_state(&key)
                && let Some(value) = inspect_partition(&key, &state, owner)?
            {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn with_snapshot_visible_partition_for_row<E, T, C>(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        before_visibility_probe: &mut C,
        inspect_visible_partition: impl FnMut(
            &VectorPartitionKey,
            &Arc<IndexState>,
            &mut C,
            &mut VectorCopyAdmission,
        ) -> std::result::Result<Option<T>, E>,
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<T>, E>
    where
        E: From<Error>,
        C: FnMut() -> std::result::Result<(), E>,
    {
        self.with_snapshot_visible_partition_for_row_handling_mismatch(
            row_id,
            snapshot,
            before_visibility_probe,
            inspect_visible_partition,
            owner,
            |mismatch| Err(E::from(mismatch.error())),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn with_snapshot_visible_partition_for_row_handling_mismatch<E, T, C>(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        before_visibility_probe: &mut C,
        mut inspect_visible_partition: impl FnMut(
            &VectorPartitionKey,
            &Arc<IndexState>,
            &mut C,
            &mut VectorCopyAdmission,
        ) -> std::result::Result<Option<T>, E>,
        owner: &mut VectorCopyAdmission,
        mut on_mismatch: impl FnMut(SnapshotPartitionMismatch) -> std::result::Result<Option<T>, E>,
    ) -> std::result::Result<Option<T>, E>
    where
        E: From<Error>,
        C: FnMut() -> std::result::Result<(), E>,
    {
        let reverse = self.durable_reverse_memberships.read().clone();
        if let Some(reverse) = reverse {
            let mut selection_copy = owner.local_scratch();
            before_visibility_probe()?;
            #[cfg(feature = "test-seams")]
            crate::mem::observe_bounded_candidate_grouping_for_test(
                crate::mem::BoundedCandidateGroupingEventForTest::RowDirectoryLookup,
            );
            let key = with_membership_read(before_visibility_probe, |before_read| {
                reverse.visible_partition_admitted(
                    row_id,
                    snapshot,
                    before_read,
                    selection_copy.as_mut().unwrap_or(owner),
                )
            })?;
            let Some(key) = key else {
                return Ok(None);
            };
            let Some(state) = self.partition_state(&key) else {
                return on_mismatch(SnapshotPartitionMismatch::AbsentPartition);
            };
            #[cfg(feature = "test-seams")]
            crate::mem::observe_bounded_candidate_grouping_for_test(
                crate::mem::BoundedCandidateGroupingEventForTest::SnapshotVisibilityProbe,
            );
            // The reverse directory and partition source share one pinned root.
            // Authenticate the forward identity too, detecting inconsistent sidecars.
            if state
                .bounded_visible_directory_entry_by_row(
                    row_id,
                    snapshot,
                    before_visibility_probe,
                    owner,
                )?
                .is_none()
            {
                return on_mismatch(SnapshotPartitionMismatch::ForwardMembershipAbsent);
            }
            return inspect_visible_partition(&key, &state, before_visibility_probe, owner);
        }
        self.with_partition_candidates_for_row(
            row_id,
            |key, state, owner| {
                before_visibility_probe()?;
                #[cfg(feature = "test-seams")]
                crate::mem::observe_bounded_candidate_grouping_for_test(
                    crate::mem::BoundedCandidateGroupingEventForTest::SnapshotVisibilityProbe,
                );
                if state
                    .bounded_visible_directory_entry_by_row(
                        row_id,
                        snapshot,
                        before_visibility_probe,
                        owner,
                    )?
                    .is_some()
                {
                    inspect_visible_partition(key, state, before_visibility_probe, owner)
                } else {
                    Ok(None)
                }
            },
            owner,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn with_admitted_snapshot_visible_partition_for_row<T>(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
        mut inspect_visible_partition: impl FnMut(
            &VectorPartitionKey,
            &Arc<IndexState>,
            Option<&dyn RawVectorMembershipSelection>,
            &mut VectorCopyAdmission,
        ) -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        let reverse = self.durable_reverse_memberships.read().clone();
        if let Some(reverse) = reverse {
            let mut selection_copy = owner.local_scratch();
            #[cfg(feature = "test-seams")]
            crate::mem::observe_bounded_candidate_grouping_for_test(
                crate::mem::BoundedCandidateGroupingEventForTest::RowDirectoryLookup,
            );
            let key = reverse.visible_partition_admitted(
                row_id,
                snapshot,
                &mut || true,
                selection_copy.as_mut().unwrap_or(owner),
            )?;
            let Some(key) = key else {
                return Ok(None);
            };
            let state = self.partition_state(&key).ok_or_else(|| {
                Error::Other("durable vector reverse directory names an absent partition".into())
            })?;
            #[cfg(feature = "test-seams")]
            crate::mem::observe_bounded_candidate_grouping_for_test(
                crate::mem::BoundedCandidateGroupingEventForTest::SnapshotVisibilityProbe,
            );
            let source = state.durable_membership_source();
            let selected = match source
                .as_ref()
                .filter(|source| source.supports_root_selection())
            {
                Some(source) => source.select_visible_by_row(
                    row_id,
                    snapshot,
                    &mut || true,
                    selection_copy.as_mut().unwrap_or(owner),
                )?,
                None => None,
            };
            let visible = if source
                .as_ref()
                .is_some_and(|source| source.supports_root_selection())
            {
                selected.is_some()
            } else {
                state
                    .bounded_visible_directory_entry_by_row(
                        row_id,
                        snapshot,
                        &mut || Ok::<(), Error>(()),
                        owner,
                    )?
                    .is_some()
            };
            if !visible {
                return Err(Error::Other(
                    "durable vector reverse and partition memberships disagree".into(),
                ));
            }
            return inspect_visible_partition(&key, &state, selected.as_deref(), owner);
        }
        self.with_partition_candidates_for_row(
            row_id,
            |key, state, owner| {
                let mut selection_copy = owner.local_scratch();
                #[cfg(feature = "test-seams")]
                crate::mem::observe_bounded_candidate_grouping_for_test(
                    crate::mem::BoundedCandidateGroupingEventForTest::SnapshotVisibilityProbe,
                );
                let source = state.durable_membership_source();
                let selected = match source
                    .as_ref()
                    .filter(|source| source.supports_root_selection())
                {
                    Some(source) => source.select_visible_by_row(
                        row_id,
                        snapshot,
                        &mut || true,
                        selection_copy.as_mut().unwrap_or(owner),
                    )?,
                    None => None,
                };
                let visible = if source
                    .as_ref()
                    .is_some_and(|source| source.supports_root_selection())
                {
                    selected.is_some()
                } else {
                    state
                        .bounded_visible_directory_entry_by_row(
                            row_id,
                            snapshot,
                            &mut || Ok::<(), Error>(()),
                            owner,
                        )?
                        .is_some()
                };
                if visible {
                    inspect_visible_partition(key, state, selected.as_deref(), owner)
                } else {
                    Ok(None)
                }
            },
            owner,
        )
    }

    fn note_partition_history(&self, row_id: RowId, key: &VectorPartitionKey) {
        let mut history = self.row_partition_history.write();
        // Publication has already reserved one retained key slot. Starting at
        // one and extending exactly keeps the retained-directory delta aligned
        // with that pre-commit reservation instead of Vec's four-slot append
        // growth making the first historical identity larger than admitted.
        let mut created = false;
        let keys = history.entry(row_id).or_insert_with(|| {
            created = true;
            Vec::with_capacity(1)
        });
        if let Err(position) = keys.binary_search_by(|candidate| candidate.as_ref().cmp(key)) {
            let before = if created {
                (0, 0)
            } else {
                history_entry_terms(keys, keys.capacity())
            };
            if keys.len() == keys.capacity() {
                keys.reserve_exact(1);
            }
            keys.insert(position, Arc::new(key.clone()));
            let after = history_entry_terms(keys, keys.capacity());
            drop(history);
            self.adjust_history_terms(before, after);
        }
    }

    fn note_live_insert(&self, key: &VectorPartitionKey, entry: &StoredVectorEntry) {
        self.note_partition_history(entry.row_id, key);
        if entry.deleted_tx.is_none() {
            let previous = self
                .row_to_partition
                .write()
                .insert(entry.row_id, Arc::new(key.clone()));
            self.adjust_row_terms(previous.as_deref(), Some(key));
        }
    }

    fn note_tombstone(&self, key: &VectorPartitionKey, row_id: RowId) {
        self.note_partition_history(row_id, key);
        let mut rows = self.row_to_partition.write();
        if rows
            .get(&row_id)
            .is_some_and(|current| current.as_ref() == key)
        {
            let removed = rows.remove(&row_id);
            drop(rows);
            self.adjust_row_terms(removed.as_deref(), None);
        }
    }

    fn note_move(
        &self,
        source_key: &VectorPartitionKey,
        target_key: &VectorPartitionKey,
        old_row_id: RowId,
        new_row_id: RowId,
    ) {
        self.note_partition_history(old_row_id, source_key);
        self.note_partition_history(new_row_id, target_key);
        let mut rows = self.row_to_partition.write();
        let removed = if rows
            .get(&old_row_id)
            .is_some_and(|current| current.as_ref() == source_key)
        {
            rows.remove(&old_row_id)
        } else {
            None
        };
        let replaced = rows.insert(new_row_id, Arc::new(target_key.clone()));
        drop(rows);
        self.adjust_row_terms(removed.as_deref(), None);
        self.adjust_row_terms(replaced.as_deref(), Some(target_key));
    }

    fn rebuild_current_rows(&self) {
        let partitions = self.partition_snapshots();
        self.rebuild_current_rows_from(&partitions);
    }

    fn rebuild_current_rows_from(&self, partitions: &[(VectorPartitionKey, Arc<IndexState>)]) {
        let mut latest = HashMap::<RowId, (TxId, Lsn, Arc<VectorPartitionKey>)>::new();
        let mut history = HashMap::<RowId, Vec<Arc<VectorPartitionKey>>>::new();
        for (key, state) in partitions {
            for entry in state.raw_directory.read().iter() {
                let keys = history
                    .entry(entry.row_id)
                    .or_insert_with(|| Vec::with_capacity(1));
                if keys.last().is_none_or(|prior| prior.as_ref() != key) {
                    if keys.len() == keys.capacity() {
                        keys.reserve_exact(1);
                    }
                    keys.push(Arc::new(key.clone()));
                }
                if entry.deleted_tx.is_some() {
                    continue;
                }
                let replace = latest
                    .get(&entry.row_id)
                    .is_none_or(|(tx, lsn, _)| (entry.created_tx, entry.lsn) > (*tx, *lsn));
                if replace {
                    latest.insert(
                        entry.row_id,
                        (entry.created_tx, entry.lsn, Arc::new(key.clone())),
                    );
                }
            }
        }
        let latest = latest
            .into_iter()
            .map(|(row_id, (_, _, key))| (row_id, key))
            .collect::<HashMap<_, _>>();
        let new_rows = Self::row_map_terms(&latest);
        let new_history = Self::history_map_terms(&history);
        let old_history = Self::history_map_terms(&std::mem::replace(
            &mut *self.row_partition_history.write(),
            history,
        ));
        let old_rows = Self::row_map_terms(&std::mem::replace(
            &mut *self.row_to_partition.write(),
            latest,
        ));
        self.reset_row_map_terms(old_rows, old_history, new_rows, new_history);
    }

    /// Keyed removal of one retired route that retains no entry.
    fn remove_empty_partition(&self, key: &VectorPartitionKey) {
        if !self.layout.read().is_partitioned() {
            return;
        }
        let mut partitions = self.partitions.write();
        if partitions
            .get(key)
            .is_some_and(|state| state.entry_count() == 0)
            && let Some(state) = partitions.remove(key)
        {
            state.detach_limit_from(&self.limit_ledger);
        }
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
            partitions.retain(|key, state| {
                let keep = !removed.contains(key);
                if !keep {
                    state.detach_limit_from(&self.limit_ledger);
                }
                keep
            });
            removed
        };
        if !removed.is_empty() {
            let mut history = self.row_partition_history.write();
            let before = Self::history_map_terms(&history);
            history.retain(|_, keys| {
                keys.retain(|key| !removed.contains(key.as_ref()));
                keys.shrink_to_fit();
                !keys.is_empty()
            });
            let after = Self::history_map_terms(&history);
            drop(history);
            self.adjust_history_terms(before, after);
        }
        !removed.is_empty()
    }

    fn raw_allocation_bytes_matching(&self, keep: impl Fn(&Arc<IndexState>) -> bool) -> usize {
        let partitions = self.partitions.read();
        let mut total = 0usize;
        for (position, state) in partitions.values().enumerate() {
            if !keep(state) {
                continue;
            }
            let vectors = state.vectors.read();
            total = total.saturating_add(vectors.owned_payload_bytes);
            for (identity, bytes) in &vectors.payload_memberships {
                let already_counted = partitions.values().take(position).any(|prior| {
                    keep(prior)
                        && prior
                            .vectors
                            .read()
                            .payload_memberships
                            .contains_key(identity)
                });
                if !already_counted {
                    total = total
                        .saturating_add(*bytes)
                        .saturating_add(crate::quantized::shared_payload_header_bytes());
                }
            }
        }
        total
    }

    fn retained_bytes(&self, index: &VectorIndexRef) -> usize {
        self.directory_ownership(index)
            .total()
            .saturating_add(self.raw_allocation_bytes_matching(|_| true))
    }

    /// Exact disjoint owners in the lightweight registry image. These are
    /// logical retained-byte charges, so growth is deterministic and can be
    /// admitted before publication instead of inferred from allocator
    /// capacity after mutation.
    fn directory_ownership(&self, index: &VectorIndexRef) -> VectorDirectoryOwnership {
        if !self.derived.load(Ordering::SeqCst) {
            return self.directory_ownership_recount(index);
        }
        let figures = self.directory_tally.figures();
        VectorDirectoryOwnership {
            raw_reverse_directory: figures.raw_reverse_directory,
            partition_vectors: self
                .directory_fixed_bytes(index)
                .saturating_add(figures.partition_vectors),
            typed_partition_keys: figures.typed_partition_keys,
        }
    }

    fn directory_fixed_bytes(&self, index: &VectorIndexRef) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.registry_copy_bytes.load(Ordering::SeqCst))
            .saturating_add(self.erasure_ledger_bytes.load(Ordering::SeqCst))
            .saturating_add(index.table.capacity())
            .saturating_add(index.column.capacity())
            .saturating_add(self.layout.read().estimated_bytes())
    }

    /// The same figure recounted from every map; the oracle the derived
    /// figure above must equal.
    fn directory_ownership_recount(&self, index: &VectorIndexRef) -> VectorDirectoryOwnership {
        let layout_bytes = self.layout.read().estimated_bytes();
        let partitions = self.partitions.read();
        let mut ownership = VectorDirectoryOwnership {
            partition_vectors: std::mem::size_of::<Self>()
                .saturating_add(self.registry_copy_bytes.load(Ordering::SeqCst))
                .saturating_add(self.erasure_ledger_bytes.load(Ordering::SeqCst))
                .saturating_add(index.table.capacity())
                .saturating_add(index.column.capacity())
                .saturating_add(layout_bytes),
            ..VectorDirectoryOwnership::default()
        };
        for (key, state) in partitions.iter() {
            let (vectors, keys) = partition_entry_terms(key.estimated_bytes());
            ownership.typed_partition_keys = ownership.typed_partition_keys.saturating_add(keys);
            ownership.partition_vectors = ownership.partition_vectors.saturating_add(vectors);
            ownership.raw_reverse_directory = ownership
                .raw_reverse_directory
                .saturating_add(state.directory_byte_count());
        }
        drop(partitions);
        let rows = self.row_to_partition.read();
        let key_header = 0;
        ownership.partition_vectors = ownership.partition_vectors.saturating_add(
            rows.len()
                .saturating_mul(std::mem::size_of::<(RowId, Arc<VectorPartitionKey>)>())
                .saturating_sub(rows.len().saturating_mul(key_header)),
        );
        for key in rows.values() {
            ownership.typed_partition_keys = ownership
                .typed_partition_keys
                .saturating_add(key.estimated_bytes())
                .saturating_add(2 * std::mem::size_of::<usize>());
        }
        drop(rows);
        let history = self.row_partition_history.read();
        ownership.partition_vectors = ownership.partition_vectors.saturating_add(
            history
                .len()
                .saturating_mul(std::mem::size_of::<(RowId, Vec<Arc<VectorPartitionKey>>)>()),
        );
        for keys in history.values() {
            ownership.partition_vectors = ownership.partition_vectors.saturating_add(
                keys.capacity()
                    .saturating_mul(std::mem::size_of::<Arc<VectorPartitionKey>>()),
            );
            for key in keys {
                ownership.typed_partition_keys = ownership.typed_partition_keys.saturating_add(
                    key.estimated_bytes()
                        .saturating_add(2 * std::mem::size_of::<usize>()),
                );
            }
        }
        ownership
    }

    /// Retained registry identity without raw bodies or graph pages. This is
    /// the category writable open keeps resident for every idle partition.
    fn directory_retained_bytes(&self, index: &VectorIndexRef) -> usize {
        self.directory_ownership(index).total()
    }

    fn install_directory_accounting(&self, bytes: usize) {
        // The declared owner exists before preparation. Its initial retained
        // reservation has already been admitted by the producer.
        let mut charge = self.directory_accounting.lock();
        debug_assert_eq!(charge.bytes, 0);
        charge.bytes = bytes;
    }

    fn settle_directory_accounting(
        &self,
        index: &VectorIndexRef,
        reservation: &mut RetainedMemoryCharge,
    ) {
        let new_bytes = self.directory_retained_bytes(index);
        let mut charge = self.directory_accounting.lock();
        debug_assert!(crate::memory_budget::same_funder(
            charge.accountant.as_ref(),
            reservation.accountant.as_ref()
        ));
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
        let mut charge = self.directory_accounting.lock();
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
struct ProjectedPartitionState {
    total_entries: usize,
    live_by_row: HashMap<RowId, usize>,
    latest_live_rows: HashSet<RowId>,
    counts_toward_partition_limit: bool,
    /// Live rows of a durable partition this batch does not touch; only the
    /// touched rows are probed into `live_by_row`.
    untracked_live: usize,
    durable: bool,
    /// Creation of each touched live durable row, and the membership events
    /// this batch publishes, for the partition's pending-work counts.
    live_created: HashMap<RowId, TxId>,
    events: Vec<DurableMembershipEvent>,
}

/// One durable membership change of a published batch, as maintenance's
/// pending-insert and tombstone counts see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableMembershipEvent {
    /// A live membership created at `created` was closed at `deleted`.
    Closed { created: TxId, deleted: TxId },
    /// A live membership was opened at this transaction.
    Opened(TxId),
    /// An already-closed membership was published, closed at this transaction.
    PublishedClosed(TxId),
}

impl DurableMembershipEvent {
    /// Whether this event can still count against a covered frontier after
    /// `covered` (frontiers only advance).
    fn relevant_after(&self, covered: TxId) -> bool {
        match *self {
            Self::Closed { created, deleted } => created > covered || deleted > covered,
            Self::Opened(created) => created > covered,
            Self::PublishedClosed(deleted) => deleted > covered,
        }
    }
}

/// Membership events of a durable partition since `floor`: every event after
/// `floor` is present, so pending work against any covered frontier at or
/// after `floor` is counted from these alone. A frontier before `floor` is
/// counted once by a stream and then kept current in `cached`. The log is
/// bounded; past its capacity it is dropped and `floor` advances.
#[derive(Default)]
struct DurablePendingLog {
    floor: TxId,
    events: Vec<DurableMembershipEvent>,
    cached: Option<DurablePendingCounts>,
}

const DURABLE_PENDING_LOG_CAPACITY: usize = 4096;

/// Prepared post-batch state of one durable partition.
#[derive(Debug, Clone, PartialEq)]
struct PreparedDurableCounts {
    partition: VectorPartitionRef,
    summary: RawVectorMembershipSummary,
    events: Vec<DurableMembershipEvent>,
}

/// Maintenance's pending counts of a durable partition against one covered
/// frontier, computed once by a stream and then kept by commit events.
#[derive(Debug, Clone, Copy)]
struct DurablePendingCounts {
    covered: Option<TxId>,
    pending_inserts: usize,
    tombstones: usize,
}

impl ProjectedPartitionState {
    fn from_entry_count(total_entries: usize, counts_toward_partition_limit: bool) -> Self {
        Self {
            total_entries,
            counts_toward_partition_limit,
            ..Self::default()
        }
    }

    /// A durable partition is projected from its maintained counts plus a
    /// point probe of each row this batch touches; nothing is enumerated.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn from_durable_state(
        state: &IndexState,
        counts_toward_partition_limit: bool,
        touched_rows: &HashSet<RowId>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Self> {
        let source = state
            .durable_membership_source()
            .ok_or_else(|| Error::Other("durable projection lost its membership source".into()))?;
        let mut projected = Self {
            total_entries: state.entry_count(),
            live_by_row: HashMap::new(),
            latest_live_rows: HashSet::new(),
            counts_toward_partition_limit,
            untracked_live: state.vector_count(),
            durable: true,
            live_created: HashMap::new(),
            events: Vec::new(),
        };
        let mut rows = touched_rows.iter().copied().collect::<Vec<_>>();
        rows.sort_unstable();
        for row in rows {
            if let Some(identity) =
                source.visible_by_row(row, SnapshotId(u64::MAX), &mut || true, owner)?
            {
                projected.live_created.insert(row, identity.created_tx);
                projected.live_by_row.insert(row, 1);
                projected.latest_live_rows.insert(row);
                projected.untracked_live =
                    projected.untracked_live.checked_sub(1).ok_or_else(|| {
                        Error::Other("durable vector live count is below its live rows".into())
                    })?;
            }
        }
        Ok(projected)
    }

    fn from_state(state: &IndexState, counts_toward_partition_limit: bool) -> Self {
        {
            let entries = state.raw_directory.read();
            let mut projected = Self {
                total_entries: entries.len(),
                live_by_row: HashMap::new(),
                latest_live_rows: HashSet::new(),
                counts_toward_partition_limit,
                untracked_live: 0,
                durable: false,
                live_created: HashMap::new(),
                events: Vec::new(),
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

    fn delete_live_row(&mut self, row_id: RowId, deleted: TxId) -> usize {
        self.latest_live_rows.remove(&row_id);
        let removed = self.live_by_row.remove(&row_id).unwrap_or(0);
        if self.durable
            && removed != 0
            && let Some(created) = self.live_created.remove(&row_id)
        {
            self.events
                .push(DurableMembershipEvent::Closed { created, deleted });
        }
        removed
    }

    fn has_movable_row(&self, row_id: RowId) -> bool {
        self.latest_live_rows.contains(&row_id)
    }

    fn insert(&mut self, entry: &VectorEntry) {
        self.total_entries = self.total_entries.saturating_add(1);
        self.counts_toward_partition_limit = true;
        self.events.push(if entry.deleted_tx.is_none() {
            DurableMembershipEvent::Opened(entry.created_tx)
        } else {
            DurableMembershipEvent::PublishedClosed(entry.deleted_tx.unwrap_or(entry.created_tx))
        });
        if entry.deleted_tx.is_none() {
            self.live_created.insert(entry.row_id, entry.created_tx);
            *self.live_by_row.entry(entry.row_id).or_default() += 1;
            self.latest_live_rows.insert(entry.row_id);
        } else {
            self.latest_live_rows.remove(&entry.row_id);
        }
    }

    fn insert_moved(&mut self, row_id: RowId, tx: TxId) {
        self.events.push(DurableMembershipEvent::Opened(tx));
        self.live_created.insert(row_id, tx);
        self.total_entries = self.total_entries.saturating_add(1);
        self.counts_toward_partition_limit = true;
        *self.live_by_row.entry(row_id).or_default() += 1;
        self.latest_live_rows.insert(row_id);
    }

    fn live_count(&self) -> usize {
        self.live_by_row
            .values()
            .copied()
            .sum::<usize>()
            .saturating_add(self.untracked_live)
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

/// Bucket credit follows the map allocation, including its empty slots, rather
/// than any column currently occupying a slot. Keys and column bodies retain
/// their independent directory owners.
#[derive(Default)]
struct VectorRegistry {
    map: HashMap<VectorIndexRef, Arc<VectorColumnState>>,
    // Field order destroys buckets before returning their prepaid credit.
    bucket_charge: Option<PaidRetainedMemoryCharge>,
    identity_charges: Option<Box<RegistryIdentityCharge>>,
}

struct RegistryIdentityCharge {
    charge: PaidRetainedMemoryCharge,
    next: Option<Box<RegistryIdentityCharge>>,
}

impl Drop for RegistryIdentityCharge {
    fn drop(&mut self) {
        let mut next = self.next.take();
        while let Some(mut node) = next {
            next = node.next.take();
            drop(node);
        }
    }
}

impl VectorRegistry {
    fn new() -> Self {
        Self::default()
    }

    /// Live registries retain capacity across removals; an empty schema has
    /// no registry bucket allocation, just like a newly opened empty store.
    fn release_empty_buckets(&mut self) {
        if self.map.is_empty() {
            // Destroy the native allocation before returning its credit.
            drop(std::mem::take(&mut self.map));
            drop(self.bucket_charge.take());
        }
    }

    fn remove(&mut self, index: &VectorIndexRef) -> Option<Arc<VectorColumnState>> {
        let column = self.map.remove(index);
        self.release_empty_buckets();
        column
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn try_insert(
        &mut self,
        index: VectorIndexRef,
        column: Arc<VectorColumnState>,
    ) -> Result<Option<Arc<VectorColumnState>>> {
        if !self.map.contains_key(&index) && self.map.len() == self.map.capacity() {
            let requested = self
                .map
                .len()
                .checked_add(1)
                .ok_or_else(|| Error::Other("vector registry capacity overflow".into()))?;
            // A std HashMap has at most four physical buckets per requested
            // slot (including tiny maps). Include control bytes and padding.
            let bytes = requested
                .checked_mul(4)
                .and_then(|slots| {
                    slots.checked_mul(
                        std::mem::size_of::<(VectorIndexRef, Arc<VectorColumnState>)>()
                            + std::mem::size_of::<usize>(),
                    )
                })
                .ok_or_else(|| Error::Other("vector registry allocation overflow".into()))?;
            let accountant = column.directory_accounting.lock().accountant.clone();
            accountant.try_allocate_for_static(
                bytes,
                "vector_index",
                "prepare_vector_registry_growth",
                "Reduce vector declaration metadata or raise MEMORY_LIMIT before registry growth.",
            )?;
            let mut charge = PaidRetainedMemoryCharge::new(bytes, accountant);
            let mut replacement = HashMap::with_capacity(requested);
            // No fallible work follows admission. Move keys without copying;
            // the old allocation is destroyed before its credit is released.
            // Capacity determines the physical bucket count: std's map
            // exposes usable slots, with one-eighth reserved for controls.
            let buckets = replacement
                .capacity()
                .checked_add(1)
                .and_then(usize::checked_next_power_of_two)
                .expect("admitted registry bucket count fits");
            let actual_bytes = buckets
                * (std::mem::size_of::<(VectorIndexRef, Arc<VectorColumnState>)>() + 1)
                + 16;
            assert!(
                actual_bytes <= charge.bytes,
                "prepaid registry bucket/control slots cover the native allocation"
            );
            replacement.extend(self.map.drain());
            let old = std::mem::replace(&mut self.map, replacement);
            drop(old);
            charge.accountant.release(charge.bytes - actual_bytes);
            charge.bytes = actual_bytes;
            self.bucket_charge = Some(charge);
        }
        Ok(self.map.insert(index, column))
    }
}

impl std::ops::Deref for VectorRegistry {
    type Target = HashMap<VectorIndexRef, Arc<VectorColumnState>>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}
impl std::ops::DerefMut for VectorRegistry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}
impl<'a> IntoIterator for &'a VectorRegistry {
    type Item = (&'a VectorIndexRef, &'a Arc<VectorColumnState>);
    type IntoIter = std::collections::hash_map::Iter<'a, VectorIndexRef, Arc<VectorColumnState>>;
    fn into_iter(self) -> Self::IntoIter {
        self.map.iter()
    }
}

/// The rename token owns the write guard across durability. Detect a same-
/// thread attempt to re-enter before either non-reentrant lock is acquired.
struct VectorRegistryLock {
    inner: RwLock<VectorRegistry>,
    rename_thread: Mutex<Option<std::thread::ThreadId>>,
}
impl VectorRegistryLock {
    fn new(registry: VectorRegistry) -> Self {
        Self {
            inner: RwLock::new(registry),
            rename_thread: Mutex::new(None),
        }
    }
    fn assert_not_renaming(&self) {
        assert_ne!(
            *self.rename_thread.lock(),
            Some(std::thread::current().id()),
            "vector operation re-entered its prepared rename token"
        );
    }
    fn read(&self) -> parking_lot::RwLockReadGuard<'_, VectorRegistry> {
        self.assert_not_renaming();
        self.inner.read()
    }
    fn write(&self) -> parking_lot::RwLockWriteGuard<'_, VectorRegistry> {
        self.assert_not_renaming();
        self.inner.write()
    }
    fn rename_guard(&self) -> RegistryRenameThreadGuard<'_> {
        *self.rename_thread.lock() = Some(std::thread::current().id());
        RegistryRenameThreadGuard { registry: self }
    }
}
struct RegistryRenameThreadGuard<'a> {
    registry: &'a VectorRegistryLock,
}
impl Drop for RegistryRenameThreadGuard<'_> {
    fn drop(&mut self) {
        *self.registry.rename_thread.lock() = None;
    }
}

/// A dependency-closure registry whose complete allocation was funded before
/// durable publication. Consuming this token requires no registry growth.
#[doc(hidden)]
pub struct PreparedVectorTablePublication {
    publication: PreparedVectorPublication,
    policy_updates: Vec<PreparedVectorPolicyLayout>,
    _workspace: VectorCopyAdmission,
    swap_vector_bytes: usize,
}

impl PreparedVectorTablePublication {
    /// The affected vector image already owns its allocation credits. The
    /// enclosing engine image swap therefore admits no vector bytes again.
    #[doc(hidden)]
    pub fn memory_swap_vector_bytes(&self) -> usize {
        self.swap_vector_bytes
    }
}

struct PreparedRegistryDirectoryCharge {
    column: Arc<VectorColumnState>,
    index: VectorIndexRef,
    charge: PaidRetainedMemoryCharge,
}

struct PreparedPartitionRename {
    state: Arc<IndexState>,
    source: Option<NativeSourceArc<dyn RawVectorMembershipSource>>,
    loader: Option<NativeSourceArc<dyn DormantRawVectorLoader>>,
}

/// Precommitted rename handles. Drop only releases local handles and charges;
/// it neither changes a route nor performs storage I/O.
#[doc(hidden)]
pub struct PreparedVectorIndexRename<'a> {
    registry: parking_lot::RwLockWriteGuard<'a, VectorRegistry>,
    _bulk: parking_lot::RwLockWriteGuard<'a, ()>,
    replacement_registry: VectorRegistry,
    column: Arc<VectorColumnState>,
    sources: Vec<PreparedPartitionRename>,
    reverse: Option<NativeSourceArc<dyn RawVectorReverseMembershipSource>>,
    registry_directories: Vec<PreparedRegistryDirectoryCharge>,
    displaced: Option<Arc<VectorColumnState>>,
    // Drop preparation allocations before returning their statement credit.
    owner: VectorCopyAdmission,
    _rename_guard: RegistryRenameThreadGuard<'a>,
}

impl PreparedVectorIndexRename<'_> {
    /// Consume only after durable commit. Every copied handle is already
    /// admitted, including the complete replacement registry. This phase
    /// performs no source reads, allocation or fallible admission.
    pub fn publish(mut self) -> VectorTeardownReceipt {
        let mut receipt = VectorTeardownReceipt::default();
        if let Some(column) = self.displaced.take() {
            receipt.release_column(&column);
        }
        for prepared in self.sources.drain(..) {
            *prepared.state.durable_memberships.write() = prepared.source;
            *prepared.state.raw_loader.write() = prepared.loader;
            prepared.state.reclassify_limit();
        }
        *self.column.durable_reverse_memberships.write() = self.reverse.take();
        let outgoing = std::mem::replace(
            &mut *self.registry,
            std::mem::take(&mut self.replacement_registry),
        );
        // Destroy outgoing copied keys before returning their old funding.
        drop(outgoing);
        for mut prepared in self.registry_directories.drain(..) {
            // The prepared index key is temporary and drops with this entry.
            // The published registry key is already priced by its capacities;
            // do not retain the preparation copy in the column directory.
            prepared
                .column
                .registry_copy_bytes
                .store(0, Ordering::SeqCst);
            prepared
                .column
                .settle_directory_accounting(&prepared.index, &mut prepared.charge);
        }
        // Retained registry allocations now belong to their declared columns.
        // Destroy preparation inventories before returning statement credit.
        drop(std::mem::take(&mut self.registry_directories));
        drop(std::mem::take(&mut self.sources));
        drop(self.owner);
        receipt
    }
}

type PartitionSnapshot = (VectorPartitionKey, Arc<IndexState>);

pub struct VectorStore {
    last_maintenance_partition: Mutex<Option<(VectorPartitionRef, VectorCopyAdmission)>>,
    id: StoreId,
    registry: VectorRegistryLock,
    build_mutex: Mutex<()>,
    bulk_gate: RwLock<()>,
    #[cfg(any(test, feature = "test-seams"))]
    full_index_entries_touched: AtomicU64,
    #[cfg(feature = "test-seams")]
    pause_registry: crate::test_seam::PauseRegistry,
    #[cfg(any(test, feature = "test-seams"))]
    passive_activity: Arc<VectorPassiveActivity>,
    #[cfg(any(test, feature = "test-seams"))]
    cap_work: VectorCapWork,
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
    raw_payload_bytes: Arc<AtomicUsize>,
    /// Allocation headers remain visible in raw metadata receipts even when
    /// only a holder outside the registry retains the shared body.
    raw_payload_header_bytes: Arc<AtomicUsize>,
    memory_accountant: RwLock<Option<crate::memory_budget::MemoryBudgetOwner>>,
    /// Present on a durable-membership store: every column declared after
    /// open is created with its durable sources.
    durable_columns: RwLock<Option<NativeSourceArc<dyn DurableRawColumnFactory>>>,
    #[cfg(any(test, feature = "test-seams"))]
    memory_workspace_pause: Mutex<Option<Arc<MaintenanceProgressPauseSlot>>>,
}

/// Legacy charges to settle after release-only registry removal. Allocation-
/// owned payloads are excluded because their final holders return that credit.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct VectorTeardownReceipt {
    pub legacy_live_raw_bytes: usize,
    pub legacy_retained_raw_bytes: usize,
}

impl VectorTeardownReceipt {
    fn release_column(&mut self, column: &VectorColumnState) {
        for state in column.partitions.read().values() {
            if !state.release_accounted_raw_residency_for_removal() {
                for entry in &state.vectors.read().entries {
                    if !entry.vector.payload_is_charged() {
                        self.legacy_retained_raw_bytes = self
                            .legacy_retained_raw_bytes
                            .saturating_add(entry.estimated_bytes());
                        if entry.deleted_tx.is_none() {
                            self.legacy_live_raw_bytes = self
                                .legacy_live_raw_bytes
                                .saturating_add(entry.estimated_bytes());
                        }
                    }
                }
                state.clear_entries();
            }
        }
    }
}

/// An admitted replacement tied to its destination. Preparation leaves that
/// destination intact; publication only moves already-owned state.
pub struct PreparedLoadedVectorReplacement<'a> {
    destination: &'a VectorStore,
    registry: VectorRegistry,
    accountant: crate::memory_budget::MemoryBudgetOwner,
}

impl PreparedLoadedVectorReplacement<'_> {
    pub fn publish(self) {
        self.destination.with_bulk_maintenance(|| {
            if self.destination.memory_accountant.read().is_none() {
                *self.destination.memory_accountant.write() = Some(self.accountant.clone());
            }
            for column in self.registry.values() {
                column.go_live();
            }
            let old = std::mem::replace(&mut *self.destination.registry.write(), self.registry);
            drop(old);
        });
    }
}

struct PreparedErasurePartitionLink {
    column: Arc<VectorColumnState>,
    state: Arc<IndexState>,
    key: Arc<VectorPartitionKey>,
}

/// A fully constructed vector registry or existing-cell erasure delta.
/// Staging funds all allocations before durable publication.
pub struct PreparedVectorPublication {
    registry: VectorRegistry,
    /// State handles borrowed from the live registry because authoritative
    /// purge did not change their partition. Rollback must not clear these
    /// graphs, and successful publication must not clear them through the
    /// outgoing registry: the same `Arc` becomes part of the new image.
    preserved_states: HashSet<usize>,
    // Declaration-only publication retains an entire column by identity. A
    // column-count-sized carrier avoids gathering all of its partition ids.
    preserved_columns: Vec<usize>,
    // Erasure replaces existing cells only. The registry is a selected delta,
    // never a full replacement catalog in this mode.
    existing_cells_only: bool,
    purge_links: Vec<PreparedErasurePartitionLink>,
    // Last field: all prepared payloads and native containers die first.
    purge_workspace: Option<VectorCopyAdmission>,
}

impl PreparedVectorPublication {
    fn new(registry: VectorRegistry) -> Self {
        Self {
            registry,
            preserved_states: HashSet::new(),
            preserved_columns: Vec::new(),
            existing_cells_only: false,
            purge_links: Vec::new(),
            purge_workspace: None,
        }
    }

    fn into_parts(
        mut self,
    ) -> (
        VectorRegistry,
        HashSet<usize>,
        Vec<usize>,
        bool,
        Vec<PreparedErasurePartitionLink>,
        Option<VectorCopyAdmission>,
    ) {
        (
            std::mem::take(&mut self.registry),
            std::mem::take(&mut self.preserved_states),
            std::mem::take(&mut self.preserved_columns),
            self.existing_cells_only,
            std::mem::take(&mut self.purge_links),
            self.purge_workspace.take(),
        )
    }

    /// Bytes that need funding from the engine's enclosing image swap.
    /// Directory, raw body and graph allocations already carry their own
    /// credits. Preserved states retain the same owners across publication;
    /// neither side of the engine swap may admit or release those bytes.
    #[doc(hidden)]
    pub fn memory_swap_vector_bytes(&self) -> usize {
        0
    }

    /// Physical bytes belonging to preserved state identities. Their existing
    /// credits continue across publication; this is not an engine transfer.
    #[doc(hidden)]
    pub fn preserved_state_bytes(&self) -> usize {
        self.registry.values().fold(0usize, |total, column| {
            let preserved = |state: &Arc<IndexState>| {
                self.preserved_columns
                    .contains(&(Arc::as_ptr(column) as usize))
                    || self
                        .preserved_states
                        .contains(&(Arc::as_ptr(state) as usize))
            };
            total.saturating_add(column.raw_allocation_bytes_matching(preserved))
        })
    }
}

impl PreparedVectorPublication {}

/// A prepared purge publication can own precharged index state before the
/// durable commit. If that commit never happens, release it with the
/// publication rather than leaking its final allocation.
impl Drop for PreparedVectorPublication {
    fn drop(&mut self) {
        for column in self.registry.values() {
            if self
                .preserved_columns
                .contains(&(Arc::as_ptr(column) as usize))
            {
                continue;
            }
        }
    }
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new()
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

    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub fn registry_allocation_for_test(&self) -> (usize, usize) {
        let registry = self.registry.read();
        (
            registry.map.capacity(),
            registry
                .bucket_charge
                .as_ref()
                .map_or(0, |charge| charge.bytes),
        )
    }

    pub fn new() -> Self {
        Self {
            id: StoreId::next(),
            registry: VectorRegistryLock::new(VectorRegistry::new()),
            build_mutex: Mutex::new(()),
            bulk_gate: RwLock::new(()),
            #[cfg(any(test, feature = "test-seams"))]
            full_index_entries_touched: AtomicU64::new(0),
            #[cfg(feature = "test-seams")]
            pause_registry: crate::test_seam::PauseRegistry::default(),
            #[cfg(any(test, feature = "test-seams"))]
            passive_activity: Arc::new(VectorPassiveActivity::default()),
            #[cfg(any(test, feature = "test-seams"))]
            cap_work: VectorCapWork::default(),
            #[cfg(any(test, feature = "test-seams"))]
            maintenance_progress_pauses: Mutex::new(HashMap::new()),
            #[cfg(any(test, feature = "test-seams"))]
            maintenance_preparation_pauses: Mutex::new(HashMap::new()),
            #[cfg(any(test, feature = "test-seams"))]
            journal_truncation_faults: Mutex::new(HashMap::new()),
            last_maintenance_partition: Mutex::new(None),
            workspace_bytes: Arc::new(AtomicUsize::new(0)),
            raw_payload_bytes: Arc::new(AtomicUsize::new(0)),
            raw_payload_header_bytes: Arc::new(AtomicUsize::new(0)),
            memory_accountant: RwLock::new(None),
            durable_columns: RwLock::new(None),
            #[cfg(any(test, feature = "test-seams"))]
            memory_workspace_pause: Mutex::new(None),
        }
    }

    /// Admit a real temporary owner before vector generation publication.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn reserve_workspace(
        &self,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        bytes: usize,
        operation: &str,
        hint: &str,
    ) -> Result<VectorWorkspaceReservation> {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        VectorWorkspaceReservation::try_new(
            accountant,
            self.workspace_bytes.clone(),
            bytes,
            operation,
            hint,
        )
    }

    /// Static labels keep their original lifetime through workspace admission.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn reserve_workspace_static(
        &self,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        bytes: usize,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<VectorWorkspaceReservation> {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        VectorWorkspaceReservation::try_new_static(
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
            // Observation setup borrows the registry; it must not create an
            // unadmitted temporary partition/key inventory on the read path.
            for state in column.partitions.read().values() {
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

    /// Bind actual native construction/rewrite events to this store's genuine
    /// existing passive counter owner, including on a query worker thread.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn native_index_activity_scope_for_test(&self) -> crate::NativeIndexActivityScopeForTest {
        crate::build_observation::activity_scope(self.passive_activity.clone())
    }

    /// Cap work spent by partitioned writes since this store opened.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn cap_work_counters_for_test(&self) -> VectorCapWorkCountersForTest {
        VectorCapWorkCountersForTest {
            projected_routes: self.cap_work.projected_routes.load(Ordering::SeqCst),
            cap_edge_route_visits: self.cap_work.cap_edge_route_visits.load(Ordering::SeqCst),
            hold_lookups: self.cap_work.hold_lookups.load(Ordering::SeqCst),
            cap_edge_identity_probes: self
                .cap_work
                .cap_edge_identity_probes
                .load(Ordering::SeqCst),
            hold_streamed_records: self.cap_work.hold_streamed_records.load(Ordering::SeqCst),
        }
    }

    /// The column's maintained cap occupancy and directory figure beside a
    /// full recount. Test observation only; the recount walks every map.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn partition_limit_audit_for_test(
        &self,
        index: &VectorIndexRef,
    ) -> Option<VectorPartitionLimitAuditForTest> {
        self.with_bulk_read(|| {
            let column = self.try_column(index)?;
            let partitions = column.partition_snapshots();
            let recounted_occupied = partitions
                .iter()
                .filter(|(_, state)| state.limit_class().counted())
                .count();
            let recounted_empty_counted = partitions
                .iter()
                .filter(|(_, state)| state.limit_class() == PartitionLimitClass::CountedEmpty)
                .map(|(key, _)| key.clone())
                .collect();
            Some(VectorPartitionLimitAuditForTest {
                occupied: column.limit_ledger.occupied(),
                recounted_occupied,
                empty_counted: {
                    let mut keys = column
                        .limit_ledger
                        .empty_counted()
                        .into_iter()
                        .map(|(_, key)| key.as_ref().clone())
                        .collect::<Vec<_>>();
                    keys.sort();
                    keys
                },
                recounted_empty_counted,
                directory_bytes: column.directory_retained_bytes(index),
                recounted_directory_bytes: column.directory_ownership_recount(index).total(),
            })
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn note_cap_work(&self, note: impl FnOnce(&VectorCapWork)) {
        note(&self.cap_work);
    }

    #[cfg(not(any(test, feature = "test-seams")))]
    fn note_cap_work(&self, _note: impl FnOnce(&VectorCapWork)) {}

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn has_maintenance_progress_pause_for_test(&self, partition: &VectorPartitionRef) -> bool {
        self.maintenance_progress_pauses
            .lock()
            .get(partition)
            .is_some_and(|slot| slot.requested_vectors().is_some())
    }

    /// Observe insertion inside a pending paged batch using the existing
    /// armed pause, without publishing unfinished maintenance progress.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn maybe_pause_paged_build_insertion_for_test(
        &self,
        partition: &VectorPartitionRef,
        done: usize,
    ) {
        let slot = self
            .maintenance_progress_pauses
            .lock()
            .get(partition)
            .cloned();
        if let Some(slot) = slot {
            slot.pause_after(done);
        }
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_received_schema_publication(
        schemas: Vec<(VectorIndexRef, usize, VectorQuantization)>,
        entries: Vec<VectorEntry>,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<PreparedVectorPublication> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        let mut registry = VectorRegistry::new();
        for (index, dimension, quantization) in schemas {
            registry.try_insert(
                index,
                Arc::new(VectorColumnState::new_staged(
                    VectorIndexLayout::unpartitioned(dimension, quantization),
                    declared_owner.clone(),
                )),
            )?;
        }
        for entry in entries {
            let index = entry.index.clone();
            if !registry.contains_key(&index) {
                registry.try_insert(
                    index.clone(),
                    Arc::new(VectorColumnState::new_staged(
                        VectorIndexLayout::unpartitioned(
                            entry.vector.len(),
                            VectorQuantization::F32,
                        ),
                        declared_owner.clone(),
                    )),
                )?;
            }
            let column = registry.get(&index).expect("prepared column exists");
            let key = VectorPartitionKey::unpartitioned();
            let state = column
                .partition_state(&key)
                .expect("unpartitioned declaration always owns its empty-key state");
            let stored = state.stored_entry(entry);
            state.push_entry_with(stored, |stored| column.note_live_insert(&key, stored));
        }
        for (index, column) in &registry {
            let bytes = column.directory_retained_bytes(index);
            declared_owner.try_allocate_for_static(
                bytes,
                "vector_index",
                "prepare_vector_directory_growth",
                "Reduce vector declaration metadata or raise MEMORY_LIMIT before publication.",
            )?;
            column.install_directory_accounting(bytes);
        }
        Ok(PreparedVectorPublication::new(registry))
    }

    /// Key-aware received-schema staging. Unlike the legacy wire-shaped
    /// method above, declarations carry their full layout and entries carry
    /// their already-derived typed key.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_partitioned_received_schema_publication(
        &self,
        schemas: Vec<(VectorIndexRef, VectorIndexLayout)>,
        entries: Vec<PartitionedVectorEntry>,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        mut owner: VectorCopyAdmission,
    ) -> Result<PreparedVectorPublication> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        let mut bytes = std::mem::size_of::<PreparedVectorPublication>().saturating_add(
            3 * (2 * std::mem::size_of::<usize>() + std::mem::size_of::<AtomicUsize>()),
        );
        for (index, layout) in &schemas {
            bytes = bytes
                .saturating_add(layout.estimated_bytes().saturating_mul(16))
                .saturating_add(
                    index
                        .table
                        .len()
                        .saturating_add(index.column.len())
                        .saturating_mul(32),
                )
                .saturating_add(std::mem::size_of::<VectorColumnState>())
                .saturating_add(8 * std::mem::size_of::<usize>());
        }
        for entry in &entries {
            bytes = bytes
                .saturating_add(entry.partition_key.estimated_bytes().saturating_mul(32))
                .saturating_add(
                    entry
                        .entry
                        .index
                        .table
                        .len()
                        .saturating_add(entry.entry.index.column.len())
                        .saturating_mul(32),
                )
                .saturating_add(
                    entry
                        .entry
                        .vector
                        .capacity()
                        .saturating_mul(std::mem::size_of::<f32>())
                        .saturating_mul(2),
                )
                .saturating_add(IndexState::allocation_bytes())
                .saturating_add(32 * std::mem::size_of::<StoredVectorEntry>())
                .saturating_add(16 * crate::quantized::shared_payload_header_bytes());
        }
        owner.reserve(bytes)?;
        let schemas = schemas
            .into_iter()
            .map(|(index, layout)| {
                let column_owner = self
                    .try_column(&index)
                    .map(|column| column.directory_accounting.lock().accountant.clone())
                    .unwrap_or_else(|| declared_owner.clone());
                (index, layout, column_owner)
            })
            .collect();
        let mut publication = Self::prepare_partitioned_received_schema_admitted(schemas, entries)?;
        self.prepare_received_schema_durable_sources(&mut publication, &mut owner)?;
        let candidate = Self {
            raw_payload_bytes: self.raw_payload_bytes.clone(),
            raw_payload_header_bytes: self.raw_payload_header_bytes.clone(),
            ..Self::default()
        };
        *candidate.registry.write() = publication.into_parts().0;
        let directory_reservation = PreparedDirectoryReservation::empty();
        for (index, column) in candidate.registry.read().iter() {
            let retained =
                VectorCopyAdmission::new(column.directory_accounting.lock().accountant.clone());
            directory_reservation.reserve_index(
                index.clone(),
                retained.accountant().clone(),
                column.directory_retained_bytes(index),
            )?;
        }
        let directory_charges = directory_reservation.take();
        drop(directory_reservation);
        candidate.account_received_state_with_directory_charges(directory_charges)?;
        let registry = std::mem::take(&mut *candidate.registry.write());
        Ok(PreparedVectorPublication::new(registry))
    }

    // This helper has no public copy door: callers reserve the complete
    // candidate before entering, then adopt its retained charge on success.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn prepare_partitioned_received_schema_admitted(
        schemas: Vec<(
            VectorIndexRef,
            VectorIndexLayout,
            crate::memory_budget::MemoryBudgetOwner,
        )>,
        mut entries: Vec<PartitionedVectorEntry>,
    ) -> Result<PreparedVectorPublication> {
        // The caller admitted this whole candidate before entering. Sorting
        // its owned carrier needs no second population or key/body copies.
        entries.sort_by(|a, b| {
            (
                &a.entry.index.table,
                &a.entry.index.column,
                a.entry.row_id,
                a.entry.created_tx,
                a.entry.lsn,
                a.membership.visible_from,
            )
                .cmp(&(
                    &b.entry.index.table,
                    &b.entry.index.column,
                    b.entry.row_id,
                    b.entry.created_tx,
                    b.entry.lsn,
                    b.membership.visible_from,
                ))
        });
        for entry in &entries {
            if entry
                .membership
                .deleted_tx
                .is_some_and(|end| end < entry.membership.visible_from)
            {
                return Err(Error::Other(
                    "vector source membership interval ends before it begins".into(),
                ));
            }
        }
        for pair in entries.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            if a.entry.index == b.entry.index
                && a.entry.row_id == b.entry.row_id
                && a.entry.created_tx == b.entry.created_tx
                && a.entry.lsn == b.entry.lsn
                && a.canonical_deleted_tx != b.canonical_deleted_tx
            {
                return Err(Error::Other(
                    "canonical vector body has conflicting deletion stamps".into(),
                ));
            }
            if a.entry.index == b.entry.index
                && a.entry.row_id == b.entry.row_id
                && a.entry.created_tx == b.entry.created_tx
                && a.entry.lsn == b.entry.lsn
                && a.membership
                    .deleted_tx
                    .is_none_or(|end| end > b.membership.visible_from)
            {
                return Err(Error::Other(
                    "canonical vector body has overlapping source memberships".into(),
                ));
            }
        }
        let mut registry = VectorRegistry::new();
        for (index, layout, declared_owner) in schemas {
            Self::validate_layout(&index, &layout)?;
            registry.try_insert(
                index,
                Arc::new(VectorColumnState::new_staged(layout, declared_owner)),
            )?;
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
            let mut stored = state.stored_entry(entry.entry);
            stored.membership = entry.membership;
            stored.canonical_deleted_tx = entry.canonical_deleted_tx;
            state.push_entry_with(stored, |stored| {
                column.note_live_insert(&entry.partition_key, stored)
            });
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

    /// Build the exact replacement registry for authoritative purge.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_authoritative_purge_publication(
        schemas: Vec<(VectorIndexRef, usize, VectorQuantization)>,
        entries: Vec<VectorEntry>,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<PreparedVectorPublication> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        Self::prepare_received_schema_publication(schemas, entries, declared_owner)
    }

    /// Internal erasure bridge. Copy only declarations in the selected tables;
    /// all other registry cells, sources, policies and graph owners stay put.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn copy_erasure_layouts(
        &self,
        selected_table: impl Fn(&str) -> bool,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<(VectorIndexRef, VectorIndexLayout)>> {
        self.with_bulk_read(|| {
            let registry = self.registry.read();
            let count = registry
                .keys()
                .filter(|index| selected_table(&index.table))
                .count();
            owner.reserve(
                count.saturating_mul(std::mem::size_of::<(VectorIndexRef, VectorIndexLayout)>()),
            )?;
            let mut layouts = Vec::with_capacity(count);
            for (index, column) in registry
                .iter()
                .filter(|(index, _)| selected_table(&index.table))
            {
                let layout = column.layout.read();
                owner.reserve(
                    index
                        .table
                        .len()
                        .saturating_add(index.column.len())
                        .saturating_add(layout.estimated_bytes()),
                )?;
                layouts.push((index.clone(), layout.clone()));
            }
            Ok(layouts)
        })
    }

    /// Prepare a complete selected-table delta before durability. No registry
    /// entry may be added by erasure; publication uses existing-cell swaps.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_selected_authoritative_purge_publication(
        &self,
        schemas: Vec<(VectorIndexRef, VectorIndexLayout)>,
        entries: Vec<PartitionedVectorEntry>,
        affected_partitions: Vec<VectorPartitionRef>,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<PreparedVectorPublication> {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        let mut workspace = owner.scratch();
        // Bound simultaneous native maps/sets, their growth overlap, copied
        // keys, partition snapshots, column headers and row reverse maps.
        // Retained output directories and graphs get their standing owners
        // separately in the common preparation kernel.
        self.with_bulk_read(|| -> Result<()> {
            let live = self.registry.read();
            for (index, layout) in &schemas {
                let column = live.get(index).ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
                if *column.layout.read() != *layout {
                    return Err(Error::Other(
                        "vector erasure declaration changed during preparation".into(),
                    ));
                }
                let fixed = std::mem::size_of::<VectorColumnState>()
                    .saturating_add(2 * std::mem::size_of::<usize>())
                    .saturating_add(layout.estimated_bytes())
                    .saturating_add(index.table.len())
                    .saturating_add(index.column.len())
                    .saturating_add(std::mem::size_of::<(VectorIndexRef, PartitionKeySet)>());
                workspace.reserve(
                    fixed
                        .saturating_add(column.directory_retained_bytes(index))
                        .saturating_mul(16),
                )?;
                for (key, state) in column.partitions.read().iter() {
                    let identity = std::mem::size_of::<(VectorPartitionRef, usize)>()
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len())
                        .saturating_add(key.estimated_bytes());
                    workspace.reserve(
                        identity.saturating_mul(16).saturating_add(
                            state.entry_count().saturating_mul(
                                16 * (std::mem::size_of::<(RowId, Arc<VectorPartitionKey>)>()
                                    .saturating_add(key.estimated_bytes())),
                            ),
                        ),
                    )?;
                }
            }
            Ok(())
        })?;
        // A new key in an affected partition need not already be resident.
        for entry in &entries {
            workspace.reserve(
                std::mem::size_of::<(VectorPartitionRef, usize)>()
                    .saturating_add(entry.entry.index.table.len())
                    .saturating_add(entry.entry.index.column.len())
                    .saturating_add(entry.partition_key.estimated_bytes())
                    .saturating_mul(16)
                    .saturating_add(32 * std::mem::size_of::<StoredVectorEntry>())
                    .saturating_add(16 * crate::quantized::shared_payload_header_bytes())
                    .saturating_add(
                        entry
                            .entry
                            .vector
                            .capacity()
                            .saturating_mul(std::mem::size_of::<f32>())
                            .saturating_mul(2),
                    ),
            )?;
        }
        let mut publication = self.prepare_partitioned_authoritative_purge_publication(
            schemas,
            entries,
            affected_partitions,
            accountant,
            &mut workspace,
        )?;
        // The ordinary go_live path clones link keys and may grow the
        // counted-empty ledger. Prepare those allocations without reparenting
        // a preserved state away from the live column before durability.
        let link_count = publication
            .registry
            .values()
            .map(|column| column.partitions.read().len())
            .sum::<usize>();
        workspace.reserve(
            link_count.saturating_mul(std::mem::size_of::<PreparedErasurePartitionLink>()),
        )?;
        publication.purge_links = Vec::with_capacity(link_count);
        for column in publication.registry.values() {
            let partitions = column.partitions.read();
            let ledger_bytes = row_placement_map_bytes::<
                usize,
                (Weak<IndexState>, Arc<VectorPartitionKey>),
            >(partitions.len().saturating_mul(2));
            let accountant = column.directory_accounting.lock().accountant.clone();
            accountant.try_allocate_for_static(
                ledger_bytes,
                "vector_index",
                "prepare_purge_partition_ledger",
                "Reduce selected vector partitions or raise MEMORY_LIMIT before purge.",
            )?;
            let mut ledger_charge = PaidRetainedMemoryCharge::new(ledger_bytes, accountant);
            let mut tally = column.limit_ledger.tally.lock();
            tally.empty_counted.reserve(partitions.len());
            let actual = row_placement_map_bytes::<
                usize,
                (Weak<IndexState>, Arc<VectorPartitionKey>),
            >(tally.empty_counted.capacity());
            assert!(
                actual <= ledger_bytes,
                "prepaid erasure ledger covers native buckets"
            );
            drop(tally);
            column.erasure_ledger_bytes.store(actual, Ordering::SeqCst);
            column.directory_accounting.lock().bytes += actual;
            ledger_charge.bytes -= actual;
            for (key, state) in partitions.iter() {
                workspace.reserve(
                    key.estimated_bytes()
                        .saturating_add(2 * std::mem::size_of::<usize>()),
                )?;
                publication.purge_links.push(PreparedErasurePartitionLink {
                    column: column.clone(),
                    state: state.clone(),
                    key: Arc::new(key.clone()),
                });
                if publication
                    .preserved_states
                    .contains(&(Arc::as_ptr(state) as usize))
                    || !state.raw_resident.load(Ordering::SeqCst)
                {
                    continue;
                }
                // These rebuilt states own fresh inline payloads. Adopt their
                // exact standing body charge before durability, including abort.
                let accountant = column.directory_accounting.lock().accountant.clone();
                let bytes = state
                    .vectors
                    .read()
                    .entries
                    .iter()
                    .filter(|entry| !entry.vector.payload_is_charged())
                    .fold(0usize, |bytes, entry| {
                        bytes
                            .saturating_add(entry.estimated_bytes())
                            .saturating_add(entry.vector.payload_header_bytes())
                    });
                accountant.try_allocate_for_static(
                    bytes,
                    "vector_index",
                    "prepare_purge_vector_bodies",
                    "Reduce selected vector bodies or raise MEMORY_LIMIT before purge.",
                )?;
                let mut charge = PaidRetainedMemoryCharge::new(bytes, accountant.clone());
                state.attach_raw_accounting(
                    accountant,
                    self.raw_payload_bytes.clone(),
                    self.raw_payload_header_bytes.clone(),
                );
                charge.bytes = 0;
            }
        }
        publication.existing_cells_only = true;
        publication.purge_workspace = Some(workspace);
        Ok(publication)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_partitioned_authoritative_purge_publication(
        &self,
        schemas: Vec<(VectorIndexRef, VectorIndexLayout)>,
        entries: Vec<PartitionedVectorEntry>,
        affected_partitions: Vec<VectorPartitionRef>,
        _accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<PreparedVectorPublication> {
        let _accountant: crate::memory_budget::MemoryBudgetOwner = _accountant.into();

        let affected_partitions = affected_partitions.into_iter().collect::<HashSet<_>>();
        let mut registry = VectorRegistry::new();
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
            let declared_owner = self
                .retained_column_copy_owner(&index, owner)?
                .accountant()
                .clone();
            registry.try_insert(
                index,
                Arc::new(VectorColumnState::new_staged(layout, declared_owner)),
            )?;
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
            for (index, replacement_column) in registry.iter() {
                let Some(current_column) = current_registry.get(index) else {
                    continue;
                };
                if current_column.layout() != replacement_column.layout() {
                    return Err(Error::Other(
                        (format!(
                            "authoritative purge vector layout changed while preparing {}.{}",
                            index.table, index.column
                        ))
                        .into(),
                    ));
                }
                // A durable-membership column stays durable across purge.
                *replacement_column.durable_reverse_memberships.write() =
                    current_column.durable_reverse_memberships.read().clone();
                *replacement_column.durable_partition_factory.write() =
                    current_column.durable_partition_factory.read().clone();
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
                    return Err(Error::Other((format!(
                        "authoritative purge vector layout changed while preparing {}.{}",
                        index.table, index.column
                    )).into()));
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
                            return Err(Error::Other((format!(
                                "authoritative purge affected-partition set is incomplete for {}.{}",
                                index.table, index.column
                            )).into()));
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
            let mut stored = state.stored_entry(entry.entry);
            stored.membership = entry.membership;
            stored.canonical_deleted_tx = entry.canonical_deleted_tx;
            state.push_entry(stored);
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
            preserved_columns: Vec::new(),
            existing_cells_only: false,
            purge_links: Vec::new(),
            purge_workspace: None,
        };
        // Rebuilt partitions of a durable column hold their survivors only
        // to build their graph; afterwards they read the durable directory
        // the purge commit leaves, like every other partition there.
        for (index, column) in &publication.registry {
            let Some(factory) = column.durable_partition_factory.read().clone() else {
                continue;
            };
            for (partition_key, state) in column.admitted_partition_snapshots(owner)? {
                if state.is_durable() {
                    continue;
                }
                owner.reserve(index.table.len().saturating_add(index.column.len()))?;
                let partition = VectorPartitionRef::new(index.clone(), partition_key);
                let (memberships, loader) = factory.create(
                    &partition,
                    owner.scratch(),
                    self.retained_column_copy_owner(index, owner)?,
                )?;
                state.convert_to_durable(
                    memberships,
                    loader,
                    self.raw_payload_bytes.clone(),
                    self.raw_payload_header_bytes.clone(),
                );
            }
            column.rebuild_current_rows();
        }
        // Directory output belongs to each existing column even when the
        // replacement is prepared by a distinct executing owner. Admission
        // happens here, before durable purge; publication only installs it.
        for (index, column) in &publication.registry {
            let retained = self.retained_column_copy_owner(index, owner)?;
            let bytes = column.directory_retained_bytes(index);
            retained.accountant().try_allocate_for_static(
                bytes,
                "vector_index",
                "prepare_purge_vector_directory",
                "Reduce retained vector identities or raise MEMORY_LIMIT before purge.",
            )?;
            column.install_directory_accounting(bytes);
        }
        Ok(publication)
    }

    /// Report the durable checkpoint after a paged batch completes or aborts.
    #[doc(hidden)]
    pub fn report_partition_paged_build_progress(
        &self,
        partition: &VectorPartitionRef,
        phase: &'static str,
        reason: &'static str,
        vectors_total: usize,
        vectors_done: usize,
        checkpoint_tx: TxId,
    ) {
        let Some(state) = self.try_partition_state(&partition.index, &partition.partition_key)
        else {
            return;
        };
        state.maintenance_inspection.write().progress = Some(VectorMaintenanceProgress {
            state: phase,
            reason,
            vectors_total,
            vectors_done: vectors_done.min(vectors_total),
            checkpoint_tx,
        });
    }

    /// Clear the reported progress of a published or abandoned paged build.
    #[doc(hidden)]
    pub fn clear_partition_paged_build_progress(&self, partition: &VectorPartitionRef) {
        if let Some(state) = self.try_partition_state(&partition.index, &partition.partition_key) {
            state.maintenance_inspection.write().progress = None;
        }
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
        self.registry.read().values().any(|column| {
            if !column.layout.read().is_partitioned() {
                return false;
            }
            column.partitions.read().values().any(|state| {
                state.entry_count() == 0
                    || (!state.historical_only.load(Ordering::SeqCst) && state.vector_count() == 0)
            })
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn retire_snapshot_free_partition_states(
        &self,
        registered_snapshots: &[SnapshotId],
        owner: &mut VectorCopyAdmission,
    ) -> Result<usize> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.with_bulk_maintenance(|| {
            let mut retired = 0usize;
            let indexes = Self::sorted_refs(self.registry.read().keys().cloned());
            let sampled = self.directory_bytes_before_reclamation(&indexes);
            for index in &indexes {
                let Some(column) = self.try_column(index) else {
                    continue;
                };
                if !column.layout.read().is_partitioned() {
                    continue;
                }
                let mut has_empty_state = false;
                for (partition_key, state) in column.admitted_partition_snapshots(owner)? {
                    if state.entry_count() == 0 {
                        // A state with no raw versions cannot answer any
                        // historical read. Do not leave its empty active
                        // identity consuming an inspection slot after the
                        // source history has already been pruned.
                        has_empty_state = true;
                        continue;
                    }
                    if column.partition_is_historical_only(&partition_key)
                        || state.vector_count() != 0
                    {
                        continue;
                    }
                    let mut needed = false;
                    for snapshot in registered_snapshots {
                        needed |=
                            state.try_directory_visible_entry_count(*snapshot, None, owner)? != 0;
                    }
                    if needed {
                        continue;
                    }
                    column.mark_historical_only(&partition_key);
                    retired = retired.saturating_add(1);
                }
                if has_empty_state {
                    column.remove_empty_partitioned_states();
                }
            }
            self.reconcile_directory_accounting_for_indexes(&sampled);
            Ok(retired)
        })
    }

    /// Replace an already prepared registry after durable received-schema
    /// publication. Any outgoing index state is retired here, after the
    /// replacement is visible, through its existing vector allocation owner.
    /// Preserved state owners continue; engine settlement excludes both.
    pub fn publish_prepared_received_schema(&self, publication: PreparedVectorPublication) {
        self.registry.assert_not_renaming();
        let _bulk = self.bulk_gate.write();
        let (
            mut registry,
            preserved_states,
            preserved_columns,
            existing_cells_only,
            links,
            workspace,
        ) = publication.into_parts();
        let copied_identity_credit = registry.identity_charges.take();
        for column in registry.values() {
            if preserved_columns.contains(&(Arc::as_ptr(column) as usize)) {
                continue;
            }
            let declared_owner = column.directory_accounting.lock().accountant.clone();
            for state in column.partitions.read().values() {
                if !existing_cells_only
                    && state.raw_resident.load(Ordering::SeqCst)
                    && !preserved_states.contains(&(Arc::as_ptr(state) as usize))
                {
                    state.attach_raw_accounting(
                        declared_owner.clone(),
                        self.raw_payload_bytes.clone(),
                        self.raw_payload_header_bytes.clone(),
                    );
                }
            }
        }
        if existing_cells_only {
            for column in registry.values() {
                *column.directory_tally.figures.lock() = DirectoryFigures::default();
                column.derived.store(true, Ordering::SeqCst);
            }
            for link in links {
                IndexState::attach_limit_with_prepared_key(
                    &link.state,
                    &link.column.limit_ledger,
                    &link.column.directory_tally,
                    link.key,
                );
            }
            for column in registry.values() {
                let rows = VectorColumnState::row_map_terms(&column.row_to_partition.read());
                let history =
                    VectorColumnState::history_map_terms(&column.row_partition_history.read());
                column.reset_row_map_terms((0, 0), (0, 0), rows, history);
            }
        } else {
            drop(links);
            for column in registry.values() {
                if !preserved_columns.contains(&(Arc::as_ptr(column) as usize)) {
                    column.go_live();
                }
            }
        }
        let old_registry = if existing_cells_only {
            let mut live = self.registry.write();
            // Validate the entire unit before replacing any cell. The engine
            // holds its commit fence from preparation through publication.
            assert!(
                registry.keys().all(|index| live.contains_key(index)),
                "prepared erasure cannot create vector registry entries"
            );
            for (index, column) in registry.iter_mut() {
                std::mem::swap(live.get_mut(index).expect("validated erasure cell"), column);
            }
            // The selected carrier now owns exactly the retired columns.
            registry
        } else {
            std::mem::replace(&mut *self.registry.write(), registry)
        };
        for column in old_registry.values() {
            if preserved_columns.contains(&(Arc::as_ptr(column) as usize)) {
                continue;
            }
        }
        // Continuing columns already own one retained identity. Once outgoing
        // copied keys are destroyed, their same directory credit covers the
        // replacement keys; the staging duplicate can return its credit.
        drop(old_registry);
        drop(copied_identity_credit);
        drop(preserved_states);
        drop(preserved_columns);
        drop(workspace);
    }

    /// Prepare the complete dependency-closure handoff before the durable
    /// commit. Unaffected columns keep their exact state and declared owners;
    /// their copied identity keys are transiently admitted by the caller.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_received_schema_tables_publication(
        &self,
        mut publication: PreparedVectorPublication,
        affected_tables: &std::collections::HashSet<String>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<PreparedVectorTablePublication> {
        let swap_vector_bytes = publication.memory_swap_vector_bytes();
        self.with_bulk_maintenance(|| {
            let mut workspace = owner.scratch();
            let registry = self.registry.read();
            let key_bytes = registry
                .iter()
                .filter(|(index, _)| !affected_tables.contains(&index.table))
                .fold(0usize, |bytes, (index, _)| {
                    bytes
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len())
                });
            let preserved_count = registry
                .iter()
                .filter(|(index, _)| !affected_tables.contains(&index.table))
                .fold(0usize, |count, (_, column)| {
                    count.saturating_add(column.partitions.read().len())
                });
            workspace.reserve(
                key_bytes.saturating_add(
                    preserved_count
                        .saturating_add(publication.preserved_states.len())
                        .saturating_mul(4 * (std::mem::size_of::<usize>() + 1)),
                ),
            )?;
            for (index, column) in registry.iter() {
                if affected_tables.contains(&index.table) {
                    continue;
                }
                // The copied key is retained under this column's declared
                // directory owner. It must outlive the caller workspace.
                let accountant = column.directory_accounting.lock().accountant.clone();
                let bytes = index
                    .table
                    .len()
                    .saturating_add(index.column.len())
                    .saturating_add(std::mem::size_of::<RegistryIdentityCharge>());
                accountant.try_allocate_for_static(
                    bytes,
                    "vector_index",
                    "prepare_vector_registry_identity",
                    "Reduce vector declaration metadata or raise MEMORY_LIMIT before publication.",
                )?;
                // Identity credit belongs to the replacement allocation too.
                let identity_charge = PaidRetainedMemoryCharge::new(bytes, accountant);
                publication
                    .registry
                    .try_insert(index.clone(), column.clone())?;
                publication.registry.identity_charges = Some(Box::new(RegistryIdentityCharge {
                    charge: identity_charge,
                    next: publication.registry.identity_charges.take(),
                }));
                for state in column.partitions.read().values() {
                    publication
                        .preserved_states
                        .insert(Arc::as_ptr(state) as usize);
                }
            }
            Ok(PreparedVectorTablePublication {
                publication,
                policy_updates: Vec::new(),
                _workspace: workspace,
                swap_vector_bytes,
            })
        })
    }

    /// Internal engine assembly for declaration-only add/drop. Continuing
    /// columns keep their exact sources and standing owners; no body or
    /// partition directory is collected. A recreated table starts a new life.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_received_schema_declarations_publication(
        &self,
        schemas: Vec<(VectorIndexRef, VectorIndexLayout)>,
        affected_tables: &std::collections::HashSet<String>,
        recreated: impl Fn(&str) -> bool,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        mut owner: VectorCopyAdmission,
    ) -> Result<PreparedVectorTablePublication> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        let new_count = {
            let registry = self.registry.read();
            let mut count = 0usize;
            for (index, layout) in &schemas {
                Self::validate_layout(index, layout)?;
                if !affected_tables.contains(&index.table) {
                    return Err(Error::Other(
                        "vector declaration is outside its schema scope".into(),
                    ));
                }
                if let Some(column) = registry.get(index)
                    && !recreated(&index.table)
                {
                    if !column.layout.read().has_same_storage_shape(layout) {
                        return Err(Error::Other(
                            "vector declaration requires a payload rewrite".into(),
                        ));
                    }
                } else {
                    count += 1;
                }
            }
            count
        };
        owner.reserve(
            new_count.saturating_mul(std::mem::size_of::<(VectorIndexRef, VectorIndexLayout)>()),
        )?;
        let mut new_schemas = Vec::with_capacity(new_count);
        if new_schemas.capacity() != new_count {
            return Err(Error::Other("vector declaration capacity mismatch".into()));
        }
        {
            let registry = self.registry.read();
            for (index, layout) in &schemas {
                if !recreated(&index.table) && registry.contains_key(index) {
                    continue;
                }
                owner.reserve(
                    index
                        .table
                        .len()
                        .saturating_add(index.column.len())
                        .saturating_add(layout.estimated_bytes()),
                )?;
                if new_schemas.len() == new_schemas.capacity() {
                    return Err(Error::Other(
                        "vector declaration changed during preparation".into(),
                    ));
                }
                new_schemas.push((index.clone(), layout.clone()));
            }
        }
        if new_schemas.len() != new_count {
            return Err(Error::Other(
                "vector declaration changed during preparation".into(),
            ));
        }
        owner.reserve(
            schemas
                .len()
                .saturating_mul(std::mem::size_of::<PreparedVectorPolicyLayout>()),
        )?;
        let mut policy_updates = Vec::with_capacity(schemas.len());
        if policy_updates.capacity() != schemas.len() {
            return Err(Error::Other(
                "vector policy control capacity mismatch".into(),
            ));
        }
        for (index, layout) in &schemas {
            if recreated(&index.table) || !self.registry.read().contains_key(index) {
                continue;
            }
            owner.reserve(layout.estimated_bytes())?;
            if let Some(update) =
                self.prepare_policy_layout_update(index, layout.clone(), &mut owner)?
            {
                policy_updates.push(update);
            }
        }
        let mut publication = self.prepare_partitioned_received_schema_publication(
            new_schemas,
            Vec::new(),
            declared_owner,
            owner.scratch(),
        )?;
        self.with_bulk_maintenance(|| -> Result<()> {
            let registry = self.registry.read();
            let keep = |index: &VectorIndexRef| {
                !affected_tables.contains(&index.table)
                    || (!recreated(&index.table)
                        && schemas.iter().any(|(desired, _)| desired == index))
            };
            let count = registry.iter().filter(|(index, _)| keep(index)).count();
            owner.reserve(count.saturating_mul(std::mem::size_of::<usize>()))?;
            publication.preserved_columns = Vec::with_capacity(count);
            if publication.preserved_columns.capacity() != count {
                return Err(Error::Other(
                    "vector column identity capacity mismatch".into(),
                ));
            }
            for (index, column) in registry.iter().filter(|(index, _)| keep(index)) {
                if let Some((_, layout)) = schemas.iter().find(|(desired, _)| desired == index)
                    && !column.layout.read().has_same_storage_shape(layout)
                {
                    return Err(Error::Other(
                        "vector declaration changed during preparation".into(),
                    ));
                }
                // The independent copied identity survives under preparation
                // until the outgoing registry's physical keys are destroyed.
                // Then the continuing column's standing identity debit remains.
                owner.reserve(index.table.len().saturating_add(index.column.len()))?;
                // Mark before insertion: a later fallible insertion must not
                // let rollback clear a graph still owned by the live registry.
                publication
                    .preserved_columns
                    .push(Arc::as_ptr(column) as usize);
                publication
                    .registry
                    .try_insert(index.clone(), column.clone())?;
            }
            Ok(())
        })?;
        Ok(PreparedVectorTablePublication {
            publication,
            policy_updates,
            _workspace: owner,
            swap_vector_bytes: 0,
        })
    }

    /// Consume the funded full handoff after the durable dependency-closure
    /// commit. No buckets or keys are allocated here.
    pub fn publish_prepared_received_schema_tables(
        &self,
        mut prepared: PreparedVectorTablePublication,
    ) {
        // Keep the complete preparation owner armed across every policy body
        // and the outgoing registry's physical destruction, including unwind.
        for policy in std::mem::take(&mut prepared.policy_updates) {
            let _ = self.publish_policy_layout_update(policy);
        }
        self.publish_prepared_received_schema(prepared.publication);
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn pause_registry(&self) -> &crate::test_seam::PauseRegistry {
        &self.pause_registry
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_index(
        &self,
        index: VectorIndexRef,
        dimension: usize,
        quantization: VectorQuantization,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<()> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        self.with_bulk_read(|| {
            let column = {
                let mut registry = self.registry.write();
                if let Some(column) = registry.get(&index) { column.clone() } else {
                    let column = Arc::new(VectorColumnState::new(VectorIndexLayout::unpartitioned(
                        dimension, quantization), declared_owner.clone()));
                    let bytes = column.directory_retained_bytes(&index);
                    declared_owner.try_allocate_for_static(bytes, "vector_index", "register_vector_directory",
                        "Reduce vector declaration metadata or raise MEMORY_LIMIT before creating the index.")?;
                    column.install_directory_accounting(bytes);
                    registry.try_insert(index.clone(), column.clone())?;
                    column
                }
            };
            let key = VectorPartitionKey::unpartitioned();
            if let Some(state) = column.partition_state(&key) {
                let _index_guard = state.maintenance.lock();
                let _stack_guard = MaintenanceStackGuard::new(self.id, &index, &key);
            }
            Ok(())
        })
    }

    /// Register or update a manager-resolved declaration. Policy-only changes
    /// retain every partition; a physical-layout change is accepted only when
    /// the column has no retained vector entry.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_index_with_layout(
        &self,
        index: VectorIndexRef,
        layout: VectorIndexLayout,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        Self::validate_layout(&index, &layout)?;
        owner.reserve(
            layout
                .estimated_bytes()
                .saturating_mul(4)
                .saturating_add(
                    index
                        .table
                        .len()
                        .saturating_add(index.column.len())
                        .saturating_mul(4),
                )
                .saturating_add(std::mem::size_of::<VectorColumnState>())
                .saturating_add(IndexState::allocation_bytes())
                .saturating_add(32 * std::mem::size_of::<usize>()),
        )?;
        let paged_counts = self.paged_partition_counts(&index)?;
        self.with_bulk_maintenance(|| {
            let existing = { self.registry.read().get(&index).cloned() };
            let Some(existing) = existing else {
                let column = Arc::new(VectorColumnState::new(layout.clone(), declared_owner.clone()));
                let retained = VectorCopyAdmission::new(declared_owner.clone());
                self.make_column_durable(&index, &column, owner, retained)?;
                let bytes = column.directory_retained_bytes(&index);
                declared_owner.try_allocate_for_static(bytes, "vector_index", "register_vector_directory",
                    "Reduce vector declaration metadata or raise MEMORY_LIMIT before creating the index.")?;
                column.install_directory_accounting(bytes);
                self.registry.write().try_insert(index.clone(), column)?;
                return Ok(None);
            };
            let current = existing.scoped_layout(owner)?;
            if *current == layout {
                return Ok(None);
            }
            if current.has_same_storage_shape(&layout) {
                if let Some(max_partitions) = layout.max_partitions
                    && paged_counts.map_or_else(|| existing.live_or_retained_partition_count(), |counts| counts[3]) > max_partitions as usize
                {
                    return Err(Error::VectorPartitionLimitExceeded {
                        index: index.clone(),
                        max_partitions,
                    });
                }
                let old_layout_bytes = current.estimated_bytes();
                let new_layout_bytes = layout.estimated_bytes();
                let mut retained = existing.directory_accounting.lock();
                let growth = new_layout_bytes.saturating_sub(old_layout_bytes);
                if growth != 0 {
                    retained.accountant.try_allocate_for_static(growth, "vector_index", "register_vector_directory",
                        "Reduce vector declaration metadata or raise MEMORY_LIMIT before updating the index.")?;
                }
                *existing.layout.write() = layout.clone();
                if old_layout_bytes > new_layout_bytes {
                    retained.accountant.release(old_layout_bytes - new_layout_bytes);
                    retained.bytes -= old_layout_bytes - new_layout_bytes;
                } else { retained.bytes += growth; }
                drop(retained);
                return Ok(Some(existing));
            }
            if existing.total_entry_count() != 0 {
                return Err(Error::Other((format!(
                    "cannot reconfigure nonempty vector index {}.{}",
                    index.table, index.column
                )).into()));
            }
            let declared = existing.directory_accounting.lock().accountant.clone();
            let column = Arc::new(VectorColumnState::new(layout.clone(), declared.clone()));
            self.make_column_durable(&index, &column, owner, VectorCopyAdmission::new(declared.clone()))?;
            let bytes = column.directory_retained_bytes(&index);
            declared.try_allocate_for_static(bytes, "vector_index", "replace_vector_directory",
                "Reduce vector declaration metadata or raise MEMORY_LIMIT before replacing the index.")?;
            column.install_directory_accounting(bytes);
            self.registry.write().try_insert(index.clone(), column)?;
            Ok(None)
        })?;

        // The declaration itself is already visible and the exclusive store
        // gate is released. Foreground writes share the read side used below.
        // Only the few partitions carrying an old failure need a
        // directory-derived recheck, and a concurrent retry cannot have its
        // newer mark cleared by this one.
        Ok(())
    }

    /// After a policy-only layout change is visible, clear each recorded
    /// partition maintenance failure the new layout no longer needs. Only the
    /// few partitions carrying a failure need a directory-derived recheck, and
    /// a concurrent retry cannot have its newer mark cleared by this one.
    /// Note a recorded partition failure on the column that owns it now.
    fn note_failed_partition(&self, index: &VectorIndexRef, key: &VectorPartitionKey) {
        let column = self.registry.read().get(index).cloned();
        if let Some(column) = column {
            column.note_failed_partition(key);
        }
    }

    /// Admit a policy-only layout change for a registered index: the same
    /// dimension, quantization and partition key, with any change to search
    /// mode, `AUTO_INDEX_AT`, `MAX_PARTITIONS`, IVF or consolidation. Like
    /// `register_index_with_layout` for such a change, it keeps every
    /// partition and every graph. Everything that can fail happens here; a
    /// caller that must publish after durability applies the result with
    /// [`Self::publish_policy_layout_update`], which cannot fail. `None` means
    /// the index already has this layout.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_policy_layout_update(
        &self,
        index: &VectorIndexRef,
        layout: VectorIndexLayout,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<PreparedVectorPolicyLayout>> {
        Self::validate_layout(index, &layout)?;
        let column = self
            .try_column(index)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        // Compared in place: copying the current declaration would be an
        // unadmitted allocation as wide as its partition key.
        let (unchanged, same_shape, current_bytes) = {
            let current = column.layout.read();
            (
                *current == layout,
                current.has_same_storage_shape(&layout),
                current.estimated_bytes(),
            )
        };
        if unchanged {
            return Ok(None);
        }
        if !same_shape {
            return Err(Error::Other(
                (format!(
                    "vector index {}.{} changes its storage shape, not only its policy",
                    index.table, index.column
                ))
                .into(),
            ));
        }
        if let Some(max_partitions) = layout.max_partitions
            && self.paged_partition_counts(index)?.map_or_else(
                || column.live_or_retained_partition_count(),
                |counts| counts[3],
            ) > max_partitions as usize
        {
            return Err(Error::VectorPartitionLimitExceeded {
                index: index.clone(),
                max_partitions,
            });
        }
        // The prepared change keeps its own copy of the index name until it
        // is published; the caller's copy owner funds it until then.
        owner.reserve(index.table.len().saturating_add(index.column.len()))?;
        let growth = layout.estimated_bytes().saturating_sub(current_bytes);
        let growth = if growth == 0 {
            None
        } else {
            let accountant = column.directory_accounting.lock().accountant.clone();
            accountant.try_allocate_for_static(
                growth,
                "vector_index",
                "register_vector_directory",
                "Reduce vector declaration metadata or raise MEMORY_LIMIT before updating the index.",
            )?;
            Some(PaidRetainedMemoryCharge::new(growth, accountant))
        };
        Ok(Some(PreparedVectorPolicyLayout {
            index: index.clone(),
            column,
            layout,
            growth,
        }))
    }

    /// Apply a prepared policy-only layout change. The column keeps its
    /// partitions; only its declared policy changes. If the index
    /// was replaced since preparation, the prepared change is dropped and
    /// its admitted growth returned.
    pub fn publish_policy_layout_update(&self, prepared: PreparedVectorPolicyLayout) {
        self.with_bulk_maintenance(|| {
            let PreparedVectorPolicyLayout {
                index,
                column,
                layout,
                mut growth,
            } = prepared;
            let still_current = self
                .registry
                .read()
                .get(&index)
                .is_some_and(|live| Arc::ptr_eq(live, &column));
            if !still_current {
                return;
            }
            let old_bytes = column.layout.read().estimated_bytes();
            let new_bytes = layout.estimated_bytes();
            *column.layout.write() = layout;
            let mut retained = column.directory_accounting.lock();
            if let Some(growth) = growth.as_mut() {
                // The admitted growth now belongs to the column's directory.
                retained.bytes += growth.bytes;
                growth.bytes = 0;
            } else if old_bytes > new_bytes {
                retained.accountant.release(old_bytes - new_bytes);
                retained.bytes -= old_bytes - new_bytes;
            }
            drop(retained);
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn validate_layout(index: &VectorIndexRef, layout: &VectorIndexLayout) -> Result<()> {
        match (layout.is_partitioned(), layout.max_partitions) {
            (false, None) => Ok(()),
            (true, Some(max_partitions)) => {
                if max_partitions == 0 {
                    Err(Error::Other((format!(
                        "partitioned vector index {}.{} requires a positive effective MAX_PARTITIONS",
                        index.table, index.column
                    )).into()))
                } else {
                    Ok(())
                }
            }
            (false, Some(_)) => Err(Error::Other(
                (format!(
                    "unpartitioned vector index {}.{} cannot carry MAX_PARTITIONS",
                    index.table, index.column
                ))
                .into(),
            )),
            (true, None) => Err(Error::Other(
                (format!(
                    "partitioned vector index {}.{} requires a positive effective MAX_PARTITIONS",
                    index.table, index.column
                ))
                .into(),
            )),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn validate_partition_key(
        index: &VectorIndexRef,
        layout: &VectorIndexLayout,
        key: &VectorPartitionKey,
    ) -> Result<()> {
        if key.components().len() != layout.partition_key_columns.len() {
            return Err(Error::Other(
                (format!(
                    "vector partition key shape does not match declaration for {}.{}",
                    index.table, index.column
                ))
                .into(),
            ));
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn validate_partition_cap(
        index: &VectorIndexRef,
        column: &VectorColumnState,
        requested: &PartitionKeySet,
    ) -> Result<()> {
        let layout = column.layout();
        let Some(max_partitions) = layout.max_partitions else {
            return Ok(());
        };
        let factory = column.durable_partition_factory.read().clone();
        if let Some(factory) = factory
            && let Some(counts) = factory.catalog_scalar_counts(index)?
        {
            let mut new_count = 0usize;
            for key in requested {
                let occupied = factory
                    .catalog_scalar_occupied(index, key)?
                    .ok_or_else(|| {
                        Error::Other(
                            "paged vector occupancy capability does not match its count capability"
                                .into(),
                        )
                    })?;
                if !occupied {
                    new_count = new_count
                        .checked_add(1)
                        .ok_or_else(|| Error::Other("vector cap count overflow".into()))?;
                }
            }
            if counts[3].saturating_add(new_count) > max_partitions as usize {
                return Err(Error::VectorPartitionLimitExceeded {
                    index: index.clone(),
                    max_partitions,
                });
            }
            return Ok(());
        }
        let new_count = {
            let partitions = column.partitions.read();
            requested
                .iter()
                .filter(|key| {
                    partitions
                        .get(*key)
                        .is_none_or(|state| state.entry_count() == 0 || state.is_historical_only())
                })
                .count()
        };
        if new_count == 0 {
            return Ok(());
        }
        let occupied_count = column.occupied_partition_count();
        if occupied_count.saturating_add(new_count) > max_partitions as usize {
            return Err(Error::VectorPartitionLimitExceeded {
                index: index.clone(),
                max_partitions,
            });
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    /// Whether one route may be retired by a write that holds the
    /// registered-snapshot guard: it has no live vector after the batch, an
    /// untouched route has no pending durability or repair work, and no
    /// registered snapshot sees any of its retained memberships. The hold
    /// test is a fixed number of indexed lookups per snapshot, never a walk
    /// of the route's membership history.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn reclaim_candidate_selected(
        &self,
        partition: &VectorPartitionRef,
        changed_by_batch: bool,
        projected: &HashMap<VectorIndexRef, HashMap<VectorPartitionKey, ProjectedPartitionState>>,
        registered_snapshots: &[SnapshotId],
        owner: &mut VectorCopyAdmission,
    ) -> Result<bool> {
        let Some(column) = self.try_column(&partition.index) else {
            return Ok(false);
        };
        if !column.layout.read().is_partitioned() {
            return Ok(false);
        }
        let Some(state) =
            self.query_partition_state(&partition.index, &partition.partition_key, owner)?
        else {
            return Ok(false);
        };
        let projected_is_empty = if changed_by_batch {
            projected
                .get(&partition.index)
                .and_then(|states| states.get(&partition.partition_key))
                .is_some_and(|state| state.live_count() == 0)
        } else {
            state.vector_count() == 0
        };
        if !projected_is_empty {
            return Ok(false);
        }
        if !changed_by_batch && !state.is_durable() {
            if state.resident_pending_after(None) {
                // The cap-time sweep has no authority to discard an
                // untouched route's pending durability or repair work.
                return Ok(false);
            }
        } else if !changed_by_batch {
            let layout = column.scoped_layout(owner)?;
            let info = Self::partition_info_snapshot(
                &partition.index,
                &partition.partition_key,
                &state,
                &layout,
                owner,
            )?;
            if info.pending_inserts != 0 || info.tombstones != 0 {
                // The cap-time sweep has no authority to discard an
                // untouched route's pending durability or repair work.
                return Ok(false);
            }
        }
        self.note_cap_work(|work| {
            work.hold_lookups
                .fetch_add(registered_snapshots.len() as u64, Ordering::SeqCst);
        });
        let mut streamed_records = 0u64;
        let visible =
            state.visible_at_any_registered(registered_snapshots, owner, &mut streamed_records)?;
        self.note_cap_work(|work| {
            work.hold_streamed_records
                .fetch_add(streamed_records, Ordering::SeqCst);
        });
        Ok(!visible)
    }

    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    /// Resolve only the actual mutation's selected key, preserving reader
    /// control when the complete directory is paged rather than resident.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn query_partition_state(
        &self,
        index: &VectorIndexRef,
        key: &VectorPartitionKey,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorQueryState>> {
        if let Some(selection) = self.capture_paged_partition_sources(index, owner)? {
            return selection.probe(key, owner);
        }
        Ok(self
            .try_partition_state(index, key)
            .map(VectorQueryState::from))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "Atomic batch projection keeps inserts, deletes, moves, snapshots and their owner in one boundary."
    )]
    fn project_partitioned_batch(
        &self,
        deletes: &[PartitionedVectorDelete],
        inserts: &[PartitionedVectorEntry],
        moves: &[PartitionedVectorMove],
        reclaimable_partitions: &HashSet<VectorPartitionRef>,
        registered_snapshots: Option<&[SnapshotId]>,
        enforce_partition_cap: bool,
        owner: &mut VectorCopyAdmission,
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
                &*column.scoped_layout(owner)?,
                &partition.partition_key,
            )?;
        }

        let candidate_indexes = Self::sorted_refs(
            candidate_partitions
                .iter()
                .map(|partition| partition.index.clone()),
        );
        // Only the batch's own routes are projected. The cap count of every
        // other route is the column's maintained occupancy, which this batch
        // cannot change.
        let mut projected =
            HashMap::<VectorIndexRef, HashMap<VectorPartitionKey, ProjectedPartitionState>>::new();
        let mut counted_before = HashMap::<VectorIndexRef, (usize, usize)>::new();
        for index in candidate_indexes {
            let column = self
                .try_column(&index)
                .expect("candidate vector index was validated");
            let occupied = self
                .paged_partition_counts(&index)?
                .map_or_else(|| column.occupied_partition_count(), |counts| counts[3]);
            // Existing batch workspace already quotes these per-index keys
            // and map slots; only the added captured occupancy scalar is new.
            owner.reserve(std::mem::size_of::<usize>())?;
            counted_before.insert(index.clone(), (0, occupied));
            let states = projected.entry(index.clone()).or_default();
            for partition in candidate_partitions
                .iter()
                .filter(|partition| partition.index == index)
            {
                self.note_cap_work(|work| {
                    work.projected_routes.fetch_add(1, Ordering::SeqCst);
                });
                if let Some(state) =
                    self.query_partition_state(&partition.index, &partition.partition_key, owner)?
                {
                    let counts_toward_partition_limit = !state.is_historical_only();
                    if counts_toward_partition_limit && state.entry_count() != 0 {
                        counted_before
                            .get_mut(&index)
                            .expect("projected column counter was seeded")
                            .0 += 1;
                    }
                    let projection = if state.is_durable() {
                        ProjectedPartitionState::from_durable_state(
                            &state,
                            counts_toward_partition_limit,
                            &Self::batch_rows_touching(partition, deletes, inserts, moves),
                            owner,
                        )?
                    } else {
                        ProjectedPartitionState::from_state(&state, counts_toward_partition_limit)
                    };
                    states.insert(partition.partition_key.clone(), projection);
                }
            }
        }

        for delete in deletes {
            if let Some(state) = projected
                .get_mut(&delete.partition.index)
                .and_then(|states| states.get_mut(&delete.partition.partition_key))
            {
                state.delete_live_row(delete.row_id, delete.deleted_tx);
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
                .delete_live_row(row_move.old_row_id, row_move.tx);
            projected
                .get_mut(&row_move.index)
                .expect("move index was validated")
                .entry(row_move.target_partition_key.clone())
                .or_default()
                .insert_moved(row_move.new_row_id, row_move.tx);
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
            for partition in source_candidates.drain() {
                let selected = self.reclaim_candidate_selected(
                    &partition,
                    candidate_partitions.contains(&partition),
                    &projected,
                    registered_snapshots,
                    owner,
                )?;
                if selected {
                    effective_reclaimable_partitions.insert(partition);
                }
            }
        }

        for partition in &effective_reclaimable_partitions {
            let column = self
                .try_column(&partition.index)
                .expect("reclaimable vector index was validated");
            if !column.layout.read().is_partitioned() {
                return Err(Error::Other(
                    (format!(
                        "cannot reclaim the declaration state of unpartitioned vector index {}.{}",
                        partition.index.table, partition.index.column
                    ))
                    .into(),
                ));
            }
            let Some(state) = projected
                .get_mut(&partition.index)
                .and_then(|states| states.get_mut(&partition.partition_key))
            else {
                return Err(Error::Other(
                    (format!(
                        "cannot reclaim an absent partition of vector index {}.{}",
                        partition.index.table, partition.index.column
                    ))
                    .into(),
                ));
            };
            if state.live_count() != 0 {
                return Err(Error::Other(
                    (format!(
                        "cannot reclaim a partition with live rows in vector index {}.{}",
                        partition.index.table, partition.index.column
                    ))
                    .into(),
                ));
            }
            state.retire_maintained_state();
        }

        let projected_count = |index: &VectorIndexRef,
                               projected: &HashMap<
            VectorIndexRef,
            HashMap<VectorPartitionKey, ProjectedPartitionState>,
        >,
                               counted_before: &HashMap<VectorIndexRef, (usize, usize)>|
         -> usize {
            let after = projected.get(index).map_or(0, |states| {
                states
                    .values()
                    .filter(|state| state.counts_toward_partition_limit && state.total_entries != 0)
                    .count()
            });
            let (touched, occupied) = counted_before
                .get(index)
                .copied()
                .expect("projected column counter was seeded");
            occupied.saturating_sub(touched).saturating_add(after)
        };

        // The exact cap edge: only a write that the conservative count would
        // refuse looks for a reclaimable route, and it looks only at the
        // column's counted-empty routes (at most the declared cap), with a
        // fixed number of indexed hold lookups each.
        if enforce_partition_cap && let Some(registered_snapshots) = registered_snapshots {
            for index in Self::sorted_refs(projected.keys().cloned()) {
                let column = self
                    .try_column(&index)
                    .expect("projected vector index was validated");
                let Some(max_partitions) = column.layout.read().max_partitions else {
                    continue;
                };
                if projected_count(&index, &projected, &counted_before) <= max_partitions as usize {
                    continue;
                }
                if let Some(selection) = self.capture_paged_partition_sources(&index, owner)? {
                    let mut traversal = owner.scratch();
                    selection.visit_empty(&mut traversal, |key, state| {
                        if projected_count(&index, &projected, &counted_before) <= max_partitions as usize { return Ok::<_, Error>(false); }
                        self.note_cap_work(|work| { work.cap_edge_route_visits.fetch_add(1, Ordering::SeqCst); });
                        if projected.get(&index).is_some_and(|states| states.contains_key(key)) { return Ok(true); }
                        if state.is_historical_only() || state.entry_count() == 0 || state.vector_count() != 0 {
                            return Err(Error::Other("paged counted-empty vector directory disagrees with its captured source".into()));
                        }
                        let names = index.table.len().checked_add(index.column.len()).ok_or_else(|| Error::Other("cap key quote overflow".into()))?;
                        owner.reserve(key.estimated_bytes().checked_mul(2).and_then(|n| n.checked_add(names.checked_mul(3)?))
                            .and_then(|n| n.checked_add(std::mem::size_of::<ProjectedPartitionState>() + 2 * std::mem::size_of::<VectorPartitionRef>() + 128))
                            .ok_or_else(|| Error::Other("cap selected route quote overflow".into()))?)?;
                        let partition = VectorPartitionRef::new(index.clone(), key.clone());
                        self.note_cap_work(|work| { work.cap_edge_identity_probes.fetch_add(1, Ordering::SeqCst); });
                        if !self.reclaim_candidate_selected(&partition, false, &projected, registered_snapshots, owner)? { return Ok(true); }
                        let mut projection = ProjectedPartitionState::from_entry_count(state.entry_count(), true);
                        projection.retire_maintained_state();
                        projected.get_mut(&index).expect("projected column was seeded").insert(key.clone(), projection);
                        counted_before.get_mut(&index).expect("projected column counter was seeded").0 += 1;
                        effective_reclaimable_partitions.insert(partition);
                        Ok(projected_count(&index, &projected, &counted_before) > max_partitions as usize)
                    })?;
                    continue;
                }
                let candidates_len = column.limit_ledger.empty_counted_len();
                owner.reserve(
                    candidates_len.saturating_mul(
                        std::mem::size_of::<Arc<IndexState>>()
                            .saturating_add(std::mem::size_of::<VectorPartitionRef>())
                            .saturating_add(32),
                    ),
                )?;
                for (candidate, key) in column.limit_ledger.empty_counted() {
                    if projected_count(&index, &projected, &counted_before)
                        <= max_partitions as usize
                    {
                        break;
                    }
                    self.note_cap_work(|work| {
                        work.cap_edge_route_visits.fetch_add(1, Ordering::SeqCst);
                    });
                    if projected
                        .get(&index)
                        .is_some_and(|states| states.contains_key(key.as_ref()))
                    {
                        continue;
                    }
                    // The candidate carries its own key: one map lookup
                    // confirms the route still holds this state.
                    self.note_cap_work(|work| {
                        work.cap_edge_identity_probes.fetch_add(1, Ordering::SeqCst);
                    });
                    let Some(state) = column.partition_state(&key) else {
                        continue;
                    };
                    if !Arc::ptr_eq(&state, &candidate)
                        || state.is_historical_only()
                        || state.entry_count() == 0
                    {
                        continue;
                    }
                    let partition = VectorPartitionRef::new(index.clone(), key.as_ref().clone());
                    if !self.reclaim_candidate_selected(
                        &partition,
                        false,
                        &projected,
                        registered_snapshots,
                        owner,
                    )? {
                        continue;
                    }
                    let mut projection =
                        ProjectedPartitionState::from_entry_count(state.entry_count(), true);
                    projection.retire_maintained_state();
                    projected
                        .entry(index.clone())
                        .or_default()
                        .insert(partition.partition_key.clone(), projection);
                    counted_before
                        .get_mut(&index)
                        .expect("projected column counter was seeded")
                        .0 += 1;
                    effective_reclaimable_partitions.insert(partition);
                }
            }
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
            let count = projected_count(&index, &projected, &counted_before);
            let column = self
                .try_column(&index)
                .expect("touched vector index was validated");
            if enforce_partition_cap
                && let Some(max_partitions) = column.layout.read().max_partitions
                && count > max_partitions as usize
            {
                return Err(Error::VectorPartitionLimitExceeded {
                    index,
                    max_partitions,
                });
            }
            projected_counts.insert(index, count);
        }

        let directory_reservation = {
            let reservation = PreparedDirectoryReservation::empty();
            for index in Self::sorted_refs(touched.iter().map(|partition| partition.index.clone()))
            {
                let column = self
                    .try_column(&index)
                    .expect("touched vector index was validated");
                // Every column declares its directory owner at registration.
                // Received/replayed and memory columns keep that same funder
                // even when the surrounding store has no attached accountant.
                let accountant = column.directory_accounting.lock().accountant.clone();
                let copied_body_bytes = valid_moves
                    .iter()
                    .filter(|row_move| row_move.index == index)
                    .fold(0usize, |bytes, row_move| {
                        let body = self
                            .try_partition_state(&row_move.index, &row_move.source_partition_key)
                            .and_then(|source| {
                                source.with_stored_by_row_id(row_move.old_row_id, |entry| {
                                    if entry.vector.payload_identity().is_none() {
                                        entry.estimated_bytes()
                                    } else {
                                        0
                                    }
                                })
                            })
                            .unwrap_or(0);
                        bytes.saturating_add(body)
                    });
                let bytes = prepared_directory_growth_upper_bound_matching(
                    inserts,
                    &valid_moves,
                    &effective_reclaimable_partitions,
                    &requested,
                    |candidate| candidate == &index,
                )
                .saturating_add(copied_body_bytes);
                reservation.reserve_index(index, accountant, bytes)?;
            }
            reservation
        };

        let mut durable_counts = Vec::new();
        for partition in Self::sorted_partition_refs(touched.iter().cloned()) {
            let Some(column) = self.try_column(&partition.index) else {
                continue;
            };
            if column.durable_partition_factory.read().is_none() {
                continue;
            }
            let Some(projection) = projected
                .get(&partition.index)
                .and_then(|states| states.get(&partition.partition_key))
            else {
                continue;
            };
            let prior_tx = column
                .partition_state(&partition.partition_key)
                .map_or(TxId(0), |state| state.max_tx());
            let batch_tx = deletes
                .iter()
                .filter(|delete| delete.partition == partition)
                .map(|delete| delete.deleted_tx)
                .chain(
                    inserts
                        .iter()
                        .filter(|insert| {
                            insert.entry.index == partition.index
                                && insert.partition_key == partition.partition_key
                        })
                        .flat_map(|insert| [Some(insert.entry.created_tx), insert.entry.deleted_tx])
                        .flatten(),
                )
                .chain(
                    valid_moves
                        .iter()
                        .filter(|row_move| {
                            row_move.index == partition.index
                                && (row_move.source_partition_key == partition.partition_key
                                    || row_move.target_partition_key == partition.partition_key)
                        })
                        .map(|row_move| row_move.tx),
                )
                .max()
                .unwrap_or(TxId(0));
            durable_counts.push(PreparedDurableCounts {
                partition,
                summary: RawVectorMembershipSummary {
                    entries: projection.total_entries,
                    live_entries: projection.live_count(),
                    max_tx: prior_tx.max(batch_tx),
                },
                events: projection.events.clone(),
            });
        }

        Ok(PreparedPartitionedVectorBatch {
            deletes: Vec::new(),
            inserts: Vec::new(),
            moves: valid_moves,
            reclaimable_partitions: effective_reclaimable_partitions,
            touched,
            requested,
            projected_live_or_retained_partitions: projected_counts,
            directory_reservation,
            durable_counts,
            canonical_closures: Vec::new(),
            partition_states: PreparedPartitionStates {
                states: HashMap::new(),
            },
            preparation_owner: None,
        })
    }

    /// Every row a keyed batch reads or writes in one partition.
    fn batch_rows_touching(
        partition: &VectorPartitionRef,
        deletes: &[PartitionedVectorDelete],
        inserts: &[PartitionedVectorEntry],
        moves: &[PartitionedVectorMove],
    ) -> HashSet<RowId> {
        let mut rows = HashSet::new();
        rows.extend(
            deletes
                .iter()
                .filter(|delete| &delete.partition == partition)
                .map(|delete| delete.row_id),
        );
        rows.extend(
            inserts
                .iter()
                .filter(|insert| {
                    insert.entry.index == partition.index
                        && insert.partition_key == partition.partition_key
                })
                .map(|insert| insert.entry.row_id),
        );
        for row_move in moves
            .iter()
            .filter(|row_move| row_move.index == partition.index)
        {
            if row_move.source_partition_key == partition.partition_key {
                rows.insert(row_move.old_row_id);
            }
            if row_move.target_partition_key == partition.partition_key {
                rows.insert(row_move.new_row_id);
            }
        }
        rows
    }

    /// Validate one keyed batch without mutating the store. The engine calls
    /// this before durability and names only source partitions for which its
    /// snapshot gate proves that no active snapshot needs the maintained
    /// source state. Raw historical vector versions are never discarded here.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_partitioned_batch(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        reclaimable_partitions: HashSet<VectorPartitionRef>,
        mut owner: VectorCopyAdmission,
    ) -> Result<PreparedPartitionedVectorBatch> {
        self.with_bulk_read(|| {
            let workspace = self.partitioned_batch_workspace_bytes(
                &deletes,
                &inserts,
                &moves,
                &reclaimable_partitions,
                &owner,
            )?;
            owner.reserve(workspace)?;
            self.ensure_existing_raw_partitions_for_batch(&deletes, &inserts, &moves, &mut owner)?;
            let canonical_closures =
                self.prepare_canonical_closures(&deletes, &moves, &mut owner)?;
            let mut prepared = self.project_partitioned_batch(
                &deletes,
                &inserts,
                &moves,
                &reclaimable_partitions,
                None,
                true,
                &mut owner,
            )?;
            prepared.partition_states =
                self.prepare_partition_states(&prepared.requested, &mut owner)?;
            prepared.canonical_closures = canonical_closures;
            prepared.deletes = deletes;
            prepared.inserts = inserts;
            prepared.preparation_owner = Some(owner);
            Ok(prepared)
        })
    }

    /// Validate one keyed batch while the engine holds its snapshot-removal
    /// guard. A source state is retired only when the final batch leaves it
    /// with no live vector and none of the snapshots captured by that guard
    /// can see one of its retained versions. Raw historical vectors remain
    /// available for later EXACT reads.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_partitioned_batch_with_registered_snapshots(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        registered_snapshots: &[SnapshotId],
        mut owner: VectorCopyAdmission,
    ) -> Result<PreparedPartitionedVectorBatch> {
        self.with_bulk_read(|| {
            let workspace = self.partitioned_batch_workspace_bytes(
                &deletes,
                &inserts,
                &moves,
                &HashSet::new(),
                &owner,
            )?;
            owner.reserve(workspace)?;
            self.ensure_existing_raw_partitions_for_batch(&deletes, &inserts, &moves, &mut owner)?;
            let canonical_closures =
                self.prepare_canonical_closures(&deletes, &moves, &mut owner)?;
            let mut prepared = self.project_partitioned_batch(
                &deletes,
                &inserts,
                &moves,
                &HashSet::new(),
                Some(registered_snapshots),
                true,
                &mut owner,
            )?;
            prepared.partition_states =
                self.prepare_partition_states(&prepared.requested, &mut owner)?;
            prepared.canonical_closures = canonical_closures;
            prepared.deletes = deletes;
            prepared.inserts = inserts;
            prepared.preparation_owner = Some(owner);
            Ok(prepared)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn prepare_canonical_closures(
        &self,
        deletes: &[PartitionedVectorDelete],
        moves: &[PartitionedVectorMove],
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<PreparedCanonicalClosure>> {
        let mut closures = Vec::new();
        for (index, key, row_id, created_tx, lsn, deleted_tx, _closes_body) in deletes
            .iter()
            .map(|d| {
                (
                    &d.partition.index,
                    &d.partition.partition_key,
                    d.row_id,
                    d.before_created_tx,
                    d.before_lsn,
                    d.deleted_tx,
                    true,
                )
            })
            .chain(moves.iter().map(|m| {
                (
                    &m.index,
                    &m.source_partition_key,
                    m.old_row_id,
                    m.before_created_tx,
                    m.before_lsn,
                    m.tx,
                    m.replace_row_version
                        || m.source_partition_key != m.target_partition_key
                        || m.old_row_id != m.new_row_id,
                )
            }))
        {
            self.try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            let source = self.query_partition_state(index, key, owner)?;
            let selected = match source.as_ref() {
                Some(source) => source.try_visible_directory_entry_by_row(
                    row_id,
                    SnapshotId(deleted_tx.0),
                    owner,
                )?,
                None => None,
            };
            if selected
                .as_ref()
                .is_none_or(|entry| entry.created_tx != created_tx || entry.lsn != lsn)
            {
                return Err(Error::VectorSourceIdentityMismatch {
                    index: index.clone(),
                    row_id,
                    expected_created_tx: created_tx,
                    expected_lsn: lsn,
                    actual_created_tx: selected.as_ref().map(|entry| entry.created_tx),
                    actual_lsn: selected.as_ref().map(|entry| entry.lsn),
                });
            }
        }
        for (index, _key, row_id, created_tx, lsn, deleted_tx, closes_body) in deletes
            .iter()
            .map(|d| {
                (
                    &d.partition.index,
                    &d.partition.partition_key,
                    d.row_id,
                    d.before_created_tx,
                    d.before_lsn,
                    d.deleted_tx,
                    true,
                )
            })
            .chain(moves.iter().map(|m| {
                (
                    &m.index,
                    &m.source_partition_key,
                    m.old_row_id,
                    m.before_created_tx,
                    m.before_lsn,
                    m.tx,
                    m.replace_row_version
                        || m.source_partition_key != m.target_partition_key
                        || m.old_row_id != m.new_row_id,
                )
            }))
        {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            if !closes_body {
                continue;
            }
            column.with_partition_candidates_for_row::<Error, ()>(
                row_id,
                |_, state, owner| {
                    // Admission covers each handle and guard before its backing Vec grows.
                    if closures.len() == closures.capacity() {
                        let capacity = closures.capacity().max(1).saturating_mul(2);
                        owner.reserve(
                            (capacity - closures.capacity())
                                .saturating_mul(std::mem::size_of::<PreparedCanonicalClosure>()),
                        )?;
                        closures.reserve_exact(capacity - closures.len());
                    }
                    let residency = state.raw_residency_gate.read_arc_recursive();
                    let matches_cached_identity = {
                        let vectors = state.vectors.read();
                        vectors
                            .by_row
                            .get(&row_id)
                            .into_iter()
                            .flatten()
                            .any(|position| {
                                let entry = &vectors.entries[*position];
                                entry.created_tx == created_tx && entry.lsn == lsn
                            })
                    };
                    // Borrow this row's cache metadata only. The guard is necessary even
                    // without a match: it prevents a dormant cache from loading before
                    // the durable canonical tombstone has been published.
                    closures.push(PreparedCanonicalClosure {
                        state: state.clone(),
                        row_id,
                        created_tx,
                        lsn,
                        deleted_tx,
                        matches_cached_identity,
                        _residency: Some(residency),
                    });
                    Ok(None)
                },
                owner,
            )?;
        }
        Ok(closures)
    }

    /// Price every coexistently held route/header and projection container
    /// before the first route clone. Input payloads are moved, never copied.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn partitioned_batch_workspace_bytes(
        &self,
        deletes: &[PartitionedVectorDelete],
        inserts: &[PartitionedVectorEntry],
        moves: &[PartitionedVectorMove],
        reclaimable: &HashSet<VectorPartitionRef>,
        owner: &VectorCopyAdmission,
    ) -> Result<usize> {
        // A route clone copies its index names; its partition key is shared
        // (`VectorPartitionKey` clones are references), so the key's own
        // bytes are charged once per distinct route below, never per clone.
        let route_bytes = |index: &VectorIndexRef, _key: &VectorPartitionKey| {
            std::mem::size_of::<VectorPartitionRef>()
                .saturating_add(index.table.len())
                .saturating_add(index.column.len())
                .saturating_add(VectorPartitionKey::shared_clone_bytes())
        };
        // Per row (one input item): projection keeps at most one row slot in
        // each of its row maps and event logs, and holds at most a few
        // transient route clones (candidate, touched and requested sets)
        // before deduplication drops them. Per distinct route the projection,
        // cap and reclaim maps keep their own clones (the 32x term below) of
        // one shared key.
        let per_row = |route: usize| {
            route
                .saturating_mul(4)
                .saturating_add(8 * std::mem::size_of::<(RowId, usize)>())
        };
        let mut bytes = std::mem::size_of::<PreparedPartitionedVectorBatch>();
        for delete in deletes {
            bytes = bytes.saturating_add(per_row(route_bytes(
                &delete.partition.index,
                &delete.partition.partition_key,
            )));
        }
        for insert in inserts {
            bytes = bytes.saturating_add(per_row(route_bytes(
                &insert.entry.index,
                &insert.partition_key,
            )));
            if let Some(column) = self.try_column(&insert.entry.index) {
                let layout = column.layout.read();
                // Quantized publication constructs a new representation while
                // the caller's F32 input is still held. F32 moves its buffer.
                if layout.quantization != VectorQuantization::F32 {
                    bytes = bytes.saturating_add(crate::quantized::stored_vector_resident_bytes(
                        layout.dimension,
                        layout.quantization,
                    ));
                }
            }
        }
        for row_move in moves {
            bytes = bytes
                .saturating_add(per_row(route_bytes(
                    &row_move.index,
                    &row_move.source_partition_key,
                )))
                .saturating_add(per_row(route_bytes(
                    &row_move.index,
                    &row_move.target_partition_key,
                )));
        }
        for partition in reclaimable {
            bytes = bytes.saturating_add(
                route_bytes(&partition.index, &partition.partition_key).saturating_mul(32),
            );
        }
        // Projection covers only the batch's own routes; the cap count of
        // every other route is the column's maintained occupancy. The cap
        // edge prices its counted-empty candidates itself before it looks.
        let count = deletes
            .len()
            .checked_add(inserts.len())
            .and_then(|count| {
                moves
                    .len()
                    .checked_mul(2)
                    .and_then(|moves| count.checked_add(moves))
            })
            .and_then(|count| count.checked_add(reclaimable.len()))
            .ok_or_else(|| Error::Other("vector route count overflow".into()))?;
        let mut scratch = owner.scratch();
        scratch.reserve(
            std::alloc::Layout::array::<(&VectorIndexRef, &VectorPartitionKey)>(count)
                .map_err(|_| Error::Other("vector route layout overflow".into()))?
                .size(),
        )?;
        let mut routes = Vec::<(&VectorIndexRef, &VectorPartitionKey)>::with_capacity(count);
        routes.extend(
            deletes
                .iter()
                .map(|entry| (&entry.partition.index, &entry.partition.partition_key)),
        );
        routes.extend(
            inserts
                .iter()
                .map(|entry| (&entry.entry.index, &entry.partition_key)),
        );
        for entry in moves {
            routes.push((&entry.index, &entry.source_partition_key));
            routes.push((&entry.index, &entry.target_partition_key));
        }
        routes.extend(
            reclaimable
                .iter()
                .map(|entry| (&entry.index, &entry.partition_key)),
        );
        routes.sort_unstable_by(|(left, left_key), (right, right_key)| {
            (&left.table, &left.column, left_key).cmp(&(&right.table, &right.column, right_key))
        });
        routes.dedup();
        for (index, key) in &routes {
            bytes = bytes
                .saturating_add(route_bytes(index, key).saturating_mul(32))
                .saturating_add(key.estimated_bytes());
            // A durable projection probes only touched row identities; it
            // never builds a map proportional to durable history.
            if let Some(state) = self.try_partition_state(index, key)
                && !state.is_durable()
            {
                bytes = bytes.saturating_add(
                    state
                        .entry_count()
                        .saturating_mul(16 * std::mem::size_of::<(RowId, TxId)>()),
                );
            }
        }
        Ok(bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn ensure_existing_raw_partitions_for_batch(
        &self,
        deletes: &[PartitionedVectorDelete],
        inserts: &[PartitionedVectorEntry],
        moves: &[PartitionedVectorMove],
        owner: &mut VectorCopyAdmission,
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
                if !state.is_durable() {
                    state.ensure_raw_vectors_loaded(owner)?;
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
        mut prepared: PreparedPartitionedVectorBatch,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
    ) -> PreparedPartitionedVectorPublication {
        let _preparation_owner = prepared
            .preparation_owner
            .take()
            .expect("prepared batch owns its unique admission");
        let mut directory_reservations = prepared.directory_reservation.take();
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
                        column.partition_state(key).is_some_and(|state| {
                            state.entry_count() != 0 || !column.layout.read().is_partitioned()
                        }) && !column.partition_is_historical_only(key)
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
                    directory_reservation: _,
                    durable_counts,
                    mut canonical_closures,
                    partition_states: _,
                    preparation_owner: _preparation_owner,
                } = pending
                    .take()
                    .expect("prepared vector batch publishes once");
                self.apply_admitted_partitioned_changes(
                    deletes,
                    inserts,
                    moves,
                    lsn,
                    accountant,
                    &mut directory_reservations,
                    &mut canonical_closures,
                );
                for closure in &canonical_closures {
                    closure.publish();
                }
                self.install_durable_counts(&durable_counts);
            });
            // Publish the matching retained-directory charge before releasing
            // the bulk read guard. Reclamation takes the write side of this
            // gate and must never observe the grown directory with its old
            // charge.
            self.settle_directory_accounting_for_indexes(
                &touched_indexes,
                &mut directory_reservations,
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
                directory_reservation: _,
                durable_counts,
                mut canonical_closures,
                partition_states,
                preparation_owner: _preparation_owner,
            } = prepared;
            self.publish_partition_states(partition_states, &requested);
            let ordered = Self::sorted_partition_refs(touched);
            self.with_partition_set_maintenance(&ordered, || {
                self.apply_admitted_partitioned_changes(
                    deletes,
                    inserts,
                    moves,
                    lsn,
                    accountant,
                    &mut directory_reservations,
                    &mut canonical_closures,
                );
                for closure in &canonical_closures {
                    closure.publish();
                }
                self.install_durable_counts(&durable_counts);

                // Retirement is keyed: each retired route is marked
                // historical, and removed only if it retains no entry at all.
                // A route with no live vector maps no current row, so the
                // row maps need no rebuild.
                for partition in Self::sorted_partition_refs(reclaimable_partitions) {
                    let Some(column) = self.try_column(&partition.index) else {
                        continue;
                    };
                    if let Some(state) = column.partition_state(&partition.partition_key) {
                        debug_assert_eq!(state.vector_count(), 0);
                        state.mark_historical_only();
                    }
                    column.remove_empty_partition(&partition.partition_key);
                }
            });
            self.settle_directory_accounting_for_indexes(
                &touched_indexes,
                &mut directory_reservations,
            );
        });
        PreparedPartitionedVectorPublication
    }

    fn settle_directory_accounting_for_indexes(
        &self,
        indexes: &[VectorIndexRef],
        reservations: &mut PreparedDirectoryCharges,
    ) {
        for index in indexes {
            if let Some(reservation) = reservations.get_mut(index)
                && let Some(column) = self.try_column(index)
            {
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

    /// Build every absent state and its durable sources before publication.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn prepare_partition_states(
        &self,
        requested: &HashMap<VectorIndexRef, PartitionKeySet>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<PreparedPartitionStates> {
        owner.reserve(requested.len().saturating_mul(
            4 * std::mem::size_of::<(
                VectorIndexRef,
                BTreeMap<VectorPartitionKey, Arc<IndexState>>,
            )>(),
        ))?;
        let mut prepared = HashMap::with_capacity(requested.len());
        for (index, keys) in requested {
            let Some(column) = self.try_column(index) else {
                continue;
            };
            let layout = column.layout.read();
            let factory = column.durable_partition_factory.read().clone();
            let existing = column.partitions.read();
            let mut states = BTreeMap::new();
            for key in keys {
                if existing.contains_key(key) {
                    continue;
                }
                owner.reserve(
                    IndexState::allocation_bytes()
                        .saturating_add(8 * std::mem::size_of::<usize>())
                        .saturating_add(3 * std::mem::size_of::<AtomicUsize>())
                        .saturating_add(4 * key.estimated_bytes())
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len()),
                )?;
                let state = Arc::new(IndexState::new(layout.dimension, layout.quantization));
                if let Some(factory) = factory.as_ref() {
                    let partition = VectorPartitionRef::new(index.clone(), key.clone());
                    let (memberships, loader) = factory.create(
                        &partition,
                        owner.scratch(),
                        self.retained_column_copy_owner(index, owner)?,
                    )?;
                    state.install_durable_source(
                        memberships,
                        loader,
                        RawVectorMembershipSummary::default(),
                        self.raw_payload_bytes.clone(),
                        self.raw_payload_header_bytes.clone(),
                    );
                }
                states.insert(key.clone(), state);
            }
            if !states.is_empty() {
                owner.reserve(index.table.len().saturating_add(index.column.len()))?;
                prepared.insert(index.clone(), states);
            }
        }
        Ok(PreparedPartitionStates { states: prepared })
    }

    fn publish_partition_states(
        &self,
        prepared: PreparedPartitionStates,
        requested: &HashMap<VectorIndexRef, PartitionKeySet>,
    ) {
        for (index, states) in prepared.states {
            if let Some(column) = self.try_column(&index) {
                let mut partitions = column.partitions.write();
                for (key, state) in states {
                    if let std::collections::btree_map::Entry::Vacant(slot) = partitions.entry(key)
                    {
                        column.attach_state(slot.key(), &state);
                        slot.insert(state);
                    }
                }
            }
        }
        for (index, keys) in requested {
            if let Some(column) = self.try_column(index) {
                column.reactivate_partitions(keys);
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn with_admitted_partition_states<R>(
        &self,
        touched: &HashSet<VectorPartitionRef>,
        requested: &HashMap<VectorIndexRef, PartitionKeySet>,
        owner: &mut VectorCopyAdmission,
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
            let prepared = self.prepare_partition_states(requested, owner)?;
            self.publish_partition_states(prepared, requested);
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

    /// Filter borrowed names before creating an owned route inventory. Each
    /// capacity growth and name clone is admitted before allocation, and the
    /// unstable sort requires no additional sorting workspace.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn admitted_sorted_refs(
        &self,
        keep: impl Fn(&VectorIndexRef) -> bool,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorIndexRef>> {
        let registry = self.registry.read();
        let mut refs = Vec::<VectorIndexRef>::new();
        for index in registry.keys().filter(|index| keep(index)) {
            if refs.len() == refs.capacity() {
                let capacity = refs.capacity().saturating_mul(2).max(4);
                owner.reserve(capacity.saturating_mul(std::mem::size_of::<VectorIndexRef>()))?;
                refs.reserve_exact(capacity.saturating_sub(refs.len()));
            }
            owner.reserve(index.table.len().saturating_add(index.column.len()))?;
            refs.push(index.clone());
        }
        refs.sort_unstable_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
        Ok(refs)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn scoped_sorted_refs(
        &self,
        keep: impl Fn(&VectorIndexRef) -> bool,
        owner: &mut VectorCopyAdmission,
    ) -> Result<ScopedVectorCopy<Vec<VectorIndexRef>>> {
        ScopedVectorCopy::new(owner, |scratch| self.admitted_sorted_refs(keep, scratch))
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
        self.registry.assert_not_renaming();
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
        self.registry.assert_not_renaming();
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_or_reconfigure_empty_index(
        &self,
        index: VectorIndexRef,
        dimension: usize,
        quantization: VectorQuantization,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<()> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

        let lock_index = index.clone();
        self.with_index_maintenance(&lock_index, || {
            let mut registry = self.registry.write();
            match registry.get(&index) {
                Some(column) if column.total_entry_count() != 0 => {}
                Some(column) if column.layout().dimension == dimension => {}
                existing => {
                    let declared_owner = existing.map(|column| column.directory_accounting.lock().accountant.clone())
                        .unwrap_or_else(|| declared_owner.clone());
                    let column = Arc::new(VectorColumnState::new(VectorIndexLayout::unpartitioned(
                        dimension, quantization), declared_owner.clone()));
                    let bytes = column.directory_retained_bytes(&index);
                    declared_owner.try_allocate_for_static(bytes, "vector_index", "register_vector_directory",
                        "Reduce vector declaration metadata or raise MEMORY_LIMIT before replacing the index.")?;
                    column.install_directory_accounting(bytes);
                    registry.try_insert(index, column)?;
                }
            }
            Ok(())
        })
    }

    /// Remove a registry identity without reading or copying any raw body.
    /// Safe after durable commit and from cleanup: no admission can fail.
    pub fn teardown_index_without_copy(&self, index: &VectorIndexRef) -> VectorTeardownReceipt {
        self.with_bulk_maintenance(|| {
            let mut receipt = VectorTeardownReceipt::default();
            if let Some(column) = self.registry.write().remove(index) {
                receipt.release_column(&column);
            }
            receipt
        })
    }

    pub fn teardown_table_without_copy(&self, table: &str) -> VectorTeardownReceipt {
        self.with_bulk_maintenance(|| {
            let mut receipt = VectorTeardownReceipt::default();
            let mut registry = self.registry.write();
            for (_, column) in registry.extract_if(|index, _| index.table == table) {
                receipt.release_column(&column);
            }
            registry.release_empty_buckets();
            receipt
        })
    }

    pub fn teardown_all_without_copy(&self) -> VectorTeardownReceipt {
        self.with_bulk_maintenance(|| {
            let mut receipt = VectorTeardownReceipt::default();
            let mut registry = self.registry.write();
            for (_, column) in registry.drain() {
                receipt.release_column(&column);
            }
            registry.release_empty_buckets();
            receipt
        })
    }

    /// Remove one vector identity and return every raw entry that was owned by
    /// it. The store releases graph reservations here. The engine owns the
    /// separate admission charge for raw vector copies, and must settle that
    /// charge from these entries only after durable DDL publication succeeds.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn deregister_index(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_maintenance(|| {
            let mut entries = Vec::new();
            if let Some(column) = self.try_column(index) {
                for state in column.partitions.read().values() {
                    if state.raw_accountant.read().is_none() {
                        entries.extend(state.all_entries(index, owner)?);
                    }
                }
            }
            if let Some(column) = self.registry.write().remove(index) {
                for state in column.partitions.read().values() {
                    state.release_accounted_raw_residency_for_removal();
                }
                Ok(entries)
            } else {
                Ok(Vec::new())
            }
        })
    }

    /// Table-level counterpart of [`Self::deregister_index`]. Returning the
    /// original index identity with each entry lets the engine release exactly
    /// the charge it admitted, without treating graph bytes as raw-vector
    /// bytes a second time.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn deregister_table(
        &self,
        table: &str,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorEntry>> {
        let mut route_copy = owner.local_scratch();
        self.with_bulk_read(|| {
            let keys = self.scoped_sorted_refs(|index| index.table == table, owner)?;
            let partitions = {
                let registry = self.registry.read();
                let mut partitions = Vec::<VectorPartitionRef>::new();
                for index in &keys {
                    let Some(column) = registry.get(index) else {
                        continue;
                    };
                    let states = column.partitions.read();
                    for partition_key in states.keys() {
                        if partitions.len() == partitions.capacity() {
                            let capacity = partitions.capacity().saturating_mul(2).max(4);
                            route_copy.as_mut().unwrap_or(owner).reserve(
                                capacity.saturating_mul(std::mem::size_of::<VectorPartitionRef>()),
                            )?;
                            partitions.reserve_exact(capacity.saturating_sub(partitions.len()));
                        }
                        // The ordered lock stack also owns two route copies,
                        // and its debug teardown can create one comparison
                        // copy. Its geometric vector capacity is covered too.
                        let route_bytes = partition_key
                            .estimated_bytes()
                            .saturating_add(index.table.len())
                            .saturating_add(index.column.len())
                            .saturating_add(std::mem::size_of::<(
                                StoreId,
                                VectorIndexRef,
                                VectorPartitionKey,
                            )>());
                        route_copy
                            .as_mut()
                            .unwrap_or(owner)
                            .reserve(route_bytes.saturating_mul(8))?;
                        partitions.push(VectorPartitionRef::new(
                            index.clone(),
                            partition_key.clone(),
                        ));
                    }
                }
                partitions.sort_unstable_by(|a, b| {
                    a.index
                        .table
                        .cmp(&b.index.table)
                        .then(a.index.column.cmp(&b.index.column))
                        .then(a.partition_key.cmp(&b.partition_key))
                });
                partitions
            };
            route_copy.as_mut().unwrap_or(owner).reserve(
                keys.len()
                    .saturating_mul(
                        std::mem::size_of::<(VectorIndexRef, Arc<VectorColumnState>)>(),
                    ),
            )?;
            self.with_partition_set_maintenance(&partitions, || {
                let mut entries = Vec::new();
                for index in &keys {
                    if let Some(column) = self.try_column(index) {
                        for state in column.partitions.read().values() {
                            if state.raw_accountant.read().is_none() {
                                entries.extend(state.all_entries(index, owner)?);
                            }
                        }
                    }
                }
                let mut removed = Vec::with_capacity(keys.len());
                let mut registry = self.registry.write();
                for index in keys.into_iter() {
                    if let Some(column) = registry.remove(&index) {
                        removed.push((index, column));
                    }
                }
                drop(registry);
                for (_index, column) in removed {
                    for state in column.partitions.read().values() {
                        state.release_accounted_raw_residency_for_removal();
                    }
                }
                Ok(entries)
            })
        })
    }

    /// Prepare all rename copies before the enclosing durable transaction.
    /// The returned token holds the registry and bulk gates: until it is
    /// published or dropped, the caller must not enter another vector operation.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_index_rename<'a>(
        &'a self,
        old: &VectorIndexRef,
        new: &VectorIndexRef,
        owner: VectorCopyAdmission,
    ) -> Result<PreparedVectorIndexRename<'a>> {
        self.prepare_index_rename_inner(old, new, None, owner)
    }

    /// Prepare the explicitly named destination removal and source rename as
    /// one publication. The existing destination remains intact on refusal.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_index_replacement_rename<'a>(
        &'a self,
        old: &VectorIndexRef,
        new: &VectorIndexRef,
        replaced: &VectorIndexRef,
        owner: VectorCopyAdmission,
    ) -> Result<PreparedVectorIndexRename<'a>> {
        if replaced != new || old == new {
            return Err(Error::Other(
                "replacement must name the distinct rename destination".into(),
            ));
        }
        self.prepare_index_rename_inner(old, new, Some(replaced), owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn prepare_index_rename_inner<'a>(
        &'a self,
        old: &VectorIndexRef,
        new: &VectorIndexRef,
        replaced: Option<&VectorIndexRef>,
        mut owner: VectorCopyAdmission,
    ) -> Result<PreparedVectorIndexRename<'a>> {
        self.registry.assert_not_renaming();
        let bulk = self.bulk_gate.write();
        let registry = self.registry.write();
        if replaced.is_none() && registry.contains_key(new) {
            return Err(Error::Other(
                (format!("vector index already exists: {}.{}", new.table, new.column)).into(),
            ));
        }
        let displaced = replaced.and_then(|index| registry.get(index)).cloned();
        let column = registry
            .get(old)
            .ok_or_else(|| Error::UnknownVectorIndex { index: old.clone() })?;
        let retained_accountant = column.directory_accounting.lock().accountant.clone();
        let partitions = column.partitions.read();
        // Prepare an entire replacement registry so publication only swaps
        // ownership. Its copied keys and conservative bucket capacity are
        // admitted before any allocation, including HashMap growth.
        let registry_bytes = registry
            .iter()
            .fold(
                new.table.len().saturating_add(new.column.len()),
                |bytes, (index, _)| {
                    bytes
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len())
                },
            )
            .saturating_add(
                registry
                    .len()
                    .saturating_add(1)
                    .saturating_mul(4)
                    .saturating_mul(
                        std::mem::size_of::<(VectorIndexRef, Arc<VectorColumnState>)>()
                            .saturating_add(std::mem::size_of::<usize>()),
                    ),
            );
        let directory_identity_bytes = registry
            .keys()
            .filter(|index| replaced != Some(*index) || *index == old)
            .fold(0usize, |bytes, index| {
                let target = if index == old { new } else { index };
                bytes
                    .saturating_add(target.table.len())
                    .saturating_add(target.column.len())
            });
        owner.reserve(
            registry_bytes
                .saturating_add(directory_identity_bytes)
                .saturating_add(
                    partitions
                        .len()
                        .saturating_mul(std::mem::size_of::<PreparedPartitionRename>()),
                )
                .saturating_add(
                    registry
                        .len()
                        .saturating_mul(std::mem::size_of::<PreparedRegistryDirectoryCharge>()),
                ),
        )?;
        let mut registry_directories = Vec::with_capacity(registry.len());
        for (index, existing) in registry.iter() {
            if replaced == Some(index) && index != old {
                continue;
            }
            let renamed = index == old;
            let target = if renamed { new } else { index };
            let registry_bytes = target.table.len().saturating_add(target.column.len());
            let name_growth = target
                .table
                .capacity()
                .saturating_add(target.column.capacity())
                .saturating_sub(
                    index
                        .table
                        .capacity()
                        .saturating_add(index.column.capacity()),
                );
            let bytes = registry_bytes.saturating_add(name_growth);
            let accountant = existing.directory_accounting.lock().accountant.clone();
            accountant.try_allocate_for_static(bytes, "vector_index", "prepare_vector_directory_growth",
                "Reduce vector declaration metadata or raise MEMORY_LIMIT before renaming the index.")?;
            registry_directories.push(PreparedRegistryDirectoryCharge {
                column: existing.clone(),
                index: target.clone(),
                charge: PaidRetainedMemoryCharge::new(bytes, accountant),
            });
        }
        let mut sources = Vec::with_capacity(partitions.len());
        for state in partitions.values() {
            let source = state.durable_membership_source();
            let loader = state.raw_loader.read().clone();
            // An installed source must supply a prepared replacement. An
            // unsupported source refuses instead of silently keeping its old route.
            let source = source
                .map(|source| {
                    source
                        .for_renamed_index(
                            new,
                            &mut owner,
                            VectorCopyAdmission::new(retained_accountant.clone()),
                        )
                        .map(NativeSourceArc::from)
                })
                .transpose()?;
            let loader = loader
                .map(|loader| {
                    loader
                        .for_renamed_index(
                            new,
                            &mut owner,
                            VectorCopyAdmission::new(retained_accountant.clone()),
                        )
                        .map(NativeSourceArc::from)
                })
                .transpose()?;
            sources.push(PreparedPartitionRename {
                state: state.clone(),
                source,
                loader,
            });
        }
        let reverse = column
            .durable_reverse_memberships
            .read()
            .as_ref()
            .map(|source| {
                source
                    .for_renamed_index(
                        new,
                        &mut owner,
                        VectorCopyAdmission::new(retained_accountant.clone()),
                    )
                    .map(NativeSourceArc::from)
            })
            .transpose()?;
        let column = column.clone();
        drop(partitions);
        let mut replacement_registry = VectorRegistry::new();
        for (index, existing) in registry.iter() {
            if index != old && replaced != Some(index) {
                replacement_registry.try_insert(index.clone(), existing.clone())?;
            }
        }
        replacement_registry.try_insert(new.clone(), column.clone())?;
        Ok(PreparedVectorIndexRename {
            _rename_guard: self.registry.rename_guard(),
            registry,
            _bulk: bulk,
            replacement_registry,
            column,
            sources,
            reverse,
            registry_directories,
            owner,
            displaced,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn state(&self, index: &VectorIndexRef) -> Result<Arc<IndexState>> {
        self.partition_state(index, &VectorPartitionKey::unpartitioned())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn index_layout(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<VectorIndexLayout> {
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            column.admitted_layout(owner)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn scoped_index_layout(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<ScopedVectorCopy<VectorIndexLayout>> {
        crate::scoped_index_layout(self, index, owner)
    }

    /// Inspect scalar layout facts in place. The callback must not re-enter
    /// vector source work or mutate the layout while its read guard is held.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn with_index_layout<T>(
        &self,
        index: &VectorIndexRef,
        inspect: impl FnOnce(&VectorIndexLayout) -> T,
    ) -> Result<T> {
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            let layout = column.layout.read();
            Ok(inspect(&layout))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn partition_keys(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorPartitionKey>> {
        if let Some(selection) = self.capture_paged_partition_sources(index, owner)? {
            let layout = self.scoped_index_layout(index, owner)?;
            if selection.is_empty() && !layout.is_partitioned() {
                // A declared empty unpartitioned column has a real metadata
                // scaffold even though its authenticated directory has no rows.
                owner.reserve(std::mem::size_of::<VectorPartitionKey>())?;
                return Ok(vec![VectorPartitionKey::unpartitioned()]);
            }
            let count = selection.counts()[3];
            owner.reserve(
                count
                    .checked_mul(std::mem::size_of::<VectorPartitionKey>())
                    .ok_or_else(|| Error::Other("vector key output size overflow".into()))?,
            )?;
            let mut keys = Vec::with_capacity(count);
            let mut traversal = owner.scratch();
            selection.visit(&mut traversal, |key, state| {
                if !state.is_historical_only() {
                    if keys.len() == count {
                        return Err(Error::Other(
                            "working vector key count disagrees with its capture".into(),
                        ));
                    }
                    owner.reserve(
                        key.estimated_bytes()
                            .saturating_sub(std::mem::size_of::<VectorPartitionKey>()),
                    )?;
                    keys.push(key.clone());
                }
                Ok::<(), Error>(())
            })?;
            if keys.len() != count {
                return Err(Error::Other(
                    "working vector key count disagrees with its capture".into(),
                ));
            }
            return Ok(keys);
        }
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            let partitions = column.partitions.read();
            let selected = |state: &Arc<IndexState>| !state.is_historical_only();
            let count = partitions.values().filter(|state| selected(state)).count();
            let bytes = partitions.iter().filter(|(_, state)| selected(state)).fold(
                count.saturating_mul(std::mem::size_of::<VectorPartitionKey>()),
                |bytes, (key, _)| bytes.saturating_add(key.estimated_bytes()),
            );
            owner.reserve(bytes)?;
            let mut keys = Vec::with_capacity(count);
            keys.extend(
                partitions
                    .iter()
                    .filter(|(_, state)| selected(state))
                    .map(|(key, _)| key.clone()),
            );
            Ok(keys)
        })
    }

    /// Every stored typed key, including retired graph states that retain raw
    /// vectors solely for historical EXACT reads. Ordinary inspection and
    /// live partition counts use `partition_keys` instead.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn partition_keys_including_historical(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorPartitionKey>> {
        if let Some(selection) = self.capture_paged_partition_sources(index, owner)? {
            let layout = self.scoped_index_layout(index, owner)?;
            if selection.is_empty() && !layout.is_partitioned() {
                // A declared empty unpartitioned column has a real metadata
                // scaffold even though its authenticated directory has no rows.
                owner.reserve(std::mem::size_of::<VectorPartitionKey>())?;
                return Ok(vec![VectorPartitionKey::unpartitioned()]);
            }
            let count = selection.counts()[0];
            owner.reserve(
                count
                    .checked_mul(std::mem::size_of::<VectorPartitionKey>())
                    .ok_or_else(|| Error::Other("vector key output size overflow".into()))?,
            )?;
            let mut keys = Vec::with_capacity(count);
            let mut traversal = owner.scratch();
            selection.visit(&mut traversal, |key, _state| {
                if true {
                    if keys.len() == count {
                        return Err(Error::Other(
                            "working vector key count disagrees with its capture".into(),
                        ));
                    }
                    owner.reserve(
                        key.estimated_bytes()
                            .saturating_sub(std::mem::size_of::<VectorPartitionKey>()),
                    )?;
                    keys.push(key.clone());
                }
                Ok::<(), Error>(())
            })?;
            if keys.len() != count {
                return Err(Error::Other(
                    "working vector key count disagrees with its capture".into(),
                ));
            }
            return Ok(keys);
        }
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            let partitions = column.partitions.read();
            let selected = |_state: &Arc<IndexState>| true;
            let count = partitions.values().filter(|state| selected(state)).count();
            let bytes = partitions.iter().filter(|(_, state)| selected(state)).fold(
                count.saturating_mul(std::mem::size_of::<VectorPartitionKey>()),
                |bytes, (key, _)| bytes.saturating_add(key.estimated_bytes()),
            );
            owner.reserve(bytes)?;
            let mut keys = Vec::with_capacity(count);
            keys.extend(
                partitions
                    .iter()
                    .filter(|(_, state)| selected(state))
                    .map(|(key, _)| key.clone()),
            );
            Ok(keys)
        })
    }

    /// Visit a complete, stable snapshot of this column's active and
    /// historical-only raw-vector sources without first allocating an
    /// uncharged key list. The borrowed view exposes the exact source count
    /// and then visits each typed key in stable order; callers can admit
    /// capacity once, charge each key, cancel between sources, and clone only
    /// state handles they will pin after this call returns.
    #[doc(hidden)]
    #[doc(hidden)]
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn paged_partition_counts(&self, index: &VectorIndexRef) -> Result<Option<[usize; 6]>> {
        let Some(column) = self.try_column(index) else {
            return Ok(None);
        };
        let factory = column.durable_partition_factory.read().clone();
        drop(column);
        match factory {
            Some(factory) => factory.catalog_scalar_counts(index),
            None => Ok(None),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn query_partition_source_count(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<usize> {
        if let Some(selection) = self.capture_paged_partition_sources(index, owner)? {
            return Ok(selection.len());
        }
        self.with_partition_sources(index, |sources| Ok::<_, Error>(sources.len()))
    }
    #[doc(hidden)]
    pub fn visit_query_partition_sources<E: From<Error>>(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
        mut visit: impl FnMut(
            &VectorPartitionKey,
            VectorQueryState,
            &mut VectorCopyAdmission,
        ) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if let Some(selection) = self
            .capture_paged_partition_sources(index, owner)
            .map_err(E::from)?
        {
            let mut traversal = owner.scratch();
            return selection.visit(&mut traversal, |key, state| visit(key, state, owner));
        }
        self.with_partition_sources(index, |sources| {
            sources.visit(|key, state| visit(key, VectorQueryState::from(state.clone()), owner))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn capture_paged_partition_sources(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<PagedVectorSourceSelection>> {
        let Some(column) = self.try_column(index) else {
            return Err(Error::UnknownVectorIndex {
                index: index.clone(),
            });
        };
        let factory = column.durable_partition_factory.read().clone();
        let Some(factory) = factory else {
            return Ok(None);
        };
        let (dimension, quantization, partitioned) = {
            let layout = column.layout.read();
            (
                layout.dimension,
                layout.quantization,
                layout.is_partitioned(),
            )
        };
        drop(column);
        let Some(catalog) = factory.capture_catalog(index, owner)? else {
            return Ok(None);
        };
        Ok(Some(PagedVectorSourceSelection {
            catalog,
            dimension,
            quantization,
            partitioned,
            payload: self.raw_payload_bytes.clone(),
            payload_headers: self.raw_payload_header_bytes.clone(),
        }))
    }

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

    /// Apply already-classified scalar marks only to the current registered
    /// source, without allocating or cloning its typed partition key. Each
    /// `None` leaves its field unchanged; this adapter cannot clear marks.
    /// Intentional cancellation must be omitted from `failure`, preserving an
    /// earlier actionable failure as in `record_partition_maintenance_error`.
    ///
    /// Call after dropping all source views and outside any already-held
    /// partition-maintenance or publication closure. This callback-free path
    /// takes the ordinary bulk, maintenance, registry and partition guards,
    /// but does not enter the callback-bearing maintenance stack.
    #[doc(hidden)]
    pub fn apply_partition_source_maintenance_metadata(
        &self,
        index: &VectorIndexRef,
        expected_state: &Arc<IndexState>,
        failure: Option<VectorMaintenanceFailureDetails>,
    ) -> bool {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return false;
            };
            let _maintenance = expected_state.maintenance.lock();
            let registry = self.registry.read();
            if !registry
                .get(index)
                .is_some_and(|current| Arc::ptr_eq(current, &column))
            {
                return false;
            }
            let partitions = column.partitions.read();
            let Some(expected_key) = partitions
                .iter()
                .find(|(_, current)| Arc::ptr_eq(current, expected_state))
                .map(|(key, _)| key.clone())
            else {
                return false;
            };
            if let Some(failure) = failure {
                expected_state.record_maintenance_failure(failure);
            }
            drop(partitions);
            if failure.is_some() {
                column.note_failed_partition(&expected_key);
            }
            true
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn current_partition_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorPartitionKey>> {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(None);
            };
            let reverse = column.durable_reverse_memberships.read().clone();
            if let Some(reverse) = reverse {
                return reverse.visible_partition_admitted(
                    row_id,
                    SnapshotId(u64::MAX),
                    &mut || true,
                    owner,
                );
            }
            let key = column.row_to_partition.read().get(&row_id).cloned();
            key.map(|key| {
                owner.reserve(key.estimated_bytes())?;
                Ok((*key).clone())
            })
            .transpose()
        })
    }

    /// Fallible body-free current ownership copy, funded by the executing owner.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_current_partition_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorPartitionKey>> {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(None);
            };
            column.with_admitted_snapshot_visible_partition_for_row(
                row_id,
                SnapshotId(u64::MAX),
                owner,
                |key, _, _selected, owner| {
                    owner.reserve(key.estimated_bytes())?;
                    Ok(Some(key.clone()))
                },
            )
        })
    }

    /// Visit the current partition and then the row's retained historical
    /// partitions until the callback resolves one. The callback borrows each
    /// key/state in place, so it can charge and test snapshot membership before
    /// allocating a retained clone.
    #[doc(hidden)]
    pub fn with_partition_candidates_for_row<E, T>(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        inspect_partition: impl FnMut(
            &VectorPartitionKey,
            &Arc<IndexState>,
            &mut VectorCopyAdmission,
        ) -> std::result::Result<Option<T>, E>,
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<T>, E>
    where
        E: From<Error>,
    {
        let column = self.try_column(index).ok_or_else(|| {
            E::from(Error::UnknownVectorIndex {
                index: index.clone(),
            })
        })?;
        column.with_partition_candidates_for_row(row_id, inspect_partition, owner)
    }

    /// Resolve the row's snapshot-visible physical partition through the
    /// reverse directory. The store owns both lookup and visibility probing;
    /// callers see only a partition whose raw directory contains the row at
    /// the requested snapshot.
    #[doc(hidden)]
    /// Query selection preserves the external owner of ephemeral working states.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn with_query_snapshot_visible_partition_for_row<E, T, C>(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        before: &mut C,
        mut inspect: impl FnMut(
            &VectorPartitionKey,
            &VectorQueryState,
            &mut C,
            &mut VectorCopyAdmission,
        ) -> std::result::Result<Option<T>, E>,
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<T>, E>
    where
        E: From<Error>,
        C: FnMut() -> std::result::Result<(), E>,
    {
        let Some(selection) = self
            .capture_paged_partition_sources(index, owner)
            .map_err(E::from)?
        else {
            return self.with_snapshot_visible_partition_for_row(
                index,
                row_id,
                snapshot,
                before,
                |key, state, before, owner| {
                    inspect(key, &VectorQueryState::from(state.clone()), before, owner)
                },
                owner,
            );
        };
        let column = self.try_column(index).ok_or_else(|| {
            E::from(Error::UnknownVectorIndex {
                index: index.clone(),
            })
        })?;
        let reverse = column.durable_reverse_memberships.read().clone();
        drop(column);
        let reverse = reverse.ok_or_else(|| {
            E::from(Error::Other(
                "paged vector catalog has no reverse source".into(),
            ))
        })?;
        let mut key_owner = owner.scratch();
        before()?;
        selection.catalog.catalog.validate().map_err(E::from)?;
        let key = with_membership_read(before, |before_read| {
            reverse.visible_partition_admitted(row_id, snapshot, before_read, &mut key_owner)
        })?;
        selection.catalog.catalog.validate().map_err(E::from)?;
        let Some(key) = key else {
            return Ok(None);
        };
        let state = selection
            .probe(&key, owner)
            .map_err(E::from)?
            .ok_or_else(|| E::from(SnapshotPartitionMismatch::AbsentPartition.error()))?;
        if state
            .bounded_visible_directory_entry_by_row(row_id, snapshot, before, owner)?
            .is_none()
        {
            return Err(E::from(
                SnapshotPartitionMismatch::ForwardMembershipAbsent.error(),
            ));
        }
        let result = inspect(&key, &state, before, owner)?;
        selection.catalog.catalog.validate().map_err(E::from)?;
        Ok(result)
    }

    pub fn with_snapshot_visible_partition_for_row<E, T, C>(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        before_visibility_probe: &mut C,
        inspect_visible_partition: impl FnMut(
            &VectorPartitionKey,
            &Arc<IndexState>,
            &mut C,
            &mut VectorCopyAdmission,
        ) -> std::result::Result<Option<T>, E>,
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<T>, E>
    where
        E: From<Error>,
        C: FnMut() -> std::result::Result<(), E>,
    {
        let column = self.try_column(index).ok_or_else(|| {
            E::from(Error::UnknownVectorIndex {
                index: index.clone(),
            })
        })?;
        column.with_snapshot_visible_partition_for_row(
            row_id,
            snapshot,
            before_visibility_probe,
            inspect_visible_partition,
            owner,
        )
    }

    /// Passive count probe. Inconsistent reverse routes retain the base scan fallback.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn snapshot_visible_partition_matches(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        selected: impl Fn(&VectorPartitionKey) -> bool,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<bool>> {
        let column = self
            .try_column(index)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        column.with_snapshot_visible_partition_for_row_handling_mismatch(
            row_id,
            snapshot,
            &mut || Ok::<(), Error>(()),
            |key, _, _, _| Ok(Some(selected(key))),
            owner,
            |_mismatch| Ok(None),
        )
    }

    /// The row's vector visible at `snapshot` in one partition: a point read
    /// for a durable partition, a load of the resident partition otherwise.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_live_entry_for_row_in_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorEntry>> {
        self.with_bulk_read(|| {
            self.partition_state(index, partition_key)?
                .load_visible_entry_by_row_caching(index, row_id, snapshot, false, owner)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn live_entry_for_row_in_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorEntry>> {
        self.with_bulk_read(|| {
            let Some(state) = self.try_partition_state(index, partition_key) else {
                return Ok(None);
            };
            owner.reserve(state.copy_bytes(index, 1))?;
            Ok(state.visible_entry_by_row(index, row_id, snapshot))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn partition_info(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorPartitionInfo>> {
        self.try_partition_info(index, partition_key, owner)
    }
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_partition_info(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorPartitionInfo>> {
        if self.try_column(index).is_none() {
            return Ok(None);
        }
        if let Some(selection) = self.capture_paged_partition_sources(index, owner)? {
            let layout = self.scoped_index_layout(index, owner)?;
            if let Some(state) = selection.probe(partition_key, owner)? {
                if state.is_historical_only() {
                    return Ok(None);
                }
                return Self::partition_info_snapshot(index, partition_key, &state, &layout, owner)
                    .map(Some);
            }
            if !partition_key.components().is_empty() {
                return Ok(None);
            }
            let Some(state) = selection.empty_declared_state(owner)? else {
                return Ok(None);
            };
            return Self::partition_info_snapshot(index, partition_key, &state, &layout, owner)
                .map(Some);
        }
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(None);
            };
            if column.partition_is_historical_only(partition_key) {
                return Ok(None);
            }
            column
                .partition_state(partition_key)
                .map(|state| {
                    Self::partition_info_snapshot(
                        index,
                        partition_key,
                        &state,
                        &*column.scoped_layout(owner)?,
                        owner,
                    )
                })
                .transpose()
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn partition_info_snapshot(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        state: &IndexState,
        layout: &VectorIndexLayout,
        owner: &mut VectorCopyAdmission,
    ) -> Result<VectorPartitionInfo> {
        owner.reserve(
            std::mem::size_of::<VectorPartitionInfo>()
                .saturating_add(index.table.len())
                .saturating_add(index.column.len())
                .saturating_add(partition_key.estimated_bytes()),
        )?;
        if let Some(source) = state.durable_membership_source()
            && let Some(snapshot) = source.inspection_snapshot(owner)?
        {
            let live_rows = snapshot.summary.live_entries;
            let retained_rows = snapshot.summary.entries.saturating_sub(live_rows);
            let bytes = state
                .byte_count()
                .saturating_add(partition_key.estimated_bytes());
            let charged_vector_bytes = state.raw_payload_bytes();
            let durable_vector_bytes = snapshot.summary.entries.saturating_mul(
                crate::quantized::stored_vector_resident_bytes(state.dimension, state.quantization),
            );
            return Ok(Self::partition_info_from_counts(
                index,
                partition_key,
                state,
                layout,
                (live_rows, retained_rows, bytes),
                (
                    snapshot.pending_inserts,
                    snapshot.tombstones,
                    durable_vector_bytes,
                    charged_vector_bytes,
                    bytes.saturating_sub(charged_vector_bytes),
                ),
            ));
        }
        let live_rows = state.vector_count();
        let retained_rows = state.entry_count().saturating_sub(live_rows);
        let bytes = state
            .byte_count()
            .saturating_add(partition_key.estimated_bytes());
        let charged_vector_bytes = state.raw_payload_bytes();
        let charged_index_bytes = bytes.saturating_sub(charged_vector_bytes);
        if state.is_durable() {
            let (pending_inserts, tombstones) = state.durable_pending_counts(None, None, owner)?;
            let durable_vector_bytes =
                state
                    .entry_count()
                    .saturating_mul(crate::quantized::stored_vector_resident_bytes(
                        state.dimension,
                        state.quantization,
                    ));
            return Ok(Self::partition_info_from_counts(
                index,
                partition_key,
                state,
                layout,
                (live_rows, retained_rows, bytes),
                (
                    pending_inserts,
                    tombstones,
                    durable_vector_bytes,
                    charged_vector_bytes,
                    charged_index_bytes,
                ),
            ));
        }
        let directory = state.raw_directory.read();
        #[cfg(test)]
        state.raw_directory_entries_inspected.fetch_add(
            u64::try_from(directory.len()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
        let pending_inserts = directory
            .iter()
            .filter(|entry| entry.deleted_tx.is_none())
            .count();
        let tombstones = directory
            .iter()
            .filter(|entry| entry.deleted_tx.is_some())
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
        Ok(Self::partition_info_from_counts(
            index,
            partition_key,
            state,
            layout,
            (live_rows, retained_rows, bytes),
            (
                pending_inserts,
                tombstones,
                durable_vector_bytes,
                charged_vector_bytes,
                charged_index_bytes,
            ),
        ))
    }

    fn partition_info_from_counts(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        state: &IndexState,
        layout: &VectorIndexLayout,
        (live_rows, retained_rows, bytes): (usize, usize, usize),
        (
            pending_inserts,
            tombstones,
            durable_vector_bytes,
            charged_vector_bytes,
            charged_index_bytes,
        ): (usize, usize, usize, usize, usize),
    ) -> VectorPartitionInfo {
        let maintenance_inspection = state.maintenance_inspection();
        let maintenance_failure_details = maintenance_inspection.failure;
        VectorPartitionInfo {
            partition: VectorPartitionRef::new(index.clone(), partition_key.clone()),
            live_rows,
            retained_rows,
            bytes,
            pending_inserts,
            tombstones,
            durable_vector_bytes,
            charged_vector_bytes,
            charged_index_bytes,
            maintenance_progress: maintenance_inspection.progress,
            maintenance_failure: maintenance_failure_details.map(|details| details.failure),
            maintenance_failure_details,
            desired_policy: layout.resolve_policy(),
            maintenance_policy_revision: state.maintenance_policy_revision(),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn partition_infos(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorPartitionInfo>> {
        if let Some(selection) = self.capture_paged_partition_sources(index, owner)? {
            let layout = self.scoped_index_layout(index, owner)?;
            if !selection.is_empty() || layout.is_partitioned() {
                let count = selection.counts()[3];
                owner.reserve(
                    count
                        .checked_mul(std::mem::size_of::<VectorPartitionInfo>())
                        .ok_or_else(|| {
                            Error::Other("vector inspection output size overflow".into())
                        })?,
                )?;
                let mut infos = Vec::with_capacity(count);
                let mut traversal = owner.scratch();
                selection.visit(&mut traversal, |key, state| {
                    if !state.is_historical_only() {
                        if infos.len() == count {
                            return Err(Error::Other(
                                "working vector inspection count disagrees with its capture".into(),
                            ));
                        }
                        infos.push(Self::partition_info_snapshot(
                            index, key, &state, &layout, owner,
                        )?);
                    }
                    Ok::<(), Error>(())
                })?;
                if infos.len() != count {
                    return Err(Error::Other(
                        "working vector inspection count disagrees with its capture".into(),
                    ));
                }
                return Ok(infos);
            }
            let state = selection.empty_declared_state(owner)?.ok_or_else(|| {
                Error::Other("empty vector declaration changed during inspection".into())
            })?;
            owner.reserve(std::mem::size_of::<VectorPartitionInfo>())?;
            return Ok(vec![Self::partition_info_snapshot(
                index,
                &VectorPartitionKey::unpartitioned(),
                &state,
                &layout,
                owner,
            )?]);
        }
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            let layout = column.scoped_layout(owner)?;
            let partitions = column.partitions.read();
            let count = partitions
                .values()
                .filter(|state| !state.is_historical_only())
                .count();
            owner.reserve(count.saturating_mul(std::mem::size_of::<VectorPartitionInfo>()))?;
            let mut infos = Vec::with_capacity(count);
            for (key, state) in partitions
                .iter()
                .filter(|(_, state)| !state.is_historical_only())
            {
                infos.push(Self::partition_info_snapshot(
                    index, key, state, &layout, owner,
                )?);
            }
            Ok(infos)
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
    ) -> Option<VectorMaintenanceProgressGuard> {
        // Inspection must not make restore or import depend on a resident slot.
        // Historical-only and not-yet-registered routes may have none.
        self.try_partition_state(&partition.index, &partition.partition_key)
            .map(|state| state.begin_maintenance_progress_at(phase, reason, total, checkpoint_tx))
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn select_maintenance_partition<'a>(
        &self,
        candidates: &'a [VectorPartitionRef],
        mut owner: VectorCopyAdmission,
    ) -> Result<Option<&'a VectorPartitionRef>> {
        let mut last = self.last_maintenance_partition.lock();
        let next = last
            .as_ref()
            .and_then(|(last, _)| candidates.iter().position(|partition| partition == last))
            .map_or(0, |position| (position + 1) % candidates.len().max(1));
        let Some(selected) = candidates.get(next) else {
            return Ok(None);
        };
        owner.reserve(
            std::mem::size_of::<VectorPartitionRef>()
                .saturating_add(selected.index.table.len())
                .saturating_add(selected.index.column.len())
                .saturating_add(selected.partition_key.estimated_bytes()),
        )?;
        *last = Some((selected.clone(), owner));
        Ok(Some(selected))
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn index_quantization_storage_bytes_for_test(
        &self,
        index: &VectorIndexRef,
        dimension: usize,
    ) -> Option<usize> {
        let registry = self.registry.read();
        let column = registry.get(index)?;
        Some(column.layout.read().quantization.storage_bytes(dimension))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn index_layout_info(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<VectorIndexLayoutInfo> {
        let paged_counts = self.paged_partition_counts(index)?;
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            owner.reserve(
                std::mem::size_of::<VectorIndexLayoutInfo>()
                    .saturating_add(index.table.len())
                    .saturating_add(index.column.len()),
            )?;
            let layout = column.admitted_layout(owner)?;
            let partitioned = layout.is_partitioned();
            Ok(VectorIndexLayoutInfo {
                index: index.clone(),
                layout,
                live_partitions: paged_counts
                    .map_or_else(|| column.live_partition_count(), |counts| counts[1]),
                retained_partitions: paged_counts
                    .map_or_else(|| column.retained_partition_count(), |counts| counts[2]),
                live_or_retained_partitions: paged_counts.map_or_else(
                    || column.live_or_retained_partition_count(),
                    |counts| counts[3],
                ),
                partition_states: paged_counts.map_or_else(
                    || column.maintained_partition_state_count(),
                    |counts| if partitioned { counts[3] } else { 1 },
                ),
                bytes: column.retained_bytes(index),
            })
        })
    }

    pub fn is_empty(&self) -> bool {
        self.with_bulk_read(|| self.registry.read().is_empty())
    }

    pub fn index_count(&self) -> usize {
        self.with_bulk_read(|| self.registry.read().len())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn validate_vector(&self, index: &VectorIndexRef, actual: usize) -> Result<()> {
        self.with_bulk_read(|| {
            let expected = self.with_index_layout(index, |layout| layout.dimension)?;
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

    fn attach_legacy_raw_accounting(&self, state: &Arc<IndexState>) {
        if let Some(owner) = self.memory_accountant.read().clone() {
            state.attach_raw_accounting(
                owner,
                self.raw_payload_bytes.clone(),
                self.raw_payload_header_bytes.clone(),
            );
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_inserts(
        &self,
        inserts: Vec<VectorEntry>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_inserts_with_accountant(inserts, None, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_inserts_with_accountant(
        &self,
        inserts: Vec<VectorEntry>,
        accountant: Option<&dyn MemoryBudget>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_changes_with_accountant(
            Vec::new(),
            inserts,
            Vec::new(),
            Lsn::default(),
            accountant,
            owner,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_deletes(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId, TxId, Lsn)>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_deletes_with_accountant(deletes, None, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_deletes_with_accountant(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId, TxId, Lsn)>,
        accountant: Option<&dyn MemoryBudget>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_changes_with_accountant(
            deletes,
            Vec::new(),
            Vec::new(),
            Lsn::default(),
            accountant,
            owner,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_moves(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId, TxId, Lsn)>,
        lsn: Lsn,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_moves_with_accountant(moves, lsn, None, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_moves_with_accountant(
        &self,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId, TxId, Lsn)>,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_changes_with_accountant(Vec::new(), Vec::new(), moves, lsn, accountant, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_changes_with_accountant(
        &self,
        deletes: Vec<(VectorIndexRef, RowId, TxId, TxId, Lsn)>,
        inserts: Vec<VectorEntry>,
        moves: Vec<(VectorIndexRef, RowId, RowId, TxId, TxId, Lsn)>,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
        mut owner: VectorCopyAdmission,
    ) -> Result<()> {
        owner.reserve(
            deletes
                .len()
                .saturating_mul(std::mem::size_of::<PartitionedVectorDelete>())
                .saturating_add(
                    inserts
                        .len()
                        .saturating_mul(std::mem::size_of::<PartitionedVectorEntry>()),
                )
                .saturating_add(
                    moves
                        .len()
                        .saturating_mul(std::mem::size_of::<PartitionedVectorMove>()),
                ),
        )?;
        let deletes = deletes
            .into_iter()
            .map(|(index, row, tx, before_created_tx, before_lsn)| {
                PartitionedVectorDelete::new(
                    index,
                    VectorPartitionKey::unpartitioned(),
                    row,
                    tx,
                    before_created_tx,
                    before_lsn,
                )
            })
            .collect();
        let inserts = inserts
            .into_iter()
            .map(|entry| PartitionedVectorEntry::new(VectorPartitionKey::unpartitioned(), entry))
            .collect();
        let moves = moves
            .into_iter()
            .map(|(index, old, new, tx, before_created_tx, before_lsn)| {
                PartitionedVectorMove::new(
                    index,
                    VectorPartitionKey::unpartitioned(),
                    VectorPartitionKey::unpartitioned(),
                    old,
                    new,
                    tx,
                    before_created_tx,
                    before_lsn,
                )
            })
            .collect();
        let deletes: Vec<PartitionedVectorDelete> = deletes;
        let inserts: Vec<PartitionedVectorEntry> = inserts;
        let moves: Vec<PartitionedVectorMove> = moves;
        let indexes = deletes
            .iter()
            .map(|entry| &entry.partition.index)
            .chain(inserts.iter().map(|entry| &entry.entry.index))
            .chain(moves.iter().map(|entry| &entry.index));
        let mut route_owner = VectorCopyAdmission::new(owner.accountant().clone());
        route_owner.reserve(
            indexes
                .clone()
                .map(|index| {
                    std::mem::size_of::<VectorPartitionRef>()
                        + index.table.len()
                        + index.column.len()
                })
                .sum(),
        )?;
        let ordered = Self::sorted_partition_refs(indexes.map(|index| {
            VectorPartitionRef::new(index.clone(), VectorPartitionKey::unpartitioned())
        }));
        let result = self.with_bulk_read(|| {
            self.with_partition_set_maintenance(&ordered, || {
                self.apply_partitioned_changes_with_accountant(
                    deletes, inserts, moves, lsn, accountant, owner,
                )
            })
        });
        drop(ordered);
        drop(route_owner);
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_changes_with_accountant_ref(
        &self,
        deletes: &[(VectorIndexRef, RowId, TxId, TxId, Lsn)],
        inserts: &[VectorEntry],
        moves: &[(VectorIndexRef, RowId, RowId, TxId, TxId, Lsn)],
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
        mut owner: VectorCopyAdmission,
    ) -> Result<()> {
        let bytes = deletes
            .iter()
            .map(|(index, _, _, _, _)| {
                std::mem::size_of::<(VectorIndexRef, RowId, TxId, TxId, Lsn)>()
                    + index.table.len()
                    + index.column.len()
            })
            .sum::<usize>()
            + inserts
                .iter()
                .map(|entry| {
                    std::mem::size_of::<VectorEntry>()
                        + entry.index.table.len()
                        + entry.index.column.len()
                        + entry.vector.len() * std::mem::size_of::<f32>()
                })
                .sum::<usize>()
            + moves
                .iter()
                .map(|(index, _, _, _, _, _)| {
                    std::mem::size_of::<(VectorIndexRef, RowId, RowId, TxId, TxId, Lsn)>()
                        + index.table.len()
                        + index.column.len()
                })
                .sum::<usize>();
        owner.reserve(bytes)?;
        let mut body_inventory_owner = owner.scratch();
        body_inventory_owner.reserve(
            inserts
                .len()
                .saturating_mul(std::mem::size_of::<PaidRetainedMemoryCharge>()),
        )?;
        let mut copied_bodies = Vec::with_capacity(inserts.len());
        for entry in inserts {
            let column =
                self.try_column(&entry.index)
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: entry.index.clone(),
                    })?;
            let layout = column.layout.read();
            if layout.dimension != entry.vector.len() {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: entry.index.clone(),
                    expected: layout.dimension,
                    actual: entry.vector.len(),
                });
            }
            let body_bytes = crate::quantized::stored_vector_resident_bytes(
                layout.dimension,
                layout.quantization,
            );
            let body_owner = column.directory_accounting.lock().accountant.clone();
            body_owner.try_allocate_for_static(
                body_bytes,
                "vector_index",
                "copy_borrowed_vector_body",
                "Reduce copied vector bodies or raise MEMORY_LIMIT before publication.",
            )?;
            copied_bodies.push(PaidRetainedMemoryCharge::new(body_bytes, body_owner));
        }
        let result = self.apply_changes_with_accountant(
            deletes.to_vec(),
            inserts.to_vec(),
            moves.to_vec(),
            lsn,
            accountant,
            owner,
        );
        if result.is_ok() {
            for charge in &mut copied_bodies {
                charge.bytes = 0;
            }
        }
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_partitioned_inserts(
        &self,
        inserts: Vec<PartitionedVectorEntry>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_partitioned_inserts_with_accountant(inserts, None, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_partitioned_inserts_with_accountant(
        &self,
        inserts: Vec<PartitionedVectorEntry>,
        accountant: Option<&dyn MemoryBudget>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_partitioned_changes_with_accountant(
            Vec::new(),
            inserts,
            Vec::new(),
            Lsn::default(),
            accountant,
            owner,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_partitioned_deletes(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_partitioned_changes_with_accountant(
            deletes,
            Vec::new(),
            Vec::new(),
            Lsn::default(),
            None,
            owner,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_partitioned_moves(
        &self,
        moves: Vec<PartitionedVectorMove>,
        lsn: Lsn,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_partitioned_moves_with_accountant(moves, lsn, None, owner)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_partitioned_moves_with_accountant(
        &self,
        moves: Vec<PartitionedVectorMove>,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.apply_partitioned_changes_with_accountant(
            Vec::new(),
            Vec::new(),
            moves,
            lsn,
            accountant,
            owner,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn apply_partitioned_changes_with_accountant(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        let prepared =
            self.prepare_partitioned_batch(deletes, inserts, moves, HashSet::new(), owner)?;
        self.publish_prepared_partitioned_batch(prepared, lsn, accountant);
        Ok(())
    }

    fn install_durable_counts(&self, counts: &[PreparedDurableCounts]) {
        for prepared in counts {
            if let Some(state) = self
                .try_partition_state(&prepared.partition.index, &prepared.partition.partition_key)
                && state.is_durable()
            {
                *state.durable_counts.lock() = prepared.summary;
                state.apply_durable_pending_events(&prepared.events);
                state.reclassify_limit();
            }
        }
    }

    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    fn apply_admitted_partitioned_changes(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        lsn: Lsn,
        accountant: Option<&dyn MemoryBudget>,
        directory_reservations: &mut PreparedDirectoryCharges,
        canonical_closures: &mut [PreparedCanonicalClosure],
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
            if state.is_durable() {
                // The membership closed durably with the row; counts are
                // installed from the prepared projection. A body cache keeps
                // the closed version for older snapshots.
                let _residency = state.raw_residency_gate.read_recursive();
                if state.bodies_resident() {
                    state.tombstone_resident_bodies(delete.row_id, delete.deleted_tx);
                }
                continue;
            }
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
            if let Some(owner) = directory_reservations.owner(&insert.entry.index) {
                state.attach_raw_accounting(
                    owner,
                    self.raw_payload_bytes.clone(),
                    self.raw_payload_header_bytes.clone(),
                );
            }
            let index = insert.entry.index.clone();
            let residency = state
                .is_durable()
                .then(|| state.raw_residency_gate.read_recursive());
            if state.is_durable() && state.bodies_resident() {
                // The membership is durable with the row; a body cache keeps
                // the body exactly as the resident path does.
                let mut stored = state.stored_entry(insert.entry);
                stored.membership = insert.membership;
                stored.canonical_deleted_tx = insert.canonical_deleted_tx;
                if state.raw_residency_is_accounted() && !stored.vector.payload_is_charged() {
                    let reservation = directory_reservations
                        .get_mut(&index)
                        .expect("accounted insertion owns its prepared header reservation");
                    let header_bytes = crate::quantized::shared_payload_header_bytes();
                    assert!(
                        reservation.bytes >= header_bytes,
                        "prepared reservation covers the new shared allocation header"
                    );
                    reservation.bytes -= header_bytes;
                    stored.vector.promote_to_shared();
                }
                state.push_resident_body(stored);
                drop(residency);
                continue;
            }
            drop(residency);
            if state.is_durable() {
                // The body and membership are durable with the row; the
                // caller's per-insert body admission is returned because
                // nothing retains it.
                let bytes = crate::quantized::stored_vector_resident_bytes(
                    insert.entry.vector.len(),
                    state.quantization,
                );
                drop(insert);
                if let Some(accountant) = accountant {
                    accountant.release(bytes);
                }
                continue;
            }
            let mut stored = state.stored_entry(insert.entry);
            stored.membership = insert.membership;
            stored.canonical_deleted_tx = insert.canonical_deleted_tx;
            let already_present = state.with_entries(|entries| {
                entries.iter().any(|existing| {
                    existing.row_id == stored.row_id
                        && existing.created_tx == stored.created_tx
                        && existing.deleted_tx == stored.deleted_tx
                        && existing.lsn == stored.lsn
                        && existing.membership == stored.membership
                })
            });
            let live_without_replacement = !deleted_rows.contains(&(index.clone(), stored.row_id))
                && state.with_entries(|entries| {
                    entries.iter().any(|existing| {
                        existing.row_id == stored.row_id && existing.deleted_tx.is_none()
                    })
                });
            if already_present || live_without_replacement {
                let bytes = stored.estimated_bytes();
                drop(stored);
                if let Some(accountant) = accountant {
                    accountant.release(bytes);
                }
                continue;
            }
            if state.raw_residency_is_accounted() && !stored.vector.payload_is_charged() {
                let reservation = directory_reservations
                    .get_mut(&index)
                    .expect("accounted insertion owns its prepared header reservation");
                let header_bytes = crate::quantized::shared_payload_header_bytes();
                assert!(
                    reservation.bytes >= header_bytes,
                    "prepared reservation covers the new shared allocation header"
                );
                reservation.bytes -= header_bytes;
                stored.vector.promote_to_shared();
            }
            state.push_entry_with(stored, |stored| {
                column.note_live_insert(&insert.partition_key, stored);
            });
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
            if let Some(owner) = directory_reservations.owner(&row_move.index) {
                target.attach_raw_accounting(
                    owner,
                    self.raw_payload_bytes.clone(),
                    self.raw_payload_header_bytes.clone(),
                );
            }
            if source.is_durable() || target.is_durable() {
                // Both placements are durable with the row; preparation
                // validated the live source. Body caches follow the move: the
                // source closes its body, and the target takes a copy of it
                // when the source cached one, or drops its cache otherwise.
                let moved = {
                    let _residency = source.raw_residency_gate.read_recursive();
                    let body = source
                        .bodies_resident()
                        .then(|| source.stored_by_row_id(row_move.old_row_id))
                        .flatten();
                    if source.bodies_resident() {
                        source.tombstone_resident_bodies(row_move.old_row_id, row_move.tx);
                    }
                    body
                };
                if target.bodies_resident() {
                    match moved {
                        Some(mut moved) if moved.deleted_tx.is_none() => {
                            moved.row_id = row_move.new_row_id;
                            row_move.stamp_target(&mut moved, lsn);
                            if moved.vector.payload_identity().is_none()
                                && let Some(reservation) =
                                    directory_reservations.get_mut(&row_move.index)
                            {
                                // Preparation reserved this copy and its header.
                                let bytes =
                                    moved.estimated_bytes().saturating_add(
                                        crate::quantized::shared_payload_header_bytes(),
                                    );
                                reservation.bytes = reservation.bytes.saturating_sub(bytes);
                                moved.vector.promote_to_shared();
                            }
                            let _residency = target.raw_residency_gate.read_recursive();
                            target.push_resident_body(moved);
                        }
                        _ => {
                            // The existing move fallback invalidates its cached target.
                            // Its handles and pins were prepared before durability;
                            // release only these pins before taking the exclusive gate.
                            for closure in canonical_closures.iter_mut() {
                                if Arc::ptr_eq(&closure.state, &target) {
                                    closure._residency.take();
                                    closure.matches_cached_identity = false;
                                }
                            }
                            target.evict_raw_vectors();
                        }
                    }
                }
                continue;
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
            row_move.stamp_target(&mut moved, lsn);
            if moved.vector.payload_identity().is_none()
                && let Some(reservation) = directory_reservations.get_mut(&row_move.index)
            {
                // Preparation reserved a new body and header before this copy.
                // Its source remains owned and charged for historical readers.
                let bytes = moved
                    .estimated_bytes()
                    .saturating_add(crate::quantized::shared_payload_header_bytes());
                reservation.bytes -= bytes;
                moved.vector.promote_to_shared();
            }
            target.push_entry_with(moved, |_| {
                column.note_move(
                    &row_move.source_partition_key,
                    &row_move.target_partition_key,
                    row_move.old_row_id,
                    row_move.new_row_id,
                );
            });
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn insert_loaded_vector(
        &self,
        entry: VectorEntry,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<()> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

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
                declared_owner,
            )?;
            if let Some(column) = self.try_column(&index)
                && let Some(state) = column.partition_state(&VectorPartitionKey::unpartitioned())
            {
                self.attach_legacy_raw_accounting(&state);
                let stored_entry = state.stored_entry(entry);
                state.push_entry_with(stored_entry, |stored| {
                    column.note_live_insert(&VectorPartitionKey::unpartitioned(), stored)
                });
            }
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn insert_loaded_partitioned_vector(
        &self,
        entry: PartitionedVectorEntry,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        owner.reserve(
            entry
                .partition_key
                .estimated_bytes()
                .saturating_mul(8)
                .saturating_add(
                    entry
                        .entry
                        .index
                        .table
                        .len()
                        .saturating_add(entry.entry.index.column.len())
                        .saturating_mul(8),
                )
                .saturating_add(8 * std::mem::size_of::<VectorPartitionRef>()),
        )?;
        let partition =
            VectorPartitionRef::new(entry.entry.index.clone(), entry.partition_key.clone());
        let touched = HashSet::from([partition.clone()]);
        let requested = HashMap::from([(
            partition.index.clone(),
            [partition.partition_key.clone()]
                .into_iter()
                .collect::<PartitionKeySet>(),
        )]);
        self.with_admitted_partition_states(&touched, &requested, owner, || {
            self.with_partition_maintenance(&partition.index, &partition.partition_key, || {
                let Some(column) = self.try_column(&partition.index) else {
                    return;
                };
                let Some(state) = column.partition_state(&partition.partition_key) else {
                    return;
                };
                self.attach_legacy_raw_accounting(&state);
                let mut stored = state.stored_entry(entry.entry);
                stored.membership = entry.membership;
                stored.canonical_deleted_tx = entry.canonical_deleted_tx;
                state.push_entry_with(stored, |stored| {
                    column.note_live_insert(&partition.partition_key, stored)
                });
            });
        })
    }

    /// Attach the column's row-id reverse directory before durable partitions.
    /// The engine publishes it together with forward sources against one root.
    /// Registration reads no membership records and retains no decoded rows.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_durable_raw_reverse_source(
        &self,
        index: &VectorIndexRef,
        source: Arc<dyn RawVectorReverseMembershipSource>,
    ) -> Result<()> {
        let source: NativeSourceArc<dyn RawVectorReverseMembershipSource> = source.into();
        self.with_bulk_maintenance(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            if !column.row_to_partition.read().is_empty()
                || !column.row_partition_history.read().is_empty()
                || column.durable_reverse_memberships.read().is_some()
            {
                return Err(Error::Other(
                    "durable vector reverse source requires an empty column directory".into(),
                ));
            }
            *column.durable_reverse_memberships.write() = Some(source);
            Ok(())
        })
    }

    /// Make every partition this column creates after open durable, with the
    /// source and loader `factory` builds. Register with the reverse source.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_durable_raw_partition_factory(
        &self,
        index: &VectorIndexRef,
        factory: Arc<dyn DurableRawPartitionFactory>,
    ) -> Result<()> {
        let factory: NativeSourceArc<dyn DurableRawPartitionFactory> = factory.into();
        self.with_bulk_maintenance(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            if column.durable_reverse_memberships.read().is_none() {
                return Err(Error::Other(
                    "durable vector partition factory requires a column reverse source".into(),
                ));
            }
            *column.durable_partition_factory.write() = Some(factory);
            Ok(())
        })
    }

    /// Make every column declared from now on durable through `factory`.
    #[doc(hidden)]
    pub fn register_durable_raw_column_factory(&self, factory: Arc<dyn DurableRawColumnFactory>) {
        *self.durable_columns.write() = Some(factory.into());
    }

    /// Ownership credit for retained metadata, distinct from copy workspace.
    /// Every registered column retains its explicitly declared funder.
    /// The executing statement's owner supplies no retained fallback.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn retained_column_copy_owner(
        &self,
        index: &VectorIndexRef,
        _owner: &VectorCopyAdmission,
    ) -> Result<VectorCopyAdmission> {
        let column = self
            .try_column(index)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let accountant = column.directory_accounting.lock().accountant.clone();
        Ok(VectorCopyAdmission::new(accountant))
    }

    /// Prepare sources against the executing owner before the enclosing
    /// schema commit. Local publication performs no factory calls.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_received_schema_durable_sources(
        &self,
        publication: &mut PreparedVectorPublication,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        for (index, column) in &publication.registry {
            if column.durable_reverse_memberships.read().is_none() {
                let retained =
                    VectorCopyAdmission::new(column.directory_accounting.lock().accountant.clone());
                self.make_column_durable(index, column, owner, retained)?;
            }
        }
        Ok(())
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn validate_private_paged_columns(&self) -> Result<()> {
        let registry = self.registry.read();
        for column in registry.values() {
            let factory = column.durable_partition_factory.read().clone();
            if factory
                .as_ref()
                .and_then(|factory| factory.published_catalog_counts())
                .is_none()
            {
                return Err(Error::Other(
                    "private vector publication has no complete working catalog".into(),
                ));
            }
        }
        Ok(())
    }
    #[doc(hidden)]
    pub fn retire_private_working_partitions(&self) {
        self.with_bulk_maintenance(|| {
            let registry = self.registry.read();
            for (index, column) in registry.iter() {
                let factory = column.durable_partition_factory.read().clone();
                if factory
                    .as_ref()
                    .and_then(|factory| factory.published_catalog_counts())
                    .is_none()
                {
                    continue;
                }
                let before = column.directory_retained_bytes(index);
                // No source IO or user callback occurs here. Existing clear
                // detaches ledger keys and physically destroys map allocations.
                column.clear_partition_map();
                column.clear_row_maps();
                column.reconcile_directory_accounting_after_reclamation(index, before);
            }
        });
    }

    /// Activate declared columns only, after entering the private source loan.
    /// Partition history remains in the paged catalog, never installed here.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn activate_durable_raw_columns(&self, owner: &mut VectorCopyAdmission) -> Result<()> {
        let indexes = self.with_bulk_read(|| {
            let registry = self.registry.read();
            let bytes = registry.keys().try_fold(
                std::alloc::Layout::array::<VectorIndexRef>(registry.len())
                    .map_err(|_| Error::Other("vector column activation layout overflow".into()))?
                    .size(),
                |bytes, index| {
                    bytes
                        .checked_add(index.table.len())
                        .and_then(|n| n.checked_add(index.column.len()))
                        .ok_or_else(|| Error::Other("vector column activation key overflow".into()))
                },
            )?;
            owner.reserve(bytes)?;
            Ok::<_, Error>(registry.keys().cloned().collect::<Vec<_>>())
        })?;
        for index in indexes {
            let column = self
                .try_column(&index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            if column.durable_reverse_memberships.read().is_none() {
                let retained =
                    VectorCopyAdmission::new(column.directory_accounting.lock().accountant.clone());
                self.make_column_durable(&index, &column, owner, retained)?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn make_column_durable(
        &self,
        index: &VectorIndexRef,
        column: &VectorColumnState,
        owner: &mut VectorCopyAdmission,
        retained: VectorCopyAdmission,
    ) -> Result<()> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        let Some(factory) = self.durable_columns.read().clone() else {
            return Ok(());
        };
        let layout = column.scoped_layout(owner)?;
        let partition_retained = retained.scratch();
        let (reverse, partitions) = factory.create(index, &layout, owner.scratch(), retained)?;
        let reverse: NativeSourceArc<dyn RawVectorReverseMembershipSource> = reverse.into();
        let partitions: NativeSourceArc<dyn DurableRawPartitionFactory> = partitions.into();
        // Complete every fallible construction before changing any state.
        let snapshots = column.admitted_partition_snapshots(owner)?;
        owner.reserve(snapshots.len().saturating_mul(std::mem::size_of::<(
            Arc<IndexState>,
            NativeSourceArc<dyn RawVectorMembershipSource>,
            NativeSourceArc<dyn DormantRawVectorLoader>,
        )>()))?;
        let mut sources = Vec::with_capacity(snapshots.len());
        for (key, state) in snapshots {
            if state.is_durable() {
                continue;
            }
            owner.reserve(index.table.len().saturating_add(index.column.len()))?;
            let partition = VectorPartitionRef::new(index.clone(), key);
            let (memberships, loader) =
                partitions.create(&partition, owner.scratch(), partition_retained.scratch())?;
            sources.push((
                state,
                NativeSourceArc::<dyn RawVectorMembershipSource>::from(memberships),
                NativeSourceArc::<dyn DormantRawVectorLoader>::from(loader),
            ));
        }
        for (state, memberships, loader) in sources {
            if state.entry_count() == 0 {
                state.install_durable_source(
                    memberships.source.clone(),
                    loader.source.clone(),
                    memberships.published_summary(),
                    self.raw_payload_bytes.clone(),
                    self.raw_payload_header_bytes.clone(),
                );
            } else {
                state.convert_to_durable(
                    memberships.source.clone(),
                    loader.source.clone(),
                    self.raw_payload_bytes.clone(),
                    self.raw_payload_header_bytes.clone(),
                );
            }
        }
        column.clear_row_maps();
        *column.durable_reverse_memberships.write() = Some(reverse);
        *column.durable_partition_factory.write() = Some(partitions);
        Ok(())
    }

    /// Whether any registered column keeps its memberships in durable storage.
    #[doc(hidden)]
    pub fn has_durable_memberships(&self) -> bool {
        self.with_bulk_read(|| {
            self.registry
                .read()
                .values()
                .any(|column| column.durable_reverse_memberships.read().is_some())
        })
    }

    /// Register only captured catalog metadata and a source factory. The
    /// partition source is opened and verified on its first point/count/cursor
    /// use, never during writable-open registration.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_lazy_durable_raw_partition(
        &self,
        partition: &VectorPartitionRef,
        summary: RawVectorMembershipSummary,
        factory: Arc<dyn RawVectorMembershipFactory>,
        loader: Arc<dyn DormantRawVectorLoader>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Arc<LazyRawVectorMembershipSource>> {
        let loader: NativeSourceArc<dyn DormantRawVectorLoader> = loader.into();
        owner.reserve(
            std::mem::size_of::<LazyRawVectorMembershipSource>()
                .saturating_add(2 * std::mem::size_of::<usize>()),
        )?;
        let source = Arc::new(LazyRawVectorMembershipSource::new(summary, factory)?);
        self.register_durable_raw_partition(
            partition,
            source.clone(),
            loader.source.clone(),
            owner,
        )?;
        Ok(source)
    }

    /// Register a root-backed membership directory without enumerating records.
    /// The engine owns catalog validation, current-root publication and the
    /// source's admitted scratch. Register this on the empty registry at open;
    /// row commits update durable memberships and catalog counts atomically.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn register_durable_raw_partition(
        &self,
        partition: &VectorPartitionRef,
        memberships: Arc<dyn RawVectorMembershipSource>,
        loader: Arc<dyn DormantRawVectorLoader>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        let memberships: NativeSourceArc<dyn RawVectorMembershipSource> = memberships.into();
        let loader: NativeSourceArc<dyn DormantRawVectorLoader> = loader.into();
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.with_bulk_maintenance(|| {
            if !loader.supports_bounded_candidate_load() {
                return Err(Error::Other(
                    "durable vector memberships require bounded raw-body point access".into(),
                ));
            }
            let column =
                self.try_column(&partition.index)
                    .ok_or_else(|| Error::UnknownVectorIndex {
                        index: partition.index.clone(),
                    })?;
            let layout = column.scoped_layout(owner)?;
            owner.reserve(
                partition
                    .partition_key
                    .estimated_bytes()
                    .saturating_mul(4)
                    .saturating_add(IndexState::allocation_bytes())
                    .saturating_add(16 * std::mem::size_of::<usize>())
                    .saturating_add(3 * std::mem::size_of::<AtomicUsize>()),
            )?;
            Self::validate_partition_key(&partition.index, &layout, &partition.partition_key)?;
            if column.durable_reverse_memberships.read().is_none() {
                return Err(Error::Other(
                    "durable vector partition requires a column reverse source".into(),
                ));
            }
            let summary = memberships.published_summary();
            if summary.live_entries > summary.entries {
                return Err(Error::Other(
                    ("durable vector membership live count exceeds total count".to_string()).into(),
                ));
            }
            if layout.is_partitioned() && summary.entries == 0 {
                // Cleanup removes the final membership and its summary in the
                // same durable transaction. A current-root reopen must not
                // recreate an obsolete partition from an empty summary.
                debug_assert!(
                    false,
                    "partitioned durable membership summaries must be nonempty"
                );
                return Ok(());
            }
            let state = {
                let mut states = column.partitions.write();
                let state = states
                    .entry(partition.partition_key.clone())
                    .or_insert_with(|| {
                        Arc::new(IndexState::new(layout.dimension, layout.quantization))
                    })
                    .clone();
                column.attach_state(&partition.partition_key, &state);
                state
            };
            let _gate = state.raw_residency_gate.write();
            if state.entry_count() != 0 || state.durable_membership_source().is_some() {
                return Err(Error::Other(
                    ("durable vector membership source requires an empty partition".to_string())
                        .into(),
                ));
            }
            state.install_durable_source(
                memberships.source.clone(),
                loader.source.clone(),
                summary,
                self.raw_payload_bytes.clone(),
                self.raw_payload_header_bytes.clone(),
            );
            if summary.live_entries == 0 && summary.entries != 0 {
                state.historical_only.store(true, Ordering::SeqCst);
            }
            drop(_gate);
            state.reclassify_limit();
            Ok(())
        })
    }

    /// Install only durable identities for a file-backed open. Vector bodies
    /// remain behind each partition's loader until a read or mutation selects
    /// that partition.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn replace_with_dormant_raw_partitions(
        &self,
        dormant: Vec<DormantRawVectorPartition>,
    ) -> Result<()> {
        self.with_bulk_maintenance(|| {
            let mut live_requested = HashMap::<VectorIndexRef, PartitionKeySet>::new();
            let mut seen = HashSet::<VectorPartitionRef>::new();
            for partition in &dormant {
                if !seen.insert(partition.partition.clone()) {
                    return Err(Error::Other(
                        (format!(
                            "duplicate dormant raw vector partition for {}.{}",
                            partition.partition.index.table, partition.partition.index.column
                        ))
                        .into(),
                    ));
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
                }
                if layout.is_partitioned() {
                    column.clear_partition_map();
                }
                column.clear_row_maps();
            }
            for partition in dormant {
                let column = self
                    .try_column(&partition.partition.index)
                    .expect("dormant raw partition index was validated");
                let layout = column.layout();
                let state = {
                    let mut partitions = column.partitions.write();
                    let state = partitions
                        .entry(partition.partition.partition_key.clone())
                        .or_insert_with(|| {
                            Arc::new(IndexState::new(layout.dimension, layout.quantization))
                        })
                        .clone();
                    column.attach_state(&partition.partition.partition_key, &state);
                    state
                };
                *state.raw_payload_counter.write() = self.raw_payload_bytes.clone();
                *state.raw_payload_header_counter.write() = self.raw_payload_header_bytes.clone();
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
                            ("durable raw-vector partition is absent from the resident registry"
                                .to_string())
                            .into(),
                        )
                    })?;
                state.attach_durable_raw_loader(partition.directory, partition.loader)?;
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn replace_loaded_vectors(
        &self,
        entries: Vec<VectorEntry>,
        declared_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<()> {
        let declared_owner: crate::memory_budget::MemoryBudgetOwner = declared_owner.into();

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
                }
                if layout.is_partitioned() {
                    column.clear_partition_map();
                }
                column.clear_row_maps();
            }
            for entry in entries {
                self.insert_loaded_vector(entry, declared_owner.clone())?;
            }
            Ok(())
        })
    }

    /// Prepare a restamped image for an already-accounted detached store.
    /// All copying and admission precede publication. The supplied owner is
    /// consumed, and only actual retained bodies/directories keep its credit.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prepare_loaded_partitioned_replacement(
        &self,
        entries: Vec<PartitionedVectorEntry>,
        owner: VectorCopyAdmission,
    ) -> Result<PreparedLoadedVectorReplacement<'_>> {
        // Declare the funding before the payload so all refusal paths destroy
        // the payload before returning its capacity.
        let mut owner = owner;
        let incoming_entries = entries;
        let result = self.with_bulk_read(|| {
            let accountant = owner.accountant().clone();
            let registry = self.registry.read();
            // Bound candidate structures before constructing schemas, key
            // maps, stored bodies, and their promotion headers. Each input
            // may introduce a distinct partition and history key.
            let mut preparation_bytes = prepared_directory_growth_upper_bound(
                &incoming_entries,
                &[],
                &HashSet::new(),
                &HashMap::new(),
            )
            .saturating_mul(2)
            // The temporary store constructs three Arc<AtomicUsize> owners
            // before its raw counters are rebound to the destination.
            .saturating_add(
                3 * (2 * std::mem::size_of::<usize>() + std::mem::size_of::<AtomicUsize>()),
            );
            // The retained-funder inventory coexists with schemas and the
            // candidate. Its buckets and cloned index strings are transient
            // statement allocations, never borrowed source ownership.
            preparation_bytes = preparation_bytes.saturating_add(row_placement_map_bytes::<
                VectorIndexRef,
                crate::memory_budget::MemoryBudgetOwner,
            >(
                registry.len().saturating_mul(2),
            ));
            for (index, column) in registry.iter() {
                preparation_bytes = preparation_bytes
                    .saturating_add(column.directory_retained_bytes(index))
                    .saturating_add(std::mem::size_of::<VectorIndexLayout>())
                    .saturating_add(index.table.len().saturating_mul(2))
                    .saturating_add(index.column.len().saturating_mul(2));
            }
            for entry in &incoming_entries {
                let column =
                    registry
                        .get(&entry.entry.index)
                        .ok_or_else(|| Error::UnknownVectorIndex {
                            index: entry.entry.index.clone(),
                        })?;
                let layout = column.layout.read();
                Self::validate_partition_key(&entry.entry.index, &layout, &entry.partition_key)?;
                if entry.entry.vector.len() != layout.dimension {
                    return Err(Error::VectorIndexDimensionMismatch {
                        index: entry.entry.index.clone(),
                        expected: layout.dimension,
                        actual: entry.entry.vector.len(),
                    });
                }
                preparation_bytes = preparation_bytes
                    .saturating_add(IndexState::allocation_bytes())
                    .saturating_add(4 * std::mem::size_of::<usize>())
                    .saturating_add(entry.partition_key.estimated_bytes().saturating_mul(4))
                    .saturating_add(crate::quantized::stored_vector_resident_bytes(
                        layout.dimension,
                        layout.quantization,
                    ));
            }
            preparation_bytes =
                preparation_bytes.saturating_add(incoming_entries.len().saturating_mul(
                    row_placement_map_bytes::<RowId, Arc<VectorPartitionKey>>(3).saturating_add(
                        row_placement_map_bytes::<RowId, Vec<Arc<VectorPartitionKey>>>(3),
                    ),
                ));
            owner.reserve(preparation_bytes)?;
            let directory_owners = registry
                .iter()
                .map(|(index, column)| {
                    let declared = column.directory_accounting.lock().accountant.clone();
                    (index.clone(), declared)
                })
                .collect::<HashMap<_, _>>();
            let schemas = registry
                .iter()
                .map(|(index, column)| {
                    (
                        index.clone(),
                        column.layout(),
                        column.directory_accounting.lock().accountant.clone(),
                    )
                })
                .collect();
            drop(registry);
            let mut publication =
                Self::prepare_partitioned_received_schema_admitted(schemas, incoming_entries)?;
            self.prepare_received_schema_durable_sources(&mut publication, &mut owner)?;
            let candidate = Self::default();
            *candidate.registry.write() = publication.into_parts().0;
            // Use the destination's receipt counters from first adoption.
            let mut candidate = candidate;
            candidate.raw_payload_bytes = self.raw_payload_bytes.clone();
            candidate.raw_payload_header_bytes = self.raw_payload_header_bytes.clone();
            // The executing owner funded all temporary candidate allocation
            // up front. Admit the completed retained directories under their
            // existing column owners before adoption/publication; payloads keep
            // the existing prepaid executing-owner adoption protocol.
            let directory_reservation = PreparedDirectoryReservation::empty();
            for (index, column) in candidate.registry.read().iter() {
                directory_reservation.reserve_index(
                    index.clone(),
                    directory_owners
                        .get(index)
                        .expect("candidate declaration retains its declared owner")
                        .clone(),
                    column.directory_retained_bytes(index),
                )?;
            }
            drop(directory_owners);
            let directory_charges = directory_reservation.take();
            drop(directory_reservation);
            candidate.account_loaded_state_with_directory_charges(
                accountant.clone(),
                owner.retained_bytes(),
                Some(directory_charges),
            )?;
            let registry = std::mem::take(&mut *candidate.registry.write());
            Ok(PreparedLoadedVectorReplacement {
                destination: self,
                registry,
                accountant,
            })
        });
        if result.is_ok() {
            owner.transfer();
        }
        result
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn replace_loaded_partitioned_vectors(
        &self,
        entries: Vec<PartitionedVectorEntry>,
        owner: VectorCopyAdmission,
    ) -> Result<()> {
        self.prepare_loaded_partitioned_replacement(entries, owner)?
            .publish();
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn all_entries(&self, owner: &mut VectorCopyAdmission) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.scoped_partition_snapshots(owner)?.into_iter() {
                    entries.extend(state.all_entries(&index, owner)?);
                }
            }
            Ok(entries)
        })
    }

    /// Materialize every raw partition before returning its entries. Durable
    /// callers use this for operations such as export and schema projection
    /// that genuinely need the complete body set and can propagate a load or
    /// admission refusal.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_all_entries(&self, owner: &mut VectorCopyAdmission) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.scoped_partition_snapshots(owner)?.into_iter() {
                    for entry in state.owned_entries(&index, owner)? {
                        entries.push(entry?);
                    }
                }
            }
            Ok(entries)
        })
    }

    /// Lightweight durable identities used by startup bookkeeping. This does
    /// not fault a vector body into memory.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn raw_directory_entries(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<(VectorIndexRef, RawVectorDirectoryEntry)>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for state in column.partitions.read().values() {
                    let mut metadata = owner.scratch();
                    let mut retain = |entry: RawVectorDirectoryEntry| -> Result<()> {
                        owner.reserve(
                            std::mem::size_of::<(VectorIndexRef, RawVectorDirectoryEntry)>()
                                .saturating_add(index.table.len())
                                .saturating_add(index.column.len()),
                        )?;
                        entries.reserve_exact(1);
                        entries.push((index.clone(), entry));
                        Ok(())
                    };
                    if let Some(source) = state.durable_membership_source() {
                        // Metadata scratch and retained output have independent lifetimes.
                        source.visit_identities(&mut || true, &mut retain, &mut metadata)?;
                    } else {
                        for entry in state.raw_directory.read().iter() {
                            retain(entry.clone())?;
                        }
                    }
                }
            }
            Ok(entries)
        })
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn ensure_all_raw_vectors_loaded(&self, owner: &mut VectorCopyAdmission) -> Result<()> {
        self.with_bulk_read(|| {
            for column in self.registry.read().values() {
                for (_, state) in column.partition_snapshots() {
                    state.ensure_raw_vectors_loaded(owner)?;
                }
            }
            Ok(())
        })
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn ensure_raw_partition_loaded(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        self.with_bulk_read(|| {
            self.partition_state(index, partition_key)?
                .ensure_raw_vectors_loaded(owner)
        })
    }

    /// Admit the raw-image copies and affected graph encodings before purge
    /// loads its selected values or constructs a replacement registry.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn purge_workspace_bounds(
        &self,
        rows: &[(String, RowId)],
        owner: &mut VectorCopyAdmission,
    ) -> Result<(usize, usize)> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        let mut workspace = 0usize;
        for (index, column) in self.registry.read().iter() {
            // Selection must precede every layout/directory copy, including
            // the admission quote used by both erasure consumers.
            if !rows.iter().any(|(table, _)| table == &index.table) {
                continue;
            }
            let layout = column.scoped_layout(owner)?;
            for (_, state) in column.scoped_partition_snapshots(owner)?.into_iter() {
                // Unaffected states are carried by handle; only selected
                // partitions contribute body copies to this staged image.
                let contains_selected_row = if state.is_durable() {
                    // Durable refusal is terminal for this preparation.
                    state.durable_contains_rows(
                        rows.iter()
                            .filter(|(table, _)| table == &index.table)
                            .map(|(_, row)| *row),
                        owner,
                    )?
                } else {
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
            }
        }
        Ok((workspace, 0))
    }

    /// Load exactly the raw partitions that contain any version of one of
    /// the named rows. Authoritative purge uses this before constructing its
    /// replacement image: every version of the selected lineage must be
    /// available, while unrelated partitions remain dormant.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn ensure_raw_partitions_containing_rows(
        &self,
        table: &str,
        row_ids: &HashSet<RowId>,
        owner: &mut VectorCopyAdmission,
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
                for (partition_key, state) in column.admitted_partition_snapshots(owner)? {
                    // Probe the existing row directory: a single-row public
                    // read must not enumerate every other vector on each call.
                    // All versions still select their owning partitions.
                    if state.is_durable() {
                        // Durable projections stay on their bounded point
                        // source instead of retaining a decoded partition.
                        if state.durable_contains_rows(row_ids.iter().copied(), owner)?
                            && state.try_cache_durable_bodies(owner)?
                        {
                            loaded.push(VectorPartitionRef::new(index.clone(), partition_key));
                        }
                        continue;
                    }
                    let contains_selected_row = {
                        let by_row = state.raw_directory_by_row.read();
                        row_ids.iter().any(|row_id| by_row.contains_key(row_id))
                    };
                    if !contains_selected_row {
                        continue;
                    }
                    state.ensure_raw_vectors_loaded(owner)?;
                    loaded.push(VectorPartitionRef::new(index.clone(), partition_key));
                }
            }
            Ok(loaded)
        })
    }

    /// Every body version of partitions holding one of the selected rows.
    /// The caller admits the complete selected-partition copy first through
    /// `purge_workspace_bounds`; unaffected states are carried by handle.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn durable_entries_of_partitions_containing_rows(
        &self,
        table: &str,
        row_ids: &HashSet<RowId>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<PartitionedVectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self
                .scoped_sorted_refs(|index| index.table == table, owner)?
                .into_iter()
            {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.scoped_partition_snapshots(owner)?.into_iter()
                {
                    let contains = if state.is_durable() {
                        state.durable_contains_rows(row_ids.iter().copied(), owner)?
                    } else {
                        let directory = state.raw_directory_by_row.read();
                        row_ids.iter().any(|row| directory.contains_key(row))
                    };
                    if !contains {
                        continue;
                    }
                    owner.reserve(
                        state
                            .entry_count()
                            .saturating_mul(
                                partition_key
                                    .estimated_bytes()
                                    .saturating_add(std::mem::size_of::<PartitionedVectorEntry>()),
                            )
                            .saturating_mul(2),
                    )?;
                    for entry in state.owned_entries(&index, owner)? {
                        entries.push(PartitionedVectorEntry::new(partition_key.clone(), entry?));
                    }
                }
            }
            Ok(entries)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn all_partitioned_entries(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<PartitionedVectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.scoped_partition_snapshots(owner)?.into_iter()
                {
                    owner.reserve(
                        state
                            .entry_count()
                            .saturating_mul(
                                partition_key
                                    .estimated_bytes()
                                    .saturating_add(std::mem::size_of::<PartitionedVectorEntry>()),
                            )
                            .saturating_mul(2),
                    )?;
                    entries.extend(
                        state
                            .all_entries(&index, owner)?
                            .into_iter()
                            .map(|entry| PartitionedVectorEntry::new(partition_key.clone(), entry)),
                    );
                }
            }
            Ok(entries)
        })
    }

    /// What copying the entries of every index `keep` selects would cost at
    /// its widest point, computed without loading or cloning anything: the
    /// owned clone `try_partitioned_entries_matching` returns, plus the
    /// stored entries and their directory a store built from that clone
    /// holds. A caller admits this BEFORE it hydrates or clones, so a copy
    /// the declared limit cannot hold is refused before it exists.
    #[doc(hidden)]
    pub fn partitioned_copy_bytes_matching(&self, keep: impl Fn(&VectorIndexRef) -> bool) -> usize {
        self.with_bulk_read(|| {
            let mut bytes = 0usize;
            for (index, column) in self.registry.read().iter() {
                if !keep(index) {
                    continue;
                }
                let layout = column.layout.read();
                let clone_entry = std::mem::size_of::<PartitionedVectorEntry>()
                    .saturating_add(layout.dimension.saturating_mul(std::mem::size_of::<f32>()))
                    .saturating_add(index.table.len())
                    .saturating_add(index.column.len());
                let stored_entry = std::mem::size_of::<StoredVectorEntry>()
                    .saturating_add(crate::quantized::shared_payload_header_bytes())
                    .saturating_add(std::mem::size_of::<(usize, usize)>())
                    .saturating_add(crate::quantized::stored_vector_resident_bytes(
                        layout.dimension,
                        layout.quantization,
                    ))
                    .saturating_add(2 * std::mem::size_of::<RawVectorDirectoryEntry>());
                for (partition_key, state) in column.partitions.read().iter() {
                    let per_entry = clone_entry
                        .saturating_add(partition_key.estimated_bytes())
                        .saturating_add(stored_entry);
                    bytes = bytes.saturating_add(state.entry_count().saturating_mul(per_entry));
                }
            }
            bytes
        })
    }

    /// `try_all_partitioned_entries` restricted to the indexes `keep`
    /// selects: only those are hydrated and cloned.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_partitioned_entries_matching(
        &self,
        keep: impl Fn(&VectorIndexRef) -> bool,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<PartitionedVectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(keep, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.scoped_partition_snapshots(owner)?.into_iter()
                {
                    owner.reserve(
                        state
                            .entry_count()
                            .saturating_mul(
                                partition_key
                                    .estimated_bytes()
                                    .saturating_add(std::mem::size_of::<PartitionedVectorEntry>()),
                            )
                            .saturating_mul(2),
                    )?;
                    for entry in state.owned_entries_with_copy_preference(&index, owner, true)? {
                        entries.push(PartitionedVectorEntry::new(partition_key.clone(), entry?));
                    }
                }
            }
            Ok(entries)
        })
    }

    /// Export the resident image with exact source membership and executing
    /// copy ownership. Memory stores remain whole-resident; no row reconstruction.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_resident_partitioned_entries_with_membership(
        &self,
        owner: VectorCopyAdmission,
    ) -> Result<OwnedPartitionedVectorEntries> {
        self.try_resident_partitioned_entries_with_membership_matching(owner, |_, _| true)
    }

    /// Select borrowed column metadata before any selected image allocation.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_resident_partitioned_entries_with_membership_matching(
        &self,
        mut owner: VectorCopyAdmission,
        mut matches: impl FnMut(&VectorIndexRef, &VectorIndexLayout) -> bool,
    ) -> Result<OwnedPartitionedVectorEntries> {
        let entries = self.with_bulk_read(|| {
            let mut entries = Vec::new();
            let mut selected = Vec::<VectorIndexRef>::new();
            {
                let registry = self.registry.read();
                for (index, column) in registry.iter() {
                    if !matches(index, &column.layout.read()) {
                        continue;
                    }
                    if selected.len() == selected.capacity() {
                        let capacity = selected.capacity().saturating_mul(2).max(4);
                        owner.reserve(
                            capacity.saturating_mul(std::mem::size_of::<VectorIndexRef>()),
                        )?;
                        selected.reserve_exact(capacity.saturating_sub(selected.len()));
                    }
                    owner.reserve(index.table.len().saturating_add(index.column.len()))?;
                    selected.push(index.clone());
                }
            }
            selected.sort_unstable_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
            for index in selected {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.admitted_partition_snapshots(&mut owner)? {
                    let vectors = state.vectors.read();
                    let per_entry = std::mem::size_of::<PartitionedVectorEntry>()
                        .saturating_add(partition_key.estimated_bytes())
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len())
                        .saturating_add(state.dimension.saturating_mul(std::mem::size_of::<f32>()));
                    owner.reserve(
                        vectors
                            .entries
                            .len()
                            .saturating_mul(per_entry)
                            .saturating_mul(4),
                    )?;
                    for entry in &vectors.entries {
                        entries.push(PartitionedVectorEntry::with_membership(
                            partition_key.clone(),
                            entry.to_vector_entry(index.clone()),
                            entry.membership,
                            entry.canonical_deleted_tx,
                        ));
                    }
                }
            }
            Ok::<_, Error>(entries)
        })?;
        Ok(OwnedPartitionedVectorEntries { entries, owner })
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_all_partitioned_entries(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<PartitionedVectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (partition_key, state) in column.scoped_partition_snapshots(owner)?.into_iter()
                {
                    owner.reserve(
                        state
                            .entry_count()
                            .saturating_mul(
                                partition_key
                                    .estimated_bytes()
                                    .saturating_add(std::mem::size_of::<PartitionedVectorEntry>()),
                            )
                            .saturating_mul(2),
                    )?;
                    for entry in state.owned_entries(&index, owner)? {
                        entries.push(PartitionedVectorEntry::new(partition_key.clone(), entry?));
                    }
                }
            }
            Ok(entries)
        })
    }

    /// Retention's selected-column removal; other indexes keep their states.
    /// Whole-row removal can update reverse maps in place instead of copying
    /// every surviving partition key into a rebuilt map.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prune_row_ids_in_indexes(
        &self,
        row_ids: &HashSet<RowId>,
        indexes: &[VectorIndexRef],
        owner: &mut VectorCopyAdmission,
    ) -> Result<usize> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.with_bulk_maintenance(|| {
            for index in indexes {
                if let Some(column) = self.try_column(index) {
                    for (_, state) in column.admitted_partition_snapshots(owner)? {
                        state.refresh_durable_counts(owner)?;
                    }
                }
            }
            let sampled = self.directory_bytes_before_reclamation(indexes);
            let mut released = 0usize;
            for index in indexes {
                let Some(column) = self.try_column(index) else {
                    continue;
                };
                {
                    let partitions = column.partitions.read();
                    for state in partitions.values() {
                        if state.directory_names_any_row(row_ids) {
                            released =
                                released.saturating_add(state.prune_directory_versions(|entry| {
                                    !row_ids.contains(&entry.row_id)
                                }));
                        }
                    }
                }
                if column.layout.read().is_partitioned() {
                    column
                        .partitions
                        .write()
                        .retain(|_, state| state.entry_count() != 0);
                }
                column
                    .row_to_partition
                    .write()
                    .retain(|row, _| !row_ids.contains(row));
                column
                    .row_partition_history
                    .write()
                    .retain(|row, _| !row_ids.contains(row));
            }
            self.reconcile_directory_accounting_for_indexes(&sampled);
            Ok(released)
        })
    }

    /// Admit the complete pruning inventory before changing any partition.
    /// Summaries are read now and applied only after every column has prepared.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn prepare_pruning_columns(
        &self,
        indexes: &[VectorIndexRef],
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<PreparedPruningColumn>> {
        owner.reserve(
            indexes
                .len()
                .saturating_mul(std::mem::size_of::<PreparedPruningColumn>()),
        )?;
        let mut prepared = Vec::with_capacity(indexes.len());
        for index in indexes {
            let Some(column) = self.try_column(index) else {
                continue;
            };
            owner.reserve(index.table.len().saturating_add(index.column.len()))?;
            let snapshots = column.admitted_partition_snapshots(owner)?;
            owner.reserve(
                snapshots
                    .len()
                    .saturating_mul(std::mem::size_of::<Option<RawVectorMembershipSummary>>()),
            )?;
            let mut summaries = Vec::with_capacity(snapshots.len());
            for (key, state) in &snapshots {
                summaries.push(match state.durable_membership_source() {
                    Some(source) => Some(source.current_summary(owner)?),
                    None => None,
                });
                // Empty-state cleanup may clone every partition key into its
                // removed-key set while these snapshots are still alive. Admit
                // the full key allocation and worst-case hash-table buckets,
                // including growth overlap, independently of directory length.
                if column.layout.read().is_partitioned() {
                    owner.reserve(
                        key.estimated_bytes()
                            .saturating_add(8 * (std::mem::size_of::<VectorPartitionKey>() + 1)),
                    )?;
                }
                // Rebuilding surviving row routes temporarily coexists with the
                // old routes. Price their maximum population before pruning;
                // retained directory shrinkage stays with the column owner.
                owner.reserve(state.raw_directory.read().len().saturating_mul(
                    8 * std::mem::size_of::<(RowId, (TxId, Lsn, Arc<VectorPartitionKey>))>()
                        + 3 * key.estimated_bytes()
                        + 3 * std::mem::size_of::<Arc<VectorPartitionKey>>(),
                ))?;
            }
            prepared.push((index.clone(), column, snapshots, summaries));
        }
        Ok(prepared)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prune_row_ids(
        &self,
        row_ids: &HashSet<RowId>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<usize> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.with_bulk_maintenance(|| {
            #[cfg(feature = "test-seams")]
            self.pause_registry.maybe_pause(
                &contextdb_core::VectorIndexRef::default(),
                crate::test_seam::PauseWindow::Bulk,
            );
            let mut released = 0usize;
            let indexes = self.scoped_sorted_refs(|_| true, owner)?;
            // Sampled before anything is removed: the directory owner returns
            // exactly the identity this pass physically removes (entries and
            // emptied partition states), once, and never estimates it
            // elsewhere.
            let prepared = self.prepare_pruning_columns(&indexes, owner)?;
            owner.reserve(indexes.iter().fold(0usize, |bytes, index| {
                bytes
                    .saturating_add(std::mem::size_of::<(VectorIndexRef, usize)>())
                    .saturating_add(index.table.len())
                    .saturating_add(index.column.len())
            }))?;
            let sampled = self.directory_bytes_before_reclamation(&indexes);
            // All fallible work is complete. Mutation, settlement and the
            // caller's body-release receipt now finish on every successful path.
            for (_, _, snapshots, summaries) in &prepared {
                for ((_, state), summary) in snapshots.iter().zip(summaries) {
                    if let Some(summary) = summary {
                        state.apply_refreshed_durable_counts(*summary);
                    }
                }
            }
            for (_, column, snapshots, _) in &prepared {
                for (_, state) in snapshots {
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
                }
                column.remove_empty_partitioned_states();
                column.rebuild_current_rows_from(snapshots);
            }
            drop(prepared);
            self.reconcile_directory_accounting_for_indexes(&sampled);
            Ok(released)
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
    /// mutates an immutable index generation. Old index points remain until generation
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn prune_superseded_versions(
        &self,
        versions: &[VectorVersionIdentity],
        owner: &mut VectorCopyAdmission,
    ) -> Result<usize> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        if versions.is_empty() {
            return Ok(0);
        }
        owner.reserve(versions.iter().fold(0usize, |bytes, identity| {
            bytes
                .saturating_add(
                    8 * std::mem::size_of::<(VectorIndexRef, HashSet<(RowId, TxId, Lsn)>)>(),
                )
                .saturating_add(8 * std::mem::size_of::<(RowId, TxId, Lsn)>())
                .saturating_add(identity.index.table.len())
                .saturating_add(identity.index.column.len())
        }))?;
        let mut by_index: HashMap<VectorIndexRef, HashSet<(RowId, TxId, Lsn)>> = HashMap::new();
        for identity in versions {
            by_index
                .entry(identity.index.clone())
                .or_default()
                .insert(identity.version());
        }
        self.with_bulk_maintenance(|| {
            let mut released = 0usize;
            let indexes = self.scoped_sorted_refs(|index| by_index.contains_key(index), owner)?;
            owner.reserve(
                versions
                    .len()
                    .saturating_mul(8 * std::mem::size_of::<RowId>()),
            )?;
            // See `prune_row_ids`: sampled before removal so the directory
            // owner returns exactly this pass's own shrinkage.
            let prepared = self.prepare_pruning_columns(&indexes, owner)?;
            owner.reserve(indexes.iter().fold(0usize, |bytes, index| {
                bytes
                    .saturating_add(std::mem::size_of::<(VectorIndexRef, usize)>())
                    .saturating_add(index.table.len())
                    .saturating_add(index.column.len())
            }))?;
            let sampled = self.directory_bytes_before_reclamation(&indexes);
            // All fallible work is complete. Mutation, settlement and the
            // caller's body-release receipt now finish on every successful path.
            for (_, _, snapshots, summaries) in &prepared {
                for ((_, state), summary) in snapshots.iter().zip(summaries) {
                    if let Some(summary) = summary {
                        state.apply_refreshed_durable_counts(*summary);
                    }
                }
            }
            for (index, column, snapshots, _) in &prepared {
                let Some(versions) = by_index.get(index) else {
                    continue;
                };
                let row_ids = versions
                    .iter()
                    .map(|(row_id, _, _)| *row_id)
                    .collect::<HashSet<_>>();
                for (_, state) in snapshots {
                    if state.is_durable() {
                        state.evict_raw_vectors();
                    }

                    if !state.directory_names_any_row(&row_ids) {
                        continue;
                    }
                    released = released.saturating_add(state.prune_directory_versions(|entry| {
                        !versions.contains(&(entry.row_id, entry.created_tx, entry.lsn))
                    }));
                }
                column.remove_empty_partitioned_states();
                column.rebuild_current_rows_from(snapshots);
            }
            drop(prepared);
            self.reconcile_directory_accounting_for_indexes(&sampled);
            Ok(released)
        })
    }

    /// Name every version of `index` that `versions` selects, read from the
    /// partitions' durable directories alone: no partition is faulted in, so
    /// an idle retained partition stays dormant while a superseded version it
    /// holds is still found and can be pruned durably.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn directory_versions_matching(
        &self,
        index: &VectorIndexRef,
        versions: &HashSet<(RowId, TxId, Lsn)>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorVersionIdentity>> {
        if versions.is_empty() {
            return Ok(Vec::new());
        }
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(Vec::new());
            };
            let row_ids = versions
                .iter()
                .map(|(row, _, _)| *row)
                .collect::<HashSet<_>>();
            let mut metadata = owner.scratch();
            let mut matched = Vec::new();
            for state in column.partitions.read().values() {
                let mut retain = |entry: &RawVectorDirectoryEntry| -> Result<()> {
                    if versions.contains(&(entry.row_id, entry.created_tx, entry.lsn)) {
                        owner.reserve(
                            std::mem::size_of::<VectorVersionIdentity>()
                                .saturating_add(index.table.len())
                                .saturating_add(index.column.len()),
                        )?;
                        matched.reserve_exact(1);
                        matched.push(VectorVersionIdentity {
                            index: index.clone(),
                            row_id: entry.row_id,
                            created_tx: entry.created_tx,
                            lsn: entry.lsn,
                        });
                    }
                    Ok(())
                };
                if state.is_durable() {
                    for entry in state.durable_row_versions(&row_ids, &mut metadata)? {
                        retain(&entry)?;
                    }
                } else {
                    for entry in state.raw_directory.read().iter() {
                        retain(entry)?;
                    }
                }
            }
            Ok(matched)
        })
    }

    /// Visit selected retained identities at the existing directory frontier.
    /// The caller can admit each owned copy before making it; vector bodies
    /// and unrelated directory entries are never materialized by this visit.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn visit_directory_versions_for_rows(
        &self,
        index: &VectorIndexRef,
        row_ids: &HashSet<RowId>,
        mut visit: impl FnMut(&RawVectorDirectoryEntry) -> Result<()>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<()> {
        if row_ids.is_empty() {
            return Ok(());
        }
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(());
            };
            let partitions = column.partitions.read();
            for state in partitions.values() {
                if state.is_durable() {
                    for entry in state.durable_row_versions(row_ids, owner)? {
                        visit(&entry)?;
                    }
                    continue;
                }
                if !state.directory_names_any_row(row_ids) {
                    continue;
                }
                let directory = state.raw_directory.read();
                for page in directory.chunks(128) {
                    for entry in page {
                        if row_ids.contains(&entry.row_id) {
                            visit(entry)?;
                        }
                    }
                }
            }
            Ok(())
        })
    }

    /// Name every retained version for the selected row identities without
    /// loading vector bodies. Complete-image replacement uses this after the
    /// replacement registry is published, when the prior live version is now
    /// historical but still owns a raw body and durable membership.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn directory_versions_for_rows(
        &self,
        index: &VectorIndexRef,
        row_ids: &HashSet<RowId>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorVersionIdentity>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(Vec::new());
            };

            let mut metadata = owner.scratch();
            let mut matched = Vec::new();
            for state in column.partitions.read().values() {
                let mut retain = |entry: &RawVectorDirectoryEntry| -> Result<()> {
                    if row_ids.contains(&entry.row_id) {
                        owner.reserve(
                            std::mem::size_of::<VectorVersionIdentity>()
                                .saturating_add(index.table.len())
                                .saturating_add(index.column.len()),
                        )?;
                        matched.reserve_exact(1);
                        matched.push(VectorVersionIdentity {
                            index: index.clone(),
                            row_id: entry.row_id,
                            created_tx: entry.created_tx,
                            lsn: entry.lsn,
                        });
                    }
                    Ok(())
                };
                if state.is_durable() {
                    for entry in state.durable_row_versions(row_ids, &mut metadata)? {
                        retain(&entry)?;
                    }
                } else {
                    for entry in state.raw_directory.read().iter() {
                        retain(entry)?;
                    }
                }
            }
            Ok(matched)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn entries_for_index(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let column = self
                .try_column(index)
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })?;
            let mut entries = Vec::new();
            for (_, state) in column.scoped_partition_snapshots(owner)?.into_iter() {
                let state_entries = state
                    .owned_entries(index, owner)?
                    .collect::<Result<Vec<_>>>()?;
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

    /// Count the raw snapshot and selected live graph input without loading or
    /// cloning vector bodies. A change graph selects only the post-base suffix.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn partition_generation_entry_counts(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        snapshot: SnapshotId,
        after_lsn: Option<Lsn>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<(usize, usize)> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            if state.is_durable() {
                // At or after the frontier every membership is counted by the
                // maintained summary, and a change suffix by the journal.
                let counts = state.durable_counts().expect("durable state has counts");
                if snapshot.0 >= counts.max_tx.0 {
                    let selected = match after_lsn {
                        None => Some(counts.live_entries),
                        Some(after_lsn) => state
                            .durable_membership_source()
                            .and_then(|source| source.pending_after(after_lsn, owner).transpose())
                            .transpose()?
                            .map(|(pending, _)| pending),
                    };
                    if let Some(selected) = selected {
                        return Ok((counts.entries, selected));
                    }
                }
                let mut raw = 0usize;
                let mut selected = 0usize;
                state.visit_durable_intervals(
                    |entry| {
                        if entry.membership.visible_from.0 <= snapshot.0 {
                            raw += 1;
                            if entry
                                .membership
                                .deleted_tx
                                .is_none_or(|tx| tx.0 > snapshot.0)
                                && after_lsn.is_none_or(|lsn| entry.lsn > lsn)
                            {
                                selected += 1;
                            }
                        }
                        Ok(())
                    },
                    owner,
                )?;
                return Ok((raw, selected));
            }
            let directory = state.raw_directory.read();
            let mut raw = 0usize;
            let mut selected = 0usize;
            for entry in directory.iter().filter(|entry| {
                #[cfg(feature = "test-seams")]
                resident_directory_test_seams::observe_generation_directory_identity();
                entry.membership.visible_from.0 <= snapshot.0
            }) {
                raw = raw
                    .checked_add(1)
                    .ok_or_else(|| Error::Other("vector snapshot count overflow".into()))?;
                if entry
                    .membership
                    .deleted_tx
                    .is_none_or(|tx| tx.0 > snapshot.0)
                    && after_lsn.is_none_or(|lsn| entry.lsn > lsn)
                {
                    selected = selected
                        .checked_add(1)
                        .ok_or_else(|| Error::Other("vector graph count overflow".into()))?;
                }
            }
            Ok((raw, selected))
        })
    }

    /// Capture the finite entry prefix visible at a committed frontier. Writers
    /// only append or tombstone entries; each payload copy takes one short read
    /// lock, and a later tombstone cannot erase visibility at this snapshot.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn entries_for_partition_at_snapshot(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        snapshot: SnapshotId,
        owner: &VectorWorkspaceReservation,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            if state.is_durable() {
                return state.durable_entries_selected(
                    index,
                    snapshot,
                    &|bytes| owner.require_capacity(bytes),
                    &mut VectorCopyAdmission::new(owner.accountant().clone()),
                    |entry| entry.membership.visible_from.0 <= snapshot.0,
                );
            }
            let mut hydration_owner = VectorCopyAdmission::new(owner.accountant().clone());
            state.ensure_raw_vectors_loaded(&mut hydration_owner)?;
            let (count, snapshot_count) = {
                let vectors = state.vectors.read();
                (
                    vectors.entries.len(),
                    vectors
                        .entries
                        .iter()
                        .filter(|entry| {
                            #[cfg(feature = "test-seams")]
                            resident_directory_test_seams::observe_generation_raw_identity();
                            entry.membership.visible_from.0 <= snapshot.0
                        })
                        .count(),
                )
            };
            // The frontier's admitted raw slots never grow geometrically or
            // reserve space for later commits while bodies are copied.
            owner.require_capacity(state.generation_copy_bytes(index, snapshot_count))?;
            let mut entries = Vec::with_capacity(snapshot_count);
            for position in 0..count {
                let entry = {
                    let vectors = state.vectors.read();
                    vectors
                        .entries
                        .get(position)
                        .filter(|entry| {
                            #[cfg(feature = "test-seams")]
                            resident_directory_test_seams::observe_generation_raw_identity();
                            entry.membership.visible_from.0 <= snapshot.0
                        })
                        .map(|entry| {
                            #[cfg(feature = "test-seams")]
                            resident_directory_test_seams::observe_generation_entry_copy();
                            entry.to_vector_entry(index.clone())
                        })
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

    /// Copy only the live entries consumed by a generation at its pinned
    /// frontier. Retained history stays in the store; later appends and
    /// tombstones cannot enlarge this snapshot's admitted selection.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn entries_for_partition_generation_at_snapshot(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        snapshot: SnapshotId,
        after_lsn: Option<Lsn>,
        owner: &VectorWorkspaceReservation,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            let selected = |created_tx: TxId, deleted_tx: Option<TxId>, lsn: Lsn| {
                created_tx.0 <= snapshot.0
                    && deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
                    && after_lsn.is_none_or(|boundary| lsn > boundary)
            };
            if state.is_durable() {
                if let Some(after_lsn) = after_lsn
                    && let Some(entries) = state.durable_suffix_entries(
                        index,
                        snapshot,
                        after_lsn,
                        &|bytes| owner.require_capacity(bytes),
                        &mut VectorCopyAdmission::new(owner.accountant().clone()),
                    )?
                {
                    return Ok(entries);
                }
                return state.durable_entries_selected(
                    index,
                    snapshot,
                    &|bytes| owner.require_capacity(bytes),
                    &mut VectorCopyAdmission::new(owner.accountant().clone()),
                    |entry| selected(entry.created_tx, entry.deleted_tx, entry.lsn),
                );
            }
            let mut hydration_owner = VectorCopyAdmission::new(owner.accountant().clone());
            state.ensure_raw_vectors_loaded(&mut hydration_owner)?;
            let directory_count = state
                .raw_directory
                .read()
                .iter()
                .filter(|entry| {
                    #[cfg(feature = "test-seams")]
                    resident_directory_test_seams::observe_generation_directory_identity();
                    selected(entry.created_tx, entry.deleted_tx, entry.lsn)
                })
                .count();
            let (count, snapshot_count) = {
                let vectors = state.vectors.read();
                (
                    vectors.entries.len(),
                    vectors
                        .entries
                        .iter()
                        .filter(|entry| {
                            #[cfg(feature = "test-seams")]
                            resident_directory_test_seams::observe_generation_raw_identity();
                            selected(entry.created_tx, entry.deleted_tx, entry.lsn)
                        })
                        .count(),
                )
            };
            if snapshot_count != directory_count {
                // The engine priced this metadata frontier. Refuse an
                // inconsistent resident image before allocating any body.
                return Err(Error::StoreCorrupted {
                    path: format!("vector index {}.{}", index.table, index.column).into(),
                    reason: "generation selection differs from its raw directory".into(),
                });
            }
            owner.require_capacity(state.generation_copy_bytes(index, snapshot_count))?;
            let mut entries = Vec::with_capacity(snapshot_count);
            for position in 0..count {
                let entry = {
                    let vectors = state.vectors.read();
                    match vectors.entries.get(position).filter(|entry| {
                        #[cfg(feature = "test-seams")]
                        resident_directory_test_seams::observe_generation_raw_identity();
                        selected(entry.created_tx, entry.deleted_tx, entry.lsn)
                    }) {
                        Some(entry) => {
                            if entries.len() == snapshot_count {
                                // Valid writes only append after this frontier
                                // or add future tombstones. Never grow a body
                                // owner beyond the counted selection.
                                return Err(Error::StoreCorrupted {
                                    path: format!("vector index {}.{}", index.table, index.column)
                                        .into(),
                                    reason: "generation selection grew at its pinned frontier"
                                        .into(),
                                });
                            }
                            #[cfg(feature = "test-seams")]
                            resident_directory_test_seams::observe_generation_entry_copy();
                            Some(entry.to_vector_entry(index.clone()))
                        }
                        None => None,
                    }
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn entries_for_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            state.owned_entries(index, owner)?.collect()
        })
    }

    /// Count one column at the caller's snapshot without materializing inspection
    /// rows or traversing retained vector bodies and reverse directories.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn visible_entry_count(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<usize> {
        self.try_visible_entry_count(index, snapshot, owner)
    }

    /// Count source-backed membership without hydrating directory records.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_visible_entry_count(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<usize> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        let Some(column) = self.try_column(index) else {
            return Ok(0);
        };
        let mut count = 0usize;
        let mut position = 0usize;
        loop {
            // Only a cheap state handle crosses the storage callback. No
            // registry, partition-directory or bulk guard may be held there.
            let state = column.partitions.read().values().nth(position).cloned();
            let Some(state) = state else {
                return Ok(count);
            };
            position += 1;
            count = count
                .checked_add(state.try_directory_visible_entry_count(snapshot, None, owner)?)
                .ok_or_else(|| Error::Other("vector visible count overflow".into()))?;
        }
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

    /// Passive price of the actual partition's generation output workspace.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn generation_copy_bytes_for_test(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        count: usize,
    ) -> Option<usize> {
        let registry = self.registry.read();
        let column = registry.get(index)?;
        let partitions = column.partitions.read();
        let state = partitions.get(partition_key)?;
        Some(state.generation_copy_bytes(index, count))
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

    /// Retain an actionable maintenance failure for passive inspection. The
    /// engine calls this when durable generation work fails outside the vector
    /// crate (for example disk admission); only a later successful generation
    /// publication clears the mark.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
            self.note_failed_partition(&partition.index, &partition.partition_key);
            Ok(())
        })
    }

    /// Classify and retain a maintenance error raised by an engine-owned
    /// durable preparation or publication step outside this crate.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn find_by_row_id(
        &self,
        row_id: RowId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorEntry>> {
        self.with_bulk_read(|| {
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.scoped_partition_snapshots(owner)?.into_iter() {
                    if let Some(entry) = state.find_by_row_id(&index, row_id, owner)? {
                        return Ok(Some(entry));
                    }
                }
            }
            Ok(None)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorEntry>> {
        self.with_bulk_read(|| {
            let Some(state) = self.try_state(index) else {
                return Ok(None);
            };
            owner.reserve(state.copy_bytes(index, 1))?;
            Ok(state.visible_entry_by_row(index, row_id, snapshot))
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
        owner: &mut VectorCopyAdmission,
    ) -> std::result::Result<Option<f32>, E> {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(None);
            };
            let score = |state: &IndexState,
                         checkpoint: &mut _,
                         distance: &mut _,
                         retain: &mut _,
                         release: &mut _,
                         owner: &mut VectorCopyAdmission| {
                state.bounded_score_visible_candidate(
                    index, row_id, snapshot, query, checkpoint, distance, retain, release, owner,
                )
            };
            column.with_snapshot_visible_partition_for_row(
                row_id,
                snapshot,
                &mut before_checkpoint,
                |_, state, before_checkpoint, owner| {
                    score(
                        state,
                        before_checkpoint,
                        &mut before_distance,
                        &mut before_retain,
                        &mut release_retained,
                        owner,
                    )
                },
                owner,
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<VectorEntry>> {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(None);
            };
            column.with_admitted_snapshot_visible_partition_for_row(
                row_id,
                snapshot,
                owner,
                |_, state, selected, owner| {
                    state.load_visible_entry_by_row_selected(
                        index, row_id, snapshot, true, selected, owner,
                    )
                },
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn live_entries_for_row(
        &self,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.scoped_partition_snapshots(owner)?.into_iter() {
                    owner.reserve(state.copy_bytes(&index, 1))?;
                    if let Some(entry) = state.visible_entry_by_row(&index, row_id, snapshot) {
                        entries.push(entry);
                    }
                }
            }
            Ok(entries)
        })
    }

    /// Whether a row has a visible vector in this column. This consults only
    /// the lightweight durable directory and never faults a body into memory.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn has_live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<bool> {
        self.try_has_live_entry_for_row(index, row_id, snapshot, owner)
    }

    /// Durable counterpart of the resident-only existence probe. Integrity and
    /// storage errors remain errors rather than an absent row.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_has_live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<bool> {
        let mut probe = owner.scratch();
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return Ok(false);
            };
            Ok(column
                .with_snapshot_visible_partition_for_row(
                    row_id,
                    snapshot,
                    &mut || Ok::<(), Error>(()),
                    |_, _, _, _| Ok(Some(())),
                    &mut probe,
                )?
                .is_some())
        })
    }

    /// Whether a row has any visible vector column. This is the body-free
    /// existence check used by row delete and replacement planning.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn has_any_live_entry_for_row(
        &self,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<bool> {
        self.try_has_any_live_entry_for_row(row_id, snapshot, owner)
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_has_any_live_entry_for_row(
        &self,
        row_id: RowId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<bool> {
        let mut probe = owner.scratch();
        self.with_bulk_read(|| {
            let columns: Vec<_> = {
                let registry = self.registry.read();
                let bytes = registry
                    .len()
                    .checked_mul(std::mem::size_of::<Arc<VectorColumnState>>())
                    .ok_or_else(|| Error::Other("vector column capacity overflow".into()))?;
                probe.reserve(bytes)?;
                registry.values().cloned().collect()
            };
            for column in columns {
                if column
                    .with_snapshot_visible_partition_for_row(
                        row_id,
                        snapshot,
                        &mut || Ok::<(), Error>(()),
                        |_, _, _, _| Ok(Some(())),
                        &mut probe,
                    )?
                    .is_some()
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    /// Load only partitions that contain a visible value for this row, then
    /// return every vector column owned by that row.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_live_entries_for_row(
        &self,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorEntry>> {
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                if let Some(entry) = column.with_admitted_snapshot_visible_partition_for_row(
                    row_id,
                    snapshot,
                    owner,
                    |_, state, selected, owner| {
                        state.load_visible_entry_by_row_selected(
                            &index, row_id, snapshot, true, selected, owner,
                        )
                    },
                )? {
                    entries.push(entry);
                }
            }
            Ok(entries)
        })
    }

    /// Materialize only partitions containing one of the selected row ids and
    /// return every retained vector version for those rows.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_entries_for_rows(
        &self,
        row_ids: &HashSet<RowId>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorEntry>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.with_bulk_read(|| {
            let mut entries = Vec::new();
            for index in self.scoped_sorted_refs(|_| true, owner)?.into_iter() {
                let Some(column) = self.try_column(&index) else {
                    continue;
                };
                for (_, state) in column.scoped_partition_snapshots(owner)?.into_iter() {
                    let contains_selected_row = if state.is_durable() {
                        state.durable_contains_rows(row_ids.iter().copied(), owner)?
                    } else {
                        state
                            .raw_directory
                            .read()
                            .iter()
                            .any(|entry| row_ids.contains(&entry.row_id))
                    };
                    if !contains_selected_row {
                        continue;
                    }
                    for entry in state.owned_entries(&index, owner)? {
                        let entry = entry?;
                        if row_ids.contains(&entry.row_id) {
                            entries.push(entry);
                        }
                    }
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

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn vector_for_row_lsn(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        lsn: contextdb_core::Lsn,
        row_created_tx: TxId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<Vec<f32>>> {
        self.with_bulk_read(|| {
            let Some(state) = self.try_state(index) else {
                return Ok(None);
            };
            owner.reserve(state.copy_bytes(index, 1))?;
            Ok(state.vector_for_row_lsn(row_id, lsn, row_created_tx, snapshot))
        })
    }

    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn vector_for_row_lsn_in_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
        lsn: Lsn,
        row_created_tx: TxId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<Vec<f32>>> {
        self.with_bulk_read(|| {
            let Some(state) = self.try_partition_state(index, partition_key) else {
                return Ok(None);
            };
            owner.reserve(state.copy_bytes(index, 1))?;
            Ok(state.vector_for_row_lsn(row_id, lsn, row_created_tx, snapshot))
        })
    }

    /// Resolve an exact row version through the lightweight directories and
    /// load only the partition that owns that version.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_vector_for_row_lsn(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        lsn: Lsn,
        row_created_tx: TxId,
        snapshot: SnapshotId,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Option<Vec<f32>>> {
        let Some(column) = self.try_column(index) else {
            return Ok(None);
        };
        column.with_partition_candidates_for_row(
            row_id,
            |_, state, owner| {
                state.load_vector_for_row_lsn(index, row_id, lsn, row_created_tx, snapshot, owner)
            },
            owner,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn storage_bytes_per_entry(
        &self,
        index: &VectorIndexRef,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<usize>> {
        self.with_bulk_read(|| {
            let state = self.state(index)?;
            state.ensure_raw_vectors_loaded(owner)?;
            state.storage_bytes_per_entry(owner)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn storage_bytes_per_entry_in_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<usize>> {
        self.with_bulk_read(|| {
            let state = self.partition_state(index, partition_key)?;
            state.ensure_raw_vectors_loaded(owner)?;
            state.storage_bytes_per_entry(owner)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn live_partition_count(&self, index: &VectorIndexRef) -> Result<usize> {
        if let Some(counts) = self.paged_partition_counts(index)? {
            return Ok(counts[1]);
        }
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.live_partition_count())
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn retained_partition_count(&self, index: &VectorIndexRef) -> Result<usize> {
        if let Some(counts) = self.paged_partition_counts(index)? {
            return Ok(counts[2]);
        }
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn live_or_retained_partition_count(&self, index: &VectorIndexRef) -> Result<usize> {
        if let Some(counts) = self.paged_partition_counts(index)? {
            return Ok(counts[3]);
        }
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.live_or_retained_partition_count())
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn retained_bytes_for_index(&self, index: &VectorIndexRef) -> Result<usize> {
        self.with_bulk_read(|| {
            self.try_column(index)
                .map(|column| column.retained_bytes(index))
                .ok_or_else(|| Error::UnknownVectorIndex {
                    index: index.clone(),
                })
        })
    }

    /// Borrow the metadata credits held by registered durable sources.
    /// Callbacks must not call store or source operations, including reads.
    /// Store guards and resolved lazy-source mutexes remain held during callbacks.
    #[doc(hidden)]
    pub fn visit_retained_copy_credits(
        &self,
        caller: &dyn MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        if let Some(factory) = self.durable_columns.read().as_ref() {
            factory.visit_retained_copy_credits(caller, visit);
        }
        let registry = self.registry.read();
        for (_, column) in registry.iter() {
            if let Some(source) = column.durable_reverse_memberships.read().as_ref() {
                source.visit_retained_copy_credits(caller, visit);
            }
            if let Some(factory) = column.durable_partition_factory.read().as_ref() {
                factory.visit_retained_copy_credits(caller, visit);
            }
            let partitions = column.partitions.read();
            for state in partitions.values() {
                if let Some(source) = state.durable_memberships.read().as_ref() {
                    source.visit_retained_copy_credits(caller, visit);
                }
                if let Some(loader) = state.raw_loader.read().as_ref() {
                    loader.visit_retained_copy_credits(caller, visit);
                }
            }
        }
    }

    /// Disjoint components of the partition-vector charge, in receipt order.
    /// Borrows resident owners only; never clones a handle or changes residency.
    fn partition_vector_components(registry: &VectorRegistry) -> [usize; 13] {
        let mut components = [0usize; 13];
        components[0] = registry
            .bucket_charge
            .as_ref()
            .map_or(0, |charge| charge.bytes);
        let mut identity = registry.identity_charges.as_deref();
        while let Some(node) = identity {
            components[1] = components[1].saturating_add(node.charge.bytes);
            identity = node.next.as_deref();
        }
        for (index, column) in registry.iter() {
            components[2] = components[2].saturating_add(std::mem::size_of::<VectorColumnState>());
            components[3] = components[3]
                .saturating_add(column.registry_copy_bytes.load(Ordering::SeqCst))
                .saturating_add(column.erasure_ledger_bytes.load(Ordering::SeqCst));
            components[4] = components[4].saturating_add(index.table.capacity());
            components[5] = components[5].saturating_add(index.column.capacity());
            components[6] = components[6].saturating_add(column.layout.read().estimated_bytes());
            {
                let partitions = column.partitions.read();
                for _ in partitions.values() {
                    components[7] =
                        components[7].saturating_add(std::mem::size_of::<Arc<IndexState>>());
                    components[8] = components[8].saturating_add(IndexState::allocation_bytes());
                    components[9] = components[9].saturating_add(3 * std::mem::size_of::<usize>());
                }
            }
            components[10] = components[10].saturating_add(
                column
                    .row_to_partition
                    .read()
                    .len()
                    .saturating_mul(std::mem::size_of::<(RowId, Arc<VectorPartitionKey>)>()),
            );
            {
                let history = column.row_partition_history.read();
                components[11] =
                    components[11].saturating_add(history.len().saturating_mul(
                        std::mem::size_of::<(RowId, Vec<Arc<VectorPartitionKey>>)>(),
                    ));
                for keys in history.values() {
                    components[12] = components[12].saturating_add(
                        keys.capacity()
                            .saturating_mul(std::mem::size_of::<Arc<VectorPartitionKey>>()),
                    );
                }
            }
        }
        components
    }

    /// Passive components used by the ownership snapshot: registry buckets,
    /// registry identities, column states, registry copies and prepaid ledgers, table strings,
    /// column strings, layouts, partition handles, partition allocations,
    /// partition map controls, current row directories, historical row
    /// directories, and historical partition-key vectors, in that order.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn partition_vector_components_for_test(&self) -> [usize; 13] {
        Self::partition_vector_components(&self.registry.read())
    }

    /// Allocation-free receipt over the owners already resident in this
    /// store. It deliberately does not call a loader, eviction, or accounting
    /// reconciliation path.
    #[doc(hidden)]
    pub fn memory_ownership_snapshot_for_test(&self) -> VectorMemoryOwnershipSnapshot {
        self.memory_ownership_snapshot()
    }

    fn memory_ownership_snapshot(&self) -> VectorMemoryOwnershipSnapshot {
        let registry = self.registry.read();
        let components = Self::partition_vector_components(&registry);
        let mut receipt = VectorMemoryOwnershipSnapshot {
            temporary_workspace: self.workspace_bytes.load(Ordering::SeqCst),
            raw_vector_bodies: self.raw_payload_bytes.load(Ordering::SeqCst),
            raw_reverse_directory: self.raw_payload_header_bytes.load(Ordering::SeqCst),
            partition_vectors: components[10..]
                .iter()
                .copied()
                .fold(0usize, usize::saturating_add),
            vector_partition_controls: components[..10]
                .iter()
                .copied()
                .fold(0usize, usize::saturating_add),
            ..VectorMemoryOwnershipSnapshot::default()
        };
        for (index, column) in registry.iter() {
            let directory = column.directory_ownership(index);
            receipt.raw_reverse_directory = receipt
                .raw_reverse_directory
                .saturating_add(directory.raw_reverse_directory);
            receipt.typed_partition_keys = receipt
                .typed_partition_keys
                .saturating_add(directory.typed_partition_keys);
            // The failed-partition index owns charged keys independently of
            // each partition's inline maintenance failure marker.
            receipt.vector_partition_controls = receipt.vector_partition_controls.saturating_add(
                column
                    .failed_partitions
                    .lock()
                    .keys
                    .iter()
                    .map(VectorColumnState::failed_partition_entry_bytes)
                    .fold(0usize, usize::saturating_add),
            );
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn account_loaded_state(
        &self,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<usize> {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        self.account_loaded_state_within(accountant, 0)
    }

    /// `account_loaded_state` for a store whose contents were admitted ahead
    /// of their allocation: `reserved` bytes already charged to `accountant`
    /// become the objects' own charges. Only what the objects hold beyond
    /// `reserved` is newly admitted, and a surplus is released. On an error
    /// nothing of `reserved` is touched; the caller still owns it.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn account_loaded_state_within(
        &self,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        reserved: usize,
    ) -> Result<usize> {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        self.account_loaded_state_with_directory_charges(accountant, reserved, None)
    }

    // Received owned bodies stay inline. Each column admits its retained
    // bodies before publication; the executing workspace remains transient.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn account_received_state_with_directory_charges(
        &self,
        mut directory_charges: PreparedDirectoryCharges,
    ) -> Result<()> {
        let registry = self.registry.read();
        let mut body_charges = Vec::with_capacity(registry.len());
        for column in registry.values() {
            let accountant = column.directory_accounting.lock().accountant.clone();
            let bytes = column
                .partitions
                .read()
                .values()
                .fold(0usize, |bytes, state| {
                    bytes.saturating_add(
                        state
                            .vectors
                            .read()
                            .entries
                            .iter()
                            .filter(|entry| !entry.vector.payload_is_charged())
                            .map(|entry| {
                                entry
                                    .estimated_bytes()
                                    .saturating_add(entry.vector.payload_header_bytes())
                            })
                            .sum::<usize>(),
                    )
                });
            accountant.try_allocate_for_static(
                bytes,
                "vector_index",
                "prepare_received_vector_bodies",
                "Reduce received vector bodies or raise MEMORY_LIMIT before publication.",
            )?;
            body_charges.push(PaidRetainedMemoryCharge::new(bytes, accountant));
        }
        for ((index, column), charge) in registry.iter().zip(body_charges.iter_mut()) {
            column.settle_directory_accounting(
                index,
                directory_charges
                    .get_mut(index)
                    .expect("received preparation owns each column directory"),
            );
            for state in column.partitions.read().values() {
                if state.raw_resident.load(Ordering::SeqCst) {
                    state.attach_raw_accounting(
                        charge.accountant.clone(),
                        self.raw_payload_bytes.clone(),
                        self.raw_payload_header_bytes.clone(),
                    );
                }
            }
            // Payload tokens consumed this exact column's reservation.
            charge.bytes = 0;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn account_loaded_state_with_directory_charges(
        &self,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        reserved: usize,
        mut directory_charges: Option<PreparedDirectoryCharges>,
    ) -> Result<usize> {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        self.with_bulk_maintenance(|| {
            if self.memory_accountant.read().is_some() {
                return Err(Error::Other((
                    "vector memory accounting is already attached".to_string()).into()));
            }
            let registry = self.registry.read();
            let mut directory = Vec::with_capacity(registry.len());
            let mut raw = Vec::<(Arc<IndexState>, usize)>::new();
            let mut total = 0usize;
            let mut raw_payloads = HashSet::new();
            let loaded_directories = PreparedDirectoryReservation::empty();
            let mut directory_total = 0usize;
            for (index, column) in registry.iter() {
                let bytes = column.directory_retained_bytes(index);
                directory_total = directory_total.saturating_add(bytes);
                if directory_charges.is_none() {
                    let charge = column.directory_accounting.lock();
                    let growth = bytes.saturating_sub(charge.bytes);
                    if growth != 0 {
                        loaded_directories.reserve_index(index.clone(), charge.accountant.clone(), growth)?;
                        #[cfg(feature = "test-seams")]
                        if charge.bytes == 0 {
                            let membership = column.directory_ownership(index).total()
                                .saturating_sub(column.directory_fixed_bytes(index));
                            contextdb_core::family_loads::loaded(
                                contextdb_core::family_loads::Family::VectorMembership,
                                membership,
                            );
                        }
                    } else {
                        loaded_directories.index_charges.lock().insert(index.clone(), PaidRetainedMemoryCharge::new(0, charge.accountant.clone()));
                    }
                }
                directory.push((index, column.clone(), bytes));
                for (_, state) in column.partition_snapshots() {
                    if !state.raw_resident.load(Ordering::SeqCst) {
                        continue;
                    }
                    // Price every owned body independently, and deduplicate
                    // only real shared Arc identities. No representation,
                    // reservation or counter changes before admission succeeds.
                    let bytes = state.raw_payload_bytes();
                    for entry in &state.vectors.read().entries {
                        match entry.vector.payload_identity() {
                            None => {
                                if !entry.vector.payload_is_charged() {
                                    total = total.saturating_add(entry.estimated_bytes());
                                }
                                total = total.saturating_add(crate::quantized::shared_payload_header_bytes());
                            }
                            Some(identity) if !entry.vector.payload_is_charged() && raw_payloads.insert(identity) => {
                                total = total.saturating_add(entry.estimated_bytes())
                                    .saturating_add(entry.vector.payload_header_bytes());
                            }
                            Some(_) => {}
                        }
                    }
                    // Include empty and already-accounted resident states so
                    // future legacy inserts inherit their body-only owner.
                    raw.push((state, bytes));
                }
            }
            accountant.try_allocate_for_static(
                total.saturating_sub(reserved),
                "open",
                "load_vector_directories",
                "Open the database with a larger MEMORY_LIMIT or reduce retained vector identities.",
            )?;
            let mut loaded_directory_charges = loaded_directories.take();
            for (index, column, _) in directory {
                let charge = if let Some(charges) = directory_charges.as_mut() {
                    charges.get_mut(index)
                } else { loaded_directory_charges.get_mut(index) }
                    .expect("prepared adoption owns every retained directory");
                column.settle_directory_accounting(index, charge);
            }
            for (state, bytes) in raw {
                {
                    let mut vectors = state.vectors.write();
                    for entry in &mut vectors.entries {
                        entry.vector.promote_to_shared_with_header_owner(Some(accountant.clone()));
                    }
                    vectors.rebuild_row_positions();
                }
                state.raw_resident_bytes.store(bytes, Ordering::SeqCst);
                state.attach_raw_accounting(accountant.clone(), self.raw_payload_bytes.clone(), self.raw_payload_header_bytes.clone());
            }
            for column in registry.values() {
                for (_, state) in column.partition_snapshots() {
                    *state.raw_payload_counter.write() = self.raw_payload_bytes.clone();
                    *state.raw_payload_header_counter.write() = self.raw_payload_header_bytes.clone();
                }
            }
            // Adoption's transient owner inventory and deduplication set must
            // be physically gone before their workspace capacity is returned.
            drop(directory_charges.take());
            drop(raw_payloads);
            if reserved > total {
                accountant.release(reserved - total);
            }
            *self.memory_accountant.write() = Some(accountant);
            Ok(total.saturating_add(directory_total))
        })
    }

    /// Whether the current row's partition has adopted allocation-owned raw
    /// reservations. A commit must not return a reservation merely because it
    /// tombstoned one membership; the final shared payload holder releases it.
    pub fn raw_entry_charge_owned_by_state(&self, index: &VectorIndexRef, row_id: RowId) -> bool {
        self.with_bulk_read(|| {
            let Some(column) = self.try_column(index) else {
                return false;
            };
            if column.durable_reverse_memberships.read().is_none() {
                let current = column.row_to_partition.read().get(&row_id).cloned();
                return current.is_some_and(|key| {
                    column
                        .partition_state(&key)
                        .is_some_and(|state| state.raw_residency_is_accounted())
                });
            }
            // Durable-only bodies have no resident per-insert charge to
            // refund. A later lookup may populate an allocation-owned cache;
            // inspecting that body must not manufacture a legacy release.
            column.partitions.read().values().all(|state| {
                if state.raw_residency_is_accounted() {
                    return true;
                }
                let vectors = state.vectors.read();
                !vectors.by_row.get(&row_id).is_some_and(|positions| {
                    positions.iter().any(|position| {
                        let entry = &vectors.entries[*position];
                        entry.deleted_tx.is_none() && !entry.vector.payload_is_charged()
                    })
                })
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn index_infos(&self, owner: &mut VectorCopyAdmission) -> Result<Vec<VectorIndexInfo>> {
        self.with_bulk_read(|| {
            let registry = self.registry.read();
            owner.reserve(
                registry
                    .len()
                    .saturating_mul(std::mem::size_of::<VectorIndexInfo>()),
            )?;
            let mut infos = Vec::with_capacity(registry.len());
            for (index, column) in registry.iter() {
                owner.reserve(index.table.len().saturating_add(index.column.len()))?;
                let (dimension, quantization) = {
                    let layout = column.layout.read();
                    (layout.dimension, layout.quantization)
                };
                infos.push(VectorIndexInfo {
                    index: index.clone(),
                    dimension,
                    quantization,
                    vector_count: column.live_vector_count(),
                    bytes: column.retained_bytes(index),
                });
            }
            drop(registry);
            for info in &mut infos {
                if let Some(counts) = self.paged_partition_counts(&info.index)? {
                    info.vector_count = counts[4];
                }
            }
            infos.sort_unstable_by(|a, b| {
                a.index
                    .table
                    .cmp(&b.index.table)
                    .then(a.index.column.cmp(&b.index.column))
            });
            Ok(infos)
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn index_layout_infos(
        &self,
        owner: &mut VectorCopyAdmission,
    ) -> Result<Vec<VectorIndexLayoutInfo>> {
        self.with_bulk_read(|| {
            let registry = self.registry.read();
            owner.reserve(
                registry
                    .len()
                    .saturating_mul(std::mem::size_of::<VectorIndexLayoutInfo>()),
            )?;
            let mut infos = Vec::with_capacity(registry.len());
            for (index, column) in registry.iter() {
                let layout = column.layout.read();
                let bytes = index
                    .table
                    .len()
                    .saturating_add(index.column.len())
                    .saturating_add(
                        layout
                            .partition_key_columns
                            .len()
                            .saturating_mul(std::mem::size_of::<String>()),
                    )
                    .saturating_add(
                        layout
                            .partition_key_columns
                            .iter()
                            .map(String::len)
                            .sum::<usize>(),
                    );
                owner.reserve(bytes)?;
                let owned_layout = layout.clone();
                drop(layout);
                infos.push(VectorIndexLayoutInfo {
                    index: index.clone(),
                    layout: owned_layout,
                    live_partitions: column.live_partition_count(),
                    retained_partitions: column.retained_partition_count(),
                    live_or_retained_partitions: column.live_or_retained_partition_count(),
                    partition_states: column.maintained_partition_state_count(),
                    bytes: column.retained_bytes(index),
                });
            }
            drop(registry);
            for info in &mut infos {
                if let Some(counts) = self.paged_partition_counts(&info.index)? {
                    info.live_partitions = counts[1];
                    info.retained_partitions = counts[2];
                    info.live_or_retained_partitions = counts[3];
                    info.partition_states = if info.layout.is_partitioned() {
                        counts[3]
                    } else {
                        1
                    };
                }
            }
            infos.sort_unstable_by(|a, b| {
                a.index
                    .table
                    .cmp(&b.index.table)
                    .then(a.index.column.cmp(&b.index.column))
            });
            Ok(infos)
        })
    }

    pub fn build_lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.build_mutex.lock()
    }
}

#[cfg(test)]
mod tests {
    pub(super) fn seed_inline_loaded(
        store: &super::VectorStore,
        entries: Vec<contextdb_core::VectorEntry>,
    ) {
        for entry in entries {
            let index = entry.index.clone();
            let column = store.try_column(&index).unwrap();
            let state = store.state(&index).unwrap();
            let stored = state.stored_entry(entry);
            state.push_entry_with(stored, |entry| {
                column.note_live_insert(&contextdb_core::VectorPartitionKey::unpartitioned(), entry)
            });
            let mut charge = column.directory_accounting.lock();
            let bytes = column.directory_retained_bytes(&index);
            let growth = bytes - charge.bytes;
            charge
                .accountant
                .try_allocate_for(growth, "test", "loaded_fixture_directory", "test")
                .unwrap();
            charge.bytes = bytes;
        }
    }

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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
                    membership: VectorMembershipProvenance::new(
                        TxId(version + 1),
                        Lsn(version + 1),
                        TxId(version + 1),
                        Some(TxId(version + 2)),
                    ),
                });
            }
            directory.push(RawVectorDirectoryEntry {
                row_id: RowId(1),
                created_tx: TxId(history + 1),
                deleted_tx: None,
                lsn: Lsn(history + 1),
                membership: VectorMembershipProvenance::new(
                    TxId(history + 1),
                    Lsn(history + 1),
                    TxId(history + 1),
                    None,
                ),
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
    pub(super) struct CountingMemoryBudget {
        limit: Option<usize>,
        live_bytes: AtomicUsize,
        refuse_directory_growth: AtomicBool,
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
    }

    impl MemoryBudget for CountingMemoryBudget {
        fn try_allocate_for(
            &self,
            bytes: usize,
            subsystem: &str,
            operation: &str,
            hint: &str,
        ) -> Result<()> {
            loop {
                let used = self.live_bytes.load(Ordering::SeqCst);
                let available = if operation == "prepare_vector_directory_growth"
                    && self.refuse_directory_growth.load(Ordering::SeqCst)
                {
                    0
                } else {
                    self.limit
                        .map_or(usize::MAX, |limit| limit.saturating_sub(used))
                };
                if bytes > available {
                    return Err(Error::MemoryBudgetExceeded {
                        subsystem: subsystem.to_owned().into(),
                        operation: operation.to_owned().into(),
                        requested_bytes: bytes,
                        available_bytes: available,
                        budget_limit_bytes: self.limit.unwrap_or(usize::MAX),
                        hint: hint.to_owned().into(),
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
            membership: VectorMembershipProvenance::new(
                entry.created_tx,
                entry.lsn,
                entry.created_tx,
                entry.deleted_tx,
            ),
        }
    }

    impl DormantRawVectorLoader for CandidateOnlyRawLoader {
        fn load(
            &self,
            _directory: &[RawVectorDirectoryEntry],
            _owner: &mut VectorCopyAdmission,
        ) -> Result<LoadedRawVectorPartition> {
            self.bulk_reads.fetch_add(1, Ordering::SeqCst);
            Err(Error::Other(
                ("candidate-only test loader rejects bulk hydration".to_string()).into(),
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn dormant_candidate_scoring_charges_releases_and_cancels_without_bulk_hydration() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

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
            .score_visible_candidate(
                &index,
                RowId(7),
                SnapshotId(3),
                &[1.0, 0.0],
                &mut executing_admission,
            )
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
                &mut executing_admission,
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
            &mut executing_admission,
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
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

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
                .score_visible_candidate(
                    &index,
                    RowId(9),
                    SnapshotId(5),
                    &query,
                    &mut executing_admission,
                )
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

    #[derive(Debug, Default)]
    struct PruningAdmissionBudget {
        ledger: CountingMemoryBudget,
        calls: AtomicUsize,
        fail_at: AtomicUsize,
    }

    impl MemoryBudget for PruningAdmissionBudget {
        fn try_allocate_for(
            &self,
            bytes: usize,
            subsystem: &str,
            operation: &str,
            hint: &str,
        ) -> Result<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_at.load(Ordering::SeqCst) {
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned().into(),
                    operation: operation.to_owned().into(),
                    requested_bytes: bytes,
                    available_bytes: 0,
                    budget_limit_bytes: 0,
                    hint: hint.to_owned().into(),
                });
            }
            self.ledger
                .try_allocate_for(bytes, subsystem, operation, hint)
        }
        fn release(&self, bytes: usize) {
            self.ledger.release(bytes);
        }
    }

    #[test]
    fn standalone_mutation_copy_refusal_preserves_every_column_and_returns_credit() {
        let indexes = [
            VectorIndexRef::new("mutations", "a"),
            VectorIndexRef::new("mutations", "b"),
        ];
        let mut refusals = 0;
        for fail_at in 1..128 {
            let store = VectorStore::default();
            let declared = [
                Arc::new(CountingMemoryBudget::default()),
                Arc::new(CountingMemoryBudget::default()),
            ];
            for (index, owner) in indexes.iter().zip(&declared) {
                store
                    .register_index(index.clone(), 2, VectorQuantization::F32, owner.clone())
                    .unwrap();
            }
            let body_bytes =
                crate::quantized::stored_vector_resident_bytes(2, VectorQuantization::F32);
            for owner in &declared {
                owner
                    .try_allocate_for(body_bytes, "test", "caller_body", "test")
                    .unwrap();
            }
            let before = declared.each_ref().map(|owner| owner.live_bytes());
            let workspace = Arc::new(PruningAdmissionBudget::default());
            workspace.fail_at.store(fail_at, Ordering::SeqCst);
            let result = store.apply_inserts(
                indexes
                    .iter()
                    .map(|index| entry(index, 7, TxId(1), [1.0, 0.0]))
                    .collect(),
                VectorCopyAdmission::new(workspace.clone()),
            );
            assert_eq!(workspace.ledger.live_bytes(), 0);
            if let Err(error) = result {
                assert!(matches!(error, Error::MemoryBudgetExceeded { .. }));
                refusals += 1;
                for index in &indexes {
                    assert_eq!(store.state(index).unwrap().entry_count(), 0);
                }
                assert_eq!(declared.each_ref().map(|owner| owner.live_bytes()), before);
                for owner in &declared {
                    owner.release(body_bytes);
                }
            } else {
                for index in &indexes {
                    assert_eq!(store.state(index).unwrap().entry_count(), 1);
                }
                drop(store);
                assert_eq!(declared.each_ref().map(|owner| owner.live_bytes()), [0, 0]);
                assert!(refusals > 1);
                return;
            }
            drop(store);
            assert_eq!(declared.each_ref().map(|owner| owner.live_bytes()), [0, 0]);
        }
        panic!("positive control must complete after the finite preparation admissions");
    }

    // Sweep every real preparation admission, including the second column.
    // The first nonrefusing run is the positive control; each refused store
    // must also complete an ordinary retry with exact body-release credit.
    fn pruning_refusal_preserves_credit(superseded: bool) {
        let mut refusals = 0;
        for fail_at in 1..64 {
            let store = VectorStore::default();
            let indexes = [
                VectorIndexRef::new("items", "a"),
                VectorIndexRef::new("items", "b"),
            ];
            let declared = [
                Arc::new(CountingMemoryBudget::default()),
                Arc::new(CountingMemoryBudget::default()),
            ];
            let body = Arc::new(CountingMemoryBudget::default());
            let workspace = Arc::new(PruningAdmissionBudget::default());
            let mut inserts = Vec::new();
            for (index, declared) in indexes.iter().zip(&declared) {
                store
                    .register_index(index.clone(), 2, VectorQuantization::F32, declared.clone())
                    .unwrap();
                inserts.push(entry(index, 7, TxId(1), [1.0, 0.0]));
                inserts.push(entry(index, 7, TxId(2), [0.0, 1.0]));
            }
            // Legacy caller-owned bodies exercise the nonzero release receipt.
            // Settle fixture directory growth with each real declared owner,
            // leaving raw bodies under the separate caller admission.
            seed_inline_loaded(&store, inserts);
            for (index, owner) in indexes.iter().zip(&declared) {
                let column = store.try_column(index).unwrap();
                let growth = column.directory_retained_bytes(index)
                    - column.directory_accounting.lock().bytes;
                owner
                    .try_allocate_for(growth, "vector_directory", "fixture_growth", "test")
                    .unwrap();
                let mut reservation = RetainedMemoryCharge {
                    bytes: growth,
                    accountant: owner.clone().into(),
                };
                column.settle_directory_accounting(index, &mut reservation);
            }
            assert_eq!(workspace.ledger.live_bytes(), 0);
            let mut body_bytes = 0;
            let mut removed_bytes = 0;
            for index in &indexes {
                let state = store.state(index).unwrap();
                for entry in &state.vectors.read().entries {
                    body_bytes += entry.estimated_bytes();
                    if !superseded || entry.created_tx == TxId(1) {
                        removed_bytes += entry.estimated_bytes();
                    }
                }
            }
            assert!(removed_bytes > 0);
            body.try_allocate_for(body_bytes, "insert", "caller_body", "test")
                .unwrap();
            let directory_before = declared.each_ref().map(|owner| owner.live_bytes());
            let versions = indexes
                .iter()
                .map(|index| VectorVersionIdentity {
                    index: index.clone(),
                    row_id: RowId(7),
                    created_tx: TxId(1),
                    lsn: Lsn(1),
                })
                .collect::<Vec<_>>();
            workspace.calls.store(0, Ordering::SeqCst);
            workspace.fail_at.store(fail_at, Ordering::SeqCst);
            let mut admission = VectorCopyAdmission::new(workspace.clone());
            let result = if superseded {
                store.prune_superseded_versions(&versions, &mut admission)
            } else {
                store.prune_row_ids(&HashSet::from([RowId(7)]), &mut admission)
            };
            let refused = result.is_err();
            let released = match result {
                Err(error) => {
                    assert!(matches!(error, Error::MemoryBudgetExceeded { .. }));
                    refusals += 1;
                    assert_eq!(body.live_bytes(), body_bytes);
                    assert_eq!(
                        declared.each_ref().map(|owner| owner.live_bytes()),
                        directory_before
                    );
                    for index in &indexes {
                        let state = store.state(index).unwrap();
                        assert_eq!(state.raw_directory.read().len(), 2);
                        assert_eq!(state.vectors.read().entries.len(), 2);
                    }
                    drop(admission);
                    assert_eq!(workspace.ledger.live_bytes(), 0);
                    workspace.fail_at.store(0, Ordering::SeqCst);
                    let mut retry = VectorCopyAdmission::new(workspace.clone());
                    let released = if superseded {
                        store.prune_superseded_versions(&versions, &mut retry)
                    } else {
                        store.prune_row_ids(&HashSet::from([RowId(7)]), &mut retry)
                    }
                    .unwrap();
                    drop(retry);
                    released
                }
                Ok(released) => {
                    drop(admission);
                    released
                }
            };
            assert_eq!(released, removed_bytes);
            body.release(released);
            assert_eq!(body.live_bytes(), body_bytes - removed_bytes);
            assert_eq!(workspace.ledger.live_bytes(), 0);
            for (index, (owner, before)) in
                indexes.iter().zip(declared.iter().zip(directory_before))
            {
                assert!(owner.live_bytes() < before);
                assert_eq!(
                    store.state(index).unwrap().raw_directory.read().len(),
                    usize::from(superseded)
                );
            }
            drop(store);
            body.release(body_bytes - removed_bytes);
            assert_eq!(body.live_bytes(), 0);
            assert_eq!(declared.each_ref().map(|owner| owner.live_bytes()), [0, 0]);
            if !refused {
                assert!(refusals > 1);
                return;
            }
        }
        panic!("positive pruning control was never reached");
    }

    #[test]
    fn row_pruning_second_column_preparation_refusal_preserves_directory_and_body_credit() {
        pruning_refusal_preserves_credit(false);
    }

    #[test]
    fn superseded_pruning_second_column_preparation_refusal_preserves_directory_and_body_credit() {
        pruning_refusal_preserves_credit(true);
    }

    #[derive(Debug, Default)]
    struct PruningRequestBudget {
        ledger: CountingMemoryBudget,
        refuse_bytes: AtomicUsize,
        requests: Mutex<Vec<usize>>,
        /// Live bytes at the moment the refused request was made. Pruning
        /// charges a scratch owner local to the call, which gives its charge
        /// back when the call returns, so what was funded at the refusal is
        /// observed here, not after the call.
        live_at_refusal: AtomicUsize,
        /// The most bytes held at once since the last reset.
        peak: AtomicUsize,
    }

    impl MemoryBudget for PruningRequestBudget {
        fn try_allocate_for(
            &self,
            bytes: usize,
            subsystem: &str,
            operation: &str,
            hint: &str,
        ) -> Result<()> {
            self.requests.lock().push(bytes);
            if bytes != 0 && bytes == self.refuse_bytes.load(Ordering::SeqCst) {
                self.live_at_refusal
                    .store(self.ledger.live_bytes(), Ordering::SeqCst);
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned().into(),
                    operation: operation.to_owned().into(),
                    requested_bytes: bytes,
                    available_bytes: 0,
                    budget_limit_bytes: 0,
                    hint: hint.to_owned().into(),
                });
            }
            self.ledger
                .try_allocate_for(bytes, subsystem, operation, hint)?;
            self.peak
                .fetch_max(self.ledger.live_bytes(), Ordering::SeqCst);
            Ok(())
        }
        #[allow(clippy::needless_borrows_for_generic_args)] // Preserve the existing test expression and assertion contract.
        fn release(&self, bytes: usize) {
            self.ledger.release(bytes);
        }
    }

    fn empty_text_partition_cleanup_is_admitted(superseded: bool) {
        let store = VectorStore::default();
        let index = index();
        let key = text_key("large partition identity".repeat(32768));
        let declared = Arc::new(CountingMemoryBudget::default());
        let workspace = Arc::new(PruningRequestBudget::default());
        let body = Arc::new(CountingMemoryBudget::default());
        let mut registration = VectorCopyAdmission::new(workspace.clone());
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(4),
                declared.clone(),
                &mut registration,
            )
            .unwrap();
        drop(registration);
        let insert = partitioned_entry(&index, &key, 7, TxId(1), [1.0, 0.0]);
        let body_bytes = crate::quantized::stored_vector_resident_bytes(
            insert.entry.vector.len(),
            VectorQuantization::F32,
        );
        body.try_allocate_for(body_bytes, "insert", "caller_body", "test")
            .unwrap();
        store
            .insert_loaded_partitioned_vector(
                insert,
                &mut VectorCopyAdmission::new(workspace.clone()),
            )
            .unwrap();
        let state = store.partition_state(&index, &key).unwrap();
        let column = store.try_column(&index).unwrap();
        let growth =
            column.directory_retained_bytes(&index) - column.directory_accounting.lock().bytes;
        declared
            .try_allocate_for(growth, "test", "loaded_fixture_directory", "test")
            .unwrap();
        let mut charge = RetainedMemoryCharge {
            bytes: growth,
            accountant: declared.clone().into(),
        };
        column.settle_directory_accounting(&index, &mut charge);
        drop(charge);
        // Create an empty partition through the actual directory-removal
        // primitive, retaining the column key and its old row history.
        let sampled = store.directory_bytes_before_reclamation(std::slice::from_ref(&index));
        assert_eq!(state.prune_directory_versions(|_| false), body_bytes);
        body.release(body_bytes);
        store.reconcile_directory_accounting_for_indexes(&sampled);
        assert_eq!(state.entry_count(), 0);
        assert_eq!(state.raw_directory.read().len(), 0);
        assert_eq!(column.partitions.read().len(), 1);
        assert_eq!(column.row_partition_history.read().len(), 1);
        let retained_before = declared.live_bytes();
        assert!(retained_before >= key.estimated_bytes());
        assert_eq!(workspace.ledger.live_bytes(), 0);
        let cleanup_bytes =
            key.estimated_bytes() + 8 * (std::mem::size_of::<VectorPartitionKey>() + 1);
        workspace.requests.lock().clear();
        workspace
            .refuse_bytes
            .store(cleanup_bytes, Ordering::SeqCst);
        let versions = [VectorVersionIdentity {
            index: index.clone(),
            row_id: RowId(7),
            created_tx: TxId(1),
            lsn: Lsn(1),
        }];
        let mut admission = VectorCopyAdmission::new(workspace.clone());
        let result = if superseded {
            store.prune_superseded_versions(&versions, &mut admission)
        } else {
            store.prune_row_ids(&HashSet::from([RowId(7)]), &mut admission)
        };
        assert!(
            matches!(result, Err(Error::MemoryBudgetExceeded { requested_bytes, .. }) if requested_bytes == cleanup_bytes)
        );
        assert!(workspace.requests.lock().contains(&cleanup_bytes));
        assert!(
            workspace.live_at_refusal.load(Ordering::SeqCst) >= key.estimated_bytes(),
            "snapshot key remains funded at cleanup refusal"
        );
        assert_eq!(
            workspace.ledger.live_bytes(),
            0,
            "the refused call's local scratch gives its charge back when it returns"
        );
        assert_eq!(column.partitions.read().len(), 1);
        assert_eq!(column.row_partition_history.read().len(), 1);
        assert_eq!(declared.live_bytes(), retained_before);
        assert_eq!(body.live_bytes(), 0);
        drop(admission);
        assert_eq!(workspace.ledger.live_bytes(), 0);
        workspace.refuse_bytes.store(0, Ordering::SeqCst);
        workspace.requests.lock().clear();
        workspace.peak.store(0, Ordering::SeqCst);
        let mut retry = VectorCopyAdmission::new(workspace.clone());
        let released = if superseded {
            store.prune_superseded_versions(&versions, &mut retry)
        } else {
            store.prune_row_ids(&HashSet::from([RowId(7)]), &mut retry)
        }
        .unwrap();
        assert_eq!(released, 0);
        assert!(workspace.requests.lock().contains(&cleanup_bytes));
        assert!(
            workspace.peak.load(Ordering::SeqCst) >= 2 * key.estimated_bytes(),
            "snapshot and cleanup key allocations are simultaneously funded"
        );
        assert_eq!(column.partitions.read().len(), 0);
        assert_eq!(column.row_partition_history.read().len(), 0);
        assert_eq!(column.row_to_partition.read().len(), 0);
        assert!(declared.live_bytes() < retained_before);
        drop(retry);
        assert_eq!(workspace.ledger.live_bytes(), 0);
        drop(state);
        drop(column);
        drop(store);
        assert_eq!(declared.live_bytes(), 0);
        assert_eq!(body.live_bytes(), 0);
    }

    #[test]
    fn row_pruning_empty_large_text_partition_admits_cleanup_key_before_mutation() {
        empty_text_partition_cleanup_is_admitted(false);
    }

    #[test]
    fn superseded_pruning_empty_large_text_partition_admits_cleanup_key_before_mutation() {
        empty_text_partition_cleanup_is_admitted(true);
    }

    // A catalog record backed by owned bytes. The executing source method
    // admits its actual decoded record before copying or verifying it.
    struct PruningCatalogSource {
        published: RawVectorMembershipSummary,
        record: Vec<u8>,
        reads: AtomicUsize,
    }

    impl PruningCatalogSource {
        fn new(
            published: RawVectorMembershipSummary,
            current: RawVectorMembershipSummary,
            record_bytes: usize,
        ) -> Self {
            assert!(record_bytes >= 24);
            let mut record = vec![0; record_bytes];
            record[..8].copy_from_slice(&(current.entries as u64).to_le_bytes());
            record[8..16].copy_from_slice(&(current.live_entries as u64).to_le_bytes());
            record[16..24].copy_from_slice(&current.max_tx.0.to_le_bytes());
            Self {
                published,
                record,
                reads: AtomicUsize::new(0),
            }
        }
    }

    impl RawVectorMembershipSource for PruningCatalogSource {
        fn published_summary(&self) -> RawVectorMembershipSummary {
            self.published
        }
        fn current_summary(
            &self,
            owner: &mut VectorCopyAdmission,
        ) -> Result<RawVectorMembershipSummary> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let mut scratch = owner.scratch();
            scratch.reserve(self.record.len())?;
            let decoded = self.record.clone();
            assert!(decoded[24..].iter().all(|byte| *byte == 0));
            Ok(RawVectorMembershipSummary {
                entries: u64::from_le_bytes(decoded[..8].try_into().unwrap()) as usize,
                live_entries: u64::from_le_bytes(decoded[8..16].try_into().unwrap()) as usize,
                max_tx: TxId(u64::from_le_bytes(decoded[16..24].try_into().unwrap())),
            })
        }
        fn visible_count(
            &self,
            _: SnapshotId,
            _: &mut dyn FnMut() -> bool,
            _: &mut VectorCopyAdmission,
        ) -> Result<usize> {
            panic!("pruning must use catalog metadata")
        }
        fn visible_by_row(
            &self,
            _: RowId,
            _: SnapshotId,
            _: &mut dyn FnMut() -> bool,
            _: &mut VectorCopyAdmission,
        ) -> Result<Option<RawVectorDirectoryEntry>> {
            panic!("pruning must use catalog metadata")
        }
        fn contains_row_lsn(
            &self,
            _: RowId,
            _: Lsn,
            _: &mut dyn FnMut() -> bool,
            _: &mut VectorCopyAdmission,
        ) -> Result<bool> {
            panic!("pruning must use catalog metadata")
        }
        fn identity_by_row_lsn(
            &self,
            _: RowId,
            _: Lsn,
            _: &mut dyn FnMut() -> bool,
            _: &mut VectorCopyAdmission,
        ) -> Result<Option<RawVectorDirectoryEntry>> {
            panic!("pruning must use catalog metadata")
        }
        fn open(
            &self,
            _: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
        ) -> Result<Box<dyn RawVectorMembershipCursor>> {
            panic!("pruning must use catalog metadata")
        }
    }

    struct PruningResidentLoader {
        entries: Vec<VectorEntry>,
        owner: Arc<CountingMemoryBudget>,
    }

    impl DormantRawVectorLoader for PruningResidentLoader {
        fn load_candidate(
            &self,
            identity: &RawVectorDirectoryEntry,
            caller_charged: bool,
            request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
        ) -> Result<LoadedRawVectorCandidate> {
            let entry = self
                .entries
                .iter()
                .find(|entry| raw_identity(entry) == *identity)
                .ok_or_else(|| Error::Other("pruning fixture candidate identity missing".into()))?;
            if !request(DormantVectorLoadRequest::Checkpoint) {
                return Err(Error::ReadCancelled);
            }
            let bytes = entry.estimated_bytes();
            if caller_charged {
                if !request(DormantVectorLoadRequest::Reserve(bytes)) {
                    return Err(Error::ReadCancelled);
                }
                Ok(LoadedRawVectorCandidate::caller_charged(
                    entry.clone(),
                    bytes,
                ))
            } else {
                self.owner
                    .try_allocate_for(bytes, "vector", "load_pruning_candidate", "test")?;
                Ok(LoadedRawVectorCandidate::loader_charged(
                    entry.clone(),
                    bytes,
                    self.owner.clone(),
                ))
            }
        }

        fn load(
            &self,
            _: &[RawVectorDirectoryEntry],
            owner: &mut VectorCopyAdmission,
        ) -> Result<LoadedRawVectorPartition> {
            self.load_all(owner)
        }
        fn load_all(&self, owner: &mut VectorCopyAdmission) -> Result<LoadedRawVectorPartition> {
            load_co_created_fixture_carriers(
                &self.entries,
                None,
                self.owner.clone(),
                VectorQuantization::F32,
                owner,
            )
        }
    }

    #[test]
    fn durable_hydration_uses_executing_read_workspace_before_writer_body_load() {
        let declared = Arc::new(CountingMemoryBudget::default());
        let writer = Arc::new(CountingMemoryBudget::default());
        let refusing_read = Arc::new(CountingMemoryBudget::with_limit(0));
        let admitted_read = Arc::new(CountingMemoryBudget::default());
        let store = VectorStore::default();
        let index = VectorIndexRef::new("durable_hydration", "embedding");
        store
            .register_index(index.clone(), 2, VectorQuantization::F32, declared.clone())
            .unwrap();
        let state = store.state(&index).unwrap();
        let summary = RawVectorMembershipSummary {
            entries: 1,
            live_entries: 1,
            max_tx: TxId(1),
        };
        let source = Arc::new(PruningCatalogSource::new(summary, summary, 24));
        state.install_durable_source(
            source.clone(),
            Arc::new(PruningResidentLoader {
                entries: vec![entry(&index, 7, TxId(1), [1.0, 0.0])],
                owner: writer.clone(),
            }),
            summary,
            store.raw_payload_bytes.clone(),
            store.raw_payload_header_bytes.clone(),
        );
        let before = store.passive_activity_counters_for_test();
        let mut refusal = VectorCopyAdmission::new(refusing_read.clone());
        assert!(matches!(
            state.ensure_raw_vectors_loaded(&mut refusal),
            Err(Error::MemoryBudgetExceeded { .. })
        ));
        assert!(!state.raw_resident.load(Ordering::SeqCst));
        assert_eq!(writer.live_bytes(), 0);
        assert_eq!(refusing_read.live_bytes(), 0);
        assert_eq!(source.reads.load(Ordering::SeqCst), 0);
        assert_eq!(store.passive_activity_counters_for_test(), before);
        let mut admitted = VectorCopyAdmission::new(admitted_read.clone());
        state.ensure_raw_vectors_loaded(&mut admitted).unwrap();
        assert_eq!(admitted_read.live_bytes(), 0);
        assert_eq!(
            writer.live_bytes(),
            8 + crate::quantized::shared_payload_header_bytes()
        );
        assert!(state.raw_resident.load(Ordering::SeqCst));
        drop(state);
        drop(store);
        assert_eq!(writer.live_bytes(), 0);
        assert_eq!(declared.live_bytes(), 0);
    }

    fn durable_pruning_summary_refusal_is_atomic(superseded: bool) {
        let store = VectorStore::default();
        let indexes = [
            VectorIndexRef::new("items", "a"),
            VectorIndexRef::new("items", "b"),
        ];
        let declared = [
            Arc::new(CountingMemoryBudget::default()),
            Arc::new(CountingMemoryBudget::default()),
        ];
        let bodies = [
            Arc::new(CountingMemoryBudget::default()),
            Arc::new(CountingMemoryBudget::default()),
        ];
        let workspace = Arc::new(PruningRequestBudget::default());
        let published = RawVectorMembershipSummary {
            entries: 2,
            live_entries: 2,
            max_tx: TxId(1),
        };
        let current = RawVectorMembershipSummary {
            entries: 1,
            live_entries: 1,
            max_tx: TxId(3),
        };
        let sources = [
            Arc::new(PruningCatalogSource::new(published, current, 24)),
            Arc::new(PruningCatalogSource::new(published, current, 8191)),
        ];
        let mut states = Vec::new();
        for (((index, declared), body), source) in
            indexes.iter().zip(&declared).zip(&bodies).zip(&sources)
        {
            store
                .register_index(index.clone(), 2, VectorQuantization::F32, declared.clone())
                .unwrap();
            let state = store.state(index).unwrap();
            let loader = Arc::new(PruningResidentLoader {
                entries: vec![
                    entry(index, 7, TxId(1), [1.0, 0.0]),
                    entry(index, 8, TxId(1), [0.0, 1.0]),
                ],
                owner: body.clone(),
            });
            state.install_durable_source(
                source.clone(),
                loader,
                published,
                store.raw_payload_bytes.clone(),
                store.raw_payload_header_bytes.clone(),
            );
            let mut loading_owner = VectorCopyAdmission::new(workspace.clone());
            state.ensure_raw_vectors_loaded(&mut loading_owner).unwrap();
            drop(loading_owner);
            assert!(state.raw_resident.load(Ordering::SeqCst));
            state.apply_durable_pending_events(&[
                DurableMembershipEvent::Opened(TxId(2)),
                DurableMembershipEvent::Closed {
                    created: TxId(1),
                    deleted: TxId(3),
                },
            ]);
            let mut inspection = VectorCopyAdmission::new(workspace.clone());
            assert_eq!(
                state
                    .durable_pending_counts(Some(TxId(1)), None, &mut inspection)
                    .unwrap(),
                (1, 1)
            );
            drop(inspection);
            states.push(state);
        }
        let retained_before = declared.each_ref().map(|owner| owner.live_bytes());
        let bodies_before = bodies.each_ref().map(|owner| owner.live_bytes());
        assert!(bodies_before.iter().all(|bytes| *bytes > 0));
        let pending_before = states
            .iter()
            .map(|state| {
                let log = state.durable_pending.lock();
                (
                    log.floor,
                    log.events.clone(),
                    log.cached
                        .map(|counts| (counts.covered, counts.pending_inserts, counts.tombstones)),
                )
            })
            .collect::<Vec<_>>();
        let vectors_before = states
            .iter()
            .map(|state| {
                state
                    .vectors
                    .read()
                    .entries
                    .iter()
                    .map(|entry| {
                        (
                            entry.row_id,
                            entry.created_tx,
                            entry.lsn,
                            entry.vector.to_f32(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let resident_before = states
            .iter()
            .map(|state| state.raw_resident_bytes.load(Ordering::SeqCst))
            .collect::<Vec<_>>();
        let payload_before = (
            store.raw_payload_bytes.load(Ordering::SeqCst),
            store.raw_payload_header_bytes.load(Ordering::SeqCst),
        );
        workspace.refuse_bytes.store(8191, Ordering::SeqCst);
        let versions = indexes
            .iter()
            .map(|index| VectorVersionIdentity {
                index: index.clone(),
                row_id: RowId(7),
                created_tx: TxId(1),
                lsn: Lsn(1),
            })
            .collect::<Vec<_>>();
        let mut admission = VectorCopyAdmission::new(workspace.clone());
        let result = if superseded {
            store.prune_superseded_versions(&versions, &mut admission)
        } else {
            store.prune_row_ids(&HashSet::from([RowId(7)]), &mut admission)
        };
        assert!(matches!(
            result,
            Err(Error::MemoryBudgetExceeded {
                requested_bytes: 8191,
                ..
            })
        ));
        assert_eq!(
            sources
                .each_ref()
                .map(|source| source.reads.load(Ordering::SeqCst)),
            [1, 1],
            "earlier changed summary was read before later catalog refusal"
        );
        assert_eq!(
            declared.each_ref().map(|owner| owner.live_bytes()),
            retained_before
        );
        assert_eq!(
            bodies.each_ref().map(|owner| owner.live_bytes()),
            bodies_before
        );
        assert_eq!(
            (
                store.raw_payload_bytes.load(Ordering::SeqCst),
                store.raw_payload_header_bytes.load(Ordering::SeqCst)
            ),
            payload_before
        );
        for (position, state) in states.iter().enumerate() {
            assert!(state.raw_resident.load(Ordering::SeqCst));
            assert_eq!(
                state.raw_resident_bytes.load(Ordering::SeqCst),
                resident_before[position]
            );
            assert_eq!(*state.durable_counts.lock(), published);
            assert_eq!(state.entry_count(), published.entries);
            assert_eq!(state.vector_count(), published.live_entries);
            assert_eq!(state.raw_directory.read().len(), 0);
            assert_eq!(
                state
                    .vectors
                    .read()
                    .entries
                    .iter()
                    .map(|entry| (
                        entry.row_id,
                        entry.created_tx,
                        entry.lsn,
                        entry.vector.to_f32()
                    ))
                    .collect::<Vec<_>>(),
                vectors_before[position]
            );
            let log = state.durable_pending.lock();
            assert_eq!(
                (
                    log.floor,
                    log.events.clone(),
                    log.cached.map(|counts| (
                        counts.covered,
                        counts.pending_inserts,
                        counts.tombstones
                    ))
                ),
                pending_before[position]
            );
        }
        drop(admission);
        assert_eq!(workspace.ledger.live_bytes(), 0);
        workspace.refuse_bytes.store(0, Ordering::SeqCst);
        let mut retry = VectorCopyAdmission::new(workspace.clone());
        let released = if superseded {
            store.prune_superseded_versions(&versions, &mut retry)
        } else {
            store.prune_row_ids(&HashSet::from([RowId(7)]), &mut retry)
        }
        .unwrap();
        assert_eq!(
            released, 0,
            "loaded bodies return directly to their actual owners"
        );
        assert_eq!(
            sources
                .each_ref()
                .map(|source| source.reads.load(Ordering::SeqCst)),
            [2, 2]
        );
        assert_eq!(bodies.each_ref().map(|owner| owner.live_bytes()), [0, 0]);
        assert_eq!(
            (
                store.raw_payload_bytes.load(Ordering::SeqCst),
                store.raw_payload_header_bytes.load(Ordering::SeqCst)
            ),
            (0, 0)
        );
        for state in &states {
            assert!(!state.raw_resident.load(Ordering::SeqCst));
            assert_eq!(state.vectors.read().entries.len(), 0);
            assert_eq!(*state.durable_counts.lock(), current);
            assert_eq!(state.entry_count(), current.entries);
            assert_eq!(state.vector_count(), current.live_entries);
            let log = state.durable_pending.lock();
            assert_eq!(log.floor, TxId(u64::MAX));
            assert_eq!(log.events.len(), 0);
            assert!(log.cached.is_none());
        }
        assert_eq!(
            declared.each_ref().map(|owner| owner.live_bytes()),
            retained_before
        );
        drop(retry);
        assert_eq!(workspace.ledger.live_bytes(), 0);
        drop(states);
        drop(store);
        assert_eq!(declared.each_ref().map(|owner| owner.live_bytes()), [0, 0]);
        assert_eq!(bodies.each_ref().map(|owner| owner.live_bytes()), [0, 0]);
    }

    #[test]
    fn row_pruning_later_durable_summary_refusal_preserves_residency_counts_pending_and_owners() {
        durable_pruning_summary_refusal_is_atomic(false);
    }

    #[test]
    fn superseded_pruning_later_durable_summary_refusal_preserves_residency_counts_pending_and_owners()
     {
        durable_pruning_summary_refusal_is_atomic(true);
    }

    #[test]
    fn registration_owner_survives_rename_without_retaining_statement_workspace() {
        let store = VectorStore::default();
        let old = VectorIndexRef::new("items", "v");
        let new = VectorIndexRef::new("items", "longer_embedding_identity");
        let unrelated = VectorIndexRef::new("other", "embedding");
        let declared = Arc::new(CountingMemoryBudget::default());
        let unrelated_owner = Arc::new(CountingMemoryBudget::default());
        let executing = Arc::new(CountingMemoryBudget::default());
        store
            .register_index(old.clone(), 2, VectorQuantization::F32, declared.clone())
            .unwrap();
        store
            .register_index(
                unrelated.clone(),
                2,
                VectorQuantization::F32,
                unrelated_owner.clone(),
            )
            .unwrap();
        assert!(store.memory_accountant.read().is_none());
        let before = declared.live_bytes();
        let unrelated_before = unrelated_owner.live_bytes();
        assert_eq!(
            before,
            store.retained_bytes_for_index(&old).unwrap()
                + store
                    .registry
                    .read()
                    .bucket_charge
                    .as_ref()
                    .map_or(0, |charge| charge.bytes)
        );
        let prepared = store
            .prepare_index_rename(&old, &new, VectorCopyAdmission::new(executing.clone()))
            .unwrap();
        assert!(executing.live_bytes() > 0);
        assert!(declared.live_bytes() > before);
        assert!(unrelated_owner.live_bytes() > unrelated_before);
        drop(prepared);
        assert_eq!(executing.live_bytes(), 0);
        assert_eq!(declared.live_bytes(), before);
        assert_eq!(unrelated_owner.live_bytes(), unrelated_before);
        assert!(store.contains_index(&old));
        assert!(!store.contains_index(&new));
        let prepared = store
            .prepare_index_rename(&old, &new, VectorCopyAdmission::new(executing.clone()))
            .unwrap();
        prepared.publish();
        assert_eq!(executing.live_bytes(), 0);
        assert!(!store.contains_index(&old));
        assert!(store.contains_index(&new));
        assert_eq!(
            declared.live_bytes(),
            store.retained_bytes_for_index(&new).unwrap()
                + store
                    .registry
                    .read()
                    .bucket_charge
                    .as_ref()
                    .filter(|charge| std::ptr::addr_eq(
                        charge.accountant.as_ref(),
                        (declared.clone() as Arc<dyn MemoryBudget>).as_ref()
                    ))
                    .map_or(0, |charge| charge.bytes)
        );
        assert_eq!(
            unrelated_owner.live_bytes(),
            store.retained_bytes_for_index(&unrelated).unwrap()
                + store
                    .registry
                    .read()
                    .bucket_charge
                    .as_ref()
                    .filter(|charge| std::ptr::addr_eq(
                        charge.accountant.as_ref(),
                        (unrelated_owner.clone() as Arc<dyn MemoryBudget>).as_ref()
                    ))
                    .map_or(0, |charge| charge.bytes)
        );
        assert!(unrelated_owner.live_bytes() > unrelated_before);
        let retained_unrelated = store.retained_bytes_for_index(&unrelated).unwrap();
        for _ in 0..3 {
            store
                .prepare_index_rename(&new, &old, VectorCopyAdmission::new(executing.clone()))
                .unwrap()
                .publish();
            assert_eq!(executing.live_bytes(), 0);
            assert_eq!(
                declared.live_bytes(),
                store.retained_bytes_for_index(&old).unwrap()
                    + store
                        .registry
                        .read()
                        .bucket_charge
                        .as_ref()
                        .filter(|charge| std::ptr::addr_eq(
                            charge.accountant.as_ref(),
                            (declared.clone() as Arc<dyn MemoryBudget>).as_ref()
                        ))
                        .map_or(0, |charge| charge.bytes)
            );
            store
                .prepare_index_rename(&old, &new, VectorCopyAdmission::new(executing.clone()))
                .unwrap()
                .publish();
            assert_eq!(executing.live_bytes(), 0);
            assert_eq!(
                declared.live_bytes(),
                store.retained_bytes_for_index(&new).unwrap()
                    + store
                        .registry
                        .read()
                        .bucket_charge
                        .as_ref()
                        .filter(|charge| std::ptr::addr_eq(
                            charge.accountant.as_ref(),
                            (declared.clone() as Arc<dyn MemoryBudget>).as_ref()
                        ))
                        .map_or(0, |charge| charge.bytes)
            );
            assert_eq!(
                unrelated_owner.live_bytes(),
                retained_unrelated
                    + store
                        .registry
                        .read()
                        .bucket_charge
                        .as_ref()
                        .filter(|charge| std::ptr::addr_eq(
                            charge.accountant.as_ref(),
                            (unrelated_owner.clone() as Arc<dyn MemoryBudget>).as_ref()
                        ))
                        .map_or(0, |charge| charge.bytes)
            );
        }
        let retained = store
            .retained_column_copy_owner(&new, &VectorCopyAdmission::new(executing.clone()))
            .unwrap();
        let declared_trait: Arc<dyn MemoryBudget> = declared.clone();
        assert!(std::ptr::addr_eq(
            retained.accountant().as_ref(),
            declared_trait.as_ref()
        ));
        drop(retained);
        drop(store);
        assert_eq!(declared.live_bytes(), 0);
        assert_eq!(unrelated_owner.live_bytes(), 0);
        assert_eq!(executing.live_bytes(), 0);
    }

    #[test]
    fn rename_workspace_refusal_preserves_declared_owner_before_publication() {
        let store = VectorStore::default();
        let old = VectorIndexRef::new("items", "v");
        let new = VectorIndexRef::new("items", "longer_embedding_identity");
        let declared = Arc::new(CountingMemoryBudget::default());
        let executing = Arc::new(CountingMemoryBudget::with_limit(0));
        store
            .register_index(old.clone(), 2, VectorQuantization::F32, declared.clone())
            .unwrap();
        let before = declared.live_bytes();
        assert!(matches!(
            store.prepare_index_rename(&old, &new, VectorCopyAdmission::new(executing.clone())),
            Err(Error::MemoryBudgetExceeded { .. })
        ));
        assert_eq!(declared.live_bytes(), before);
        assert_eq!(executing.live_bytes(), 0);
        assert!(store.contains_index(&old));
        assert!(!store.contains_index(&new));
        drop(store);
        assert_eq!(declared.live_bytes(), 0);
    }

    #[test]
    fn rename_retained_owner_refusal_preserves_all_columns_and_returns_statement_credit() {
        let store = VectorStore::default();
        let old = VectorIndexRef::new("items", "v");
        let new = VectorIndexRef::new("items", "longer_embedding_identity");
        let unrelated = VectorIndexRef::new("other", "embedding");
        let declared = Arc::new(CountingMemoryBudget::default());
        let unrelated_owner = Arc::new(CountingMemoryBudget::default());
        let executing = Arc::new(CountingMemoryBudget::default());
        store
            .register_index(old.clone(), 2, VectorQuantization::F32, declared.clone())
            .unwrap();
        store
            .register_index(
                unrelated.clone(),
                2,
                VectorQuantization::F32,
                unrelated_owner.clone(),
            )
            .unwrap();
        let before = declared.live_bytes();
        let unrelated_before = unrelated_owner.live_bytes();
        declared
            .refuse_directory_growth
            .store(true, Ordering::SeqCst);
        assert!(
            matches!(store.prepare_index_rename(&old, &new, VectorCopyAdmission::new(executing.clone())),
            Err(Error::MemoryBudgetExceeded { ref operation, .. }) if operation == "prepare_vector_directory_growth")
        );
        assert_eq!(declared.live_bytes(), before);
        assert_eq!(unrelated_owner.live_bytes(), unrelated_before);
        assert_eq!(executing.live_bytes(), 0);
        assert!(store.contains_index(&old));
        assert!(store.contains_index(&unrelated));
        assert!(!store.contains_index(&new));
        declared
            .refuse_directory_growth
            .store(false, Ordering::SeqCst);
        store
            .prepare_index_rename(&old, &new, VectorCopyAdmission::new(executing.clone()))
            .unwrap()
            .publish();
        assert_eq!(executing.live_bytes(), 0);
        drop(store);
        assert_eq!(declared.live_bytes(), 0);
        assert_eq!(unrelated_owner.live_bytes(), 0);
    }

    #[test]
    fn unpartitioned_registration_keeps_one_explicit_empty_state() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let _registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        store
            .register_index(
                index.clone(),
                2,
                VectorQuantization::F32,
                declared_column_owner.clone(),
            )
            .unwrap();

        assert!(store.contains_index(&index));
        assert_eq!(
            store
                .partition_keys(&index, &mut executing_admission)
                .unwrap(),
            vec![VectorPartitionKey::unpartitioned()]
        );
        assert_eq!(store.state(&index).unwrap().entry_count(), 0);
        let empty = store
            .index_layout_info(&index, &mut executing_admission)
            .unwrap();
        assert_eq!(empty.partition_states, 1);
        assert_eq!(empty.live_partitions, 0);
        assert_eq!(empty.retained_partitions, 0);
        assert_eq!(empty.live_or_retained_partitions, 0);

        store
            .apply_inserts(
                vec![entry(&index, 1, TxId(1), [1.0, 0.0])],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();
        assert_eq!(
            store
                .entries_for_index(&index, &mut executing_admission)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .current_partition_for_row(&index, RowId(1), &mut executing_admission)
                .unwrap(),
            Some(VectorPartitionKey::unpartitioned())
        );
    }

    #[test]
    fn typed_partition_keys_remain_distinct_and_inspection_separates_retention() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(4),
                declared_column_owner.clone(),
                &mut registration_workspace,
            )
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
                VectorCopyAdmission::new(executing_accountant.clone()),
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
            .apply_partitioned_inserts(
                vec![
                    partitioned_entry(&index, &integer, 1, TxId(1), [1.0, 0.0]),
                    partitioned_entry(&index, &text, 2, TxId(2), [0.0, 1.0]),
                ],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
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
                store
                    .current_partition_for_row(&index, own, &mut executing_admission)
                    .unwrap(),
                Some(key.clone())
            );
            assert_eq!(
                store
                    .live_entry_for_row_in_partition(
                        &index,
                        key,
                        own,
                        SnapshotId(2),
                        &mut executing_admission
                    )
                    .unwrap()
                    .unwrap()
                    .vector,
                vector
            );
            assert!(
                store
                    .live_entry_for_row_in_partition(
                        &index,
                        key,
                        other,
                        SnapshotId(2),
                        &mut executing_admission
                    )
                    .unwrap()
                    .is_none()
            );
        }
        assert!(store.try_partition_state(&index, &integer).is_some());
        assert!(store.try_partition_state(&index, &text).is_some());
        let summary = store
            .index_layout_info(&index, &mut executing_admission)
            .unwrap();
        assert_eq!(summary.live_partitions, 2);
        assert_eq!(summary.retained_partitions, 0);
        assert_eq!(summary.live_or_retained_partitions, 2);
        assert!(
            store
                .partition_infos(&index, &mut executing_admission)
                .unwrap()
                .iter()
                .all(|partition| partition.retained_rows == 0)
        );
        assert!(matches!(
            store
                .register_index_with_layout(
                    index.clone(),
                    partitioned_layout(1),
                    declared_column_owner.clone(),
                    &mut registration_workspace
                )
                .unwrap_err(),
            Error::VectorPartitionLimitExceeded {
                max_partitions: 1,
                ..
            }
        ));
        assert_eq!(
            store
                .index_layout(&index, &mut executing_admission)
                .unwrap()
                .max_partitions,
            Some(4)
        );
    }

    #[test]
    fn partition_cap_rejects_a_whole_multi_key_batch_without_state_creation() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(1),
                declared_column_owner.clone(),
                &mut registration_workspace,
            )
            .unwrap();
        let first = text_key("private-first");
        let second = text_key("private-second");

        let error = store
            .apply_partitioned_inserts(
                vec![
                    partitioned_entry(&index, &first, 1, TxId(1), [1.0, 0.0]),
                    partitioned_entry(&index, &second, 2, TxId(2), [0.0, 1.0]),
                ],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap_err();

        assert!(matches!(
            error,
            Error::VectorPartitionLimitExceeded {
                max_partitions: 1,
                ..
            }
        ));
        assert!(
            store
                .partition_keys(&index, &mut executing_admission)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .entries_for_index(&index, &mut executing_admission)
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.live_or_retained_partition_count(&index).unwrap(), 0);
    }

    #[test]
    fn prepared_move_releases_its_last_unretained_source_at_the_cap() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        let source = text_key("source");
        let target = text_key("target");
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(1),
                declared_column_owner.clone(),
                &mut registration_workspace,
            )
            .unwrap();
        store
            .apply_partitioned_inserts(
                vec![partitioned_entry(&index, &source, 7, TxId(1), [1.0, 0.0])],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
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
                    TxId(1),
                    Lsn(1),
                )],
                HashSet::from([VectorPartitionRef::new(index.clone(), source.clone())]),
                VectorCopyAdmission::new(executing_accountant.clone()),
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

        assert_eq!(
            store
                .partition_keys(&index, &mut executing_admission)
                .unwrap(),
            vec![target.clone()]
        );
        assert_eq!(
            store
                .partition_keys_including_historical(&index, &mut executing_admission)
                .unwrap(),
            vec![source.clone(), target.clone()]
        );
        assert!(store.partition_is_historical_only(&index, &source));
        assert!(
            store
                .partition_info(&index, &source, &mut executing_admission)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .live_entry_for_row_in_partition(
                    &index,
                    &source,
                    RowId(7),
                    SnapshotId(1),
                    &mut executing_admission
                )
                .unwrap()
                .is_some(),
            "retiring the maintained source must preserve its raw historical vector"
        );
        assert_eq!(
            store
                .current_partition_for_row(&index, RowId(7), &mut executing_admission)
                .unwrap(),
            Some(target)
        );
        assert_eq!(store.live_partition_count(&index).unwrap(), 1);
        assert_eq!(store.retained_partition_count(&index).unwrap(), 0);
        assert_eq!(store.live_or_retained_partition_count(&index).unwrap(), 1);
    }

    #[test]
    fn snapshot_free_partition_retirement_reuses_the_retained_key_owner() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
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
                    declared_column_owner.clone(),
                    &mut registration_workspace,
                )
                .unwrap();
            store
                .insert_loaded_partitioned_vector(
                    partitioned_entry(&index, &partition_key, 7, TxId(1), [1.0, 0.0]),
                    &mut VectorCopyAdmission::new(executing_accountant.clone()),
                )
                .unwrap();
            store.account_loaded_state(accountant.clone()).unwrap();
            let charged_before_retirement = accountant.live_bytes();

            store
                .apply_partitioned_deletes(
                    vec![PartitionedVectorDelete::new(
                        index.clone(),
                        partition_key.clone(),
                        RowId(7),
                        TxId(2),
                        TxId(1),
                        Lsn(1),
                    )],
                    VectorCopyAdmission::new(executing_accountant.clone()),
                )
                .unwrap();
            assert_eq!(
                store
                    .retire_snapshot_free_partition_states(&[], &mut executing_admission)
                    .unwrap(),
                1
            );
            assert!(store.partition_is_historical_only(&index, &partition_key));
            assert!(
                accountant.live_bytes() <= charged_before_retirement,
                "retiring maintained state must not allocate a duplicate partition key"
            );

            let released = store
                .prune_row_ids(&HashSet::from([RowId(7)]), &mut executing_admission)
                .unwrap();
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
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        let source = text_key("source");
        let target = text_key("target");
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(1),
                declared_column_owner.clone(),
                &mut registration_workspace,
            )
            .unwrap();
        store
            .apply_partitioned_inserts(
                vec![partitioned_entry(&index, &source, 1, TxId(1), [1.0, 0.0])],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();

        let prepared = store.prepare_partitioned_batch(
            Vec::new(),
            Vec::new(),
            vec![PartitionedVectorMove::new(
                index.clone(),
                source.clone(),
                target.clone(),
                RowId(99),
                RowId(99),
                TxId(2),
                TxId(1),
                Lsn(1),
            )],
            HashSet::new(),
            VectorCopyAdmission::new(executing_accountant.clone()),
        );
        assert!(matches!(
            prepared,
            Err(Error::VectorSourceIdentityMismatch {
                row_id: RowId(99),
                actual_created_tx: None,
                actual_lsn: None,
                ..
            })
        ));
        assert_eq!(executing_accountant.live_bytes(), 0);

        assert_eq!(
            store
                .partition_keys(&index, &mut executing_admission)
                .unwrap(),
            vec![source.clone()]
        );
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
                    TxId(1),
                    Lsn(1),
                )],
                Lsn(3),
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();
        assert_eq!(
            store
                .entries_for_index(&index, &mut executing_admission)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn partition_keys_are_returned_in_typed_tree_order() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(4),
                declared_column_owner.clone(),
                &mut registration_workspace,
            )
            .unwrap();
        let a = text_key("a");
        let m = text_key("m");
        let z = text_key("z");

        store
            .apply_partitioned_inserts(
                vec![
                    partitioned_entry(&index, &z, 3, TxId(3), [1.0, 0.0]),
                    partitioned_entry(&index, &a, 1, TxId(1), [1.0, 0.0]),
                    partitioned_entry(&index, &m, 2, TxId(2), [1.0, 0.0]),
                ],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();

        assert_eq!(
            store
                .partition_keys(&index, &mut executing_admission)
                .unwrap(),
            vec![a, m, z]
        );
    }

    #[test]
    fn row_membership_moves_directly_while_history_stays_in_its_source_state() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        let source = text_key("source");
        let target = text_key("target");
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(4),
                declared_column_owner.clone(),
                &mut registration_workspace,
            )
            .unwrap();
        store
            .apply_partitioned_inserts(
                vec![partitioned_entry(&index, &source, 7, TxId(1), [1.0, 0.0])],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
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
                    TxId(1),
                    Lsn(1),
                )],
                Lsn(2),
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();

        assert_eq!(
            store
                .current_partition_for_row(&index, RowId(7), &mut executing_admission)
                .unwrap(),
            Some(target.clone())
        );
        assert!(
            store
                .live_entry_for_row_in_partition(
                    &index,
                    &source,
                    RowId(7),
                    SnapshotId(1),
                    &mut executing_admission
                )
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store
                .partition_info(&index, &source, &mut executing_admission)
                .unwrap()
                .unwrap()
                .retained_rows,
            1
        );
        assert_eq!(
            store
                .partition_info(&index, &target, &mut executing_admission)
                .unwrap()
                .unwrap()
                .retained_rows,
            0
        );
        assert_eq!(store.live_or_retained_partition_count(&index).unwrap(), 2);

        store
            .apply_partitioned_deletes(
                vec![PartitionedVectorDelete::new(
                    index.clone(),
                    target,
                    RowId(7),
                    TxId(3),
                    TxId(2),
                    Lsn(2),
                )],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();
        assert_eq!(
            store
                .current_partition_for_row(&index, RowId(7), &mut executing_admission)
                .unwrap(),
            None
        );
    }

    #[test]
    fn retained_bytes_include_large_text_partition_keys() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let _executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(4),
                declared_column_owner.clone(),
                &mut registration_workspace,
            )
            .unwrap();
        let before = store.retained_bytes_for_index(&index).unwrap();
        let large = "x".repeat(128 * 1024);
        let key = text_key(large.clone());

        store
            .apply_partitioned_inserts(
                vec![partitioned_entry(&index, &key, 1, TxId(1), [1.0, 0.0])],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();

        let after = store.retained_bytes_for_index(&index).unwrap();
        assert!(after.saturating_sub(before) >= large.len());
    }

    #[test]
    fn recording_a_failure_for_an_absent_partition_is_a_noop() {
        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let mut registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        store
            .record_partition_maintenance_error(
                &VectorPartitionRef::new(index.clone(), text_key("cold-catalog-only")),
                &Error::Other(("safe local test failure".to_owned()).into()),
            )
            .expect("a catalog-only route has no in-memory mark to record");

        store
            .register_index_with_layout(
                index.clone(),
                partitioned_layout(2),
                declared_column_owner.clone(),
                &mut registration_workspace,
            )
            .unwrap();
        store
            .record_partition_maintenance_error(
                &VectorPartitionRef::new(index, text_key("dropped-before-record")),
                &Error::Other(("safe local test failure".to_owned()).into()),
            )
            .expect("a dropped sampled partition cannot abort cycle closing");
    }

    #[test]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn maintenance_failure_marks_are_stable_until_successful_publication() {
        let executing_accountant = Arc::new(CountingMemoryBudget::default());
        let mut executing_admission = VectorCopyAdmission::new(executing_accountant.clone());

        let declared_column_owner = Arc::new(CountingMemoryBudget::default());
        let _registration_workspace = VectorCopyAdmission::new(declared_column_owner.clone());
        let store = VectorStore::default();
        let index = index();
        let partition_key = VectorPartitionKey::unpartitioned();
        let partition = VectorPartitionRef::new(index.clone(), partition_key.clone());
        store
            .register_index(
                index.clone(),
                2,
                VectorQuantization::F32,
                declared_column_owner.clone(),
            )
            .unwrap();
        store
            .apply_inserts(
                vec![entry(&index, 1, TxId(1), [1.0, 0.0])],
                VectorCopyAdmission::new(executing_accountant.clone()),
            )
            .unwrap();

        store
            .record_partition_maintenance_error(&partition, &Error::ReadCancelled)
            .unwrap();
        assert_eq!(
            store
                .partition_info(&index, &partition_key, &mut executing_admission)
                .unwrap()
                .unwrap()
                .maintenance_failure,
            None,
            "stopping before any failure leaves the initial build pending"
        );

        for (error, failure, reason, action) in [
            (
                Error::MemoryBudgetExceeded {
                    subsystem: "vector_index".to_owned().into(),
                    operation: "prepare_vector_generation".to_owned().into(),
                    requested_bytes: 2,
                    available_bytes: 1,
                    budget_limit_bytes: 1,
                    hint: "raise memory".to_owned().into(),
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
                Error::Other(("codec rejected the candidate".to_owned()).into()),
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
            assert!(
                store
                    .partition_info(&index, &partition_key, &mut executing_admission)
                    .unwrap()
                    .unwrap()
                    .maintenance_progress
                    .is_none()
            );
            for _ in 0..2 {
                let info = store
                    .partition_info(&index, &partition_key, &mut executing_admission)
                    .unwrap()
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

        // The engine clears the mark once the matching durable publication has
        // succeeded; nothing short of that clears it.
        store
            .clear_partition_maintenance_failure(&partition)
            .unwrap();
        assert_eq!(
            store
                .partition_info(&index, &partition_key, &mut executing_admission)
                .unwrap()
                .and_then(|info| info.maintenance_failure),
            None,
            "only a successful publication clears the retained failure"
        );
    }
}

#[cfg(test)]
mod transaction_visibility_tests;

#[cfg(test)]
#[path = "store/shared_payload_tests.rs"]
mod shared_payload_tests;

#[cfg(test)]
#[path = "store/received_publication_accounting_tests.rs"]
mod received_publication_accounting_tests;

#[cfg(test)]
#[path = "store/detached_copy_payload_tests.rs"]
mod detached_copy_payload_tests;

#[cfg(test)]
#[path = "store/policy_layout_update_tests.rs"]
mod policy_layout_update_tests;

#[cfg(test)]
#[path = "store/generation_selection_tests.rs"]
mod generation_selection_tests;

#[cfg(test)]
#[path = "store/cdb120_cap_ledger_tests.rs"]
mod cdb120_cap_ledger_tests;

#[cfg(test)]
mod reverse_probe_lifetime_tests {
    use super::*;

    #[derive(Debug)]
    struct ProbeBudget {
        limit: usize,
        live: AtomicUsize,
        peak: AtomicUsize,
    }

    impl MemoryBudget for ProbeBudget {
        fn try_allocate_for(
            &self,
            bytes: usize,
            subsystem: &str,
            operation: &str,
            hint: &str,
        ) -> Result<()> {
            let live = self.live.load(Ordering::SeqCst);
            let available = self.limit.saturating_sub(live);
            if bytes > available {
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned().into(),
                    operation: operation.to_owned().into(),
                    requested_bytes: bytes,
                    available_bytes: available,
                    budget_limit_bytes: self.limit,
                    hint: hint.to_owned().into(),
                });
            }
            self.live.store(live + bytes, Ordering::SeqCst);
            self.peak.fetch_max(live + bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.live.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes, "probe charge must release exactly once");
        }
    }

    struct ProbeSource {
        growth: AtomicUsize,
        cancel: AtomicBool,
        key_materializations: AtomicUsize,
    }

    impl RawVectorReverseMembershipSource for ProbeSource {
        fn visible_partition_admitted(
            &self,
            row: RowId,
            _snapshot: SnapshotId,
            before_read: &mut dyn FnMut() -> bool,
            owner: &mut VectorCopyAdmission,
        ) -> Result<Option<VectorPartitionKey>> {
            if !before_read() {
                return Err(Error::ReadCancelled);
            }
            owner.reserve(4096)?;
            owner.reserve(self.growth.load(Ordering::SeqCst))?;
            if self.cancel.load(Ordering::SeqCst) {
                return Err(Error::ReadCancelled);
            }
            if row != RowId(1) {
                return Ok(None);
            }
            owner.reserve(VectorPartitionKey::unpartitioned().estimated_bytes())?;
            self.key_materializations.fetch_add(1, Ordering::SeqCst);
            Ok(Some(VectorPartitionKey::unpartitioned()))
        }

        fn open_row(
            &self,
            _row: RowId,
            _before_read: &mut dyn FnMut() -> bool,
            _owner: &mut VectorCopyAdmission,
        ) -> Result<Box<dyn RawVectorReverseMembershipCursor>> {
            Err(Error::Other("fixture supports point probes only".into()))
        }
    }

    fn fixture() -> (VectorStore, VectorIndexRef, Arc<ProbeSource>) {
        let store = VectorStore::default();
        let source = Arc::new(ProbeSource {
            growth: AtomicUsize::new(0),
            cancel: AtomicBool::new(false),
            key_materializations: AtomicUsize::new(0),
        });
        let mut entries = Vec::new();
        for column in ["first", "second", "third"] {
            let index = VectorIndexRef::new("probe_rows", column);
            store
                .register_index(
                    index.clone(),
                    2,
                    VectorQuantization::F32,
                    crate::memory_budget::unlimited_memory_budget(),
                )
                .unwrap();
            store
                .register_durable_raw_reverse_source(&index, source.clone())
                .unwrap();
            entries.push(VectorEntry {
                index,
                row_id: RowId(1),
                vector: vec![1.0, 0.0],
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            });
        }
        store
            .apply_inserts(
                entries,
                VectorCopyAdmission::new(Arc::new(
                    crate::store::tests::CountingMemoryBudget::default(),
                )),
            )
            .unwrap();
        (store, VectorIndexRef::new("probe_rows", "first"), source)
    }

    fn owner(limit: usize) -> (Arc<ProbeBudget>, VectorCopyAdmission) {
        let budget = Arc::new(ProbeBudget {
            limit,
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let mut owner = VectorCopyAdmission::new(budget.clone());
        owner.reserve(64).unwrap();
        (budget, owner)
    }

    #[test]
    fn repeated_positive_and_negative_probes_keep_the_callers_charge_constant() {
        let (store, index, source) = fixture();
        let (budget, mut owner) = owner(16 * 1024);
        let snapshot = SnapshotId(1);
        for _ in 0..100_000 {
            for (row, expected) in [(RowId(1), true), (RowId(2), false)] {
                assert_eq!(
                    store
                        .try_has_live_entry_for_row(&index, row, snapshot, &mut owner)
                        .unwrap(),
                    expected,
                );
                assert_eq!(owner.retained_bytes(), 64);
                assert_eq!(budget.live.load(Ordering::SeqCst), 64);
                assert_eq!(
                    store
                        .try_has_any_live_entry_for_row(row, snapshot, &mut owner)
                        .unwrap(),
                    expected,
                );
                assert_eq!(owner.retained_bytes(), 64);
                assert_eq!(budget.live.load(Ordering::SeqCst), 64);
            }
        }
        assert_eq!(source.key_materializations.load(Ordering::SeqCst), 200_000);
        assert!(budget.peak.load(Ordering::SeqCst) <= 16 * 1024);
        drop(owner);
        assert_eq!(budget.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn refused_probe_window_releases_partial_work_before_a_successful_retry() {
        let (store, index, source) = fixture();
        let (budget, mut owner) = owner(8192);
        let snapshot = SnapshotId(1);
        source.growth.store(8192, Ordering::SeqCst);
        for any_column in [false, true] {
            let result = if any_column {
                store.try_has_any_live_entry_for_row(RowId(1), snapshot, &mut owner)
            } else {
                store.try_has_live_entry_for_row(&index, RowId(1), snapshot, &mut owner)
            };
            assert!(matches!(result, Err(Error::MemoryBudgetExceeded { .. })));
            assert_eq!(source.key_materializations.load(Ordering::SeqCst), 0);
            assert_eq!(owner.retained_bytes(), 64);
            assert_eq!(budget.live.load(Ordering::SeqCst), 64);
        }
        source.growth.store(0, Ordering::SeqCst);
        assert!(
            store
                .try_has_live_entry_for_row(&index, RowId(1), snapshot, &mut owner)
                .unwrap()
        );
        assert!(
            store
                .try_has_any_live_entry_for_row(RowId(1), snapshot, &mut owner)
                .unwrap()
        );
        assert_eq!(source.key_materializations.load(Ordering::SeqCst), 2);
        assert_eq!(owner.retained_bytes(), 64);
        assert_eq!(budget.live.load(Ordering::SeqCst), 64);
    }

    #[test]
    fn cancelled_probe_releases_window_and_column_inventory_and_preserves_error() {
        let (store, index, source) = fixture();
        let (budget, mut owner) = owner(8192);
        let snapshot = SnapshotId(1);
        source.cancel.store(true, Ordering::SeqCst);
        assert!(matches!(
            store.try_has_live_entry_for_row(&index, RowId(1), snapshot, &mut owner),
            Err(Error::ReadCancelled),
        ));
        assert!(matches!(
            store.try_has_any_live_entry_for_row(RowId(1), snapshot, &mut owner),
            Err(Error::ReadCancelled),
        ));
        assert_eq!(source.key_materializations.load(Ordering::SeqCst), 0);
        assert_eq!(owner.retained_bytes(), 64);
        assert_eq!(budget.live.load(Ordering::SeqCst), 64);
    }
}
