use crate::quantized::{StoredVector, StoredVectorEntry, quantized_hnsw_distance};
use anndists::dist::distances::{DistCosine, Distance};
use contextdb_core::{Error, Result, RowId, VectorEntry, VectorIndexRef, VectorQuantization};
use hnsw_rs::hnsw::{
    Hnsw, HnswBoundedSearchError, HnswSearchScratchContainer, HnswSearchScratchEvent, LoadedHnsw,
};
pub use hnsw_rs::hnsw::{
    HnswAllowedSearchIncomplete, HnswAllowedSearchLimits, HnswAllowedSearchStatus,
};
use hnsw_rs::hnswio::{decode_owned_graph, encode_owned_graph_into};
use parking_lot::RwLock;
#[cfg(feature = "test-seams")]
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(feature = "test-seams")]
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct HnswIndex {
    hnsw: HnswInner,
    id_to_row: RwLock<HashMap<usize, RowId>>,
    row_to_point: RwLock<BTreeMap<RowId, hnsw_rs::hnsw::PointId>>,
    exact_rows: RwLock<HashMap<Vec<u8>, Vec<RowId>>>,
    next_id: AtomicUsize,
    build_serial: u64,
    dimension: usize,
    quantization: VectorQuantization,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
}

/// Fully validated and encoded work for one fresh-graph insertion. The engine
/// retains this value and its admission until the row transaction is durable.
pub(crate) struct PreparedHnswInsertion {
    build_serial: u64,
    entry: StoredVectorEntry,
    encoded: Option<Vec<u8>>,
    exact_key: Option<Vec<u8>>,
}

/// Final rows from the bounded HNSW path.  `retained_bytes` covers only the
/// returned row vector's observable capacity; every HNSW scratch container
/// has already emitted its matching release event.
#[derive(Debug)]
pub struct HnswSearchMemoryResult {
    pub rows: Vec<(RowId, f32)>,
    pub retained_bytes: usize,
}

/// Final rows and honest traversal state for an allowed-row graph search.
/// Rows accompanying `Incomplete` are diagnostic only and must not be
/// published as a query answer.
#[derive(Debug)]
pub struct HnswAllowedSearchMemoryResult {
    pub rows: Vec<(RowId, f32)>,
    pub retained_bytes: usize,
    pub status: HnswAllowedSearchStatus,
    pub visited_nodes: usize,
    pub vector_evaluations: usize,
    pub allowed_admissions: usize,
}

static NEXT_HNSW_BUILD_SERIAL: AtomicUsize = AtomicUsize::new(1);
const HNSW_BUILD_SEED: u64 = 0x55c8_6f2d_21a4_7bd3;

enum HnswInner {
    F32Mutable(Hnsw<'static, f32, HnswF32Distance>),
    F32Sealed(LoadedHnsw<f32, HnswF32Distance>),
    QuantizedMutable(Hnsw<'static, u8, DistQuantizedCosine>),
    QuantizedSealed(LoadedHnsw<u8, DistQuantizedCosine>),
}

const DURABLE_ENVELOPE_MAGIC: [u8; 8] = *b"CDHNSW\0\x01";
const DURABLE_ENVELOPE_VERSION: u16 = 1;

#[cfg(not(feature = "test-seams"))]
type HnswF32Distance = DistCosine;

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy)]
struct HnswF32Distance;

#[cfg(feature = "test-seams")]
impl Distance<f32> for HnswF32Distance {
    fn eval(&self, va: &[f32], vb: &[f32]) -> f32 {
        observe_hnsw_candidate_distance();
        DistCosine.eval(va, vb)
    }
}

fn hnsw_f32_distance() -> HnswF32Distance {
    #[cfg(feature = "test-seams")]
    {
        HnswF32Distance
    }
    #[cfg(not(feature = "test-seams"))]
    {
        DistCosine
    }
}

struct VectorSearchScratch<'a, E> {
    acquire: &'a mut dyn FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    release: &'a mut dyn FnMut(HnswSearchScratchEvent),
}

impl<E> VectorSearchScratch<'_, E>
where
    E: From<Error>,
{
    fn next_capacity(&self, current: usize, required: usize) -> std::result::Result<usize, E> {
        if current == 0 {
            return Ok(required);
        }
        Ok(current
            .checked_mul(2)
            .ok_or_else(|| E::from(Error::Other("bounded HNSW capacity overflow".to_string())))?
            .max(required))
    }

    fn release_capacity(
        &mut self,
        container: HnswSearchScratchContainer,
        capacity: usize,
        element_bytes: usize,
    ) {
        if capacity != 0 && element_bytes != 0 {
            (self.release)(HnswSearchScratchEvent::Release {
                container,
                capacity,
                element_bytes,
            });
        }
    }

    fn allocate_vec<T>(
        &mut self,
        container: HnswSearchScratchContainer,
        requested_capacity: usize,
        operation: &str,
    ) -> std::result::Result<Vec<T>, E> {
        if requested_capacity == 0 || std::mem::size_of::<T>() == 0 {
            return Ok(Vec::new());
        }
        let element_bytes = std::mem::size_of::<T>();
        (self.acquire)(HnswSearchScratchEvent::Reserve {
            container,
            previous_capacity: 0,
            requested_capacity,
            element_bytes,
        })?;
        let mut values = Vec::new();
        if values.try_reserve_exact(requested_capacity).is_err() {
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(E::from(Error::Other(format!(
                "bounded HNSW {operation} allocation failed"
            ))));
        }
        let actual_capacity = values.capacity();
        if actual_capacity < requested_capacity {
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(E::from(Error::Other(format!(
                "bounded HNSW {operation} capacity moved backwards"
            ))));
        }
        if let Err(error) = (self.acquire)(HnswSearchScratchEvent::Reconcile {
            container,
            requested_capacity,
            actual_capacity,
            element_bytes,
        }) {
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(error);
        }
        Ok(values)
    }

    /// Grow a scratch vector. Each migrated element is charged through the
    /// caller's work control first, exactly as the set migration below does,
    /// so a doubling chain stays inside the work budget and stays cancellable.
    fn ensure_vec_capacity<T: Copy>(
        &mut self,
        values: &mut Vec<T>,
        container: HnswSearchScratchContainer,
        required: usize,
        operation: &str,
        before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if required <= values.capacity() {
            return Ok(());
        }
        let previous_capacity = values.capacity();
        let requested_capacity = self.next_capacity(previous_capacity, required)?;
        let mut replacement = self.allocate_vec(container, requested_capacity, operation)?;
        let mut position = 0usize;
        while position < values.len() {
            if let Err(error) = before_work() {
                self.release_capacity(container, replacement.capacity(), std::mem::size_of::<T>());
                return Err(error);
            }
            let value = match values.get(position) {
                Some(value) => *value,
                None => {
                    self.release_capacity(
                        container,
                        replacement.capacity(),
                        std::mem::size_of::<T>(),
                    );
                    return Err(E::from(Error::Other(format!(
                        "bounded HNSW {operation} changed during migration"
                    ))));
                }
            };
            position = match position.checked_add(1) {
                Some(position) => position,
                None => {
                    self.release_capacity(
                        container,
                        replacement.capacity(),
                        std::mem::size_of::<T>(),
                    );
                    return Err(E::from(Error::Other(format!(
                        "bounded HNSW {operation} migration position overflow"
                    ))));
                }
            };
            replacement.push(value);
        }
        let old = std::mem::replace(values, replacement);
        drop(old);
        self.release_capacity(container, previous_capacity, std::mem::size_of::<T>());
        Ok(())
    }

    fn allocate_set<T>(
        &mut self,
        container: HnswSearchScratchContainer,
        requested_capacity: usize,
        operation: &str,
    ) -> std::result::Result<HashSet<T>, E>
    where
        T: std::hash::Hash + Eq,
    {
        if requested_capacity == 0 || std::mem::size_of::<T>() == 0 {
            return Ok(HashSet::new());
        }
        let element_bytes = std::mem::size_of::<T>();
        (self.acquire)(HnswSearchScratchEvent::Reserve {
            container,
            previous_capacity: 0,
            requested_capacity,
            element_bytes,
        })?;
        let mut values = HashSet::new();
        if values.try_reserve(requested_capacity).is_err() {
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(E::from(Error::Other(format!(
                "bounded HNSW {operation} allocation failed"
            ))));
        }
        let actual_capacity = values.capacity();
        if actual_capacity < requested_capacity {
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(E::from(Error::Other(format!(
                "bounded HNSW {operation} capacity moved backwards"
            ))));
        }
        if let Err(error) = (self.acquire)(HnswSearchScratchEvent::Reconcile {
            container,
            requested_capacity,
            actual_capacity,
            element_bytes,
        }) {
            self.release_capacity(container, requested_capacity, element_bytes);
            return Err(error);
        }
        Ok(values)
    }

    fn ensure_set_capacity<T>(
        &mut self,
        values: &mut HashSet<T>,
        container: HnswSearchScratchContainer,
        required: usize,
        operation: &str,
        before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E>
    where
        T: std::hash::Hash + Eq + Copy,
    {
        if required <= values.capacity() {
            return Ok(());
        }
        let previous_capacity = values.capacity();
        let requested_capacity = self.next_capacity(previous_capacity, required)?;
        let mut replacement = self.allocate_set(container, requested_capacity, operation)?;
        let mut remaining = values.len();
        let mut source = values.iter();
        while remaining != 0 {
            if let Err(error) = before_work() {
                self.release_capacity(container, replacement.capacity(), std::mem::size_of::<T>());
                return Err(error);
            }
            let Some(value) = source.next().copied() else {
                self.release_capacity(container, replacement.capacity(), std::mem::size_of::<T>());
                return Err(E::from(Error::Other(format!(
                    "bounded HNSW {operation} changed during migration"
                ))));
            };
            remaining = match remaining.checked_sub(1) {
                Some(remaining) => remaining,
                None => {
                    self.release_capacity(
                        container,
                        replacement.capacity(),
                        std::mem::size_of::<T>(),
                    );
                    return Err(E::from(Error::Other(format!(
                        "bounded HNSW {operation} migration underflow"
                    ))));
                }
            };
            replacement.insert(value);
        }
        let old = std::mem::replace(values, replacement);
        drop(old);
        self.release_capacity(container, previous_capacity, std::mem::size_of::<T>());
        Ok(())
    }

    fn release_vec<T>(&mut self, values: &Vec<T>, container: HnswSearchScratchContainer) {
        self.release_capacity(container, values.capacity(), std::mem::size_of::<T>());
    }

    fn release_set<T>(&mut self, values: &HashSet<T>, container: HnswSearchScratchContainer) {
        self.release_capacity(container, values.capacity(), std::mem::size_of::<T>());
    }
}

fn map_core_bounded_error<E>(error: HnswBoundedSearchError<E>) -> E
where
    E: From<Error>,
{
    match error {
        HnswBoundedSearchError::Callback(error) => error,
        HnswBoundedSearchError::CapacityOverflow(operation) => E::from(Error::Other(format!(
            "bounded HNSW {operation} capacity overflow"
        ))),
        HnswBoundedSearchError::AllocationFailed(operation) => E::from(Error::Other(format!(
            "bounded HNSW {operation} allocation failed"
        ))),
        HnswBoundedSearchError::CapacityInvariant(operation) => E::from(Error::Other(format!(
            "bounded HNSW {operation} capacity moved backwards"
        ))),
        HnswBoundedSearchError::SearchInvariant(operation) => E::from(Error::Other(format!(
            "bounded HNSW {operation} violated a search invariant"
        ))),
    }
}

fn quantized_query_bounds(query: &[f32]) -> (f32, f32) {
    let Some((first, rest)) = query.split_first() else {
        return (0.0, 0.0);
    };
    rest.iter()
        .copied()
        .fold((*first, *first), |(min, max), value| {
            (min.min(value), max.max(value))
        })
}

fn encode_bounded_quantized_query<E>(
    query: &[f32],
    quantization: VectorQuantization,
    scratch: &mut VectorSearchScratch<'_, E>,
) -> std::result::Result<Vec<u8>, E>
where
    E: From<Error>,
{
    let payload_len = match quantization {
        VectorQuantization::SQ8 => query.len(),
        VectorQuantization::SQ4 => query.len().div_ceil(2),
        VectorQuantization::F32 => {
            return Err(E::from(Error::Other(
                "bounded HNSW requested a quantized query for F32 data".to_string(),
            )));
        }
    };
    let encoded_len = 12usize
        .checked_add(payload_len)
        .ok_or_else(|| E::from(Error::Other("bounded HNSW query size overflow".to_string())))?;
    let mut encoded = scratch.allocate_vec(
        HnswSearchScratchContainer::QueryBytes,
        encoded_len,
        "quantized query",
    )?;
    let (min, max) = quantized_query_bounds(query);
    encoded.extend_from_slice(&(query.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&min.to_le_bytes());
    encoded.extend_from_slice(&max.to_le_bytes());
    let range = max - min;
    match quantization {
        VectorQuantization::SQ8 => {
            for value in query {
                let quantized = if range <= f32::EPSILON {
                    0
                } else {
                    (((*value - min) / range) * 255.0).round().clamp(0.0, 255.0) as u8
                };
                encoded.push(quantized);
            }
        }
        VectorQuantization::SQ4 => {
            let mut values = query.iter();
            while let Some(high) = values.next() {
                let quantize = |value: f32| {
                    if range <= f32::EPSILON {
                        0
                    } else {
                        (((value - min) / range) * 15.0).round().clamp(0.0, 15.0) as u8
                    }
                };
                let high = quantize(*high) & 0x0f;
                let low = values.next().map_or(0, |value| quantize(*value)) & 0x0f;
                encoded.push((high << 4) | low);
            }
        }
        VectorQuantization::F32 => {
            scratch.release_vec(&encoded, HnswSearchScratchContainer::QueryBytes);
            return Err(E::from(Error::Other(
                "bounded HNSW reached the quantized encoder with F32 data".to_string(),
            )));
        }
    }
    Ok(encoded)
}

fn bounded_f32_exact_key<E>(
    query: &[f32],
    scratch: &mut VectorSearchScratch<'_, E>,
) -> std::result::Result<Vec<u8>, E>
where
    E: From<Error>,
{
    let bytes = query
        .len()
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| E::from(Error::Other("bounded HNSW exact-key overflow".to_string())))?;
    let mut key = scratch.allocate_vec(HnswSearchScratchContainer::ExactKey, bytes, "exact key")?;
    for value in query {
        key.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    Ok(key)
}

fn bounded_exact_query_score<E>(
    query: &[f32],
    quantization: VectorQuantization,
    encoded: Option<&[u8]>,
    before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<Option<f32>, E>
where
    E: From<Error>,
{
    before_distance()?;
    let (mut dot, mut query_norm, mut vector_norm) = (0.0_f32, 0.0_f32, 0.0_f32);
    match quantization {
        VectorQuantization::F32 => {
            for value in query {
                dot += *value * *value;
                query_norm += *value * *value;
                vector_norm += *value * *value;
            }
        }
        VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
            let Some(encoded) = encoded else {
                return Ok(None);
            };
            let (min, max) = quantized_query_bounds(query);
            let range = max - min;
            let levels = if matches!(quantization, VectorQuantization::SQ8) {
                255.0
            } else {
                15.0
            };
            let scale = if range <= f32::EPSILON {
                0.0
            } else {
                range / levels
            };
            let payload = encoded.get(12..).unwrap_or_default();
            for (index, value) in query.iter().copied().enumerate() {
                let quantized = match quantization {
                    VectorQuantization::SQ8 => payload.get(index).copied().unwrap_or(0),
                    VectorQuantization::SQ4 => {
                        let byte = payload.get(index / 2).copied().unwrap_or(0);
                        if index.is_multiple_of(2) {
                            byte >> 4
                        } else {
                            byte & 0x0f
                        }
                    }
                    VectorQuantization::F32 => {
                        return Err(E::from(Error::Other(
                            "bounded HNSW scored a quantized query against F32 data".to_string(),
                        )));
                    }
                };
                let stored = if scale == 0.0 {
                    min
                } else {
                    min + quantized as f32 * scale
                };
                dot += value * stored;
                query_norm += value * value;
                vector_norm += stored * stored;
            }
        }
    }
    let score = if query_norm == 0.0 || vector_norm == 0.0 {
        0.0
    } else {
        dot / (query_norm.sqrt() * vector_norm.sqrt())
    };
    Ok((score.is_finite() && score > 0.0).then_some(score))
}

/// Proof-only evidence emitted from the real HNSW distance-evaluation path.
///
/// The private field is intentional: code above the vector source cannot
/// manufacture an event after an eager `Hnsw::search` and pretend that it
/// stopped before candidate work.  Only this module can construct the event,
/// immediately before the candidate distance calculation below.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub struct HnswCandidateDistanceEvent {
    completed_candidates: u64,
    _source_loop_provenance: (),
}

#[cfg(feature = "test-seams")]
impl HnswCandidateDistanceEvent {
    pub const fn completed_candidates(&self) -> u64 {
        self.completed_candidates
    }
}

/// Behavior-neutral handoff for bounded-source poison and budget proofs.
/// Production builds have no observer branch.  Under `test-seams`, installing
/// no observer is also the ordinary path and produces byte-for-byte identical
/// distance results.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub trait HnswCandidateObserver: Send + Sync {
    fn before_candidate_distance(&self, event: HnswCandidateDistanceEvent);
}

#[cfg(feature = "test-seams")]
std::thread_local! {
    static HNSW_CANDIDATE_OBSERVER: RefCell<Option<Arc<dyn HnswCandidateObserver>>> =
        RefCell::new(None);
    static HNSW_COMPLETED_CANDIDATES: Cell<u64> = const { Cell::new(0) };
}

/// Install an observer only for the dynamic extent of one bounded vector
/// source call.  Nested calls and unwinding restore the prior observer and
/// ordinal, so a test request cannot leak state into a later ordinary search.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_hnsw_candidate_observer<R>(
    observer: Arc<dyn HnswCandidateObserver>,
    search: impl FnOnce() -> R,
) -> R {
    struct ObserverGuard {
        prior_observer: Option<Arc<dyn HnswCandidateObserver>>,
        prior_completed: u64,
    }

    impl Drop for ObserverGuard {
        fn drop(&mut self) {
            HNSW_CANDIDATE_OBSERVER.with(|slot| {
                slot.replace(self.prior_observer.take());
            });
            HNSW_COMPLETED_CANDIDATES.with(|completed| {
                completed.set(self.prior_completed);
            });
        }
    }

    let prior_observer = HNSW_CANDIDATE_OBSERVER.with(|slot| slot.replace(Some(observer)));
    let prior_completed = HNSW_COMPLETED_CANDIDATES.with(|completed| completed.replace(0));
    let _guard = ObserverGuard {
        prior_observer,
        prior_completed,
    };
    search()
}

#[cfg(feature = "test-seams")]
fn observe_hnsw_candidate_distance() {
    let observer = HNSW_CANDIDATE_OBSERVER.with(|slot| slot.borrow().as_ref().map(Arc::clone));
    let Some(observer) = observer else {
        return;
    };
    let completed_candidates = HNSW_COMPLETED_CANDIDATES.with(|completed| {
        let current = completed.get();
        completed.set(current.saturating_add(1));
        current
    });
    observer.before_candidate_distance(HnswCandidateDistanceEvent {
        completed_candidates,
        _source_loop_provenance: (),
    });
}

#[derive(Debug, Clone, Copy)]
struct DistQuantizedCosine {
    quantization: VectorQuantization,
}

impl Distance<u8> for DistQuantizedCosine {
    fn eval(&self, va: &[u8], vb: &[u8]) -> f32 {
        #[cfg(feature = "test-seams")]
        observe_hnsw_candidate_distance();
        quantized_hnsw_distance(va, vb, self.quantization)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswGraphStats {
    pub point_count: usize,
    pub layer0_points: usize,
    pub layer0_neighbor_edges: usize,
    pub max_level_observed: u8,
    pub dimension: usize,
}

impl HnswIndex {
    /// Build a fresh mutable graph with the caller's resolved column policy.
    /// Restart uses this for the journal suffix after a sealed generation.
    #[doc(hidden)]
    pub fn from_vector_entries_with_policy(
        entries: &[VectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
    ) -> Self {
        let stored = entries
            .iter()
            .map(|entry| StoredVectorEntry::from_vector_entry_ref(entry, quantization))
            .collect::<Vec<_>>();
        Self::new_with_policy(&stored, dimension, quantization, policy)
    }

    #[doc(hidden)]
    pub fn from_vector_entries_with_progress(
        entries: &[VectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
        mut progress: impl FnMut(usize) -> Result<()>,
    ) -> Result<Self> {
        progress(0)?;
        let stored = entries
            .iter()
            .map(|entry| StoredVectorEntry::from_vector_entry_ref(entry, quantization))
            .collect::<Vec<_>>();
        Self::new_with_policy_and_progress(&stored, dimension, quantization, policy, progress)
    }

    #[cfg(test)]
    pub(crate) fn new(
        entries: &[StoredVectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Self {
        let policy = crate::store::VectorIndexLayout::unpartitioned(dimension, quantization)
            .resolve_policy(entries.len(), 1);
        Self::new_with_policy(entries, dimension, quantization, policy)
    }

    pub(crate) fn new_with_policy(
        entries: &[StoredVectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
    ) -> Self {
        Self::new_with_policy_and_progress(entries, dimension, quantization, policy, |_| Ok(()))
            .expect("infallible graph progress callback")
    }

    pub(crate) fn new_with_policy_and_progress(
        entries: &[StoredVectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
        mut progress: impl FnMut(usize) -> Result<()>,
    ) -> Result<Self> {
        progress(0)?;
        let mut sorted_entries = entries
            .iter()
            .filter(|entry| entry.deleted_tx.is_none())
            .collect::<Vec<_>>();
        sorted_entries.sort_by_key(|entry| {
            (
                entry.lsn,
                entry.created_tx,
                insertion_key(entry),
                entry.row_id,
            )
        });

        let m = policy.hnsw_m;
        let ef_construction = policy.hnsw_ef_construction;
        let ef_search = policy.hnsw_ef_search;
        let max_elements = sorted_entries.len().max(1);
        let hnsw = match quantization {
            VectorQuantization::F32 => {
                let mut hnsw = Hnsw::new_with_seed(
                    m,
                    max_elements,
                    16,
                    ef_construction,
                    hnsw_f32_distance(),
                    HNSW_BUILD_SEED,
                );
                hnsw.set_extend_candidates(true);
                hnsw.set_keeping_pruned(true);
                HnswInner::F32Mutable(hnsw)
            }
            VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
                let mut hnsw = Hnsw::new_with_seed(
                    m,
                    max_elements,
                    16,
                    ef_construction,
                    DistQuantizedCosine { quantization },
                    HNSW_BUILD_SEED,
                );
                hnsw.set_extend_candidates(true);
                hnsw.set_keeping_pruned(true);
                HnswInner::QuantizedMutable(hnsw)
            }
        };
        let mut id_to_row = HashMap::with_capacity(sorted_entries.len());
        let mut row_to_point = BTreeMap::new();
        let mut exact_rows = HashMap::<Vec<u8>, Vec<RowId>>::with_capacity(sorted_entries.len());
        let mut inserted_count = 0usize;

        match &hnsw {
            HnswInner::F32Mutable(index) => {
                for entry in &sorted_entries {
                    let Some(vector) = entry.vector.as_f32_slice() else {
                        continue;
                    };
                    let data_id = inserted_count;
                    inserted_count += 1;
                    id_to_row.insert(data_id, entry.row_id);
                    if let Some(key) = exact_key_for_stored_vector(&entry.vector) {
                        exact_rows.entry(key).or_default().push(entry.row_id);
                    }
                    row_to_point.insert(
                        entry.row_id,
                        index.insert_slice_with_point_id((vector, data_id)),
                    );
                    progress(inserted_count)?;
                }
            }
            HnswInner::QuantizedMutable(index) => {
                for entry in &sorted_entries {
                    let encoded = entry.vector.to_hnsw_u8();
                    if encoded.is_empty() {
                        continue;
                    }
                    let data_id = inserted_count;
                    inserted_count += 1;
                    id_to_row.insert(data_id, entry.row_id);
                    if let Some(key) = exact_key_for_stored_vector(&entry.vector) {
                        exact_rows.entry(key).or_default().push(entry.row_id);
                    }
                    row_to_point.insert(
                        entry.row_id,
                        index.insert_slice_with_point_id((encoded.as_slice(), data_id)),
                    );
                    progress(inserted_count)?;
                }
            }
            HnswInner::F32Sealed(_) | HnswInner::QuantizedSealed(_) => {
                unreachable!("a freshly constructed HNSW graph is always mutable")
            }
        }

        for row_ids in exact_rows.values_mut() {
            row_ids.sort_unstable();
            row_ids.dedup();
        }

        Ok(Self {
            hnsw,
            id_to_row: RwLock::new(id_to_row),
            row_to_point: RwLock::new(row_to_point),
            exact_rows: RwLock::new(exact_rows),
            next_id: AtomicUsize::new(inserted_count),
            build_serial: NEXT_HNSW_BUILD_SERIAL.fetch_add(1, Ordering::SeqCst) as u64,
            dimension,
            quantization,
            m,
            ef_construction,
            ef_search,
        })
    }

    /// Validate and encode an insertion without changing the fresh graph.
    /// The caller admits and retains this work until transaction publication.
    pub(crate) fn prepare_insert(
        &self,
        entry: &StoredVectorEntry,
        additional_inserts: usize,
    ) -> Result<PreparedHnswInsertion> {
        if entry.deleted_tx.is_some() {
            return Err(Error::Other(
                "cannot insert a deleted vector into a mutable HNSW graph".to_string(),
            ));
        }
        if entry.vector.len() != self.dimension {
            return Err(Error::Other(format!(
                "cannot insert vector dimension {} into HNSW dimension {}",
                entry.vector.len(),
                self.dimension
            )));
        }
        let encoded = match (&entry.vector, self.quantization) {
            (StoredVector::F32(_), VectorQuantization::F32) => None,
            (StoredVector::SQ8 { .. }, VectorQuantization::SQ8)
            | (StoredVector::SQ4 { .. }, VectorQuantization::SQ4) => {
                let encoded = entry.vector.to_hnsw_u8();
                if encoded.is_empty() {
                    return Err(Error::Other(
                        "cannot insert an empty quantized vector into HNSW".to_string(),
                    ));
                }
                Some(encoded)
            }
            _ => {
                return Err(Error::Other(
                    "stored vector quantization does not match its HNSW graph".to_string(),
                ));
            }
        };
        let exact_key = exact_key_for_stored_vector(&entry.vector);

        self.next_id
            .load(Ordering::Acquire)
            .checked_add(additional_inserts)
            .ok_or_else(|| Error::Other("HNSW data-id space exhausted".to_string()))?;
        if matches!(
            &self.hnsw,
            HnswInner::F32Sealed(_) | HnswInner::QuantizedSealed(_)
        ) {
            return Err(Error::Other(
                "cannot insert into a sealed durable HNSW graph".to_string(),
            ));
        }
        Ok(PreparedHnswInsertion {
            build_serial: self.build_serial,
            entry: entry.clone(),
            encoded,
            exact_key,
        })
    }

    pub(crate) fn publish_insert(&self, prepared: PreparedHnswInsertion) {
        let PreparedHnswInsertion {
            build_serial,
            entry,
            encoded,
            exact_key,
        } = prepared;
        assert_eq!(
            self.build_serial, build_serial,
            "prepared insertion belongs to this fresh graph"
        );
        // The mapping lock serializes id assignment and stays held through the
        // vendor insertion. A concurrent search that sees the new graph point
        // therefore cannot observe it without its external row mapping.
        let mut id_to_row = self.id_to_row.write();
        let mut exact_rows = self.exact_rows.write();
        let data_id = self.next_id.load(Ordering::Relaxed);
        let next_id = data_id + 1;

        match (&self.hnsw, &entry.vector, encoded.as_deref()) {
            (HnswInner::F32Mutable(index), StoredVector::F32(vector), None) => {
                self.row_to_point.write().insert(
                    entry.row_id,
                    index.insert_slice_with_point_id((vector, data_id)),
                );
            }
            (HnswInner::QuantizedMutable(index), _, Some(vector)) => {
                self.row_to_point.write().insert(
                    entry.row_id,
                    index.insert_slice_with_point_id((vector, data_id)),
                );
            }
            _ => unreachable!("prepared HNSW insertion retains its validated representation"),
        }

        id_to_row.insert(data_id, entry.row_id);
        if let Some(key) = exact_key {
            let rows = exact_rows.entry(key).or_default();
            if let Err(position) = rows.binary_search(&entry.row_id) {
                rows.insert(position, entry.row_id);
            }
        }
        self.next_id.store(next_id, Ordering::Release);
    }

    #[allow(dead_code)]
    pub(crate) fn insert(&self, entry: &StoredVectorEntry) -> Result<()> {
        let prepared = self.prepare_insert(entry, 1)?;
        self.publish_insert(prepared);
        Ok(())
    }

    /// Produce one ContextDB durable envelope for a graph built in this
    /// process.  It includes the vendor topology and point values together
    /// with the row and exact-vector maps, so loading never scans the vector
    /// store or rebuilds a graph.
    #[doc(hidden)]
    pub fn encode_durable_generation(&self) -> Result<Vec<u8>> {
        // One buffer owns the envelope, graph and maps. Backfill the length
        // and checksum after appending their bytes; never retain separate
        // graph, payload and envelope copies at the same time.
        let mut envelope = Vec::new();
        envelope.extend_from_slice(&DURABLE_ENVELOPE_MAGIC);
        envelope.extend_from_slice(&DURABLE_ENVELOPE_VERSION.to_le_bytes());
        let checksum_offset = envelope.len();
        write_u64(&mut envelope, 0);
        let payload_offset = envelope.len();
        write_u64(&mut envelope, self.dimension as u64);
        write_u8(&mut envelope, quantization_tag(self.quantization));
        write_u64(&mut envelope, self.ef_search as u64);
        let graph_length_offset = envelope.len();
        write_u64(&mut envelope, 0);
        let graph_offset = envelope.len();
        match &self.hnsw {
            HnswInner::F32Mutable(graph) => encode_owned_graph_into(graph, &mut envelope),
            HnswInner::QuantizedMutable(graph) => encode_owned_graph_into(graph, &mut envelope),
            HnswInner::F32Sealed(_) | HnswInner::QuantizedSealed(_) => {
                return Err(Error::Other(
                    "a sealed durable HNSW graph is already a generation and cannot be re-encoded"
                        .to_string(),
                ));
            }
        }
        .map_err(|error| Error::Other(format!("could not encode durable HNSW graph: {error}")))?;
        let graph_length = u64::try_from(envelope.len() - graph_offset)
            .map_err(|_| Error::Other("durable HNSW byte length exceeds u64".to_string()))?;
        envelope[graph_length_offset..graph_offset].copy_from_slice(&graph_length.to_le_bytes());
        let id_to_row = self.id_to_row.read();
        let mut rows = id_to_row
            .iter()
            .map(|(data_id, row_id)| (*data_id, row_id.0))
            .collect::<Vec<_>>();
        rows.sort_unstable_by_key(|(data_id, _)| *data_id);
        let exact_rows = self.exact_rows.read();
        // Construction and insertion keep each row list sorted. Borrow the
        // keys and lists while ordering map entries for deterministic bytes.
        let mut exact = exact_rows.iter().collect::<Vec<_>>();
        exact.sort_unstable_by(|left, right| left.0.cmp(right.0));

        write_u64(&mut envelope, rows.len() as u64);
        for (data_id, row_id) in rows {
            write_u64(&mut envelope, data_id as u64);
            write_u64(&mut envelope, row_id);
        }
        write_u64(&mut envelope, exact.len() as u64);
        for (key, row_ids) in exact {
            write_bytes(&mut envelope, key)?;
            write_u64(&mut envelope, row_ids.len() as u64);
            for row_id in row_ids {
                write_u64(&mut envelope, row_id.0);
            }
        }
        let checksum = durable_checksum(&envelope[payload_offset..]);
        envelope[checksum_offset..payload_offset].copy_from_slice(&checksum.to_le_bytes());
        Ok(envelope)
    }

    /// Load and seal one ContextDB durable graph generation.  A caller must
    /// provide the column definition it is loading for; incompatible
    /// dimension or quantization is refused before a graph is exposed.
    #[doc(hidden)]
    pub fn decode_durable_generation(
        bytes: &[u8],
        expected_dimension: usize,
        expected_quantization: VectorQuantization,
    ) -> Result<Self> {
        let mut reader = DurableReader::new(bytes);
        if reader.read_exact(DURABLE_ENVELOPE_MAGIC.len())? != DURABLE_ENVELOPE_MAGIC.as_slice() {
            return Err(Error::Other(
                "invalid durable HNSW envelope magic".to_string(),
            ));
        }
        if reader.read_u16()? != DURABLE_ENVELOPE_VERSION {
            return Err(Error::Other(
                "unsupported durable HNSW envelope version".to_string(),
            ));
        }
        let expected_checksum = reader.read_u64()?;
        let payload = reader.remaining();
        if durable_checksum(payload) != expected_checksum {
            return Err(Error::Other(
                "durable HNSW envelope checksum mismatch".to_string(),
            ));
        }
        let mut payload = DurableReader::new(payload);
        let dimension = payload.read_usize()?;
        if dimension != expected_dimension {
            return Err(Error::Other(format!(
                "durable HNSW dimension {dimension} does not match expected dimension {expected_dimension}"
            )));
        }
        let quantization = quantization_from_tag(payload.read_u8()?)?;
        if quantization != expected_quantization {
            return Err(Error::Other(
                "durable HNSW quantization does not match the vector column".to_string(),
            ));
        }
        let ef_search = payload.read_usize()?;
        if ef_search == 0 {
            return Err(Error::Other(
                "durable HNSW has an invalid search breadth".to_string(),
            ));
        }
        let graph_bytes = payload.read_bytes()?;
        let row_count = payload.read_usize()?;
        if row_count > payload.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW row mapping length is invalid".to_string(),
            ));
        }
        let mut id_to_row = HashMap::with_capacity(row_count);
        let mut previous_data_id = None;
        for _ in 0..row_count {
            let data_id = payload.read_usize()?;
            if previous_data_id.is_some_and(|previous| data_id <= previous)
                || id_to_row
                    .insert(data_id, RowId(payload.read_u64()?))
                    .is_some()
            {
                return Err(Error::Other(
                    "durable HNSW has invalid row mappings".to_string(),
                ));
            }
            previous_data_id = Some(data_id);
        }
        let exact_count = payload.read_usize()?;
        if exact_count > payload.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW exact mapping length is invalid".to_string(),
            ));
        }
        let mut exact_rows = HashMap::with_capacity(exact_count);
        let mut previous_key: Option<Vec<u8>> = None;
        for _ in 0..exact_count {
            let key = payload.read_bytes()?.to_vec();
            if key.is_empty()
                || previous_key
                    .as_ref()
                    .is_some_and(|previous| key <= *previous)
            {
                return Err(Error::Other(
                    "durable HNSW has invalid exact-vector mappings".to_string(),
                ));
            }
            let count = payload.read_usize()?;
            if count > payload.remaining().len() / 8 {
                return Err(Error::Other(
                    "durable HNSW exact row length is invalid".to_string(),
                ));
            }
            let mut rows = Vec::with_capacity(count);
            let mut previous_row = None;
            for _ in 0..count {
                let row = RowId(payload.read_u64()?);
                if previous_row.is_some_and(|previous| row <= previous) {
                    return Err(Error::Other(
                        "durable HNSW exact-vector rows are not ordered".to_string(),
                    ));
                }
                previous_row = Some(row);
                rows.push(row);
            }
            if rows.is_empty() || exact_rows.insert(key.clone(), rows).is_some() {
                return Err(Error::Other(
                    "durable HNSW has duplicate exact-vector mappings".to_string(),
                ));
            }
            previous_key = Some(key);
        }
        if !payload.is_empty() {
            return Err(Error::Other(
                "durable HNSW envelope has trailing payload bytes".to_string(),
            ));
        }
        let hnsw = match quantization {
            VectorQuantization::F32 => HnswInner::F32Sealed(
                decode_owned_graph(graph_bytes, hnsw_f32_distance()).map_err(|error| {
                    Error::Other(format!("could not load durable F32 HNSW graph: {error}"))
                })?,
            ),
            VectorQuantization::SQ8 | VectorQuantization::SQ4 => HnswInner::QuantizedSealed(
                decode_owned_graph(graph_bytes, DistQuantizedCosine { quantization }).map_err(
                    |error| {
                        Error::Other(format!(
                            "could not load durable quantized HNSW graph: {error}"
                        ))
                    },
                )?,
            ),
        };
        let graph_points = match &hnsw {
            HnswInner::F32Sealed(graph) => graph.get_nb_point(),
            HnswInner::QuantizedSealed(graph) => graph.get_nb_point(),
            HnswInner::F32Mutable(_) | HnswInner::QuantizedMutable(_) => unreachable!(),
        };
        if graph_points != id_to_row.len() {
            return Err(Error::Other(
                "durable HNSW graph point count does not match row mappings".to_string(),
            ));
        }
        let mapped_points_match = match &hnsw {
            HnswInner::F32Sealed(graph) => graph
                .get_point_indexation()
                .into_iter()
                .all(|point| id_to_row.contains_key(&point.get_origin_id())),
            HnswInner::QuantizedSealed(graph) => graph
                .get_point_indexation()
                .into_iter()
                .all(|point| id_to_row.contains_key(&point.get_origin_id())),
            HnswInner::F32Mutable(_) | HnswInner::QuantizedMutable(_) => unreachable!(),
        };
        if !mapped_points_match {
            return Err(Error::Other(
                "durable HNSW graph points do not match row mappings".to_string(),
            ));
        }
        let mut row_to_point = BTreeMap::new();
        match &hnsw {
            HnswInner::F32Sealed(graph) => {
                for point in graph.get_point_indexation() {
                    row_to_point.insert(id_to_row[&point.get_origin_id()], point.get_point_id());
                }
            }
            HnswInner::QuantizedSealed(graph) => {
                for point in graph.get_point_indexation() {
                    row_to_point.insert(id_to_row[&point.get_origin_id()], point.get_point_id());
                }
            }
            _ => unreachable!(),
        }
        let (m, ef_construction) = match &hnsw {
            HnswInner::F32Sealed(graph) => {
                (graph.get_max_nb_connection(), graph.get_ef_construction())
            }
            HnswInner::QuantizedSealed(graph) => {
                (graph.get_max_nb_connection(), graph.get_ef_construction())
            }
            _ => unreachable!(),
        };
        Ok(Self {
            hnsw,
            id_to_row: RwLock::new(id_to_row),
            row_to_point: RwLock::new(row_to_point),
            exact_rows: RwLock::new(exact_rows),
            next_id: AtomicUsize::new(graph_points),
            build_serial: NEXT_HNSW_BUILD_SERIAL.fetch_add(1, Ordering::SeqCst) as u64,
            dimension,
            quantization,
            m,
            ef_construction,
            ef_search,
        })
    }

    /// Read the allocation bound from the existing envelope without decoding
    /// or retaining points. Old catalogs may contain an earlier charge formula.
    #[doc(hidden)]
    pub fn durable_resident_bound(
        bytes: &[u8],
        dimension: usize,
        quantization: VectorQuantization,
        m: usize,
    ) -> Result<usize> {
        let mut reader = DurableReader::new(bytes);
        if reader.read_exact(8)? != DURABLE_ENVELOPE_MAGIC
            || reader.read_u16()? != DURABLE_ENVELOPE_VERSION
        {
            return Err(Error::Other("invalid durable HNSW envelope".to_string()));
        }
        let checksum = reader.read_u64()?;
        if durable_checksum(reader.remaining()) != checksum {
            return Err(Error::Other(
                "durable HNSW envelope checksum mismatch".to_string(),
            ));
        }
        if reader.read_usize()? != dimension
            || quantization_from_tag(reader.read_u8()?)? != quantization
        {
            return Err(Error::Other(
                "durable HNSW declaration mismatch".to_string(),
            ));
        }
        reader.read_usize()?;
        let graph = reader.read_bytes()?;
        // The pinned owned-graph codec starts with its u16 version then its
        // fixed-width usize connection limit. Check it before graph allocation.
        let mut topology = DurableReader::new(graph);
        topology.read_u16()?;
        if topology.read_usize()? != m {
            return Err(Error::Other(
                "durable HNSW topology disagrees with its catalog".to_string(),
            ));
        }
        let count = reader.read_usize()?;
        if count > reader.remaining().len() / 16 {
            return Err(Error::Other(
                "durable HNSW row mapping length is invalid".to_string(),
            ));
        }
        Ok(Self::estimated_resident_bytes_with_m(
            count,
            dimension,
            quantization,
            m,
        ))
    }

    /// Number of vectors currently indexed in the HNSW graph.
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[doc(hidden)]
    pub fn policy_values(&self) -> (usize, usize, usize) {
        (self.m, self.ef_construction, self.ef_search)
    }

    /// The same ownership estimate used for graph admission. Durable catalog
    /// metadata records this value so lazy restart can reserve memory before
    /// decoding graph bytes.
    #[doc(hidden)]
    pub fn estimated_resident_bytes(&self) -> usize {
        Self::estimated_resident_bytes_with_m(self.len(), self.dimension, self.quantization, self.m)
    }

    #[doc(hidden)]
    pub fn estimated_build_reservation(
        count: usize,
        dimension: usize,
        quantization: VectorQuantization,
        policy: crate::store::ResolvedVectorPolicy,
    ) -> usize {
        crate::mem::estimate_hnsw_build_reservation(count, dimension, quantization, policy)
    }

    #[doc(hidden)]
    pub fn estimated_resident_bytes_for_count(
        count: usize,
        dimension: usize,
        quantization: VectorQuantization,
    ) -> usize {
        let policy = crate::store::VectorIndexLayout::unpartitioned(dimension, quantization)
            .resolve_policy(count, 1);
        Self::estimated_resident_bytes_with_m(count, dimension, quantization, policy.hnsw_m)
    }

    /// Conservative retained ownership bound: point values and lookup maps,
    /// all possible layer headers, and neighbour capacity for the resolved M.
    /// The bottom layer keeps 2*M links; the other fifteen keep M each.
    #[doc(hidden)]
    pub fn estimated_resident_bytes_with_m(
        count: usize,
        dimension: usize,
        quantization: VectorQuantization,
        m: usize,
    ) -> usize {
        let entry_bytes = match quantization {
            VectorQuantization::F32 => quantization.storage_bytes(dimension),
            VectorQuantization::SQ8 => dimension.saturating_add(12),
            VectorQuantization::SQ4 => dimension.div_ceil(2).saturating_add(12),
        };
        let exact_key_bytes = entry_bytes
            .saturating_add(std::mem::size_of::<RowId>())
            .saturating_add(64);
        std::mem::size_of::<Self>()
            .saturating_add(16 * std::mem::size_of::<Vec<usize>>())
            .saturating_add(std::mem::size_of::<usize>())
            .saturating_add(
                count.saturating_mul(
                    entry_bytes
                        .saturating_mul(3)
                        .saturating_add(exact_key_bytes)
                        .saturating_add(256)
                        .saturating_add(16 * std::mem::size_of::<Vec<usize>>())
                        .saturating_add(
                            m.saturating_mul(17)
                                .saturating_mul(2 * std::mem::size_of::<usize>()),
                        ),
                ),
            )
    }

    #[doc(hidden)]
    pub fn graph_stats(&self) -> HnswGraphStats {
        let (point_count, layer0_neighbor_edges, max_level_observed) = match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => hnsw_stats(hnsw),
            HnswInner::F32Sealed(hnsw) => hnsw_loaded_stats(hnsw),
            HnswInner::QuantizedMutable(hnsw) => hnsw_stats(hnsw),
            HnswInner::QuantizedSealed(hnsw) => hnsw_loaded_stats(hnsw),
        };

        HnswGraphStats {
            point_count,
            layer0_points: point_count,
            layer0_neighbor_edges,
            max_level_observed,
            dimension: self.dimension,
        }
    }

    fn original_query_score(&self, point: &hnsw_rs::hnsw::PointId, query: &[f32]) -> Option<f32> {
        let score = |stored: &[f32]| {
            #[cfg(feature = "test-seams")]
            observe_hnsw_candidate_distance();
            crate::cosine_similarity(stored, query)
        };
        match &self.hnsw {
            HnswInner::F32Mutable(graph) => {
                graph.get_point_indexation().with_point_data(point, score)
            }
            HnswInner::F32Sealed(graph) => {
                graph.get_point_indexation().with_point_data(point, score)
            }
            HnswInner::QuantizedMutable(graph) => graph
                .get_point_indexation()
                .with_point_data(point, |stored| {
                    #[cfg(feature = "test-seams")]
                    observe_hnsw_candidate_distance();
                    crate::quantized::quantized_hnsw_query_score(stored, query, self.quantization)
                })
                .flatten(),
            HnswInner::QuantizedSealed(graph) => graph
                .get_point_indexation()
                .with_point_data(point, |stored| {
                    #[cfg(feature = "test-seams")]
                    observe_hnsw_candidate_distance();
                    crate::quantized::quantized_hnsw_query_score(stored, query, self.quantization)
                })
                .flatten(),
        }
    }

    pub fn search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        self.search_with_ef_search(index, query, k, self.ef_search)
    }

    pub(crate) fn search_with_ef_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<(RowId, f32)>> {
        if k == 0 {
            return Ok(Vec::new());
        }

        let got = query.len();
        if got != self.dimension {
            return Err(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: self.dimension,
                actual: got,
            });
        }

        let ef = hnsw_search_ef(ef_search, k);
        let neighbors = match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => hnsw.search(query, ef, ef),
            HnswInner::F32Sealed(hnsw) => hnsw.search(query, ef, ef),
            HnswInner::QuantizedMutable(hnsw) => {
                let encoded = StoredVector::from_f32(query, self.quantization).to_hnsw_u8();
                hnsw.search(&encoded, ef, ef)
            }
            HnswInner::QuantizedSealed(hnsw) => {
                let encoded = StoredVector::from_f32(query, self.quantization).to_hnsw_u8();
                hnsw.search(&encoded, ef, ef)
            }
        };
        let id_to_row = self.id_to_row.read();
        let cap = hnsw_search_candidate_cap(ef_search, k);
        let mut scored = Vec::with_capacity(
            cap.saturating_add(neighbors.len())
                .min(cap.saturating_mul(2)),
        );
        if let Some((key, exact_score)) = exact_key_for_query(query, self.quantization)
            && let Some(row_ids) = self.exact_rows.read().get(&key)
        {
            scored.extend(
                row_ids
                    .iter()
                    .take(cap)
                    .map(|row_id| (*row_id, exact_score)),
            );
        }

        scored.extend(neighbors.into_iter().filter_map(|neighbor| {
            id_to_row.get(&neighbor.d_id).copied().map(|row_id| {
                (
                    row_id,
                    self.original_query_score(&neighbor.p_id, query)
                        .unwrap_or(1.0 - neighbor.distance),
                )
            })
        }));
        scored.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let mut seen = HashSet::new();
        scored.retain(|(row_id, _)| seen.insert(*row_id));
        scored.truncate(cap);
        Ok(scored)
    }

    /// Additive bounded HNSW search with transactional, exact-capacity events.
    #[allow(clippy::too_many_arguments)]
    pub fn search_with_bounded_memory<E, S, D, A, R>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        before_source_entry: S,
        before_distance: D,
        acquire: A,
        release: R,
    ) -> std::result::Result<HnswSearchMemoryResult, E>
    where
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        self.search_with_bounded_memory_at_ef(
            index,
            query,
            k,
            self.ef_search,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_with_bounded_memory_at_ef<E, S, D, A, R>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        ef_search: usize,
        mut before_source_entry: S,
        mut before_distance: D,
        mut acquire: A,
        mut release: R,
    ) -> std::result::Result<HnswSearchMemoryResult, E>
    where
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        if k == 0 {
            return Ok(HnswSearchMemoryResult {
                rows: Vec::new(),
                retained_bytes: 0,
            });
        }
        if query.len() != self.dimension {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: self.dimension,
                actual: query.len(),
            }));
        }

        let ef = hnsw_search_ef(ef_search, k);
        let mut encoded_query = None;
        if matches!(
            &self.hnsw,
            HnswInner::QuantizedMutable(_) | HnswInner::QuantizedSealed(_)
        ) {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            encoded_query = Some(encode_bounded_quantized_query(
                query,
                self.quantization,
                &mut scratch,
            )?);
        }
        let searched = match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => hnsw.search_with_bounded_control(
                query,
                ef,
                ef,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::F32Sealed(hnsw) => hnsw.search_with_bounded_control(
                query,
                ef,
                ef,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::QuantizedMutable(hnsw) => {
                let encoded = encoded_query.as_deref().unwrap_or_default();
                hnsw.search_with_bounded_control(
                    encoded,
                    ef,
                    ef,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )
            }
            HnswInner::QuantizedSealed(hnsw) => {
                let encoded = encoded_query.as_deref().unwrap_or_default();
                hnsw.search_with_bounded_control(
                    encoded,
                    ef,
                    ef,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )
            }
        };
        let raw = match searched {
            Ok(raw) => raw,
            Err(error) => {
                if let Some(encoded) = encoded_query.as_ref() {
                    let mut scratch = VectorSearchScratch {
                        acquire: &mut acquire,
                        release: &mut release,
                    };
                    scratch.release_vec(encoded, HnswSearchScratchContainer::QueryBytes);
                }
                return Err(map_core_bounded_error(error));
            }
        };
        let mut scored = Vec::new();
        let mut seen = HashSet::new();
        let mut rows = Vec::new();
        let mut exact_key = None;
        let cap = hnsw_search_candidate_cap(ef_search, k);
        let merge = (|| -> std::result::Result<(), E> {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            let exact_score = bounded_exact_query_score(
                query,
                self.quantization,
                encoded_query.as_deref(),
                &mut before_distance,
            )?;
            if let Some(exact_score) = exact_score {
                if matches!(self.quantization, VectorQuantization::F32) {
                    exact_key = Some(bounded_f32_exact_key(query, &mut scratch)?);
                }
                let key = exact_key
                    .as_deref()
                    .or(encoded_query.as_deref())
                    .unwrap_or_default();
                before_source_entry()?;
                let exact_rows = self.exact_rows.read();
                if let Some(row_ids) = exact_rows.get(key) {
                    let row_limit = cap.min(row_ids.len());
                    let mut row_position = 0usize;
                    while row_position < row_limit {
                        before_source_entry()?;
                        let row_id = *row_ids.get(row_position).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW exact-row position changed".to_string(),
                            ))
                        })?;
                        row_position = row_position.checked_add(1).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW exact-row position overflow".to_string(),
                            ))
                        })?;
                        let required = scored.len().checked_add(1).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW merge-result length overflow".to_string(),
                            ))
                        })?;
                        scratch.ensure_vec_capacity(
                            &mut scored,
                            HnswSearchScratchContainer::MergeResults,
                            required,
                            "merge results",
                            &mut before_source_entry,
                        )?;
                        scored.push((row_id, exact_score));
                    }
                }
            }
            before_source_entry()?;
            let id_to_row = self.id_to_row.read();
            let mut neighbour_position = 0usize;
            while neighbour_position < raw.neighbours.len() {
                before_source_entry()?;
                let neighbour = raw.neighbours.get(neighbour_position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW raw-neighbour position changed".to_string(),
                    ))
                })?;
                neighbour_position = neighbour_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW raw-neighbour position overflow".to_string(),
                    ))
                })?;
                let Some(row_id) = id_to_row.get(&neighbour.d_id).copied() else {
                    continue;
                };
                let required = scored.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW merge-result length overflow".to_string(),
                    ))
                })?;
                scratch.ensure_vec_capacity(
                    &mut scored,
                    HnswSearchScratchContainer::MergeResults,
                    required,
                    "merge results",
                    &mut before_source_entry,
                )?;
                before_distance()?;
                scored.push((
                    row_id,
                    self.original_query_score(&neighbour.p_id, query)
                        .unwrap_or(1.0 - neighbour.distance),
                ));
            }
            scored.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let mut scored_position = 0usize;
            while scored_position < scored.len() {
                before_source_entry()?;
                let (row_id, score) = *scored.get(scored_position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW scored-row position changed".to_string(),
                    ))
                })?;
                scored_position = scored_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW scored-row position overflow".to_string(),
                    ))
                })?;
                if seen.contains(&row_id) {
                    continue;
                }
                let required = seen.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW seen-row length overflow".to_string(),
                    ))
                })?;
                scratch.ensure_set_capacity(
                    &mut seen,
                    HnswSearchScratchContainer::SeenRows,
                    required,
                    "seen rows",
                    &mut before_source_entry,
                )?;
                seen.insert(row_id);
                if rows.len() == cap {
                    break;
                }
                let required = rows.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW result length overflow".to_string(),
                    ))
                })?;
                scratch.ensure_vec_capacity(
                    &mut rows,
                    HnswSearchScratchContainer::Result,
                    required,
                    "result rows",
                    &mut before_source_entry,
                )?;
                rows.push((row_id, score));
            }
            Ok(())
        })();
        let retained_bytes = rows
            .capacity()
            .checked_mul(std::mem::size_of::<(RowId, f32)>())
            .ok_or_else(|| {
                E::from(Error::Other(
                    "bounded HNSW result capacity exceeds the native address space".to_string(),
                ))
            });
        let mut scratch = VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        };
        if let Some(key) = exact_key.as_ref() {
            scratch.release_vec(key, HnswSearchScratchContainer::ExactKey);
        }
        if let Some(encoded) = encoded_query.as_ref() {
            scratch.release_vec(encoded, HnswSearchScratchContainer::QueryBytes);
        }
        scratch.release_vec(&raw.neighbours, HnswSearchScratchContainer::Result);
        scratch.release_vec(&scored, HnswSearchScratchContainer::MergeResults);
        scratch.release_set(&seen, HnswSearchScratchContainer::SeenRows);
        match merge {
            Ok(()) => {
                let retained_bytes = match retained_bytes {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        scratch.release_vec(&rows, HnswSearchScratchContainer::Result);
                        return Err(error);
                    }
                };
                Ok(HnswSearchMemoryResult {
                    rows,
                    retained_bytes,
                })
            }
            Err(error) => {
                scratch.release_vec(&rows, HnswSearchScratchContainer::Result);
                Err(error)
            }
        }
    }

    /// Bounded indexed search for a relationally supplied allowed-row set.
    /// The predicate is consulted while graph candidates are admitted, not as
    /// a second pass over the graph or vector store.  When the hard traversal
    /// limits cannot yield the requested number of allowed rows, `status` is
    /// `Incomplete` and callers must use their typed refusal path.
    #[allow(clippy::too_many_arguments)]
    pub fn search_allowed_with_bounded_memory<E, S, D, A, R, F>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        allowed_count: usize,
        limits: HnswAllowedSearchLimits,
        is_allowed: F,
        before_source_entry: S,
        before_distance: D,
        acquire: A,
        release: R,
    ) -> std::result::Result<HnswAllowedSearchMemoryResult, E>
    where
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(RowId) -> bool,
    {
        self.search_allowed_with_bounded_memory_at_ef(
            index,
            query,
            k,
            allowed_count,
            limits,
            self.ef_search,
            std::iter::empty,
            false,
            is_allowed,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_allowed_with_bounded_memory_at_ef<E, S, D, A, R, F, I>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        allowed_count: usize,
        limits: HnswAllowedSearchLimits,
        ef_search: usize,
        seed_rows: impl Fn() -> I,
        seed_all: bool,
        mut is_allowed: F,
        mut before_source_entry: S,
        mut before_distance: D,
        mut acquire: A,
        mut release: R,
    ) -> std::result::Result<HnswAllowedSearchMemoryResult, E>
    where
        I: Iterator<Item = u64>,
        E: From<contextdb_core::Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
        F: FnMut(RowId) -> bool,
    {
        let mut required_results = k.min(allowed_count);
        let candidate_capacity = hnsw_search_ef(ef_search, k);
        if required_results == 0 {
            return Ok(HnswAllowedSearchMemoryResult {
                rows: Vec::new(),
                retained_bytes: 0,
                status: HnswAllowedSearchStatus::Complete,
                visited_nodes: 0,
                vector_evaluations: 0,
                allowed_admissions: 0,
            });
        }
        if query.len() != self.dimension {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: self.dimension,
                actual: query.len(),
            }));
        }

        let mut encoded_query = None;
        if matches!(
            &self.hnsw,
            HnswInner::QuantizedMutable(_) | HnswInner::QuantizedSealed(_)
        ) {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            encoded_query = Some(encode_bounded_quantized_query(
                query,
                self.quantization,
                &mut scratch,
            )?);
        }
        let ef = hnsw_search_ef(ef_search, required_results);
        let id_to_row = self.id_to_row.read();
        let mut seed_points = Vec::new();
        let mut supplied_seed_rows = false;
        let mut layer_allowed_count = allowed_count.min(self.row_to_point.read().len());
        let seeds = (|| -> std::result::Result<(), E> {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            let row_to_point = self.row_to_point.read();
            if seed_all {
                // Seek bounded, dispersed logical keys directly. Walking every
                // key to pick an ordinal sample would make graph startup linear.
                before_source_entry()?;
                if let (Some((first, _)), Some((last, _))) = (
                    row_to_point.first_key_value(),
                    row_to_point.last_key_value(),
                ) {
                    let capacity = ef.min(allowed_count).min(row_to_point.len());
                    scratch.ensure_vec_capacity(
                        &mut seed_points,
                        HnswSearchScratchContainer::Result,
                        capacity,
                        "graph seed directory",
                        &mut before_source_entry,
                    )?;
                    if row_to_point.len() <= ef {
                        // A small logical directory fits entirely inside the
                        // declared graph effort, including widely spaced keys.
                        for (row, point) in row_to_point.iter() {
                            before_source_entry()?;
                            if seed_points.len() == capacity {
                                break;
                            }
                            if is_allowed(*row) {
                                seed_points.push(*point);
                            }
                        }
                    } else {
                        for slot in 0..capacity {
                            before_source_entry()?;
                            let raw = first.0 as u128
                                + (last.0 - first.0) as u128 * slot as u128
                                    / capacity.saturating_sub(1).max(1) as u128;
                            if let Some((row, point)) =
                                row_to_point.range(RowId(raw as u64)..).next()
                                && is_allowed(*row)
                                && !seed_points.contains(point)
                            {
                                seed_points.push(*point);
                            }
                        }
                    }
                }
            } else {
                // The candidate directory spans the whole requested scope,
                // but this graph can contribute only its own eligible rows.
                // Count by direct membership probes, never by walking excluded
                // graph points. A base with one current row must not retry for
                // rows that exist only in the change graph or fresh tail.
                let mut members = 0usize;
                for raw in seed_rows() {
                    supplied_seed_rows = true;
                    before_source_entry()?;
                    if row_to_point.contains_key(&RowId(raw)) && is_allowed(RowId(raw)) {
                        members += 1;
                    }
                }
                if supplied_seed_rows {
                    layer_allowed_count = members;
                    required_results = k.min(members);
                }
                let capacity = ef.min(members);
                scratch.ensure_vec_capacity(
                    &mut seed_points,
                    HnswSearchScratchContainer::Result,
                    capacity,
                    "graph seed directory",
                    &mut before_source_entry,
                )?;
                let mut selected = 0usize;
                for raw in seed_rows() {
                    if seed_points.len() == capacity {
                        break;
                    }
                    before_source_entry()?;
                    let Some(point) = row_to_point.get(&RowId(raw)) else {
                        continue;
                    };
                    if !is_allowed(RowId(raw)) {
                        continue;
                    }
                    // Evenly spaced eligible ordinals supply exactly capacity
                    // seeds, including when members is not divisible by EF.
                    // This covers small layers completely and supplies at least
                    // k distinct candidates in larger, disconnected layers.
                    let next =
                        (seed_points.len() as u128 * members as u128 / capacity as u128) as usize;
                    if selected == next {
                        seed_points.push(*point);
                    }
                    selected += 1;
                }
            }
            Ok(())
        })();
        if let Err(error) = seeds {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            scratch.release_vec(&seed_points, HnswSearchScratchContainer::Result);
            if let Some(encoded) = encoded_query.as_ref() {
                scratch.release_vec(encoded, HnswSearchScratchContainer::QueryBytes);
            }
            return Err(error);
        }
        // An explicit candidate directory with no point in this layer proves
        // the layer contributes nothing. Traversing excluded points cannot
        // discover a missing identity and would charge unrelated history.
        if !seed_all && supplied_seed_rows && seed_points.is_empty() {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            scratch.release_vec(&seed_points, HnswSearchScratchContainer::Result);
            if let Some(encoded) = encoded_query.as_ref() {
                scratch.release_vec(encoded, HnswSearchScratchContainer::QueryBytes);
            }
            return Ok(HnswAllowedSearchMemoryResult {
                rows: Vec::new(),
                retained_bytes: 0,
                status: HnswAllowedSearchStatus::Complete,
                visited_nodes: 0,
                vector_evaluations: 0,
                allowed_admissions: 0,
            });
        }
        let searched = match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => hnsw.search_allowed_with_bounded_control(
                query,
                required_results,
                ef,
                limits,
                &seed_points,
                layer_allowed_count,
                |data_id| id_to_row[&data_id].0,
                |data_id| {
                    id_to_row
                        .get(&data_id)
                        .is_some_and(|row_id| is_allowed(*row_id))
                },
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::F32Sealed(hnsw) => hnsw.search_allowed_with_bounded_control(
                query,
                required_results,
                ef,
                limits,
                &seed_points,
                layer_allowed_count,
                |data_id| id_to_row[&data_id].0,
                |data_id| {
                    id_to_row
                        .get(&data_id)
                        .is_some_and(|row_id| is_allowed(*row_id))
                },
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::QuantizedMutable(hnsw) => {
                let encoded = encoded_query.as_deref().unwrap_or_default();
                hnsw.search_allowed_with_bounded_control(
                    encoded,
                    required_results,
                    ef,
                    limits,
                    &seed_points,
                    layer_allowed_count,
                    |data_id| id_to_row[&data_id].0,
                    |data_id| {
                        id_to_row
                            .get(&data_id)
                            .is_some_and(|row_id| is_allowed(*row_id))
                    },
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )
            }
            HnswInner::QuantizedSealed(hnsw) => {
                let encoded = encoded_query.as_deref().unwrap_or_default();
                hnsw.search_allowed_with_bounded_control(
                    encoded,
                    required_results,
                    ef,
                    limits,
                    &seed_points,
                    layer_allowed_count,
                    |data_id| id_to_row[&data_id].0,
                    |data_id| {
                        id_to_row
                            .get(&data_id)
                            .is_some_and(|row_id| is_allowed(*row_id))
                    },
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut acquire,
                    &mut release,
                )
            }
        };
        VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        }
        .release_vec(&seed_points, HnswSearchScratchContainer::Result);
        drop(seed_points);
        let raw = match searched {
            Ok(raw) => raw,
            Err(error) => {
                if let Some(encoded) = encoded_query.as_ref() {
                    let mut scratch = VectorSearchScratch {
                        acquire: &mut acquire,
                        release: &mut release,
                    };
                    scratch.release_vec(encoded, HnswSearchScratchContainer::QueryBytes);
                }
                return Err(map_core_bounded_error(error));
            }
        };
        let raw_status = raw.status;
        let raw_retained = raw.neighbours;
        let map_rows = (|| -> std::result::Result<(Vec<(RowId, f32)>, bool), E> {
            let mut scratch = VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            };
            let mut rows = scratch.allocate_vec(
                HnswSearchScratchContainer::Result,
                candidate_capacity,
                "allowed result rows",
            )?;
            let mut seen = scratch.allocate_set(
                HnswSearchScratchContainer::SeenRows,
                candidate_capacity,
                "allowed result rows",
            )?;
            let mut exact_key = None;
            let mut exact_bucket_completes = false;
            let mapped = (|| -> std::result::Result<(), E> {
                if let Some(exact_score) = bounded_exact_query_score(
                    query,
                    self.quantization,
                    encoded_query.as_deref(),
                    &mut before_distance,
                )? {
                    if matches!(self.quantization, VectorQuantization::F32) {
                        exact_key = Some(bounded_f32_exact_key(query, &mut scratch)?);
                    }
                    let key = exact_key
                        .as_deref()
                        .or(encoded_query.as_deref())
                        .unwrap_or_default();
                    before_source_entry()?;
                    let exact_rows = self.exact_rows.read();
                    if let Some(row_ids) = exact_rows.get(key) {
                        if supplied_seed_rows {
                            // Candidate ids and exact buckets are both ordered.
                            // Probe the bucket from the allowed directory, so
                            // excluded equal-vector history cannot turn the
                            // canonical tie correction into a complete pass.
                            for raw in seed_rows() {
                                if rows.len() == candidate_capacity {
                                    break;
                                }
                                before_source_entry()?;
                                let row_id = RowId(raw);
                                if row_ids.binary_search(&row_id).is_ok()
                                    && is_allowed(row_id)
                                    && seen.insert(row_id)
                                {
                                    rows.push((row_id, exact_score));
                                }
                            }
                        } else {
                            let mut exact_position = 0usize;
                            while exact_position < row_ids.len() && rows.len() < candidate_capacity
                            {
                                before_source_entry()?;
                                let row_id = *row_ids.get(exact_position).ok_or_else(|| {
                                    E::from(Error::Other(
                                        "bounded allowed HNSW exact-row position changed"
                                            .to_string(),
                                    ))
                                })?;
                                exact_position =
                                    exact_position.checked_add(1).ok_or_else(|| {
                                        E::from(Error::Other(
                                            "bounded allowed HNSW exact-row position overflow"
                                                .to_string(),
                                        ))
                                    })?;
                                if is_allowed(row_id) && seen.insert(row_id) {
                                    rows.push((row_id, exact_score));
                                }
                            }
                        }
                    }
                    exact_bucket_completes = rows.len() == candidate_capacity;
                }

                let mut position = 0usize;
                while !exact_bucket_completes && position < raw_retained.len() {
                    before_source_entry()?;
                    let neighbour = raw_retained.get(position).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded allowed HNSW neighbour position changed".to_string(),
                        ))
                    })?;
                    position = position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded allowed HNSW neighbour position overflow".to_string(),
                        ))
                    })?;
                    let Some(row_id) = id_to_row.get(&neighbour.d_id).copied() else {
                        continue;
                    };
                    if !seen.insert(row_id) {
                        continue;
                    }
                    before_distance()?;
                    rows.push((
                        row_id,
                        self.original_query_score(&neighbour.p_id, query)
                            .unwrap_or(1.0 - neighbour.distance),
                    ));
                    if rows.len() == candidate_capacity {
                        break;
                    }
                }
                rows.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                Ok(())
            })();
            if let Some(key) = exact_key.as_ref() {
                scratch.release_vec(key, HnswSearchScratchContainer::ExactKey);
            }
            scratch.release_set(&seen, HnswSearchScratchContainer::SeenRows);
            match mapped {
                Ok(()) => Ok((rows, exact_bucket_completes)),
                Err(error) => {
                    scratch.release_vec(&rows, HnswSearchScratchContainer::Result);
                    Err(error)
                }
            }
        })();
        let mut scratch = VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        };
        if let Some(encoded) = encoded_query.as_ref() {
            scratch.release_vec(encoded, HnswSearchScratchContainer::QueryBytes);
        }
        scratch.release_vec(&raw_retained, HnswSearchScratchContainer::Result);
        let (rows, exact_bucket_completes) = map_rows?;
        let retained_bytes = match rows
            .capacity()
            .checked_mul(std::mem::size_of::<(RowId, f32)>())
        {
            Some(bytes) => bytes,
            None => {
                scratch.release_vec(&rows, HnswSearchScratchContainer::Result);
                return Err(E::from(Error::Other(
                    "bounded allowed HNSW result capacity exceeds the native address space"
                        .to_string(),
                )));
            }
        };
        let status = if exact_bucket_completes {
            HnswAllowedSearchStatus::Complete
        } else if rows.len() >= required_results {
            raw_status
        } else {
            HnswAllowedSearchStatus::Incomplete(
                HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
            )
        };
        Ok(HnswAllowedSearchMemoryResult {
            rows,
            retained_bytes,
            status,
            visited_nodes: raw.visited_nodes,
            vector_evaluations: raw.vector_evaluations,
            allowed_admissions: raw.allowed_admissions,
        })
    }

    /// Ordinary-reader counterpart to the request-accounted allowed-id walk.
    /// EF_SEARCH bounds algorithm breadth; only a calling surface can supply
    /// a work or time refusal. Embedded reads still charge graph scratch.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_allowed_at_ef<F, A, R, I>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        allowed_count: usize,
        ef_search: usize,
        seed_rows: impl Fn() -> I,
        seed_all: bool,
        is_allowed: F,
        acquire: A,
        release: R,
    ) -> Result<HnswAllowedSearchMemoryResult>
    where
        I: Iterator<Item = u64>,
        F: FnMut(RowId) -> bool,
        A: FnMut(HnswSearchScratchEvent) -> Result<()>,
        R: FnMut(HnswSearchScratchEvent),
    {
        self.search_allowed_with_bounded_memory_at_ef(
            index,
            query,
            k,
            allowed_count,
            HnswAllowedSearchLimits {
                max_visited_nodes: usize::MAX,
                max_vector_evaluations: usize::MAX,
            },
            ef_search,
            seed_rows,
            seed_all,
            is_allowed,
            || Ok::<(), Error>(()),
            || Ok::<(), Error>(()),
            acquire,
            release,
        )
    }

    #[doc(hidden)]
    pub fn raw_entry_count_for_row(&self, row_id: RowId) -> usize {
        self.id_to_row
            .read()
            .values()
            .filter(|indexed_row| **indexed_row == row_id)
            .count()
    }

    #[doc(hidden)]
    pub fn build_serial_for_test(&self) -> u64 {
        self.build_serial
    }

    #[doc(hidden)]
    pub fn graph_topology_digest_for_test(&self) -> u64 {
        match &self.hnsw {
            HnswInner::F32Mutable(hnsw) => hnsw_topology_digest(hnsw),
            HnswInner::F32Sealed(hnsw) => hnsw_loaded_topology_digest(hnsw),
            HnswInner::QuantizedMutable(hnsw) => hnsw_topology_digest(hnsw),
            HnswInner::QuantizedSealed(hnsw) => hnsw_loaded_topology_digest(hnsw),
        }
    }
}

fn exact_key_for_stored_vector(vector: &StoredVector) -> Option<Vec<u8>> {
    match vector {
        StoredVector::F32(values) => Some(f32_exact_key(values)),
        StoredVector::SQ8 { .. } | StoredVector::SQ4 { .. } => {
            let encoded = vector.to_hnsw_u8();
            (!encoded.is_empty()).then_some(encoded)
        }
    }
}

fn quantization_tag(quantization: VectorQuantization) -> u8 {
    match quantization {
        VectorQuantization::F32 => 0,
        VectorQuantization::SQ8 => 1,
        VectorQuantization::SQ4 => 2,
    }
}

fn quantization_from_tag(tag: u8) -> Result<VectorQuantization> {
    match tag {
        0 => Ok(VectorQuantization::F32),
        1 => Ok(VectorQuantization::SQ8),
        2 => Ok(VectorQuantization::SQ4),
        _ => Err(Error::Other(
            "durable HNSW has an unknown quantization".to_string(),
        )),
    }
}

/// A deterministic checksum for a generation envelope.  The envelope also
/// checks every length and cross-reference before allocation/exposure; this
/// checksum catches damaged but otherwise decodable bytes.
fn durable_checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u64::try_from(value.len())
        .map_err(|_| Error::Other("durable HNSW byte length exceeds u64".to_string()))?;
    write_u64(out, length);
    out.extend_from_slice(value);
    Ok(())
}

struct DurableReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> DurableReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| Error::Other("durable HNSW envelope length overflow".to_string()))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| Error::Other("durable HNSW envelope is truncated".to_string()))?;
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .map_err(|_| Error::Other("durable HNSW u16 is truncated".to_string()))?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| Error::Other("durable HNSW u64 is truncated".to_string()))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_usize(&mut self) -> Result<usize> {
        usize::try_from(self.read_u64()?).map_err(|_| {
            Error::Other("durable HNSW value exceeds this platform address space".to_string())
        })
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.read_usize()?;
        self.read_exact(length)
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.position..]
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

fn exact_key_for_query(query: &[f32], quantization: VectorQuantization) -> Option<(Vec<u8>, f32)> {
    let exact_score = exact_query_score(query, quantization)?;
    match quantization {
        VectorQuantization::F32 => Some((f32_exact_key(query), exact_score)),
        VectorQuantization::SQ8 | VectorQuantization::SQ4 => {
            let encoded = StoredVector::from_f32(query, quantization).to_hnsw_u8();
            (!encoded.is_empty()).then_some((encoded, exact_score))
        }
    }
}

fn exact_query_score(query: &[f32], quantization: VectorQuantization) -> Option<f32> {
    let score = StoredVector::from_f32(query, quantization).cosine_similarity(query);
    (score.is_finite() && score > 0.0).then_some(score)
}

fn f32_exact_key(values: &[f32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        key.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    key
}

fn hnsw_stats<T, D>(hnsw: &Hnsw<'_, T, D>) -> (usize, usize, u8)
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let indexation = hnsw.get_point_indexation();
    let layer0_neighbor_edges = indexation
        .get_layer_iterator(0)
        .map(|point| {
            point
                .get_neighborhood_id()
                .first()
                .map_or(0, |neighbors| neighbors.len())
        })
        .sum();
    (
        hnsw.get_nb_point(),
        layer0_neighbor_edges,
        hnsw.get_max_level_observed(),
    )
}

fn hnsw_loaded_stats<T, D>(hnsw: &LoadedHnsw<T, D>) -> (usize, usize, u8)
where
    T: Clone + Send + Sync + 'static,
    D: Distance<T> + Send + Sync,
{
    hnsw_stats_from_parts(
        hnsw.get_nb_point(),
        hnsw.get_max_level_observed(),
        hnsw.get_point_indexation(),
    )
}

fn hnsw_stats_from_parts<T>(
    point_count: usize,
    max_level_observed: u8,
    indexation: &hnsw_rs::hnsw::PointIndexation<'_, T>,
) -> (usize, usize, u8)
where
    T: Clone + Send + Sync,
{
    let layer0_neighbor_edges = indexation
        .get_layer_iterator(0)
        .map(|point| {
            point
                .get_neighborhood_id()
                .first()
                .map_or(0, |neighbors| neighbors.len())
        })
        .sum();
    (point_count, layer0_neighbor_edges, max_level_observed)
}

fn hnsw_topology_digest<T, D>(hnsw: &Hnsw<'_, T, D>) -> u64
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let mut digest = 0xcbf2_9ce4_8422_2325u64;
    digest_u64(&mut digest, hnsw.get_nb_point() as u64);
    digest_u64(&mut digest, hnsw.get_max_level() as u64);
    digest_u64(&mut digest, hnsw.get_max_level_observed() as u64);

    let indexation = hnsw.get_point_indexation();
    for layer in 0..hnsw.get_max_level() {
        digest_u64(&mut digest, layer as u64);
        for point in indexation.get_layer_iterator(layer) {
            let point_id = point.get_point_id();
            digest_u64(&mut digest, point.get_origin_id() as u64);
            digest_u64(&mut digest, point_id.0 as u64);
            digest_i32(&mut digest, point_id.1);
            let neighborhoods = point.get_neighborhood_id();
            for (neighbor_layer, neighbors) in neighborhoods.iter().enumerate() {
                digest_u64(&mut digest, neighbor_layer as u64);
                digest_u64(&mut digest, neighbors.len() as u64);
                for neighbor in neighbors {
                    digest_u64(&mut digest, neighbor.d_id as u64);
                    digest_u64(&mut digest, neighbor.distance.to_bits() as u64);
                    digest_u64(&mut digest, neighbor.p_id.0 as u64);
                    digest_i32(&mut digest, neighbor.p_id.1);
                }
            }
        }
    }
    digest
}

fn hnsw_loaded_topology_digest<T, D>(hnsw: &LoadedHnsw<T, D>) -> u64
where
    T: Clone + Send + Sync + 'static,
    D: Distance<T> + Send + Sync,
{
    hnsw_topology_digest_from_parts(
        hnsw.get_nb_point(),
        hnsw.get_max_level(),
        hnsw.get_max_level_observed(),
        hnsw.get_point_indexation(),
    )
}

fn hnsw_topology_digest_from_parts<T>(
    point_count: usize,
    max_level: usize,
    max_level_observed: u8,
    indexation: &hnsw_rs::hnsw::PointIndexation<'_, T>,
) -> u64
where
    T: Clone + Send + Sync,
{
    let mut digest = 0xcbf2_9ce4_8422_2325u64;
    digest_u64(&mut digest, point_count as u64);
    digest_u64(&mut digest, max_level as u64);
    digest_u64(&mut digest, max_level_observed as u64);
    for layer in 0..max_level {
        digest_u64(&mut digest, layer as u64);
        for point in indexation.get_layer_iterator(layer) {
            let point_id = point.get_point_id();
            digest_u64(&mut digest, point.get_origin_id() as u64);
            digest_u64(&mut digest, point_id.0 as u64);
            digest_i32(&mut digest, point_id.1);
            let neighborhoods = point.get_neighborhood_id();
            for (neighbor_layer, neighbors) in neighborhoods.iter().enumerate() {
                digest_u64(&mut digest, neighbor_layer as u64);
                digest_u64(&mut digest, neighbors.len() as u64);
                for neighbor in neighbors {
                    digest_u64(&mut digest, neighbor.d_id as u64);
                    digest_u64(&mut digest, neighbor.distance.to_bits() as u64);
                    digest_u64(&mut digest, neighbor.p_id.0 as u64);
                    digest_i32(&mut digest, neighbor.p_id.1);
                }
            }
        }
    }
    digest
}

fn digest_i32(digest: &mut u64, value: i32) {
    digest_u64(digest, value as u32 as u64);
}

fn digest_u64(digest: &mut u64, value: u64) {
    *digest ^= value;
    *digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
}

pub(crate) fn hnsw_search_candidate_cap_for_ef(ef_search: usize, k: usize) -> usize {
    hnsw_search_candidate_cap(ef_search, k)
}

fn hnsw_search_candidate_cap(ef_search: usize, k: usize) -> usize {
    hnsw_search_ef(ef_search, k).max(k)
}

fn hnsw_search_ef(ef_search: usize, k: usize) -> usize {
    ef_search.max(k).max(1)
}

fn insertion_key(entry: &StoredVectorEntry) -> u64 {
    let mut x = entry.row_id.0 ^ entry.lsn.0 ^ entry.created_tx.0;
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::{Lsn, TxId};

    #[test]
    fn prepared_insert_validation_fails_without_changing_graph_points() {
        let initial = entry(RowId(1), &[1.0, 0.0], TxId(1));
        let graph = HnswIndex::new(&[initial], 2, VectorQuantization::F32);
        let topology = graph.graph_topology_digest_for_test();
        let invalid = entry(RowId(2), &[1.0, 0.0, 0.0], TxId(2));
        assert!(graph.prepare_insert(&invalid, 1).is_err());
        assert_eq!(graph.len(), 1);
        assert_eq!(graph.graph_topology_digest_for_test(), topology);
        graph.next_id.store(usize::MAX, Ordering::SeqCst);
        let valid = entry(RowId(2), &[0.0, 1.0], TxId(2));
        assert!(graph.prepare_insert(&valid, 1).is_err());
        assert_eq!(graph.graph_topology_digest_for_test(), topology);
    }

    #[test]
    fn resolved_search_breadth_is_raised_only_to_the_requested_result_count() {
        assert_eq!(hnsw_search_ef(32, 10), 32);
        assert_eq!(hnsw_search_candidate_cap_for_ef(32, 10), 32);
        assert_eq!(hnsw_search_ef(4, 10), 10);
        assert_eq!(hnsw_search_ef(0, 0), 1);
    }

    #[test]
    fn predicate_search_without_seed_rows_still_traverses_but_absent_candidates_do_not() {
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let entries = [
                entry_with_quantization(RowId(1), &[1.0, 0.0, 0.0], TxId(1), quantization),
                entry_with_quantization(RowId(2), &[0.0, 1.0, 0.0], TxId(2), quantization),
            ];
            let graph = HnswIndex::new(&entries, 3, quantization);
            let index = VectorIndexRef::new("items", "embedding");
            let query = [0.9, 0.1, 0.0];
            let limits = HnswAllowedSearchLimits {
                max_visited_nodes: 1024,
                max_vector_evaluations: 1024,
            };
            let result = graph
                .search_allowed_with_bounded_memory(
                    &index,
                    &query,
                    1,
                    2,
                    limits,
                    |_| true,
                    || Ok::<(), Error>(()),
                    || Ok::<(), Error>(()),
                    |_| Ok::<(), Error>(()),
                    |_| {},
                )
                .unwrap();
            assert_eq!(result.rows[0].0, RowId(1));
            assert!(result.visited_nodes > 0);
            let result = graph
                .search_allowed_with_bounded_memory_at_ef(
                    &index,
                    &query,
                    1,
                    1,
                    limits,
                    32,
                    || std::iter::once(99),
                    false,
                    |row| row == RowId(99),
                    || Ok::<(), Error>(()),
                    || panic!("a layer with no allowed identity must not evaluate vectors"),
                    |_| Ok::<(), Error>(()),
                    |_| {},
                )
                .unwrap();
            assert!(result.rows.is_empty());
            assert_eq!(result.visited_nodes, 0);
        }
    }

    fn entry(row_id: RowId, vector: &[f32], tx: TxId) -> StoredVectorEntry {
        entry_with_quantization(row_id, vector, tx, VectorQuantization::F32)
    }

    fn entry_with_quantization(
        row_id: RowId,
        vector: &[f32],
        tx: TxId,
        quantization: VectorQuantization,
    ) -> StoredVectorEntry {
        StoredVectorEntry {
            row_id,
            vector: StoredVector::from_f32(vector, quantization),
            created_tx: tx,
            deleted_tx: None,
            lsn: Lsn(tx.0),
        }
    }

    #[test]
    fn mutable_graph_insertion_preserves_existing_points_and_build_identity() {
        let original = entry(RowId(11), &[1.0, 0.0, 0.0], TxId(1));
        let graph = HnswIndex::new(&[original], 3, VectorQuantization::F32);
        let build_serial = graph.build_serial_for_test();

        graph
            .insert(&entry(RowId(11), &[0.0, 1.0, 0.0], TxId(2)))
            .unwrap();
        graph
            .insert(&entry(RowId(29), &[0.0, 0.0, 1.0], TxId(3)))
            .unwrap();

        assert_eq!(graph.build_serial_for_test(), build_serial);
        assert_eq!(graph.len(), 3);
        assert_eq!(graph.graph_stats().point_count, 3);
        assert_eq!(graph.raw_entry_count_for_row(RowId(11)), 2);
        assert_eq!(graph.raw_entry_count_for_row(RowId(29)), 1);

        let index = VectorIndexRef::new("items", "embedding");
        assert_eq!(
            graph.search(&index, &[1.0, 0.0, 0.0], 1).unwrap()[0].0,
            RowId(11)
        );
        assert_eq!(
            graph.search(&index, &[0.0, 1.0, 0.0], 1).unwrap()[0].0,
            RowId(11)
        );
        assert_eq!(
            graph.search(&index, &[0.0, 0.0, 1.0], 1).unwrap()[0].0,
            RowId(29)
        );
    }

    #[test]
    fn mutable_quantized_graphs_insert_without_rebuilding() {
        for quantization in [VectorQuantization::SQ8, VectorQuantization::SQ4] {
            let original =
                entry_with_quantization(RowId(11), &[1.0, 0.0, 0.0], TxId(1), quantization);
            let graph = HnswIndex::new(&[original], 3, quantization);
            let build_serial = graph.build_serial_for_test();

            graph
                .insert(&entry_with_quantization(
                    RowId(29),
                    &[0.0, 1.0, 0.0],
                    TxId(2),
                    quantization,
                ))
                .unwrap();

            assert_eq!(graph.build_serial_for_test(), build_serial);
            assert_eq!(graph.len(), 2);
            assert_eq!(graph.raw_entry_count_for_row(RowId(11)), 1);
            assert_eq!(graph.raw_entry_count_for_row(RowId(29)), 1);
            assert_eq!(
                graph
                    .search(
                        &VectorIndexRef::new("items", "embedding"),
                        &[0.0, 1.0, 0.0],
                        1,
                    )
                    .unwrap()[0]
                    .0,
                RowId(29)
            );
        }
    }

    #[test]
    fn one_buffer_generation_preserves_canonical_bytes_for_every_encoding() {
        use hnsw_rs::hnswio::encode_owned_graph;
        // The previous independent envelope assembly is the byte contract.
        let previous_envelope = |this: &HnswIndex| -> Result<Vec<u8>> {
            let graph = match &this.hnsw {
                HnswInner::F32Mutable(graph) => encode_owned_graph(graph),
                HnswInner::QuantizedMutable(graph) => encode_owned_graph(graph),
                HnswInner::F32Sealed(_) | HnswInner::QuantizedSealed(_) => {
                    return Err(Error::Other(
                        "a sealed durable HNSW graph is already a generation and cannot be re-encoded"
                            .to_string(),
                    ));
                }
            }
            .map_err(|error| Error::Other(format!("could not encode durable HNSW graph: {error}")))?;
            let id_to_row = this.id_to_row.read();
            let mut rows = id_to_row
                .iter()
                .map(|(data_id, row_id)| (*data_id, row_id.0))
                .collect::<Vec<_>>();
            rows.sort_unstable_by_key(|(data_id, _)| *data_id);
            let exact_rows = this.exact_rows.read();
            let mut exact = exact_rows
                .iter()
                .map(|(key, row_ids)| {
                    let mut row_ids = row_ids.iter().map(|row_id| row_id.0).collect::<Vec<_>>();
                    row_ids.sort_unstable();
                    (key.clone(), row_ids)
                })
                .collect::<Vec<_>>();
            exact.sort_unstable_by(|left, right| left.0.cmp(&right.0));

            let mut payload = Vec::new();
            write_u64(&mut payload, this.dimension as u64);
            write_u8(&mut payload, quantization_tag(this.quantization));
            write_u64(&mut payload, this.ef_search as u64);
            write_bytes(&mut payload, &graph)?;
            write_u64(&mut payload, rows.len() as u64);
            for (data_id, row_id) in rows {
                write_u64(&mut payload, data_id as u64);
                write_u64(&mut payload, row_id);
            }
            write_u64(&mut payload, exact.len() as u64);
            for (key, row_ids) in exact {
                write_bytes(&mut payload, &key)?;
                write_u64(&mut payload, row_ids.len() as u64);
                for row_id in row_ids {
                    write_u64(&mut payload, row_id);
                }
            }
            let checksum = durable_checksum(&payload);
            let mut envelope = Vec::with_capacity(
                DURABLE_ENVELOPE_MAGIC
                    .len()
                    .saturating_add(2)
                    .saturating_add(8)
                    .saturating_add(payload.len()),
            );
            envelope.extend_from_slice(&DURABLE_ENVELOPE_MAGIC);
            envelope.extend_from_slice(&DURABLE_ENVELOPE_VERSION.to_le_bytes());
            write_u64(&mut envelope, checksum);
            envelope.extend_from_slice(&payload);
            Ok(envelope)
        };
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let entries = (0..192_u64)
                .map(|ordinal| {
                    let vector = (0..64)
                        .map(|component| ((ordinal % 23 + component) % 31) as f32 / 31.0)
                        .collect::<Vec<_>>();
                    entry_with_quantization(
                        RowId(192 - ordinal),
                        &vector,
                        TxId(ordinal + 1),
                        quantization,
                    )
                })
                .collect::<Vec<_>>();
            let graph = HnswIndex::new(&entries, 64, quantization);
            // Exercise insertion's sorted exact-row membership as well.
            graph
                .insert(&entry_with_quantization(
                    RowId(300),
                    &[0.25; 64],
                    TxId(300),
                    quantization,
                ))
                .unwrap();
            let expected = previous_envelope(&graph).unwrap();
            let actual = graph.encode_durable_generation().unwrap();
            assert_eq!(actual, expected, "canonical bytes for {quantization:?}");
            let loaded = HnswIndex::decode_durable_generation(&actual, 64, quantization).unwrap();
            assert_eq!(
                loaded.graph_topology_digest_for_test(),
                graph.graph_topology_digest_for_test()
            );
            assert_eq!(loaded.len(), graph.len());
        }
    }

    #[test]
    fn durable_generation_round_trip_keeps_search_maps_and_seals_the_graph() {
        let entries = [
            entry(RowId(11), &[1.0, 0.0, 0.0], TxId(1)),
            entry(RowId(29), &[0.0, 1.0, 0.0], TxId(2)),
        ];
        let graph = HnswIndex::new(&entries, 3, VectorQuantization::F32);
        let topology = graph.graph_topology_digest_for_test();
        let bytes = graph.encode_durable_generation().unwrap();
        let loaded =
            HnswIndex::decode_durable_generation(&bytes, 3, VectorQuantization::F32).unwrap();

        assert_eq!(loaded.graph_topology_digest_for_test(), topology);
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded
                .search(
                    &VectorIndexRef::new("items", "embedding"),
                    &[0.0, 1.0, 0.0],
                    1
                )
                .unwrap()[0]
                .0,
            RowId(29)
        );
        assert!(
            loaded
                .insert(&entry(RowId(31), &[0.0, 0.0, 1.0], TxId(3)))
                .is_err()
        );
    }

    #[test]
    fn durable_generation_refuses_corruption_and_incompatible_column_definition() {
        let graph = HnswIndex::new(
            &[entry(RowId(11), &[1.0, 0.0], TxId(1))],
            2,
            VectorQuantization::F32,
        );
        let bytes = graph.encode_durable_generation().unwrap();
        assert!(
            HnswIndex::decode_durable_generation(
                &bytes[..bytes.len() - 1],
                2,
                VectorQuantization::F32
            )
            .is_err()
        );
        assert!(HnswIndex::decode_durable_generation(&bytes, 3, VectorQuantization::F32).is_err());
        assert!(HnswIndex::decode_durable_generation(&bytes, 2, VectorQuantization::SQ8).is_err());
    }

    #[test]
    fn embedded_filtered_breadth_matches_the_same_walk_without_caller_work_limits() {
        let entries = (0..512_u64)
            .map(|row| {
                let vector = (0..32)
                    .map(|d| (((row + 11) * (d + 37) * 177 + d * d) % 997) as f32 / 997.0)
                    .collect::<Vec<_>>();
                entry(RowId(row + 1), &vector, TxId(row + 1))
            })
            .collect::<Vec<_>>();
        let policy = crate::store::ResolvedVectorPolicy {
            auto_index_at: 1,
            hnsw_m: 2,
            hnsw_ef_construction: 64,
            hnsw_ef_search: 64,
            policy_revision: 1,
            auto_index_at_source: "declared",
            ef_search_source: "declared",
        };
        let graph = HnswIndex::new_with_policy(&entries, 32, VectorQuantization::F32, policy);
        let index = VectorIndexRef::new("items", "embedding");
        for probe in 0..8_u64 {
            let query = (0..32)
                .map(|d| (((probe + 1003) * (d + 37) * 177 + d * d) % 997) as f32 / 997.0)
                .collect::<Vec<_>>();
            let embedded = graph
                .search_allowed_at_ef(
                    &index,
                    &query,
                    10,
                    256,
                    64,
                    || (1..=512).filter(|id| id % 2 == 0),
                    false,
                    |id| id.0 % 2 == 0,
                    |_| Ok(()),
                    |_| {},
                )
                .unwrap();
            let bounded = graph
                .search_allowed_with_bounded_memory_at_ef(
                    &index,
                    &query,
                    10,
                    256,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: usize::MAX,
                        max_vector_evaluations: usize::MAX,
                    },
                    64,
                    || (1..=512).filter(|id| id % 2 == 0),
                    false,
                    |id| id.0 % 2 == 0,
                    || Ok::<_, Error>(()),
                    || Ok::<_, Error>(()),
                    |_| Ok::<_, Error>(()),
                    |_| {},
                )
                .unwrap();
            eprintln!(
                "probe={probe} visits={} distances={} status={:?}",
                embedded.visited_nodes, embedded.vector_evaluations, embedded.status
            );
            assert_eq!(embedded.status, bounded.status);
            assert_eq!(embedded.rows, bounded.rows);
            assert_eq!(embedded.visited_nodes, bounded.visited_nodes);
            assert!(embedded.vector_evaluations < entries.len());
        }
    }

    #[test]
    fn layered_allowed_membership_bounds_history_work_and_preserves_results() {
        // The one current base row is surrounded by excluded history; the
        // other nine current rows belong to later graphs. The query is held
        // out, so an exact-vector bucket cannot hide a failed graph walk.
        let index = VectorIndexRef::new("items", "embedding");
        let query = [0.83, 0.41, 0.19];
        let policy = crate::store::ResolvedVectorPolicy {
            auto_index_at: 1,
            hnsw_m: 8,
            hnsw_ef_construction: 32,
            hnsw_ef_search: 16,
            policy_revision: 1,
            auto_index_at_source: "declared",
            ef_search_source: "declared",
        };
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let current = (1..=10_u64)
                .map(|row| {
                    entry_with_quantization(
                        RowId(row),
                        &[row as f32 + 0.5, 11.0 - row as f32, 1.0],
                        TxId(row),
                        quantization,
                    )
                })
                .collect::<Vec<_>>();
            let mut expected = None;
            for history in [0, 64, 512, 2048_u64] {
                let mut base_entries = vec![current[0].clone()];
                base_entries.extend((0..history).map(|row| {
                    entry_with_quantization(
                        RowId(100 + row),
                        &[0.2, 0.3 + row as f32 / 4096.0, 0.9],
                        TxId(1),
                        quantization,
                    )
                }));
                let seal = |entries: &[StoredVectorEntry]| {
                    let built = HnswIndex::new_with_policy(entries, 3, quantization, policy);
                    HnswIndex::decode_durable_generation(
                        &built.encode_durable_generation().unwrap(),
                        3,
                        quantization,
                    )
                    .unwrap()
                };
                let layers = [
                    seal(&base_entries),
                    seal(&current[1..5]),
                    HnswIndex::new_with_policy(&current[5..], 3, quantization, policy),
                ];
                let mut merged = Vec::new();
                let mut visits = 0usize;
                let mut distances = 0usize;
                for (layer, expected_ids) in layers.iter().zip([1..=1, 2..=5, 6..=10]) {
                    let embedded = layer
                        .search_allowed_at_ef(
                            &index,
                            &query,
                            10,
                            10,
                            16,
                            || 1..=10,
                            false,
                            |row| row.0 <= 10,
                            |_| Ok(()),
                            |_| {},
                        )
                        .unwrap();
                    let mut source_work = 0usize;
                    let mut distance_work = 0usize;
                    let bounded = layer
                        .search_allowed_with_bounded_memory_at_ef(
                            &index,
                            &query,
                            10,
                            10,
                            HnswAllowedSearchLimits {
                                max_visited_nodes: 128,
                                max_vector_evaluations: 128,
                            },
                            16,
                            || 1..=10,
                            false,
                            |row| row.0 <= 10,
                            || {
                                source_work += 1;
                                Ok::<_, Error>(())
                            },
                            || {
                                distance_work += 1;
                                Ok::<_, Error>(())
                            },
                            |_| Ok::<_, Error>(()),
                            |_| {},
                        )
                        .unwrap();
                    assert_eq!(embedded.status, HnswAllowedSearchStatus::Complete);
                    assert_eq!(bounded.status, HnswAllowedSearchStatus::Complete);
                    assert_eq!(embedded.rows, bounded.rows);
                    assert_eq!(embedded.visited_nodes, bounded.visited_nodes);
                    assert_eq!(embedded.vector_evaluations, bounded.vector_evaluations);
                    assert_eq!(
                        embedded
                            .rows
                            .iter()
                            .map(|(row, _)| row.0)
                            .collect::<HashSet<_>>(),
                        expected_ids.collect::<HashSet<_>>()
                    );
                    // These fixed bounds include upper-level entry navigation,
                    // seed probes, exact-key checks and scratch migration.
                    assert!(source_work < 4096, "history={history} source={source_work}");
                    assert!(
                        distance_work < 128,
                        "history={history} distances={distance_work}"
                    );
                    println!(
                        "layer quantization={quantization:?} history={history} rows={} source={source_work} distances={distance_work} visits={}",
                        embedded.rows.len(),
                        embedded.visited_nodes
                    );
                    visits += embedded.visited_nodes;
                    distances += embedded.vector_evaluations;
                    merged.extend(embedded.rows);
                }
                merged.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
                assert_eq!(merged.len(), 10);
                if let Some(expected) = &expected {
                    assert_eq!(&merged, expected);
                } else {
                    expected = Some(merged);
                }
                assert!(visits < 384, "history={history} visits={visits}");
                assert!(distances < 384, "history={history} distances={distances}");
            }
        }
    }

    #[test]
    fn restricted_exact_ties_probe_only_allowed_layer_membership() {
        for history in [0, 64, 2048_u64] {
            let entries = (1..=history)
                .chain(10_000..10_010)
                .map(|row| entry(RowId(row), &[1.0, 0.0, 0.0], TxId(1)))
                .collect::<Vec<_>>();
            let graph = HnswIndex::new(&entries, 3, VectorQuantization::F32);
            let mut source_work = 0usize;
            let found = graph
                .search_allowed_with_bounded_memory_at_ef(
                    &VectorIndexRef::new("items", "embedding"),
                    &[1.0, 0.0, 0.0],
                    10,
                    10,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: 128,
                        max_vector_evaluations: 128,
                    },
                    16,
                    || 10_000..10_010,
                    false,
                    |row| row.0 >= 10_000,
                    || {
                        source_work += 1;
                        Ok::<_, Error>(())
                    },
                    || Ok::<_, Error>(()),
                    |_| Ok::<_, Error>(()),
                    |_| {},
                )
                .unwrap();
            assert_eq!(found.status, HnswAllowedSearchStatus::Complete);
            assert_eq!(
                found.rows.iter().map(|(row, _)| row.0).collect::<Vec<_>>(),
                (10_000..10_010).collect::<Vec<_>>()
            );
            assert!(source_work < 1024, "history={history} source={source_work}");
            assert!(found.visited_nodes < 128);
            assert!(found.vector_evaluations < 128);
            println!(
                "ties history={history} rows={} source={source_work} visits={} distances={}",
                found.rows.len(),
                found.visited_nodes,
                found.vector_evaluations
            );
        }
    }

    #[test]
    fn layer_seed_spacing_fills_declared_breadth_with_uneven_membership() {
        let entries = (1..=17)
            .map(|row| entry(RowId(row), &[row as f32, 19.0 - row as f32, 1.0], TxId(1)))
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, 3, VectorQuantization::F32);
        let found = graph
            .search_allowed_at_ef(
                &VectorIndexRef::new("items", "embedding"),
                &[0.9, 0.2, 0.1],
                10,
                513,
                10,
                || 1..=513,
                false,
                |_| true,
                |_| Ok(()),
                |_| {},
            )
            .unwrap();
        assert_eq!(found.status, HnswAllowedSearchStatus::Complete);
        assert_eq!(found.rows.len(), 10);
        assert!(found.rows.iter().all(|(id, _)| (1..=17).contains(&id.0)));
    }

    #[test]
    fn ordinary_allowed_search_excluded_history_adds_no_visit_or_distance_work() {
        let mut entries = Vec::new();
        for ordinal in 0..8_u64 {
            entries.push(entry(
                RowId(ordinal + 1),
                &[ordinal as f32 + 1.0, 9.0 - ordinal as f32, 1.0],
                TxId(ordinal + 1),
            ));
        }
        let policy = crate::store::ResolvedVectorPolicy {
            auto_index_at: 1,
            hnsw_m: 8,
            hnsw_ef_construction: 32,
            hnsw_ef_search: 16,
            policy_revision: 1,
            auto_index_at_source: "declared",
            ef_search_source: "declared",
        };
        let graph = HnswIndex::new_with_policy(&entries, 3, VectorQuantization::F32, policy);
        let search = |graph: &HnswIndex| {
            graph
                .search_allowed_at_ef(
                    &VectorIndexRef::new("items", "embedding"),
                    &[1.0, 8.0, 1.0],
                    4,
                    8,
                    16,
                    std::iter::empty,
                    false,
                    |row_id| row_id.0 <= 8,
                    |_| Ok(()),
                    |_| {},
                )
                .expect("ordinary allowed-id traversal completes or refuses within its fixed work")
        };
        let before_history = search(&graph);
        for ordinal in 8..2_000_u64 {
            graph
                .insert(&entry(
                    RowId(ordinal + 1),
                    &[ordinal as f32 + 1.0, 2_001.0 - ordinal as f32, 1.0],
                    TxId(ordinal + 1),
                ))
                .expect("append excluded history to the maintained graph");
        }
        let after_history = search(&graph);

        assert!(
            after_history.visited_nodes <= before_history.visited_nodes,
            "excluded history does not add allowed graph visits"
        );
        assert!(
            after_history.vector_evaluations <= before_history.vector_evaluations,
            "excluded history does not add allowed vector distances"
        );
        assert!(after_history.rows.iter().all(|(row_id, _)| row_id.0 <= 8));
    }
}
