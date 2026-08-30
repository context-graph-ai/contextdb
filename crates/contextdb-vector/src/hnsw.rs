use crate::quantized::{StoredVector, StoredVectorEntry, quantized_hnsw_distance};
use anndists::dist::distances::{DistCosine, Distance};
use contextdb_core::{Error, Result, RowId, VectorIndexRef, VectorQuantization};
use hnsw_rs::hnsw::{
    Hnsw, HnswBoundedSearchError, HnswSearchScratchContainer, HnswSearchScratchEvent,
};
use parking_lot::RwLock;
#[cfg(feature = "test-seams")]
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
#[cfg(feature = "test-seams")]
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct HnswIndex {
    hnsw: HnswInner,
    id_to_row: RwLock<HashMap<usize, RowId>>,
    exact_rows: RwLock<HashMap<Vec<u8>, Vec<RowId>>>,
    next_id: AtomicUsize,
    build_serial: u64,
    dimension: usize,
    quantization: VectorQuantization,
    ef_search: usize,
}

/// Final rows from the bounded HNSW path.  `retained_bytes` covers only the
/// returned row vector's observable capacity; every HNSW scratch container
/// has already emitted its matching release event.
#[derive(Debug)]
pub struct HnswSearchMemoryResult {
    pub rows: Vec<(RowId, f32)>,
    pub retained_bytes: usize,
}

static NEXT_HNSW_BUILD_SERIAL: AtomicUsize = AtomicUsize::new(1);
const HNSW_BUILD_SEED: u64 = 0x55c8_6f2d_21a4_7bd3;

enum HnswInner {
    F32(Hnsw<'static, f32, HnswF32Distance>),
    Quantized(Hnsw<'static, u8, DistQuantizedCosine>),
}

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
    pub(crate) fn new(
        entries: &[StoredVectorEntry],
        dimension: usize,
        quantization: VectorQuantization,
    ) -> Self {
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

        let (m, ef_construction, ef_search) = select_params(sorted_entries.len(), quantization);
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
                HnswInner::F32(hnsw)
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
                HnswInner::Quantized(hnsw)
            }
        };
        let mut id_to_row = HashMap::with_capacity(sorted_entries.len());
        let mut exact_rows = HashMap::<Vec<u8>, Vec<RowId>>::with_capacity(sorted_entries.len());
        let mut inserted_count = 0usize;

        match &hnsw {
            HnswInner::F32(index) => {
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
                    index.insert((vector, data_id));
                }
            }
            HnswInner::Quantized(index) => {
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
                    index.insert((encoded.as_slice(), data_id));
                }
            }
        }

        for row_ids in exact_rows.values_mut() {
            row_ids.sort_unstable();
            row_ids.dedup();
        }

        Self {
            hnsw,
            id_to_row: RwLock::new(id_to_row),
            exact_rows: RwLock::new(exact_rows),
            next_id: AtomicUsize::new(inserted_count),
            build_serial: NEXT_HNSW_BUILD_SERIAL.fetch_add(1, Ordering::SeqCst) as u64,
            dimension,
            quantization,
            ef_search,
        }
    }

    /// Number of vectors currently indexed in the HNSW graph.
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[doc(hidden)]
    pub fn graph_stats(&self) -> HnswGraphStats {
        let (point_count, layer0_neighbor_edges, max_level_observed) = match &self.hnsw {
            HnswInner::F32(hnsw) => hnsw_stats(hnsw),
            HnswInner::Quantized(hnsw) => hnsw_stats(hnsw),
        };

        HnswGraphStats {
            point_count,
            layer0_points: point_count,
            layer0_neighbor_edges,
            max_level_observed,
            dimension: self.dimension,
        }
    }

    pub fn search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
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

        let ef = hnsw_search_ef(self.ef_search, k);
        let neighbors = match &self.hnsw {
            HnswInner::F32(hnsw) => hnsw.search(query, ef, ef),
            HnswInner::Quantized(hnsw) => {
                let encoded = StoredVector::from_f32(query, self.quantization).to_hnsw_u8();
                hnsw.search(&encoded, ef, ef)
            }
        };
        let id_to_row = self.id_to_row.read();
        let cap = hnsw_search_candidate_cap(self.ef_search, k);
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
            id_to_row
                .get(&neighbor.d_id)
                .copied()
                .map(|row_id| (row_id, 1.0 - neighbor.distance))
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

        let ef = hnsw_search_ef(self.ef_search, k);
        let mut encoded_query = None;
        if matches!(&self.hnsw, HnswInner::Quantized(_)) {
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
            HnswInner::F32(hnsw) => hnsw.search_with_bounded_control(
                query,
                ef,
                ef,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            ),
            HnswInner::Quantized(hnsw) => {
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
        let cap = hnsw_search_candidate_cap(self.ef_search, k);
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
                scored.push((row_id, 1.0 - neighbour.distance));
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
            HnswInner::F32(hnsw) => hnsw_topology_digest(hnsw),
            HnswInner::Quantized(hnsw) => hnsw_topology_digest(hnsw),
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

fn digest_i32(digest: &mut u64, value: i32) {
    digest_u64(digest, value as u32 as u64);
}

fn digest_u64(digest: &mut u64, value: u64) {
    *digest ^= value;
    *digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
}

pub(crate) fn hnsw_search_candidate_cap_for_count(
    count: usize,
    quantization: VectorQuantization,
    k: usize,
) -> usize {
    let (_, _, ef_search) = select_params(count, quantization);
    hnsw_search_candidate_cap(ef_search, k)
}

fn hnsw_search_candidate_cap(ef_search: usize, k: usize) -> usize {
    hnsw_search_ef(ef_search, k).max(k)
}

fn hnsw_search_ef(ef_search: usize, k: usize) -> usize {
    ef_search.max(k.saturating_mul(10)).max(1)
}

fn select_params(count: usize, quantization: VectorQuantization) -> (usize, usize, usize) {
    if !matches!(quantization, VectorQuantization::F32) {
        return match count {
            0..=5000 => (8, 32, 96.min(count.max(32))),
            5001..=50000 => (12, 64, 128),
            _ => (12, 64, 128),
        };
    }
    match count {
        0..=5000 => (16, 200, count.max(200)),
        5001..=50000 => (24, 400, 400),
        _ => (16, 200, 200),
    }
}

fn insertion_key(entry: &StoredVectorEntry) -> u64 {
    let mut x = entry.row_id.0 ^ entry.lsn.0 ^ entry.created_tx.0;
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
