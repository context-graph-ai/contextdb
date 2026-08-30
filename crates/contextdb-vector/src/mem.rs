use crate::memory_budget::{MemoryBudget, unlimited_memory_budget};
use crate::{HnswIndex, store::VectorStore};
use contextdb_core::read_contract::ReadFailure;
use contextdb_core::*;
use contextdb_tx::{TransactionManager, WriteSetApplicator};
use hnsw_rs::hnsw::HnswSearchScratchEvent;
use parking_lot::RwLock;
use roaring::RoaringTreemap;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

const HNSW_THRESHOLD: usize = 1000;
const QUANTIZED_EXACT_SEARCH_LIMIT: usize = 5000;

fn sort_vector_scores(rows: &mut [(RowId, f32)]) {
    rows.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
}

fn vector_score_quality(left: &(RowId, f32), right: &(RowId, f32)) -> std::cmp::Ordering {
    left.1
        .total_cmp(&right.1)
        .then_with(|| right.0.cmp(&left.0))
}

fn push_bounded_score_heap(scores: &mut Vec<(RowId, f32)>, score: (RowId, f32)) -> Result<()> {
    scores.push(score);
    let mut child = scores.len().checked_sub(1).ok_or_else(|| {
        Error::Other("bounded brute-force heap lost its inserted score".to_string())
    })?;
    while child != 0 {
        let parent = child
            .checked_sub(1)
            .ok_or_else(|| Error::Other("bounded brute-force heap parent underflow".to_string()))?
            / 2;
        if vector_score_quality(&scores[child], &scores[parent]) != std::cmp::Ordering::Less {
            break;
        }
        scores.swap(child, parent);
        child = parent;
    }
    Ok(())
}

fn replace_bounded_score_heap_root(scores: &mut [(RowId, f32)], score: (RowId, f32)) -> Result<()> {
    let Some(root) = scores.first_mut() else {
        return Err(Error::Other(
            "bounded brute-force heap has no replacement root".to_string(),
        ));
    };
    *root = score;
    let mut parent = 0usize;
    loop {
        let Some(left) = parent.checked_mul(2).and_then(|value| value.checked_add(1)) else {
            return Err(Error::Other(
                "bounded brute-force heap child overflow".to_string(),
            ));
        };
        if left >= scores.len() {
            return Ok(());
        }
        let right = left
            .checked_add(1)
            .ok_or_else(|| Error::Other("bounded brute-force heap child overflow".to_string()))?;
        let child = if right < scores.len()
            && vector_score_quality(&scores[right], &scores[left]) == std::cmp::Ordering::Less
        {
            right
        } else {
            left
        };
        if vector_score_quality(&scores[child], &scores[parent]) != std::cmp::Ordering::Less {
            return Ok(());
        }
        scores.swap(parent, child);
        parent = child;
    }
}

fn query_has_positive_finite_norm(query: &[f32]) -> bool {
    let mut norm = 0.0_f32;
    for value in query {
        if !value.is_finite() {
            return false;
        }
        norm += value * value;
    }
    norm > 0.0
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchDebugTrace {
    pub index: VectorIndexRef,
    pub used_hnsw: bool,
    pub hnsw_len: Option<usize>,
    pub hnsw_candidate_count: usize,
    pub hnsw_candidate_row_ids: Vec<RowId>,
    pub final_row_ids: Vec<RowId>,
    pub supplemented_row_count: usize,
    pub fallback_reason: Option<&'static str>,
}

impl VectorSearchDebugTrace {
    fn brute_force(
        index: &VectorIndexRef,
        rows: &[(RowId, f32)],
        fallback_reason: &'static str,
        hnsw_len: Option<usize>,
    ) -> Self {
        Self {
            index: index.clone(),
            used_hnsw: false,
            hnsw_len,
            hnsw_candidate_count: 0,
            hnsw_candidate_row_ids: Vec::new(),
            final_row_ids: rows.iter().map(|(row_id, _)| *row_id).collect(),
            supplemented_row_count: 0,
            fallback_reason: Some(fallback_reason),
        }
    }

    fn hnsw(
        index: &VectorIndexRef,
        hnsw_len: usize,
        hnsw_candidate_row_ids: Vec<RowId>,
        rows: &[(RowId, f32)],
        supplemented_row_count: usize,
    ) -> Self {
        Self {
            index: index.clone(),
            used_hnsw: true,
            hnsw_len: Some(hnsw_len),
            hnsw_candidate_count: hnsw_candidate_row_ids.len(),
            hnsw_candidate_row_ids,
            final_row_ids: rows.iter().map(|(row_id, _)| *row_id).collect(),
            supplemented_row_count,
            fallback_reason: None,
        }
    }
}

/// One-step result from a suspendable vector source. `Pending` means source
/// preparation progressed without producing an output row; work is charged
/// only when the source examines a real vector candidate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BoundedVectorStep {
    Pending,
    Row(RowId, f32),
    Exhausted,
}

/// Owned exact-search continuation. It holds no vector-store lock and advances
/// only after the current candidate has been admitted and inspected.
#[derive(Debug)]
pub struct BoundedBruteForceCursor {
    index: VectorIndexRef,
    query: Vec<f32>,
    k: usize,
    candidates: Option<Vec<u64>>,
    snapshot: SnapshotId,
    position: usize,
    end: usize,
    pending_score: Option<(RowId, f32)>,
    scored: Vec<(RowId, f32)>,
    output_position: usize,
    prepared: bool,
    retained_bytes: usize,
}

/// A search trace whose row-id vectors and index names were admitted through
/// the request's memory callbacks. It is deliberately not clonable: a copy
/// would duplicate that retained payload outside the accounting that admitted
/// it. Publish it by moving the payload out with [`BoundedVectorSearchTrace::into_trace`],
/// which transfers the charged allocation instead of duplicating it.
#[derive(Debug)]
pub struct BoundedVectorSearchTrace {
    trace: VectorSearchDebugTrace,
    retained_bytes: usize,
}

impl BoundedVectorSearchTrace {
    /// Bytes admitted for this trace, already included in the owning result's
    /// `retained_bytes`.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Take the admitted payload. No borrowing view is offered: the inner
    /// trace derives `Clone`, so a shared reference would let any caller
    /// duplicate the admitted payload without a charge. The receiver owns the
    /// charge from here on and settles it by releasing [`Self::retained_bytes`]
    /// at the moment the payload leaves bounded accounting.
    pub fn into_trace(self) -> VectorSearchDebugTrace {
        self.trace
    }
}

/// Materialized output from the causally fallible HNSW source. The search
/// itself remains graph-driven, but every query-to-candidate distance is
/// preceded by the caller's fallible control callback.
#[derive(Debug)]
pub struct BoundedHnswResult {
    pub rows: Vec<(RowId, f32)>,
    pub trace: BoundedVectorSearchTrace,
    pub retained_bytes: usize,
}

impl BoundedBruteForceCursor {
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Consume the continuation and report the bytes still owed back. A
    /// discarded cursor keeps its charge until the caller releases this
    /// amount, so disposing through this method is what balances it.
    pub fn into_retained_bytes(self) -> usize {
        self.retained_bytes
    }

    pub fn is_exhausted(&self) -> bool {
        self.prepared && self.output_position >= self.scored.len()
    }
}

fn checked_vector_add(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_add(right).ok_or_else(|| {
        Error::Other(format!(
            "bounded vector {operation} memory size exceeds the native address space"
        ))
    })
}

fn checked_vector_mul(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_mul(right).ok_or_else(|| {
        Error::Other(format!(
            "bounded vector {operation} memory size exceeds the native address space"
        ))
    })
}

fn apply_hnsw_scratch_acquire<E>(
    event: HnswSearchScratchEvent,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    let bytes = |capacity, element_bytes, operation| {
        checked_vector_mul(capacity, element_bytes, operation).map_err(E::from)
    };
    match event {
        HnswSearchScratchEvent::Reserve {
            previous_capacity,
            requested_capacity,
            element_bytes,
            ..
        } => {
            let growth = requested_capacity
                .checked_sub(previous_capacity)
                .ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW capacity moved backwards before allocation".to_string(),
                    ))
                })?;
            before_retain(bytes(growth, element_bytes, "HNSW reserve")?)
        }
        HnswSearchScratchEvent::Reconcile {
            requested_capacity,
            actual_capacity,
            element_bytes,
            ..
        } if actual_capacity > requested_capacity => before_retain(bytes(
            actual_capacity
                .checked_sub(requested_capacity)
                .ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW capacity reconciliation underflow".to_string(),
                    ))
                })?,
            element_bytes,
            "HNSW capacity reconciliation",
        )?),
        HnswSearchScratchEvent::Reconcile { .. } => Ok(()),
        HnswSearchScratchEvent::Release { .. } => Err(E::from(Error::Other(
            "bounded HNSW sent a release through its acquire callback".to_string(),
        ))),
    }
}

fn apply_hnsw_scratch_release(
    event: HnswSearchScratchEvent,
    release_retained: &mut impl FnMut(usize),
) {
    if let HnswSearchScratchEvent::Release {
        capacity,
        element_bytes,
        ..
    } = event
    {
        // Giving a charge back must never itself fail: the acquire side already
        // refused any size the address space cannot represent, so saturating
        // here can only ever reproduce the amount that was charged.
        release_retained(capacity.saturating_mul(element_bytes));
    }
}

fn reconcile_vector_allocation<E>(
    admitted: usize,
    actual: usize,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<usize, E> {
    if actual > admitted {
        if let Err(error) = before_retain(actual - admitted) {
            release_retained(admitted);
            return Err(error);
        }
    } else if admitted > actual {
        release_retained(admitted - actual);
    }
    Ok(actual)
}

fn allocate_vector_copy<E, T: Copy>(
    source: &[T],
    operation: &str,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<T>, usize), E>
where
    E: From<Error>,
{
    if source.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let requested =
        checked_vector_mul(source.len(), std::mem::size_of::<T>(), operation).map_err(E::from)?;
    before_retain(requested)?;
    let mut values = Vec::new();
    if values.try_reserve_exact(source.len()).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} allocation failed"
        ))));
    }
    let actual = match checked_vector_mul(values.capacity(), std::mem::size_of::<T>(), operation) {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(requested);
            return Err(E::from(error));
        }
    };
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} capacity moved backwards"
        ))));
    }
    let actual = reconcile_vector_allocation(requested, actual, before_retain, release_retained)?;
    let mut position = 0usize;
    while position < source.len() {
        if let Err(error) = before_work() {
            release_retained(actual);
            return Err(error);
        }
        let value = match source.get(position) {
            Some(value) => *value,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other(format!(
                    "bounded vector {operation} source position changed"
                ))));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other(format!(
                    "bounded vector {operation} source position overflow"
                ))));
            }
        };
        values.push(value);
    }
    Ok((values, actual))
}

fn allocate_vector_string<E>(
    source: &str,
    operation: &str,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(String, usize), E>
where
    E: From<Error>,
{
    if source.is_empty() {
        return Ok((String::new(), 0));
    }
    let requested = source.len();
    before_retain(requested)?;
    let mut owned = String::new();
    if owned.try_reserve_exact(requested).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} allocation failed"
        ))));
    }
    let actual = owned.capacity();
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} capacity moved backwards"
        ))));
    }
    let actual = reconcile_vector_allocation(requested, actual, before_retain, release_retained)?;
    owned.push_str(source);
    Ok((owned, actual))
}

fn allocate_vector_index<E>(
    source: &VectorIndexRef,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(VectorIndexRef, usize), E>
where
    E: From<Error>,
{
    let (table, table_bytes) = allocate_vector_string(
        &source.table,
        "HNSW trace table name",
        before_retain,
        release_retained,
    )?;
    let (column, column_bytes) = match allocate_vector_string(
        &source.column,
        "HNSW trace column name",
        before_retain,
        release_retained,
    ) {
        Ok(column) => column,
        Err(error) => {
            release_retained(table_bytes);
            return Err(error);
        }
    };
    let retained_bytes = match checked_vector_add(table_bytes, column_bytes, "HNSW trace index") {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(column_bytes);
            release_retained(table_bytes);
            return Err(E::from(error));
        }
    };
    Ok((VectorIndexRef { table, column }, retained_bytes))
}

fn next_vector_capacity(current: usize, required: usize) -> Result<usize> {
    if current == 0 {
        return Ok(required);
    }
    Ok(current
        .checked_mul(2)
        .ok_or_else(|| Error::Other("bounded vector capacity overflow".to_string()))?
        .max(required))
}

fn grow_vector_copy<E, T: Copy>(
    values: &mut Vec<T>,
    required: usize,
    retained_bytes: Option<&mut usize>,
    operation: &str,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<usize, E>
where
    E: From<Error>,
{
    if required <= values.capacity() {
        return checked_vector_mul(values.capacity(), std::mem::size_of::<T>(), operation)
            .map_err(E::from);
    }
    let previous_capacity = values.capacity();
    let requested_capacity = next_vector_capacity(previous_capacity, required).map_err(E::from)?;
    let requested = checked_vector_mul(requested_capacity, std::mem::size_of::<T>(), operation)
        .map_err(E::from)?;
    before_retain(requested)?;
    let mut replacement = Vec::new();
    if replacement.try_reserve_exact(requested_capacity).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} allocation failed"
        ))));
    }
    let actual =
        match checked_vector_mul(replacement.capacity(), std::mem::size_of::<T>(), operation) {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(requested);
                return Err(E::from(error));
            }
        };
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} capacity moved backwards"
        ))));
    }
    let actual = reconcile_vector_allocation(requested, actual, before_retain, release_retained)?;
    let previous = match checked_vector_mul(previous_capacity, std::mem::size_of::<T>(), operation)
    {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(actual);
            return Err(E::from(error));
        }
    };
    let next_retained = retained_bytes
        .as_deref()
        .map(|retained| {
            actual
                .checked_sub(previous)
                .and_then(|growth| retained.checked_add(growth))
                .ok_or_else(|| {
                    E::from(Error::Other(format!(
                        "bounded vector {operation} retained-memory overflow"
                    )))
                })
        })
        .transpose();
    let next_retained = match next_retained {
        Ok(next) => next,
        Err(error) => {
            release_retained(actual);
            return Err(error);
        }
    };
    let mut position = 0usize;
    while position < values.len() {
        if let Err(error) = before_work() {
            release_retained(actual);
            return Err(error);
        }
        let value = match values.get(position) {
            Some(value) => *value,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other(format!(
                    "bounded vector {operation} changed during migration"
                ))));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other(format!(
                    "bounded vector {operation} migration position overflow"
                ))));
            }
        };
        replacement.push(value);
    }
    let old = std::mem::replace(values, replacement);
    drop(old);
    release_retained(previous);
    if let (Some(retained), Some(next)) = (retained_bytes, next_retained) {
        *retained = next;
    }
    Ok(actual)
}

fn grow_vector_set<E, T>(
    values: &mut HashSet<T>,
    required: usize,
    operation: &str,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<usize, E>
where
    E: From<Error>,
    T: std::hash::Hash + Eq + Copy,
{
    if required <= values.capacity() {
        return checked_vector_mul(values.capacity(), std::mem::size_of::<T>(), operation)
            .map_err(E::from);
    }
    let previous_capacity = values.capacity();
    let requested_capacity = next_vector_capacity(previous_capacity, required).map_err(E::from)?;
    let requested = checked_vector_mul(requested_capacity, std::mem::size_of::<T>(), operation)
        .map_err(E::from)?;
    before_retain(requested)?;
    let mut replacement = HashSet::new();
    if replacement.try_reserve(requested_capacity).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} allocation failed"
        ))));
    }
    let actual =
        match checked_vector_mul(replacement.capacity(), std::mem::size_of::<T>(), operation) {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(requested);
                return Err(E::from(error));
            }
        };
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} capacity moved backwards"
        ))));
    }
    let actual = reconcile_vector_allocation(requested, actual, before_retain, release_retained)?;
    let mut remaining = values.len();
    let mut source = values.iter();
    while remaining != 0 {
        if let Err(error) = before_work() {
            release_retained(actual);
            return Err(error);
        }
        let Some(value) = source.next().copied() else {
            release_retained(actual);
            return Err(E::from(Error::Other(format!(
                "bounded vector {operation} changed during migration"
            ))));
        };
        remaining = match remaining.checked_sub(1) {
            Some(remaining) => remaining,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other(format!(
                    "bounded vector {operation} migration underflow"
                ))));
            }
        };
        replacement.insert(value);
    }
    let previous = match checked_vector_mul(previous_capacity, std::mem::size_of::<T>(), operation)
    {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(actual);
            return Err(E::from(error));
        }
    };
    let old = std::mem::replace(values, replacement);
    drop(old);
    release_retained(previous);
    Ok(actual)
}

fn allocate_vector_row_ids<E>(
    source: &[(RowId, f32)],
    operation: &str,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<RowId>, usize), E>
where
    E: From<Error>,
{
    let requested = checked_vector_mul(source.len(), std::mem::size_of::<RowId>(), operation)
        .map_err(E::from)?;
    if requested != 0 {
        before_retain(requested)?;
    }
    let mut row_ids = Vec::new();
    if row_ids.try_reserve_exact(source.len()).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} allocation failed"
        ))));
    }
    let actual =
        match checked_vector_mul(row_ids.capacity(), std::mem::size_of::<RowId>(), operation) {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(requested);
                return Err(E::from(error));
            }
        };
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} capacity moved backwards"
        ))));
    }
    let actual = reconcile_vector_allocation(requested, actual, before_retain, release_retained)?;
    let mut position = 0usize;
    while position < source.len() {
        if let Err(error) = before_work() {
            release_retained(actual);
            return Err(error);
        }
        let row_id = match source.get(position) {
            Some((row_id, _)) => *row_id,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other(format!(
                    "bounded vector {operation} source position changed"
                ))));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other(format!(
                    "bounded vector {operation} source position overflow"
                ))));
            }
        };
        row_ids.push(row_id);
    }
    Ok((row_ids, actual))
}

#[allow(clippy::too_many_arguments)]
fn bounded_hnsw_trace<E>(
    index: &VectorIndexRef,
    hnsw_len: usize,
    raw_candidates: &[(RowId, f32)],
    rows: &[(RowId, f32)],
    supplemented_row_count: usize,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<BoundedVectorSearchTrace, E>
where
    E: From<Error>,
{
    let (owned_index, index_bytes) = allocate_vector_index(index, before_retain, release_retained)?;
    let (candidate_row_ids, candidate_bytes) = match allocate_vector_row_ids(
        raw_candidates,
        "HNSW trace candidates",
        before_work,
        before_retain,
        release_retained,
    ) {
        Ok(ids) => ids,
        Err(error) => {
            release_retained(index_bytes);
            return Err(error);
        }
    };
    let (final_row_ids, final_bytes) = match allocate_vector_row_ids(
        rows,
        "HNSW trace final rows",
        before_work,
        before_retain,
        release_retained,
    ) {
        Ok(ids) => ids,
        Err(error) => {
            release_retained(candidate_bytes);
            release_retained(index_bytes);
            return Err(error);
        }
    };
    let retained_bytes = match checked_vector_add(index_bytes, candidate_bytes, "HNSW trace")
        .and_then(|bytes| checked_vector_add(bytes, final_bytes, "HNSW trace"))
    {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(final_bytes);
            release_retained(candidate_bytes);
            release_retained(index_bytes);
            return Err(E::from(error));
        }
    };
    Ok(BoundedVectorSearchTrace {
        trace: VectorSearchDebugTrace {
            index: owned_index,
            used_hnsw: true,
            hnsw_len: Some(hnsw_len),
            hnsw_candidate_count: candidate_row_ids.len(),
            hnsw_candidate_row_ids: candidate_row_ids,
            final_row_ids,
            supplemented_row_count,
            fallback_reason: None,
        },
        retained_bytes,
    })
}

pub struct MemVectorExecutor<S: WriteSetApplicator> {
    store: Arc<VectorStore>,
    tx_mgr: Arc<TransactionManager<S>>,
    accountant: Arc<dyn MemoryBudget>,
}

impl<S: WriteSetApplicator> MemVectorExecutor<S> {
    pub fn new(
        store: Arc<VectorStore>,
        tx_mgr: Arc<TransactionManager<S>>,
        hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
    ) -> Self {
        Self::new_with_accountant(store, tx_mgr, hnsw, unlimited_memory_budget())
    }

    pub fn new_with_accountant(
        store: Arc<VectorStore>,
        tx_mgr: Arc<TransactionManager<S>>,
        _hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Self {
        Self {
            store,
            tx_mgr,
            accountant,
        }
    }

    /// Create a fallible, owned brute-force continuation. Query and candidate
    /// allocations are admitted and reconciled before their values are copied.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_brute_force_cursor<E>(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedBruteForceCursor, E>
    where
        E: From<Error>,
    {
        // Resolving the index handle takes the registry lock, so the control
        // runs before any source state is read or locked.
        before_source_entry()?;
        let state = self.store.state(&index).map_err(E::from)?;
        if query.len() != state.dimension() {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index,
                expected: state.dimension(),
                actual: query.len(),
            }));
        }
        let index_bytes =
            checked_vector_add(index.table.capacity(), index.column.capacity(), "index")
                .map_err(E::from)?;
        before_retain(index_bytes)?;
        let (owned_query, query_bytes) = match allocate_vector_copy(
            query,
            "query",
            &mut before_source_entry,
            &mut before_retain,
            &mut release_retained,
        ) {
            Ok(query) => query,
            Err(error) => {
                release_retained(index_bytes);
                return Err(error);
            }
        };
        let (owned_candidates, candidate_bytes) = match candidates {
            Some(candidates) => match allocate_vector_copy(
                candidates,
                "candidate ids",
                &mut before_source_entry,
                &mut before_retain,
                &mut release_retained,
            ) {
                Ok((candidates, bytes)) => (Some(candidates), bytes),
                Err(error) => {
                    release_retained(query_bytes);
                    release_retained(index_bytes);
                    return Err(error);
                }
            },
            None => (None, 0),
        };
        let retained_bytes = match checked_vector_add(index_bytes, query_bytes, "query state")
            .and_then(|bytes| checked_vector_add(bytes, candidate_bytes, "candidate ids"))
        {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(candidate_bytes);
                release_retained(query_bytes);
                release_retained(index_bytes);
                return Err(E::from(error));
            }
        };
        // `entry_count` takes the vector-entry lock a writer may hold, so the
        // caller gets its refusal point before the acquisition.
        if let Err(error) = before_source_entry() {
            release_retained(retained_bytes);
            return Err(error);
        }
        let end = state.entry_count();
        Ok(BoundedBruteForceCursor {
            index,
            query: owned_query,
            k,
            candidates: owned_candidates,
            snapshot,
            position: 0,
            end,
            pending_score: None,
            scored: Vec::new(),
            output_position: 0,
            prepared: false,
            retained_bytes,
        })
    }

    /// Advance exactly one candidate/retained-score/output boundary.
    pub fn bounded_brute_force_step<E>(
        &self,
        cursor: &mut BoundedBruteForceCursor,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_distance: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedVectorStep, E>
    where
        E: From<Error>,
    {
        if cursor.prepared {
            let Some((row_id, score)) = cursor.scored.get(cursor.output_position).copied() else {
                return Ok(BoundedVectorStep::Exhausted);
            };
            cursor.output_position = cursor.output_position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded vector output position overflow".to_string(),
                ))
            })?;
            return Ok(BoundedVectorStep::Row(row_id, score));
        }

        if let Some(candidate) = cursor.pending_score {
            if cursor.k != 0 {
                if cursor.scored.len() < cursor.k {
                    let required = cursor.scored.len().checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded brute-force score length overflow".to_string(),
                        ))
                    })?;
                    grow_vector_copy(
                        &mut cursor.scored,
                        required,
                        Some(&mut cursor.retained_bytes),
                        "brute-force scores",
                        &mut before_source_entry,
                        &mut before_retain,
                        &mut release_retained,
                    )?;
                    push_bounded_score_heap(&mut cursor.scored, candidate).map_err(E::from)?;
                } else if cursor.scored.first().is_some_and(|worst| {
                    vector_score_quality(&candidate, worst) == std::cmp::Ordering::Greater
                }) {
                    replace_bounded_score_heap_root(&mut cursor.scored, candidate)
                        .map_err(E::from)?;
                }
            }
            cursor.pending_score = None;
            return Ok(BoundedVectorStep::Pending);
        }

        if cursor.position >= cursor.end {
            sort_vector_scores(&mut cursor.scored);
            cursor.prepared = true;
            return Ok(BoundedVectorStep::Pending);
        }

        // The control runs before the index handle is resolved, so no source
        // state is read or locked until the caller has admitted this candidate.
        before_source_entry()?;
        let position = cursor.position;
        let state = self.store.state(&cursor.index).map_err(E::from)?;
        let scored = state.with_entries(|entries| -> std::result::Result<Option<_>, E> {
            let entry = entries.get(position).ok_or_else(|| {
                E::from(Error::ReadFailure(ReadFailure::invalid_continuation(
                    "the candidate this read left off at was removed while it was suspended"
                        .to_string(),
                )))
            })?;
            if !entry.visible_at(cursor.snapshot)
                || cursor
                    .candidates
                    .as_ref()
                    .is_some_and(|candidates| candidates.binary_search(&entry.row_id.0).is_err())
            {
                return Ok(None);
            }
            before_distance()?;
            Ok(Some((
                entry.row_id,
                entry.vector.cosine_similarity(&cursor.query),
            )))
        })?;
        cursor.position = cursor.position.checked_add(1).ok_or_else(|| {
            E::from(Error::Other(
                "bounded vector candidate position overflow".to_string(),
            ))
        })?;
        cursor.pending_score = scored;
        Ok(BoundedVectorStep::Pending)
    }

    /// Run only an already-built, snapshot-compatible HNSW graph. `None`
    /// means the exact brute-force continuation must be used instead. The
    /// HNSW reports every real scratch-capacity transition through the two
    /// memory callbacks, so no graph-length estimate is used for admission.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_hnsw_search<E>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_distance: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<Option<BoundedHnswResult>, E>
    where
        E: From<Error>,
    {
        // Resolving the index handle already reads registered source state, so
        // the control runs first.
        before_source_entry()?;
        let state = self.store.state(index).map_err(E::from)?;
        if query.len() != state.dimension() {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: state.dimension(),
                actual: query.len(),
            }));
        }
        let snapshot_tx = TxId::from_snapshot(snapshot);
        if !query_has_positive_finite_norm(query) {
            return Ok(None);
        }
        // The eligibility facts come from one charged pass over the stored
        // entries. Reading them through `entry_count`, `max_tx` and
        // `vector_count` would iterate every entry two or three times under the
        // store lock with nothing charged and no cancellation point, so a
        // request whose budget is already spent could not refuse until after
        // the whole index had been read.
        let eligibility = state.bounded_hnsw_eligibility(snapshot_tx, &mut before_source_entry)?;
        if eligibility.entry_count == 0
            || eligibility.newer_than_snapshot
            || eligibility.live_count < HNSW_THRESHOLD
            || (!matches!(state.quantization(), VectorQuantization::F32)
                && eligibility.live_count <= QUANTIZED_EXACT_SEARCH_LIMIT)
        {
            return Ok(None);
        }
        let Some(lock) = state.hnsw().get() else {
            return Ok(None);
        };
        // A writer can hold the graph lock, so the caller gets its refusal
        // point immediately before the acquisition rather than behind it.
        before_source_entry()?;
        let guard = lock.read();
        let Some(hnsw) = guard.as_ref() else {
            return Ok(None);
        };
        let hnsw_len = hnsw.len();
        let graph_covering_request =
            crate::hnsw::hnsw_search_candidate_cap_for_count(hnsw_len, state.quantization(), k)
                >= hnsw_len;
        if candidates.is_some() && !graph_covering_request {
            return Ok(None);
        }

        let searched = hnsw.search_with_bounded_memory(
            index,
            query,
            k,
            &mut before_source_entry,
            &mut before_distance,
            |event| apply_hnsw_scratch_acquire(event, &mut before_retain),
            |event| apply_hnsw_scratch_release(event, &mut release_retained),
        )?;
        let raw_result_bytes = searched.retained_bytes;
        let raw_candidates = searched.rows;
        // Test-seam only: shorten what the graph handed back, without touching
        // the allocation already charged above. Compiled out in production.
        #[cfg(feature = "test-seams")]
        let raw_candidates = {
            let mut raw_candidates = raw_candidates;
            self.store
                .graph_candidate_caps()
                .cap_graph_candidates(index, &mut raw_candidates);
            raw_candidates
        };
        let raw_candidate_count = raw_candidates.len();
        let supplement_missing = graph_covering_request
            || raw_candidate_count
                .checked_add(64)
                .is_some_and(|count| count >= hnsw_len);
        let mut raw_row_ids = HashSet::new();
        let mut raw_row_id_bytes = 0usize;
        if supplement_missing {
            let mut raw_position = 0usize;
            while raw_position < raw_candidates.len() {
                if let Err(error) = before_source_entry() {
                    release_retained(raw_result_bytes);
                    release_retained(raw_row_id_bytes);
                    return Err(error);
                }
                let row_id = match raw_candidates.get(raw_position) {
                    Some((row_id, _)) => *row_id,
                    None => {
                        release_retained(raw_result_bytes);
                        release_retained(raw_row_id_bytes);
                        return Err(E::from(Error::Other(
                            "bounded HNSW raw-row position changed".to_string(),
                        )));
                    }
                };
                raw_position = match raw_position.checked_add(1) {
                    Some(position) => position,
                    None => {
                        release_retained(raw_result_bytes);
                        release_retained(raw_row_id_bytes);
                        return Err(E::from(Error::Other(
                            "bounded HNSW raw-row position overflow".to_string(),
                        )));
                    }
                };
                if raw_row_ids.contains(&row_id) {
                    continue;
                }
                let required = raw_row_ids.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW supplemented-row set overflow".to_string(),
                    ))
                });
                let required = match required {
                    Ok(required) => required,
                    Err(error) => {
                        release_retained(raw_result_bytes);
                        release_retained(raw_row_id_bytes);
                        return Err(error);
                    }
                };
                raw_row_id_bytes = match grow_vector_set(
                    &mut raw_row_ids,
                    required,
                    "HNSW supplemented row ids",
                    &mut before_source_entry,
                    &mut before_retain,
                    &mut release_retained,
                ) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        release_retained(raw_result_bytes);
                        release_retained(raw_row_id_bytes);
                        return Err(error);
                    }
                };
                raw_row_ids.insert(row_id);
            }
        }
        let mut rows = Vec::new();
        let mut row_bytes = 0usize;
        let mut supplemented_row_count = 0usize;
        // `with_entries` takes the vector-entry lock, and the rerank below
        // charges only once it is inside the loop, so the refusal point comes
        // before the acquisition.
        if let Err(error) = before_source_entry() {
            release_retained(raw_row_id_bytes);
            release_retained(raw_result_bytes);
            return Err(error);
        }
        let scored = state.with_entries(|entries| -> std::result::Result<(), E> {
            let mut raw_position = 0usize;
            while raw_position < raw_candidates.len() {
                before_source_entry()?;
                let (row_id, raw_score) = *raw_candidates.get(raw_position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW raw-candidate position changed".to_string(),
                    ))
                })?;
                raw_position = raw_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW raw-candidate position overflow".to_string(),
                    ))
                })?;
                let mut matched = None;
                let mut entry_position = 0usize;
                while entry_position < entries.len() {
                    before_source_entry()?;
                    let entry = entries.get(entry_position).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW vector-entry position changed".to_string(),
                        ))
                    })?;
                    entry_position = entry_position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW vector-entry position overflow".to_string(),
                        ))
                    })?;
                    if entry.row_id == row_id && entry.visible_at(snapshot) {
                        matched = Some(entry);
                        break;
                    }
                }
                let Some(entry) = matched else {
                    continue;
                };
                if let Some(candidate_ids) = candidates {
                    before_source_entry()?;
                    if candidate_ids.binary_search(&row_id.0).is_err() {
                        continue;
                    }
                }
                let score = if raw_score >= 1.0 {
                    1.0
                } else {
                    before_distance()?;
                    entry.vector.cosine_similarity(query)
                };
                let required = rows.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW visible-row length overflow".to_string(),
                    ))
                })?;
                row_bytes = grow_vector_copy(
                    &mut rows,
                    required,
                    None,
                    "HNSW visible rows",
                    &mut before_source_entry,
                    &mut before_retain,
                    &mut release_retained,
                )?;
                rows.push((row_id, score));
            }
            if supplement_missing {
                let mut entry_position = 0usize;
                while entry_position < entries.len() {
                    before_source_entry()?;
                    let entry = entries.get(entry_position).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW supplemented-entry position changed".to_string(),
                        ))
                    })?;
                    entry_position = entry_position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW supplemented-entry position overflow".to_string(),
                        ))
                    })?;
                    if !entry.visible_at(snapshot) {
                        continue;
                    }
                    before_source_entry()?;
                    if raw_row_ids.contains(&entry.row_id) {
                        continue;
                    }
                    if let Some(candidate_ids) = candidates {
                        before_source_entry()?;
                        if candidate_ids.binary_search(&entry.row_id.0).is_err() {
                            continue;
                        }
                    }
                    before_distance()?;
                    let score = entry.vector.cosine_similarity(query);
                    let required = rows.len().checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW supplemented-row length overflow".to_string(),
                        ))
                    })?;
                    row_bytes = grow_vector_copy(
                        &mut rows,
                        required,
                        None,
                        "HNSW supplemented rows",
                        &mut before_source_entry,
                        &mut before_retain,
                        &mut release_retained,
                    )?;
                    rows.push((entry.row_id, score));
                    supplemented_row_count =
                        supplemented_row_count.checked_add(1).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW supplement counter overflow".to_string(),
                            ))
                        })?;
                }
            }
            Ok(())
        });
        if let Err(error) = scored {
            release_retained(row_bytes);
            release_retained(raw_row_id_bytes);
            release_retained(raw_result_bytes);
            return Err(error);
        }
        sort_vector_scores(&mut rows);
        // The graph is an accelerator, not the answer. A live graph reaches
        // only part of itself, and points retired since it was built drop out
        // of what it returns, so it can hand back fewer usable neighbours than
        // the caller asked for. The eager search answers that by reading the
        // stored vectors exactly, and a caller here asked the same question of
        // the same committed state, so it is owed the same rows rather than a
        // quietly shorter answer. The exact continuation reads them: it charges
        // every entry it touches against this request's declared work and
        // memory ceilings, consults its cancellation at the same cadence, and
        // refuses with the typed document naming the ceiling it could not pay.
        // The condition is the eager search's own, so both agree on when the
        // exact read is required.
        if rows.len() < k && raw_candidate_count < hnsw_len && !graph_covering_request {
            release_retained(row_bytes);
            release_retained(raw_row_id_bytes);
            release_retained(raw_result_bytes);
            return Ok(None);
        }
        rows.truncate(k);
        let trace = match bounded_hnsw_trace(
            index,
            hnsw_len,
            &raw_candidates,
            &rows,
            supplemented_row_count,
            &mut before_source_entry,
            &mut before_retain,
            &mut release_retained,
        ) {
            Ok(trace) => trace,
            Err(error) => {
                release_retained(row_bytes);
                release_retained(raw_row_id_bytes);
                release_retained(raw_result_bytes);
                return Err(error);
            }
        };
        let trace_bytes = trace.retained_bytes();
        let retained_bytes = match checked_vector_add(row_bytes, trace_bytes, "HNSW result") {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(trace_bytes);
                release_retained(row_bytes);
                release_retained(raw_row_id_bytes);
                release_retained(raw_result_bytes);
                return Err(E::from(error));
            }
        };
        release_retained(raw_row_id_bytes);
        release_retained(raw_result_bytes);
        Ok(Some(BoundedHnswResult {
            rows,
            trace,
            retained_bytes,
        }))
    }

    fn brute_force_search_state(
        &self,
        _index: &VectorIndexRef,
        state: &crate::store::IndexState,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Vec<(RowId, f32)> {
        let mut scored: Vec<(RowId, f32)> = state.with_entries(|entries| {
            let mut scored = Vec::new();
            for entry in entries {
                if !entry.visible_at(snapshot) {
                    continue;
                }

                if let Some(cands) = candidates
                    && !cands.contains(entry.row_id.0)
                {
                    continue;
                }

                let sim = entry.vector.cosine_similarity(query);
                scored.push((entry.row_id, sim));
            }
            scored
        });

        sort_vector_scores(&mut scored);
        scored.truncate(k);
        scored
    }

    fn brute_force_search(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        self.store.with_registered_state(index, |state| {
            if query.len() != state.dimension() {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: index.clone(),
                    expected: state.dimension(),
                    actual: query.len(),
                });
            }

            Ok(self.brute_force_search_state(index, &state, query, k, candidates, snapshot))
        })?
    }

    fn build_hnsw_from_state(
        &self,
        index: &VectorIndexRef,
        state: &crate::store::IndexState,
    ) -> Option<HnswIndex> {
        let dim = state.dimension();
        let entry_count = state.vector_count();
        let final_bytes = estimate_hnsw_bytes(entry_count, dim, state.quantization());
        let reservation_bytes =
            estimate_hnsw_build_reservation(entry_count, dim, state.quantization());
        if self
            .accountant
            .try_allocate_for(
                reservation_bytes,
                "vector_index",
                &format!("build_hnsw@{}.{}", index.table, index.column),
                "Reduce vector volume or raise MEMORY_LIMIT so the HNSW index can be built.",
            )
            .is_err()
        {
            return None;
        }

        #[cfg(feature = "test-seams")]
        self.store
            .pause_registry()
            .maybe_pause(index, crate::test_seam::PauseWindow::Build);

        let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            state.with_entries(|entries| HnswIndex::new(entries, dim, state.quantization()))
        }))
        .ok();
        if built.is_none() {
            self.accountant.release(reservation_bytes);
        } else {
            self.accountant
                .release(reservation_bytes.saturating_sub(final_bytes));
            state.set_hnsw_bytes_with_accountant(final_bytes, self.accountant.clone());
        }
        built
    }

    pub fn search_with_strategy(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<(Vec<(RowId, f32)>, VectorSearchDebugTrace)> {
        self.store.with_bulk_read(|| {
            if k == 0 {
                return Ok((
                    Vec::new(),
                    VectorSearchDebugTrace::brute_force(&index, &[], "empty_limit", None),
                ));
            }
            enum SearchStep {
                Done(Vec<(RowId, f32)>, VectorSearchDebugTrace),
                BuildHnsw,
            }

            loop {
                let snapshot_tx = TxId::from_snapshot(snapshot);
                let step = self.store.with_registered_state(&index, |state| {
                    if query.len() != state.dimension() {
                        return Err(Error::VectorIndexDimensionMismatch {
                            index: index.clone(),
                            expected: state.dimension(),
                            actual: query.len(),
                        });
                    }
                    if state.entry_count() == 0 {
                        return Ok(SearchStep::Done(
                            Vec::new(),
                            VectorSearchDebugTrace::brute_force(&index, &[], "empty_index", None),
                        ));
                    }

                    if !query_has_positive_finite_norm(query) {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "non_positive_query_norm",
                            state.hnsw_len(),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }

                    if state.max_tx() > snapshot_tx {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "snapshot_has_newer_vectors",
                            state.hnsw_len(),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }
                    if state.vector_count() < HNSW_THRESHOLD {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "below_hnsw_threshold",
                            state.hnsw_len(),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }
                    if !matches!(state.quantization(), VectorQuantization::F32)
                        && state.vector_count() <= QUANTIZED_EXACT_SEARCH_LIMIT
                    {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "quantized_exact_search",
                            state.hnsw_len(),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }

                    let Some(lock) = state.hnsw().get() else {
                        return Ok(SearchStep::BuildHnsw);
                    };
                    let guard = lock.read();
                    let Some(hnsw) = guard.as_ref() else {
                        return Ok(SearchStep::BuildHnsw);
                    };
                    #[cfg(feature = "test-seams")]
                    self.store
                        .pause_registry()
                        .maybe_pause(&index, crate::test_seam::PauseWindow::Search);

                    let raw_candidates = hnsw.search(&index, query, k)?;
                    // Test-seam only: the same shortening the bounded kernel
                    // sees, so an armed index hands both routes the identical
                    // candidate list. Compiled out in production.
                    #[cfg(feature = "test-seams")]
                    let raw_candidates = {
                        let mut raw_candidates = raw_candidates;
                        self.store
                            .graph_candidate_caps()
                            .cap_graph_candidates(&index, &mut raw_candidates);
                        raw_candidates
                    };
                    let hnsw_len = hnsw.len();
                    let hnsw_candidate_row_ids = raw_candidates
                        .iter()
                        .map(|(row_id, _)| *row_id)
                        .collect::<Vec<_>>();
                    let raw_candidate_count = raw_candidates.len();
                    let graph_covering_request = crate::hnsw::hnsw_search_candidate_cap_for_count(
                        hnsw_len,
                        state.quantization(),
                        k,
                    ) >= hnsw_len;

                    if candidates.is_some()
                        && raw_candidate_count < hnsw_len
                        && !graph_covering_request
                    {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "candidate_filter_requires_exact_scan",
                            Some(hnsw_len),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }

                    let supplement_missing = graph_covering_request
                        || raw_candidate_count.saturating_add(64) >= hnsw_len;
                    let raw_row_ids = if supplement_missing {
                        raw_candidates
                            .iter()
                            .map(|(row_id, _)| *row_id)
                            .collect::<HashSet<_>>()
                    } else {
                        HashSet::new()
                    };
                    let (mut visible, supplemented_row_count) = state.with_entries(|entries| {
                        let mut visible = raw_candidates
                            .into_iter()
                            .filter_map(|(rid, raw_score)| {
                                entries
                                    .iter()
                                    .find(|entry| entry.row_id == rid && entry.visible_at(snapshot))
                                    .and_then(|entry| {
                                        if let Some(cands) = candidates
                                            && !cands.contains(entry.row_id.0)
                                        {
                                            return None;
                                        }
                                        let score = if raw_score >= 1.0 {
                                            // Exact-key hits preserve self-probe semantics for
                                            // quantized vectors; approximate neighbors are reranked
                                            // against the stored vector payload below.
                                            1.0
                                        } else {
                                            entry.vector.cosine_similarity(query)
                                        };
                                        Some((entry.row_id, score))
                                    })
                            })
                            .collect::<Vec<_>>();
                        let mut supplemented_row_count = 0usize;
                        if supplement_missing {
                            for entry in entries {
                                if raw_row_ids.contains(&entry.row_id)
                                    || !entry.visible_at(snapshot)
                                {
                                    continue;
                                }
                                if let Some(cands) = candidates
                                    && !cands.contains(entry.row_id.0)
                                {
                                    continue;
                                }
                                visible.push((entry.row_id, entry.vector.cosine_similarity(query)));
                                supplemented_row_count += 1;
                            }
                        }
                        (visible, supplemented_row_count)
                    });

                    sort_vector_scores(&mut visible);
                    if visible.len() < k
                        && raw_candidate_count < hnsw_len
                        && !graph_covering_request
                    {
                        let rows = self.brute_force_search_state(
                            &index, &state, query, k, candidates, snapshot,
                        );
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "hnsw_candidates_insufficient",
                            Some(hnsw_len),
                        );
                        return Ok(SearchStep::Done(rows, trace));
                    }
                    visible.truncate(k);
                    let trace = VectorSearchDebugTrace::hnsw(
                        &index,
                        hnsw_len,
                        hnsw_candidate_row_ids,
                        &visible,
                        supplemented_row_count,
                    );
                    Ok(SearchStep::Done(visible, trace))
                })??;

                match step {
                    SearchStep::Done(rows, trace) => return Ok((rows, trace)),
                    SearchStep::BuildHnsw => {
                        if self.ensure_hnsw_built(&index, snapshot)? {
                            continue;
                        }
                        let rows =
                            self.brute_force_search(&index, query, k, candidates, snapshot)?;
                        let trace = VectorSearchDebugTrace::brute_force(
                            &index,
                            &rows,
                            "hnsw_build_unavailable",
                            None,
                        );
                        return Ok((rows, trace));
                    }
                }
            }
        })
    }

    #[doc(hidden)]
    pub fn search_with_strategy_for_test(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<(Vec<(RowId, f32)>, VectorSearchDebugTrace)> {
        self.search_with_strategy(index, query, k, candidates, snapshot)
    }

    /// Build this index's approximate graph if this snapshot is entitled to
    /// one, and answer whether a graph is now there to search.
    ///
    /// This is the one place a graph is built. It takes the store's
    /// maintenance lock, re-reads the index state underneath it, applies the
    /// same freshness and size rules a search applies, and passes through the
    /// same build window a caller can pause. Every caller that needs a graph
    /// asks here, so none of them can build on terms another does not, and a
    /// paused build is one pause however the search reached it.
    ///
    /// `false` means the graph is not available for this snapshot -- the index
    /// has moved on past it, it holds too few vectors to be worth a graph, or
    /// the memory to build one was refused -- and the caller answers by
    /// scoring the entries directly.
    pub fn ensure_hnsw_built(&self, index: &VectorIndexRef, snapshot: SnapshotId) -> Result<bool> {
        enum BuildOutcome {
            Ready,
            Fallback,
        }
        let snapshot_tx = TxId::from_snapshot(snapshot);
        let outcome = self.store.with_index_maintenance(index, || {
            let Some(current) = self.store.try_state(index) else {
                return Err(Error::UnknownVectorIndex {
                    index: index.clone(),
                });
            };
            if current.max_tx() > snapshot_tx || current.vector_count() < HNSW_THRESHOLD {
                return Ok(BuildOutcome::Fallback);
            }
            let current_lock = current.hnsw().get_or_init(|| RwLock::new(None));
            let mut guard = current_lock.write();
            if guard.is_none() {
                let Some(hnsw) = self.build_hnsw_from_state(index, &current) else {
                    return Ok(BuildOutcome::Fallback);
                };
                *guard = Some(hnsw);
            }
            Ok(BuildOutcome::Ready)
        })?;
        Ok(matches!(outcome, BuildOutcome::Ready))
    }

    pub fn hnsw_eligible_without_build(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
    ) -> bool {
        self.store.with_bulk_read(|| {
            let Some(state) = self.store.try_state(index) else {
                return false;
            };
            if state.entry_count() == 0 {
                return false;
            }
            let snapshot_tx = TxId::from_snapshot(snapshot);
            if state.max_tx() > snapshot_tx {
                return false;
            }
            if state.vector_count() < HNSW_THRESHOLD {
                return false;
            }
            matches!(state.quantization(), VectorQuantization::F32)
                || state.vector_count() > QUANTIZED_EXACT_SEARCH_LIMIT
        })
    }

    pub fn hnsw_search_covers_all_without_build(&self, index: &VectorIndexRef, k: usize) -> bool {
        self.store.with_bulk_read(|| {
            let Some(state) = self.store.try_state(index) else {
                return false;
            };
            let graph_len = state.hnsw_len().unwrap_or_else(|| state.vector_count());
            crate::hnsw::hnsw_search_candidate_cap_for_count(graph_len, state.quantization(), k)
                >= graph_len
        })
    }
}

impl<S: WriteSetApplicator> VectorExecutor for MemVectorExecutor<S> {
    fn search(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        self.search_with_strategy(index, query, k, candidates, snapshot)
            .map(|(rows, _)| rows)
    }

    fn insert_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
    ) -> Result<()> {
        self.store.validate_vector(&index, vector.len())?;
        let entry = VectorEntry {
            index: index.clone(),
            row_id,
            vector,
            created_tx: tx,
            deleted_tx: None,
            lsn: contextdb_core::Lsn(0),
        };
        let existing_live = self
            .store
            .live_entry_for_row(&index, row_id, self.tx_mgr.snapshot())
            .is_some();

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.vector_inserts
                .retain(|pending| !(pending.index == index && pending.row_id == row_id));
            let mut moved_sources = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_moves.len() {
                let (move_index, old_row_id, new_row_id, _) = &ws.vector_moves[pos];
                if *move_index == index && *new_row_id == row_id {
                    moved_sources.push(*old_row_id);
                    ws.vector_moves.remove(pos);
                } else {
                    pos += 1;
                }
            }
            for old_row_id in moved_sources {
                if !ws
                    .vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == old_row_id
                    })
                {
                    ws.vector_deletes.push((index.clone(), old_row_id, tx));
                }
            }
            let already_deleted =
                ws.vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == row_id
                    });
            if existing_live && !already_deleted {
                ws.vector_deletes.push((index.clone(), row_id, tx));
            }
            ws.vector_inserts.push(entry);
        })?;

        Ok(())
    }

    fn delete_vector(&self, tx: TxId, index: VectorIndexRef, row_id: RowId) -> Result<()> {
        self.store.state(&index)?;
        let existing_live = self
            .store
            .live_entry_for_row(&index, row_id, self.tx_mgr.snapshot())
            .is_some();
        self.tx_mgr.with_write_set(tx, |ws| {
            let insert_count = ws.vector_inserts.len();
            ws.vector_inserts
                .retain(|entry| !(entry.index == index && entry.row_id == row_id));
            let canceled_insert = ws.vector_inserts.len() != insert_count;
            let mut moved_sources = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_moves.len() {
                let (move_index, old_row_id, new_row_id, _) = &ws.vector_moves[pos];
                if *move_index == index && *new_row_id == row_id {
                    moved_sources.push(*old_row_id);
                    ws.vector_moves.remove(pos);
                } else {
                    pos += 1;
                }
            }
            let pending_move_from_row =
                ws.vector_moves
                    .iter()
                    .any(|(move_index, old_row_id, _, _)| {
                        *move_index == index && *old_row_id == row_id
                    });
            let canceled_move_to_row = !moved_sources.is_empty();
            for old_row_id in moved_sources {
                if !ws
                    .vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == old_row_id
                    })
                {
                    ws.vector_deletes.push((index.clone(), old_row_id, tx));
                }
            }
            let already_deleted =
                ws.vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == row_id
                    });
            if !pending_move_from_row
                && ((!canceled_insert && !canceled_move_to_row) || existing_live)
                && !already_deleted
            {
                ws.vector_deletes.push((index, row_id, tx));
            }
        })?;

        Ok(())
    }
}

pub(crate) fn estimate_hnsw_bytes(
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

pub(crate) fn estimate_hnsw_build_reservation(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
) -> usize {
    let final_bytes = estimate_hnsw_bytes(entry_count, dimension, quantization);
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
