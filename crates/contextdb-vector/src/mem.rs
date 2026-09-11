use crate::hnsw::{HnswAllowedSearchIncomplete, HnswAllowedSearchLimits, HnswAllowedSearchStatus};
use crate::memory_budget::{MemoryBudget, unlimited_memory_budget};
use crate::{
    HnswIndex,
    store::{VectorGraphLayerAvailability, VectorStore},
};
use contextdb_core::read_contract::ReadFailure;
use contextdb_core::*;
use contextdb_tx::{TransactionManager, WriteSetApplicator};
use hnsw_rs::hnsw::HnswSearchScratchEvent;
use parking_lot::RwLock;
use roaring::RoaringTreemap;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

enum QuarantineAwarePreloadError<E> {
    Control(E),
    Store(Error),
}

impl<E> From<Error> for QuarantineAwarePreloadError<E> {
    fn from(error: Error) -> Self {
        Self::Store(error)
    }
}

thread_local! {
    // SQL has already bound and typed these prefixes. Retain that fact through
    // the vector call so a partition predicate never needs a table walk just
    // to rediscover the states it named.
    static PARTITION_SEARCH_SCOPES: RefCell<Vec<(VectorIndexRef, Vec<VectorPartitionKey>)>> = const { RefCell::new(Vec::new()) };
    static SEARCH_POLICY_LIMIT: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Carry the original SQL/Rust LIMIT while the vector layer returns a larger
/// rank candidate pool. Candidate expansion must not multiply default breadth
/// a second time. The dynamic scope has no allocation and restores on unwind.
#[doc(hidden)]
pub fn with_search_policy_limit<T>(limit: usize, operation: impl FnOnce() -> T) -> T {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            SEARCH_POLICY_LIMIT.with(|value| value.set(self.0));
        }
    }
    let _restore = Restore(SEARCH_POLICY_LIMIT.with(|value| value.replace(Some(limit))));
    operation()
}

fn search_policy_limit(k: usize) -> usize {
    SEARCH_POLICY_LIMIT.with(|value| value.get().unwrap_or(k))
}

fn partition_key_has_prefix(key: &VectorPartitionKey, prefix: &VectorPartitionKey) -> bool {
    key.components().starts_with(prefix.components())
}

/// Whether a staged row belongs to the current statement's selected key prefixes.
#[doc(hidden)]
pub fn partition_key_is_selected(index: &VectorIndexRef, key: &VectorPartitionKey) -> bool {
    PARTITION_SEARCH_SCOPES.with(|scopes| {
        scopes
            .borrow()
            .iter()
            .rev()
            .find(|(selected, _)| selected == index)
            .is_none_or(|(_, prefixes)| {
                prefixes
                    .iter()
                    .any(|prefix| partition_key_has_prefix(key, prefix))
            })
    })
}

/// Run one vector operation with finite, typed SQL partition prefixes.
pub fn with_selected_partition_prefixes<T>(
    index: &VectorIndexRef,
    prefixes: Vec<VectorPartitionKey>,
    operation: impl FnOnce() -> T,
) -> T {
    PARTITION_SEARCH_SCOPES.with(|scopes| scopes.borrow_mut().push((index.clone(), prefixes)));
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            PARTITION_SEARCH_SCOPES.with(|scopes| {
                scopes.borrow_mut().pop();
            });
        }
    }
    let _restore = Restore;
    operation()
}

/// Scores and their store charge have one owner, including while a caller ranks
/// or consumes them. Capacity is admitted before allocation and never grows
/// through an uncharged `Vec::push`.
#[doc(hidden)]
pub struct SearchScores {
    rows: Vec<(RowId, f32)>,
    credit: ScoreCredit,
    index: VectorIndexRef,
    exact_budget: bool,
}

struct ScoreCredit {
    budget: Arc<dyn MemoryBudget>,
    bytes: usize,
}

impl Drop for ScoreCredit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

/// Candidate row ids restricted to one partition. The allocation remains
/// charged while every graph layer in that partition reuses the slice.
struct SearchCandidateIds {
    rows: Vec<u64>,
    credit: ScoreCredit,
}

impl SearchCandidateIds {
    fn new(budget: Arc<dyn MemoryBudget>) -> Self {
        Self {
            rows: Vec::new(),
            credit: ScoreCredit { budget, bytes: 0 },
        }
    }

    fn try_push(&mut self, row_id: RowId) -> Result<()> {
        let wanted = self
            .rows
            .len()
            .checked_add(1)
            .ok_or_else(|| Error::Other("partition candidate capacity overflow".into()))?;
        if wanted > self.rows.capacity() {
            let requested_capacity = if self.rows.capacity() == 0 {
                wanted
            } else {
                self.rows
                    .capacity()
                    .checked_mul(2)
                    .ok_or_else(|| Error::Other("partition candidate capacity overflow".into()))?
                    .max(wanted)
            };
            let requested_bytes = requested_capacity
                .checked_mul(std::mem::size_of::<u64>())
                .ok_or_else(|| Error::Other("partition candidate capacity overflow".into()))?;
            let mut replacement = Self::new(self.credit.budget.clone());
            replacement.credit.budget.try_allocate_for(
                requested_bytes,
                "vector_search",
                "partition_candidates",
                "Narrow the authorized scope or raise MEMORY_LIMIT.",
            )?;
            replacement.credit.bytes = requested_bytes;
            replacement
                .rows
                .try_reserve_exact(requested_capacity)
                .map_err(|_| Error::Other("partition candidate allocation failed".into()))?;
            let actual_bytes = replacement
                .rows
                .capacity()
                .checked_mul(std::mem::size_of::<u64>())
                .ok_or_else(|| Error::Other("partition candidate capacity overflow".into()))?;
            if actual_bytes > requested_bytes {
                replacement.credit.budget.try_allocate_for(
                    actual_bytes - requested_bytes,
                    "vector_search",
                    "partition_candidates",
                    "Narrow the authorized scope or raise MEMORY_LIMIT.",
                )?;
                replacement.credit.bytes = actual_bytes;
            }
            replacement.rows.extend_from_slice(&self.rows);
            let old = std::mem::replace(self, replacement);
            drop(old);
        }
        self.rows.push(row_id.0);
        Ok(())
    }

    fn sort_unstable(&mut self) {
        self.rows.sort_unstable();
    }
}

impl std::ops::Deref for SearchCandidateIds {
    type Target = [u64];

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl SearchScores {
    pub fn new(index: VectorIndexRef, budget: Arc<dyn MemoryBudget>) -> Self {
        Self {
            rows: Vec::new(),
            credit: ScoreCredit { budget, bytes: 0 },
            index,
            exact_budget: false,
        }
    }

    pub fn for_exact(mut self) -> Self {
        self.exact_budget = true;
        self
    }

    pub fn reserve(&mut self, additional: usize) -> Result<()> {
        let wanted = self
            .rows
            .len()
            .checked_add(additional)
            .ok_or_else(|| Error::Other("vector score capacity overflow".into()))?;
        if wanted <= self.rows.capacity() {
            return Ok(());
        }
        let bytes = wanted
            .checked_mul(std::mem::size_of::<(RowId, f32)>())
            .ok_or_else(|| Error::Other("vector score capacity overflow".into()))?;
        let mut replacement = Self::new(self.index.clone(), self.credit.budget.clone());
        replacement.exact_budget = self.exact_budget;
        replacement.charge(bytes)?;
        replacement
            .rows
            .try_reserve_exact(wanted)
            .map_err(|_| Error::Other("vector score allocation failed".into()))?;
        let actual = replacement.rows.capacity() * std::mem::size_of::<(RowId, f32)>();
        if actual > replacement.credit.bytes {
            replacement.charge(actual - replacement.credit.bytes)?;
        }
        replacement.rows.extend_from_slice(&self.rows);
        let old = std::mem::replace(self, replacement);
        drop(old);
        Ok(())
    }

    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.credit
            .budget
            .try_allocate_for(
                bytes,
                "vector_search",
                if self.exact_budget {
                    "exact_scores"
                } else {
                    "merged_scores"
                },
                "Narrow the authorized scope or raise MEMORY_LIMIT.",
            )
            .map_err(|error| match error {
                Error::MemoryBudgetExceeded {
                    available_bytes, ..
                } if self.exact_budget => Error::VectorExactSearchBudgetExceeded {
                    index: self.index.clone(),
                    required_bytes: bytes as u64,
                    available_bytes: available_bytes as u64,
                },
                other => other,
            })?;
        self.credit.bytes += bytes;
        Ok(())
    }

    pub fn try_push(&mut self, row: (RowId, f32)) -> Result<()> {
        self.reserve(1)?;
        self.rows.push(row);
        Ok(())
    }

    pub fn try_extend(&mut self, rows: impl IntoIterator<Item = (RowId, f32)>) -> Result<()> {
        let rows = rows.into_iter();
        self.reserve(rows.size_hint().0)?;
        for row in rows {
            self.try_push(row)?;
        }
        Ok(())
    }

    pub fn truncate(&mut self, len: usize) {
        self.rows.truncate(len);
    }
    pub fn retain(&mut self, predicate: impl FnMut(&(RowId, f32)) -> bool) {
        self.rows.retain(predicate);
    }

    /// Transfer the result allocation out of engine ownership at a public API
    /// boundary. Internal consumers keep `SearchScores` until consumption ends.
    pub fn into_vec(self) -> Vec<(RowId, f32)> {
        self.rows
    }
}

impl std::ops::Deref for SearchScores {
    type Target = [(RowId, f32)];
    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}
impl std::ops::DerefMut for SearchScores {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rows
    }
}

#[doc(hidden)]
pub struct SearchScoreIter {
    rows: std::vec::IntoIter<(RowId, f32)>,
    _credit: ScoreCredit,
}
impl Iterator for SearchScoreIter {
    type Item = (RowId, f32);
    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}
impl IntoIterator for SearchScores {
    type Item = (RowId, f32);
    type IntoIter = SearchScoreIter;
    fn into_iter(self) -> Self::IntoIter {
        #[cfg(feature = "test-seams")]
        observe_exact_scores_for_test(
            ExactScorePhaseForTest::Consuming,
            self.rows.capacity(),
            self.rows.len(),
        );
        SearchScoreIter {
            rows: self.rows.into_iter(),
            _credit: self.credit,
        }
    }
}

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

#[allow(clippy::too_many_arguments)]
fn merge_bounded_vector_score<E>(
    scores: &mut Vec<(RowId, f32)>,
    retained_bytes: &mut usize,
    candidate: (RowId, f32),
    k: usize,
    before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    if k == 0 {
        return Ok(());
    }
    if scores.len() < k {
        let required_capacity = scores.len().checked_add(1).ok_or_else(|| {
            E::from(Error::Other(
                "bounded vector merge length overflow".to_string(),
            ))
        })?;
        grow_vector_copy(
            scores,
            required_capacity,
            Some(retained_bytes),
            "partitioned vector merge",
            before_source_entry,
            before_retain,
            release_retained,
        )?;
        push_bounded_score_heap(scores, candidate).map_err(E::from)?;
    } else if scores
        .first()
        .is_some_and(|worst| vector_score_quality(&candidate, worst) == std::cmp::Ordering::Greater)
    {
        replace_bounded_score_heap_root(scores, candidate).map_err(E::from)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn bounded_exact_merge_state<E>(
    state: &crate::store::IndexState,
    query: &[f32],
    k: usize,
    candidates: Option<&[u64]>,
    snapshot: SnapshotId,
    scores: &mut Vec<(RowId, f32)>,
    retained_bytes: &mut usize,
    before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
    before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<usize, E>
where
    E: From<Error>,
{
    before_source_entry()?;
    state.ensure_raw_vectors_loaded().map_err(E::from)?;
    let entry_count = state.entry_count();
    let mut compared = 0usize;
    for position in 0..entry_count {
        before_source_entry()?;
        let candidate = state.with_entries(|entries| -> std::result::Result<Option<_>, E> {
            let entry = entries.get(position).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded exact vector position changed".to_string(),
                ))
            })?;
            if !entry.visible_at(snapshot)
                || candidates
                    .is_some_and(|candidates| candidates.binary_search(&entry.row_id.0).is_err())
            {
                return Ok(None);
            }
            before_distance()?;
            Ok(Some((entry.row_id, entry.vector.cosine_similarity(query))))
        })?;
        let Some(candidate) = candidate else {
            continue;
        };
        compared = compared.checked_add(1).ok_or_else(|| {
            E::from(Error::Other(
                "bounded exact vector count overflow".to_string(),
            ))
        })?;
        merge_bounded_vector_score(
            scores,
            retained_bytes,
            candidate,
            k,
            before_source_entry,
            before_retain,
            release_retained,
        )?;
    }
    Ok(compared)
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
    /// Maximum per-partition EF_SEARCH used by this maintained-route query.
    /// `None` means the query did not traverse a graph.
    pub hnsw_ef_search: Option<usize>,
    /// Durable generation identities actually searched for this result. The
    /// mutable tail has no durable identity and is deliberately absent.
    pub selected_generations: Vec<crate::store::VectorGraphGeneration>,
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
            hnsw_ef_search: None,
            selected_generations: Vec::new(),
        }
    }

    fn hnsw(
        index: &VectorIndexRef,
        hnsw_len: usize,
        hnsw_candidate_row_ids: Vec<RowId>,
        rows: &[(RowId, f32)],
        supplemented_row_count: usize,
        hnsw_ef_search: usize,
        selected_generations: Vec<crate::store::VectorGraphGeneration>,
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
            hnsw_ef_search: Some(hnsw_ef_search),
            selected_generations,
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
    _index: VectorIndexRef,
    query: Vec<f32>,
    k: usize,
    candidates: Option<Vec<u64>>,
    snapshot: SnapshotId,
    snapshot_sources: Vec<BoundedSnapshotVectorSource>,
    source_position: usize,
    position: usize,
    state_end: Option<usize>,
    pending_score: Option<(RowId, f32)>,
    scored: Vec<(RowId, f32)>,
    output_position: usize,
    prepared: bool,
    retained_bytes: usize,
}

/// A store-owned entry source pinned when the cursor is opened. Exact stepping
/// uses the source handle directly; it never re-resolves "the active state for
/// this partition" after a suspension. The present store can expose active
/// retained states through this handle. Archived-generation sources must join
/// the same enumeration seam before generation reclamation is enabled.
#[derive(Clone)]
struct BoundedSnapshotVectorSource {
    partition_key: VectorPartitionKey,
    state: Arc<crate::store::IndexState>,
}

impl std::fmt::Debug for BoundedSnapshotVectorSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundedSnapshotVectorSource")
            .field("partition_key", &self.partition_key)
            .finish_non_exhaustive()
    }
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

/// Why a bounded graph search deliberately asks its caller to use the exact
/// continuation.  Only a caller whose already-resolved mode permits exact
/// work may follow this outcome; the vector layer never performs that work as
/// a hidden supplement to an indexed search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedHnswExactReason {
    ExactMode,
    AggregateBelowIndexedThreshold,
    QuantizedAggregateUsesExact,
}

/// Why no complete maintained graph route exists for this request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedHnswUnavailableReason {
    NonPositiveFiniteQueryNorm,
    SnapshotNotCovered,
    MaintainedGraphMissing,
}

/// Honest outcome of the bounded maintained-graph route.  In particular,
/// `Unavailable` and `Incomplete` carry no publishable rows: INDEXED must turn
/// them into its typed refusal, while AUTO may choose exact only because its
/// resolved mode already permits that choice.
///
/// `Complete` retains payload whose allocation was admitted through the
/// bounded-memory callbacks. Boxing it would add an unaccounted allocation on
/// that path, so this representation intentionally remains inline.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BoundedHnswOutcome {
    CompleteEmpty,
    Complete(BoundedHnswResult),
    ExactRequired {
        reason: BoundedHnswExactReason,
        aggregate_allowed_count: usize,
    },
    Unavailable {
        reason: BoundedHnswUnavailableReason,
    },
    Incomplete {
        reason: HnswAllowedSearchIncomplete,
    },
}

type BoundedHnswLayerSearchResult<E> = std::result::Result<
    (
        usize,
        Vec<(RowId, f32)>,
        usize,
        Option<HnswAllowedSearchIncomplete>,
    ),
    E,
>;

type HnswStateLayerSearchResult = Result<(usize, bool, SearchScores)>;

struct HnswStateRows {
    rows: SearchScores,
    hnsw_len: usize,
    candidate_row_ids: Vec<RowId>,
    selected_generations: Vec<crate::store::VectorGraphGeneration>,
}

enum HnswStateSearch {
    Ready(HnswStateRows),
    ExactRequired {
        reason: &'static str,
        filtered_route_missing: bool,
    },
}

struct HnswStateSearchRequest<'a> {
    index: &'a VectorIndexRef,
    query: &'a [f32],
    k: usize,
    candidates: Option<&'a RoaringTreemap>,
    partition_candidate_ids: Option<&'a [u64]>,
    allowed_count: usize,
    snapshot: SnapshotId,
    ef_search: usize,
    #[cfg(feature = "test-seams")]
    partition_key: &'a VectorPartitionKey,
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
        drop(replacement);
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} allocation failed"
        ))));
    }
    let actual =
        match checked_vector_mul(replacement.capacity(), std::mem::size_of::<T>(), operation) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(replacement);
                release_retained(requested);
                return Err(E::from(error));
            }
        };
    if actual < requested {
        drop(replacement);
        release_retained(requested);
        return Err(E::from(Error::Other(format!(
            "bounded vector {operation} capacity moved backwards"
        ))));
    }
    if actual > requested
        && let Err(error) = before_retain(actual - requested)
    {
        drop(replacement);
        release_retained(requested);
        return Err(error);
    }
    let previous = match checked_vector_mul(previous_capacity, std::mem::size_of::<T>(), operation)
    {
        Ok(bytes) => bytes,
        Err(error) => {
            drop(replacement);
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
            drop(replacement);
            release_retained(actual);
            return Err(error);
        }
    };
    let mut position = 0usize;
    while position < values.len() {
        if let Err(error) = before_work() {
            drop(replacement);
            release_retained(actual);
            return Err(error);
        }
        let value = match values.get(position) {
            Some(value) => *value,
            None => {
                drop(replacement);
                release_retained(actual);
                return Err(E::from(Error::Other(format!(
                    "bounded vector {operation} changed during migration"
                ))));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                drop(replacement);
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

#[allow(clippy::too_many_arguments)]
fn bounded_visit_partition_candidate_ids<E>(
    state: &crate::store::IndexState,
    candidate_ids: &[u64],
    snapshot: SnapshotId,
    before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
    mut visit: impl FnMut(RowId) -> std::result::Result<(), E>,
) -> std::result::Result<(), E> {
    if candidate_ids.len() <= state.entry_count() {
        for raw_row_id in candidate_ids {
            before_source_entry()?;
            let row_id = RowId(*raw_row_id);
            if state.directory_has_visible_row(row_id, snapshot) {
                visit(row_id)?;
            }
        }
        return Ok(());
    }
    state.bounded_visit_visible_ids(snapshot, &mut *before_source_entry, |row_id| {
        if candidate_ids.binary_search(&row_id.0).is_ok() {
            visit(row_id)?;
        }
        Ok(())
    })
}

#[allow(clippy::too_many_arguments)]
fn bounded_partition_candidate_ids<E>(
    state: &crate::store::IndexState,
    candidate_ids: &[u64],
    snapshot: SnapshotId,
    before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<u64>, usize), E>
where
    E: From<Error>,
{
    let mut count = 0usize;
    bounded_visit_partition_candidate_ids(
        state,
        candidate_ids,
        snapshot,
        before_source_entry,
        |_| {
            count = count.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded partition candidate count overflow".to_string(),
                ))
            })?;
            Ok(())
        },
    )?;
    let mut partition_ids = Vec::new();
    let mut retained_bytes = 0usize;
    if count != 0 {
        grow_vector_copy(
            &mut partition_ids,
            count,
            Some(&mut retained_bytes),
            "partition candidate intersection",
            before_source_entry,
            before_retain,
            release_retained,
        )?;
    }
    let populated = bounded_visit_partition_candidate_ids(
        state,
        candidate_ids,
        snapshot,
        before_source_entry,
        |row_id| {
            partition_ids.push(row_id.0);
            Ok(())
        },
    );
    if let Err(error) = populated {
        drop(partition_ids);
        release_retained(retained_bytes);
        return Err(error);
    }
    if partition_ids.len() != count {
        drop(partition_ids);
        release_retained(retained_bytes);
        return Err(E::from(Error::Other(
            "bounded partition candidate intersection changed".to_string(),
        )));
    }
    partition_ids.sort_unstable();
    Ok((partition_ids, retained_bytes))
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
    hnsw_ef_search: usize,
    selected_generations: Vec<crate::store::VectorGraphGeneration>,
    selected_generation_bytes: usize,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<BoundedVectorSearchTrace, E>
where
    E: From<Error>,
{
    let (owned_index, index_bytes) =
        match allocate_vector_index(index, before_retain, release_retained) {
            Ok(index) => index,
            Err(error) => {
                release_retained(selected_generation_bytes);
                return Err(error);
            }
        };
    let (candidate_row_ids, candidate_bytes) = match allocate_vector_row_ids(
        raw_candidates,
        "HNSW trace candidates",
        before_work,
        before_retain,
        release_retained,
    ) {
        Ok(ids) => ids,
        Err(error) => {
            release_retained(selected_generation_bytes);
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
            release_retained(selected_generation_bytes);
            release_retained(candidate_bytes);
            release_retained(index_bytes);
            return Err(error);
        }
    };
    let retained_bytes = match checked_vector_add(index_bytes, candidate_bytes, "HNSW trace")
        .and_then(|bytes| checked_vector_add(bytes, final_bytes, "HNSW trace"))
        .and_then(|bytes| checked_vector_add(bytes, selected_generation_bytes, "HNSW trace"))
    {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(selected_generation_bytes);
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
            hnsw_ef_search: Some(hnsw_ef_search),
            selected_generations,
        },
        retained_bytes,
    })
}

fn allocate_snapshot_source_slots<E>(
    capacity: usize,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
where
    E: From<Error>,
{
    if capacity == 0 {
        return Ok((Vec::new(), 0));
    }
    let requested = checked_vector_mul(
        capacity,
        std::mem::size_of::<BoundedSnapshotVectorSource>(),
        "snapshot vector sources",
    )
    .map_err(E::from)?;
    before_retain(requested)?;
    let mut keys = Vec::new();
    if keys.try_reserve_exact(capacity).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded snapshot vector-source allocation failed".to_string(),
        )));
    }
    let actual = match checked_vector_mul(
        keys.capacity(),
        std::mem::size_of::<BoundedSnapshotVectorSource>(),
        "snapshot vector sources",
    ) {
        Ok(actual) => actual,
        Err(error) => {
            release_retained(requested);
            return Err(E::from(error));
        }
    };
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded snapshot vector-source capacity moved backwards".to_string(),
        )));
    }
    let actual = reconcile_vector_allocation(requested, actual, before_retain, release_retained)?;
    Ok((keys, actual))
}

fn retain_snapshot_source<E>(
    sources: &mut Vec<BoundedSnapshotVectorSource>,
    key: VectorPartitionKey,
    state: Arc<crate::store::IndexState>,
    retained_bytes: &mut usize,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    let position = match sources.binary_search_by(|source| source.partition_key.cmp(&key)) {
        Ok(_) => return Ok(()),
        Err(position) => position,
    };
    if sources.len() == sources.capacity() {
        return Err(E::from(Error::Other(
            "bounded vector partition scope exceeds its declared maximum".to_string(),
        )));
    }
    let heap_bytes = key
        .estimated_bytes()
        .saturating_sub(std::mem::size_of::<VectorPartitionKey>());
    if heap_bytes != 0 {
        before_retain(heap_bytes)?;
    }
    if let Err(error) = before_work() {
        release_retained(heap_bytes);
        return Err(error);
    }
    let next_retained = match retained_bytes.checked_add(heap_bytes) {
        Some(retained) => retained,
        None => {
            release_retained(heap_bytes);
            return Err(E::from(Error::Other(
                "bounded vector partition-key retained-memory overflow".to_string(),
            )));
        }
    };
    sources.insert(
        position,
        BoundedSnapshotVectorSource {
            partition_key: key,
            state,
        },
    );
    *retained_bytes = next_retained;
    Ok(())
}

pub struct MemVectorExecutor<S: WriteSetApplicator> {
    store: Arc<VectorStore>,
    tx_mgr: Arc<TransactionManager<S>>,
    _accountant: Arc<dyn MemoryBudget>,
}

impl<S: WriteSetApplicator> MemVectorExecutor<S> {
    fn selected_partition_prefixes(
        &self,
        index: &VectorIndexRef,
    ) -> Option<Vec<VectorPartitionKey>> {
        PARTITION_SEARCH_SCOPES.with(|scopes| {
            scopes
                .borrow()
                .iter()
                .rev()
                .find_map(|(scoped_index, prefixes)| {
                    (scoped_index == index).then(|| prefixes.clone())
                })
        })
    }

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
            _accountant: accountant,
        }
    }

    fn all_bounded_snapshot_sources<E>(
        &self,
        index: &VectorIndexRef,
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
    where
        E: From<Error>,
    {
        self.store.with_partition_sources(index, |sources| {
            let (mut selected, mut retained_bytes) =
                allocate_snapshot_source_slots(sources.len(), before_retain, release_retained)?;
            let populated = sources.visit(|key, state| {
                before_source_entry()?;
                retain_snapshot_source(
                    &mut selected,
                    key.clone(),
                    state.clone(),
                    &mut retained_bytes,
                    before_source_entry,
                    before_retain,
                    release_retained,
                )
            });
            if let Err(error) = populated {
                drop(selected);
                release_retained(retained_bytes);
                return Err(error);
            }
            Ok((selected, retained_bytes))
        })
    }

    fn bounded_scoped_snapshot_sources<E>(
        &self,
        index: &VectorIndexRef,
        prefixes: &[VectorPartitionKey],
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
    where
        E: From<Error>,
    {
        self.store.with_partition_sources(index, |sources| {
            let mut selected_count = 0usize;
            sources.visit(|key, _| {
                before_source_entry()?;
                if prefixes
                    .iter()
                    .any(|prefix| partition_key_has_prefix(key, prefix))
                {
                    selected_count += 1;
                }
                Ok::<(), E>(())
            })?;
            let (mut selected, mut retained_bytes) =
                allocate_snapshot_source_slots(selected_count, before_retain, release_retained)?;
            let populated = sources.visit(|key, state| {
                before_source_entry()?;
                if prefixes
                    .iter()
                    .any(|prefix| partition_key_has_prefix(key, prefix))
                {
                    retain_snapshot_source(
                        &mut selected,
                        key.clone(),
                        state.clone(),
                        &mut retained_bytes,
                        before_source_entry,
                        before_retain,
                        release_retained,
                    )?;
                }
                Ok(())
            });
            if let Err(error) = populated {
                drop(selected);
                release_retained(retained_bytes);
                return Err(error);
            }
            Ok((selected, retained_bytes))
        })
    }

    /// Resolve the physical states a bounded query may need. Candidate row
    /// ids narrow the set only when current membership also proves that each
    /// candidate's version visible to this snapshot is in that same state.
    /// Any missing proof falls back to every retained state, while the
    /// candidate bitmap remains the row-level filter.
    #[allow(clippy::too_many_arguments)]
    fn bounded_snapshot_sources<E>(
        &self,
        index: &VectorIndexRef,
        layout: &crate::store::VectorIndexLayout,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
    where
        E: From<Error>,
    {
        if candidates.is_some_and(<[u64]>::is_empty) {
            return Ok((Vec::new(), 0));
        }
        if let Some(prefixes) = self.selected_partition_prefixes(index) {
            return self.bounded_scoped_snapshot_sources(
                index,
                &prefixes,
                before_source_entry,
                before_retain,
                release_retained,
            );
        }
        if !layout.is_partitioned() || candidates.is_none() {
            return self.all_bounded_snapshot_sources(
                index,
                before_source_entry,
                before_retain,
                release_retained,
            );
        }
        // One current source per candidate is the largest sound narrowed set.
        let capacity = candidates.map_or(0, <[u64]>::len);
        let (mut selected, mut retained_bytes) =
            allocate_snapshot_source_slots(capacity, before_retain, release_retained)?;
        let mut narrowing_is_sound = candidates.is_some();
        let resolved = (|| -> std::result::Result<(), E> {
            if let Some(candidate_ids) = candidates {
                let mut position = 0usize;
                while position < candidate_ids.len() {
                    before_source_entry()?;
                    let raw_row_id = *candidate_ids.get(position).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded vector candidate scope changed".to_string(),
                        ))
                    })?;
                    position = position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded vector candidate-scope position overflow".to_string(),
                        ))
                    })?;
                    let row_id = RowId(raw_row_id);
                    before_source_entry()?;
                    let Some(partition_key) = self.store.current_partition_for_row(index, row_id)
                    else {
                        narrowing_is_sound = false;
                        break;
                    };
                    before_source_entry()?;
                    let Some(state) = self.store.try_partition_state(index, &partition_key) else {
                        narrowing_is_sound = false;
                        break;
                    };
                    before_source_entry()?;
                    if !state.directory_has_visible_row(row_id, snapshot) {
                        narrowing_is_sound = false;
                        break;
                    }
                    retain_snapshot_source(
                        &mut selected,
                        partition_key,
                        state,
                        &mut retained_bytes,
                        before_source_entry,
                        before_retain,
                        release_retained,
                    )?;
                }
            }
            if narrowing_is_sound {
                return Ok(());
            }
            Ok(())
        })();
        if let Err(error) = resolved {
            drop(selected);
            release_retained(retained_bytes);
            return Err(error);
        }
        if narrowing_is_sound {
            return Ok((selected, retained_bytes));
        }
        drop(selected);
        release_retained(retained_bytes);
        self.all_bounded_snapshot_sources(
            index,
            before_source_entry,
            before_retain,
            release_retained,
        )
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
        let layout = self.store.index_layout(&index).map_err(E::from)?;
        if query.len() != layout.dimension {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index,
                expected: layout.dimension,
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
        let base_retained_bytes = match checked_vector_add(index_bytes, query_bytes, "query state")
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
        let (snapshot_sources, source_bytes) = if k == 0 {
            (Vec::new(), 0)
        } else {
            match self.bounded_snapshot_sources(
                &index,
                &layout,
                owned_candidates.as_deref(),
                snapshot,
                &mut before_source_entry,
                &mut before_retain,
                &mut release_retained,
            ) {
                Ok(scope) => scope,
                Err(error) => {
                    release_retained(base_retained_bytes);
                    return Err(error);
                }
            }
        };
        let mut retained_bytes = match checked_vector_add(
            base_retained_bytes,
            source_bytes,
            "partitioned exact cursor",
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(source_bytes);
                release_retained(base_retained_bytes);
                return Err(E::from(error));
            }
        };
        let mut scored = Vec::new();
        let prepare = (|| {
            let mut count = 0usize;
            if let Some(ids) = owned_candidates.as_ref() {
                for id in ids {
                    for source in &snapshot_sources {
                        before_source_entry()?;
                        if source.state.directory_has_visible_row(RowId(*id), snapshot) {
                            count = count.saturating_add(1);
                            break;
                        }
                    }
                }
            } else {
                for source in &snapshot_sources {
                    count =
                        count.saturating_add(source.state.bounded_directory_visible_entry_count(
                            snapshot,
                            None,
                            &mut before_source_entry,
                        )?);
                }
            }
            grow_vector_copy(
                &mut scored,
                count.min(k),
                Some(&mut retained_bytes),
                "exact scores",
                &mut before_source_entry,
                &mut before_retain,
                &mut release_retained,
            )
        })();
        if let Err(error) = prepare {
            drop(scored);
            drop(snapshot_sources);
            drop(owned_candidates);
            drop(owned_query);
            release_retained(retained_bytes);
            return Err(error);
        }
        Ok(BoundedBruteForceCursor {
            _index: index,
            query: owned_query,
            k,
            candidates: owned_candidates,
            snapshot,
            snapshot_sources,
            source_position: 0,
            position: 0,
            state_end: None,
            pending_score: None,
            scored,
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
            // Score the complete authorized set while retaining only the
            // requested best candidates. Ranked callers request their complete
            // formula pool; ordinary LIMIT reads need only their top-k heap.
            merge_bounded_vector_score(
                &mut cursor.scored,
                &mut cursor.retained_bytes,
                candidate,
                cursor.k,
                &mut before_source_entry,
                &mut before_retain,
                &mut release_retained,
            )?;
            cursor.pending_score = None;
            return Ok(BoundedVectorStep::Pending);
        }

        if let Some(candidate_ids) = cursor.candidates.as_ref() {
            let Some(raw_row_id) = candidate_ids.get(cursor.position).copied() else {
                sort_vector_scores(&mut cursor.scored);
                cursor.scored.truncate(cursor.k);
                cursor.prepared = true;
                return Ok(BoundedVectorStep::Pending);
            };
            let row_id = RowId(raw_row_id);
            for source in &cursor.snapshot_sources {
                if !source
                    .state
                    .directory_has_visible_row(row_id, cursor.snapshot)
                {
                    continue;
                }
                cursor.pending_score = source
                    .state
                    .bounded_score_visible_candidate(
                        &cursor._index,
                        row_id,
                        cursor.snapshot,
                        &cursor.query,
                        &mut before_source_entry,
                        &mut before_distance,
                        &mut before_retain,
                        &mut release_retained,
                    )?
                    .map(|score| (row_id, score));
                break;
            }
            cursor.position = cursor.position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded vector candidate position overflow".to_string(),
                ))
            })?;
            return Ok(BoundedVectorStep::Pending);
        }

        if cursor.source_position >= cursor.snapshot_sources.len() {
            sort_vector_scores(&mut cursor.scored);
            cursor.scored.truncate(cursor.k);
            cursor.prepared = true;
            return Ok(BoundedVectorStep::Pending);
        }
        if cursor.state_end.is_none() {
            before_source_entry()?;
            let Some(source) = cursor.snapshot_sources.get(cursor.source_position) else {
                return Err(E::from(Error::ReadFailure(
                    ReadFailure::invalid_continuation(
                        "the bounded vector partition position changed while suspended".to_string(),
                    ),
                )));
            };
            before_source_entry()?;
            source.state.ensure_raw_vectors_loaded().map_err(E::from)?;
            cursor.state_end = Some(source.state.entry_count());
        }
        if cursor.position >= cursor.state_end.unwrap_or_default() {
            cursor.source_position = cursor.source_position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded vector partition position overflow".to_string(),
                ))
            })?;
            cursor.position = 0;
            cursor.state_end = None;
            return Ok(BoundedVectorStep::Pending);
        }

        // Reading the pinned source handle and the source entry are distinct
        // boundaries, so a resumed cursor can be cancelled before it takes
        // the entry lock.
        before_source_entry()?;
        let position = cursor.position;
        let source = cursor
            .snapshot_sources
            .get(cursor.source_position)
            .ok_or_else(|| {
                E::from(Error::ReadFailure(ReadFailure::invalid_continuation(
                    "the bounded vector partition position changed while suspended".to_string(),
                )))
            })?;
        before_source_entry()?;
        let scored = source
            .state
            .with_entries(|entries| -> std::result::Result<Option<_>, E> {
                let entry = entries.get(position).ok_or_else(|| {
                    E::from(Error::ReadFailure(ReadFailure::invalid_continuation(
                        "the candidate this read left off at was removed while it was suspended"
                            .to_string(),
                    )))
                })?;
                if !entry.visible_at(cursor.snapshot)
                    || cursor.candidates.as_ref().is_some_and(|candidates| {
                        candidates.binary_search(&entry.row_id.0).is_err()
                    })
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

    /// Enumerate scoped membership without loading or scoring vector bodies.
    /// Every directory touch and retained source handle uses the caller's budget.
    pub fn bounded_visit_visible_ids<E>(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        mut before_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        mut visit: impl FnMut(RowId) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        before_entry()?;
        let layout = self.store.index_layout(index).map_err(E::from)?;
        let (sources, bytes) = self.bounded_snapshot_sources(
            index,
            &layout,
            None,
            snapshot,
            &mut before_entry,
            &mut before_retain,
            &mut release_retained,
        )?;
        let result = sources.iter().try_for_each(|source| {
            source
                .state
                .bounded_visit_visible_ids(snapshot, &mut before_entry, &mut visit)
        });
        drop(sources);
        release_retained(bytes);
        result
    }

    /// Count the rows admitted by the same candidate bitmap and conservative
    /// partition selection used by the bounded sources. AUTO calls this once
    /// for the whole request; it must not decide exact-versus-indexed from one
    /// partition at a time.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_authorized_visible_count<E>(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<usize, E>
    where
        E: From<Error>,
    {
        before_source_entry()?;
        let layout = self.store.index_layout(index).map_err(E::from)?;
        let (snapshot_sources, source_bytes) = self.bounded_snapshot_sources(
            index,
            &layout,
            candidates,
            snapshot,
            &mut before_source_entry,
            &mut before_retain,
            &mut release_retained,
        )?;
        let counted = (|| -> std::result::Result<usize, E> {
            if let Some(candidate_ids) = candidates {
                let mut count = 0usize;
                let mut candidate_position = 0usize;
                while candidate_position < candidate_ids.len() {
                    before_source_entry()?;
                    let row_id =
                        RowId(*candidate_ids.get(candidate_position).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded vector candidate count changed".to_string(),
                            ))
                        })?);
                    candidate_position = candidate_position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded vector candidate-count position overflow".to_string(),
                        ))
                    })?;
                    if snapshot_sources
                        .iter()
                        .any(|source| source.state.directory_has_visible_row(row_id, snapshot))
                    {
                        count = count.checked_add(1).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded vector aggregate allowed count overflow".to_string(),
                            ))
                        })?;
                    }
                }
                return Ok(count);
            }
            let mut count = 0usize;
            let mut partition_position = 0usize;
            while partition_position < snapshot_sources.len() {
                before_source_entry()?;
                let source = snapshot_sources.get(partition_position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded vector count partition position changed".to_string(),
                    ))
                })?;
                partition_position = partition_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded vector count partition position overflow".to_string(),
                    ))
                })?;
                before_source_entry()?;
                let state_count = source.state.bounded_directory_visible_entry_count(
                    snapshot,
                    candidates,
                    &mut before_source_entry,
                )?;
                count = count.checked_add(state_count).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded vector aggregate allowed count overflow".to_string(),
                    ))
                })?;
            }
            Ok(count)
        })();
        drop(snapshot_sources);
        release_retained(source_bytes);
        counted
    }

    /// Run each selected partition through its snapshot-compatible graph when
    /// available. AUTO exact-compares only a partition whose maintained route
    /// is unavailable or incomplete; INDEXED returns one all-or-nothing
    /// refusal. All retained graph and exact candidates share one global
    /// score/order/tie-break merge before publication.
    /// `aggregate_allowed_count` must be the result of
    /// [`Self::bounded_authorized_visible_count`] for this same index,
    /// candidate bitmap, and snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_hnsw_search<E>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        mode: VectorSearchMode,
        aggregate_allowed_count: usize,
        allowed_search_limits: HnswAllowedSearchLimits,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_distance: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedHnswOutcome, E>
    where
        E: From<Error>,
    {
        before_source_entry()?;
        let layout = self.store.index_layout(index).map_err(E::from)?;
        if query.len() != layout.dimension {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: layout.dimension,
                actual: query.len(),
            }));
        }
        if k == 0 || aggregate_allowed_count == 0 || candidates.is_some_and(<[u64]>::is_empty) {
            return Ok(BoundedHnswOutcome::CompleteEmpty);
        }
        if mode == VectorSearchMode::Exact {
            return Ok(BoundedHnswOutcome::ExactRequired {
                reason: BoundedHnswExactReason::ExactMode,
                aggregate_allowed_count,
            });
        }
        if mode == VectorSearchMode::Auto
            && aggregate_allowed_count < layout.effective_auto_index_at()
        {
            return Ok(BoundedHnswOutcome::ExactRequired {
                reason: if matches!(layout.quantization, VectorQuantization::F32) {
                    BoundedHnswExactReason::AggregateBelowIndexedThreshold
                } else {
                    BoundedHnswExactReason::QuantizedAggregateUsesExact
                },
                aggregate_allowed_count,
            });
        }
        if !query_has_positive_finite_norm(query) {
            return Ok(BoundedHnswOutcome::Unavailable {
                reason: BoundedHnswUnavailableReason::NonPositiveFiniteQueryNorm,
            });
        }

        let (snapshot_sources, source_bytes) = self.bounded_snapshot_sources(
            index,
            &layout,
            candidates,
            snapshot,
            &mut before_source_entry,
            &mut before_retain,
            &mut release_retained,
        )?;
        if snapshot_sources.is_empty() {
            release_retained(source_bytes);
            return Ok(BoundedHnswOutcome::CompleteEmpty);
        }

        let snapshot_tx = TxId::from_snapshot(snapshot);
        let mut merged = Some(Vec::<(RowId, f32)>::new());
        let mut merged_bytes = 0usize;
        let mut graph_trace_candidates = Some(Vec::<(RowId, f32)>::new());
        let mut graph_trace_candidate_bytes = 0usize;
        let mut selected_generations = Vec::<crate::store::VectorGraphGeneration>::new();
        let mut selected_generation_bytes = 0usize;
        let mut aggregate_hnsw_len = 0usize;
        let mut max_hnsw_ef_search = 0usize;
        let mut exact_candidate_count = 0usize;
        let mut partition_candidate_bytes = 0usize;
        let searched = (|| -> std::result::Result<BoundedHnswOutcome, E> {
            let mut partition_position = 0usize;
            while partition_position < snapshot_sources.len() {
                before_source_entry()?;
                let source = snapshot_sources.get(partition_position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW partition position changed".to_string(),
                    ))
                })?;
                partition_position = partition_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW partition position overflow".to_string(),
                    ))
                })?;
                let state = &source.state;
                let (partition_candidate_ids, local_candidate_bytes) =
                    if let Some(candidate_ids) = candidates {
                        bounded_partition_candidate_ids(
                            state,
                            candidate_ids,
                            snapshot,
                            &mut before_source_entry,
                            &mut before_retain,
                            &mut release_retained,
                        )?
                    } else {
                        (Vec::new(), 0)
                    };
                partition_candidate_bytes = match checked_vector_add(
                    partition_candidate_bytes,
                    local_candidate_bytes,
                    "partition candidate intersections",
                ) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        drop(partition_candidate_ids);
                        release_retained(local_candidate_bytes);
                        return Err(E::from(error));
                    }
                };
                let allowed_count = partition_candidate_ids.len();
                if candidates.is_some() && allowed_count == 0 {
                    continue;
                }

                // Snapshot compatibility and the unfiltered per-state count
                // use the shared visibility boundary index. A filtered route
                // materializes this partition's intersection once and reuses
                // it for every graph layer below.
                let eligibility =
                    state.bounded_hnsw_eligibility(snapshot_tx, &mut before_source_entry)?;
                let ef_search = layout
                    .resolve_policy(eligibility.live_count, search_policy_limit(k))
                    .hnsw_ef_search;
                max_hnsw_ef_search = max_hnsw_ef_search.max(ef_search);
                let allowed_count = if candidates.is_some() {
                    allowed_count
                } else {
                    eligibility.live_count
                };
                if eligibility.entry_count == 0 || allowed_count == 0 {
                    continue;
                }

                let required = k.min(allowed_count);
                self.store.reclaim_idle_graphs_for(
                    state.dormant_load_bytes_for_snapshot(snapshot),
                    Some(state),
                );
                let mut preload_checkpoint =
                    || before_source_entry().map_err(QuarantineAwarePreloadError::Control);
                let mut preload_retain =
                    |bytes| before_retain(bytes).map_err(QuarantineAwarePreloadError::Control);
                let preload = state.preload_snapshot_compatible_hnsw_layers_for_request(
                    snapshot,
                    &mut preload_checkpoint,
                    &mut preload_retain,
                    &mut release_retained,
                );
                match preload {
                    Ok(_) => {}
                    Err(QuarantineAwarePreloadError::Control(error)) => return Err(error),
                    Err(QuarantineAwarePreloadError::Store(error @ Error::ReadCancelled)) => {
                        return Err(E::from(error));
                    }
                    Err(QuarantineAwarePreloadError::Store(_))
                        if state.route_quarantine().is_some() => {}
                    Err(QuarantineAwarePreloadError::Store(error)) => {
                        return Err(E::from(error));
                    }
                }
                let (availability, layer_results) = state.with_snapshot_compatible_hnsw_layers(
                    snapshot,
                    false,
                    |hnsw| -> BoundedHnswLayerSearchResult<E> {
                        before_source_entry()?;
                        let hnsw_len = hnsw.len();
                        // An empty fresh tail contributes no candidates. It cannot
                        // make an otherwise complete sealed route unavailable.
                        if hnsw_len == 0 {
                            return Ok((0, Vec::new(), 0, None));
                        }
                        if let Some(candidate_ids) = candidates {
                            let result = hnsw.search_allowed_with_bounded_memory_at_ef(
                                index,
                                query,
                                k,
                                allowed_count,
                                allowed_search_limits,
                                ef_search,
                                || partition_candidate_ids.iter().copied(),
                                false,
                                |row_id| {
                                    candidate_ids.binary_search(&row_id.0).is_ok()
                                        && state.directory_has_visible_row(row_id, snapshot)
                                },
                                &mut before_source_entry,
                                &mut before_distance,
                                |event| apply_hnsw_scratch_acquire(event, &mut before_retain),
                                |event| apply_hnsw_scratch_release(event, &mut release_retained),
                            )?;
                            let incomplete = match result.status {
                                HnswAllowedSearchStatus::Complete => None,
                                HnswAllowedSearchStatus::Incomplete(reason) => Some(reason),
                            };
                            Ok((hnsw_len, result.rows, result.retained_bytes, incomplete))
                        } else {
                            let result = hnsw.search_allowed_with_bounded_memory_at_ef(
                                index,
                                query,
                                k,
                                allowed_count,
                                HnswAllowedSearchLimits {
                                    max_visited_nodes: usize::MAX,
                                    max_vector_evaluations: usize::MAX,
                                },
                                ef_search,
                                std::iter::empty,
                                true,
                                |row_id| state.directory_has_visible_row(row_id, snapshot),
                                &mut before_source_entry,
                                &mut before_distance,
                                |event| apply_hnsw_scratch_acquire(event, &mut before_retain),
                                |event| apply_hnsw_scratch_release(event, &mut release_retained),
                            )?;
                            Ok((hnsw_len, result.rows, result.retained_bytes, None))
                        }
                    },
                )?;
                if availability != VectorGraphLayerAvailability::Ready {
                    if mode == VectorSearchMode::Indexed {
                        return Ok(BoundedHnswOutcome::Unavailable {
                            reason: BoundedHnswUnavailableReason::MaintainedGraphMissing,
                        });
                    }
                    let scores = merged.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW merge state was already consumed".to_string(),
                        ))
                    })?;
                    exact_candidate_count =
                        exact_candidate_count.saturating_add(bounded_exact_merge_state(
                            state,
                            query,
                            k,
                            candidates,
                            snapshot,
                            scores,
                            &mut merged_bytes,
                            &mut before_source_entry,
                            &mut before_distance,
                            &mut before_retain,
                            &mut release_retained,
                        )?);
                    continue;
                }
                self.store.mark_sealed_graph_used(state);
                let mut raw_candidates = Vec::new();
                let mut raw_result_bytes = 0usize;
                // A complete chain has a base and at most one sealed change;
                // the optional third layer is a mutable tail with no identity.
                let mut layer_generations = [None, None];
                let mut layer_generation_count = 0usize;
                let mut state_hnsw_len = 0usize;
                let mut incomplete_reason = None;
                for (generation, (hnsw_len, layer_rows, layer_bytes, incomplete)) in layer_results {
                    state_hnsw_len = state_hnsw_len.saturating_add(hnsw_len);
                    if let Some(reason) = incomplete.filter(|reason| {
                        *reason != HnswAllowedSearchIncomplete::InsufficientAllowedCandidates
                    }) {
                        drop(layer_rows);
                        release_retained(layer_bytes);
                        incomplete_reason.get_or_insert(reason);
                        continue;
                    }
                    if incomplete_reason.is_some() {
                        drop(layer_rows);
                        release_retained(layer_bytes);
                        continue;
                    }
                    let contributes_new_candidate = layer_rows.iter().any(|(row_id, _)| {
                        !raw_candidates
                            .iter()
                            .any(|(existing_row_id, _)| existing_row_id == row_id)
                    });
                    if contributes_new_candidate && let Some(generation) = generation {
                        let Some(slot) = layer_generations.get_mut(layer_generation_count) else {
                            drop(layer_rows);
                            release_retained(layer_bytes);
                            return Err(E::from(Error::Other(
                                "snapshot-compatible vector chain has more than two sealed layers"
                                    .to_string(),
                            )));
                        };
                        *slot = Some(generation);
                        layer_generation_count += 1;
                    }
                    let needed = raw_candidates
                        .len()
                        .checked_add(layer_rows.len())
                        .ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW layer candidate length overflow".to_string(),
                            ))
                        })?;
                    grow_vector_copy(
                        &mut raw_candidates,
                        needed,
                        Some(&mut raw_result_bytes),
                        "partitioned HNSW layer merge",
                        &mut before_source_entry,
                        &mut before_retain,
                        &mut release_retained,
                    )?;
                    raw_candidates.extend(layer_rows);
                    release_retained(layer_bytes);
                }

                if let Some(reason) = incomplete_reason {
                    drop(raw_candidates);
                    release_retained(raw_result_bytes);
                    if mode == VectorSearchMode::Indexed {
                        return Ok(BoundedHnswOutcome::Incomplete { reason });
                    }
                    let scores = merged.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW merge state was already consumed".to_string(),
                        ))
                    })?;
                    exact_candidate_count =
                        exact_candidate_count.saturating_add(bounded_exact_merge_state(
                            state,
                            query,
                            k,
                            candidates,
                            snapshot,
                            scores,
                            &mut merged_bytes,
                            &mut before_source_entry,
                            &mut before_distance,
                            &mut before_retain,
                            &mut release_retained,
                        )?);
                    continue;
                }

                #[cfg(feature = "test-seams")]
                self.store.graph_candidate_caps().cap_graph_candidates(
                    index,
                    Some(&source.partition_key),
                    &mut raw_candidates,
                );
                let incomplete = (raw_candidates.len() < required)
                    .then_some(HnswAllowedSearchIncomplete::InsufficientAllowedCandidates);
                if let Some(reason) = incomplete {
                    drop(raw_candidates);
                    release_retained(raw_result_bytes);
                    if mode == VectorSearchMode::Indexed {
                        return Ok(BoundedHnswOutcome::Incomplete { reason });
                    }
                    let scores = merged.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW merge state was already consumed".to_string(),
                        ))
                    })?;
                    exact_candidate_count =
                        exact_candidate_count.saturating_add(bounded_exact_merge_state(
                            state,
                            query,
                            k,
                            candidates,
                            snapshot,
                            scores,
                            &mut merged_bytes,
                            &mut before_source_entry,
                            &mut before_distance,
                            &mut before_retain,
                            &mut release_retained,
                        )?);
                    continue;
                }

                let mut accepted = 0usize;
                for raw_position in 0..raw_candidates.len() {
                    before_source_entry()?;
                    let (row_id, _) = raw_candidates[raw_position];
                    before_source_entry()?;
                    if !state.directory_has_visible_row(row_id, snapshot)
                        || candidates
                            .is_some_and(|candidates| candidates.binary_search(&row_id.0).is_err())
                    {
                        continue;
                    }
                    // Layer identities can overlap. Score each logical row once,
                    // using its native snapshot-visible body and the original query.
                    if raw_candidates[..accepted]
                        .iter()
                        .any(|(id, _)| *id == row_id)
                    {
                        continue;
                    }
                    // Cleanup can leave one directory version while an immutable
                    // graph still contains an older point. Directory cardinality
                    // cannot certify that point's score. Resolve only this row's
                    // visible native body, with the caller's work and memory budget.
                    let Some(score) = state.bounded_score_visible_candidate(
                        index,
                        row_id,
                        snapshot,
                        query,
                        &mut before_source_entry,
                        &mut before_distance,
                        &mut before_retain,
                        &mut release_retained,
                    )?
                    else {
                        continue;
                    };
                    raw_candidates[accepted] = (row_id, score);
                    accepted = accepted.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW accepted-row count overflow".to_string(),
                        ))
                    })?;
                }
                raw_candidates.truncate(accepted);
                if accepted < required {
                    drop(raw_candidates);
                    release_retained(raw_result_bytes);
                    if mode == VectorSearchMode::Indexed {
                        return Ok(BoundedHnswOutcome::Incomplete {
                            reason: HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
                        });
                    }
                    let scores = merged.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW merge state was already consumed".to_string(),
                        ))
                    })?;
                    exact_candidate_count =
                        exact_candidate_count.saturating_add(bounded_exact_merge_state(
                            state,
                            query,
                            k,
                            candidates,
                            snapshot,
                            scores,
                            &mut merged_bytes,
                            &mut before_source_entry,
                            &mut before_distance,
                            &mut before_retain,
                            &mut release_retained,
                        )?);
                    continue;
                }
                aggregate_hnsw_len = aggregate_hnsw_len.saturating_add(state_hnsw_len);
                {
                    let trace_candidates = graph_trace_candidates.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW trace candidate state was already consumed".to_string(),
                        ))
                    })?;
                    let required_capacity = trace_candidates
                        .len()
                        .checked_add(raw_candidates.len())
                        .ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW trace candidate length overflow".to_string(),
                            ))
                        })?;
                    grow_vector_copy(
                        trace_candidates,
                        required_capacity,
                        Some(&mut graph_trace_candidate_bytes),
                        "HNSW trace graph candidates",
                        &mut before_source_entry,
                        &mut before_retain,
                        &mut release_retained,
                    )?;
                    trace_candidates.extend(raw_candidates.iter().copied());
                }
                {
                    let scores = merged.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW merge state was already consumed".to_string(),
                        ))
                    })?;
                    for candidate in raw_candidates.iter().copied() {
                        merge_bounded_vector_score(
                            scores,
                            &mut merged_bytes,
                            candidate,
                            k,
                            &mut before_source_entry,
                            &mut before_retain,
                            &mut release_retained,
                        )?;
                    }
                }
                drop(raw_candidates);
                release_retained(raw_result_bytes);
                for generation in layer_generations.into_iter().flatten() {
                    let required_capacity =
                        selected_generations.len().checked_add(1).ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded HNSW selected-generation length overflow".to_string(),
                            ))
                        })?;
                    grow_vector_copy(
                        &mut selected_generations,
                        required_capacity,
                        Some(&mut selected_generation_bytes),
                        "HNSW trace selected generations",
                        &mut before_source_entry,
                        &mut before_retain,
                        &mut release_retained,
                    )?;
                    selected_generations.push(generation);
                }
            }

            let Some(rows) = merged.as_mut() else {
                return Err(E::from(Error::Other(
                    "bounded HNSW merge state was consumed before publication".to_string(),
                )));
            };
            if rows.is_empty() {
                return Ok(BoundedHnswOutcome::CompleteEmpty);
            }
            sort_vector_scores(rows);
            let trace_generations = std::mem::take(&mut selected_generations);
            let trace_generation_bytes = std::mem::take(&mut selected_generation_bytes);
            let trace = bounded_hnsw_trace(
                index,
                aggregate_hnsw_len,
                graph_trace_candidates.as_deref().ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW trace candidates disappeared before publication".to_string(),
                    ))
                })?,
                rows,
                exact_candidate_count,
                max_hnsw_ef_search,
                trace_generations,
                trace_generation_bytes,
                &mut before_source_entry,
                &mut before_retain,
                &mut release_retained,
            )?;
            drop(graph_trace_candidates.take());
            release_retained(graph_trace_candidate_bytes);
            graph_trace_candidate_bytes = 0;
            let trace_bytes = trace.retained_bytes();
            let retained_bytes =
                match checked_vector_add(merged_bytes, trace_bytes, "partitioned HNSW result") {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        release_retained(trace_bytes);
                        return Err(E::from(error));
                    }
                };
            let rows = match merged.take() {
                Some(rows) => rows,
                None => {
                    release_retained(trace_bytes);
                    return Err(E::from(Error::Other(
                        "bounded HNSW merge state disappeared at publication".to_string(),
                    )));
                }
            };
            Ok(BoundedHnswOutcome::Complete(BoundedHnswResult {
                rows,
                trace,
                retained_bytes,
            }))
        })();
        release_retained(partition_candidate_bytes);
        release_retained(source_bytes);
        match searched {
            Ok(outcome @ BoundedHnswOutcome::Complete(_)) => Ok(outcome),
            Ok(outcome) => {
                drop(merged.take());
                release_retained(merged_bytes);
                drop(graph_trace_candidates.take());
                release_retained(graph_trace_candidate_bytes);
                release_retained(selected_generation_bytes);
                Ok(outcome)
            }
            Err(error) => {
                drop(merged.take());
                release_retained(merged_bytes);
                drop(graph_trace_candidates.take());
                release_retained(graph_trace_candidate_bytes);
                release_retained(selected_generation_bytes);
                Err(error)
            }
        }
    }

    fn all_partition_states(
        &self,
        index: &VectorIndexRef,
    ) -> Result<Vec<Arc<crate::store::IndexState>>> {
        let layout = self.store.index_layout(index)?;
        if !layout.is_partitioned() {
            return Ok(vec![self.store.state(index)?]);
        }
        // A partition state is allowed to disappear when its last retained
        // version is reclaimed. The column remains a valid vector index, so
        // absence of one enumerated state is an empty state, not an unknown
        // index. The enclosing bulk read keeps the returned Arc snapshots
        // stable for the rest of this search.
        Ok(self
            .store
            .partition_keys_including_historical(index)?
            .into_iter()
            .filter_map(|key| self.store.try_partition_state(index, &key))
            .collect())
    }

    fn all_partition_sources(
        &self,
        index: &VectorIndexRef,
    ) -> Result<Vec<BoundedSnapshotVectorSource>> {
        let layout = self.store.index_layout(index)?;
        if !layout.is_partitioned() {
            return Ok(vec![BoundedSnapshotVectorSource {
                partition_key: VectorPartitionKey::unpartitioned(),
                state: self.store.state(index)?,
            }]);
        }
        Ok(self
            .store
            .partition_keys_including_historical(index)?
            .into_iter()
            .filter_map(|partition_key| {
                self.store
                    .try_partition_state(index, &partition_key)
                    .map(|state| BoundedSnapshotVectorSource {
                        partition_key,
                        state,
                    })
            })
            .collect())
    }

    /// Narrow by current membership only when every candidate proves that the
    /// same state contains its visible version at this snapshot. A key move
    /// after an old snapshot fails that proof and deliberately selects every
    /// declared state rather than losing the retained source version.
    fn selected_partition_sources(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<BoundedSnapshotVectorSource>> {
        if candidates.is_some_and(|candidates| candidates.is_empty()) {
            return Ok(Vec::new());
        }
        let layout = self.store.index_layout(index)?;
        if !layout.is_partitioned() {
            return Ok(vec![BoundedSnapshotVectorSource {
                partition_key: VectorPartitionKey::unpartitioned(),
                state: self.store.state(index)?,
            }]);
        }
        if let Some(prefixes) = self.selected_partition_prefixes(index) {
            return Ok(self
                .store
                .partition_keys_including_historical(index)?
                .into_iter()
                .filter(|key| {
                    prefixes
                        .iter()
                        .any(|prefix| partition_key_has_prefix(key, prefix))
                })
                .filter_map(|key| {
                    self.store.try_partition_state(index, &key).map(|state| {
                        BoundedSnapshotVectorSource {
                            partition_key: key,
                            state,
                        }
                    })
                })
                .collect());
        }
        let Some(candidates) = candidates else {
            return Ok(self
                .store
                .partition_keys_including_historical(index)?
                .into_iter()
                .filter_map(|partition_key| {
                    self.store
                        .try_partition_state(index, &partition_key)
                        .map(|state| BoundedSnapshotVectorSource {
                            partition_key,
                            state,
                        })
                })
                .collect());
        };

        let mut selected = BTreeMap::<VectorPartitionKey, Arc<crate::store::IndexState>>::new();
        for raw_row_id in candidates.iter() {
            let row_id = RowId(raw_row_id);
            let Some(partition_key) = self.store.current_partition_for_row(index, row_id) else {
                return self.all_partition_sources(index);
            };
            let Some(state) = self.store.try_partition_state(index, &partition_key) else {
                // Membership can name a state whose final retained version
                // was reclaimed. It cannot safely narrow this snapshot, so
                // use every remaining state and keep the candidate bitmap as
                // the row-level authorization/filter boundary.
                return self.all_partition_sources(index);
            };
            if !state.directory_has_visible_row(row_id, snapshot) {
                return self.all_partition_sources(index);
            }
            selected.entry(partition_key).or_insert(state);
        }

        Ok(selected
            .into_iter()
            .map(|(partition_key, state)| BoundedSnapshotVectorSource {
                partition_key,
                state,
            })
            .collect())
    }

    fn brute_force_search_sources(
        &self,
        index: &VectorIndexRef,
        sources: &[BoundedSnapshotVectorSource],
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<SearchScores> {
        let mut scored = SearchScores::new(index.clone(), self._accountant.clone()).for_exact();
        // Directory metadata is snapshot-visible and already narrowed to the
        // selected partitions and authorized identities; unrelated data never
        // contributes to this admission.
        let capacity = k.min(Self::aggregate_authorized_source_count(
            sources, candidates, snapshot,
        ));
        scored.reserve(capacity)?;
        // Evaluate every eligible vector, retaining only the requested best
        // scores. Ranking callers request their complete candidate set here.
        let mut consider = |score: (RowId, f32)| -> Result<()> {
            if capacity == 0 {
                return Ok(());
            }
            if scored.len() < capacity {
                push_bounded_score_heap(&mut scored.rows, score)?;
            } else if vector_score_quality(&score, &scored[0]) == std::cmp::Ordering::Greater {
                replace_bounded_score_heap_root(&mut scored.rows, score)?;
            }
            Ok(())
        };
        if let Some(candidates) = candidates {
            for id in candidates.iter() {
                let row_id = RowId(id);
                for source in sources {
                    if !source.state.directory_has_visible_row(row_id, snapshot) {
                        continue;
                    }
                    if let Some(score) = source
                        .state
                        .score_visible_candidate(index, row_id, snapshot, query)?
                    {
                        consider((row_id, score))?;
                    }
                    break;
                }
            }
            sort_vector_scores(&mut scored);
            scored.truncate(k);
            return Ok(scored);
        }
        for source in sources {
            source.state.ensure_raw_vectors_loaded()?;
            source.state.with_entries(|entries| {
                for entry in entries {
                    if !entry.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(candidates) = candidates
                        && !candidates.contains(entry.row_id.0)
                    {
                        continue;
                    }
                    consider((entry.row_id, entry.vector.cosine_similarity(query)))?;
                }
                Ok::<(), Error>(())
            })?;
        }
        sort_vector_scores(&mut scored);
        scored.truncate(k);
        Ok(scored)
    }

    fn aggregate_authorized_source_count(
        sources: &[BoundedSnapshotVectorSource],
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> usize {
        if let Some(candidates) = candidates {
            return candidates
                .iter()
                .filter(|id| {
                    sources
                        .iter()
                        .any(|source| source.state.directory_has_visible_row(RowId(*id), snapshot))
                })
                .count();
        }
        sources.iter().fold(0usize, |count, source| {
            count.saturating_add(
                source
                    .state
                    .directory_visible_entry_count(snapshot, candidates),
            )
        })
    }

    /// Passive aggregate used by EXPLAIN's policy disclosure. It reads only
    /// the already-resident raw directories and honors the same authorization
    /// bitmap and selected partition prefixes as execution; it never loads a
    /// vector body or graph and never builds a route.
    pub fn authorized_visible_count_without_build(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<usize> {
        self.store.with_bulk_read(|| {
            let sources = self.selected_partition_sources(index, candidates, snapshot)?;
            Ok(Self::aggregate_authorized_source_count(
                &sources, candidates, snapshot,
            ))
        })
    }

    /// Resolve the global rank pool from the same selected partition policies
    /// as the graph walk, charging source selection to the active reader.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_rank_candidate_k<E>(
        &self,
        index: &VectorIndexRef,
        limit: usize,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        mut before_source: impl FnMut() -> std::result::Result<(), E>,
        mut acquire: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release: impl FnMut(usize),
    ) -> std::result::Result<usize, E>
    where
        E: From<Error>,
    {
        let layout = self.store.index_layout(index).map_err(E::from)?;
        let (sources, bytes) = self.bounded_snapshot_sources(
            index,
            &layout,
            candidates,
            snapshot,
            &mut before_source,
            &mut acquire,
            &mut release,
        )?;
        let result = (|| {
            let mut breadth = limit;
            let mut allowed = 0usize;
            for source in &sources {
                let count = source.state.bounded_directory_visible_entry_count(
                    snapshot,
                    candidates,
                    &mut before_source,
                )?;
                if count == 0 {
                    continue;
                }
                allowed = allowed.saturating_add(count);
                breadth = breadth.max(
                    layout
                        .resolve_policy(
                            source.state.directory_visible_entry_count(snapshot, None),
                            limit,
                        )
                        .hnsw_ef_search,
                );
            }
            Ok(breadth.min(allowed).max(limit))
        })();
        drop(sources);
        release(bytes);
        result
    }

    fn search_hnsw_state(
        &self,
        state: &crate::store::IndexState,
        request: HnswStateSearchRequest<'_>,
    ) -> Result<HnswStateSearch> {
        let HnswStateSearchRequest {
            index,
            query,
            k,
            candidates,
            partition_candidate_ids,
            allowed_count,
            snapshot,
            ef_search,
            #[cfg(feature = "test-seams")]
            partition_key,
        } = request;
        #[cfg(feature = "test-seams")]
        self.store
            .pause_registry()
            .maybe_pause(index, crate::test_seam::PauseWindow::Search);

        let layers = state.with_snapshot_compatible_hnsw_layers(
            snapshot,
            true,
            |hnsw| -> HnswStateLayerSearchResult {
                let hnsw_len = hnsw.len();
                if hnsw_len == 0 {
                    return Ok((
                        0,
                        true,
                        SearchScores::new(index.clone(), self._accountant.clone()),
                    ));
                }
                let credit = RefCell::new(ScoreCredit {
                    budget: self._accountant.clone(),
                    bytes: 0,
                });
                let searched = hnsw.search_allowed_at_ef(
                    index,
                    query,
                    k,
                    allowed_count,
                    ef_search,
                    || {
                        partition_candidate_ids
                            .into_iter()
                            .flat_map(|ids| ids.iter().copied())
                    },
                    candidates.is_none(),
                    |row_id| {
                        candidates.is_none_or(|ids| ids.contains(row_id.0))
                            && state.directory_has_visible_row(row_id, snapshot)
                    },
                    |event| {
                        apply_hnsw_scratch_acquire(event, &mut |bytes| {
                            let mut credit = credit.borrow_mut();
                            credit.budget.try_allocate_for(
                                bytes,
                                "vector_search",
                                "graph_scratch",
                                "Reduce EF_SEARCH or query scope, or raise MEMORY_LIMIT.",
                            )?;
                            credit.bytes += bytes;
                            Ok::<_, Error>(())
                        })
                    },
                    |event| {
                        apply_hnsw_scratch_release(event, &mut |bytes| {
                            let mut credit = credit.borrow_mut();
                            credit.budget.release(bytes);
                            credit.bytes -= bytes;
                        })
                    },
                )?;
                let complete = matches!(
                    searched.status,
                    HnswAllowedSearchStatus::Complete
                        | HnswAllowedSearchStatus::Incomplete(
                            HnswAllowedSearchIncomplete::InsufficientAllowedCandidates
                        )
                );
                let rows = SearchScores {
                    rows: searched.rows,
                    credit: credit.into_inner(),
                    index: index.clone(),
                    exact_budget: false,
                };
                Ok((hnsw_len, complete, rows))
            },
        );
        let (availability, searched_layers) = match layers {
            Ok(layers) => layers,
            Err(error @ Error::ReadCancelled) => return Err(error),
            Err(_error) if state.route_quarantine().is_some() => {
                return Ok(HnswStateSearch::ExactRequired {
                    reason: state
                        .route_quarantine()
                        .map_or("maintained_hnsw_unavailable", |reason| reason.as_str()),
                    filtered_route_missing: false,
                });
            }
            Err(error) => return Err(error),
        };
        if availability != VectorGraphLayerAvailability::Ready {
            return Ok(HnswStateSearch::ExactRequired {
                reason: if availability == VectorGraphLayerAvailability::Dormant {
                    "maintained_hnsw_dormant"
                } else {
                    "maintained_hnsw_unavailable"
                },
                filtered_route_missing: false,
            });
        }
        self.store.mark_sealed_graph_used(state);
        let mut hnsw_len = 0usize;
        let mut allowed_traversal_complete = true;
        let mut raw_candidates = SearchScores::new(index.clone(), self._accountant.clone());
        let mut selected_generations = Vec::new();
        for (generation, (layer_len, layer_complete, rows)) in searched_layers {
            let contributes_new_candidate = rows.iter().any(|(row_id, _)| {
                !raw_candidates
                    .iter()
                    .any(|(existing_row_id, _)| existing_row_id == row_id)
            });
            if contributes_new_candidate && let Some(generation) = generation {
                selected_generations.push(generation);
            }
            hnsw_len = hnsw_len.saturating_add(layer_len);
            allowed_traversal_complete &= layer_complete;
            raw_candidates.try_extend(rows)?;
        }
        #[cfg(feature = "test-seams")]
        let raw_candidates = {
            let mut raw_candidates = raw_candidates;
            self.store.graph_candidate_caps().cap_graph_candidates(
                index,
                Some(partition_key),
                &mut raw_candidates.rows,
            );
            raw_candidates
        };
        if candidates.is_some() && !allowed_traversal_complete {
            return Ok(HnswStateSearch::ExactRequired {
                reason: "bounded_allowed_candidates_incomplete",
                filtered_route_missing: true,
            });
        }

        let candidate_row_ids = raw_candidates
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect::<Vec<_>>();
        let mut visible = SearchScores::new(index.clone(), self._accountant.clone());
        visible.reserve(raw_candidates.len())?;
        for (row_id, _) in raw_candidates {
            if let Some(candidates) = candidates
                && !candidates.contains(row_id.0)
            {
                continue;
            }
            if !state.directory_has_visible_row(row_id, snapshot) {
                continue;
            }
            if visible.iter().any(|(id, _)| *id == row_id) {
                continue;
            }
            // An immutable graph may still name a pruned version. Publish the
            // snapshot-visible native score once per logical candidate, regardless
            // of how many versions cleanup left in its directory.
            let Some(score) = state.score_visible_candidate(index, row_id, snapshot, query)? else {
                continue;
            };
            visible.try_push((row_id, score))?;
        }
        sort_vector_scores(&mut visible);
        if visible.len() < k.min(allowed_count) && candidates.is_some() {
            return Ok(HnswStateSearch::ExactRequired {
                reason: "bounded_allowed_candidates_incomplete",
                filtered_route_missing: true,
            });
        }
        visible.truncate(k);
        Ok(HnswStateSearch::Ready(HnswStateRows {
            rows: visible,
            hnsw_len,
            candidate_row_ids,
            selected_generations,
        }))
    }

    pub fn search_with_mode(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: contextdb_core::VectorSearchMode,
    ) -> Result<(Vec<(RowId, f32)>, VectorSearchDebugTrace)> {
        self.search_with_mode_owned(index, query, k, candidates, snapshot, mode)
            .map(|(rows, trace)| (rows.into_vec(), trace))
    }

    pub fn search_with_mode_owned(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: contextdb_core::VectorSearchMode,
    ) -> Result<(SearchScores, VectorSearchDebugTrace)> {
        self.store.with_bulk_read(|| {
            if k == 0 {
                return Ok((
                    SearchScores::new(index.clone(), self._accountant.clone()),
                    VectorSearchDebugTrace::brute_force(&index, &[], "empty_limit", None),
                ));
            }
            let layout = self.store.index_layout(&index)?;
            if query.len() != layout.dimension {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: index.clone(),
                    expected: layout.dimension,
                    actual: query.len(),
                });
            }
            if candidates.is_some_and(|candidates| candidates.is_empty()) {
                return Ok((
                    SearchScores::new(index.clone(), self._accountant.clone()),
                    VectorSearchDebugTrace::brute_force(&index, &[], "empty_candidates", None),
                ));
            }
            let sources = self.selected_partition_sources(&index, candidates, snapshot)?;
            if sources.is_empty() {
                return Ok((
                    SearchScores::new(index.clone(), self._accountant.clone()),
                    VectorSearchDebugTrace::brute_force(&index, &[], "empty_index", None),
                ));
            }
            let exact = |reason: &'static str,
                         hnsw_len: Option<usize>|
             -> Result<(SearchScores, VectorSearchDebugTrace)> {
                let rows = self
                    .brute_force_search_sources(&index, &sources, query, k, candidates, snapshot)?;
                let trace = VectorSearchDebugTrace::brute_force(&index, &rows, reason, hnsw_len);
                #[cfg(feature = "test-seams")]
                observe_exact_scores_for_test(
                    ExactScorePhaseForTest::Scored,
                    rows.rows.capacity(),
                    rows.rows.len(),
                );
                Ok((rows, trace))
            };
            if mode == contextdb_core::VectorSearchMode::Exact {
                return exact("exact_requested", None);
            }
            if !query_has_positive_finite_norm(query) {
                if mode == contextdb_core::VectorSearchMode::Indexed {
                    return Err(Error::VectorIndexedRouteUnavailable {
                        index: index.clone(),
                    });
                }
                return exact("non_positive_query_norm", None);
            }

            if mode == contextdb_core::VectorSearchMode::Auto {
                let authorized_count =
                    Self::aggregate_authorized_source_count(&sources, candidates, snapshot);
                if authorized_count < layout.effective_auto_index_at() {
                    return exact(
                        if matches!(layout.quantization, VectorQuantization::F32) {
                            "below_hnsw_threshold"
                        } else {
                            "quantized_exact_search"
                        },
                        None,
                    );
                }
            }

            let mut merged = SearchScores::new(index.clone(), self._accountant.clone());
            let mut candidate_row_ids = Vec::<RowId>::new();
            let mut selected_generations = Vec::new();
            let mut aggregate_hnsw_len = 0usize;
            let mut max_hnsw_ef_search = 0usize;
            let mut exact_candidate_count = 0usize;
            let mut first_fallback_reason = None;
            let mut searched_nonempty_partition = false;
            for source in &sources {
                let visible_partition_count =
                    source.state.directory_visible_entry_count(snapshot, None);
                let mut partition_candidate_ids =
                    candidates.map(|_| SearchCandidateIds::new(self._accountant.clone()));
                if let (Some(candidate_ids), Some(partition_ids)) =
                    (candidates, partition_candidate_ids.as_mut())
                {
                    if usize::try_from(candidate_ids.len()).unwrap_or(usize::MAX)
                        <= source.state.entry_count()
                    {
                        for raw_row_id in candidate_ids.iter() {
                            let row_id = RowId(raw_row_id);
                            if source.state.directory_has_visible_row(row_id, snapshot) {
                                partition_ids.try_push(row_id)?;
                            }
                        }
                    } else {
                        source.state.bounded_visit_visible_ids(
                            snapshot,
                            || Ok::<_, Error>(()),
                            |row_id| {
                                if candidate_ids.contains(row_id.0) {
                                    partition_ids.try_push(row_id)?;
                                }
                                Ok(())
                            },
                        )?;
                    }
                    partition_ids.sort_unstable();
                }
                let allowed_count = partition_candidate_ids
                    .as_deref()
                    .map_or(visible_partition_count, <[u64]>::len);
                if allowed_count == 0 {
                    continue;
                }
                searched_nonempty_partition = true;
                let ef_search = layout
                    .resolve_policy(visible_partition_count, search_policy_limit(k))
                    .hnsw_ef_search;
                max_hnsw_ef_search = max_hnsw_ef_search.max(ef_search);
                self.store.reclaim_idle_graphs_for(
                    source.state.dormant_load_bytes_for_snapshot(snapshot),
                    Some(&source.state),
                );
                match self.search_hnsw_state(
                    &source.state,
                    HnswStateSearchRequest {
                        index: &index,
                        query,
                        k,
                        candidates,
                        partition_candidate_ids: partition_candidate_ids.as_deref(),
                        allowed_count,
                        snapshot,
                        ef_search,
                        #[cfg(feature = "test-seams")]
                        partition_key: &source.partition_key,
                    },
                )? {
                    HnswStateSearch::Ready(result) => {
                        aggregate_hnsw_len = aggregate_hnsw_len.saturating_add(result.hnsw_len);
                        candidate_row_ids.extend(result.candidate_row_ids);
                        selected_generations.extend(result.selected_generations);
                        merged.try_extend(result.rows)?;
                    }
                    HnswStateSearch::ExactRequired {
                        reason,
                        filtered_route_missing,
                    } => {
                        if mode == contextdb_core::VectorSearchMode::Indexed {
                            return Err(if filtered_route_missing {
                                Error::VectorFilteredRouteUnavailable {
                                    predicate_columns: Vec::new(),
                                    index: index.clone(),
                                }
                            } else {
                                Error::VectorIndexedRouteUnavailable {
                                    index: index.clone(),
                                }
                            });
                        }
                        let exact_rows = self.brute_force_search_sources(
                            &index,
                            std::slice::from_ref(source),
                            query,
                            k,
                            candidates,
                            snapshot,
                        )?;
                        exact_candidate_count =
                            exact_candidate_count.saturating_add(exact_rows.len());
                        first_fallback_reason.get_or_insert(reason);
                        merged.try_extend(exact_rows)?;
                    }
                }
            }
            if !searched_nonempty_partition {
                return Ok((
                    SearchScores::new(index.clone(), self._accountant.clone()),
                    VectorSearchDebugTrace::brute_force(&index, &[], "empty_index", None),
                ));
            }
            sort_vector_scores(&mut merged);
            merged.truncate(k);
            if aggregate_hnsw_len == 0 {
                let trace = VectorSearchDebugTrace::brute_force(
                    &index,
                    &merged,
                    first_fallback_reason.unwrap_or("maintained_hnsw_unavailable"),
                    None,
                );
                return Ok((merged, trace));
            }
            let trace = VectorSearchDebugTrace::hnsw(
                &index,
                aggregate_hnsw_len,
                candidate_row_ids,
                &merged,
                exact_candidate_count,
                max_hnsw_ef_search,
                selected_generations,
            );
            Ok((merged, trace))
        })
    }

    /// Compatibility route for existing lower-level callers. Their published
    /// behavior is the column-default automatic ladder; engine query surfaces
    /// that resolve an explicit mode call [`Self::search_with_mode`] instead.
    pub fn search_with_strategy(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<(Vec<(RowId, f32)>, VectorSearchDebugTrace)> {
        self.search_with_mode(
            index,
            query,
            k,
            candidates,
            snapshot,
            contextdb_core::VectorSearchMode::Auto,
        )
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

    /// Compatibility availability probe. Despite its retained name, this is
    /// now an observer: only explicit store maintenance may build a graph.
    pub fn ensure_hnsw_built(&self, index: &VectorIndexRef, snapshot: SnapshotId) -> Result<bool> {
        self.store.with_bulk_read(|| {
            let states = self.all_partition_states(index)?;
            let snapshot_tx = TxId::from_snapshot(snapshot);
            for state in states.iter().filter(|state| state.vector_count() != 0) {
                if state.max_tx() > snapshot_tx || !state.has_complete_hnsw_route() {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }

    pub fn hnsw_eligible_without_build(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> bool {
        self.store.with_bulk_read(|| {
            let Ok(sources) = self.selected_partition_sources(index, candidates, snapshot) else {
                return false;
            };
            let aggregate_count =
                Self::aggregate_authorized_source_count(&sources, candidates, snapshot);
            if aggregate_count == 0 {
                return false;
            }
            sources
                .iter()
                .filter(|source| {
                    source
                        .state
                        .directory_visible_entry_count(snapshot, candidates)
                        != 0
                })
                .all(|source| source.state.has_complete_hnsw_route())
        })
    }

    /// Report route-layer structure without loading any dormant graph. The
    /// caller supplies the already-authorized candidate set and may install a
    /// partition-prefix scope with `with_selected_partition_prefixes`, exactly
    /// as execution does.
    pub fn graph_layer_presence_without_load(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<(bool, bool, bool)> {
        self.store.with_bulk_read(|| {
            let sources = self.selected_partition_sources(index, candidates, snapshot)?;
            let mut base = false;
            let mut change = false;
            let mut tail = false;
            for source in sources {
                if source
                    .state
                    .directory_visible_entry_count(snapshot, candidates)
                    == 0
                {
                    continue;
                }
                let status = source.state.graph_generation_status();
                let sealed_base = status.base.is_some() || status.dormant_base.is_some();
                base |= sealed_base
                    || (source.state.has_complete_hnsw_route() && status.fresh_tail_entries != 0);
                change |= status.change.is_some() || status.dormant_change.is_some();
                tail |= sealed_base && status.fresh_tail_entries != 0;
            }
            Ok((base, change, tail))
        })
    }

    /// Runtime twin of [`Self::graph_layer_presence_without_load`] for the
    /// pull kernel's already-sorted authorized candidate list. Reusing that
    /// list keeps route disclosure from rescanning the relational table just
    /// to reconstruct authorization the query has already applied.
    pub fn graph_layer_presence_without_load_for_sorted_ids(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
    ) -> Result<(bool, bool, bool)> {
        self.store.with_bulk_read(|| {
            let sources = self.selected_partition_sources(index, None, snapshot)?;
            let mut base = false;
            let mut change = false;
            let mut tail = false;
            for source in sources {
                if source.state.bounded_directory_visible_entry_count(
                    snapshot,
                    candidates,
                    || Ok::<(), Error>(()),
                )? == 0
                {
                    continue;
                }
                let status = source.state.graph_generation_status();
                let sealed_base = status.base.is_some() || status.dormant_base.is_some();
                base |= sealed_base
                    || (source.state.has_complete_hnsw_route() && status.fresh_tail_entries != 0);
                change |= status.change.is_some() || status.dormant_change.is_some();
                tail |= sealed_base && status.fresh_tail_entries != 0;
            }
            Ok((base, change, tail))
        })
    }

    pub fn hnsw_search_covers_all_without_build(
        &self,
        index: &VectorIndexRef,
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> bool {
        self.store.with_bulk_read(|| {
            let Ok(layout) = self.store.index_layout(index) else {
                return false;
            };
            let Ok(sources) = self.selected_partition_sources(index, candidates, snapshot) else {
                return false;
            };
            let nonempty = sources
                .iter()
                .filter(|source| {
                    source
                        .state
                        .directory_visible_entry_count(snapshot, candidates)
                        != 0
                })
                .collect::<Vec<_>>();
            !nonempty.is_empty()
                && nonempty.into_iter().all(|source| {
                    let visible_partition_count =
                        source.state.directory_visible_entry_count(snapshot, None);
                    let allowed_count = source
                        .state
                        .directory_visible_entry_count(snapshot, candidates);
                    let ef_search = layout
                        .resolve_policy(visible_partition_count, search_policy_limit(k))
                        .hnsw_ef_search;
                    source.state.has_complete_hnsw_route()
                        && crate::hnsw::hnsw_search_candidate_cap_for_ef(ef_search, k)
                            >= allowed_count
                })
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

#[cfg(feature = "test-seams")]
pub(crate) fn estimate_hnsw_bytes(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
) -> usize {
    let policy = crate::store::VectorIndexLayout::unpartitioned(dimension, quantization)
        .resolve_policy(entry_count, 1);
    HnswIndex::estimated_resident_bytes_with_m(entry_count, dimension, quantization, policy.hnsw_m)
}

pub(crate) fn estimate_hnsw_build_reservation(
    entry_count: usize,
    dimension: usize,
    quantization: VectorQuantization,
    policy: crate::store::ResolvedVectorPolicy,
) -> usize {
    let final_bytes = HnswIndex::estimated_resident_bytes_with_m(
        entry_count,
        dimension,
        quantization,
        policy.hnsw_m,
    );
    let m = policy.hnsw_m;
    let ef_construction = policy.hnsw_ef_construction;
    let max_level_bound = 16usize;
    let stored_vector_bytes = quantization.storage_bytes(dimension);
    let word = std::mem::size_of::<usize>();
    // In-memory maintenance copies the sampled live entries before releasing
    // the partition publication mutex. The copy keeps foreground writes out
    // of graph construction while this reservation owns its exact payload
    // and entry-array upper bound.
    let source_snapshot = entry_count.saturating_mul(
        std::mem::size_of::<crate::quantized::StoredVectorEntry>()
            .saturating_add(stored_vector_bytes),
    );
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
        .saturating_add(source_snapshot)
        .saturating_add(sorted_entry_refs)
        .saturating_add(cloned_vectors_and_refs)
        .saturating_add(map_and_exact_key_overhead)
        .saturating_add(graph_link_upper_bound)
        .saturating_add(construction_scratch)
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExactScorePhaseForTest {
    Scored,
    Consuming,
}

#[cfg(feature = "test-seams")]
type ExactScoreObserver = Arc<dyn Fn(ExactScorePhaseForTest, usize, usize) + Send + Sync>;
#[cfg(feature = "test-seams")]
thread_local! {
    static EXACT_SCORE_OBSERVER: RefCell<Option<ExactScoreObserver>> = const { RefCell::new(None) };
}
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_exact_score_observer_for_test<T>(
    observer: ExactScoreObserver,
    action: impl FnOnce() -> T,
) -> T {
    let previous = EXACT_SCORE_OBSERVER.with(|slot| slot.replace(Some(observer)));
    struct Restore(Option<ExactScoreObserver>);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXACT_SCORE_OBSERVER.with(|slot| slot.replace(self.0.take()));
        }
    }
    let _restore = Restore(previous);
    action()
}
#[cfg(feature = "test-seams")]
fn observe_exact_scores_for_test(phase: ExactScorePhaseForTest, capacity: usize, len: usize) {
    let observer = EXACT_SCORE_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer(phase, capacity, len);
    }
}
