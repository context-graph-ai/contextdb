use crate::hnsw::{
    BoundedHnswQuerySelfScores, HnswAllowedSearchIncomplete, HnswAllowedSearchLimits,
    HnswAllowedSearchStatus,
};
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

/// What one graph-walk evaluation costs, in nanoseconds.
///
/// Measured, not fitted: release build on the lane host (AMD Ryzen 9 6900HX,
/// 16 CPUs), file route through a real `ReadSession`, one Context of 12,500
/// vectors x 768 at `EF_SEARCH = 400`, graph built at `M = 24`, 100 held-out
/// queries timed on a warm second pass. The walk answered at 51.5 ms warm
/// p95 over 9,733 evaluations (admission plus expansion, mean); 51.5 ms /
/// 9,733 = 5.3 us. Raw log:
/// `logs/f11-step4c-12500-ef400-91a02804.log` (summary line `mode=INDEXED`).
pub const AUTO_WALK_EVALUATION_NANOS: u64 = 5_300;

/// What one exhaustive comparison costs, in nanoseconds, measured in the same
/// run as [`AUTO_WALK_EVALUATION_NANOS`]: the exhaustive route over the same
/// 12,500 vectors answered at 847.7 ms warm p95; 847.7 ms / 12,500 = 67.8 us.
/// It is this much dearer than a walk evaluation because it reads and
/// authenticates each row's own stored body rather than the graph's copy.
/// Raw log: `logs/f11-step4c-12500-ef400-91a02804.log` (summary line
/// `mode=EXACT`).
pub const AUTO_EXHAUSTIVE_COMPARISON_NANOS: u64 = 67_800;

/// A resident comparison does not read or authenticate a durable body.
/// Measured in release on the same Ryzen 9 6900HX host: 2,000 resident
/// vectors x 32 dimensions, 100 held-out exhaustive queries, 3,395 ns per
/// comparison including bounded query setup.
pub const AUTO_IN_MEMORY_EXHAUSTIVE_COMPARISON_NANOS: u64 = 3_400;

/// Why AUTO answered a whole statement by comparing its admitted rows.
pub const AUTO_STATEMENT_EXHAUSTIVE_REASON: &str = "statement_exhaustive_cheaper_than_walk";

/// The walk budget a statement's one search is given, `cap(ef, k) x
/// M_max0`, for a caller that knows the widest admitted serving graph only by
/// its declared degree: the layer-0 maximum degree comes from the build's own
/// definition of that layer, the same one the loaded graphs report.
#[doc(hidden)]
pub fn auto_statement_walk_budget(ef_search: usize, k: usize, serving_hnsw_m: usize) -> usize {
    crate::hnsw::union_candidate_budget(
        ef_search,
        k,
        hnsw_rs::hnsw::layer0_max_nb_connection(serving_hnsw_m),
    )
}

/// The file-route crossover for a statement whose admitted bodies must all
/// be read and authenticated. The live decision prices each source separately
/// because some statements mix resident and stored bodies.
///
/// The walk's whole budget is one search over every admitted Context. The
/// comparison side reads every admitted row once, at the file-route cost.
#[doc(hidden)]
pub fn auto_statement_prefers_exhaustive(
    walk_budget: usize,
    aggregate_allowed_count: usize,
) -> bool {
    auto_statement_prefers_exhaustive_at_cost(
        walk_budget,
        (aggregate_allowed_count as u128)
            .saturating_mul(u128::from(AUTO_EXHAUSTIVE_COMPARISON_NANOS)),
    )
}

fn auto_statement_prefers_exhaustive_at_cost(walk_budget: usize, exhaustive_nanos: u128) -> bool {
    exhaustive_nanos <= (walk_budget as u128).saturating_mul(u128::from(AUTO_WALK_EVALUATION_NANOS))
}

/// Estimate the work a healthy graph search spends before its budget. On a
/// 2,048-row corpus the measured expansion was 315-1,364 evaluations against
/// a 3,200 allowance; at 12,500 rows it was about 9,700 against 19,200. The
/// estimate reaches the full allowance at 20,000 admitted rows. Layer opening
/// is added separately, so splitting rows can make exhaustive work cheaper.
fn auto_statement_expected_expansion(budget: usize, allowed: usize) -> usize {
    budget.saturating_mul(allowed).div_ceil(20_000).min(budget)
}

/// Price one graph's entry and greedy descent through layer zero. Each
/// successive level reduces the live population by about its build degree;
/// the bottom layer can expose up to the stored layer-zero maximum degree.
fn auto_layer_admission_estimate(live: usize, m: usize, layer_zero_degree: usize) -> usize {
    let mut remaining = live.max(1);
    let mut levels = 0usize;
    while remaining > 1 {
        remaining = remaining.div_ceil(m.max(2));
        levels += 1;
    }
    layer_zero_degree.max(1).saturating_mul(levels.max(1))
}

/// What one admitted partition cost this find, in the order it was entered.
#[allow(clippy::too_many_arguments)]
fn note_partition_attribution(
    route: &str,
    reason: &str,
    live_count: usize,
    entry_count: usize,
    allowed_count: usize,
    ef_search: usize,
    exact_comparisons: usize,
    layers: &std::cell::Cell<u64>,
    visited: &std::cell::Cell<u64>,
    evaluations: &std::cell::Cell<u64>,
    started: std::time::Instant,
    pages_before: (u64, u64, u64, u64),
) {
    use contextdb_core::find_attribution::Field;
    if !contextdb_core::find_attribution::enabled() {
        return;
    }
    let pages_now = contextdb_core::find_attribution::page_meters();
    contextdb_core::find_attribution::note_partition(vec![
        (
            "page_lookups".to_string(),
            Field::Count(pages_now.0.saturating_sub(pages_before.0)),
        ),
        (
            "page_hits".to_string(),
            Field::Count(pages_now.1.saturating_sub(pages_before.1)),
        ),
        (
            "page_fills".to_string(),
            Field::Count(pages_now.2.saturating_sub(pages_before.2)),
        ),
        (
            "page_fill_bytes".to_string(),
            Field::Count(pages_now.3.saturating_sub(pages_before.3)),
        ),
        ("route".to_string(), Field::Text(route.to_string())),
        ("reason".to_string(), Field::Text(reason.to_string())),
        ("live_count".to_string(), Field::Count(live_count as u64)),
        ("entry_count".to_string(), Field::Count(entry_count as u64)),
        (
            "allowed_count".to_string(),
            Field::Count(allowed_count as u64),
        ),
        ("ef_search".to_string(), Field::Count(ef_search as u64)),
        (
            "exact_comparisons".to_string(),
            Field::Count(exact_comparisons as u64),
        ),
        ("layers_searched".to_string(), Field::Count(layers.get())),
        ("visited_nodes".to_string(), Field::Count(visited.get())),
        (
            "vector_evaluations".to_string(),
            Field::Count(evaluations.get()),
        ),
        (
            "ms".to_string(),
            Field::Millis(started.elapsed().as_nanos() as f64 / 1e6),
        ),
    ]);
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
    index: &VectorIndexRef,
    state: &crate::store::IndexState,
    query: &[f32],
    k: usize,
    candidates: Option<&[u64]>,
    snapshot: SnapshotId,
    scores: &mut Vec<(RowId, f32)>,
    retained_bytes: &mut usize,
    bodies: &mut crate::store::RawVectorBodyReads,
    before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
    before_candidate: &mut impl FnMut() -> std::result::Result<(), E>,
    before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<usize, E>
where
    E: From<Error>,
{
    // An exhaustive search reads each row's own stored body. It is the route a
    // caller reaches for when the maintained graph is gone or suspect, so it
    // never takes its numbers from one -- and it reads those bodies one at a
    // time through the statement's one open read, admitting each before it is
    // read, so it never needs room for a whole partition of them.
    if let Some(candidate_ids) = candidates {
        let mut compared = 0usize;
        for raw_row_id in candidate_ids {
            before_source_entry()?;
            let row_id = RowId(*raw_row_id);
            let Some(score) = state.bounded_score_stored_body(
                index,
                row_id,
                snapshot,
                query,
                bodies,
                before_source_entry,
                before_distance,
                before_retain,
                release_retained,
            )?
            else {
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
                (row_id, score),
                k,
                before_source_entry,
                before_retain,
                release_retained,
            )?;
        }
        return Ok(compared);
    }
    before_source_entry()?;
    // A partition whose vectors fit is loaded once and compared from memory,
    // which every later find reuses. Only when the memory to hold it is
    // refused does the comparison read the bodies one at a time instead, so a
    // find the reader could not hold is answered rather than refused, and a
    // find it could hold does not re-read every body on every call.
    let stream = state.streams_raw_bodies()
        && match state.ensure_raw_vectors_loaded() {
            Ok(()) => false,
            Err(Error::MemoryBudgetExceeded { .. }) => true,
            Err(error) => return Err(E::from(error)),
        };
    if stream {
        let mut compared = 0usize;
        let mut position = 0usize;
        loop {
            before_source_entry()?;
            let Some(visible) = state.visible_directory_row_at(position, snapshot) else {
                break;
            };
            position = position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded exact vector position overflow".to_string(),
                ))
            })?;
            // The one exhaustive-scan charge for this directory entry; dead and
            // future versions consumed the step too.
            before_candidate()?;
            let Some(row_id) = visible else {
                continue;
            };
            let Some(score) = state.bounded_score_stored_body(
                index,
                row_id,
                snapshot,
                query,
                bodies,
                before_source_entry,
                before_distance,
                before_retain,
                release_retained,
            )?
            else {
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
                (row_id, score),
                k,
                before_source_entry,
                before_retain,
                release_retained,
            )?;
        }
        return Ok(compared);
    }
    state.ensure_raw_vectors_loaded().map_err(E::from)?;
    let entry_count = state.entry_count();
    let mut compared = 0usize;
    for position in 0..entry_count {
        before_source_entry()?;
        before_candidate()?;
        let candidate = state.with_entries(|entries| -> std::result::Result<Option<_>, E> {
            let entry = entries.get(position).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded exact vector position changed".to_string(),
                ))
            })?;
            if !entry.visible_at(snapshot) {
                return Ok(None);
            }
            before_distance()?;
            #[cfg(feature = "test-seams")]
            observe_native_vector_score_for_test();
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorPartitionSearchRoute {
    Exact,
    Indexed,
}

/// The route and reason selected while one non-empty partition was executed.
/// Entries stay in selected-partition order; disclosure redacts keys separately.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorPartitionSearchTrace {
    pub route: VectorPartitionSearchRoute,
    pub reason: &'static str,
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
    /// Actual per-partition choices in selected-partition order.
    pub partition_routes: Vec<VectorPartitionSearchTrace>,
    /// Maximum per-partition EF_SEARCH used by this maintained-route query.
    /// `None` means the query did not traverse a graph.
    pub hnsw_ef_search: Option<usize>,
    /// Durable generation identities actually searched for this result. The
    /// mutable tail has no durable identity and is deliberately absent.
    pub selected_generations: Vec<crate::store::VectorGraphGeneration>,
    /// The one union walk's budget and terminal state, when a walk ran.
    pub budget_receipt: Option<VectorSearchBudgetReceipt>,
}

/// Work paid by one statement-wide graph walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorSearchBudgetReceipt {
    pub global_candidate_budget: usize,
    pub admission_evaluations: usize,
    pub expansion_evaluations: usize,
    pub layers_admitted: usize,
    pub budget_terminal: &'static str,
}

impl VectorSearchDebugTrace {
    fn brute_force(
        index: &VectorIndexRef,
        rows: &[(RowId, f32)],
        fallback_reason: &'static str,
        hnsw_len: Option<usize>,
        partition_routes: Vec<VectorPartitionSearchTrace>,
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
            partition_routes,
            hnsw_ef_search: None,
            selected_generations: Vec::new(),
            budget_receipt: None,
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
    candidate_filter: bool,
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
    /// The one open read this cursor's exhaustive comparisons read stored
    /// bodies through, across every partition and every resumption.
    body_reads: crate::store::RawVectorBodyReads,
    /// A partition whose vectors the memory cannot hold whole is compared one
    /// stored body at a time instead of refused. Set for automatic exhaustive
    /// work, and for an exact comparison over a committed image, which has no
    /// raw-vector cache a later find reuses.
    stream_when_refused: bool,
    /// The current partition is being compared one stored body at a time.
    streaming_source: bool,
    membership_cursor: Option<Box<dyn crate::store::RawVectorMembershipCursor>>,
    membership_reserved_bytes: usize,
    pending_membership: Option<crate::store::RawVectorDirectoryEntry>,
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
    candidate_ids: Option<Vec<u64>>,
    point_load: bool,
    candidate_ids_cover_all_live: bool,
    allowed_count: usize,
    live_count: usize,
}

impl std::fmt::Debug for BoundedSnapshotVectorSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundedSnapshotVectorSource")
            .field("partition_key", &self.partition_key)
            .finish_non_exhaustive()
    }
}

/// One admitted snapshot partitioning of the relational candidate set. Every
/// later route decision and exact/graph consumer borrows these same local
/// slices, so selected-partition fanout cannot multiply membership work.
#[derive(Debug)]
pub struct BoundedVectorCandidateGroups {
    snapshot: SnapshotId,
    filtered: bool,
    sources: Vec<BoundedSnapshotVectorSource>,
    aggregate_allowed_count: usize,
    auto_index_at: usize,
    every_partition_fits_base_ef: bool,
    retained_bytes: usize,
}

impl BoundedVectorCandidateGroups {
    pub fn is_filtered(&self) -> bool {
        self.filtered
    }

    pub fn aggregate_allowed_count(&self) -> usize {
        self.aggregate_allowed_count
    }

    pub fn auto_uses_only_exact_at_base_ef(&self) -> bool {
        self.aggregate_allowed_count < self.auto_index_at || self.every_partition_fits_base_ef
    }

    pub fn partition_allowed_count(&self, key: &VectorPartitionKey) -> usize {
        self.sources
            .binary_search_by(|source| source.partition_key.cmp(key))
            .ok()
            .and_then(|position| self.sources.get(position))
            .map_or(0, |source| source.allowed_count)
    }

    pub fn partition_live_count(&self, key: &VectorPartitionKey) -> usize {
        self.sources
            .binary_search_by(|source| source.partition_key.cmp(key))
            .ok()
            .and_then(|position| self.sources.get(position))
            .map_or(0, |source| source.live_count)
    }

    /// Visit the exact non-empty snapshot sources this grouped search owns.
    /// The callback receives no partition identity: consumers can publish one
    /// redacted entry per executed source without reconstructing a newer,
    /// live-only partition inventory or exposing the typed key.
    #[doc(hidden)]
    pub fn visit_nonempty_partitions<E>(
        &self,
        mut visit: impl FnMut(
            usize,
            usize,
            Option<crate::store::VectorServingPolicy>,
        ) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        for source in self
            .sources
            .iter()
            .filter(|source| source.allowed_count != 0)
        {
            visit(
                source.allowed_count,
                source.live_count,
                source.state.snapshot_serving_policy(self.snapshot),
            )?;
        }
        Ok(())
    }

    pub fn graph_layer_presence_without_load(&self) -> (bool, bool, bool) {
        let mut base = false;
        let mut change = false;
        let mut tail = false;
        for source in self
            .sources
            .iter()
            .filter(|source| source.allowed_count != 0)
        {
            let status = source.state.graph_generation_status();
            let sealed_base = status.base.is_some() || status.dormant_base.is_some();
            base |= sealed_base
                || (source.state.has_complete_hnsw_route() && status.fresh_tail_entries != 0);
            change |= status.change.is_some() || status.dormant_change.is_some();
            tail |= sealed_base && status.fresh_tail_entries != 0;
        }
        (base, change, tail)
    }

    pub fn into_retained_bytes(self) -> usize {
        self.retained_bytes
    }

    fn into_cursor_parts(self) -> (SnapshotId, bool, Vec<BoundedSnapshotVectorSource>, usize) {
        (
            self.snapshot,
            self.filtered,
            self.sources,
            self.retained_bytes,
        )
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

    pub fn partition_routes(&self) -> &[VectorPartitionSearchTrace] {
        &self.trace.partition_routes
    }

    pub fn used_hnsw(&self) -> bool {
        self.trace.used_hnsw
    }

    pub fn fallback_reason(&self) -> Option<&'static str> {
        self.trace.fallback_reason
    }

    pub fn budget_receipt(&self) -> Option<VectorSearchBudgetReceipt> {
        self.trace.budget_receipt
    }

    pub fn override_exact_reason(&mut self, reason: &'static str) {
        self.trace.fallback_reason = Some(reason);
        for partition in &mut self.trace.partition_routes {
            if partition.route == VectorPartitionSearchRoute::Exact {
                partition.reason = reason;
            }
        }
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
    /// AUTO weighed the statement's one walk against comparing every admitted
    /// row, at their measured unit costs, and the comparison was no dearer.
    StatementExhaustiveCheaperThanWalk,
    ExactMode,
    AggregateBelowIndexedThreshold,
    QuantizedAggregateUsesExact,
    SmallPartitionCandidateSetUsesExact,
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
        trace: BoundedVectorSearchTrace,
    },
    Unavailable {
        reason: BoundedHnswUnavailableReason,
    },
    Incomplete {
        reason: HnswAllowedSearchIncomplete,
    },
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

    /// A committed image reads each stored body when the partition does not
    /// fit. A writable store does not: its exact search keeps the load.
    pub fn exact_comparison_streams_when_partition_does_not_fit(&self) -> bool {
        self.snapshot_sources.iter().any(|source| {
            source
                .state
                .exact_comparison_streams_when_partition_does_not_fit()
        })
    }

    /// Compare a partition whose vectors the memory cannot hold whole by
    /// reading each stored body through its own point read, rather than
    /// refusing it.
    pub fn streaming_bodies_it_cannot_hold(mut self) -> Self {
        self.stream_when_refused = true;
        self
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

pub(crate) fn apply_hnsw_scratch_acquire<E>(
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

pub(crate) fn apply_hnsw_scratch_release(
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
    budget_receipt: Option<VectorSearchBudgetReceipt>,
    selected_generation_bytes: usize,
    partition_routes: Vec<VectorPartitionSearchTrace>,
    partition_route_bytes: usize,
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
                release_retained(partition_route_bytes);
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
            release_retained(partition_route_bytes);
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
            release_retained(partition_route_bytes);
            release_retained(candidate_bytes);
            release_retained(index_bytes);
            return Err(error);
        }
    };
    let retained_bytes = match checked_vector_add(index_bytes, candidate_bytes, "HNSW trace")
        .and_then(|bytes| checked_vector_add(bytes, final_bytes, "HNSW trace"))
        .and_then(|bytes| checked_vector_add(bytes, selected_generation_bytes, "HNSW trace"))
        .and_then(|bytes| checked_vector_add(bytes, partition_route_bytes, "HNSW trace"))
    {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(selected_generation_bytes);
            release_retained(partition_route_bytes);
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
            partition_routes,
            hnsw_ef_search: Some(hnsw_ef_search),
            selected_generations,
            budget_receipt,
        },
        retained_bytes,
    })
}

fn allocate_partition_route_slots<E>(
    capacity: usize,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<VectorPartitionSearchTrace>, usize), E>
where
    E: From<Error>,
{
    if capacity == 0 {
        return Ok((Vec::new(), 0));
    }
    let requested = checked_vector_mul(
        capacity,
        std::mem::size_of::<VectorPartitionSearchTrace>(),
        "partition route receipt",
    )
    .map_err(E::from)?;
    before_retain(requested)?;
    let mut routes = Vec::new();
    if routes.try_reserve_exact(capacity).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded vector partition route receipt allocation failed".to_string(),
        )));
    }
    let actual = match checked_vector_mul(
        routes.capacity(),
        std::mem::size_of::<VectorPartitionSearchTrace>(),
        "partition route receipt",
    ) {
        Ok(actual) => actual,
        Err(error) => {
            release_retained(requested);
            return Err(E::from(error));
        }
    };
    let actual = reconcile_vector_allocation(requested, actual, before_retain, release_retained)?;
    Ok((routes, actual))
}

fn bounded_exact_route_trace<E>(
    index: &VectorIndexRef,
    partition_routes: Vec<VectorPartitionSearchTrace>,
    partition_route_bytes: usize,
    fallback_reason: &'static str,
    budget_receipt: Option<VectorSearchBudgetReceipt>,
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
                release_retained(partition_route_bytes);
                return Err(error);
            }
        };
    let retained_bytes = match checked_vector_add(
        index_bytes,
        partition_route_bytes,
        "exact partition route receipt",
    ) {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(index_bytes);
            release_retained(partition_route_bytes);
            return Err(E::from(error));
        }
    };
    Ok(BoundedVectorSearchTrace {
        trace: VectorSearchDebugTrace {
            index: owned_index,
            used_hnsw: false,
            hnsw_len: None,
            hnsw_candidate_count: 0,
            hnsw_candidate_row_ids: Vec::new(),
            final_row_ids: Vec::new(),
            supplemented_row_count: 0,
            fallback_reason: Some(fallback_reason),
            partition_routes,
            hnsw_ef_search: None,
            selected_generations: Vec::new(),
            budget_receipt,
        },
        retained_bytes,
    })
}

fn bounded_uniform_exact_outcome<E>(
    index: &VectorIndexRef,
    groups: &BoundedVectorCandidateGroups,
    reason: BoundedHnswExactReason,
    route_reason: &'static str,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<BoundedHnswOutcome, E>
where
    E: From<Error>,
{
    let (mut partition_routes, partition_route_bytes) =
        allocate_partition_route_slots(groups.sources.len(), before_retain, release_retained)?;
    partition_routes.extend(
        groups
            .sources
            .iter()
            .filter(|source| source.allowed_count != 0)
            .map(|_| VectorPartitionSearchTrace {
                route: VectorPartitionSearchRoute::Exact,
                reason: route_reason,
            }),
    );
    let trace = bounded_exact_route_trace(
        index,
        partition_routes,
        partition_route_bytes,
        route_reason,
        None,
        before_retain,
        release_retained,
    )?;
    Ok(BoundedHnswOutcome::ExactRequired {
        reason,
        aggregate_allowed_count: groups.aggregate_allowed_count(),
        trace,
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
    key: &VectorPartitionKey,
    state: Arc<crate::store::IndexState>,
    retained_bytes: &mut usize,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    let position = match sources.binary_search_by(|source| source.partition_key.cmp(key)) {
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
            partition_key: key.clone(),
            state,
            candidate_ids: None,
            point_load: false,
            candidate_ids_cover_all_live: false,
            allowed_count: 0,
            live_count: 0,
        },
    );
    *retained_bytes = next_retained;
    Ok(())
}

/// How far a stored score may sit from the score its walk distance gives for
/// the same vector: the two are summed by different float paths, so equal
/// vectors can differ in the last bits.
const WALK_SCORE_TOLERANCE: f32 = 1e-5;

/// Whether a row the walk reached at `distance` can no longer enter the best
/// `required` of `accepted`: at least `required` accepted rows already score
/// strictly better than its walk score, beyond float noise. A row that would
/// tie keeps being scored, because an equal score is ordered by row id and
/// may still take a place.
fn walk_cannot_beat(accepted: &[(RowId, f32)], required: usize, distance: f32) -> bool {
    if required == 0 {
        return true;
    }
    let mut better = 0usize;
    let walk_score = 1.0 - distance;
    for (_, score) in accepted {
        if *score - WALK_SCORE_TOLERANCE > walk_score {
            better += 1;
            if better >= required {
                return true;
            }
        }
    }
    false
}

/// One partition a statement answers through its one union search.
struct UnionPartition<'g> {
    source_position: usize,
    candidates: Option<&'g [u64]>,
    allowed_count: usize,
    live_count: usize,
    entry_count: usize,
    bounded_limits: bool,
    layers: Vec<crate::store::SnapshotCompatibleLayer<'g>>,
    /// What this entry and its layer list were charged when admitted.
    charged_bytes: usize,
    /// How many partition routes the statement had recorded when this
    /// partition was admitted, so its own route is recorded in scope order.
    routes_before: usize,
    started: std::time::Instant,
    pages: (u64, u64, u64, u64),
}

/// What a statement's one union search resolved.
enum UnionOutcome {
    /// Every candidate re-scored from its own partition's stored vector, and
    /// the `(partition, layer)` pairs that contributed one.
    Answered {
        accepted: Vec<(RowId, f32)>,
        accepted_bytes: usize,
        /// Every row the walk handed back, for the route trace.
        walked: Vec<(RowId, f32)>,
        walked_bytes: usize,
        contributing: Vec<(usize, usize)>,
        /// Partitions whose graph candidates fell short of what they need,
        /// answered by comparing their rows instead.
        short: Vec<bool>,
        short_bytes: usize,
    },
    /// The search could not establish a full answer: its budget ran out, its
    /// admission alone exceeded the budget, or too few admitted rows came back.
    Unresolved(HnswAllowedSearchIncomplete),
}

struct UnionSearchResult {
    outcome: UnionOutcome,
    receipt: VectorSearchBudgetReceipt,
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
                    key,
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
        layout: &crate::store::VectorIndexLayout,
        prefixes: &[VectorPartitionKey],
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
    where
        E: From<Error>,
    {
        if prefixes
            .iter()
            .all(|prefix| prefix.components().len() == layout.partition_key_columns.len())
        {
            let (mut selected, mut retained_bytes) =
                allocate_snapshot_source_slots(prefixes.len(), before_retain, release_retained)?;
            for prefix in prefixes {
                let Some(state) = self.store.try_partition_state(index, prefix) else {
                    continue;
                };
                if let Err(error) = retain_snapshot_source(
                    &mut selected,
                    prefix,
                    state,
                    &mut retained_bytes,
                    before_source_entry,
                    before_retain,
                    release_retained,
                ) {
                    drop(selected);
                    release_retained(retained_bytes);
                    return Err(error);
                }
            }
            return Ok((selected, retained_bytes));
        }
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
                        key,
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

    /// Resolve the physical states an unfiltered bounded query may need.
    /// Filtered execution uses `bounded_candidate_groups`, whose snapshot-aware
    /// row placement retains only the states that own admitted candidates.
    #[allow(clippy::too_many_arguments)]
    fn bounded_snapshot_sources<E>(
        &self,
        index: &VectorIndexRef,
        layout: &crate::store::VectorIndexLayout,
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
    where
        E: From<Error>,
    {
        if let Some(prefixes) = self.selected_partition_prefixes(index) {
            return self.bounded_scoped_snapshot_sources(
                index,
                layout,
                &prefixes,
                before_source_entry,
                before_retain,
                release_retained,
            );
        }
        self.all_bounded_snapshot_sources(
            index,
            before_source_entry,
            before_retain,
            release_retained,
        )
    }

    /// Resolve every authorized candidate's snapshot-visible partition once,
    /// retain one local sorted slice per selected source, and charge the one
    /// placement that makes the candidate available to exact or graph search.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_candidate_groups<E>(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        mut before_checkpoint: impl FnMut() -> std::result::Result<(), E>,
        mut before_placement: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedVectorCandidateGroups, E>
    where
        E: From<Error>,
    {
        before_checkpoint()?;
        let layout = self.store.index_layout(index).map_err(E::from)?;
        let filtered = candidates.is_some();
        if candidates.is_some_and(<[u64]>::is_empty) {
            return Ok(BoundedVectorCandidateGroups {
                snapshot,
                filtered,
                sources: Vec::new(),
                aggregate_allowed_count: 0,
                auto_index_at: layout.effective_auto_index_at(),
                every_partition_fits_base_ef: true,
                retained_bytes: 0,
            });
        }

        let prefixes = self.selected_partition_prefixes(index);
        let discover_sources = layout.is_partitioned() && filtered && prefixes.is_none();
        let (mut sources, mut retained_bytes) = if let Some(prefixes) = prefixes.as_deref() {
            self.bounded_scoped_snapshot_sources(
                index,
                &layout,
                prefixes,
                &mut before_checkpoint,
                &mut before_retain,
                &mut release_retained,
            )?
        } else if discover_sources {
            allocate_snapshot_source_slots(
                candidates.map_or(0, <[u64]>::len).min(
                    layout
                        .max_partitions
                        .map_or(candidates.map_or(0, <[u64]>::len), |partitions| {
                            partitions as usize
                        }),
                ),
                &mut before_retain,
                &mut release_retained,
            )?
        } else {
            self.all_bounded_snapshot_sources(
                index,
                &mut before_checkpoint,
                &mut before_retain,
                &mut release_retained,
            )?
        };

        let repeat_global_candidate_pass =
            bounded_candidate_grouping_repeats_global_pass_for_test();
        let suppress_placement_charge =
            bounded_candidate_grouping_suppresses_placement_charge_for_test();
        let grouped = (|| -> std::result::Result<(), E> {
            if let Some(candidate_ids) = candidates {
                for raw_row_id in candidate_ids {
                    let row_id = RowId(*raw_row_id);
                    self.store
                        .with_snapshot_visible_partition_for_row::<E, (), _>(
                            index,
                            row_id,
                            snapshot,
                            &mut before_checkpoint,
                            |key, state, before_checkpoint| {
                                let position = match sources
                                    .binary_search_by(|source| source.partition_key.cmp(key))
                                {
                                    Ok(position) => position,
                                    Err(_) if !discover_sources => return Ok(Some(())),
                                    Err(_) => {
                                        retain_snapshot_source(
                                            &mut sources,
                                            key,
                                            state.clone(),
                                            &mut retained_bytes,
                                            before_checkpoint,
                                            &mut before_retain,
                                            &mut release_retained,
                                        )?;
                                        sources
                                            .binary_search_by(|source| {
                                                source.partition_key.cmp(key)
                                            })
                                            .map_err(|_| {
                                                E::from(Error::Other(
                                                    "bounded vector candidate source disappeared"
                                                        .to_string(),
                                                ))
                                            })?
                                    }
                                };
                                if !suppress_placement_charge {
                                    before_placement()?;
                                }
                                let source = sources.get_mut(position).ok_or_else(|| {
                                    E::from(Error::Other(
                                        "bounded vector candidate group changed".to_string(),
                                    ))
                                })?;
                                let local = source.candidate_ids.get_or_insert_with(Vec::new);
                                let required = local.len().checked_add(1).ok_or_else(|| {
                                    E::from(Error::Other(
                                        "bounded vector candidate group length overflow"
                                            .to_string(),
                                    ))
                                })?;
                                grow_vector_copy(
                                    local,
                                    required,
                                    Some(&mut retained_bytes),
                                    "partition candidate group",
                                    before_checkpoint,
                                    &mut before_retain,
                                    &mut release_retained,
                                )?;
                                local.push(row_id.0);
                                #[cfg(feature = "test-seams")]
                                observe_bounded_candidate_grouping_for_test(
                                    BoundedCandidateGroupingEventForTest::Placement,
                                );
                                source.allowed_count =
                                    source.allowed_count.checked_add(1).ok_or_else(|| {
                                        E::from(Error::Other(
                                            "bounded vector partition candidate count overflow"
                                                .to_string(),
                                        ))
                                    })?;
                                Ok(Some(()))
                            },
                        )?;
                }
                if repeat_global_candidate_pass {
                    let physical_partition_count = self
                        .store
                        .partition_keys_including_historical(index)
                        .map_err(E::from)?
                        .len();
                    for _ in 1..physical_partition_count {
                        for raw_row_id in candidate_ids {
                            self.store
                                .with_snapshot_visible_partition_for_row::<E, (), _>(
                                    index,
                                    RowId(*raw_row_id),
                                    snapshot,
                                    &mut before_checkpoint,
                                    |_key, _state, _before_checkpoint| Ok(Some(())),
                                )?;
                        }
                    }
                }
                for source in &mut sources {
                    let candidate_ids = source.candidate_ids.get_or_insert_with(Vec::new);
                    let candidates_are_unique_and_sorted =
                        candidate_ids.windows(2).all(|pair| pair[0] < pair[1]);
                    let live_count = source.state.bounded_directory_visible_entry_count(
                        snapshot,
                        None,
                        &mut before_checkpoint,
                    )?;
                    source.live_count = live_count;
                    source.candidate_ids_cover_all_live = source.allowed_count != 0
                        && source.allowed_count == live_count
                        && candidates_are_unique_and_sorted;
                }
                return Ok(());
            }

            for source in &mut sources {
                let live_count = source.state.bounded_directory_visible_entry_count(
                    snapshot,
                    None,
                    &mut before_checkpoint,
                )?;
                source.allowed_count = live_count;
                source.live_count = live_count;
                // An unrestricted partition is compared as a whole: EXACT
                // loads it when the declaration can hold it, and AUTO
                // streams one stored body at a time only after that load
                // is refused. Collecting every live identity here would
                // skip the load and never install a resident partition.
            }
            Ok(())
        })();
        if let Err(error) = grouped {
            drop(sources);
            release_retained(retained_bytes);
            return Err(error);
        }
        let aggregate_allowed_count = sources.iter().fold(0usize, |count, source| {
            count.saturating_add(source.allowed_count)
        });
        let every_partition_fits_base_ef = sources
            .iter()
            .filter(|source| source.allowed_count != 0)
            .all(|source| {
                source.live_count <= layout.resolve_policy(source.live_count, 0).hnsw_ef_search
            });
        Ok(BoundedVectorCandidateGroups {
            snapshot,
            filtered,
            sources,
            aggregate_allowed_count,
            auto_index_at: layout.effective_auto_index_at(),
            every_partition_fits_base_ef,
            retained_bytes,
        })
    }

    /// Compatibility entrypoint for callers that have not already admitted a
    /// candidate grouping. Engine execution uses the grouped entrypoint below.
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
        let groups = self.bounded_candidate_groups(
            &index,
            candidates,
            snapshot,
            &mut before_source_entry,
            || Ok(()),
            &mut before_retain,
            &mut release_retained,
        )?;
        self.bounded_brute_force_cursor_grouped(
            index,
            query,
            k,
            groups,
            before_source_entry,
            before_retain,
            release_retained,
        )
    }

    /// Create a fallible, owned brute-force continuation from the query's one
    /// admitted candidate grouping. Query allocation is reconciled before copy;
    /// partition-local candidate slices transfer directly into the cursor.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_brute_force_cursor_grouped<E>(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: BoundedVectorCandidateGroups,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedBruteForceCursor, E>
    where
        E: From<Error>,
    {
        let aggregate_allowed_count = groups.aggregate_allowed_count();
        let (snapshot, candidate_filter, snapshot_sources, group_bytes) =
            groups.into_cursor_parts();
        // Resolving the index handle takes the registry lock, so the control
        // runs before any source state is read or locked.
        if let Err(error) = before_source_entry() {
            release_retained(group_bytes);
            return Err(error);
        }
        let layout = match self.store.index_layout(&index) {
            Ok(layout) => layout,
            Err(error) => {
                release_retained(group_bytes);
                return Err(E::from(error));
            }
        };
        if query.len() != layout.dimension {
            release_retained(group_bytes);
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index,
                expected: layout.dimension,
                actual: query.len(),
            }));
        }
        let index_bytes =
            match checked_vector_add(index.table.capacity(), index.column.capacity(), "index") {
                Ok(bytes) => bytes,
                Err(error) => {
                    release_retained(group_bytes);
                    return Err(E::from(error));
                }
            };
        if let Err(error) = before_retain(index_bytes) {
            release_retained(group_bytes);
            return Err(error);
        }
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
                release_retained(group_bytes);
                return Err(error);
            }
        };
        let mut retained_bytes = match checked_vector_add(index_bytes, query_bytes, "query state")
            .and_then(|bytes| checked_vector_add(bytes, group_bytes, "candidate groups"))
        {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(query_bytes);
                release_retained(index_bytes);
                release_retained(group_bytes);
                return Err(E::from(error));
            }
        };
        let mut scored = Vec::new();
        let prepare = grow_vector_copy(
            &mut scored,
            aggregate_allowed_count.min(k),
            Some(&mut retained_bytes),
            "exact scores",
            &mut before_source_entry,
            &mut before_retain,
            &mut release_retained,
        );
        if let Err(error) = prepare {
            drop(scored);
            drop(snapshot_sources);
            drop(owned_query);
            release_retained(retained_bytes);
            return Err(error);
        }
        Ok(BoundedBruteForceCursor {
            _index: index,
            query: owned_query,
            k,
            candidate_filter,
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
            body_reads: crate::store::RawVectorBodyReads::default(),
            stream_when_refused: false,
            streaming_source: false,
            membership_cursor: None,
            membership_reserved_bytes: 0,
            pending_membership: None,
        })
    }

    /// Advance exactly one candidate/retained-score/output boundary.
    pub fn bounded_brute_force_step<E>(
        &self,
        cursor: &mut BoundedBruteForceCursor,
        before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        before_distance: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedVectorStep, E>
    where
        E: From<Error>,
    {
        let before_source_entry = std::cell::RefCell::new(before_source_entry);
        self.bounded_brute_force_step_charged(
            cursor,
            || (before_source_entry.borrow_mut())(),
            || (before_source_entry.borrow_mut())(),
            || (before_source_entry.borrow_mut())(),
            before_distance,
            before_retain,
            release_retained,
        )
    }

    /// Advance exactly one candidate/retained-score/output boundary, with a
    /// distinct callback for the exhaustive physical-entry charge.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_brute_force_step_charged<E>(
        &self,
        cursor: &mut BoundedBruteForceCursor,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_precharged_candidate: impl FnMut() -> std::result::Result<(), E>,
        mut before_exhaustive_candidate: impl FnMut() -> std::result::Result<(), E>,
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

        if cursor.candidate_filter
            || (cursor.stream_when_refused
                && cursor
                    .snapshot_sources
                    .get(cursor.source_position)
                    .is_some_and(|source| source.point_load))
        {
            let Some(source) = cursor.snapshot_sources.get(cursor.source_position) else {
                sort_vector_scores(&mut cursor.scored);
                cursor.scored.truncate(cursor.k);
                cursor.prepared = true;
                return Ok(BoundedVectorStep::Pending);
            };
            let candidate_ids = source.candidate_ids.as_deref().ok_or_else(|| {
                E::from(Error::Other(
                    "bounded filtered vector source lost its candidate group".to_string(),
                ))
            })?;
            let Some(raw_row_id) = candidate_ids.get(cursor.position).copied() else {
                cursor.source_position =
                    cursor.source_position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded vector partition position overflow".to_string(),
                        ))
                    })?;
                cursor.position = 0;
                return Ok(BoundedVectorStep::Pending);
            };
            let row_id = RowId(raw_row_id);
            before_precharged_candidate()?;
            // Brute force IS the exhaustive route; it reads stored bodies,
            // through the one read this cursor keeps open.
            cursor.pending_score = source
                .state
                .bounded_score_stored_body(
                    &cursor._index,
                    row_id,
                    cursor.snapshot,
                    &cursor.query,
                    &mut cursor.body_reads,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut before_retain,
                    &mut release_retained,
                )?
                .map(|score| (row_id, score));
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
            if let Some(memberships) = source.state.durable_membership_source() {
                let mut interrupted = None;
                let mut reserved = 0usize;
                let opened = memberships.open(&mut |request| {
                    if interrupted.is_some() {
                        return false;
                    }
                    let result = match request {
                        crate::store::DormantVectorLoadRequest::Reserve(bytes) => {
                            match reserved.checked_add(bytes) {
                                Some(total) => before_retain(bytes).map(|()| reserved = total),
                                None => Err(E::from(Error::Other(
                                    "membership cursor reservation overflow".to_string(),
                                ))),
                            }
                        }
                        _ => before_source_entry(),
                    };
                    if let Err(error) = result {
                        interrupted = Some(error);
                        false
                    } else {
                        true
                    }
                });
                if let Some(error) = interrupted {
                    drop(opened);
                    release_retained(reserved);
                    return Err(error);
                }
                let opened = match opened {
                    Ok(opened) => opened,
                    Err(error) => {
                        release_retained(reserved);
                        return Err(E::from(error));
                    }
                };
                cursor.retained_bytes = match cursor.retained_bytes.checked_add(reserved) {
                    Some(bytes) => bytes,
                    None => {
                        drop(opened);
                        release_retained(reserved);
                        return Err(E::from(Error::Other(
                            "membership cursor retained bytes overflow".to_string(),
                        )));
                    }
                };
                cursor.membership_reserved_bytes = reserved;
                cursor.membership_cursor = Some(opened);
                cursor.streaming_source = true;
                cursor.state_end = Some(usize::MAX);
            } else {
                match source.state.ensure_raw_vectors_loaded() {
                    Ok(()) => {
                        cursor.streaming_source = false;
                        cursor.state_end = Some(source.state.entry_count());
                    }
                    Err(Error::MemoryBudgetExceeded { .. })
                        if cursor.stream_when_refused && source.state.streams_raw_bodies() =>
                    {
                        // The directory's end is found by reading past it.
                        cursor.streaming_source = true;
                        cursor.state_end = Some(usize::MAX);
                    }
                    Err(error) => return Err(E::from(error)),
                }
            }
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
        if cursor.streaming_source {
            let visible = if let Some(memberships) = cursor.membership_cursor.as_mut() {
                let mut interrupted = None;
                let next = if let Some(entry) = cursor.pending_membership.as_ref() {
                    Ok(Some(entry.clone()))
                } else {
                    memberships.next(&mut |request| {
                    let result = match request {
                        crate::store::DormantVectorLoadRequest::Reserve(_) => Err(E::from(Error::Other(
                            "membership cursor must admit its bounded window before opening".to_string(),
                        ))),
                        _ => before_source_entry(),
                    };
                    if let Err(error) = result { interrupted = Some(error); false } else { true }
                })
                };
                if let Some(error) = interrupted {
                    return Err(error);
                }
                cursor.pending_membership = next.map_err(E::from)?;
                cursor.pending_membership.as_ref().map(|entry| {
                    (entry.created_tx.0 <= cursor.snapshot.0
                        && entry.deleted_tx.is_none_or(|tx| tx.0 > cursor.snapshot.0))
                    .then_some(entry.row_id)
                })
            } else {
                source
                    .state
                    .visible_directory_row_at(position, cursor.snapshot)
            };
            let Some(visible) = visible else {
                cursor.membership_cursor.take();
                let reserved = std::mem::take(&mut cursor.membership_reserved_bytes);
                cursor.retained_bytes -= reserved;
                release_retained(reserved);
                cursor.source_position =
                    cursor.source_position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded vector partition position overflow".to_string(),
                        ))
                    })?;
                cursor.position = 0;
                cursor.state_end = None;
                cursor.streaming_source = false;
                return Ok(BoundedVectorStep::Pending);
            };
            // The same one exhaustive-scan charge per directory entry as the
            // resident scan below; dead and future versions consume it too.
            before_exhaustive_candidate()?;
            cursor.pending_score = match visible {
                Some(row_id) => source
                    .state
                    .bounded_score_stored_body(
                        &cursor._index,
                        row_id,
                        cursor.snapshot,
                        &cursor.query,
                        &mut cursor.body_reads,
                        &mut before_source_entry,
                        &mut before_distance,
                        &mut before_retain,
                        &mut release_retained,
                    )?
                    .map(|score| (row_id, score)),
                None => None,
            };
            cursor.pending_membership.take();
            cursor.position = cursor.position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded vector candidate position overflow".to_string(),
                ))
            })?;
            return Ok(BoundedVectorStep::Pending);
        }
        // This is the one exhaustive-scan charge for the stored entry about
        // to be read. Visibility is deliberately checked afterwards: dead or
        // future versions still consumed this physical scan step.
        before_exhaustive_candidate()?;
        let scored = source
            .state
            .with_entries(|entries| -> std::result::Result<Option<_>, E> {
                let entry = entries.get(position).ok_or_else(|| {
                    E::from(Error::ReadFailure(ReadFailure::invalid_continuation(
                        "the candidate this read left off at was removed while it was suspended"
                            .to_string(),
                    )))
                })?;
                if !entry.visible_at(cursor.snapshot) {
                    return Ok(None);
                }
                before_distance()?;
                #[cfg(feature = "test-seams")]
                observe_native_vector_score_for_test();
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
            &mut before_entry,
            &mut before_retain,
            &mut release_retained,
        )?;
        let result = sources.iter().try_for_each(|source| {
            source.state.bounded_visit_visible_ids_charged(
                snapshot,
                &mut before_entry,
                &mut before_retain,
                &mut release_retained,
                &mut visit,
            )
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
        let groups = self.bounded_candidate_groups(
            index,
            candidates,
            snapshot,
            &mut before_source_entry,
            || Ok(()),
            &mut before_retain,
            &mut release_retained,
        )?;
        let count = groups.aggregate_allowed_count();
        release_retained(groups.into_retained_bytes());
        Ok(count)
    }

    /// Compatibility entrypoint for callers that supply the pre-grouping
    /// shape. It builds the same snapshot-aware groups used by the engine,
    /// then releases their admitted memory after graph execution completes.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_hnsw_search<E>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&[u64]>,
        snapshot: SnapshotId,
        mode: VectorSearchMode,
        _aggregate_allowed_count: usize,
        allowed_search_limits: HnswAllowedSearchLimits,
        mut before_checkpoint: impl FnMut() -> std::result::Result<(), E>,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_distance: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedHnswOutcome, E>
    where
        E: From<Error>,
    {
        let groups = self.bounded_candidate_groups(
            index,
            candidates,
            snapshot,
            &mut before_checkpoint,
            || Ok(()),
            &mut before_retain,
            &mut release_retained,
        )?;
        let result = self.bounded_hnsw_search_grouped(
            index,
            query,
            k,
            &groups,
            mode,
            allowed_search_limits,
            &mut before_source_entry,
            &mut before_checkpoint,
            &mut before_distance,
            &mut before_retain,
            &mut release_retained,
        );
        release_retained(groups.into_retained_bytes());
        result
    }

    /// Run each selected partition through its snapshot-compatible graph when
    /// available. AUTO exact-compares only a partition whose maintained route
    /// is unavailable or incomplete; INDEXED returns one all-or-nothing
    /// refusal. All retained graph and exact candidates share one global
    /// score/order/tie-break merge before publication.
    /// Candidate membership and the aggregate count come from `groups`, the
    /// single admitted partitioning prepared for this query.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_hnsw_search_grouped<E>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: &BoundedVectorCandidateGroups,
        mode: VectorSearchMode,
        allowed_search_limits: HnswAllowedSearchLimits,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_exact_candidate: impl FnMut() -> std::result::Result<(), E>,
        mut before_distance: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedHnswOutcome, E>
    where
        E: From<Error>,
    {
        let snapshot = groups.snapshot;
        let aggregate_allowed_count = groups.aggregate_allowed_count();
        before_source_entry()?;
        let layout = self.store.index_layout(index).map_err(E::from)?;
        if query.len() != layout.dimension {
            return Err(E::from(Error::VectorIndexDimensionMismatch {
                index: index.clone(),
                expected: layout.dimension,
                actual: query.len(),
            }));
        }
        if k == 0 || aggregate_allowed_count == 0 {
            return Ok(BoundedHnswOutcome::CompleteEmpty);
        }
        let snapshot_sources = &groups.sources;
        if snapshot_sources.is_empty() {
            return Ok(BoundedHnswOutcome::CompleteEmpty);
        }
        if mode == VectorSearchMode::Exact {
            return bounded_uniform_exact_outcome(
                index,
                groups,
                BoundedHnswExactReason::ExactMode,
                "exact_requested",
                &mut before_retain,
                &mut release_retained,
            );
        }
        if mode == VectorSearchMode::Auto
            && aggregate_allowed_count < layout.effective_auto_index_at()
        {
            let (reason, route_reason) = if matches!(layout.quantization, VectorQuantization::F32) {
                (
                    BoundedHnswExactReason::AggregateBelowIndexedThreshold,
                    "aggregate_below_auto_index_at",
                )
            } else {
                (
                    BoundedHnswExactReason::QuantizedAggregateUsesExact,
                    "quantized_exact_search",
                )
            };
            return bounded_uniform_exact_outcome(
                index,
                groups,
                reason,
                route_reason,
                &mut before_retain,
                &mut release_retained,
            );
        }
        if !query_has_positive_finite_norm(query) {
            if mode == VectorSearchMode::Auto {
                return bounded_uniform_exact_outcome(
                    index,
                    groups,
                    BoundedHnswExactReason::SmallPartitionCandidateSetUsesExact,
                    "non_positive_query_norm",
                    &mut before_retain,
                    &mut release_retained,
                );
            }
            return Ok(BoundedHnswOutcome::Unavailable {
                reason: BoundedHnswUnavailableReason::NonPositiveFiniteQueryNorm,
            });
        }

        // One search, one breadth. The profile is resolved from what this
        // statement admits in total rather than from each partition's own live
        // count, so a narrower scope and a wider one over the same corpus ask
        // the same question of the same graphs.
        let statement_live_count = snapshot_sources
            .iter()
            .map(|source| source.live_count)
            .fold(0usize, usize::saturating_add);
        let statement_policy = layout.resolve_policy(statement_live_count, search_policy_limit(k));
        if mode == VectorSearchMode::Auto {
            // One decision for the statement, before any graph is loaded: the
            // walk this statement would run is one search with one budget over
            // every admitted Context, so it is weighed against comparing every
            // admitted row, each side at its measured unit cost. No Context
            // decides for itself -- a partition with no usable graph still
            // falls back to comparing its rows below, and that is the only
            // per-partition exhaustive work AUTO does.
            let mut serving_hnsw_m = 0usize;
            let mut exhaustive_nanos = 0u128;
            let mut admission_estimate = 0usize;
            for source in snapshot_sources {
                before_source_entry()?;
                if source.allowed_count == 0 {
                    continue;
                }
                #[cfg(feature = "test-seams")]
                crate::auto_planning_test_seams::note_partition_cost();
                let comparison_nanos = if source.state.streams_raw_bodies() {
                    AUTO_EXHAUSTIVE_COMPARISON_NANOS
                } else {
                    AUTO_IN_MEMORY_EXHAUSTIVE_COMPARISON_NANOS
                };
                exhaustive_nanos = exhaustive_nanos.saturating_add(
                    (source.allowed_count as u128).saturating_mul(u128::from(comparison_nanos)),
                );
                let degree = source.state.snapshot_serving_policy(snapshot).map_or_else(
                    || {
                        layout
                            .resolve_policy(source.live_count, search_policy_limit(k))
                            .hnsw_m
                    },
                    |serving| serving.hnsw_m,
                );
                serving_hnsw_m = serving_hnsw_m.max(degree);
                admission_estimate = admission_estimate.saturating_add(
                    auto_layer_admission_estimate(
                        source.live_count,
                        degree,
                        hnsw_rs::hnsw::layer0_max_nb_connection(degree),
                    )
                    .saturating_mul(
                        source
                            .state
                            .dormant_layer_count_for_snapshot(snapshot)
                            .max(1),
                    ),
                );
            }
            let walk_budget =
                auto_statement_walk_budget(statement_policy.hnsw_ef_search, k, serving_hnsw_m);
            let expected_walk = admission_estimate.saturating_add(
                auto_statement_expected_expansion(walk_budget, aggregate_allowed_count),
            );
            contextdb_core::find_attribution::count(
                "auto_statement_walk_budget",
                walk_budget as u64,
            );
            if serving_hnsw_m != 0
                && auto_statement_prefers_exhaustive_at_cost(expected_walk, exhaustive_nanos)
            {
                return bounded_uniform_exact_outcome(
                    index,
                    groups,
                    BoundedHnswExactReason::StatementExhaustiveCheaperThanWalk,
                    AUTO_STATEMENT_EXHAUSTIVE_REASON,
                    &mut before_retain,
                    &mut release_retained,
                );
            }
        }

        let snapshot_tx = TxId::from_snapshot(snapshot);
        // Each load makes room for exactly what it reads from its saved
        // topology, by evicting idle graphs of partitions this statement is
        // not searching -- never one it is about to search, which would make a
        // find over several Contexts re-read every graph it had already read.
        let admitted = snapshot_sources
            .iter()
            .map(|source| source.state.as_ref())
            .collect::<Vec<_>>();
        let _searched_partitions =
            crate::store::VectorStore::declare_statement_partitions(&admitted);
        let _layers_to_load = {
            let dormant_bytes = snapshot_sources
                .iter()
                .map(|source| source.state.dormant_load_bytes_for_snapshot(snapshot))
                .fold(0usize, usize::saturating_add);
            contextdb_core::find_attribution::count(
                "graph_admission_priced_bytes",
                dormant_bytes as u64,
            );
            // Make room once, up front, for what the catalog says these loads
            // need at least, from idle graphs this statement is not searching.
            // A load that turns out to need more makes room for the rest
            // itself, from the same idle graphs and then from serving pages.
            self.store.reclaim_idle_graphs_for(dormant_bytes, &admitted);
            // And for the serving pages those loads could use, from idle
            // partitions outside this statement: they give up pages they can
            // re-read, never their topology.
            let dormant_pages = snapshot_sources
                .iter()
                .map(|source| source.state.dormant_page_bytes_for_snapshot(snapshot))
                .fold(0usize, usize::saturating_add);
            if dormant_bytes != 0
                && let Some(available) = self.store.memory_available_bytes()
            {
                let wanted = dormant_bytes.saturating_add(dormant_pages);
                if available < wanted {
                    self.store
                        .give_back_serving_pages_outside_statement(wanted - available);
                }
            }
            // Every saved layer not already resident will load during this
            // statement -- a partition's base and its change are a load each,
            // the same layers whose least loads `dormant_bytes` sums. Each load
            // keeps room for the least loads of the ones after it and takes
            // its share of what is free beyond that, rather than all of it.
            contextdb_core::serving_cache_share::declare_layers_to_load(
                snapshot_sources
                    .iter()
                    .map(|source| source.state.dormant_layer_count_for_snapshot(snapshot))
                    .fold(0usize, usize::saturating_add),
                dormant_bytes,
                dormant_pages,
            )
        };
        let mut query_self_scores = BoundedHnswQuerySelfScores::new(query);
        // Every partition this statement compares exhaustively reads its
        // bodies through this one open read.
        let mut body_reads = crate::store::RawVectorBodyReads::default();
        let mut merged = Some(Vec::<(RowId, f32)>::new());
        let mut merged_bytes = 0usize;
        let mut graph_trace_candidates = Some(Vec::<(RowId, f32)>::new());
        let mut graph_trace_candidate_bytes = 0usize;
        let mut selected_generations = Vec::<crate::store::VectorGraphGeneration>::new();
        let mut selected_generation_bytes = 0usize;
        let mut union_receipt = None;
        let mut aggregate_hnsw_len = 0usize;
        let mut max_hnsw_ef_search = 0usize;
        let mut exact_candidate_count = 0usize;
        let (mut partition_routes, mut partition_route_bytes) = allocate_partition_route_slots(
            snapshot_sources.len(),
            &mut before_retain,
            &mut release_retained,
        )?;
        let mut first_fallback_reason = None;
        // Every partition with a usable graph is answered by one search over
        // all of their layers together, after the loop.
        let mut union_partitions = Vec::<UnionPartition<'_>>::new();
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
                let partition_started = std::time::Instant::now();
                let partition_pages = contextdb_core::find_attribution::page_meters();
                let partition_evaluations = std::cell::Cell::new(0u64);
                let partition_visited = std::cell::Cell::new(0u64);
                let partition_layers = std::cell::Cell::new(0u64);
                let candidates = if source.candidate_ids_cover_all_live {
                    None
                } else {
                    source.candidate_ids.as_deref()
                };
                let allowed_count = source.allowed_count;
                if groups.filtered && allowed_count == 0 {
                    continue;
                }

                // Snapshot compatibility and the unfiltered per-state count
                // use the shared visibility boundary index. A filtered route
                // materializes this partition's intersection once and reuses
                // it for every graph layer below.
                let eligibility =
                    state.bounded_hnsw_eligibility(snapshot_tx, &mut before_source_entry)?;
                let ef_search = statement_policy.hnsw_ef_search;
                let allowed_count = if groups.filtered {
                    allowed_count
                } else {
                    eligibility.live_count
                };
                if eligibility.entry_count == 0 || allowed_count == 0 {
                    continue;
                }

                max_hnsw_ef_search = max_hnsw_ef_search.max(ef_search);
                let every_graph_point_is_allowed =
                    matches!(layout.quantization, VectorQuantization::F32)
                        && eligibility.entry_count == eligibility.live_count
                        && (!groups.filtered || source.candidate_ids_cover_all_live);

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
                    // A graph this read could not fit in the memory it declared
                    // is a graph this partition does not have. AUTO answers a
                    // partition with no usable graph by comparing its rows, so
                    // it answers this one the same way rather than failing a
                    // statement it can still complete. INDEXED asked for the
                    // graph over the whole scope and is refused, unchanged.
                    Err(QuarantineAwarePreloadError::Store(Error::MemoryBudgetExceeded {
                        ..
                    })) if mode == VectorSearchMode::Auto => {}
                    Err(QuarantineAwarePreloadError::Store(error)) => {
                        return Err(E::from(error));
                    }
                }
                // Every non-empty layer this partition's snapshot selects --
                // sealed base, sealed change and mutable tail, paged or held
                // in memory -- joins the statement's one union search. No
                // partition walks its layers on its own: one with no usable
                // graph (quarantined, an incomplete chain, nothing maintained,
                // or a chain whose layers hold no point) is refused under
                // INDEXED and compared row by row under AUTO.
                // The selected chain has at most base, change and tail. Charge
                // its finite owner vector before capturing any paged root.
                let admitted_layer_bytes = 3usize.saturating_mul(std::mem::size_of::<
                    crate::store::SnapshotCompatibleLayer<'_>,
                >());
                before_retain(admitted_layer_bytes)?;
                let (availability, owned_layers) = state.snapshot_compatible_owned_layers(snapshot);
                if availability == VectorGraphLayerAvailability::Ready && !owned_layers.is_empty() {
                    self.store.mark_sealed_graph_used(state);
                    let charged_bytes = std::mem::size_of::<UnionPartition<'_>>().saturating_add(
                        owned_layers.capacity().saturating_mul(std::mem::size_of::<
                            crate::store::SnapshotCompatibleLayer<'_>,
                        >()),
                    );
                    let layer_bytes =
                        owned_layers.capacity().saturating_mul(std::mem::size_of::<
                            crate::store::SnapshotCompatibleLayer<'_>,
                        >());
                    if let Err(error) = before_retain(std::mem::size_of::<UnionPartition<'_>>()) {
                        drop(owned_layers);
                        release_retained(admitted_layer_bytes);
                        return Err(error);
                    }
                    release_retained(admitted_layer_bytes.saturating_sub(layer_bytes));
                    union_partitions.push(UnionPartition {
                        source_position: partition_position - 1,
                        candidates,
                        allowed_count,
                        live_count: eligibility.live_count,
                        entry_count: eligibility.entry_count,
                        bounded_limits: every_graph_point_is_allowed || candidates.is_some(),
                        layers: owned_layers,
                        charged_bytes,
                        routes_before: partition_routes.len(),
                        started: partition_started,
                        pages: partition_pages,
                    });
                    continue;
                }
                drop(owned_layers);
                release_retained(admitted_layer_bytes);
                let graph_ready = availability == VectorGraphLayerAvailability::Ready;
                if mode == VectorSearchMode::Indexed {
                    return Ok(if graph_ready {
                        BoundedHnswOutcome::Incomplete {
                            reason: HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
                        }
                    } else {
                        BoundedHnswOutcome::Unavailable {
                            reason: BoundedHnswUnavailableReason::MaintainedGraphMissing,
                        }
                    });
                }
                let reason = if graph_ready {
                    "maintained_graph_incomplete"
                } else {
                    "maintained_graph_unavailable"
                };
                let scores = merged.as_mut().ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW merge state was already consumed".to_string(),
                    ))
                })?;
                let partition_compared = bounded_exact_merge_state(
                    index,
                    state,
                    query,
                    k,
                    candidates,
                    snapshot,
                    scores,
                    &mut merged_bytes,
                    &mut body_reads,
                    &mut before_source_entry,
                    &mut before_exact_candidate,
                    &mut before_distance,
                    &mut before_retain,
                    &mut release_retained,
                )?;
                exact_candidate_count = exact_candidate_count.saturating_add(partition_compared);
                note_partition_attribution(
                    "exact",
                    reason,
                    eligibility.live_count,
                    eligibility.entry_count,
                    allowed_count,
                    ef_search,
                    partition_compared,
                    &partition_layers,
                    &partition_visited,
                    &partition_evaluations,
                    partition_started,
                    partition_pages,
                );
                first_fallback_reason.get_or_insert(reason);
                partition_routes.push(VectorPartitionSearchTrace {
                    route: VectorPartitionSearchRoute::Exact,
                    reason,
                });
            }

            if !union_partitions.is_empty() {
                let outcome = self.bounded_union_search(
                    index,
                    query,
                    k,
                    snapshot,
                    snapshot_sources,
                    &mut union_partitions,
                    statement_policy.hnsw_ef_search,
                    allowed_search_limits,
                    &mut query_self_scores,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut before_retain,
                    &mut release_retained,
                )?;
                union_receipt = Some(outcome.receipt);
                let resolved = match outcome.outcome {
                    UnionOutcome::Answered {
                        accepted,
                        accepted_bytes,
                        walked,
                        walked_bytes,
                        contributing,
                        short,
                        short_bytes,
                    } => Some((
                        accepted,
                        accepted_bytes,
                        walked,
                        walked_bytes,
                        contributing,
                        short,
                        short_bytes,
                    )),
                    UnionOutcome::Unresolved(reason) => {
                        if mode == VectorSearchMode::Indexed {
                            return Ok(BoundedHnswOutcome::Incomplete { reason });
                        }
                        None
                    }
                };
                match resolved {
                    Some((
                        accepted,
                        accepted_bytes,
                        walked,
                        walked_bytes,
                        contributing,
                        short,
                        short_bytes,
                    )) => {
                        let merge = (|| -> std::result::Result<(), E> {
                            let trace_candidates =
                                graph_trace_candidates.as_mut().ok_or_else(|| {
                                    E::from(Error::Other(
                                        "bounded HNSW trace candidate state was already consumed"
                                            .to_string(),
                                    ))
                                })?;
                            let required_capacity = trace_candidates
                                .len()
                                .checked_add(walked.len())
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
                            trace_candidates.extend(walked.iter().copied());
                            let scores = merged.as_mut().ok_or_else(|| {
                                E::from(Error::Other(
                                    "bounded HNSW merge state was already consumed".to_string(),
                                ))
                            })?;
                            for candidate in accepted.iter().copied() {
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
                            Ok(())
                        })();
                        drop(accepted);
                        release_retained(accepted_bytes);
                        drop(walked);
                        release_retained(walked_bytes);
                        if let Err(error) = merge {
                            drop(short);
                            release_retained(short_bytes);
                            return Err(error);
                        }
                        let answered =
                            (|| -> std::result::Result<Option<BoundedHnswOutcome>, E> {
                                for (position, partition) in union_partitions.iter().enumerate() {
                                    if short.get(position).copied().unwrap_or(false) {
                                        if mode == VectorSearchMode::Indexed {
                                            return Ok(Some(BoundedHnswOutcome::Incomplete {
                                        reason:
                                            HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
                                    }));
                                        }
                                        before_source_entry()?;
                                        let source = snapshot_sources
                                            .get(partition.source_position)
                                            .ok_or_else(|| {
                                                E::from(Error::Other(
                                                    "bounded HNSW union partition position changed"
                                                        .to_string(),
                                                ))
                                            })?;
                                        let scores = merged.as_mut().ok_or_else(|| {
                                            E::from(Error::Other(
                                                "bounded HNSW merge state was already consumed"
                                                    .to_string(),
                                            ))
                                        })?;
                                        let partition_compared = bounded_exact_merge_state(
                                            index,
                                            &source.state,
                                            query,
                                            k,
                                            partition.candidates,
                                            snapshot,
                                            scores,
                                            &mut merged_bytes,
                                            &mut body_reads,
                                            &mut before_source_entry,
                                            &mut before_exact_candidate,
                                            &mut before_distance,
                                            &mut before_retain,
                                            &mut release_retained,
                                        )?;
                                        exact_candidate_count = exact_candidate_count
                                            .saturating_add(partition_compared);
                                        note_partition_attribution(
                                            "exact",
                                            "maintained_graph_incomplete",
                                            partition.live_count,
                                            partition.entry_count,
                                            partition.allowed_count,
                                            statement_policy.hnsw_ef_search,
                                            partition_compared,
                                            &Cell::new(partition.layers.len() as u64),
                                            &Cell::new(0),
                                            &Cell::new(0),
                                            partition.started,
                                            partition.pages,
                                        );
                                        first_fallback_reason
                                            .get_or_insert("maintained_graph_incomplete");
                                        partition_routes.insert(
                                            partition.routes_before + position,
                                            VectorPartitionSearchTrace {
                                                route: VectorPartitionSearchRoute::Exact,
                                                reason: "maintained_graph_incomplete",
                                            },
                                        );
                                        continue;
                                    }
                                    for (layer_position, layer) in
                                        partition.layers.iter().enumerate()
                                    {
                                        aggregate_hnsw_len =
                                            aggregate_hnsw_len.saturating_add(layer.len());
                                        let Some(generation) = layer.identity else {
                                            continue;
                                        };
                                        if !contributing.contains(&(position, layer_position)) {
                                            continue;
                                        }
                                        let required_capacity = selected_generations
                                            .len()
                                            .checked_add(1)
                                            .ok_or_else(|| {
                                                E::from(Error::Other(
                                            "bounded HNSW selected-generation length overflow"
                                                .to_string(),
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
                                    note_partition_attribution(
                                        "indexed",
                                        "maintained_graph_ready",
                                        partition.live_count,
                                        partition.entry_count,
                                        partition.allowed_count,
                                        statement_policy.hnsw_ef_search,
                                        0,
                                        &Cell::new(partition.layers.len() as u64),
                                        &Cell::new(0),
                                        &Cell::new(0),
                                        partition.started,
                                        partition.pages,
                                    );
                                    // In scope order, among the routes the loop
                                    // recorded, inside the slots already charged.
                                    partition_routes.insert(
                                        partition.routes_before + position,
                                        VectorPartitionSearchTrace {
                                            route: VectorPartitionSearchRoute::Indexed,
                                            reason: "maintained_graph_ready",
                                        },
                                    );
                                }
                                Ok(None)
                            })();
                        drop(short);
                        release_retained(short_bytes);
                        if let Some(outcome) = answered? {
                            return Ok(outcome);
                        }
                    }
                    None => {
                        // AUTO answers what the one search could not resolve by
                        // comparing those partitions' rows, inside the same
                        // budget, through the statement's one open body read.
                        for (position, partition) in union_partitions.iter().enumerate() {
                            before_source_entry()?;
                            let source = snapshot_sources
                                .get(partition.source_position)
                                .ok_or_else(|| {
                                    E::from(Error::Other(
                                        "bounded HNSW union partition position changed".to_string(),
                                    ))
                                })?;
                            let scores = merged.as_mut().ok_or_else(|| {
                                E::from(Error::Other(
                                    "bounded HNSW merge state was already consumed".to_string(),
                                ))
                            })?;
                            let partition_compared = bounded_exact_merge_state(
                                index,
                                &source.state,
                                query,
                                k,
                                partition.candidates,
                                snapshot,
                                scores,
                                &mut merged_bytes,
                                &mut body_reads,
                                &mut before_source_entry,
                                &mut before_exact_candidate,
                                &mut before_distance,
                                &mut before_retain,
                                &mut release_retained,
                            )?;
                            exact_candidate_count =
                                exact_candidate_count.saturating_add(partition_compared);
                            note_partition_attribution(
                                "exact",
                                "maintained_graph_incomplete",
                                partition.live_count,
                                partition.entry_count,
                                partition.allowed_count,
                                statement_policy.hnsw_ef_search,
                                partition_compared,
                                &Cell::new(partition.layers.len() as u64),
                                &Cell::new(0),
                                &Cell::new(0),
                                partition.started,
                                partition.pages,
                            );
                            first_fallback_reason.get_or_insert("maintained_graph_incomplete");
                            partition_routes.insert(
                                partition.routes_before + position,
                                VectorPartitionSearchTrace {
                                    route: VectorPartitionSearchRoute::Exact,
                                    reason: "maintained_graph_incomplete",
                                },
                            );
                        }
                    }
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
            let trace_routes = std::mem::take(&mut partition_routes);
            let trace_route_bytes = std::mem::take(&mut partition_route_bytes);
            let trace = if aggregate_hnsw_len == 0 {
                drop(trace_generations);
                release_retained(trace_generation_bytes);
                bounded_exact_route_trace(
                    index,
                    trace_routes,
                    trace_route_bytes,
                    first_fallback_reason.unwrap_or("maintained_graph_unavailable"),
                    union_receipt,
                    &mut before_retain,
                    &mut release_retained,
                )?
            } else {
                bounded_hnsw_trace(
                    index,
                    aggregate_hnsw_len,
                    graph_trace_candidates.as_deref().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded HNSW trace candidates disappeared before publication"
                                .to_string(),
                        ))
                    })?,
                    rows,
                    exact_candidate_count,
                    max_hnsw_ef_search,
                    trace_generations,
                    union_receipt,
                    trace_generation_bytes,
                    trace_routes,
                    trace_route_bytes,
                    &mut before_source_entry,
                    &mut before_retain,
                    &mut release_retained,
                )?
            };
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
        let union_bytes = union_partitions
            .iter()
            .map(|partition| partition.charged_bytes)
            .fold(0usize, usize::saturating_add);
        drop(union_partitions);
        release_retained(union_bytes);
        match searched {
            Ok(outcome @ BoundedHnswOutcome::Complete(_)) => Ok(outcome),
            Ok(outcome) => {
                drop(merged.take());
                release_retained(merged_bytes);
                drop(graph_trace_candidates.take());
                release_retained(graph_trace_candidate_bytes);
                release_retained(selected_generation_bytes);
                release_retained(partition_route_bytes);
                Ok(outcome)
            }
            Err(error) => {
                drop(merged.take());
                release_retained(merged_bytes);
                drop(graph_trace_candidates.take());
                release_retained(graph_trace_candidate_bytes);
                release_retained(selected_generation_bytes);
                release_retained(partition_route_bytes);
                Err(error)
            }
        }
    }

    /// One best-first search over every admitted layer of every partition in
    /// `partitions`, with one frontier, one result heap and one candidate
    /// budget, followed by re-scoring each candidate from its own partition's
    /// stored vector. Expansion work is the budget's, not the scope's.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::drop_non_drop)] // Explicitly end callback borrows before partition publication.
    fn bounded_union_search<E>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        snapshot: SnapshotId,
        sources: &[BoundedSnapshotVectorSource],
        partitions: &mut [UnionPartition<'_>],
        ef_search: usize,
        allowed_search_limits: HnswAllowedSearchLimits,
        query_self_scores: &mut BoundedHnswQuerySelfScores<'_>,
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<UnionSearchResult, E>
    where
        E: From<Error>,
    {
        let serving_layer0_degree = partitions
            .iter()
            .flat_map(|partition| partition.layers.iter())
            .filter_map(|layer| layer.graph())
            .map(|graph| graph.layer0_max_degree())
            .max()
            .unwrap_or(1);
        let budget = crate::hnsw::union_candidate_budget(ef_search, k, serving_layer0_degree);
        let limits = if partitions.iter().any(|partition| partition.bounded_limits) {
            allowed_search_limits
        } else {
            HnswAllowedSearchLimits {
                max_visited_nodes: usize::MAX,
                max_vector_evaluations: usize::MAX,
            }
        };
        let layer_count = partitions
            .iter()
            .map(|partition| partition.layers.len())
            .sum::<usize>();
        let layer_bytes = layer_count
            .saturating_mul(std::mem::size_of::<crate::hnsw::HnswUnionLayer<'_>>())
            .saturating_add(layer_count.saturating_mul(std::mem::size_of::<(usize, usize)>()));
        before_retain(layer_bytes)?;
        let mut layers = Vec::with_capacity(layer_count);
        // Which (partition, layer) each union layer is.
        let mut layer_owner = Vec::with_capacity(layer_count);
        for (position, partition) in partitions.iter().enumerate() {
            for (layer_position, layer) in partition.layers.iter().enumerate() {
                // Every layer is held until this search ends; a released one
                // here would be a search over fewer layers than its snapshot
                // selected, so it is refused rather than skipped.
                let Some(graph) = layer.graph() else {
                    drop(layers);
                    drop(layer_owner);
                    release_retained(layer_bytes);
                    return Err(E::from(Error::Other(
                        "a union search layer was released before its search ran".to_string(),
                    )));
                };
                layers.push(crate::hnsw::HnswUnionLayer {
                    graph,
                    partition: position,
                    allowed_count: partition.allowed_count,
                    live_count: partition.live_count,
                    seed_rows: partition
                        .candidates
                        .map(crate::hnsw::union::UnionSeedRows::Slice),
                });
                layer_owner.push((position, layer_position));
            }
        }
        // A superseded point can have a better graph distance than its
        // current stored vector. When a graph row has more than one version,
        // certify a possible stop from visible stored scores instead.
        let graph_scores_are_current = partitions
            .iter()
            .all(|partition| partition.entry_count == partition.live_count);
        let started = std::time::Instant::now();
        let retained = std::cell::RefCell::new(&mut *before_retain);
        let released = std::cell::RefCell::new(&mut *release_retained);
        let searched = crate::hnsw::search_union_allowed_bounded(
            &layers,
            query,
            k,
            ef_search,
            budget,
            limits,
            graph_scores_are_current,
            true,
            query_self_scores,
            |position, row_id| {
                partitions.get(position).is_some_and(|partition| {
                    partition
                        .candidates
                        .is_none_or(|candidates| candidates.binary_search(&row_id.0).is_ok())
                        && sources
                            .get(partition.source_position)
                            .is_some_and(|source| {
                                source.state.directory_has_visible_row(row_id, snapshot)
                            })
                })
            },
            |position, row_id, source_entry, distance| {
                let partition = &partitions[position];
                let state = &sources[partition.source_position].state;
                state.bounded_score_visible_candidate(
                    index,
                    row_id,
                    snapshot,
                    query,
                    None,
                    source_entry,
                    distance,
                    &mut |bytes| (*retained.borrow_mut())(bytes),
                    &mut |bytes| (*released.borrow_mut())(bytes),
                )
            },
            &mut *before_source_entry,
            &mut *before_distance,
            |event| apply_hnsw_scratch_acquire(event, &mut |bytes| (*retained.borrow_mut())(bytes)),
            |event| apply_hnsw_scratch_release(event, &mut |bytes| (*released.borrow_mut())(bytes)),
        );
        drop(retained);
        drop(released);
        drop(layers);
        // The walk is over. A held tail's read lock goes back now, before
        // resolving takes each partition's publication lock: a publication
        // waiting for that tail's write side already holds it.
        for partition in partitions.iter_mut() {
            for layer in &mut partition.layers {
                layer.release_tail();
            }
        }
        let partitions = &*partitions;
        let searched = match searched {
            Ok(searched) => searched,
            Err(error) => {
                drop(layer_owner);
                release_retained(layer_bytes);
                return Err(error);
            }
        };
        #[cfg(feature = "test-seams")]
        crate::hnsw::record_union_search(crate::hnsw::UnionSearchReceipt {
            budget: searched.budget,
            resolved_ef_search: ef_search,
            admission_evaluations: searched.admission_evaluations,
            expansion_evaluations: searched.expansion_evaluations,
            layers_admitted: searched.layers_admitted,
            partitions: partitions.len(),
            frontier_exhausted: searched.frontier_exhausted,
            terminal: searched.terminal.as_str(),
            below_admission_floor: searched.below_admission_floor.is_some(),
            expansion_cap: searched.expansion_cap,
            // What one expanded point can reach: the widest admitted
            // graph's layer-0 maximum degree, read from the graphs.
            layer_zero_degree: serving_layer0_degree,
        });
        if contextdb_core::find_attribution::enabled() {
            use contextdb_core::find_attribution::count;
            count("union_searches", 1);
            count("union_candidate_budget", searched.budget as u64);
            count(
                "union_admission_evaluations",
                searched.admission_evaluations as u64,
            );
            count(
                "union_expansion_evaluations",
                searched.expansion_evaluations as u64,
            );
            count("union_layers_admitted", searched.layers_admitted as u64);
            count(
                match searched.terminal {
                    crate::hnsw::UnionTerminal::AnswerComplete => "union_terminal_answer_complete",
                    crate::hnsw::UnionTerminal::FrontierExhausted => {
                        "union_terminal_frontier_exhausted"
                    }
                    crate::hnsw::UnionTerminal::Refused => "union_terminal_refused",
                },
                1,
            );
            count(
                "union_frontier_exhausted",
                u64::from(searched.frontier_exhausted),
            );
            count("union_partitions", partitions.len() as u64);
            count(
                "union_walk_micros",
                started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            );
        }
        let receipt = VectorSearchBudgetReceipt {
            global_candidate_budget: searched.budget,
            admission_evaluations: searched.admission_evaluations,
            expansion_evaluations: searched.expansion_evaluations,
            layers_admitted: searched.layers_admitted,
            budget_terminal: searched.terminal.as_str(),
        };
        let resolved = self.resolve_union_candidates(
            index,
            query,
            k,
            snapshot,
            sources,
            partitions,
            &searched,
            &layer_owner,
            before_source_entry,
            before_distance,
            before_retain,
            release_retained,
        );
        let result_bytes = searched.retained_bytes;
        drop(searched);
        release_retained(result_bytes);
        drop(layer_owner);
        release_retained(layer_bytes);
        resolved.map(|outcome| UnionSearchResult {
            receipt: if matches!(outcome, UnionOutcome::Unresolved(_)) {
                VectorSearchBudgetReceipt {
                    budget_terminal: "refused",
                    ..receipt
                }
            } else {
                receipt
            },
            outcome,
        })
    }

    /// Re-score the union's kept rows from their stored vectors, nearest by
    /// walk distance first, until the `k` best accepted rows cannot be beaten
    /// by the next row's walk distance. On rows whose graph point is the
    /// version the snapshot reads, the walk distance is the stored score, so
    /// this stops after exactly `k`. A row the walk reached through a
    /// superseded point, or through a coarser representation, scores lower
    /// than its walk distance promised, and the next row is scored in its
    /// place instead of the page going out short or out of order.
    #[allow(clippy::too_many_arguments)]
    fn resolve_union_candidates<E>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        snapshot: SnapshotId,
        sources: &[BoundedSnapshotVectorSource],
        partitions: &[UnionPartition<'_>],
        searched: &crate::hnsw::HnswUnionSearchResult,
        layer_owner: &[(usize, usize)],
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
    ) -> std::result::Result<UnionOutcome, E>
    where
        E: From<Error>,
    {
        // A walk over F32 layers reaches a row at the distance its stored
        // vector scores, so the next row's walk distance bounds what it can
        // still score and scoring stops once the page cannot change. A
        // quantized layer's walk distance is its code's, which sits on either
        // side of the score of the vector the row stores: every kept row is
        // scored, and the page is the best of those scores.
        let walk_distance_is_score = matches!(
            self.store
                .index_layout(index)
                .map_err(E::from)?
                .quantization,
            VectorQuantization::F32
        ) && partitions
            .iter()
            .all(|partition| partition.entry_count == partition.live_count);
        if searched.below_admission_floor.is_some() {
            return Ok(UnionOutcome::Unresolved(
                HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
            ));
        }
        if let HnswAllowedSearchStatus::Incomplete(reason) = searched.status {
            return Ok(UnionOutcome::Unresolved(reason));
        }
        if searched.terminal == crate::hnsw::UnionTerminal::Refused {
            return Ok(UnionOutcome::Unresolved(
                HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
            ));
        }
        // A partition whose graph handed back fewer candidates than it needs
        // cannot be answered from the graph; it is compared exhaustively,
        // as the per-partition route compared a partition whose walk fell
        // short. Only the candidate-cap proof seam produces one today.
        let short_bytes = partitions.len().saturating_mul(std::mem::size_of::<bool>());
        before_retain(short_bytes)?;
        #[allow(unused_mut)]
        let mut short = vec![false; partitions.len()];
        #[cfg(feature = "test-seams")]
        for (position, partition) in partitions.iter().enumerate() {
            if let Some(source) = sources.get(partition.source_position)
                && let Some(cap) = self
                    .store
                    .graph_candidate_caps()
                    .graph_candidate_cap(index, Some(&source.partition_key))
            {
                short[position] = cap < k.min(partition.allowed_count);
            }
        }
        let required = partitions
            .iter()
            .zip(&short)
            .filter(|(_, short)| !**short)
            .map(|(partition, _)| partition.allowed_count)
            .fold(0usize, usize::saturating_add)
            .min(k);
        // Each partition's candidates are scored against the serving pages
        // its walk already read, opened when its first candidate is scored.
        // Beside it, how many of that partition's candidates were considered.
        let slot_bytes = partitions.len().saturating_mul(std::mem::size_of::<(
            Option<Option<crate::store::SealedPageScoring>>,
            usize,
        )>());
        before_retain(slot_bytes)?;
        let mut scoring = partitions
            .iter()
            .map(|_| (None::<Option<crate::store::SealedPageScoring>>, 0usize))
            .collect::<Vec<_>>();
        let mut accepted = Vec::<(RowId, f32)>::new();
        let mut accepted_bytes = 0usize;
        // Every row the walk handed back, for the route trace: scored rows at
        // their stored score, the rest at the score their walk distance gives.
        let mut walked = Vec::<(RowId, f32)>::new();
        let mut walked_bytes = 0usize;
        let mut contributing = Vec::<(usize, usize)>::new();
        let resolved = (|| -> std::result::Result<(), E> {
            let mut scoring_open = true;
            for (position, candidate) in searched.candidates.iter().enumerate() {
                before_source_entry()?;
                // A short partition's candidates are neither scored nor
                // reported: its rows are compared instead.
                if short.get(candidate.partition).copied().unwrap_or(false) {
                    continue;
                }
                if scoring_open
                    && walk_distance_is_score
                    && accepted.len() >= required
                    && walk_cannot_beat(&accepted, required, candidate.distance)
                {
                    scoring_open = false;
                }
                let required_capacity = walked.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW union walked-row count overflow".to_string(),
                    ))
                })?;
                grow_vector_copy(
                    &mut walked,
                    required_capacity,
                    Some(&mut walked_bytes),
                    "HNSW union walked rows",
                    before_source_entry,
                    before_retain,
                    release_retained,
                )?;
                let walk_score = 1.0 - candidate.distance;
                if !scoring_open {
                    if !walked.iter().any(|(id, _)| *id == candidate.row_id) {
                        walked.push((
                            candidate.row_id,
                            candidate.resolved_score.unwrap_or(walk_score),
                        ));
                    }
                    continue;
                }
                let partition = partitions.get(candidate.partition).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW union partition position changed".to_string(),
                    ))
                })?;
                let source = sources.get(partition.source_position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW union partition position changed".to_string(),
                    ))
                })?;
                let state = &source.state;
                let (page_scoring, considered) = &mut scoring[candidate.partition];
                *considered += 1;
                #[cfg(feature = "test-seams")]
                if self
                    .store
                    .graph_candidate_caps()
                    .graph_candidate_cap(index, Some(&source.partition_key))
                    .is_some_and(|cap| *considered > cap)
                {
                    continue;
                }
                if !state.directory_has_visible_row(candidate.row_id, snapshot)
                    || partition.candidates.is_some_and(|candidates| {
                        candidates.binary_search(&candidate.row_id.0).is_err()
                    })
                {
                    continue;
                }
                // Layer identities can overlap. Score each logical row once.
                if searched.candidates[..position].iter().any(|earlier| {
                    earlier.partition == candidate.partition && earlier.row_id == candidate.row_id
                }) {
                    continue;
                }
                if page_scoring.is_none() && candidate.resolved_score.is_none() {
                    *page_scoring = Some(state.sealed_page_scoring(
                        snapshot,
                        query,
                        before_source_entry,
                        before_retain,
                        release_retained,
                    )?);
                }
                let score = match candidate.resolved_score {
                    Some(score) => Some(score),
                    None => state.bounded_score_visible_candidate(
                        index,
                        candidate.row_id,
                        snapshot,
                        query,
                        page_scoring.as_mut().and_then(Option::as_mut),
                        before_source_entry,
                        before_distance,
                        before_retain,
                        release_retained,
                    )?,
                };
                let Some(score) = score else {
                    walked.push((candidate.row_id, walk_score));
                    continue;
                };
                walked.push((candidate.row_id, score));
                let required_capacity = accepted.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded HNSW union accepted-row count overflow".to_string(),
                    ))
                })?;
                grow_vector_copy(
                    &mut accepted,
                    required_capacity,
                    Some(&mut accepted_bytes),
                    "HNSW union accepted rows",
                    before_source_entry,
                    before_retain,
                    release_retained,
                )?;
                accepted.push((candidate.row_id, score));
                let owner = layer_owner.get(candidate.layer).copied();
                if let Some(owner) = owner
                    && !contributing.contains(&owner)
                {
                    contributing.push(owner);
                }
            }
            Ok(())
        })();
        drop(scoring);
        release_retained(slot_bytes);
        if let Err(error) = resolved {
            drop(accepted);
            release_retained(accepted_bytes);
            drop(walked);
            release_retained(walked_bytes);
            drop(short);
            release_retained(short_bytes);
            return Err(error);
        }
        if accepted.len() < required {
            drop(accepted);
            release_retained(accepted_bytes);
            drop(walked);
            release_retained(walked_bytes);
            drop(short);
            release_retained(short_bytes);
            return Ok(UnionOutcome::Unresolved(
                HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
            ));
        }
        Ok(UnionOutcome::Answered {
            accepted,
            accepted_bytes,
            walked,
            walked_bytes,
            contributing,
            short,
            short_bytes,
        })
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
                candidate_ids: None,
                point_load: false,
                candidate_ids_cover_all_live: false,
                allowed_count: 0,
                live_count: 0,
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
                        candidate_ids: None,
                        point_load: false,
                        candidate_ids_cover_all_live: false,
                        allowed_count: 0,
                        live_count: 0,
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
                candidate_ids: None,
                point_load: false,
                candidate_ids_cover_all_live: false,
                allowed_count: 0,
                live_count: 0,
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
                            candidate_ids: None,
                            point_load: false,
                            candidate_ids_cover_all_live: false,
                            allowed_count: 0,
                            live_count: 0,
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
                            candidate_ids: None,
                            point_load: false,
                            candidate_ids_cover_all_live: false,
                            allowed_count: 0,
                            live_count: 0,
                        })
                })
                .collect());
        };

        let mut selected = BTreeMap::<VectorPartitionKey, Arc<crate::store::IndexState>>::new();
        for raw_row_id in candidates.iter() {
            let row_id = RowId(raw_row_id);
            let Some(partition_key) = self.store.try_current_partition_for_row(index, row_id)?
            else {
                return self.all_partition_sources(index);
            };
            let Some(state) = self.store.try_partition_state(index, &partition_key) else {
                // Membership can name a state whose final retained version
                // was reclaimed. It cannot safely narrow this snapshot, so
                // use every remaining state and keep the candidate bitmap as
                // the row-level authorization/filter boundary.
                return self.all_partition_sources(index);
            };
            if !state.try_directory_has_visible_row(row_id, snapshot)? {
                return self.all_partition_sources(index);
            }
            selected.entry(partition_key).or_insert(state);
        }

        Ok(selected
            .into_iter()
            .map(|(partition_key, state)| BoundedSnapshotVectorSource {
                partition_key,
                state,
                candidate_ids: None,
                point_load: false,
                candidate_ids_cover_all_live: false,
                allowed_count: 0,
                live_count: 0,
            })
            .collect())
    }

    /// `stream_when_refused` is set for AUTO's exhaustive work: a partition
    /// whose vectors the database's memory cannot hold whole is then compared
    /// by reading each stored vector through its own point read, one at a
    /// time, instead of being refused. An exhaustive search the caller asked
    /// for outright keeps its whole-partition load and its typed refusal.
    #[allow(clippy::too_many_arguments)]
    fn brute_force_search_sources(
        &self,
        index: &VectorIndexRef,
        sources: &[BoundedSnapshotVectorSource],
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        stream_when_refused: bool,
    ) -> Result<SearchScores> {
        let mut scored = SearchScores::new(index.clone(), self._accountant.clone()).for_exact();
        // Directory metadata is snapshot-visible and already narrowed to the
        // selected partitions and authorized identities; unrelated data never
        // contributes to this admission.
        let capacity = k.min(Self::aggregate_authorized_source_count(
            sources, candidates, snapshot,
        )?);
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
                for source in sources.iter() {
                    if !source
                        .state
                        .try_directory_has_visible_row(row_id, snapshot)?
                    {
                        continue;
                    }
                    if let Some(score) = source.state.score_visible_candidate(
                        index, row_id, snapshot, query,
                        // The exhaustive route reads each row's own body.
                        None,
                    )? {
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
            if source.state.durable_membership_source().is_some() {
                self.stream_source_bodies(index, source, query, snapshot, &mut consider)?;
                continue;
            }
            match source.state.ensure_raw_vectors_loaded() {
                Ok(()) => {}
                Err(Error::MemoryBudgetExceeded { .. })
                    if stream_when_refused && source.state.streams_raw_bodies() =>
                {
                    self.stream_source_bodies(index, source, query, snapshot, &mut consider)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
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
                    #[cfg(feature = "test-seams")]
                    observe_native_vector_score_for_test();
                    consider((entry.row_id, entry.vector.cosine_similarity(query)))?;
                }
                Ok::<(), Error>(())
            })?;
        }
        sort_vector_scores(&mut scored);
        scored.truncate(k);
        Ok(scored)
    }

    /// Compare every row one partition's snapshot shows by reading each
    /// row's own stored vector through a point read, admitting each body
    /// against the database's memory before it is read and returning it
    /// straight after, so the comparison never needs room for the partition.
    fn stream_source_bodies(
        &self,
        index: &VectorIndexRef,
        source: &BoundedSnapshotVectorSource,
        query: &[f32],
        snapshot: SnapshotId,
        consider: &mut impl FnMut((RowId, f32)) -> Result<()>,
    ) -> Result<()> {
        let accountant = self._accountant.clone();
        let mut before_retain = |bytes: usize| -> Result<()> {
            accountant.try_allocate_for(
                bytes,
                "vector_search",
                "exhaustive_body_read",
                "Reduce the query scope, or raise MEMORY_LIMIT.",
            )
        };
        let mut release_retained = |bytes: usize| accountant.release(bytes);
        let mut bodies = crate::store::RawVectorBodyReads::default();
        if source.state.durable_membership_source().is_some() {
            return source.state.bounded_visit_visible_ids_charged(
                snapshot,
                || Ok::<(), Error>(()),
                |bytes| {
                    accountant.try_allocate_for(
                        bytes,
                        "vector_search",
                        "membership_window",
                        "Reduce the query scope, or raise MEMORY_LIMIT.",
                    )
                },
                |bytes| accountant.release(bytes),
                |row_id| {
                    if let Some(score) = source.state.bounded_score_stored_body(
                        index,
                        row_id,
                        snapshot,
                        query,
                        &mut bodies,
                        &mut || Ok::<(), Error>(()),
                        &mut || Ok(()),
                        &mut before_retain,
                        &mut release_retained,
                    )? {
                        consider((row_id, score))?;
                    }
                    Ok(())
                },
            );
        }
        let mut position = 0usize;
        while let Some(visible) = source.state.visible_directory_row_at(position, snapshot) {
            position = position
                .checked_add(1)
                .ok_or_else(|| Error::Other("exhaustive vector position overflow".to_string()))?;
            let Some(row_id) = visible else {
                continue;
            };
            if let Some(score) = source.state.bounded_score_stored_body(
                index,
                row_id,
                snapshot,
                query,
                &mut bodies,
                &mut || Ok::<(), Error>(()),
                &mut || Ok(()),
                &mut before_retain,
                &mut release_retained,
            )? {
                consider((row_id, score))?;
            }
        }
        Ok(())
    }

    fn aggregate_resident_authorized_source_count(
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

    fn aggregate_authorized_source_count(
        sources: &[BoundedSnapshotVectorSource],
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<usize> {
        if let Some(candidates) = candidates {
            let mut count = 0usize;
            for id in candidates.iter() {
                for source in sources {
                    if source
                        .state
                        .try_directory_has_visible_row(RowId(id), snapshot)?
                    {
                        count += 1;
                        break;
                    }
                }
            }
            return Ok(count);
        }
        sources.iter().try_fold(0usize, |count, source| {
            count
                .checked_add(
                    source
                        .state
                        .try_directory_visible_entry_count(snapshot, None)?,
                )
                .ok_or_else(|| Error::Other("vector authorized count overflow".into()))
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
            Self::aggregate_authorized_source_count(&sources, candidates, snapshot)
        })
    }

    /// Compatibility entrypoint that admits and releases its own candidate
    /// grouping. Engine execution keeps one grouping across all vector stages.
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
        let groups = self.bounded_candidate_groups(
            index,
            candidates,
            snapshot,
            &mut before_source,
            || Ok(()),
            &mut acquire,
            &mut release,
        )?;
        let result = self.bounded_rank_candidate_k_grouped(index, limit, &groups, before_source);
        release(groups.into_retained_bytes());
        result
    }

    /// Resolve the global rank pool from the same selected partition policies
    /// as the graph walk, charging source selection to the active reader.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_rank_candidate_k_grouped<E>(
        &self,
        index: &VectorIndexRef,
        limit: usize,
        groups: &BoundedVectorCandidateGroups,
        mut before_source: impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<usize, E>
    where
        E: From<Error>,
    {
        before_source()?;
        let layout = self.store.index_layout(index).map_err(E::from)?;
        let mut breadth = limit;
        let mut allowed = 0usize;
        for source in &groups.sources {
            before_source()?;
            let count = source.allowed_count;
            if count == 0 {
                continue;
            }
            allowed = allowed.saturating_add(count);
            breadth = breadth.max(
                layout
                    .resolve_policy(
                        source.state.bounded_directory_visible_entry_count(
                            groups.snapshot,
                            None,
                            &mut before_source,
                        )?,
                        limit,
                    )
                    .hnsw_ef_search,
            );
        }
        Ok(breadth.min(allowed).max(limit))
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
                    VectorSearchDebugTrace::brute_force(
                        &index,
                        &[],
                        "empty_limit",
                        None,
                        Vec::new(),
                    ),
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
                    VectorSearchDebugTrace::brute_force(
                        &index,
                        &[],
                        "empty_candidates",
                        None,
                        Vec::new(),
                    ),
                ));
            }
            let sources = self.selected_partition_sources(&index, candidates, snapshot)?;
            if sources.is_empty() {
                return Ok((
                    SearchScores::new(index.clone(), self._accountant.clone()),
                    VectorSearchDebugTrace::brute_force(
                        &index,
                        &[],
                        "empty_index",
                        None,
                        Vec::new(),
                    ),
                ));
            }
            let exact = |reason: &'static str,
                         hnsw_len: Option<usize>|
             -> Result<(SearchScores, VectorSearchDebugTrace)> {
                // AUTO's exhaustive work streams what the database's memory
                // cannot hold whole; EXACT keeps its whole-partition load.
                let rows = self.brute_force_search_sources(
                    &index,
                    &sources,
                    query,
                    k,
                    candidates,
                    snapshot,
                    mode == contextdb_core::VectorSearchMode::Auto,
                )?;
                let partition_routes = sources
                    .iter()
                    .map(|source| {
                        Ok((source
                            .state
                            .try_directory_visible_entry_count(snapshot, candidates)?
                            != 0)
                            .then_some(VectorPartitionSearchTrace {
                                route: VectorPartitionSearchRoute::Exact,
                                reason,
                            }))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect();
                let trace = VectorSearchDebugTrace::brute_force(
                    &index,
                    &rows,
                    reason,
                    hnsw_len,
                    partition_routes,
                );
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
                    Self::aggregate_authorized_source_count(&sources, candidates, snapshot)?;
                if authorized_count < layout.effective_auto_index_at() {
                    return exact(
                        if matches!(layout.quantization, VectorQuantization::F32) {
                            "aggregate_below_auto_index_at"
                        } else {
                            "quantized_exact_search"
                        },
                        None,
                    );
                }
            }

            // One search over every non-empty layer of every admitted
            // partition -- the one a SQL find runs, with the same snapshot
            // pinning, the same per-partition fallback for a partition with no
            // usable graph, the same current-row and candidate recheck and the
            // same memory charges. No owner door walks a partition or a layer
            // on its own.
            // A search pauses here, inside its bulk read and before it has
            // charged anything, so a test can hold it in progress.
            #[cfg(feature = "test-seams")]
            self.store
                .pause_registry()
                .maybe_pause(&index, crate::test_seam::PauseWindow::Search);
            let accountant = self._accountant.clone();
            let mut before_retain = |bytes: usize| -> Result<()> {
                accountant.try_allocate_for(
                    bytes,
                    "vector_search",
                    "graph_scratch",
                    "Reduce EF_SEARCH or query scope, or raise MEMORY_LIMIT.",
                )
            };
            let mut release_retained = |bytes: usize| accountant.release(bytes);
            // The authorized candidates, as the sorted identity list the
            // grouping places into partitions, charged while it is held.
            let candidate_ids = match candidates {
                Some(candidates) => {
                    let len = usize::try_from(candidates.len())
                        .map_err(|_| Error::Other("vector candidate count overflow".to_string()))?;
                    let bytes = len
                        .checked_mul(std::mem::size_of::<u64>())
                        .ok_or_else(|| Error::Other("vector candidate count overflow".into()))?;
                    before_retain(bytes)?;
                    let mut ids = Vec::<u64>::new();
                    if ids.try_reserve_exact(len).is_err() {
                        release_retained(bytes);
                        return Err(Error::Other("vector candidate allocation failed".into()));
                    }
                    ids.extend(candidates.iter());
                    Some((ids, bytes))
                }
                None => None,
            };
            let searched = (|| -> Result<BoundedHnswOutcome> {
                let groups = self.bounded_candidate_groups(
                    &index,
                    candidate_ids.as_ref().map(|(ids, _)| ids.as_slice()),
                    snapshot,
                    || Ok::<(), Error>(()),
                    || Ok(()),
                    &mut before_retain,
                    &mut release_retained,
                )?;
                let outcome = self.bounded_hnsw_search_grouped(
                    &index,
                    query,
                    k,
                    &groups,
                    mode,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: usize::MAX,
                        max_vector_evaluations: usize::MAX,
                    },
                    || Ok::<(), Error>(()),
                    || Ok(()),
                    || Ok(()),
                    &mut before_retain,
                    &mut release_retained,
                );
                release_retained(groups.into_retained_bytes());
                outcome
            })();
            if let Some((ids, bytes)) = candidate_ids {
                drop(ids);
                release_retained(bytes);
            }
            let unavailable = |filtered_route_missing: bool| {
                if filtered_route_missing {
                    Error::VectorFilteredRouteUnavailable {
                        predicate_columns: Vec::new(),
                        index: index.clone(),
                    }
                } else {
                    Error::VectorIndexedRouteUnavailable {
                        index: index.clone(),
                    }
                }
            };
            match searched? {
                BoundedHnswOutcome::CompleteEmpty => Ok((
                    SearchScores::new(index.clone(), self._accountant.clone()),
                    VectorSearchDebugTrace::brute_force(
                        &index,
                        &[],
                        "empty_index",
                        None,
                        Vec::new(),
                    ),
                )),
                BoundedHnswOutcome::Complete(BoundedHnswResult {
                    mut rows,
                    trace,
                    retained_bytes,
                }) => {
                    // The rows keep their charge and carry it out; the trace
                    // leaves bounded accounting here, as the SQL find's does.
                    let trace_bytes = trace.retained_bytes();
                    let trace = trace.into_trace();
                    release_retained(trace_bytes);
                    rows.truncate(k);
                    let scores = SearchScores {
                        rows,
                        credit: ScoreCredit {
                            budget: self._accountant.clone(),
                            bytes: retained_bytes.saturating_sub(trace_bytes),
                        },
                        index: index.clone(),
                        exact_budget: false,
                    };
                    Ok((scores, trace))
                }
                BoundedHnswOutcome::ExactRequired { trace, .. } => {
                    let trace_bytes = trace.retained_bytes();
                    let mut trace = trace.into_trace();
                    release_retained(trace_bytes);
                    let rows = self.brute_force_search_sources(
                        &index,
                        &sources,
                        query,
                        k,
                        candidates,
                        snapshot,
                        mode == contextdb_core::VectorSearchMode::Auto,
                    )?;
                    trace.final_row_ids = rows.iter().map(|(row_id, _)| *row_id).collect();
                    #[cfg(feature = "test-seams")]
                    observe_exact_scores_for_test(
                        ExactScorePhaseForTest::Scored,
                        rows.rows.capacity(),
                        rows.rows.len(),
                    );
                    Ok((rows, trace))
                }
                BoundedHnswOutcome::Unavailable { .. } => {
                    if mode == contextdb_core::VectorSearchMode::Indexed {
                        return Err(unavailable(false));
                    }
                    exact("maintained_graph_unavailable", None)
                }
                BoundedHnswOutcome::Incomplete { .. } => {
                    if mode == contextdb_core::VectorSearchMode::Indexed {
                        return Err(unavailable(candidates.is_some()));
                    }
                    exact("maintained_graph_incomplete", None)
                }
            }
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
                Self::aggregate_resident_authorized_source_count(&sources, candidates, snapshot);
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

    #[doc(hidden)]
    pub fn try_hnsw_eligible_without_build(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        self.store.with_bulk_read(|| {
            let sources = self.selected_partition_sources(index, candidates, snapshot)?;
            if Self::aggregate_authorized_source_count(&sources, candidates, snapshot)? == 0 {
                return Ok(false);
            }
            for source in &sources {
                if source
                    .state
                    .try_directory_visible_entry_count(snapshot, candidates)?
                    != 0
                    && !source.state.has_complete_hnsw_route()
                {
                    return Ok(false);
                }
            }
            Ok(true)
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
                    .try_directory_visible_entry_count(snapshot, candidates)?
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

    #[doc(hidden)]
    pub fn try_hnsw_search_covers_all_without_build(
        &self,
        index: &VectorIndexRef,
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        self.store.with_bulk_read(|| {
            let layout = self.store.index_layout(index)?;
            let sources = self.selected_partition_sources(index, candidates, snapshot)?;
            let mut nonempty = false;
            for source in sources {
                let allowed = source
                    .state
                    .try_directory_visible_entry_count(snapshot, candidates)?;
                if allowed == 0 {
                    continue;
                }
                nonempty = true;
                let count = source
                    .state
                    .try_directory_visible_entry_count(snapshot, None)?;
                let ef = layout
                    .resolve_policy(count, search_policy_limit(k))
                    .hnsw_ef_search;
                if !source.state.has_complete_hnsw_route()
                    || crate::hnsw::hnsw_search_candidate_cap_for_ef(ef, k) < allowed
                {
                    return Ok(false);
                }
            }
            Ok(nonempty)
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

/// Proof-only observer for native-vector scoring. The event is emitted at the
/// cosine calculation itself, including exact fallback and graph reranking.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub trait NativeVectorScoreObserver: Send + Sync {
    fn before_native_vector_score(&self);
}

#[cfg(feature = "test-seams")]
thread_local! {
    static NATIVE_VECTOR_SCORE_OBSERVER: RefCell<Option<Arc<dyn NativeVectorScoreObserver>>> =
        const { RefCell::new(None) };
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_native_vector_score_observer_for_test<T>(
    observer: Arc<dyn NativeVectorScoreObserver>,
    action: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<Arc<dyn NativeVectorScoreObserver>>);

    impl Drop for Restore {
        fn drop(&mut self) {
            NATIVE_VECTOR_SCORE_OBSERVER.with(|slot| slot.replace(self.0.take()));
        }
    }

    let _restore = Restore(NATIVE_VECTOR_SCORE_OBSERVER.with(|slot| slot.replace(Some(observer))));
    action()
}

#[cfg(feature = "test-seams")]
pub(crate) fn observe_native_vector_score_for_test() {
    let observer = NATIVE_VECTOR_SCORE_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.before_native_vector_score();
    }
}

/// Independent proof events at the physical candidate-to-partition boundary.
/// The row-directory and visibility events are emitted by the store primitives
/// that perform those lookups. Placement is emitted only after the candidate is
/// appended to a partition-local search group.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedCandidateGroupingEventForTest {
    RowDirectoryLookup,
    SnapshotVisibilityProbe,
    Placement,
}

/// Production-dead faults used to prove that the fixed-candidate assertions
/// reject both partition-multiplied lookup work and an omitted placement
/// charge while the returned rows remain unchanged.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedCandidateGroupingFaultForTest {
    RepeatGlobalCandidatePassPerPartition,
    SuppressPlacementCharge,
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub trait BoundedCandidateGroupingObserverForTest: Send + Sync {
    fn observe(&self, event: BoundedCandidateGroupingEventForTest);
}

#[cfg(feature = "test-seams")]
thread_local! {
    static BOUNDED_CANDIDATE_GROUPING_OBSERVER: RefCell<Option<Arc<dyn BoundedCandidateGroupingObserverForTest>>> =
        const { RefCell::new(None) };
    static BOUNDED_CANDIDATE_GROUPING_FAULT: Cell<Option<BoundedCandidateGroupingFaultForTest>> =
        const { Cell::new(None) };
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_bounded_candidate_grouping_observer_for_test<T>(
    observer: Arc<dyn BoundedCandidateGroupingObserverForTest>,
    action: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<Arc<dyn BoundedCandidateGroupingObserverForTest>>);

    impl Drop for Restore {
        fn drop(&mut self) {
            BOUNDED_CANDIDATE_GROUPING_OBSERVER.with(|slot| slot.replace(self.0.take()));
        }
    }

    let _restore =
        Restore(BOUNDED_CANDIDATE_GROUPING_OBSERVER.with(|slot| slot.replace(Some(observer))));
    action()
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_bounded_candidate_grouping_fault_for_test<T>(
    fault: BoundedCandidateGroupingFaultForTest,
    action: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<BoundedCandidateGroupingFaultForTest>);

    impl Drop for Restore {
        fn drop(&mut self) {
            BOUNDED_CANDIDATE_GROUPING_FAULT.with(|slot| slot.set(self.0));
        }
    }

    let _restore = Restore(BOUNDED_CANDIDATE_GROUPING_FAULT.with(|slot| slot.replace(Some(fault))));
    action()
}

#[cfg(feature = "test-seams")]
pub(crate) fn observe_bounded_candidate_grouping_for_test(
    event: BoundedCandidateGroupingEventForTest,
) {
    let observer = BOUNDED_CANDIDATE_GROUPING_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.observe(event);
    }
}

#[cfg(feature = "test-seams")]
fn bounded_candidate_grouping_repeats_global_pass_for_test() -> bool {
    BOUNDED_CANDIDATE_GROUPING_FAULT.with(|slot| {
        slot.get()
            == Some(BoundedCandidateGroupingFaultForTest::RepeatGlobalCandidatePassPerPartition)
    })
}

#[cfg(not(feature = "test-seams"))]
fn bounded_candidate_grouping_repeats_global_pass_for_test() -> bool {
    false
}

#[cfg(feature = "test-seams")]
fn bounded_candidate_grouping_suppresses_placement_charge_for_test() -> bool {
    BOUNDED_CANDIDATE_GROUPING_FAULT.with(|slot| {
        slot.get() == Some(BoundedCandidateGroupingFaultForTest::SuppressPlacementCharge)
    })
}

#[cfg(not(feature = "test-seams"))]
fn bounded_candidate_grouping_suppresses_placement_charge_for_test() -> bool {
    false
}

#[cfg(test)]
mod route_cost_tests {
    use crate::store::VectorIndexLayout;
    use contextdb_core::VectorQuantization;

    const K: usize = 10;

    /// The automatic route walks a partition it can walk, at the sizes the
    /// find lane measured end to end (release, one Context, 768 dimensions,
    /// 100 held-out queries, warm: 32.0 ms p95 for the graph against 630.0 ms
    /// exhaustive at 10,000 vectors, 36.0 ms against 838.0 ms at 12,500).
    /// The undeclared profile's breadth and degree fix the walk's budget, and
    /// the measured unit costs make that budget cheaper than comparing every
    /// row.
    #[test]
    fn the_statement_crossover_walks_a_partition_larger_than_one_walk_over_it() {
        let layout = VectorIndexLayout::unpartitioned(768, VectorQuantization::F32);
        for live in [1_250_usize, 10_000, 12_500] {
            let policy = layout.resolve_policy(live, K);
            assert_eq!(
                policy.ef_search_source, "compatibility_profile",
                "these cells declare no breadth of their own"
            );
            let budget = super::auto_statement_walk_budget(policy.hnsw_ef_search, K, policy.hnsw_m);
            assert!(
                !super::auto_statement_prefers_exhaustive(budget, live),
                "a {live}-vector statement is walked with a budget of {budget}, not compared \
                 vector by vector"
            );
        }
        // And the other side of the crossover: a statement whose rows cost no
        // more to compare than the walk over them is compared outright.
        let small = layout.resolve_policy(64, K);
        let budget = super::auto_statement_walk_budget(small.hnsw_ef_search, K, small.hnsw_m);
        assert!(super::auto_statement_prefers_exhaustive(budget, 64));
    }

    /// The decision reads only the statement's aggregate admitted rows and its
    /// one walk budget, and weighs each side by its measured unit cost: the
    /// crossover sits where the admitted rows' comparisons cost exactly what
    /// the walk's budget costs.
    #[test]
    fn the_statement_crossover_weighs_both_routes_at_their_measured_unit_costs() {
        let budget = super::auto_statement_walk_budget(400, K, 16);
        assert_eq!(
            budget,
            400 * 32,
            "the layer-0 maximum degree of an M = 16 graph is 32"
        );
        let walk_nanos = budget as u128 * u128::from(super::AUTO_WALK_EVALUATION_NANOS);
        let crossover = (walk_nanos / u128::from(super::AUTO_EXHAUSTIVE_COMPARISON_NANOS)) as usize;
        assert!(super::auto_statement_prefers_exhaustive(budget, crossover));
        assert!(!super::auto_statement_prefers_exhaustive(
            budget,
            crossover + 1
        ));
    }
}
