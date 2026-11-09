use crate::store::VectorStore;
use contextdb_core::read_contract::ReadFailure;
use contextdb_core::*;
use contextdb_tx::{TransactionManager, WriteSetApplicator};
use roaring::RoaringTreemap;
use std::cell::{Cell, RefCell};
use std::sync::Arc;

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

/// The original limit of an enclosing ranked read, which wants the whole
/// refined candidate pool of the route selected for that limit.
#[doc(hidden)]
pub fn search_policy_limit() -> Option<usize> {
    SEARCH_POLICY_LIMIT.with(std::cell::Cell::get)
}

#[cfg(feature = "test-seams")]
thread_local! {
    static VECTOR_ADMISSION_SITE: std::cell::Cell<&'static str> = const { std::cell::Cell::new("") };
}

/// Runs one exact-comparison admission and, when it fails, names it, so a
/// proof can tell which reservation a refusal came from. Only the first
/// failure since the last reset is named: an admission nested in another fails
/// first, and the enclosing one only passes its error on. The naming is
/// compiled out of production builds.
#[inline]
fn admitted_at<T, E>(
    _site: &'static str,
    admission: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<T, E> {
    let result = admission();
    #[cfg(feature = "test-seams")]
    if result.is_err() {
        VECTOR_ADMISSION_SITE.with(|site| {
            if site.get().is_empty() {
                site.set(_site);
            }
        });
    }
    result
}

/// The exact-comparison admission whose reservation failed first on this
/// thread since the last reset, or `""` if none failed: `route_slots`,
/// `cursor_window`, `body_read`, `merge_state` or `disconnected_slots`.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn refused_vector_admission_site_for_test() -> &'static str {
    VECTOR_ADMISSION_SITE.with(|site| site.get())
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn reset_vector_admission_site_for_test() {
    VECTOR_ADMISSION_SITE.with(|site| site.set(""));
}

#[cfg(feature = "test-seams")]
thread_local! {
    static VECTOR_ADMISSION_REFUSAL: std::cell::Cell<&'static str> = const { std::cell::Cell::new("") };
}

/// The bytes the named admission reserves: `bytes`, unless a proof asked this
/// thread to refuse that admission once. Then it asks the real budget for
/// more than any declaration holds, so the refusal, its cleanup and its typed
/// error are the production ones. Identity in production builds.
#[doc(hidden)]
#[inline]
pub fn admission_bytes(_site: &'static str, bytes: usize) -> usize {
    #[cfg(feature = "test-seams")]
    if VECTOR_ADMISSION_REFUSAL.with(|refused| refused.get() == _site) {
        VECTOR_ADMISSION_REFUSAL.with(|refused| refused.set(""));
        return 1 << 50;
    }
    bytes
}

/// Ask the next reservation of the named admission on this thread to be
/// refused, once.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn refuse_vector_admission_once_for_test(site: &'static str) {
    VECTOR_ADMISSION_REFUSAL.with(|refused| refused.set(site));
}

/// The most one row id can cost a `RoaringTreemap` while it is built, and
/// again as the output of an intersection, with no density assumption: the
/// worst case is every id in its own high-32 bucket (a treemap entry, a bitmap
/// and a container each). Measured, not assumed: the counting-allocator proof
/// `tests/roaring_treemap_growth_stays_within_its_bound.rs` checks sparse,
/// container-sparse and dense ids against it.
pub const ROARING_TREEMAP_BYTES_PER_ID_UPPER_BOUND: usize = 1_024;

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

/// A statement filter checked on reached rows. `check` reads one row under the
/// statement's snapshot and authorization.
pub struct RowCheck<'a, E> {
    pub check: &'a mut dyn FnMut(RowId) -> std::result::Result<bool, E>,
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

// capture969 begin
#[cfg(feature = "test-seams")]
type TraceHandoffObserver = fn(&'static str, usize, usize, usize);
#[cfg(feature = "test-seams")]
thread_local! {
    static TRACE_HANDOFF_CUT: std::cell::Cell<Option<TraceHandoffObserver>> =
        const { std::cell::Cell::new(None) };
}
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn set_trace_handoff_observer_for_test(
    callback: Option<fn(&'static str, usize, usize, usize)>,
) {
    TRACE_HANDOFF_CUT.with(|slot| slot.set(callback));
}
#[cfg(feature = "test-seams")]
pub(crate) fn trace_handoff_cut_for_test(
    site: &'static str,
    address: usize,
    bytes: usize,
    payer: usize,
) {
    if let Some(callback) = TRACE_HANDOFF_CUT.with(std::cell::Cell::get) {
        callback(site, address, bytes, payer);
    }
}
#[cfg(feature = "test-seams")]
struct TracePendingBoundaryForTest((usize, usize, usize));
#[cfg(feature = "test-seams")]
impl Drop for TracePendingBoundaryForTest {
    fn drop(&mut self) {
        // Saved scalar address is an observation, never dereferenced here.
        trace_handoff_cut_for_test("pending-scope-exit", self.0.0, self.0.1, self.0.2);
    }
}
// capture969 end
/// Scores and their store charge have one owner, including while a caller ranks
/// or consumes them. Capacity is admitted before allocation and never grows
/// through an uncharged `Vec::push`.
#[doc(hidden)]
#[derive(Debug)]
pub struct SearchScores {
    rows: Vec<(RowId, f32)>,
    index: VectorIndexRef,
    credit: ScoreCredit,
    exact_budget: bool,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct ScoreCredit {
    budget: crate::memory_budget::MemoryBudgetOwner,
    bytes: usize,
    _control_invoice:
        Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
}
impl ScoreCredit {
    fn holding(budget: impl Into<crate::memory_budget::MemoryBudgetOwner>, bytes: usize) -> Self {
        let budget: crate::memory_budget::MemoryBudgetOwner = budget.into();

        let control_invoice = budget.control_invoice();
        Self {
            budget,
            bytes,
            _control_invoice: control_invoice,
        }
    }
}

impl crate::PrepaidVectorCopyCharge for ScoreCredit {
    fn accountant(&self) -> crate::memory_budget::MemoryBudgetOwner {
        self.budget.clone()
    }
    fn retained_bytes(&self) -> usize {
        self.bytes
    }
    fn transfer(mut self: Box<Self>) {
        self.bytes = 0;
    }
}

impl Drop for ScoreCredit {
    fn drop(&mut self) {
        // capture969 begin
        #[cfg(feature = "test-seams")]
        trace_handoff_cut_for_test(
            "score-credit-drop",
            self as *const Self as usize,
            self.bytes,
            self.budget.stable_funder_identity(),
        );
        // capture969 end
        self.budget.release(self.bytes);
    }
}

impl SearchScores {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn new(
        index: &VectorIndexRef,
        budget: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<Self> {
        let budget: crate::memory_budget::MemoryBudgetOwner = budget.into();

        let (index, bytes) = allocate_vector_index(
            index,
            &mut |bytes| {
                budget.try_allocate_for_static(
                    bytes,
                    "vector_search",
                    "score_identity",
                    "Reduce query scope or raise MEMORY_LIMIT.",
                )
            },
            &mut |bytes| budget.release(bytes),
        )?;
        Ok(Self {
            rows: Vec::new(),
            index,
            credit: ScoreCredit::holding(budget, bytes),
            exact_budget: false,
        })
    }

    /// Consume the allocation and its actual charge together. The destination
    /// must consume this charge through the matched prepaid admission bridge.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn into_prepaid(
        self,
        destination: &crate::memory_budget::MemoryBudgetOwner,
    ) -> Result<(Vec<(RowId, f32)>, ScoreCredit)> {
        if !crate::memory_budget::same_funder(destination.as_ref(), self.credit.budget.as_ref()) {
            return Err(Error::Other(
                "vector score destination has a different accountant".into(),
            ));
        }
        Ok((self.rows, self.credit))
    }

    pub fn for_exact(mut self) -> Self {
        self.exact_budget = true;
        self
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
        let mut replacement = Self::new(&self.index, self.credit.budget.clone())?;
        replacement.exact_budget = self.exact_budget;
        replacement.charge(bytes)?;
        replacement
            .rows
            .try_reserve_exact(wanted)
            .map_err(|_| Error::Other("vector score allocation failed".into()))?;
        let actual = replacement.rows.capacity() * std::mem::size_of::<(RowId, f32)>();
        if actual > bytes {
            replacement.charge(actual - bytes)?;
        }
        replacement.rows.extend_from_slice(&self.rows);
        let old = std::mem::replace(self, replacement);
        drop(old);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn charge(&mut self, bytes: usize) -> Result<()> {
        charge_score_credit(&mut self.credit, &self.index, self.exact_budget, bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_push(&mut self, row: (RowId, f32)) -> Result<()> {
        self.reserve(1)?;
        self.rows.push(row);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn charge_score_credit(
    credit: &mut ScoreCredit,
    index: &VectorIndexRef,
    exact_budget: bool,
    bytes: usize,
) -> Result<()> {
    credit
        .budget
        .try_allocate_for(
            bytes,
            "vector_search",
            if exact_budget {
                "exact_scores"
            } else {
                "merged_scores"
            },
            "Narrow the authorized scope or raise MEMORY_LIMIT.",
        )
        .map_err(|error| match error {
            Error::MemoryBudgetExceeded {
                available_bytes, ..
            } if exact_budget => Error::VectorExactSearchBudgetExceeded {
                index: index.clone(),
                required_bytes: bytes as u64,
                available_bytes: available_bytes as u64,
            },
            other => other,
        })?;
    credit.bytes += bytes;
    Ok(())
}

/// Exact-route scores keyed by canonical similarity (higher is nearer), kept
/// until every merge and LIMIT is final, then narrowed once into the public
/// [`SearchScores`]. Capacity is admitted before allocation exactly as for
/// [`SearchScores`]. It has no `Deref`, so no caller can reorder a row away
/// from its key.
#[doc(hidden)]
#[derive(Debug)]
pub struct CanonicalScores {
    // Field order is drop order: the rows are freed before their credit refunds.
    rows: Vec<(RowId, f64)>,
    index: VectorIndexRef,
    credit: ScoreCredit,
    exact_budget: bool,
}

impl CanonicalScores {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn new(
        index: &VectorIndexRef,
        budget: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<Self> {
        let SearchScores {
            rows: _,
            index,
            credit,
            exact_budget,
        } = SearchScores::new(index, budget)?;
        Ok(Self {
            rows: Vec::new(),
            index,
            credit,
            exact_budget,
        })
    }

    pub fn for_exact(mut self) -> Self {
        self.exact_budget = true;
        self
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The rows with their canonical keys, in their current order.
    pub fn rows(&self) -> &[(RowId, f64)] {
        &self.rows
    }

    /// Grow with the old and the new buffer both funded, as
    /// [`SearchScores::reserve`] does; the old buffer is freed first.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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
            .checked_mul(std::mem::size_of::<(RowId, f64)>())
            .ok_or_else(|| Error::Other("vector score capacity overflow".into()))?;
        let mut replacement = Self::new(&self.index, self.credit.budget.clone())?;
        replacement.exact_budget = self.exact_budget;
        charge_score_credit(
            &mut replacement.credit,
            &replacement.index,
            replacement.exact_budget,
            bytes,
        )?;
        replacement
            .rows
            .try_reserve_exact(wanted)
            .map_err(|_| Error::Other("vector score allocation failed".into()))?;
        let actual = replacement.rows.capacity() * std::mem::size_of::<(RowId, f64)>();
        if actual > bytes {
            charge_score_credit(
                &mut replacement.credit,
                &replacement.index,
                replacement.exact_budget,
                actual - bytes,
            )?;
        }
        replacement.rows.extend_from_slice(&self.rows);
        let old = std::mem::replace(self, replacement);
        drop(old);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn try_push(&mut self, row: (RowId, f64)) -> Result<()> {
        self.reserve(1)?;
        self.rows.push(row);
        Ok(())
    }

    pub fn truncate(&mut self, len: usize) {
        self.rows.truncate(len);
    }

    pub fn retain(&mut self, predicate: impl FnMut(&(RowId, f64)) -> bool) {
        self.rows.retain(predicate);
    }

    /// Key descending, then row id ascending: the one exact answer order.
    pub fn sort_canonical(&mut self) {
        sort_vector_scores(&mut self.rows);
    }

    /// The public f32 scores in this order, each `key as f32`. The output is
    /// admitted at its actual capacity while these rows are still alive and
    /// charged; these rows are then freed before their charge is refunded. On
    /// a refusal both buffers drop with their credits.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn narrow(self) -> Result<SearchScores> {
        let mut narrowed = SearchScores::new(&self.index, self.credit.budget.clone())?;
        narrowed.exact_budget = self.exact_budget;
        narrowed.reserve(self.rows.len())?;
        narrowed
            .rows
            .extend(self.rows.iter().map(|&(row_id, key)| (row_id, key as f32)));
        drop(self);
        Ok(narrowed)
    }
}

/// A ranking score: the displayed f32 or the exact route's canonical f64 key.
/// Higher is nearer; equal scores order by row id.
trait RankScore: Copy {
    fn rank_cmp(&self, other: &Self) -> std::cmp::Ordering;
}
impl RankScore for f32 {
    fn rank_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_cmp(other)
    }
}
impl RankScore for f64 {
    fn rank_cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_cmp(other)
    }
}

fn sort_vector_scores<S: RankScore>(rows: &mut [(RowId, S)]) {
    rows.sort_unstable_by(|a, b| b.1.rank_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
}

fn vector_score_quality<S: RankScore>(left: &(RowId, S), right: &(RowId, S)) -> std::cmp::Ordering {
    left.1.rank_cmp(&right.1).then_with(|| right.0.cmp(&left.0))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn push_bounded_score_heap<S: RankScore>(
    scores: &mut Vec<(RowId, S)>,
    score: (RowId, S),
) -> Result<()> {
    scores.push(score);
    let mut child = scores.len().checked_sub(1).ok_or_else(|| {
        Error::Other(("bounded brute-force heap lost its inserted score".to_string()).into())
    })?;
    while child != 0 {
        let parent = child.checked_sub(1).ok_or_else(|| {
            Error::Other(("bounded brute-force heap parent underflow".to_string()).into())
        })? / 2;
        if vector_score_quality(&scores[child], &scores[parent]) != std::cmp::Ordering::Less {
            break;
        }
        scores.swap(child, parent);
        child = parent;
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn replace_bounded_score_heap_root<S: RankScore>(
    scores: &mut [(RowId, S)],
    score: (RowId, S),
) -> Result<()> {
    let Some(root) = scores.first_mut() else {
        return Err(Error::Other(
            ("bounded brute-force heap has no replacement root".to_string()).into(),
        ));
    };
    *root = score;
    let mut parent = 0usize;
    loop {
        let Some(left) = parent.checked_mul(2).and_then(|value| value.checked_add(1)) else {
            return Err(Error::Other(
                ("bounded brute-force heap child overflow".to_string()).into(),
            ));
        };
        if left >= scores.len() {
            return Ok(());
        }
        let right = left.checked_add(1).ok_or_else(|| {
            Error::Other(("bounded brute-force heap child overflow".to_string()).into())
        })?;
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
fn merge_bounded_vector_score<E, S: RankScore>(
    scores: &mut Vec<(RowId, S)>,
    retained_bytes: &mut usize,
    candidate: (RowId, S),
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
                ("bounded vector merge length overflow".to_string()).into(),
            ))
        })?;
        admitted_at("merge_state", || {
            grow_vector_copy(
                scores,
                required_capacity,
                Some(retained_bytes),
                "partitioned vector merge",
                before_source_entry,
                &mut |bytes| before_retain(admission_bytes("merge_state", bytes)),
                release_retained,
            )
        })?;
        push_bounded_score_heap(scores, candidate).map_err(E::from)?;
    } else if scores
        .first()
        .is_some_and(|worst| vector_score_quality(&candidate, worst) == std::cmp::Ordering::Greater)
    {
        replace_bounded_score_heap_root(scores, candidate).map_err(E::from)?;
    }
    Ok(())
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
    /// The successful call visited every vector it could have returned: an
    /// exact scan, or an index traversal that probed every list it counted.
    pub traversal_complete: bool,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchDebugTrace {
    pub index: VectorIndexRef,
    pub final_row_ids: Vec<RowId>,
    pub supplemented_row_count: usize,
    pub fallback_reason: Option<&'static str>,
    /// Actual per-partition choices in selected-partition order.
    pub partition_routes: Vec<VectorPartitionSearchTrace>,
    /// The one union walk's budget and terminal state, when a walk ran.
    pub budget_receipt: Option<VectorSearchBudgetReceipt>,
}

/// Scoring paid by the partitions that executed the native indexed route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorSearchBudgetReceipt {
    /// Stored vectors compared at full precision by the indexed partitions:
    /// the canonical re-ranks plus the committed-tail comparisons.
    pub rescore_evaluations: usize,
}

/// Fixed borrowed invoice. Zero-capacity buffers have no native allocation.
/// The Arc payload address is an identity, NOT an invented Arc allocation Layout.
#[cfg(feature = "test-seams")]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct TraceInvoiceForTest {
    // table, column, routes, final rows
    pub allocations: [(usize, usize, usize); 4],
    pub capacities: [usize; 4],
    pub original_payer: usize,
    pub forwarding_payer: usize,
    pub owner_control_invoice_present: bool,
    pub owner_payload_pointer: usize,
    pub prepaid_control: (usize, usize, usize),
    pub prepaid_bytes: usize,
    pub additional_bytes: usize,
    pub prepaid_control_bytes: usize,
    pub score_credit_bytes: usize,
    pub admission_control_invoice_present: bool,
}
#[cfg(feature = "test-seams")]
impl TraceInvoiceForTest {
    pub fn payload_layout_bytes(&self) -> usize {
        self.allocations.iter().map(|allocation| allocation.1).sum()
    }
    /// Trace carrier debit only. The provider Arc and its external native
    /// keeper are separate allocations; this partial receipt excludes them.
    /// Qualification must also reconcile their actual allocator requests.
    pub fn complete_debit(&self) -> usize {
        self.prepaid_bytes
            + self.additional_bytes
            + self.prepaid_control_bytes
            + self.score_credit_bytes
    }
}
#[cfg(feature = "test-seams")]
fn trace_capacities_for_test(trace: &VectorSearchDebugTrace) -> [usize; 4] {
    [
        trace.index.table.capacity(),
        trace.index.column.capacity(),
        trace.partition_routes.capacity(),
        trace.final_row_ids.capacity(),
    ]
}
#[cfg(feature = "test-seams")]
fn trace_allocations_for_test(trace: &VectorSearchDebugTrace) -> [(usize, usize, usize); 4] {
    fn buffer<T>(pointer: *const T, capacity: usize) -> (usize, usize, usize) {
        let layout = std::alloc::Layout::array::<T>(capacity).unwrap();
        (pointer as usize, layout.size(), layout.align())
    }
    [
        buffer(trace.index.table.as_ptr(), trace.index.table.capacity()),
        buffer(trace.index.column.as_ptr(), trace.index.column.capacity()),
        buffer(
            trace.partition_routes.as_ptr(),
            trace.partition_routes.capacity(),
        ),
        buffer(trace.final_row_ids.as_ptr(), trace.final_row_ids.capacity()),
    ]
}

/// Public materialized trace and the executing owner's retained charge.
/// Borrowed observations cannot deep-clone the admitted trace.
#[derive(Debug)]
pub struct OwnedVectorSearchTrace {
    trace: VectorSearchDebugTrace,
    credit: ScoreCredit,
    admission: Option<crate::VectorCopyAdmission>,
}

impl OwnedVectorSearchTrace {
    /// Rebuild an owned trace from a materialized trace and the admission
    /// that already holds its retained allocation, so publication keeps the
    /// same charge instead of copying or re-reserving it.
    #[doc(hidden)]
    pub fn from_admitted(
        trace: VectorSearchDebugTrace,
        executing_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        admission: crate::VectorCopyAdmission,
    ) -> OwnedVectorSearchTrace {
        let executing_owner: crate::memory_budget::MemoryBudgetOwner = executing_owner.into();

        OwnedVectorSearchTrace {
            trace,
            credit: ScoreCredit::holding(executing_owner, 0),
            admission: Some(admission),
        }
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    // Visit the actual retained payer only for proofs that already pin it.
    // Scalar-only observations pass a no-op and acquire no ownership.
    pub fn complete_invoice_for_test(
        &self,
        visit: impl FnOnce(&contextdb_core::read_memory::ReadMemoryOwner),
    ) -> TraceInvoiceForTest {
        let mut receipt = TraceInvoiceForTest {
            allocations: trace_allocations_for_test(&self.trace),
            capacities: trace_capacities_for_test(&self.trace),
            score_credit_bytes: self.credit.bytes,
            ..Default::default()
        };
        if let Some(admission) = self.admission.as_ref()
            && let Some(original) = admission.complete_invoice_for_test(&mut receipt)
        {
            visit(original);
        }
        receipt
    }

    pub fn supplemented_row_count(&self) -> usize {
        self.trace.supplemented_row_count
    }

    #[doc(hidden)]
    pub fn retained_credit_bytes_for_test(&self) -> usize {
        self.credit.bytes
            + self
                .admission
                .as_ref()
                .map_or(0, crate::VectorCopyAdmission::retained_bytes)
    }

    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub fn debug_trace_for_test(&self) -> &VectorSearchDebugTrace {
        &self.trace
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn brute_force<S>(
        index: &VectorIndexRef,
        rows: &[(RowId, S)],
        reason: &'static str,
        routes: Vec<VectorPartitionSearchTrace>,
        budget: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<Self> {
        let budget: crate::memory_budget::MemoryBudgetOwner = budget.into();

        Self::brute_force_with_owner(
            index,
            rows,
            reason,
            routes,
            crate::VectorCopyAdmission::new(budget),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn brute_force_with_owner<S>(
        index: &VectorIndexRef,
        rows: &[(RowId, S)],
        reason: &'static str,
        routes: Vec<VectorPartitionSearchTrace>,
        admission: crate::VectorCopyAdmission,
    ) -> Result<Self> {
        let budget = admission.accountant().clone();
        let admission = std::cell::RefCell::new(admission);
        let (owned_index, _) = allocate_vector_index(
            index,
            &mut |bytes| admission.borrow_mut().reserve(bytes),
            &mut |_bytes| {},
        )?;
        let (final_row_ids, _) = allocate_vector_row_ids(
            rows,
            "exact trace final rows",
            &mut || Ok::<(), Error>(()),
            &mut |bytes| admission.borrow_mut().reserve(bytes),
            &mut |_bytes| {},
        )?;
        let admission = admission.into_inner();
        let trace = VectorSearchDebugTrace {
            index: owned_index,
            final_row_ids,
            supplemented_row_count: 0,
            fallback_reason: Some(reason),
            partition_routes: routes,
            budget_receipt: None,
        };
        Ok(Self {
            trace,
            credit: ScoreCredit::holding(budget, 0),
            admission: Some(admission),
        })
    }

    pub fn index(&self) -> &VectorIndexRef {
        &self.trace.index
    }
    pub fn final_row_ids(&self) -> &[RowId] {
        &self.trace.final_row_ids
    }
    pub fn partition_routes(&self) -> &[VectorPartitionSearchTrace] {
        &self.trace.partition_routes
    }
    pub fn fallback_reason(&self) -> Option<&'static str> {
        self.trace.fallback_reason
    }
    pub fn budget_receipt(&self) -> Option<VectorSearchBudgetReceipt> {
        self.trace.budget_receipt
    }

    /// The destination consumes this charge via VectorCopyAdmission::from_prepaid
    /// against its actual owner and adopts the charge exactly once before transfer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn into_prepaid(
        mut self,
        destination: &crate::memory_budget::MemoryBudgetOwner,
    ) -> Result<(VectorSearchDebugTrace, ScoreCredit)> {
        if !crate::memory_budget::same_funder(destination.as_ref(), self.credit.budget.as_ref()) {
            return Err(Error::Other(
                "vector trace destination has a different accountant".into(),
            ));
        }
        if let Some(admission) = self.admission.take() {
            self.credit.bytes += admission.retained_bytes();
            admission.transfer();
        }
        Ok((self.trace, self.credit))
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
    /// The exhaustive scan's one-pass read state across resumed steps.
    one_pass: crate::store::OnePassSession,
    query: Vec<f32>,
    k: usize,
    candidate_filter: bool,
    snapshot: SnapshotId,
    snapshot_sources: Vec<BoundedSnapshotVectorSource>,
    source_position: usize,
    position: usize,
    state_end: Option<usize>,
    /// Candidates and the retained heap carry the canonical ranking key; a
    /// Row step displays `key as f32`.
    pending_score: Option<(RowId, f64)>,
    scored: Vec<(RowId, f64)>,
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
    historical_partition: Option<crate::store::HistoricalRawVectorPartition>,
    membership_cursor: Option<Box<dyn crate::store::RawVectorMembershipCursor>>,
    membership_reserved_bytes: usize,
    // Keep the exact body plus the visibility scalars actually used on retry.
    // A second full body/provenance record was a redundant boxed payload.
    pending_membership: Option<(
        crate::store::RawVectorDirectoryEntry,
        RowId,
        TxId,
        Option<TxId>,
    )>,
}

/// A store-owned entry source pinned when the cursor is opened. Exact stepping
/// uses the source handle directly; it never re-resolves "the active state for
/// this partition" after a suspension. The present store can expose active
/// retained states through this handle. Archived-generation sources must join
/// the same enumeration seam before generation reclamation is enabled.
#[derive(Clone)]
struct BoundedSnapshotVectorSource {
    partition_key: VectorPartitionKey,
    state: crate::store::VectorQueryState,
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
    /// The statement's filter is checked row by row as the search reaches
    /// rows; no list of allowed rows exists, and allowed counts are unknown.
    row_checked: bool,
    sources: Vec<BoundedSnapshotVectorSource>,
    aggregate_allowed_count: usize,
    retained_bytes: usize,
}

impl BoundedVectorCandidateGroups {
    /// Scalar metadata only; the caller moves the one source array into its
    /// cursor and restores it here after scan-only ownership has ended.
    #[doc(hidden)]
    pub fn disclosure_metadata(&self) -> Self {
        Self {
            snapshot: self.snapshot,
            filtered: self.filtered,
            row_checked: self.row_checked,
            sources: Vec::new(),
            aggregate_allowed_count: self.aggregate_allowed_count,
            retained_bytes: self.retained_bytes,
        }
    }

    pub fn is_filtered(&self) -> bool {
        self.filtered
    }

    /// Mark groups built without a candidate list as carrying a filter the
    /// search checks row by row: it then never treats a partition as wholly
    /// allowed, and never decides anything from an allowed count.
    pub fn mark_row_checked(&mut self) {
        self.row_checked = true;
    }

    pub fn is_row_checked(&self) -> bool {
        self.row_checked
    }

    pub fn aggregate_allowed_count(&self) -> usize {
        self.aggregate_allowed_count
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
        mut visit: impl FnMut(usize, usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        for source in self
            .sources
            .iter()
            .filter(|source| source.allowed_count != 0)
        {
            visit(source.allowed_count, source.live_count)?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn native_snapshot(&self) -> SnapshotId {
        self.snapshot
    }

    /// Borrow the exact captured tuple and its already admitted eligibility
    /// window for a native maintained search. No body or newer state is loaded.
    #[doc(hidden)]
    pub fn visit_native_partitions<E>(
        &self,
        mut visit: impl FnMut(&VectorPartitionKey, Option<&[u64]>, usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        for source in &self.sources {
            if source.allowed_count == 0 {
                continue;
            }
            visit(
                &source.partition_key,
                if source.candidate_ids_cover_all_live {
                    None
                } else {
                    source.candidate_ids.as_deref()
                },
                source.allowed_count,
            )?;
        }
        Ok(())
    }

    /// Inspect only the already captured nonempty sources, one fixed
    /// metadata window at a time. This never builds a partition-info array.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn has_pending_tail_without_load(
        &self,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<bool> {
        for source in self
            .sources
            .iter()
            .filter(|source| source.allowed_count != 0)
        {
            if source.state.has_pending_work_for_read(owner)? {
                return Ok(true);
            }
        }
        Ok(false)
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
/// it. Publish it by consuming the actual caller's prepaid charge through
/// [`BoundedVectorSearchTrace::into_owned`].
#[derive(Debug)]
pub struct BoundedVectorSearchTrace {
    trace: VectorSearchDebugTrace,
    retained_bytes: usize,
}

impl BoundedVectorSearchTrace {
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn allocation_invoice_for_test(&self) -> TraceInvoiceForTest {
        TraceInvoiceForTest {
            allocations: trace_allocations_for_test(&self.trace),
            capacities: trace_capacities_for_test(&self.trace),
            prepaid_bytes: self.retained_bytes,
            ..Default::default()
        }
    }

    /// Bytes admitted for this trace, already included in the owning result's
    /// `retained_bytes`.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Wrap the exact trace and the byte credit already held by the shared
    /// execution kernel. The native caller admits every buffer before allocation.
    #[doc(hidden)]
    pub fn from_prepaid(trace: VectorSearchDebugTrace, retained_bytes: usize) -> Self {
        Self {
            trace,
            retained_bytes,
        }
    }

    pub fn partition_routes(&self) -> &[VectorPartitionSearchTrace] {
        &self.trace.partition_routes
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

    /// Move the trace together with the actual caller's already held charge.
    /// The prepaid bridge rejects another accountant and retains the complete
    /// token until output drop or a consuming matched destination transfer.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn into_owned<C: crate::PrepaidVectorCopyCharge + 'static>(
        self,
        executing_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
        charge: C,
    ) -> Result<OwnedVectorSearchTrace> {
        let executing_owner: crate::memory_budget::MemoryBudgetOwner = executing_owner.into();

        // Field order also covers provider callback unwind: destroy the real
        // buffers before returning the prepaid payload invoice to its funder.
        struct PendingTrace<C> {
            trace: BoundedVectorSearchTrace,
            charge: Option<C>,
        }
        let mut pending = PendingTrace {
            trace: self,
            charge: Some(charge),
        };
        // capture969 begin
        #[cfg(feature = "test-seams")]
        let _pending_boundary = {
            let charge = pending.charge.as_ref().unwrap();
            let tuple = (
                charge.credit_address_for_test(),
                charge.retained_bytes(),
                charge.original_payer_identity_for_test(),
            );
            trace_handoff_cut_for_test("pending-credit", tuple.0, tuple.1, tuple.2);
            TracePendingBoundaryForTest(tuple)
        };
        // capture969 end
        if pending.charge.as_ref().unwrap().retained_bytes() < pending.trace.retained_bytes {
            drop(pending);
            return Err(Error::Other(
                "vector trace prepaid charge is smaller than its retained allocation".into(),
            ));
        }
        // Complete all provider callbacks before moving the prepaid charge.
        // A failed Box admission leaves that charge beside the live buffers.
        let credit = ScoreCredit::holding(executing_owner.clone(), 0);
        let admission = crate::VectorCopyAdmission::from_prepaid_retaining_charge(
            executing_owner,
            &mut pending.charge,
        )?;
        Ok(OwnedVectorSearchTrace {
            trace: pending.trace.trace,
            credit,
            admission: Some(admission),
        })
    }

    // Only vector-internal consumers with the same callback-accounted owner
    // may attach the charge directly. Public extraction requires a real token.
    #[doc(hidden)]
    #[cfg(feature = "test-seams")]
    pub fn debug_trace_for_test(&self) -> &VectorSearchDebugTrace {
        &self.trace
    }
}

impl BoundedBruteForceCursor {
    /// The kernel supplies its retained query to each subsequent step.
    /// Physically drop this duplicate before returning its original invoice.
    #[doc(hidden)]
    pub fn release_query_copy(&mut self, mut release: impl FnMut(usize)) {
        let query = std::mem::take(&mut self.query);
        let bytes = query.capacity() * std::mem::size_of::<f32>();
        drop(query);
        self.retained_bytes -= bytes;
        release(bytes);
    }

    /// A pending score checks its scalar row and merges a heap; a prepared
    /// cursor only returns scalar scores. Neither phase decodes vector bytes.
    #[doc(hidden)]
    pub fn needs_vector_decode(&self) -> bool {
        !self.prepared && self.pending_score.is_none()
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Consume the continuation and report the bytes still owed back. A
    /// discarded cursor keeps its charge until the caller releases this
    /// amount, so disposing through this method is what balances it.
    pub fn into_retained_bytes(self) -> usize {
        self.retained_bytes
    }

    /// End scan-only ownership before constructing the final disclosure.
    /// Move the original source array and its invoice; no source copy occurs.
    #[doc(hidden)]
    pub fn into_disclosure_groups(
        self,
        mut groups: BoundedVectorCandidateGroups,
    ) -> (BoundedVectorCandidateGroups, usize) {
        let Self {
            snapshot_sources,
            retained_bytes,
            ..
        } = self;
        let scan_bytes = retained_bytes - groups.retained_bytes;
        groups.sources = snapshot_sources;
        (groups, scan_bytes)
    }

    pub fn is_exhausted(&self) -> bool {
        self.prepared && self.output_position >= self.scored.len()
    }

    /// The canonical ranking key of the Row step this cursor returned last:
    /// the prepared element just before the output position. No rescoring and
    /// no separate copy; `None` before the first Row step.
    #[doc(hidden)]
    pub fn last_row_canonical_score(&self) -> Option<f64> {
        if !self.prepared {
            return None;
        }
        self.output_position
            .checked_sub(1)
            .and_then(|at| self.scored.get(at))
            .map(|&(_, key)| key)
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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn checked_vector_add(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_add(right).ok_or_else(|| {
        Error::Other(
            (format!("bounded vector {operation} memory size exceeds the native address space"))
                .into(),
        )
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn checked_vector_mul(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_mul(right).ok_or_else(|| {
        Error::Other(
            (format!("bounded vector {operation} memory size exceeds the native address space"))
                .into(),
        )
    })
}

/// Admit the entire allocation before growth and return its actual capacity
/// invoice. A failing allocation/reconciliation drops storage before refund.
fn allocate_vector_slots<E, T>(
    capacity: usize,
    operation: &str,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<T>, usize), E>
where
    E: From<Error>,
{
    let requested =
        checked_vector_mul(capacity, std::mem::size_of::<T>(), operation).map_err(E::from)?;
    if requested != 0 {
        before_retain(requested)?;
    }
    let mut values = Vec::new();
    if values.try_reserve_exact(capacity).is_err() {
        drop(values);
        release_retained(requested);
        return Err(E::from(Error::Other(
            (format!("bounded vector {operation} allocation failed")).into(),
        )));
    }
    let actual = match checked_vector_mul(values.capacity(), std::mem::size_of::<T>(), operation) {
        Ok(bytes) => bytes,
        Err(error) => {
            drop(values);
            release_retained(requested);
            return Err(E::from(error));
        }
    };
    if actual < requested {
        drop(values);
        release_retained(requested);
        return Err(E::from(Error::Other(
            (format!("bounded vector {operation} capacity moved backwards")).into(),
        )));
    }
    if actual > requested
        && let Err(error) = before_retain(actual - requested)
    {
        drop(values);
        release_retained(requested);
        return Err(error);
    }
    Ok((values, actual))
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
    let (mut values, actual) =
        allocate_vector_slots(source.len(), operation, before_retain, release_retained)?;
    let mut position = 0usize;
    while position < source.len() {
        if let Err(error) = before_work() {
            drop(values);
            release_retained(actual);
            return Err(error);
        }
        let value = match source.get(position) {
            Some(value) => *value,
            None => {
                drop(values);
                release_retained(actual);
                return Err(E::from(Error::Other(
                    (format!("bounded vector {operation} source position changed")).into(),
                )));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                drop(values);
                release_retained(actual);
                return Err(E::from(Error::Other(
                    (format!("bounded vector {operation} source position overflow")).into(),
                )));
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
        drop(owned);
        release_retained(requested);
        return Err(E::from(Error::Other(
            (format!("bounded vector {operation} allocation failed")).into(),
        )));
    }
    let actual = owned.capacity();
    if actual < requested {
        drop(owned);
        release_retained(requested);
        return Err(E::from(Error::Other(
            (format!("bounded vector {operation} capacity moved backwards")).into(),
        )));
    }
    if actual > requested
        && let Err(error) = before_retain(actual - requested)
    {
        drop(owned);
        release_retained(requested);
        return Err(error);
    }
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
        "vector trace table name",
        before_retain,
        release_retained,
    )?;
    let (column, column_bytes) = match allocate_vector_string(
        &source.column,
        "vector trace column name",
        before_retain,
        release_retained,
    ) {
        Ok(column) => column,
        Err(error) => {
            drop(table);
            release_retained(table_bytes);
            return Err(error);
        }
    };
    let retained_bytes = match checked_vector_add(table_bytes, column_bytes, "vector trace index") {
        Ok(bytes) => bytes,
        Err(error) => {
            drop(column);
            release_retained(column_bytes);
            drop(table);
            release_retained(table_bytes);
            return Err(E::from(error));
        }
    };
    Ok((VectorIndexRef { table, column }, retained_bytes))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn next_vector_capacity(current: usize, required: usize) -> Result<usize> {
    if current == 0 {
        return Ok(required);
    }
    Ok(current
        .checked_mul(2)
        .ok_or_else(|| Error::Other(("bounded vector capacity overflow".to_string()).into()))?
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
        return Err(E::from(Error::Other(
            (format!("bounded vector {operation} allocation failed")).into(),
        )));
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
        return Err(E::from(Error::Other(
            (format!("bounded vector {operation} capacity moved backwards")).into(),
        )));
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
                    E::from(Error::Other(
                        (format!("bounded vector {operation} retained-memory overflow")).into(),
                    ))
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
                return Err(E::from(Error::Other(
                    (format!("bounded vector {operation} changed during migration")).into(),
                )));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                drop(replacement);
                release_retained(actual);
                return Err(E::from(Error::Other(
                    (format!("bounded vector {operation} migration position overflow")).into(),
                )));
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

fn allocate_vector_row_ids<E, S>(
    source: &[(RowId, S)],
    operation: &str,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<RowId>, usize), E>
where
    E: From<Error>,
{
    let (mut row_ids, actual) =
        allocate_vector_slots(source.len(), operation, before_retain, release_retained)?;
    let mut position = 0usize;
    while position < source.len() {
        if let Err(error) = before_work() {
            drop(row_ids);
            release_retained(actual);
            return Err(error);
        }
        let row_id = match source.get(position) {
            Some((row_id, _)) => *row_id,
            None => {
                drop(row_ids);
                release_retained(actual);
                return Err(E::from(Error::Other(
                    (format!("bounded vector {operation} source position changed")).into(),
                )));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                drop(row_ids);
                release_retained(actual);
                return Err(E::from(Error::Other(
                    (format!("bounded vector {operation} source position overflow")).into(),
                )));
            }
        };
        row_ids.push(row_id);
    }
    Ok((row_ids, actual))
}

fn allocate_snapshot_source_slots<E>(
    capacity: usize,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
where
    E: From<Error>,
{
    allocate_vector_slots(
        capacity,
        "snapshot vector sources",
        before_retain,
        release_retained,
    )
}

fn retain_snapshot_source<E>(
    sources: &mut Vec<BoundedSnapshotVectorSource>,
    key: &VectorPartitionKey,
    state: impl Into<crate::store::VectorQueryState>,
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
            ("bounded vector partition scope exceeds its declared maximum".to_string()).into(),
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
                ("bounded vector partition-key retained-memory overflow".to_string()).into(),
            )));
        }
    };
    sources.insert(
        position,
        BoundedSnapshotVectorSource {
            partition_key: key.clone(),
            state: state.into(),
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

enum ExecutorTransactions<S: WriteSetApplicator> {
    Legacy(Arc<TransactionManager<S>>),
    Owned(contextdb_tx::manager::EngineTransactionHandle<S>),
}
impl<S: WriteSetApplicator> ExecutorTransactions<S> {
    fn snapshot(&self) -> SnapshotId {
        match self {
            Self::Legacy(manager) => manager.snapshot(),
            Self::Owned(handle) => handle.with_transactions(|transactions| transactions.snapshot()),
        }
    }
    // Existing standalone/public mutations retain their genuine Legacy route.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn with_write_set<R>(
        &self,
        tx: TxId,
        f: impl FnOnce(&mut contextdb_tx::WriteSet) -> R,
    ) -> Result<R> {
        match self {
            Self::Legacy(manager) => manager.with_write_set(tx, f),
            Self::Owned(_) => Err(Error::InvalidStateTransition(
                "owned executor requires its admitted engine producer".into(),
            )),
        }
    }
}

pub struct MemVectorExecutor<S: WriteSetApplicator> {
    store: Arc<VectorStore>,
    tx_mgr: ExecutorTransactions<S>,
    _accountant: crate::memory_budget::MemoryBudgetOwner,
}

impl<S: WriteSetApplicator> MemVectorExecutor<S> {
    #[doc(hidden)]
    pub fn new_engine(
        store: Arc<VectorStore>,
        transactions: contextdb_tx::manager::EngineTransactionHandle<S>,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Self {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        Self {
            store,
            tx_mgr: ExecutorTransactions::Owned(transactions),
            _accountant: accountant,
        }
    }
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn stage_engine_vector(
        &self,
        tx: TxId,
        entry: VectorEntry,
        scalar: Option<(RowId, ColName)>,
        invoice: contextdb_tx::staged_source::OriginalVectorInvoice,
    ) -> Result<()> {
        struct Original {
            entry: VectorEntry,
            invoice: contextdb_tx::staged_source::OriginalVectorInvoice,
        }
        let original = Original { entry, invoice };
        let ExecutorTransactions::Owned(handle) = &self.tx_mgr else {
            return Err(Error::InvalidStateTransition(
                "engine vector requires owned executor".into(),
            ));
        };
        self.store
            .validate_vector(&original.entry.index, original.entry.vector.len())?;
        handle.with_transactions(|transactions| {
            transactions.with_engine(tx, |guard| {
                let prepared = guard.prepare_vector(original.entry, scalar, original.invoice)?;
                guard.publish(prepared);
                Ok(())
            })
        })
    }

    /// The accountant must belong to the actual executing statement.
    pub fn new(
        store: Arc<VectorStore>,
        tx_mgr: Arc<TransactionManager<S>>,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Self {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        Self::new_with_accountant(store, tx_mgr, accountant)
    }

    pub fn new_with_accountant(
        store: Arc<VectorStore>,
        tx_mgr: Arc<TransactionManager<S>>,
        accountant: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Self {
        let accountant: crate::memory_budget::MemoryBudgetOwner = accountant.into();

        Self {
            store,
            tx_mgr: ExecutorTransactions::Legacy(tx_mgr),
            _accountant: accountant,
        }
    }

    fn retain_paged_snapshot_sources<E: From<Error>>(
        &self,
        sources: &crate::store::PagedVectorSourceSelection,
        prefixes: Option<&[VectorPartitionKey]>,
        before_work: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E> {
        let matches = |key: &VectorPartitionKey| {
            prefixes.is_none_or(|prefixes| {
                prefixes
                    .iter()
                    .any(|prefix| partition_key_has_prefix(key, prefix))
            })
        };
        let count = if prefixes.is_none() {
            sources.len()
        } else {
            let mut count = 0usize;
            sources.visit_checked(owner, before_work, |key, _| {
                if matches(key) {
                    count = count.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other("vector source count overflow".into()))
                    })?;
                }
                Ok::<(), E>(())
            })?;
            count
        };
        let (mut selected, mut retained_bytes) =
            allocate_snapshot_source_slots(count, before_retain, release_retained)?;
        let result = sources.visit_checked(owner, before_work, |key, state| {
            if matches(key) {
                retain_snapshot_source(
                    &mut selected,
                    key,
                    state,
                    &mut retained_bytes,
                    &mut || Ok::<(), E>(()),
                    before_retain,
                    release_retained,
                )?;
            }
            Ok::<(), E>(())
        });
        if let Err(error) = result {
            drop(selected);
            release_retained(retained_bytes);
            return Err(error);
        }
        Ok((selected, retained_bytes))
    }

    fn all_bounded_snapshot_sources<E>(
        &self,
        index: &VectorIndexRef,
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
    where
        E: From<Error>,
    {
        if let Some(sources) = self
            .store
            .capture_paged_partition_sources(index, owner)
            .map_err(E::from)?
        {
            return self.retain_paged_snapshot_sources(
                &sources,
                None,
                before_source_entry,
                before_retain,
                release_retained,
                owner,
            );
        }
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

    #[expect(
        clippy::too_many_arguments,
        reason = "Source capture keeps layout, snapshot, selection, admission and cancellation owners explicit."
    )]
    fn bounded_scoped_snapshot_sources<E>(
        &self,
        index: &VectorIndexRef,
        layout: &crate::store::VectorIndexLayout,
        prefixes: &[VectorPartitionKey],
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
    where
        E: From<Error>,
    {
        if let Some(sources) = self
            .store
            .capture_paged_partition_sources(index, owner)
            .map_err(E::from)?
        {
            if prefixes
                .iter()
                .all(|prefix| prefix.components().len() == layout.partition_key_columns.len())
            {
                let (mut selected, mut retained_bytes) = allocate_snapshot_source_slots(
                    prefixes.len(),
                    before_retain,
                    release_retained,
                )?;
                let result = (|| {
                    for prefix in prefixes {
                        before_source_entry()?;
                        if let Some(state) = sources.probe(prefix, owner).map_err(E::from)? {
                            retain_snapshot_source(
                                &mut selected,
                                prefix,
                                state,
                                &mut retained_bytes,
                                before_source_entry,
                                before_retain,
                                release_retained,
                            )?;
                        }
                    }
                    Ok::<(), E>(())
                })();
                if let Err(error) = result {
                    drop(selected);
                    release_retained(retained_bytes);
                    return Err(error);
                }
                return Ok((selected, retained_bytes));
            }
            return self.retain_paged_snapshot_sources(
                &sources,
                Some(prefixes),
                before_source_entry,
                before_retain,
                release_retained,
                owner,
            );
        }
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

    fn with_selected_partition_prefixes<T>(
        &self,
        index: &VectorIndexRef,
        operation: impl FnOnce(Option<&[VectorPartitionKey]>) -> T,
    ) -> T {
        PARTITION_SEARCH_SCOPES.with(|scopes| {
            let scopes = scopes.borrow();
            let prefixes = scopes
                .iter()
                .rev()
                .find(|(selected, _)| selected == index)
                .map(|(_, prefixes)| prefixes.as_slice());
            operation(prefixes)
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
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E>
    where
        E: From<Error>,
    {
        self.with_selected_partition_prefixes(index, |prefixes| {
            if let Some(prefixes) = prefixes {
                return self.bounded_scoped_snapshot_sources(
                    index,
                    layout,
                    prefixes,
                    before_source_entry,
                    before_retain,
                    release_retained,
                    owner,
                );
            }
            self.all_bounded_snapshot_sources(
                index,
                before_source_entry,
                before_retain,
                release_retained,
                owner,
            )
        })
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
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<BoundedVectorCandidateGroups, E>
    where
        E: From<Error>,
    {
        before_checkpoint()?;
        let layout = self
            .store
            .scoped_index_layout(index, owner)
            .map_err(E::from)?;
        let filtered = candidates.is_some();
        if candidates.is_some_and(<[u64]>::is_empty) {
            return Ok(BoundedVectorCandidateGroups {
                snapshot,
                filtered,
                row_checked: false,
                sources: Vec::new(),
                aggregate_allowed_count: 0,
                retained_bytes: 0,
            });
        }

        let (discover_sources, mut sources, mut retained_bytes) = self
            .with_selected_partition_prefixes(index, |prefixes| {
                let discover_sources = layout.is_partitioned() && filtered && prefixes.is_none();
                let (sources, retained_bytes) = if let Some(prefixes) = prefixes {
                    self.bounded_scoped_snapshot_sources(
                        index,
                        &layout,
                        prefixes,
                        &mut before_checkpoint,
                        &mut before_retain,
                        &mut release_retained,
                        owner,
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
                        owner,
                    )?
                };
                Ok::<_, E>((discover_sources, sources, retained_bytes))
            })?;

        let repeat_global_candidate_pass =
            bounded_candidate_grouping_repeats_global_pass_for_test();
        let suppress_placement_charge =
            bounded_candidate_grouping_suppresses_placement_charge_for_test();
        let grouped =
            (|| -> std::result::Result<(), E> {
                if let Some(candidate_ids) = candidates {
                    for raw_row_id in candidate_ids {
                        let row_id = RowId(*raw_row_id);
                        self.store
                            .with_query_snapshot_visible_partition_for_row::<E, (), _>(
                                index,
                                row_id,
                                snapshot,
                                &mut before_checkpoint,
                                |key, state, before_checkpoint, _owner| {
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
                                                    E::from(Error::Other((
                                                    "bounded vector candidate source disappeared"
                                                        .to_string()).into()))
                                                })?
                                        }
                                    };
                                    if !suppress_placement_charge {
                                        before_placement()?;
                                    }
                                    let source = sources.get_mut(position).ok_or_else(|| {
                                        E::from(Error::Other(
                                            ("bounded vector candidate group changed".to_string())
                                                .into(),
                                        ))
                                    })?;
                                    let local = source.candidate_ids.get_or_insert_with(Vec::new);
                                    let required = local.len().checked_add(1).ok_or_else(|| {
                                        E::from(Error::Other(
                                            ("bounded vector candidate group length overflow"
                                                .to_string())
                                            .into(),
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
                                            E::from(Error::Other((
                                            "bounded vector partition candidate count overflow"
                                                .to_string()).into()))
                                        })?;
                                    Ok(Some(()))
                                },
                                owner,
                            )?;
                    }
                    if repeat_global_candidate_pass {
                        let physical_partition_count = self
                            .store
                            .query_partition_source_count(index, owner)
                            .map_err(E::from)?;
                        for _ in 1..physical_partition_count {
                            for raw_row_id in candidate_ids {
                                self.store
                                    .with_query_snapshot_visible_partition_for_row::<E, (), _>(
                                        index,
                                        RowId(*raw_row_id),
                                        snapshot,
                                        &mut before_checkpoint,
                                        |_key, _state, _before_checkpoint, _owner| Ok(Some(())),
                                        owner,
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
                            owner,
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
                        owner,
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
        Ok(BoundedVectorCandidateGroups {
            snapshot,
            filtered,
            row_checked: false,
            sources,
            aggregate_allowed_count,
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
        owner: &mut crate::VectorCopyAdmission,
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
            owner,
        )?;
        self.bounded_brute_force_cursor_grouped(
            index,
            query,
            k,
            groups,
            before_source_entry,
            before_retain,
            release_retained,
            owner,
        )
    }

    /// Create a fallible, owned brute-force continuation from the query's one
    /// admitted candidate grouping. Query allocation is reconciled before copy;
    /// partition-local candidate slices transfer directly into the cursor.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn bounded_brute_force_cursor_grouped<E>(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: BoundedVectorCandidateGroups,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
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
            drop(snapshot_sources);
            release_retained(group_bytes);
            return Err(error);
        }
        let layout = match self.store.scoped_index_layout(&index, owner) {
            Ok(layout) => layout,
            Err(error) => {
                drop(snapshot_sources);
                release_retained(group_bytes);
                return Err(E::from(error));
            }
        };
        if query.len() != layout.dimension {
            drop(snapshot_sources);
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
                    drop(snapshot_sources);
                    release_retained(group_bytes);
                    return Err(E::from(error));
                }
            };
        if let Err(error) = before_retain(index_bytes) {
            drop(snapshot_sources);
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
                drop(index);
                release_retained(index_bytes);
                drop(snapshot_sources);
                release_retained(group_bytes);
                return Err(error);
            }
        };
        let mut retained_bytes = match checked_vector_add(index_bytes, query_bytes, "query state")
            .and_then(|bytes| checked_vector_add(bytes, group_bytes, "candidate groups"))
        {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(owned_query);
                release_retained(query_bytes);
                drop(index);
                release_retained(index_bytes);
                drop(snapshot_sources);
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
            drop(index);
            release_retained(retained_bytes);
            return Err(error);
        }
        Ok(BoundedBruteForceCursor {
            one_pass: crate::store::OnePassSession::default(),
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
            historical_partition: None,
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
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<BoundedVectorStep, E>
    where
        E: From<Error>,
    {
        let before_source_entry = std::cell::RefCell::new(before_source_entry);
        self.bounded_brute_force_step_charged(
            cursor,
            None,
            || (before_source_entry.borrow_mut())(),
            || (before_source_entry.borrow_mut())(),
            || (before_source_entry.borrow_mut())(),
            before_distance,
            before_retain,
            release_retained,
            owner,
        )
    }

    /// Advance exactly one candidate/retained-score/output boundary, with a
    /// distinct callback for the exhaustive physical-entry charge.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_brute_force_step_charged<E>(
        &self,
        cursor: &mut BoundedBruteForceCursor,
        admit_row: Option<&mut dyn FnMut(RowId) -> std::result::Result<bool, E>>,
        before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        before_precharged_candidate: impl FnMut() -> std::result::Result<(), E>,
        before_exhaustive_candidate: impl FnMut() -> std::result::Result<(), E>,
        before_distance: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<BoundedVectorStep, E>
    where
        E: From<Error>,
    {
        let query = std::mem::take(&mut cursor.query);
        let result = self.bounded_brute_force_step_with_query(
            cursor,
            &query,
            admit_row,
            before_source_entry,
            before_precharged_candidate,
            before_exhaustive_candidate,
            before_distance,
            before_retain,
            release_retained,
            owner,
        );
        cursor.query = query;
        result
    }

    /// Kernel path: the calling continuation retains and funds the query.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_brute_force_step_with_query<E>(
        &self,
        cursor: &mut BoundedBruteForceCursor,
        query: &[f32],
        mut admit_row: Option<&mut dyn FnMut(RowId) -> std::result::Result<bool, E>>,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_precharged_candidate: impl FnMut() -> std::result::Result<(), E>,
        mut before_exhaustive_candidate: impl FnMut() -> std::result::Result<(), E>,
        mut before_distance: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
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
                    ("bounded vector output position overflow".to_string()).into(),
                ))
            })?;
            return Ok(BoundedVectorStep::Row(row_id, score as f32));
        }
        // Exhaustive membership, authentication and body reads share the
        // same cache-growth allowance across every resumed step and partition.
        let state = cursor
            .snapshot_sources
            .get(cursor.source_position)
            .map(|source| source.state.clone());
        let _one_pass = state.and_then(|state| state.one_pass_reads(&mut cursor.one_pass));

        if let Some(candidate) = cursor.pending_score {
            if admit_row.is_some() {
                // Body scoring finished on the preceding step. Its native
                // handles are only reuse, and must not overlap the scalar
                // predicate's independently owned row window under pressure.
                cursor.body_reads.release_scan();
            }
            // A statement filter checked row by row is asked before a scored
            // row competes.
            if let Some(admit_row) = admit_row.as_mut()
                && !admit_row(candidate.0)?
            {
                cursor.pending_score = None;
                return Ok(BoundedVectorStep::Pending);
            }
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
                    ("bounded filtered vector source lost its candidate group".to_string()).into(),
                ))
            })?;
            let Some(raw_row_id) = candidate_ids.get(cursor.position).copied() else {
                cursor.source_position =
                    cursor.source_position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other(
                            ("bounded vector partition position overflow".to_string()).into(),
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
                    query,
                    &mut cursor.body_reads,
                    &mut before_source_entry,
                    &mut before_distance,
                    &mut before_retain,
                    &mut release_retained,
                    owner,
                )?
                .map(|score| (row_id, score));
            cursor.position = cursor.position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    ("bounded vector candidate position overflow".to_string()).into(),
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
            match source.state.historical_partition(owner) {
                Ok(Some(partition)) => {
                    cursor.state_end = Some(partition.len());
                    cursor.streaming_source = false;
                    cursor.historical_partition = Some(partition);
                }
                Ok(None) => {}
                Err(Error::MemoryBudgetExceeded { .. })
                    if cursor.stream_when_refused && source.state.streams_raw_bodies() =>
                {
                    cursor.streaming_source = true;
                    cursor.state_end = Some(usize::MAX);
                }
                Err(error) => return Err(E::from(error)),
            }
            let durable_streams = source.state.durable_membership_source().is_some()
                && !source
                    .state
                    .try_cache_durable_bodies(owner)
                    .map_err(E::from)?;
            if let Some(memberships) = source
                .state
                .durable_membership_source()
                .filter(|_| durable_streams)
            {
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
                                    ("membership cursor reservation overflow".to_string()).into(),
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
                            ("membership cursor retained bytes overflow".to_string()).into(),
                        )));
                    }
                };
                cursor.membership_reserved_bytes = reserved;
                cursor.membership_cursor = Some(opened);
                cursor.streaming_source = true;
                cursor.state_end = Some(usize::MAX);
            } else if cursor.state_end.is_none() {
                match source.state.ensure_raw_vectors_loaded(owner) {
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
            cursor.historical_partition.take();
            cursor.source_position = cursor.source_position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    ("bounded vector partition position overflow".to_string()).into(),
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
                let mut additional_reserved = 0usize;
                let retained_before = cursor.retained_bytes;
                let membership_before = cursor.membership_reserved_bytes;
                let mut interrupted = None;
                let next = if let Some(entry) = cursor.pending_membership.as_ref() {
                    Ok(Some(entry.clone()))
                } else {
                    memberships
                        .next_with_body_identity(&mut |request| {
                            let result = match request {
                                crate::store::DormantVectorLoadRequest::Reserve(bytes) => {
                                    match additional_reserved.checked_add(bytes) {
                                        Some(total)
                                            if retained_before.checked_add(total).is_some()
                                                && membership_before
                                                    .checked_add(total)
                                                    .is_some() =>
                                        {
                                            before_retain(bytes)
                                                .map(|()| additional_reserved = total)
                                        }
                                        _ => Err(E::from(Error::Other(
                                            "membership window reservation overflow".into(),
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
                        })
                        .map(|next| {
                            next.map(|(interval, body)| {
                                (
                                    body,
                                    interval.row_id,
                                    interval.created_tx,
                                    interval.deleted_tx,
                                )
                            })
                        })
                };
                cursor.retained_bytes += additional_reserved;
                cursor.membership_reserved_bytes += additional_reserved;
                if let Some(error) = interrupted {
                    return Err(error);
                }
                cursor.pending_membership = next.map_err(E::from)?;
                cursor
                    .pending_membership
                    .as_ref()
                    .map(|(_, row, visible_from, deleted)| {
                        (visible_from.0 <= cursor.snapshot.0
                            && deleted.is_none_or(|tx| tx.0 > cursor.snapshot.0))
                        .then_some(*row)
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
                            ("bounded vector partition position overflow".to_string()).into(),
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
            if visible.is_some()
                && cursor.pending_membership.is_some()
                && let Some(memberships) = cursor.membership_cursor.as_deref()
            {
                // The stream already authenticated this exact body identity.
                // Bind its existing receipt to the scan at the same root and
                // route instead of decoding a second membership for the body.
                source
                    .state
                    .bind_streamed_membership(memberships, &mut cursor.body_reads, owner)
                    .map_err(E::from)?;
            }
            cursor.pending_score = match visible {
                Some(row_id) if cursor.pending_membership.is_some() => source
                    .state
                    .bounded_score_streamed_body(
                        &cursor._index,
                        cursor.pending_membership.as_ref().unwrap().0.clone(),
                        cursor.snapshot,
                        query,
                        &mut cursor.body_reads,
                        &mut before_source_entry,
                        &mut before_distance,
                        &mut before_retain,
                        &mut release_retained,
                        owner,
                    )?
                    .map(|score| (row_id, score)),
                Some(row_id) => source
                    .state
                    .bounded_score_stored_body(
                        &cursor._index,
                        row_id,
                        cursor.snapshot,
                        query,
                        &mut cursor.body_reads,
                        &mut before_source_entry,
                        &mut before_distance,
                        &mut before_retain,
                        &mut release_retained,
                        owner,
                    )?
                    .map(|score| (row_id, score)),
                None => None,
            };
            cursor.pending_membership.take();
            cursor.position = cursor.position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    ("bounded vector candidate position overflow".to_string()).into(),
                ))
            })?;
            return Ok(BoundedVectorStep::Pending);
        }
        // This is the one exhaustive-scan charge for the stored entry about
        // to be read. Visibility is deliberately checked afterwards: dead or
        // future versions still consumed this physical scan step.
        before_exhaustive_candidate()?;
        let scored = if let Some(partition) = cursor.historical_partition.as_ref() {
            if partition.entry(position).is_some_and(|entry| {
                entry.membership.visible_from.0 <= cursor.snapshot.0
                    && entry
                        .membership
                        .deleted_tx
                        .is_none_or(|tx| tx.0 > cursor.snapshot.0)
            }) {
                before_distance()?;
            }
            source
                .state
                .historical_partition_score(partition, position, cursor.snapshot, query)
                .map_err(E::from)?
        } else {
            source
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
                        entry.vector.canonical_similarity(query),
                    )))
                })?
        };
        cursor.position = cursor.position.checked_add(1).ok_or_else(|| {
            E::from(Error::Other(
                ("bounded vector candidate position overflow".to_string()).into(),
            ))
        })?;
        cursor.pending_score = scored;
        Ok(BoundedVectorStep::Pending)
    }

    /// Enumerate scoped membership without loading or scoring vector bodies.
    /// Every directory touch and retained source handle uses the caller's budget.
    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    pub fn bounded_visit_visible_ids<E>(
        &self,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        mut before_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        mut visit: impl FnMut(RowId) -> std::result::Result<(), E>,
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        before_entry()?;
        let layout = self
            .store
            .scoped_index_layout(index, owner)
            .map_err(E::from)?;
        let (sources, bytes) = self.bounded_snapshot_sources(
            index,
            &layout,
            &mut before_entry,
            &mut before_retain,
            &mut release_retained,
            owner,
        )?;
        let result = sources.iter().try_for_each(|source| {
            source.state.bounded_visit_visible_ids_charged(
                snapshot,
                &mut before_entry,
                &mut before_retain,
                &mut release_retained,
                &mut visit,
                owner,
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
        owner: &mut crate::VectorCopyAdmission,
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
            owner,
        )?;
        let count = groups.aggregate_allowed_count();
        release_retained(groups.into_retained_bytes());
        Ok(count)
    }

    /// Narrow through the captured reverse and forward membership at this snapshot.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn selected_partition_sources(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<Vec<BoundedSnapshotVectorSource>> {
        if candidates.is_some_and(|ids| ids.is_empty()) {
            return Ok(Vec::new());
        }
        let partitioned = self
            .store
            .with_index_layout(index, |layout| layout.is_partitioned())?;
        PARTITION_SEARCH_SCOPES.with(|scopes| {
            let scopes = scopes.borrow();
            let prefixes = partitioned
                .then(|| {
                    scopes
                        .iter()
                        .rev()
                        .find(|(scoped, _)| scoped == index)
                        .map(|(_, prefixes)| prefixes)
                })
                .flatten();
            let mut routes = Vec::<BoundedSnapshotVectorSource>::new();
            let mut narrowed = false;
            if partitioned
                && prefixes.is_none()
                && let Some(candidates) = candidates
            {
                let capacity = usize::try_from(candidates.len())
                    .map_err(|_| Error::Other("vector candidate capacity overflow".into()))?;
                owner.reserve(
                    capacity
                        .checked_mul(std::mem::size_of::<BoundedSnapshotVectorSource>())
                        .ok_or_else(|| Error::Other("vector candidate capacity overflow".into()))?,
                )?;
                routes = Vec::with_capacity(capacity);
                narrowed = true;
                for id in candidates.iter() {
                    let row = RowId(id);
                    let selected = self.store.with_query_snapshot_visible_partition_for_row(
                        index,
                        row,
                        snapshot,
                        &mut || Ok::<(), Error>(()),
                        |key, state, _, owner| {
                            owner.reserve(key.estimated_bytes())?;
                            Ok(Some((key.clone(), state.clone())))
                        },
                        owner,
                    )?;
                    let Some((key, state)) = selected else {
                        narrowed = false;
                        break;
                    };
                    routes.push(BoundedSnapshotVectorSource {
                        partition_key: key,
                        state,
                        candidate_ids: None,
                        point_load: false,
                        candidate_ids_cover_all_live: false,
                        allowed_count: 0,
                        live_count: 0,
                    });
                }
            }
            if narrowed {
                routes.sort_unstable_by(|a, b| a.partition_key.cmp(&b.partition_key));
                routes.dedup_by(|a, b| a.partition_key == b.partition_key);
                return Ok(routes);
            }
            drop(routes);
            let selected = |key: &VectorPartitionKey| {
                prefixes.is_none_or(|prefixes| {
                    prefixes
                        .iter()
                        .any(|prefix| partition_key_has_prefix(key, prefix))
                })
            };
            let mut count = 0usize;
            self.store
                .visit_query_partition_sources(index, owner, |key, _, _| {
                    if selected(key) {
                        count = count.checked_add(1).ok_or_else(|| {
                            Error::Other("vector source capacity overflow".into())
                        })?;
                    }
                    Ok::<(), Error>(())
                })?;
            owner.reserve(
                count
                    .checked_mul(std::mem::size_of::<BoundedSnapshotVectorSource>())
                    .ok_or_else(|| Error::Other("vector source capacity overflow".into()))?,
            )?;
            let mut result = Vec::with_capacity(count);
            self.store
                .visit_query_partition_sources(index, owner, |key, state, owner| {
                    if selected(key) {
                        if result.len() == count {
                            return Err(Error::Other(
                                "vector source selection changed after its admitted count".into(),
                            ));
                        }
                        owner.reserve(key.estimated_bytes())?;
                        result.push(BoundedSnapshotVectorSource {
                            partition_key: key.clone(),
                            state,
                            candidate_ids: None,
                            point_load: false,
                            candidate_ids_cover_all_live: false,
                            allowed_count: 0,
                            live_count: 0,
                        });
                    }
                    Ok::<(), Error>(())
                })?;
            Ok(result)
        })
    }

    /// Borrow resident facts within the current prefix scope. Authorization
    /// is applied by the visitor's resident directory counters.
    ///
    /// Durable sources stream authenticated memberships and one admitted body
    /// at a time. AUTO may also stream a resident loader when bulk admission
    /// refuses; explicit EXACT preserves that loader's typed refusal.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn brute_force_search_sources(
        &self,
        index: &VectorIndexRef,
        sources: &[BoundedSnapshotVectorSource],
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        stream_when_refused: bool,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<(CanonicalScores, usize)> {
        let mut scored = CanonicalScores::new(index, owner.accountant().clone())?.for_exact();
        // Grow only for rows this pass actually authenticates. A historical
        // snapshot needs no preliminary membership-count pass.
        let capacity = k;
        // Evaluate every eligible vector, retaining only the requested best
        // scores. Ranking callers request their complete candidate set here.
        let mut consider = |score: (RowId, f64)| -> Result<()> {
            if capacity == 0 {
                return Ok(());
            }
            if scored.len() < capacity {
                if scored.len() == scored.rows.capacity() {
                    let next_capacity = scored
                        .rows
                        .capacity()
                        .checked_mul(2)
                        .unwrap_or(capacity)
                        .max(1)
                        .min(capacity);
                    scored.reserve(next_capacity - scored.len())?;
                }
                push_bounded_score_heap(&mut scored.rows, score)?;
            } else if vector_score_quality(&score, &scored.rows[0]) == std::cmp::Ordering::Greater {
                replace_bounded_score_heap_root(&mut scored.rows, score)?;
            }
            Ok(())
        };
        let mut partition_count = 0usize;
        let mut session = crate::store::OnePassSession::default();
        for source in sources {
            // Include historical/cache-load preflight in the same allowance
            // as the streamed memberships and bodies of every partition.
            let _one_pass = source.state.one_pass_reads(&mut session);
            let mut source_has_rows = false;
            let mut source_consider = |score| {
                source_has_rows = true;
                consider(score)
            };
            match source.state.historical_partition(owner) {
                Ok(Some(partition)) => {
                    for position in 0..partition.len() {
                        if candidates.is_some_and(|candidates| {
                            partition
                                .entry(position)
                                .is_none_or(|entry| !candidates.contains(entry.entry.row_id.0))
                        }) {
                            continue;
                        }
                        if let Some(score) = source
                            .state
                            .historical_partition_score(&partition, position, snapshot, query)?
                        {
                            source_consider(score)?;
                        }
                    }
                    partition_count += usize::from(source_has_rows);
                    continue;
                }
                Ok(None) => {}
                Err(Error::MemoryBudgetExceeded { .. })
                    if stream_when_refused && source.state.streams_raw_bodies() =>
                {
                    self.stream_source_bodies(
                        index,
                        source,
                        query,
                        snapshot,
                        candidates,
                        &mut source_consider,
                        owner,
                    )?;
                    partition_count += usize::from(source_has_rows);
                    continue;
                }
                Err(error) => return Err(error),
            }
            if source.state.durable_membership_source().is_some()
                && !source.state.try_cache_durable_bodies(owner)?
            {
                self.stream_source_bodies(
                    index,
                    source,
                    query,
                    snapshot,
                    candidates,
                    &mut source_consider,
                    owner,
                )?;
                partition_count += usize::from(source_has_rows);
                continue;
            }
            match source.state.ensure_raw_vectors_loaded(owner) {
                Ok(()) => {}
                Err(Error::MemoryBudgetExceeded { .. })
                    if stream_when_refused && source.state.streams_raw_bodies() =>
                {
                    self.stream_source_bodies(
                        index,
                        source,
                        query,
                        snapshot,
                        candidates,
                        &mut source_consider,
                        owner,
                    )?;
                    partition_count += usize::from(source_has_rows);
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
                    source_consider((entry.row_id, entry.vector.canonical_similarity(query)))?;
                }
                Ok::<(), Error>(())
            })?;
            partition_count += usize::from(source_has_rows);
        }
        scored.sort_canonical();
        scored.truncate(k);
        Ok((scored, partition_count))
    }

    /// Compare every row one partition's snapshot shows by reading each
    /// row's own stored vector through a point read, admitting each body
    /// against the database's memory before it is read and returning it
    /// straight after, so the comparison never needs room for the partition.
    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn stream_source_bodies(
        &self,
        index: &VectorIndexRef,
        source: &BoundedSnapshotVectorSource,
        query: &[f32],
        snapshot: SnapshotId,
        candidates: Option<&RoaringTreemap>,
        consider: &mut impl FnMut((RowId, f64)) -> Result<()>,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<()> {
        let accountant = owner.accountant().clone();
        let mut before_retain = |bytes: usize| -> Result<()> {
            accountant.try_allocate_for_static(
                bytes,
                "vector_search",
                "exhaustive_body_read",
                "Reduce the query scope, or raise MEMORY_LIMIT.",
            )
        };
        let mut release_retained = |bytes: usize| accountant.release(bytes);
        let durable = source.state.durable_membership_source();
        let mut bodies = if durable.is_some() {
            crate::store::RawVectorBodyReads::default()
        } else {
            crate::store::RawVectorBodyReads::one_pass()
        };
        // The caller holds one scan scope across preflight and every source.
        if let Some(memberships) = durable {
            let mut window = owner.scratch();
            let mut interrupted = None;
            let opened = memberships.open(&mut |request| {
                let result = match request {
                    crate::store::DormantVectorLoadRequest::Reserve(bytes) => window.reserve(bytes),
                    _ => Ok(()),
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
            let mut cursor = opened?;
            loop {
                let mut interrupted = None;
                let next = cursor.next_with_body_identity(&mut |request| {
                    let result = match request {
                        crate::store::DormantVectorLoadRequest::Reserve(bytes) => {
                            window.reserve(bytes)
                        }
                        _ => Ok(()),
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
                let Some((visible, identity)) = next? else {
                    break;
                };
                if candidates.is_some_and(|ids| !ids.contains(visible.row_id.0))
                    || visible.created_tx.0 > snapshot.0
                    || visible.deleted_tx.is_some_and(|tx| tx.0 <= snapshot.0)
                {
                    continue;
                }
                source
                    .state
                    .bind_streamed_membership(&*cursor, &mut bodies, owner)?;
                if let Some(score) = source.state.bounded_score_streamed_body(
                    index,
                    identity,
                    snapshot,
                    query,
                    &mut bodies,
                    &mut || Ok::<(), Error>(()),
                    &mut || Ok(()),
                    &mut before_retain,
                    &mut release_retained,
                    owner,
                )? {
                    consider((visible.row_id, score))?;
                }
            }
            return Ok(());
        }
        let mut position = 0usize;
        while let Some(visible) = source.state.visible_directory_row_at(position, snapshot) {
            position = position.checked_add(1).ok_or_else(|| {
                Error::Other(("exhaustive vector position overflow".to_string()).into())
            })?;
            let Some(row_id) = visible else {
                continue;
            };
            if candidates.is_some_and(|ids| !ids.contains(row_id.0)) {
                continue;
            }
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
                owner,
            )? {
                consider((row_id, score))?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn aggregate_authorized_source_count(
        sources: &[BoundedSnapshotVectorSource],
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<usize> {
        if let Some(candidates) = candidates {
            let mut count = 0usize;
            for id in candidates.iter() {
                for source in sources {
                    if source
                        .state
                        .try_directory_has_visible_row(RowId(id), snapshot, owner)?
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
                        .try_directory_visible_entry_count(snapshot, None, owner)?,
                )
                .ok_or_else(|| Error::Other("vector authorized count overflow".into()))
        })
    }

    /// Passive aggregate used by EXPLAIN's policy disclosure. It reads only
    /// the already-resident raw directories and honors the same authorization
    /// bitmap and selected partition prefixes as execution; it never loads a
    /// vector body or graph and never builds a route.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn authorized_visible_count_without_build(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<usize> {
        if candidates.is_some_and(|ids| ids.is_empty()) {
            return Ok(0);
        }
        let partitioned = self
            .store
            .with_index_layout(index, |layout| layout.is_partitioned())?;
        let mut metadata = owner.local_scratch();
        self.store.with_bulk_read(|| {
            PARTITION_SEARCH_SCOPES.with(|scopes| {
                let scopes = scopes.borrow();
                let prefixes = partitioned
                    .then(|| {
                        scopes
                            .iter()
                            .rev()
                            .find(|(scoped, _)| scoped == index)
                            .map(|(_, prefixes)| prefixes)
                    })
                    .flatten();
                let selected = |key: &VectorPartitionKey| {
                    prefixes.is_none_or(|prefixes| {
                        prefixes
                            .iter()
                            .any(|prefix| partition_key_has_prefix(key, prefix))
                    })
                };
                if partitioned && let Some(ids) = candidates {
                    let mut count = 0usize;
                    let mut fallback = false;
                    for id in ids.iter() {
                        let visible = match self.store.snapshot_visible_partition_matches(
                            index,
                            RowId(id),
                            snapshot,
                            selected,
                            metadata.as_mut().unwrap_or(owner),
                        )? {
                            Some(visible) => visible,
                            None => {
                                fallback = true;
                                break;
                            }
                        };
                        count = count.checked_add(usize::from(visible)).ok_or_else(|| {
                            Error::Other("vector authorized count overflow".into())
                        })?;
                    }
                    if !fallback {
                        return Ok(count);
                    }
                }
                // Query source windows are read without directory guards;
                // each retained state keeps its actual reader control invoice.
                let states =
                    crate::ScopedVectorCopy::new(metadata.as_mut().unwrap_or(owner), |copies| {
                        let capacity = self.store.query_partition_source_count(index, copies)?;
                        copies.reserve(
                            capacity
                                .checked_mul(std::mem::size_of::<crate::store::VectorQueryState>())
                                .ok_or_else(|| {
                                    Error::Other("vector source capacity overflow".into())
                                })?,
                        )?;
                        let mut states = Vec::with_capacity(capacity);
                        self.store.visit_query_partition_sources(
                            index,
                            copies,
                            |key, state, _| {
                                if selected(key) {
                                    if states.len() == capacity {
                                        return Err(Error::Other(
                                            "vector source count changed after admission".into(),
                                        ));
                                    }
                                    states.push(state);
                                }
                                Ok::<(), Error>(())
                            },
                        )?;
                        Ok(states)
                    })?;
                let mut count = 0usize;
                if let Some(ids) = candidates {
                    for id in ids.iter() {
                        for state in states.iter() {
                            if state.try_directory_has_visible_row(
                                RowId(id),
                                snapshot,
                                metadata.as_mut().unwrap_or(owner),
                            )? {
                                count = count.checked_add(1).ok_or_else(|| {
                                    Error::Other("vector authorized count overflow".into())
                                })?;
                                break;
                            }
                        }
                    }
                } else {
                    for state in states.iter() {
                        count = count
                            .checked_add(state.try_directory_visible_entry_count(
                                snapshot,
                                None,
                                metadata.as_mut().unwrap_or(owner),
                            )?)
                            .ok_or_else(|| {
                                Error::Other("vector authorized count overflow".into())
                            })?;
                    }
                }
                Ok(count)
            })
        })
    }

    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn search_with_mode(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: contextdb_core::VectorSearchMode,
        executing_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        let executing_owner: crate::memory_budget::MemoryBudgetOwner = executing_owner.into();

        self.search_with_mode_owned(index, query, k, candidates, snapshot, mode, executing_owner)
    }

    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn search_with_mode_owned(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: contextdb_core::VectorSearchMode,
        executing_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        let (scores, trace) = self.search_with_mode_owned_canonical(
            index,
            query,
            k,
            candidates,
            snapshot,
            mode,
            executing_owner,
        )?;
        Ok((scores.narrow()?, trace))
    }

    /// [`Self::search_with_mode_owned`] keeping the exact route's canonical
    /// keys, for callers that merge or limit the answer further.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)] // The same input list as search_with_mode_owned.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn search_with_mode_owned_canonical(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: contextdb_core::VectorSearchMode,
        executing_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<(CanonicalScores, OwnedVectorSearchTrace)> {
        let executing_owner: crate::memory_budget::MemoryBudgetOwner = executing_owner.into();

        let route_owner = crate::VectorCopyAdmission::new(executing_owner.clone());
        self.search_with_mode_route(
            index,
            query,
            k,
            candidates,
            (snapshot, mode),
            (executing_owner, route_owner),
        )
    }

    /// Preserve the search ladder while inheriting the executing route's
    /// durable-read preference and its actual temporary-capacity funder.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn search_with_mode_admission(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot_mode: (SnapshotId, contextdb_core::VectorSearchMode),
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        let (scores, trace) = self.search_with_mode_admission_canonical(
            index,
            query,
            k,
            candidates,
            snapshot_mode,
            owner,
        )?;
        Ok((scores.narrow()?, trace))
    }

    /// [`Self::search_with_mode_admission`] keeping the exact route's
    /// canonical keys, for callers that merge or limit the answer further.
    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn search_with_mode_admission_canonical(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot_mode: (SnapshotId, contextdb_core::VectorSearchMode),
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<(CanonicalScores, OwnedVectorSearchTrace)> {
        self.search_with_mode_route(
            index,
            query,
            k,
            candidates,
            snapshot_mode,
            (owner.funder(), owner.scratch()),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn search_with_mode_route(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot_mode: (SnapshotId, contextdb_core::VectorSearchMode),
        owners: (
            crate::memory_budget::MemoryBudgetOwner,
            crate::VectorCopyAdmission,
        ),
    ) -> Result<(CanonicalScores, OwnedVectorSearchTrace)> {
        let (snapshot, mode) = snapshot_mode;
        let (executing_owner, mut route_owner) = owners;
        self.store.with_bulk_read(|| {
            if k == 0 {
                return Ok((
                    CanonicalScores::new(&index, executing_owner.clone())?,
                    OwnedVectorSearchTrace::brute_force::<f64>(
                        &index,
                        &[],
                        "empty_limit",
                        Vec::new(),
                        executing_owner.clone(),
                    )?,
                ));
            }
            let layout = self.store.scoped_index_layout(&index, &mut route_owner)?;
            if query.len() != layout.dimension {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: index.clone(),
                    expected: layout.dimension,
                    actual: query.len(),
                });
            }
            if candidates.is_some_and(|candidates| candidates.is_empty()) {
                return Ok((
                    CanonicalScores::new(&index, executing_owner.clone())?,
                    OwnedVectorSearchTrace::brute_force::<f64>(
                        &index,
                        &[],
                        "empty_candidates",
                        Vec::new(),
                        executing_owner.clone(),
                    )?,
                ));
            }
            let source_candidates = if mode == contextdb_core::VectorSearchMode::Exact {
                // EXACT authenticates candidates during its one membership pass;
                // route narrowing must not add per-row membership seeks first.
                None
            } else {
                candidates
            };
            let sources = self.selected_partition_sources(
                &index,
                source_candidates,
                snapshot,
                &mut route_owner,
            )?;
            if sources.is_empty() {
                return Ok((
                    CanonicalScores::new(&index, executing_owner.clone())?,
                    OwnedVectorSearchTrace::brute_force::<f64>(
                        &index,
                        &[],
                        "empty_index",
                        Vec::new(),
                        executing_owner.clone(),
                    )?,
                ));
            }
            let exact = |reason: &'static str,
                         owner: &mut crate::VectorCopyAdmission|
             -> Result<(CanonicalScores, OwnedVectorSearchTrace)> {
                // Durable EXACT and exhaustive AUTO share the authenticated
                // membership/body stream; resident loader policy is preserved.
                let (rows, partition_count) = self.brute_force_search_sources(
                    &index,
                    &sources,
                    query,
                    k,
                    candidates,
                    snapshot,
                    mode == contextdb_core::VectorSearchMode::Auto,
                    owner,
                )?;
                let mut trace_owner = crate::VectorCopyAdmission::new(executing_owner.clone());
                trace_owner.reserve(
                    partition_count
                        .checked_mul(std::mem::size_of::<VectorPartitionSearchTrace>())
                        .ok_or_else(|| Error::Other("vector trace capacity overflow".into()))?,
                )?;
                let partition_routes = vec![
                    VectorPartitionSearchTrace {
                        route: VectorPartitionSearchRoute::Exact,
                        reason,
                        traversal_complete: true,
                    };
                    partition_count
                ];
                let trace = OwnedVectorSearchTrace::brute_force_with_owner(
                    &index,
                    rows.rows(),
                    reason,
                    partition_routes,
                    trace_owner,
                )?;
                #[cfg(feature = "test-seams")]
                observe_exact_scores_for_test(
                    ExactScorePhaseForTest::Scored,
                    rows.rows.capacity(),
                    rows.rows.len(),
                );
                Ok((rows, trace))
            };
            if mode == contextdb_core::VectorSearchMode::Exact {
                return exact("exact_requested", &mut route_owner);
            }
            if !query_has_positive_finite_norm(query) {
                if mode == contextdb_core::VectorSearchMode::Indexed {
                    return Err(Error::VectorIndexedRouteUnavailable {
                        index: index.clone(),
                    });
                }
                return exact("non_positive_query_norm", &mut route_owner);
            }

            if mode == contextdb_core::VectorSearchMode::Auto {
                let authorized_count = Self::aggregate_authorized_source_count(
                    &sources,
                    candidates,
                    snapshot,
                    &mut route_owner,
                )?;
                if authorized_count < layout.effective_auto_index_at() {
                    return exact(
                        if matches!(layout.quantization, VectorQuantization::F32) {
                            "aggregate_below_auto_index_at"
                        } else {
                            "quantized_exact_search"
                        },
                        &mut route_owner,
                    );
                }
            }

            if mode == contextdb_core::VectorSearchMode::Indexed {
                return Err(Error::VectorIndexedRouteUnavailable {
                    index: index.clone(),
                });
            }
            exact("maintained_graph_unavailable", &mut route_owner)
        })
    }

    /// Compatibility route for existing lower-level callers. Their published
    /// behavior is the column-default automatic ladder; engine query surfaces
    /// that resolve an explicit mode call [`Self::search_with_mode`] instead.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn search_with_strategy(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        executing_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        let executing_owner: crate::memory_budget::MemoryBudgetOwner = executing_owner.into();

        self.search_with_mode(
            index,
            query,
            k,
            candidates,
            snapshot,
            contextdb_core::VectorSearchMode::Auto,
            executing_owner,
        )
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn search_with_strategy_for_test(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        executing_owner: impl Into<crate::memory_budget::MemoryBudgetOwner>,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        let executing_owner: crate::memory_budget::MemoryBudgetOwner = executing_owner.into();

        self.search_with_strategy(index, query, k, candidates, snapshot, executing_owner)
    }
}

impl<S: WriteSetApplicator> VectorExecutor for MemVectorExecutor<S> {
    type SearchOwner = crate::memory_budget::MemoryBudgetOwner;
    type SearchOutput = SearchScores;
    fn search(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        executing_owner: Self::SearchOwner,
    ) -> Result<Self::SearchOutput> {
        self.search_with_strategy(index, query, k, candidates, snapshot, executing_owner)
            .map(|(rows, _)| rows)
    }

    fn insert_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
        executing_owner: Self::SearchOwner,
    ) -> Result<()> {
        let mut owner = crate::VectorCopyAdmission::new(executing_owner);
        self.store.validate_vector(&index, vector.len())?;
        let entry = VectorEntry {
            index: index.clone(),
            row_id,
            vector,
            created_tx: tx,
            deleted_tx: None,
            lsn: contextdb_core::Lsn(0),
        };
        let existing_live = self.store.has_live_entry_for_row(
            &index,
            row_id,
            self.tx_mgr.snapshot(),
            &mut owner,
        )?;

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

    fn delete_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        executing_owner: Self::SearchOwner,
    ) -> Result<()> {
        let mut owner = crate::VectorCopyAdmission::new(executing_owner);
        self.store.state(&index)?;
        let existing_live = self.store.has_live_entry_for_row(
            &index,
            row_id,
            self.tx_mgr.snapshot(),
            &mut owner,
        )?;
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
    fn after_native_vector_score(&self, _event: NativeVectorScoreEvent) {}
}
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct NativeVectorScoreEvent {
    row_id: RowId,
    created_tx: contextdb_core::TxId,
    lsn: contextdb_core::Lsn,
    snapshot: SnapshotId,
    score: f32,
    body_allocation: Option<(usize, usize, usize)>,
    _producer: (),
}
#[cfg(feature = "test-seams")]
impl NativeVectorScoreEvent {
    pub fn identity(&self) -> (RowId, contextdb_core::TxId, contextdb_core::Lsn) {
        (self.row_id, self.created_tx, self.lsn)
    }
    pub fn snapshot(&self) -> SnapshotId {
        self.snapshot
    }
    pub fn score(&self) -> f32 {
        self.score
    }
    pub fn body_allocation(&self) -> Option<(usize, usize, usize)> {
        self.body_allocation
    }
}
#[cfg(feature = "test-seams")]
pub(crate) fn observe_native_score_identity_for_test(
    row_id: RowId,
    created_tx: contextdb_core::TxId,
    lsn: contextdb_core::Lsn,
    snapshot: SnapshotId,
    score: f32,
    body_allocation: Option<(usize, usize, usize)>,
) {
    let observer = NATIVE_VECTOR_SCORE_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.after_native_vector_score(NativeVectorScoreEvent {
            row_id,
            created_tx,
            lsn,
            snapshot,
            score,
            body_allocation,
            _producer: (),
        });
    }
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
