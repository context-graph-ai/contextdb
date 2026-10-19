use crate::hnsw::{
    BoundedHnswQuerySelfScores, HnswAllowedSearchIncomplete, HnswAllowedSearchLimits,
    HnswAllowedSearchStatus,
};
use crate::memory_budget::MemoryBudget;
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
use std::sync::{Arc, OnceLock};

enum QuarantineAwarePreloadError<E> {
    Control(E),
    RetainControl(E),
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

/// The route reason of a partition an unfiltered walk found holding live rows
/// its maintained graph does not connect: it is compared row by row, under
/// INDEXED as under AUTO, because the graph cannot say which rows it left out.
pub const GRAPH_FRONTIER_DISCONNECTED_REASON: &str = "graph_frontier_disconnected";

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

#[cfg(feature = "test-seams")]
thread_local! {
    static AFTER_WALK_HOOK: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-seams")]
fn run_after_walk_hook_for_test() {
    let hook = AFTER_WALK_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(mut hook) = hook {
        hook();
    }
}

/// Run `hook` once on this thread, in the next union search, after its walk
/// has given its tail guards back and before its candidates are resolved and
/// any disconnection is reported: the window a publication can land in.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_after_walk_hook_for_test<R>(hook: Box<dyn FnMut()>, search: impl FnOnce() -> R) -> R {
    AFTER_WALK_HOOK.with(|slot| *slot.borrow_mut() = Some(hook));
    let result = search();
    AFTER_WALK_HOOK.with(|slot| slot.borrow_mut().take());
    result
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

/// Why a partition was compared row by row although its graph is maintained:
/// its captured live rows are below the column's declared `FLAT_BELOW`.
pub const PARTITION_BELOW_FLAT_THRESHOLD_REASON: &str = "partition_below_flat_threshold";

/// The walk budget a statement's one search is given, `cap(ef, k) x
/// M_max0`, for a caller that knows the widest admitted serving graph only by
/// its declared degree: the layer-0 maximum degree comes from the build's own
/// definition of that layer, the same one the loaded graphs report.
#[doc(hidden)]
pub fn auto_statement_walk_budget(ef_search: usize, k: usize, serving_hnsw_m: usize) -> usize {
    crate::hnsw::union_candidate_budget(
        ef_search,
        k,
        crate::hnsw::layer0_serving_degree(serving_hnsw_m),
    )
}

/// The file-route crossover for a statement whose admitted bodies must all
/// be read and authenticated. The live decision prices each source separately
/// because some statements mix resident and stored bodies.
///
/// The walk's whole budget is one search over every admitted Context. The
/// comparison side reads every admitted row once, at the file-route cost.
/// Only the unit tests of the crossover arithmetic call it now: execution and
/// EXPLAIN both route by [`auto_statement_cost`].
#[cfg(test)]
fn auto_statement_prefers_exhaustive(walk_budget: usize, aggregate_allowed_count: usize) -> bool {
    auto_statement_prefers_exhaustive_at_cost(
        walk_budget,
        (aggregate_allowed_count as u128)
            .saturating_mul(u128::from(AUTO_EXHAUSTIVE_COMPARISON_NANOS)),
    )
}

fn auto_statement_prefers_exhaustive_at_cost(walk_budget: usize, exhaustive_nanos: u128) -> bool {
    exhaustive_nanos <= (walk_budget as u128).saturating_mul(u128::from(AUTO_WALK_EVALUATION_NANOS))
}

/// What AUTO's statement cost reads from one selected partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoPartitionCost {
    /// Rows the statement may return from this partition.
    pub allowed: usize,
    /// Every live row of the partition, which its graph walk navigates.
    pub live: usize,
    /// The degree of the graph that serves the snapshot, or the resolved
    /// policy's degree when none is built yet.
    pub hnsw_m: usize,
    /// Saved layers the snapshot still has to load.
    pub dormant_layers: usize,
    /// Whether comparing a row reads and authenticates its stored body.
    pub streams_raw_bodies: bool,
    /// Whether the partition is below the column's `FLAT_BELOW`: the walk
    /// route then compares its rows exactly, so it is priced as comparison.
    pub flat: bool,
}

/// AUTO's one decision for a statement, priced the same way wherever it is
/// made: by the executor before it loads any graph, and by a passive EXPLAIN
/// that predicts the route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoStatementCost {
    /// The cap each selected partition's walk receives.
    pub walk_budget: usize,
    /// Evaluations the walks are expected to spend, opening included.
    pub expected_walk: usize,
    /// What comparing every allowed row costs, in nanoseconds.
    pub exhaustive_nanos: u128,
    /// Comparing every allowed row costs no more than the expected walk.
    pub prefers_exhaustive: bool,
}

/// Price one statement's AUTO choice over its selected nonempty partitions.
///
/// `expansion_ceiling_percent` is the column's effective
/// `AUTO_EXPANSION_CEILING`: the most of its walk budget one partition's
/// graph search is expected to spend.
pub fn auto_statement_cost<I>(
    ef_search: usize,
    k: usize,
    expansion_ceiling_percent: usize,
    partitions: I,
) -> AutoStatementCost
where
    I: IntoIterator<Item = AutoPartitionCost>,
    I::IntoIter: Clone,
{
    let partitions = partitions
        .into_iter()
        .filter(|partition| partition.allowed != 0);
    let serving_hnsw_m = partitions
        .clone()
        .map(|partition| partition.hnsw_m)
        .max()
        .unwrap_or(0);
    let walk_budget = auto_statement_walk_budget(ef_search, k, serving_hnsw_m);
    let mut expected_walk = 0usize;
    let mut exhaustive_nanos = 0u128;
    let mut flat_nanos = 0u128;
    for partition in partitions {
        let comparison_nanos = if partition.streams_raw_bodies {
            AUTO_EXHAUSTIVE_COMPARISON_NANOS
        } else {
            AUTO_IN_MEMORY_EXHAUSTIVE_COMPARISON_NANOS
        };
        let partition_comparison_nanos =
            (partition.allowed as u128).saturating_mul(u128::from(comparison_nanos));
        exhaustive_nanos = exhaustive_nanos.saturating_add(partition_comparison_nanos);
        // A flat partition costs the walk route exactly what it costs EXACT.
        if partition.flat {
            flat_nanos = flat_nanos.saturating_add(partition_comparison_nanos);
            continue;
        }
        // Each partition's walk receives the whole cap and can spend it only
        // on its own rows, so its expected share is taken against both.
        expected_walk = expected_walk
            .saturating_add(
                auto_layer_admission_estimate(
                    partition.live,
                    partition.hnsw_m,
                    crate::hnsw::layer0_serving_degree(partition.hnsw_m),
                )
                .saturating_mul(partition.dormant_layers.max(1)),
            )
            .saturating_add(auto_statement_expected_expansion(
                walk_budget,
                partition.allowed,
                partition.live,
                expansion_ceiling_percent,
            ));
    }
    AutoStatementCost {
        walk_budget,
        expected_walk,
        exhaustive_nanos,
        prefers_exhaustive: serving_hnsw_m != 0
            && auto_statement_prefers_exhaustive_at_cost(
                expected_walk,
                exhaustive_nanos.saturating_sub(flat_nanos),
            ),
    }
}

/// Estimate the work one partition's healthy graph search spends of its cap:
/// linear in the partition's admitted rows (on a 2,048-row corpus the measured expansion was
/// 315-1,364 evaluations against a 3,200 allowance), reaching its ceiling of
/// a quarter of the cap at 5,000 admitted rows. Layer opening is added
/// separately, so splitting rows can make exhaustive work cheaper.
///
/// The estimate was taken once for the whole statement against one cap; each
/// partition's walk receives the whole cap, and a sum of linear estimates is
/// the statement's linear estimate, so only where the ceiling applies does
/// the per-partition sum differ from it.
///
/// The ceiling was the whole budget, reached at 20,000 rows. That predicted
/// 9,600 evaluations for the 10,000-row single-Context corpus, whose walk
/// spends 1,287-1,428, so AUTO compared every row there although the graph
/// answered the same page in a third of the time (release, resident:
/// 3.4 ms against 11.4 ms per query). The file-route measurement at 12,500
/// rows spent half its budget; there the walk is chosen either way.
///
/// A residual filter is priced twice over. The walk navigates every live row
/// of the graph, not only the allowed ones, so the linear rule reads the live
/// count; and it keeps expanding until its beam holds enough allowed rows, so
/// that count is widened by `live / allowed`, the inverse of the filter's
/// selectivity. With no filter (`allowed == live`) this is the rule above.
/// Without it AUTO walked the frozen three-Context cell, 625 allowed of 1,250
/// live rows per partition: priced at 588 evaluations, the walk scored 1,866.9
/// of 1,875 allowed rows and answered at 44.7 ms p95 against EXACT's 22.8 ms.
fn auto_statement_expected_expansion(
    budget: usize,
    allowed: usize,
    live: usize,
    ceiling_percent: usize,
) -> usize {
    let navigated = live.max(allowed);
    let widened = (navigated as u128)
        .saturating_mul(navigated as u128)
        .div_ceil(allowed.max(1) as u128);
    let widened = usize::try_from(widened).unwrap_or(usize::MAX);
    budget
        .saturating_mul(widened)
        .div_ceil(20_000)
        .min(budget.saturating_mul(ceiling_percent).div_ceil(100))
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

/// A statement filter checked on reached rows. Each selected partition has
/// `allowance` uncached predicate reads, derived from the unchanged declared
/// breadth. The caller's bounded identity probe has its own separate allowance.
/// `check` reads one row under the statement's snapshot and authorization.
pub struct RowCheck<'a, E> {
    pub allowance: usize,
    pub check: &'a mut dyn FnMut(RowId) -> std::result::Result<bool, E>,
}

/// Physical storage and its credit travel together, including temporary
/// replacements. Drop the allocation before returning its admitted bytes.
struct RowCheckAllocation<'a, T: Default> {
    value: T,
    bytes: usize,
    release: &'a dyn Fn(usize),
}

impl<T: Default> Drop for RowCheckAllocation<'_, T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.value));
        if self.bytes != 0 {
            (self.release)(self.bytes);
        }
    }
}

impl<'a, T: Default> RowCheckAllocation<'a, T> {
    fn new(release: &'a dyn Fn(usize)) -> Self {
        Self {
            value: T::default(),
            bytes: 0,
            release,
        }
    }

    fn reconcile<E>(
        &mut self,
        actual: usize,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if actual > self.bytes {
            before_retain(actual - self.bytes)?;
        } else if actual < self.bytes {
            (self.release)(self.bytes - actual);
        }
        self.bytes = actual;
        Ok(())
    }
}

/// One snapshot-bound verdict per row, with an independent pre-read debit
/// for each stable selected source position. Shared rows and repeated layers
/// reuse the verdict without charging another partition or reading again.
struct RowCheckLedger<'check, 'storage, E> {
    allowance: usize,
    max_entries: usize,
    partition_checks: RowCheckAllocation<'storage, Vec<usize>>,
    exhausted: bool,
    error: Option<E>,
    verdicts: Option<RowCheckAllocation<'storage, std::collections::HashMap<RowId, bool>>>,
    check: &'check mut dyn FnMut(RowId) -> std::result::Result<bool, E>,
}

impl<'check, 'storage, E: From<Error>> RowCheckLedger<'check, 'storage, E> {
    fn new(
        row_check: RowCheck<'check, E>,
        partitions: usize,
        release: &'storage dyn Fn(usize),
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Self, E> {
        let max_entries =
            checked_vector_mul(partitions, row_check.allowance, "row-check allowance")
                .map_err(E::from)?;
        let mut partition_checks = RowCheckAllocation::<Vec<usize>>::new(release);
        if partitions != 0 {
            let bytes = checked_vector_mul(
                partitions,
                std::mem::size_of::<usize>(),
                "row-check counters",
            )
            .map_err(E::from)?;
            #[cfg(feature = "test-seams")]
            crate::test_seam::observe_row_check_cache_event(3, bytes);
            before_retain(bytes)?;
            partition_checks.bytes = bytes;
            #[cfg(feature = "test-seams")]
            let counter_observation = crate::test_seam::observe_row_check_cache_allocation(bytes);
            if partition_checks
                .value
                .try_reserve_exact(partitions)
                .is_err()
            {
                drop(partition_checks);
                #[cfg(feature = "test-seams")]
                drop(counter_observation);
                return Err(E::from(Error::Other(
                    ("row-check counters allocation failed".to_string()).into(),
                )));
            }
            #[cfg(feature = "test-seams")]
            drop(counter_observation);
            let actual = checked_vector_mul(
                partition_checks.value.capacity(),
                std::mem::size_of::<usize>(),
                "row-check counters",
            )
            .map_err(E::from)?;
            partition_checks.reconcile(actual, before_retain)?;
            partition_checks.value.resize(partitions, 0);
        }
        Ok(Self {
            allowance: row_check.allowance,
            max_entries,
            partition_checks,
            exhausted: false,
            error: None,
            verdicts: Some(RowCheckAllocation::new(release)),
            check: row_check.check,
        })
    }

    fn ensure_cache_slot(
        &mut self,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        let Some(cache) = self.verdicts.as_mut() else {
            return Ok(());
        };
        let required =
            checked_vector_add(cache.value.len(), 1, "row-check cache length").map_err(E::from)?;
        if required <= cache.value.capacity() {
            return Ok(());
        }
        let next = if cache.value.capacity() == 0 {
            required
        } else {
            checked_vector_mul(cache.value.capacity(), 2, "row-check cache capacity")
                .map_err(E::from)?
        }
        .max(required)
        .min(self.max_entries);
        if next < required {
            return Err(E::from(Error::Other(
                ("row-check cache exceeded its partition allowances".to_string()).into(),
            )));
        }
        let bytes =
            crate::hnsw::checked_hash_allocation_bytes::<(RowId, bool)>(next).map_err(E::from)?;
        // The entire replacement coexists with, and is funded beside, the old
        // map. The scoped replacement refunds on every error or unwind.
        let mut replacement =
            RowCheckAllocation::<std::collections::HashMap<RowId, bool>>::new(cache.release);
        #[cfg(feature = "test-seams")]
        crate::test_seam::observe_row_check_cache_event(0, bytes);
        before_retain(bytes)?;
        replacement.bytes = bytes;
        #[cfg(feature = "test-seams")]
        let cache_observation = crate::test_seam::observe_row_check_cache_allocation(bytes);
        if replacement.value.try_reserve(next).is_err() {
            drop(replacement);
            #[cfg(feature = "test-seams")]
            drop(cache_observation);
            return Err(E::from(Error::Other(
                ("row-check verdict cache allocation failed".to_string()).into(),
            )));
        }
        #[cfg(feature = "test-seams")]
        drop(cache_observation);
        let actual = crate::hnsw::checked_hash_allocation_bytes::<(RowId, bool)>(
            replacement.value.capacity(),
        )
        .map_err(E::from)?;
        replacement.reconcile(actual, before_retain)?;
        for (&row, &verdict) in &cache.value {
            before_work()?;
            #[cfg(feature = "test-seams")]
            crate::test_seam::observe_row_check_cache_event(4, 0);
            replacement.value.insert(row, verdict);
        }
        let old = std::mem::replace(cache, replacement);
        drop(old);
        Ok(())
    }

    /// Ensure an admitted slot, then debit this partition before its uncached
    /// predicate read. Failure is terminal and never performs an unfunded read.
    fn admit(
        &mut self,
        source_position: usize,
        row: RowId,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> bool {
        if self.error.is_some() {
            return false;
        }
        if let Some(verdict) = self
            .verdicts
            .as_ref()
            .and_then(|cache| cache.value.get(&row))
        {
            return *verdict;
        }
        let Some(&checks) = self.partition_checks.value.get(source_position) else {
            self.error = Some(E::from(Error::Other(
                ("row-check source position changed".to_string()).into(),
            )));
            return false;
        };
        if self.verdicts.is_some() {
            if checks >= self.allowance {
                self.exhausted = true;
                return false;
            }
            if let Err(error) = self.ensure_cache_slot(before_retain, before_work) {
                self.error = Some(error);
                return false;
            }
            self.partition_checks.value[source_position] += 1;
        }
        #[cfg(feature = "test-seams")]
        crate::test_seam::observe_row_check_cache_event(6, 0);
        match (self.check)(row) {
            Ok(verdict) => {
                if let Some(cache) = self.verdicts.as_mut() {
                    cache.value.insert(row, verdict);
                }
                verdict
            }
            Err(error) => {
                self.error = Some(error);
                false
            }
        }
    }

    /// AUTO streams exact work under request limits. Cache storage and its
    /// credit go back immediately, before that comparison pass needs memory.
    fn lift(&mut self) {
        self.exhausted = false;
        #[cfg(feature = "test-seams")]
        crate::test_seam::observe_row_check_cache_event(7, 0);
        drop(self.verdicts.take());
        #[cfg(feature = "test-seams")]
        crate::test_seam::observe_row_check_cache_event(8, 0);
    }
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
    budget: Arc<dyn MemoryBudget>,
    bytes: usize,
    _control_invoice: Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
}
impl ScoreCredit {
    fn holding(budget: Arc<dyn MemoryBudget>, bytes: usize) -> Self {
        let control_invoice = budget.control_invoice();
        Self { budget, bytes, _control_invoice: control_invoice }
    }
}

impl crate::PrepaidVectorCopyCharge for ScoreCredit {
    fn accountant(&self) -> Arc<dyn MemoryBudget> {
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
        self.budget.release(self.bytes);
    }
}

impl SearchScores {
    pub fn new(index: &VectorIndexRef, budget: Arc<dyn MemoryBudget>) -> Result<Self> {
        let (index, bytes) = allocate_vector_index(
            index,
            &mut |bytes| {
                budget.try_allocate_for(
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
    pub fn into_prepaid(
        self,
        destination: &Arc<dyn MemoryBudget>,
    ) -> Result<(Vec<(RowId, f32)>, ScoreCredit)> {
        if !crate::memory_budget::same_funder(destination.as_ref(), self.credit.budget.as_ref()) {
            return Err(Error::Other(
                "vector score destination has a different accountant".into()));
        }
        Ok((self.rows, self.credit))
    }

    fn from_charged(
        rows: Vec<(RowId, f32)>,
        index: &VectorIndexRef,
        budget: Arc<dyn MemoryBudget>,
        bytes: usize,
    ) -> Result<Self> {
        // Attach ownership before a fallible additional identity allocation.
        let mut scores = Self {
            rows,
            index: VectorIndexRef::new("", ""),
            credit: ScoreCredit::holding(budget, bytes),
            exact_budget: false,
        };
        let (index, index_bytes) = allocate_vector_index(
            index,
            &mut |bytes| {
                scores.credit.budget.try_allocate_for(
                    bytes,
                    "vector_search",
                    "score_identity",
                    "Reduce query scope or raise MEMORY_LIMIT.",
                )
            },
            &mut |bytes| scores.credit.budget.release(bytes),
        )?;
        scores.index = index;
        scores.credit.bytes += index_bytes;
        Ok(scores)
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
        Error::Other(("bounded brute-force heap lost its inserted score".to_string()).into())
    })?;
    while child != 0 {
        let parent = child
            .checked_sub(1)
            .ok_or_else(|| Error::Other(("bounded brute-force heap parent underflow".to_string()).into()))?
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
        return Err(Error::Other((
            "bounded brute-force heap has no replacement root".to_string()).into()));
    };
    *root = score;
    let mut parent = 0usize;
    loop {
        let Some(left) = parent.checked_mul(2).and_then(|value| value.checked_add(1)) else {
            return Err(Error::Other((
                "bounded brute-force heap child overflow".to_string()).into()));
        };
        if left >= scores.len() {
            return Ok(());
        }
        let right = left
            .checked_add(1)
            .ok_or_else(|| Error::Other(("bounded brute-force heap child overflow".to_string()).into()))?;
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
            E::from(Error::Other((
                "bounded vector merge length overflow".to_string()).into()))
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

#[allow(clippy::too_many_arguments)]
fn bounded_exact_merge_durable<E>(
    index: &VectorIndexRef,
    state: &crate::store::IndexState,
    memberships: &dyn crate::store::RawVectorMembershipSource,
    streamed: Option<(&dyn crate::store::CapturedVectorStreamedTail, Option<&[u64]>)>,
    score_bodies: bool,
    query: &[f32],
    k: usize,
    snapshot: SnapshotId,
    scores: &mut Vec<(RowId, f32)>,
    retained_bytes: &mut usize,
    bodies: &mut crate::store::RawVectorBodyReads,
    before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
    before_candidate: &mut impl FnMut() -> std::result::Result<(), E>,
    before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
    owner: &mut crate::VectorCopyAdmission,
    source_position: usize,
    row_check: Option<&RefCell<RowCheckLedger<'_, '_, E>>>,
    evaluation_limit: Option<(usize, &Cell<bool>)>,
) -> std::result::Result<usize, E>
where
    E: From<Error>,
{
    before_source_entry()?;
    let mut reserved = 0usize;
    let mut interrupted = None;
    let opened = memberships.open(&mut |request| {
        if interrupted.is_some() {
            return false;
        }
        let result = match request {
            crate::store::DormantVectorLoadRequest::Reserve(bytes) => {
                admitted_at("cursor_window", || {
                    before_retain(admission_bytes("cursor_window", bytes))
                })
                .map(|()| reserved += bytes)
            }
            _ => before_source_entry(),
        };
        match result {
            Ok(()) => true,
            Err(error) => {
                interrupted = Some(error);
                false
            }
        }
    });
    let result = (|| {
        if let Some(error) = interrupted.take() {
            return Err(error);
        }
        let mut cursor = opened.map_err(E::from)?;
        let mut compared = 0usize;
        let mut evaluated = 0usize;
        loop {
            before_source_entry()?;
            let mut interrupted = None;
            let next = cursor.next_with_body_identity(&mut |_| {
                if interrupted.is_some() {
                    return false;
                }
                match before_source_entry() {
                    Ok(()) => true,
                    Err(error) => {
                        interrupted = Some(error);
                        false
                    }
                }
            });
            if let Some(error) = interrupted {
                return Err(error);
            }
            let Some((entry, body_identity)) = next.map_err(E::from)? else {
                break;
            };
            if entry.created_tx.0 > snapshot.0
                || entry.deleted_tx.is_some_and(|tx| tx.0 <= snapshot.0)
            {
                continue;
            }
            if let Some((tail, candidates)) = streamed {
                if candidates.is_some_and(|ids| ids.binary_search(&entry.row_id.0).is_err()) { continue; }
                let mut interrupted = None;
                let included = tail.includes(&entry, &body_identity, snapshot, owner, &mut || {
                    match before_source_entry() {
                        Ok(()) => true, Err(error) => { interrupted = Some(error); false },
                    }
                });
                if let Some(error) = interrupted { return Err(error); }
                if !included.map_err(E::from)? { continue; }
            }
            if !score_bodies {
                compared = compared.checked_add(1).ok_or_else(|| E::from(
                    Error::Other("streamed tail count overflow".into())))?;
                continue;
            }
            // The statement's filter, checked on this row before it competes.
            if let Some(ledger) = row_check
                && !ledger.borrow_mut().admit(
                    source_position,
                    entry.row_id,
                    before_retain,
                    before_source_entry,
                )
            {
                continue;
            }
            if let Some((limit, exhausted)) = evaluation_limit
                && evaluated >= limit
            {
                exhausted.set(true);
                break;
            }
            evaluated += 1;
            state
                .bind_streamed_membership(&*cursor, bodies, owner)
                .map_err(E::from)?;
            before_candidate()?;
            let Some(score) = admitted_at("body_read", || {
                state.bounded_score_streamed_body(
                    index,
                    body_identity,
                    snapshot,
                    query,
                    bodies,
                    before_source_entry,
                    before_distance,
                    &mut |bytes| before_retain(admission_bytes("body_read", bytes)),
                    release_retained,
                    owner,
                )
            })?
            else {
                continue;
            };
            compared += 1;
            merge_bounded_vector_score(
                scores,
                retained_bytes,
                (entry.row_id, score),
                k,
                before_source_entry,
                before_retain,
                release_retained,
            )?;
        }
        Ok(compared)
    })();
    release_retained(reserved);
    result
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
    owner: &mut crate::VectorCopyAdmission,
    source_position: usize,
    row_check: Option<&RefCell<RowCheckLedger<'_, '_, E>>>,
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
            let Some(score) = admitted_at("body_read", || {
                state.bounded_score_stored_body(
                    index,
                    row_id,
                    snapshot,
                    query,
                    bodies,
                    before_source_entry,
                    before_distance,
                    &mut |bytes| before_retain(admission_bytes("body_read", bytes)),
                    release_retained,
                    owner,
                )
            })?
            else {
                continue;
            };
            compared = compared.checked_add(1).ok_or_else(|| {
                E::from(Error::Other((
                    "bounded exact vector count overflow".to_string()).into()))
            })?;
            // The statement's filter, checked on this row before it competes.
            if let Some(ledger) = row_check
                && !ledger.borrow_mut().admit(
                    source_position,
                    row_id,
                    before_retain,
                    before_source_entry,
                )
            {
                continue;
            }
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
    if let Some(memberships) = state.durable_membership_source()
        && !state.try_cache_durable_bodies(owner).map_err(E::from)?
    {
        // Bodies that do not fit stream: the durable directory through one
        // admitted window, then each visible row's body point-read once.
        let previous = bodies.set_one_pass(false);
        let compared = bounded_exact_merge_durable(
            index,
            state,
            memberships.as_ref(),
            None,
            true,
            query,
            k,
            snapshot,
            scores,
            retained_bytes,
            bodies,
            before_source_entry,
            before_candidate,
            before_distance,
            before_retain,
            release_retained,
            owner,
            source_position,
            row_check,
            None,
        );
        bodies.set_one_pass(previous);
        return compared;
    }
    before_source_entry()?;
    let historical_stream = match state.historical_partition(owner) {
        Ok(Some(partition)) => {
            let mut compared = 0usize;
            for position in 0..partition.len() {
                before_source_entry()?;
                before_candidate()?;
                if partition.entry(position).is_some_and(|entry| {
                    entry.membership.visible_from.0 <= snapshot.0
                        && entry
                            .membership
                            .deleted_tx
                            .is_none_or(|tx| tx.0 > snapshot.0)
                }) {
                    before_distance()?;
                }
                if let Some(score) = state
                    .historical_partition_score(&partition, position, snapshot, query)
                    .map_err(E::from)?
                {
                    compared = compared.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other("bounded exact vector count overflow".into()))
                    })?;
                    // The statement's filter, checked on this row before it competes.
                    if let Some(ledger) = row_check
                        && !ledger.borrow_mut().admit(
                            source_position,
                            score.0,
                            before_retain,
                            before_source_entry,
                        )
                    {
                        continue;
                    }
                    merge_bounded_vector_score(
                        scores,
                        retained_bytes,
                        score,
                        k,
                        before_source_entry,
                        before_retain,
                        release_retained,
                    )?;
                }
            }
            return Ok(compared);
        }
        Ok(None) => false,
        // A copy refused for memory is not a refusal when the bodies can
        // stream instead. The copy's own reservations are admitted, and their
        // refusal proved, by the raw loader that makes them; this route takes
        // it only for an owner reading history over a non-durable partition.
        Err(Error::MemoryBudgetExceeded { .. }) if state.streams_raw_bodies() => true,
        Err(error) => return Err(E::from(error)),
    };
    // A partition whose vectors fit is loaded once and compared from memory,
    // which every later find reuses. Only when the memory to hold it is
    // refused does the comparison read the bodies one at a time instead, so a
    // find the reader could not hold is answered rather than refused, and a
    // find it could hold does not re-read every body on every call.
    let stream = historical_stream
        || state.streams_raw_bodies()
            && match state.ensure_raw_vectors_loaded(owner) {
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
                E::from(Error::Other((
                    "bounded exact vector position overflow".to_string()).into()))
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
                owner,
            )?
            else {
                continue;
            };
            compared = compared.checked_add(1).ok_or_else(|| {
                E::from(Error::Other((
                    "bounded exact vector count overflow".to_string()).into()))
            })?;
            // The statement's filter, checked on this row before it competes.
            if let Some(ledger) = row_check
                && !ledger.borrow_mut().admit(
                    source_position,
                    row_id,
                    before_retain,
                    before_source_entry,
                )
            {
                continue;
            }
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
    state.ensure_raw_vectors_loaded(owner).map_err(E::from)?;
    let entry_count = state.entry_count();
    let mut compared = 0usize;
    for position in 0..entry_count {
        before_source_entry()?;
        before_candidate()?;
        let candidate = state.with_entries(|entries| -> std::result::Result<Option<_>, E> {
            let entry = entries.get(position).ok_or_else(|| {
                E::from(Error::Other((
                    "bounded exact vector position changed".to_string()).into()))
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
            E::from(Error::Other((
                "bounded exact vector count overflow".to_string()).into()))
        })?;
        // The statement's filter, checked on this row before it competes.
        if let Some(ledger) = row_check
            && !ledger.borrow_mut().admit(
                source_position,
                candidate.0,
                before_retain,
                before_source_entry,
            )
        {
            continue;
        }
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
    /// Stored vectors read to score rows -- the walk's for rows updated
    /// since their graph was built, and final candidate scoring's -- from the
    /// rescoring allowance the declared breadth sizes.
    pub rescore_evaluations: usize,
    pub layers_admitted: usize,
    pub budget_terminal: &'static str,
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
        executing_owner: Arc<dyn MemoryBudget>,
        admission: crate::VectorCopyAdmission,
    ) -> OwnedVectorSearchTrace {
        OwnedVectorSearchTrace {
            trace,
            credit: ScoreCredit::holding(executing_owner, 0),
            admission: Some(admission),
        }
    }

    pub fn hnsw_len(&self) -> Option<usize> {
        self.trace.hnsw_len
    }
    pub fn hnsw_candidate_count(&self) -> usize {
        self.trace.hnsw_candidate_count
    }
    pub fn hnsw_candidate_row_ids(&self) -> &[RowId] {
        &self.trace.hnsw_candidate_row_ids
    }
    pub fn supplemented_row_count(&self) -> usize {
        self.trace.supplemented_row_count
    }
    pub fn hnsw_ef_search(&self) -> Option<usize> {
        self.trace.hnsw_ef_search
    }
    pub fn selected_generations(&self) -> &[crate::store::VectorGraphGeneration] {
        &self.trace.selected_generations
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
    fn brute_force(
        index: &VectorIndexRef,
        rows: &[(RowId, f32)],
        reason: &'static str,
        hnsw_len: Option<usize>,
        routes: Vec<VectorPartitionSearchTrace>,
        budget: Arc<dyn MemoryBudget>,
    ) -> Result<Self> {
        Self::brute_force_with_owner(
            index,
            rows,
            reason,
            hnsw_len,
            routes,
            crate::VectorCopyAdmission::new(budget),
        )
    }

    fn brute_force_with_owner(
        index: &VectorIndexRef,
        rows: &[(RowId, f32)],
        reason: &'static str,
        hnsw_len: Option<usize>,
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
            used_hnsw: false,
            hnsw_len,
            hnsw_candidate_count: 0,
            hnsw_candidate_row_ids: Vec::new(),
            final_row_ids,
            supplemented_row_count: 0,
            fallback_reason: Some(reason),
            partition_routes: routes,
            hnsw_ef_search: None,
            selected_generations: Vec::new(),
            budget_receipt: None,
        };
        Ok(Self {
            trace,
            credit: ScoreCredit::holding(budget, 0),
            admission: Some(admission),
        })
    }

    fn from_charged(trace: BoundedVectorSearchTrace, budget: Arc<dyn MemoryBudget>) -> Self {
        let bytes = trace.retained_bytes();
        Self {
            trace: trace.into_trace(),
            credit: ScoreCredit::holding(budget, bytes),
            admission: None,
        }
    }

    fn replace_final_rows(&mut self, rows: &[(RowId, f32)]) -> Result<()> {
        let (ids, bytes) = allocate_vector_row_ids(
            rows,
            "exact trace final rows",
            &mut || Ok::<(), Error>(()),
            &mut |bytes| {
                self.credit.budget.try_allocate_for(
                    bytes,
                    "vector_search",
                    "trace",
                    "Reduce query scope or raise MEMORY_LIMIT.",
                )
            },
            &mut |bytes| self.credit.budget.release(bytes),
        )?;
        let old_bytes = self.trace.final_row_ids.capacity() * std::mem::size_of::<RowId>();
        let old = std::mem::replace(&mut self.trace.final_row_ids, ids);
        drop(old);
        self.credit.budget.release(old_bytes);
        self.credit.bytes = self.credit.bytes - old_bytes + bytes;
        Ok(())
    }

    pub fn used_hnsw(&self) -> bool {
        self.trace.used_hnsw
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
    pub fn into_prepaid(
        mut self,
        destination: &Arc<dyn MemoryBudget>,
    ) -> Result<(VectorSearchDebugTrace, ScoreCredit)> {
        if !crate::memory_budget::same_funder(destination.as_ref(), self.credit.budget.as_ref()) {
            return Err(Error::Other(
                "vector trace destination has a different accountant".into()));
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
    historical_partition: Option<crate::store::HistoricalRawVectorPartition>,
    membership_cursor: Option<Box<dyn crate::store::RawVectorMembershipCursor>>,
    membership_reserved_bytes: usize,
    // Keep the exact body plus the visibility scalars actually used on retry.
    // A second full body/provenance record was a redundant boxed payload.
    pending_membership: Option<(
        crate::store::RawVectorDirectoryEntry,
        RowId, TxId, Option<TxId>,
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
    serving_selection: Option<Arc<crate::store::CapturedVectorServingSelection>>,
    candidate_ids: Option<Vec<u64>>,
    point_load: bool,
    candidate_ids_cover_all_live: bool,
    allowed_count: usize,
    live_count: usize,
}

impl BoundedSnapshotVectorSource {
    fn serving_policy(&self, snapshot: SnapshotId) -> Option<crate::store::VectorServingPolicy> {
        match &self.serving_selection {
            Some(selection) => selection.serving_policy(),
            None => self.state.snapshot_serving_policy(snapshot),
        }
    }
    fn dormant_layer_count(&self, snapshot: SnapshotId) -> usize {
        self.serving_selection.as_ref().map_or_else(
            || self.state.dormant_layer_count_for_snapshot(snapshot),
            |selection| selection.dormant_layer_count(),
        )
    }
    fn dormant_load_bytes(&self, snapshot: SnapshotId) -> usize {
        self.serving_selection.as_ref().map_or_else(
            || self.state.dormant_load_bytes_for_snapshot(snapshot),
            |selection| selection.dormant_load_bytes(),
        )
    }
    fn streamed_suffix(&self) -> Option<&dyn crate::store::StreamedVectorSuffixHandle> {
        self.serving_selection
            .as_ref()
            .and_then(|selection| selection.streamed_suffix())
    }
    fn journal_suffix_frontier(&self) -> Option<TxId> {
        let selection = self.serving_selection.as_ref()?;
        if selection.streamed_suffix()?.saved_inserts() == 0 {
            return None;
        }
        Some(selection.inspection.change.or(selection.inspection.base)
            .map_or(TxId(0), |generation| generation.covered_tx))
    }
    fn dormant_page_bytes(&self, snapshot: SnapshotId) -> usize {
        self.serving_selection.as_ref().map_or_else(
            || self.state.dormant_page_bytes_for_snapshot(snapshot),
            |selection| selection.dormant_page_bytes(),
        )
    }
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
    auto_index_at: usize,
    every_partition_fits_base_ef: bool,
    retained_bytes: usize,
}

impl BoundedVectorCandidateGroups {
    /// Scalar metadata only; the caller moves the one source array into its
    /// cursor and restores it here after scan-only ownership has ended.
    #[doc(hidden)]
    pub fn disclosure_metadata(&self) -> Self {
        Self { snapshot: self.snapshot, filtered: self.filtered, row_checked: self.row_checked,
            sources: Vec::new(), aggregate_allowed_count: self.aggregate_allowed_count,
            auto_index_at: self.auto_index_at, every_partition_fits_base_ef: self.every_partition_fits_base_ef,
            retained_bytes: self.retained_bytes }
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
                source.serving_policy(self.snapshot),
            )?;
        }
        Ok(())
    }

    /// AUTO's statement cost over the exact snapshot sources this grouped
    /// search owns: the one pricing the executor routes by, so a passive
    /// EXPLAIN holding the same groups predicts the route it takes.
    pub fn auto_statement_cost(
        &self,
        layout: &crate::store::VectorIndexLayout,
        k: usize,
    ) -> AutoStatementCost {
        let statement_live_count = self
            .sources
            .iter()
            .map(|source| source.live_count)
            .fold(0usize, usize::saturating_add);
        let statement_policy = layout.resolve_policy(statement_live_count, search_policy_limit(k));
        auto_statement_cost(
            statement_policy.hnsw_ef_search,
            k,
            layout.effective_auto_expansion_ceiling(),
            self.sources
                .iter()
                .filter(|source| source.allowed_count != 0)
                .map(|source| AutoPartitionCost {
                    allowed: source.allowed_count,
                    live: source.live_count,
                    hnsw_m: source.serving_policy(self.snapshot).map_or_else(
                        || {
                            layout
                                .resolve_policy(source.live_count, search_policy_limit(k))
                                .hnsw_m
                        },
                        |serving| serving.hnsw_m,
                    ),
                    dormant_layers: source.dormant_layer_count(self.snapshot),
                    streams_raw_bodies: source.state.streams_raw_bodies(),
                    flat: layout.serves_flat(source.live_count),
                }),
        )
    }

    /// The row checks one statement may spend on a filter it checks row by
    /// row: its walk budget for `k` at the declared breadth,
    /// `max(EF_SEARCH, k) x (2M + 4)` for the statement's resolved policy and
    /// the widest serving graph's degree -- the same function that sizes the
    /// walk, for the whole statement across its partitions and layers.
    pub fn row_check_allowance(&self, layout: &crate::store::VectorIndexLayout, k: usize) -> usize {
        let statement_live_count = self
            .sources
            .iter()
            .map(|source| source.live_count)
            .fold(0usize, usize::saturating_add);
        let statement_policy = layout.resolve_policy(statement_live_count, search_policy_limit(k));
        let widest_m = self
            .sources
            .iter()
            .map(|source| {
                source.serving_policy(self.snapshot).map_or_else(
                    || {
                        layout
                            .resolve_policy(source.live_count, search_policy_limit(k))
                            .hnsw_m
                    },
                    |serving| serving.hnsw_m,
                )
            })
            .max()
            .unwrap_or(statement_policy.hnsw_m);
        auto_statement_walk_budget(statement_policy.hnsw_ef_search, k, widest_m)
    }

    /// Inspect only the already captured nonempty sources, one fixed
    /// metadata window at a time. This never builds a partition-info array.
    pub fn has_pending_tail_without_load(&self, owner: &mut crate::VectorCopyAdmission) -> Result<bool> {
        for source in self.sources.iter().filter(|source| source.allowed_count != 0) {
            let pending = if let Some(selection) = &source.serving_selection {
                selection.inspection.pending_inserts != 0 || selection.inspection.tombstones != 0
            } else {
                source.state.has_pending_work_for_read(owner)?
            };
            if pending { return Ok(true); }
        }
        Ok(false)
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
            if let Some(selection) = &source.serving_selection {
                let (has_base, has_change, has_suffix) = selection.layer_presence();
                base |= has_base;
                change |= has_change;
                tail |= has_suffix;
                continue;
            }
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

    fn into_cursor_parts(mut self) -> (SnapshotId, bool, Vec<BoundedSnapshotVectorSource>, usize) {
        // Exhaustive stepping uses the captured raw source and snapshot only.
        // Release graph-only loaders/controls before retaining body and score
        // workspaces, while their own admissions still cover physical free.
        for source in &mut self.sources {
            drop(source.serving_selection.take());
        }
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

    /// Move the trace together with the actual caller's already held charge.
    /// The prepaid bridge rejects another accountant and retains the complete
    /// token until output drop or a consuming matched destination transfer.
    pub fn into_owned<C: crate::PrepaidVectorCopyCharge + 'static>(
        self,
        executing_owner: Arc<dyn MemoryBudget>,
        charge: C,
    ) -> Result<OwnedVectorSearchTrace> {
        if charge.retained_bytes() < self.retained_bytes {
            drop(self);
            drop(charge);
            return Err(Error::Other(
                "vector trace prepaid charge is smaller than its retained allocation".into()));
        }
        // Validate while both payload and prepaid owner are still here, so
        // a rejected transfer drops the payload before returning its credit.
        if !crate::memory_budget::same_funder(
            executing_owner.as_ref(), charge.accountant().as_ref(),
        ) {
            drop(self);
            drop(charge);
            return Err(Error::Other(
                "vector copy prepaid charge has a different accountant".into()));
        }
        let admission = crate::VectorCopyAdmission::from_prepaid(executing_owner.clone(), charge)?;
        Ok(OwnedVectorSearchTrace {
            trace: self.trace,
            credit: ScoreCredit::holding(executing_owner, 0),
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

    fn into_trace(self) -> VectorSearchDebugTrace {
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
    /// INDEXED found a selected partition holding live rows its maintained
    /// graph does not connect; `partition` is its position in scope order.
    Disconnected {
        partition: usize,
    },
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
    pub fn into_disclosure_groups(self, mut groups: BoundedVectorCandidateGroups) -> (BoundedVectorCandidateGroups, usize) {
        let Self { snapshot_sources, retained_bytes, .. } = self;
        let scan_bytes = retained_bytes - groups.retained_bytes;
        groups.sources = snapshot_sources;
        (groups, scan_bytes)
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
        Error::Other((format!(
            "bounded vector {operation} memory size exceeds the native address space"
        )).into())
    })
}

fn checked_vector_mul(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_mul(right).ok_or_else(|| {
        Error::Other((format!(
            "bounded vector {operation} memory size exceeds the native address space"
        )).into())
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
                    E::from(Error::Other((
                        "bounded HNSW capacity moved backwards before allocation".to_string()).into()))
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
                    E::from(Error::Other((
                        "bounded HNSW capacity reconciliation underflow".to_string()).into()))
                })?,
            element_bytes,
            "HNSW capacity reconciliation",
        )?),
        HnswSearchScratchEvent::Reconcile { .. } => Ok(()),
        HnswSearchScratchEvent::Release { .. } => Err(E::from(Error::Other((
            "bounded HNSW sent a release through its acquire callback".to_string()).into()))),
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
    let requested = checked_vector_mul(capacity, std::mem::size_of::<T>(), operation)
        .map_err(E::from)?;
    if requested != 0 {
        before_retain(requested)?;
    }
    let mut values = Vec::new();
    if values.try_reserve_exact(capacity).is_err() {
        drop(values);
        release_retained(requested);
        return Err(E::from(Error::Other((format!(
            "bounded vector {operation} allocation failed"
        )).into())));
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
        return Err(E::from(Error::Other((format!(
            "bounded vector {operation} capacity moved backwards"
        )).into())));
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
                return Err(E::from(Error::Other((format!(
                    "bounded vector {operation} source position changed"
                )).into())));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                drop(values);
                release_retained(actual);
                return Err(E::from(Error::Other((format!(
                    "bounded vector {operation} source position overflow"
                )).into())));
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
        return Err(E::from(Error::Other((format!(
            "bounded vector {operation} allocation failed"
        )).into())));
    }
    let actual = owned.capacity();
    if actual < requested {
        drop(owned);
        release_retained(requested);
        return Err(E::from(Error::Other((format!(
            "bounded vector {operation} capacity moved backwards"
        )).into())));
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
            drop(table);
            release_retained(table_bytes);
            return Err(error);
        }
    };
    let retained_bytes = match checked_vector_add(table_bytes, column_bytes, "HNSW trace index") {
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
        return Err(E::from(Error::Other((format!(
            "bounded vector {operation} allocation failed"
        )).into())));
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
        return Err(E::from(Error::Other((format!(
            "bounded vector {operation} capacity moved backwards"
        )).into())));
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
                    E::from(Error::Other((format!(
                        "bounded vector {operation} retained-memory overflow"
                    )).into()))
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
                return Err(E::from(Error::Other((format!(
                    "bounded vector {operation} changed during migration"
                )).into())));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                drop(replacement);
                release_retained(actual);
                return Err(E::from(Error::Other((format!(
                    "bounded vector {operation} migration position overflow"
                )).into())));
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
                return Err(E::from(Error::Other((format!(
                    "bounded vector {operation} source position changed"
                )).into())));
            }
        };
        position = match position.checked_add(1) {
            Some(position) => position,
            None => {
                drop(row_ids);
                release_retained(actual);
                return Err(E::from(Error::Other((format!(
                    "bounded vector {operation} source position overflow"
                )).into())));
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
                drop(selected_generations);
                release_retained(selected_generation_bytes);
                drop(partition_routes);
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
            drop(selected_generations);
            release_retained(selected_generation_bytes);
            drop(partition_routes);
            release_retained(partition_route_bytes);
            drop(owned_index);
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
            drop(selected_generations);
            release_retained(selected_generation_bytes);
            drop(partition_routes);
            release_retained(partition_route_bytes);
            drop(candidate_row_ids);
            release_retained(candidate_bytes);
            drop(owned_index);
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
            drop(selected_generations);
            release_retained(selected_generation_bytes);
            drop(partition_routes);
            release_retained(partition_route_bytes);
            drop(final_row_ids);
            release_retained(final_bytes);
            drop(candidate_row_ids);
            release_retained(candidate_bytes);
            drop(owned_index);
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
    allocate_vector_slots(
        capacity,
        "partition route receipt",
        &mut |bytes| admitted_at("route_slots", || {
            before_retain(admission_bytes("route_slots", bytes))
        }),
        release_retained,
    )
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
                drop(partition_routes);
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
            drop(owned_index);
            release_retained(index_bytes);
            drop(partition_routes);
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
        return Err(E::from(Error::Other((
            "bounded vector partition scope exceeds its declared maximum".to_string()).into())));
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
            return Err(E::from(Error::Other((
                "bounded vector partition-key retained-memory overflow".to_string()).into())));
        }
    };
    sources.insert(
        position,
        BoundedSnapshotVectorSource {
            partition_key: key.clone(),
            state: state.into(),
            serving_selection: None,
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
    /// The graph this partition's walk reads, as a disconnection report
    /// names it.
    graph_stamp: crate::store::GraphStamp,
    source_position: usize,
    candidates: Option<&'g [u64]>,
    allowed_count: usize,
    live_count: usize,
    graph_allowed_count: usize,
    graph_live_count: usize,
    entry_count: usize,
    bounded_limits: bool,
    /// The statement's declared ceiling, not a per-partition credit grant.
    query_evaluation_limit: usize,
    rescore_credit: std::cell::Cell<usize>,
    /// Stored vectors final candidate scoring read for this partition, paid
    /// from `rescore_credit`; reported with the walk's own rescoring reads.
    rescore_spent: std::cell::Cell<usize>,
    layers: Vec<crate::store::SnapshotCompatibleLayer<'g>>,
    /// What this entry and its layer list were charged when admitted.
    charged_bytes: usize,
    /// How many partition routes the statement had recorded when this
    /// partition was admitted, so its own route is recorded in scope order.
    routes_before: usize,
    started: std::time::Instant,
    pages: (u64, u64, u64, u64),
}

impl UnionPartition<'_> {
    /// Whether any layer this partition walks holds rows that share another
    /// row's graph point.
    fn has_graph_members(&self) -> bool {
        self.layers
            .iter()
            .filter_map(|layer| layer.graph())
            .any(|graph| graph.has_graph_members())
    }
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
        contributing_bytes: usize,
        /// For each partition, why its graph candidates are not its answer,
        /// if they are not: it is answered by comparing its rows instead.
        short: Vec<Option<&'static str>>,
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
    _accountant: Arc<dyn MemoryBudget>,
}

impl<S: WriteSetApplicator> MemVectorExecutor<S> {
    /// The row checks a statement over `index` may spend checking its filter
    /// row by row: see `BoundedVectorCandidateGroups::row_check_allowance`.
    pub fn bounded_row_check_allowance(
        &self,
        index: &VectorIndexRef,
        k: usize,
        groups: &BoundedVectorCandidateGroups,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<usize> {
        let layout = self.store.scoped_index_layout(index, owner)?;
        Ok(groups.row_check_allowance(&layout, k))
    }

    #[doc(hidden)]
    pub fn new_engine(
        store: Arc<VectorStore>,
        transactions: contextdb_tx::manager::EngineTransactionHandle<S>,
        _hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Self {
        Self {
            store,
            tx_mgr: ExecutorTransactions::Owned(transactions),
            _accountant: accountant,
        }
    }
    #[doc(hidden)]
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
        hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Self {
        Self::new_with_accountant(store, tx_mgr, hnsw, accountant)
    }

    pub fn new_with_accountant(
        store: Arc<VectorStore>,
        tx_mgr: Arc<TransactionManager<S>>,
        _hnsw: Arc<OnceLock<RwLock<Option<HnswIndex>>>>,
        accountant: Arc<dyn MemoryBudget>,
    ) -> Self {
        Self {
            store,
            tx_mgr: ExecutorTransactions::Legacy(tx_mgr),
            _accountant: accountant,
        }
    }

    fn retain_paged_snapshot_sources<E: From<Error>>(&self,
        sources: &crate::store::PagedVectorSourceSelection, prefixes: Option<&[VectorPartitionKey]>,
        before_work: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize), owner: &mut crate::VectorCopyAdmission)
        -> std::result::Result<(Vec<BoundedSnapshotVectorSource>, usize), E> {
        let matches = |key: &VectorPartitionKey| prefixes.is_none_or(|prefixes|
            prefixes.iter().any(|prefix| partition_key_has_prefix(key, prefix)));
        let count = if prefixes.is_none() { sources.len() } else {
            let mut count = 0usize;
            sources.visit_checked(owner, before_work, |key, _| {
                if matches(key) { count = count.checked_add(1).ok_or_else(|| E::from(Error::Other("vector source count overflow".into())))?; }
                Ok::<(), E>(())
            })?;
            count
        };
        let (mut selected, mut retained_bytes) = allocate_snapshot_source_slots(count, before_retain, release_retained)?;
        let result = sources.visit_checked(owner, before_work, |key, state| {
            if matches(key) {
                retain_snapshot_source(&mut selected, key, state, &mut retained_bytes,
                    &mut || Ok::<(), E>(()), before_retain, release_retained)?;
            }
            Ok::<(), E>(())
        });
        if let Err(error) = result { drop(selected); release_retained(retained_bytes); return Err(error); }
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
        if let Some(sources) = self.store.capture_paged_partition_sources(index, owner).map_err(E::from)? {
            return self.retain_paged_snapshot_sources(&sources, None, before_source_entry,
                before_retain, release_retained, owner);
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
        if let Some(sources) = self.store.capture_paged_partition_sources(index, owner).map_err(E::from)? {
            if prefixes.iter().all(|prefix| prefix.components().len() == layout.partition_key_columns.len()) {
                let (mut selected, mut retained_bytes) = allocate_snapshot_source_slots(prefixes.len(), before_retain, release_retained)?;
                let result = (|| {
                    for prefix in prefixes {
                        before_source_entry()?;
                        if let Some(state) = sources.probe(prefix, owner).map_err(E::from)? {
                            retain_snapshot_source(&mut selected, prefix, state, &mut retained_bytes,
                                before_source_entry, before_retain, release_retained)?;
                        }
                    }
                    Ok::<(), E>(())
                })();
                if let Err(error) = result { drop(selected); release_retained(retained_bytes); return Err(error); }
                return Ok((selected, retained_bytes));
            }
            return self.retain_paged_snapshot_sources(&sources, Some(prefixes), before_source_entry,
                before_retain, release_retained, owner);
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
                auto_index_at: layout.effective_auto_index_at(),
                every_partition_fits_base_ef: true,
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
        let grouped = (|| -> std::result::Result<(), E> {
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
                                    E::from(Error::Other((
                                        "bounded vector candidate group changed".to_string()).into()))
                                })?;
                                let local = source.candidate_ids.get_or_insert_with(Vec::new);
                                let required = local.len().checked_add(1).ok_or_else(|| {
                                    E::from(Error::Other((
                                        "bounded vector candidate group length overflow"
                                            .to_string()).into()))
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
                        .query_partition_source_count(index, owner).map_err(E::from)?;
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
        for source in &mut sources {
            if owner.uses_only_raw_sources() { continue; }
            // A quarantined route serves no graph; its exact reads use the
            // raw memberships, which do not depend on the damaged catalog.
            if source.state.route_quarantine().is_some() {
                continue;
            }
            let mut capture_interrupted = None;
            let captured = source.state.capture_serving_selection_with_work(snapshot, owner, &mut || {
                match before_checkpoint() {
                    Ok(()) => true,
                    Err(error) => { capture_interrupted = Some(error); false },
                }
            });
            if let Some(error) = capture_interrupted {
                drop(sources);
                release_retained(retained_bytes);
                return Err(error);
            }
            match captured {
                Ok(selection) => source.serving_selection = selection,
                Err(error) => {
                    drop(sources);
                    release_retained(retained_bytes);
                    return Err(E::from(error));
                }
            }
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
            row_checked: false,
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
        let (snapshot, candidate_filter, snapshot_sources, group_bytes) = groups.into_cursor_parts();
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
        let query = std::mem::take(&mut cursor.query);
        let result = self.bounded_brute_force_step_with_query(cursor, &query, admit_row,
            before_source_entry, before_precharged_candidate, before_exhaustive_candidate,
            before_distance, before_retain, release_retained, owner);
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
                E::from(Error::Other((
                    "bounded vector output position overflow".to_string()).into()))
            })?;
            return Ok(BoundedVectorStep::Row(row_id, score));
        }
        // Exhaustive comparison reads each page once.
        let state = cursor
            .snapshot_sources
            .get(cursor.source_position)
            .map(|source| source.state.clone());
        let _one_pass = state
            .filter(|state| state.durable_membership_source().is_none())
            .and_then(|state| state.one_pass_reads(&mut cursor.one_pass));

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
                E::from(Error::Other((
                    "bounded filtered vector source lost its candidate group".to_string()).into()))
            })?;
            let Some(raw_row_id) = candidate_ids.get(cursor.position).copied() else {
                cursor.source_position =
                    cursor.source_position.checked_add(1).ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded vector partition position overflow".to_string()).into()))
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
                E::from(Error::Other((
                    "bounded vector candidate position overflow".to_string()).into()))
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
                                None => Err(E::from(Error::Other((
                                    "membership cursor reservation overflow".to_string()).into()))),
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
                        return Err(E::from(Error::Other((
                            "membership cursor retained bytes overflow".to_string()).into())));
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
                E::from(Error::Other((
                    "bounded vector partition position overflow".to_string()).into()))
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
                    memberships.next_with_body_identity(&mut |request| {
                        let result = match request {
                            crate::store::DormantVectorLoadRequest::Reserve(bytes) => {
                                match additional_reserved.checked_add(bytes) {
                                    Some(total)
                                        if retained_before.checked_add(total).is_some()
                                            && membership_before.checked_add(total).is_some() =>
                                    {
                                        before_retain(bytes).map(|()| additional_reserved = total)
                                    }
                                    _ => Err(E::from(Error::Other(
                                        "membership window reservation overflow".into()))),
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
                    }).map(|next| next.map(|(interval, body)|
                        (body, interval.row_id, interval.created_tx, interval.deleted_tx)))
                };
                cursor.retained_bytes += additional_reserved;
                cursor.membership_reserved_bytes += additional_reserved;
                if let Some(error) = interrupted {
                    return Err(error);
                }
                cursor.pending_membership = next.map_err(E::from)?;
                cursor.pending_membership.as_ref().map(|(_, row, visible_from, deleted)| {
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
                        E::from(Error::Other((
                            "bounded vector partition position overflow".to_string()).into()))
                    })?;
                cursor.position = 0;
                cursor.state_end = None;
                cursor.streaming_source = false;
                return Ok(BoundedVectorStep::Pending);
            };
            // The same one exhaustive-scan charge per directory entry as the
            // resident scan below; dead and future versions consume it too.
            before_exhaustive_candidate()?;
            if visible.is_some() && cursor.pending_membership.is_some()
                && let Some(memberships) = cursor.membership_cursor.as_deref()
            {
                // The stream already authenticated this exact body identity.
                // Bind its existing receipt to the scan at the same root and
                // route instead of decoding a second membership for the body.
                source.state.bind_streamed_membership(memberships,
                    &mut cursor.body_reads, owner).map_err(E::from)?;
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
                E::from(Error::Other((
                    "bounded vector candidate position overflow".to_string()).into()))
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
                        entry.vector.cosine_similarity(query),
                    )))
                })?
        };
        cursor.position = cursor.position.checked_add(1).ok_or_else(|| {
            E::from(Error::Other((
                "bounded vector candidate position overflow".to_string()).into()))
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
        owner: &mut crate::VectorCopyAdmission,
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
            owner,
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
            owner,
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
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<BoundedHnswOutcome, E>
    where
        E: From<Error>,
    {
        self.bounded_hnsw_search_grouped_with_reader_memory_refusal(
            index,
            query,
            k,
            groups,
            mode,
            |_| false,
            allowed_search_limits,
            &mut before_source_entry,
            &mut before_exact_candidate,
            &mut before_distance,
            &mut before_retain,
            &mut release_retained,
            owner,
            None,
        )
    }

    /// Workspace plumbing for the engine's typed reader-memory admission.
    /// Existing public callers keep the original never-true classification.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_hnsw_search_grouped_with_reader_memory_refusal<E>(
        &self,
        index: &VectorIndexRef,
        query: &[f32],
        k: usize,
        groups: &BoundedVectorCandidateGroups,
        mode: VectorSearchMode,
        is_reader_memory_refusal: impl Fn(&E) -> bool,
        allowed_search_limits: HnswAllowedSearchLimits,
        mut before_source_entry: impl FnMut() -> std::result::Result<(), E>,
        mut before_exact_candidate: impl FnMut() -> std::result::Result<(), E>,
        mut before_distance: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
        row_check: Option<RowCheck<'_, E>>,
    ) -> std::result::Result<BoundedHnswOutcome, E>
    where
        E: From<Error>,
    {
        // Exact partitions may be served before the union. Observe successful
        // distance admissions without replacing the caller's cancellation or
        // work-limit errors, so their spend is not granted to the union again.
        let distance_spent = Cell::new(0usize);
        let mut before_distance = || {
            before_distance()?;
            let spent = distance_spent.get().checked_add(1).ok_or_else(|| {
                E::from(Error::Other("vector query evaluation count overflow".into()))
            })?;
            distance_spent.set(spent);
            Ok::<(), E>(())
        };
        let snapshot = groups.snapshot;
        let aggregate_allowed_count = groups.aggregate_allowed_count();
        before_source_entry()?;
        let layout = self
            .store
            .scoped_index_layout(index, owner)
            .map_err(E::from)?;
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
            for source in snapshot_sources {
                before_source_entry()?;
                if source.allowed_count == 0 {
                    continue;
                }
                #[cfg(feature = "test-seams")]
                crate::auto_planning_test_seams::note_partition_cost();
            }
            let cost = groups.auto_statement_cost(&layout, k);
        // A fully enumerated residual candidate set no larger than the
        // declared search breadth is already a bounded answer domain. Compare
        // it directly under AUTO (Policies 21/23); unrelated live/history rows
        // must not turn those same admitted candidates into a graph walk.
        // Row-checked filters have no complete candidate list and cannot use
        // this decision. Explicit INDEXED keeps its maintained route.
        if mode == VectorSearchMode::Auto && groups.filtered && !groups.row_checked
            && aggregate_allowed_count <= statement_policy.hnsw_ef_search.max(search_policy_limit(k))
        {
            return bounded_uniform_exact_outcome(
                index, groups,
                BoundedHnswExactReason::SmallPartitionCandidateSetUsesExact,
                "bounded_candidate_set_within_search_breadth",
                &mut before_retain, &mut release_retained,
            );
        }
            let walk_budget = cost.walk_budget;
            contextdb_core::find_attribution::count(
                "auto_statement_walk_budget",
                walk_budget as u64,
            );
            if cost.prefers_exhaustive {
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
        let mut statement_admission = owner.scratch();
        statement_admission.reserve(checked_vector_mul(snapshot_sources.len(),
            2 * std::mem::size_of::<usize>(), "statement partition identities").map_err(E::from)?)
            .map_err(E::from)?;
        let admitted = snapshot_sources
            .iter()
            .map(|source| source.state.as_ref())
            .collect::<Vec<_>>();
        let _searched_partitions =
            crate::store::VectorStore::declare_statement_partitions(&admitted);
        let _layers_to_load = {
            let dormant_bytes = snapshot_sources
                .iter()
                .map(|source| source.dormant_load_bytes(snapshot))
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
                .map(|source| source.dormant_page_bytes(snapshot))
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
                    .map(|source| source.dormant_layer_count(snapshot))
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
        let mut union_storage_bytes = 0;
        // A filter checked row by row: one ledger for the statement, its
        // verdicts admitted before they are kept. No row check, no ledger.
        if groups.row_checked && row_check.is_none() {
            drop(partition_routes);
            release_retained(partition_route_bytes);
            return Err(E::from(Error::Other(
                ("row-checked vector candidate groups were searched without their row check"
                    .to_string())
                .into(),
            )));
        }
        let row_checked = groups.row_checked;
        let row_check_releases = RefCell::new(&mut release_retained);
        let refund_row_check = |bytes| (*row_check_releases.borrow_mut())(bytes);
        let mut release_retained = |bytes| refund_row_check(bytes);
        let row_check_ledger = match row_check {
            Some(row_check) => match RowCheckLedger::new(
                row_check,
                snapshot_sources.len(),
                &refund_row_check,
                &mut before_retain,
            ) {
                Ok(ledger) => Some(RefCell::new(ledger)),
                Err(error) => {
                    drop(partition_routes);
                    release_retained(partition_route_bytes);
                    return Err(error);
                }
            },
            None => None,
        };
        let searched = (|| -> std::result::Result<BoundedHnswOutcome, E> {
            // Pay the actual Vec capacity before allocation, rather than one
            // element at each push into an amortized multi-element allocation.
            (union_partitions, union_storage_bytes) = allocate_vector_slots(
                snapshot_sources.len(), "union partitions", &mut before_retain, &mut release_retained)?;
            let mut partition_position = 0usize;
            while partition_position < snapshot_sources.len() {
                before_source_entry()?;
                let source = snapshot_sources.get(partition_position).ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded HNSW partition position changed".to_string()).into()))
                })?;
                partition_position = partition_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded HNSW partition position overflow".to_string()).into()))
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
                // Grouping already authenticated this source's visible count
                // at the captured snapshot. Recounting opens another metadata
                // window beside the same retained groups without changing it.
                // Prefer the captured catalog's entry count over newer state.
                before_source_entry()?;
                let eligibility = crate::store::BoundedIndexEligibility {
                    entry_count: source.serving_selection.as_ref().map_or_else(
                        || state.entry_count(), |selection| selection.inspection.summary.entries),
                    live_count: source.live_count,
                };
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
                // A partition whose captured live rows are below the declared
                // FLAT_BELOW is served from its maintained flat representation:
                // every allowed row is compared exactly, through the same
                // charged path EXACT uses, and no graph is loaded for it.
                if layout.serves_flat(eligibility.live_count) {
                    let scores = merged.as_mut().ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded HNSW merge state was already consumed".to_string()).into()))
                    })?;
                    // AUTO compares rows under the request's own limits; INDEXED
                    // compares a FLAT_BELOW partition within the walk's allowance.
                    if mode == VectorSearchMode::Auto
                        && let Some(ledger) = row_check_ledger.as_ref()
                    {
                        ledger.borrow_mut().lift();
                    }
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
                        owner,
                        partition_position - 1,
                        row_check_ledger.as_ref(),
                    )?;
                    exact_candidate_count =
                        exact_candidate_count.saturating_add(partition_compared);
                    note_partition_attribution(
                        "exact",
                        PARTITION_BELOW_FLAT_THRESHOLD_REASON,
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
                    first_fallback_reason.get_or_insert(PARTITION_BELOW_FLAT_THRESHOLD_REASON);
                    partition_routes.push(VectorPartitionSearchTrace {
                        route: VectorPartitionSearchRoute::Exact,
                        reason: PARTITION_BELOW_FLAT_THRESHOLD_REASON,
                    });
                    continue;
                }
                let every_graph_point_is_allowed =
                    matches!(layout.quantization, VectorQuantization::F32)
                        && eligibility.entry_count == eligibility.live_count
                        && (!groups.filtered || source.candidate_ids_cover_all_live)
                        && !row_checked;

                let mut preload_checkpoint =
                    || before_source_entry().map_err(QuarantineAwarePreloadError::Control);
                let mut preload_retain = |bytes| {
                    before_retain(bytes).map_err(QuarantineAwarePreloadError::RetainControl)
                };
                let preload = match &source.serving_selection {
                    Some(selection) => state.preload_captured_serving_selection(
                        selection,
                        &mut preload_checkpoint,
                        &mut preload_retain,
                        &mut release_retained,
                    ),
                    None => state.preload_snapshot_compatible_hnsw_layers_for_request(
                        snapshot,
                        None,
                        &mut preload_checkpoint,
                        &mut preload_retain,
                        &mut release_retained,
                    ),
                };
                let mut preload_memory_refused = false;
                match preload {
                    Ok(_) => {}
                    Err(QuarantineAwarePreloadError::RetainControl(error))
                        if mode == VectorSearchMode::Auto && is_reader_memory_refusal(&error) =>
                    {
                        preload_memory_refused = true;
                    }
                    Err(
                        QuarantineAwarePreloadError::Control(error)
                        | QuarantineAwarePreloadError::RetainControl(error),
                    ) => return Err(error),
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
                    })) if mode == VectorSearchMode::Auto => {
                        preload_memory_refused = true;
                    }
                    Err(QuarantineAwarePreloadError::Store(error)) => {
                        return Err(E::from(error));
                    }
                }
                // Every non-empty layer this partition's snapshot selects --
                // sealed base, sealed change and mutable tail, paged or held
                // in memory -- joins the statement's one union search. No
                // partition walks its layers on its own: one with no usable
                // graph (quarantined, an incomplete chain, nothing maintained,
                // or a chain with neither points nor an authenticated streamed
                // suffix) is refused under INDEXED and compared under AUTO.
                // The selected chain has at most base, change and tail. Charge
                // its finite owner vector before capturing any paged root.
                let admitted_layer_bytes = 3usize.saturating_mul(std::mem::size_of::<
                    crate::store::SnapshotCompatibleLayer<'_>,
                >());
                before_retain(admitted_layer_bytes)?;
                // Taken before the layers, so a tail replaced in between makes a
                // later disconnection report stale rather than misattributed.
                let tail_epoch = state.tail_epoch();
                let (availability, owned_layers) = if preload_memory_refused {
                    (VectorGraphLayerAvailability::Unavailable, Vec::new())
                } else {
                    match &source.serving_selection {
                        Some(selection) => {
                            let layers = selection.owned_layers();
                            #[cfg(any(test, feature = "test-seams"))]
                            if layers.0 == VectorGraphLayerAvailability::Ready
                                && !layers.1.is_empty()
                            {
                                state.pause_graph_callback_for_test();
                            }
                            layers
                        }
                        None => state.snapshot_compatible_owned_layers(snapshot),
                    }
                };
                if availability == VectorGraphLayerAvailability::Ready
                    && (!owned_layers.is_empty()
                        || source.serving_selection.as_ref()
                            .is_some_and(|selection| selection.streamed_tail.is_some())
                        || source
                            .streamed_suffix()
                            .is_some_and(|suffix| suffix.saved_inserts() != 0))
                {
                    self.store.mark_sealed_graph_used(state);
                    let counted = (|| -> std::result::Result<(usize, usize), E> {
                    Ok(if let Some(tail) = source
                        .serving_selection.as_ref().and_then(|selection| selection.streamed_tail.as_ref()) {
                        tail.validate().map_err(E::from)?;
                        let memberships = state.durable_membership_source().ok_or_else(|| E::from(
                            Error::Other("streamed working tail has no captured directory".into())))?;
                        let mut unused_scores = Vec::new();
                        let mut unused_bytes = 0usize;
                        let mut count = |scope| bounded_exact_merge_durable(index, state,
                            memberships.as_ref(), Some((&**tail, scope)), false, query, k, snapshot,
                            &mut unused_scores, &mut unused_bytes, &mut body_reads,
                            &mut before_source_entry, &mut before_exact_candidate, &mut before_distance,
                            &mut before_retain, &mut release_retained, owner,
                            partition_position - 1, row_check_ledger.as_ref(), None);
                        let allowed_tail = count(candidates)?;
                        let live_tail = if candidates.is_some() { count(None)? } else { allowed_tail };
                        (allowed_count.checked_sub(allowed_tail).ok_or_else(|| E::from(
                            Error::Other("streamed tail exceeds captured allowed count".into())))?,
                         eligibility.live_count.checked_sub(live_tail).ok_or_else(|| E::from(
                            Error::Other("streamed tail exceeds captured live count".into())))?)
                    } else if let Some(covered_tx) = source.journal_suffix_frontier() {
                        // The graph owns only the frontier's memberships. Newer
                        // visible memberships are scored from the authenticated
                        // journal after the walk; counting them as graph members
                        // falsely reports a connected saved graph as disconnected.
                        // The bound suffix authenticates the live pending count
                        // at this exact root/frontier. A filtered scope needs only
                        // point probes for its candidates, never a partition scan.
                        let live_tail = source.streamed_suffix().expect("journal frontier has a suffix").saved_inserts();
                        let allowed_tail = if let Some(candidates) = candidates {
                            let mut count = 0usize;
                            for row in candidates {
                                let entry = state.bounded_visible_directory_entry_by_row_with_copy_admission(
                                    RowId(*row), snapshot, &mut before_source_entry, owner, None)?;
                                if entry.is_some_and(|entry| entry.membership.visible_from > covered_tx) {
                                    count = count.checked_add(1).ok_or_else(|| E::from(
                                        Error::Other("journal suffix allowed count overflow".into())))?;
                                }
                            }
                            count
                        } else { live_tail };
                        (allowed_count.checked_sub(allowed_tail).ok_or_else(|| E::from(
                            Error::Other("journal suffix exceeds captured allowed count".into())))?,
                         eligibility.live_count.checked_sub(live_tail).ok_or_else(|| E::from(
                            Error::Other("journal suffix exceeds captured live count".into())))?)
                    } else { (allowed_count, eligibility.live_count) })
                    })();
                    let (graph_allowed_count, graph_live_count) = match counted {
                        Ok(counts) => counts,
                        Err(error) => {
                            drop(owned_layers);
                            release_retained(admitted_layer_bytes);
                            return Err(error);
                        }
                    };

                    let charged_bytes = owned_layers.capacity().saturating_mul(std::mem::size_of::<
                        crate::store::SnapshotCompatibleLayer<'_>,
                    >());
                    let layer_bytes =
                        owned_layers.capacity().saturating_mul(std::mem::size_of::<
                            crate::store::SnapshotCompatibleLayer<'_>,
                        >());
                    release_retained(admitted_layer_bytes.saturating_sub(layer_bytes));
                    let graph_stamp = state.graph_stamp_of(&owned_layers, tail_epoch);
                    union_partitions.push(UnionPartition {
                        graph_stamp,
                        source_position: partition_position - 1,
                        candidates,
                        allowed_count: if row_checked {
                            crate::hnsw::union::ALLOWED_UNKNOWN
                        } else {
                            allowed_count
                        },
                        live_count: eligibility.live_count,
                        graph_allowed_count,
                        graph_live_count,
                        entry_count: eligibility.entry_count,
                        bounded_limits: every_graph_point_is_allowed
                            || candidates.is_some()
                            || row_checked,
                        query_evaluation_limit: 0,
                        rescore_credit: std::cell::Cell::new(0),
                        rescore_spent: std::cell::Cell::new(0),
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
                let reason = if preload_memory_refused {
                    "maintained_graph_memory_refused"
                } else if graph_ready {
                    "maintained_graph_incomplete"
                } else {
                    "maintained_graph_unavailable"
                };
                let scores = merged.as_mut().ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded HNSW merge state was already consumed".to_string()).into()))
                })?;
                // AUTO compares rows under the request's own limits; INDEXED
                // compares a FLAT_BELOW partition within the walk's allowance.
                if mode == VectorSearchMode::Auto
                    && let Some(ledger) = row_check_ledger.as_ref()
                {
                    ledger.borrow_mut().lift();
                }
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
                    owner,
                    partition_position - 1,
                    row_check_ledger.as_ref(),
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

            // AUTO decides up front, from declared figures, whether graph work
            // can beat EXACT: when the shared cap is not below the rows EXACT
            // would score, or every layer cannot reach its k-floor beam inside
            // the cap, the union answers by EXACT without opening a layer.
            // INDEXED always attempts the walk; the cap then refuses it by type.
            let crossover_to_exact =
                mode == VectorSearchMode::Auto && !union_partitions.is_empty() && {
                    let layer_count = union_partitions
                        .iter()
                        .map(|partition| partition.layers.len())
                        .fold(0usize, usize::saturating_add);
                    let degree = union_partitions
                        .iter()
                        .flat_map(|partition| partition.layers.iter())
                        .filter_map(|layer| layer.graph())
                        .map(|graph| graph.layer0_max_degree())
                        .max()
                        .unwrap_or(0)
                        .max(1);
                    let rows = union_partitions
                        .iter()
                        .map(|partition| partition.allowed_count)
                        .fold(0usize, usize::saturating_add);
                    let cap = crate::hnsw::union_candidate_budget(
                        statement_policy.hnsw_ef_search,
                        k,
                        degree,
                    );
                    crate::hnsw::union::graph_work_cannot_beat_exact(
                        cap,
                        rows,
                        layer_count,
                        k,
                        degree,
                    )
                };
            if crossover_to_exact {
                first_fallback_reason.get_or_insert("graph_work_not_below_exact");
            }
            if !union_partitions.is_empty() {
                let outcome = if crossover_to_exact {
                    None
                } else {
                    Some(self.bounded_union_search(
                        index,
                        query,
                        k,
                        snapshot,
                        snapshot_sources,
                        &mut union_partitions,
                        statement_policy.hnsw_ef_search,
                        layout.resolve_search_radius(),
                        HnswAllowedSearchLimits {
                            max_vector_evaluations: allowed_search_limits
                                .max_vector_evaluations
                                .checked_sub(distance_spent.get())
                                .unwrap_or(0),
                            ..allowed_search_limits
                        },
                        &mut query_self_scores,
                        &mut before_source_entry,
                        &mut before_exact_candidate,
                        &mut before_distance,
                        &mut before_retain,
                        &mut release_retained,
                        owner,
                        row_check_ledger.as_ref(),
                    )?)
                };
                let resolved = match outcome {
                    None => None,
                    Some(outcome) => {
                        union_receipt = Some(outcome.receipt);
                        match outcome.outcome {
                            UnionOutcome::Answered {
                                accepted,
                                accepted_bytes,
                                walked,
                                walked_bytes,
                                contributing,
                                contributing_bytes,
                                short,
                                short_bytes,
                            } => Some((
                                accepted,
                                accepted_bytes,
                                walked,
                                walked_bytes,
                                contributing,
                                contributing_bytes,
                                short,
                                short_bytes,
                            )),
                            UnionOutcome::Unresolved(reason) => {
                                if mode == VectorSearchMode::Indexed {
                                    return Ok(BoundedHnswOutcome::Incomplete { reason });
                                }
                                None
                            }
                        }
                    }
                };
                match resolved {
                    Some((
                        accepted,
                        accepted_bytes,
                        walked,
                        walked_bytes,
                        contributing,
                        contributing_bytes,
                        short,
                        short_bytes,
                    )) => {
                        let merge = (|| -> std::result::Result<(), E> {
                            let trace_candidates =
                                graph_trace_candidates.as_mut().ok_or_else(|| {
                                    E::from(Error::Other((
                                        "bounded HNSW trace candidate state was already consumed"
                                            .to_string()).into()))
                                })?;
                            let required_capacity = trace_candidates
                                .len()
                                .checked_add(walked.len())
                                .ok_or_else(|| {
                                    E::from(Error::Other((
                                        "bounded HNSW trace candidate length overflow".to_string()).into()))
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
                                E::from(Error::Other((
                                    "bounded HNSW merge state was already consumed".to_string()).into()))
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
                            drop(contributing);
                            release_retained(contributing_bytes);
                            drop(short);
                            release_retained(short_bytes);
                            return Err(error);
                        }
                        let answered =
                            (|| -> std::result::Result<Option<BoundedHnswOutcome>, E> {
                                for (position, partition) in union_partitions.iter().enumerate() {
                                    if let Some(reason) = short.get(position).copied().flatten() {
                                        // A graph that leaves live rows unreached is
                                        // set aside, as corrupt change state is,
                                        // until maintenance rebuilds it.
                                        if reason == GRAPH_FRONTIER_DISCONNECTED_REASON
                                            && let Some(source) =
                                                snapshot_sources.get(partition.source_position)
                                        {
                                            source
                                                .state
                                                .set_disconnected_graph(partition.graph_stamp);
                                        }
                                        // INDEXED never hides a graph that cannot
                                        // serve a partition behind comparing its
                                        // rows: the whole statement is refused,
                                        // naming the partition.
                                        if mode == VectorSearchMode::Indexed {
                                            return Ok(Some(
                                                if reason == GRAPH_FRONTIER_DISCONNECTED_REASON {
                                                    BoundedHnswOutcome::Disconnected {
                                                        partition: partition.source_position,
                                                    }
                                                } else {
                                                    BoundedHnswOutcome::Incomplete {
                                                        reason: HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
                                                    }
                                                },
                                            ));
                                        }
                                        before_source_entry()?;
                                        let source = snapshot_sources
                                            .get(partition.source_position)
                                            .ok_or_else(|| {
                                                E::from(Error::Other((
                                                    "bounded HNSW union partition position changed"
                                                        .to_string()).into()))
                                            })?;
                                        let scores = merged.as_mut().ok_or_else(|| {
                                            E::from(Error::Other((
                                                "bounded HNSW merge state was already consumed"
                                                    .to_string()).into()))
                                        })?;
                                        // AUTO compares rows under the request's own limits; INDEXED
                                        // compares a FLAT_BELOW partition within the walk's allowance.
                                        if mode == VectorSearchMode::Auto
                                            && let Some(ledger) = row_check_ledger.as_ref()
                                        {
                                            ledger.borrow_mut().lift();
                                        }
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
                                            owner,
                                            partition.source_position,
                                            row_check_ledger.as_ref(),
                                        )?;
                                        exact_candidate_count = exact_candidate_count
                                            .saturating_add(partition_compared);
                                        note_partition_attribution(
                                            "exact",
                                            reason,
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
                                        first_fallback_reason.get_or_insert(reason);
                                        partition_routes.insert(
                                            partition.routes_before + position,
                                            VectorPartitionSearchTrace {
                                                route: VectorPartitionSearchRoute::Exact,
                                                reason,
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
                                                E::from(Error::Other((
                                            "bounded HNSW selected-generation length overflow"
                                                .to_string()).into()))
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
                        drop(contributing);
                        release_retained(contributing_bytes);
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
                                    E::from(Error::Other((
                                        "bounded HNSW union partition position changed".to_string()).into()))
                                })?;
                            let scores = merged.as_mut().ok_or_else(|| {
                                E::from(Error::Other((
                                    "bounded HNSW merge state was already consumed".to_string()).into()))
                            })?;
                            // AUTO compares rows under the request's own limits; INDEXED
                            // compares a FLAT_BELOW partition within the walk's allowance.
                            if mode == VectorSearchMode::Auto
                                && let Some(ledger) = row_check_ledger.as_ref()
                            {
                                ledger.borrow_mut().lift();
                            }
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
                                owner,
                                partition.source_position,
                                row_check_ledger.as_ref(),
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
                return Err(E::from(Error::Other((
                    "bounded HNSW merge state was consumed before publication".to_string()).into())));
            };
            if rows.is_empty() {
                return Ok(BoundedHnswOutcome::CompleteEmpty);
            }
            sort_vector_scores(rows);
            let trace_candidates = if aggregate_hnsw_len == 0 {
                &[][..]
            } else {
                graph_trace_candidates.as_deref().ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded HNSW trace candidates disappeared before publication".to_string()
                    ).into()))
                })?
            };
            let trace_generations = std::mem::take(&mut selected_generations);
            let trace_generation_bytes = std::mem::take(&mut selected_generation_bytes);
            let trace_routes = std::mem::take(&mut partition_routes);
            let trace_route_bytes = std::mem::take(&mut partition_route_bytes);
            let trace = if aggregate_hnsw_len == 0 {
                drop(trace_generations);
                release_retained(trace_generation_bytes);
                let indexed = trace_routes
                    .iter()
                    .any(|route| matches!(route.route, VectorPartitionSearchRoute::Indexed));
                let mut trace = bounded_exact_route_trace(
                    index,
                    trace_routes,
                    trace_route_bytes,
                    first_fallback_reason.unwrap_or("maintained_graph_unavailable"),
                    union_receipt,
                    &mut before_retain,
                    &mut release_retained,
                )?;
                // No graph was walked. A maintained streamed suffix still
                // served an indexed partition, without an exact fallback.
                // Keep used_hnsw and the graph counters literal.
                if indexed && first_fallback_reason.is_none() {
                    trace.trace.fallback_reason = None;
                }
                trace
            } else {
                bounded_hnsw_trace(
                    index,
                    aggregate_hnsw_len,
                    trace_candidates,
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
                        drop(trace);
                        release_retained(trace_bytes);
                        return Err(E::from(error));
                    }
                };
            let rows = match merged.take() {
                Some(rows) => rows,
                None => {
                    drop(trace);
                    release_retained(trace_bytes);
                    return Err(E::from(Error::Other((
                        "bounded HNSW merge state disappeared at publication".to_string()).into())));
                }
            };
            // The published result owns this credit now; error cleanup below
            // must not also refund the former merge owner.
            merged_bytes = 0;
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
        release_retained(union_storage_bytes);
        // A row check that failed is the statement's error; an INDEXED answer
        // that ran out of row checks while comparing a partition is refused,
        // never answered from the rows it could still check.
        let searched = match row_check_ledger.map(RefCell::into_inner) {
            Some(mut ledger) => match (searched, ledger.error.take()) {
                (Ok(BoundedHnswOutcome::Complete(result)), Some(error)) => {
                    let bytes = result.retained_bytes;
                    drop(result);
                    release_retained(bytes);
                    Err(error)
                }
                (_, Some(error)) => Err(error),
                (Ok(BoundedHnswOutcome::Complete(result)), None)
                    if ledger.exhausted && mode == VectorSearchMode::Indexed =>
                {
                    let bytes = result.retained_bytes;
                    drop(result);
                    release_retained(bytes);
                    Ok(BoundedHnswOutcome::Incomplete {
                        reason: HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
                    })
                }
                (Ok(BoundedHnswOutcome::CompleteEmpty), None)
                    if ledger.exhausted && mode == VectorSearchMode::Indexed =>
                {
                    Ok(BoundedHnswOutcome::Incomplete {
                        reason: HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
                    })
                }
                (searched, None) => searched,
            },
            None => searched,
        };
        match searched {
            Ok(outcome @ BoundedHnswOutcome::Complete(_)) => Ok(outcome),
            Ok(outcome) => {
                drop(merged.take());
                release_retained(merged_bytes);
                drop(graph_trace_candidates.take());
                release_retained(graph_trace_candidate_bytes);
                drop(selected_generations);
                release_retained(selected_generation_bytes);
                drop(partition_routes);
                release_retained(partition_route_bytes);
                Ok(outcome)
            }
            Err(error) => {
                drop(merged.take());
                release_retained(merged_bytes);
                drop(graph_trace_candidates.take());
                release_retained(graph_trace_candidate_bytes);
                drop(selected_generations);
                release_retained(selected_generation_bytes);
                drop(partition_routes);
                release_retained(partition_route_bytes);
                Err(error)
            }
        }
    }

    /// Search each selected partition's admitted layers with its declared
    /// candidate budget, then merge their rows and re-score candidates from
    /// their own partition's stored vector. The request work limit is shared.
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
        search_radius: f32,
        allowed_search_limits: HnswAllowedSearchLimits,
        query_self_scores: &mut BoundedHnswQuerySelfScores<'_>,
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_streamed_candidate: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
        row_check: Option<&RefCell<RowCheckLedger<'_, '_, E>>>,
    ) -> std::result::Result<UnionSearchResult, E>
    where
        E: From<Error>,
    {
        for partition in partitions.iter_mut() {
            partition.query_evaluation_limit = allowed_search_limits.max_vector_evaluations;
        }
        let serving_layer0_degree = partitions
            .iter()
            .flat_map(|partition| partition.layers.iter())
            .filter_map(|layer| layer.graph())
            .map(|graph| graph.layer0_max_degree())
            .max()
            .unwrap_or(1);
        let budget = crate::hnsw::union_candidate_budget(ef_search, k, serving_layer0_degree);
        // The caller's evaluation limit applies even when the graph filter
        // does not use the bounded visit limit. It funds graph evaluations
        // and stored-vector rescoring together across all partitions.
        let limits = HnswAllowedSearchLimits {
            max_visited_nodes: if partitions.iter().any(|partition| partition.bounded_limits) {
                allowed_search_limits.max_visited_nodes
            } else {
                usize::MAX
            },
            max_vector_evaluations: allowed_search_limits.max_vector_evaluations,
        };
        let layer_count = partitions.iter().try_fold(0usize, |count, partition| {
            checked_vector_add(count, partition.layers.len(), "union layer count")
        }).map_err(E::from)?;
        // The graph walk already represents layer ordinals as u32. Retain
        // the same checked representation in its owner directory, avoiding
        // two pointer-sized indices per layer on 64-bit readers.
        if u32::try_from(layer_count).is_err() || u32::try_from(partitions.len()).is_err() {
            return Err(E::from(Error::Other("union layer directory exceeds native ordinal range".into())));
        }
        let mut partition_runs = Vec::<(usize, usize)>::new();
        let mut partition_run_bytes = 0usize;
        let prepared = (|| -> std::result::Result<(), E> {
            (partition_runs, partition_run_bytes) = allocate_vector_slots(
                partitions.len(), "union partition runs", before_retain, release_retained,
            )?;
            Ok(())
        })();
        if let Err(error) = prepared {
            drop(partition_runs);
            release_retained(partition_run_bytes);
            return Err(error);
        }
        // A superseded point can have a better graph distance than its
        // current stored vector. When a graph row has more than one version,
        // certify a possible stop from visible stored scores instead.
        // A row that shares another row's graph point (a graph-identity
        // member) was reached at that point's distance, not its own: its score
        // is certified from its stored vector too.
        let f32_graph_identities = matches!(self.store.with_index_layout(index, |layout| layout.quantization)
            .map_err(E::from)?, VectorQuantization::F32);
        let started = std::time::Instant::now();
        // One flag per partition for a walk that finds live rows its graph
        // does not connect, admitted before any walk can report one, so the
        // per-partition reports never grow a merged list.
        let (mut disconnected, slot_bytes) = match allocate_vector_slots(
            partitions.len(),
            "union disconnected slots",
            &mut |bytes| admitted_at("disconnected_slots", || {
                before_retain(admission_bytes("disconnected_slots", bytes))
            }),
            release_retained,
        ) {
            Ok(slots) => slots,
            Err(error) => {
                                drop(partition_runs);
                release_retained(partition_run_bytes);
                return Err(error);
            }
        };
        disconnected.resize(partitions.len(), false);
        let disconnected_slots = std::cell::RefCell::new(disconnected);
        let retained = std::cell::RefCell::new(&mut *before_retain);
        let released = std::cell::RefCell::new(&mut *release_retained);
        let navigation_session = crate::hnsw::union::NavigationSession::new(Some(owner.funder()));
        let mut navigation_copy_owner = owner.scratch();
        let admitted_owner = std::cell::RefCell::new(&mut navigation_copy_owner);
        let membership_error = std::cell::RefCell::new(None);
        let source_work_error = std::cell::RefCell::new(None);
        let source_checkpoints = std::cell::RefCell::new(&mut *before_source_entry);
        // The version of the row the snapshot sees, from the one directory
        // read that answers both whether the row is visible and which stored
        // version it is: the walk asks the first and then, for the same row,
        // the second. The snapshot is fixed for the statement, so the answer
        // is the same both times.
        let last_visible = std::cell::Cell::new(None::<(usize, RowId, Option<Lsn>)>);
        // Each admitted layer is a separate graph and runs its own search to its
        // own termination; one beam shared across disjoint graphs let a layer
        // be pruned unexpanded. Each selected partition receives the declared
        // per-index cap, admission included. The request logical work limit
        // remains shared across all partitions.
        let funder = owner.funder();
        let mut run_layer =
            |slice: &[crate::hnsw::HnswUnionLayer<'_>],
             remaining: usize,
             spent: usize,
             settled: &mut [crate::hnsw::union::HnswUnionCandidate]| {
                // Mandatory admissions must reclaim this walk's actual cache.
                // A separate empty session cannot release the retained pages.
                // Keep the session per slice: layer ordinals restart at zero
                // for another partition and must never alias its cached root.
                let navigation_session = crate::hnsw::union::NavigationSession::new(Some(funder.clone()));
                crate::hnsw::union::search_union_allowed_bounded_with_navigation(
                    slice,
                    query,
                    k,
                    ef_search,
                    remaining,
                    // The declared work limit is one too: what earlier partitions
                    // spent is gone from it; the per-index cap is funded separately.
                    HnswAllowedSearchLimits {
                        max_vector_evaluations: limits
                            .max_vector_evaluations
                            .saturating_sub(spent),
                        ..limits
                    },
                    // Currentness is a fact of this admitted partition, not
                    // of unrelated partitions visited earlier or later. F32
                    // identity members store the representative's same bits.
                    slice.iter().all(|layer| {
                        let partition = &partitions[layer.partition];
                        partition.entry_count == partition.live_count
                            && (f32_graph_identities || !partition.has_graph_members())
                            && sources.get(partition.source_position)
                                .and_then(|source| source.serving_selection.as_ref())
                                .is_none_or(|selection| selection.streamed_tail.is_none())
                    }),
                    true,
                    query_self_scores,
                    |position, row_id| {
                        let allowed = partitions.get(position).is_some_and(|partition| {
                            partition.candidates.is_none_or(|candidates| {
                                candidates.binary_search(&row_id.0).is_ok()
                            }) && sources
                                .get(partition.source_position)
                                .is_some_and(|source| {
                                    if membership_error.borrow().is_some() {
                                        return false;
                                    }
                                    if let Some(tail) = source.serving_selection.as_ref()
                                        .and_then(|selection| selection.streamed_tail.as_ref()) {
                                        match tail.includes_row(row_id, snapshot, &mut admitted_owner.borrow_mut(), &mut || {
                                            match (*source_checkpoints.borrow_mut())() {
                                                Ok(()) => true,
                                                Err(error) => { source_work_error.replace(Some(error)); false },
                                            }
                                        }) {
                                            Ok(true) => {
                                                last_visible.set(Some((position, row_id, None)));
                                                return false;
                                            },
                                            Ok(false) => {},
                                            Err(error) => { membership_error.replace(Some(error)); return false; },
                                        }
                                    }
                                    if let Some((_, _, visible)) = last_visible
                                        .get()
                                        .filter(|(at, row, _)| *at == position && *row == row_id)
                                    {
                                        return visible.is_some();
                                    }
                                    if let Some(covered_tx) = source.journal_suffix_frontier() {
                                        match source.state.try_visible_directory_entry_by_row(
                                            row_id, snapshot, &mut admitted_owner.borrow_mut()) {
                                            Ok(entry) => {
                                                let visible = entry
                                                    .filter(|entry| entry.membership.visible_from <= covered_tx)
                                                    .map(|entry| entry.lsn);
                                                last_visible.set(Some((position, row_id, visible)));
                                                return visible.is_some();
                                            }
                                            Err(error) => {
                                                membership_error.replace(Some(error));
                                                return false;
                                            }
                                        }
                                    }
                                    match source
                                        .state
                                        .try_directory_visible_lsn_with_copy_admission(
                                            row_id,
                                            snapshot,
                                            &mut admitted_owner.borrow_mut(),
                                            &mut |token, bytes, component, operation, hint| {
                                                navigation_session.reserve_for(
                                                    token, bytes, component, operation, hint,
                                                )
                                            },
                                        ) {
                                        Ok(visible) => {
                                            last_visible.set(Some((position, row_id, visible)));
                                            visible.is_some()
                                        }
                                        Err(error) => {
                                            *membership_error.borrow_mut() = Some(error);
                                            false
                                        }
                                    }
                                })
                        });
                        // A filter checked row by row is asked only about a
                        // visible row, and each uncached ask is debited
                        // before the row is read.
                        allowed
                            && row_check.is_none_or(|ledger| {
                                ledger.borrow_mut().admit(
                                    partitions[position].source_position,
                                    row_id,
                                    &mut |bytes| (*retained.borrow_mut())(bytes),
                                    &mut || (*source_checkpoints.borrow_mut())(),
                                )
                            })
                    },
                    |position, row_id, source_entry, distance| {
                        let partition = &partitions[position];
                        let state = &sources[partition.source_position].state;
                        navigation_session.release_navigation_before_body();
                        state.bounded_score_visible_candidate_with_copy_admission(
                            index,
                            row_id,
                            snapshot,
                            query,
                            None,
                            source_entry,
                            distance,
                            &mut |bytes| {
                                // Probe the same operation owner before the existing
                                // retained callback admits and observes its charge.
                                let mut admission = owner.scratch();
                                navigation_session
                                    .reserve(&mut admission, bytes)
                                    .map_err(E::from)?;
                                drop(admission);
                                (*retained.borrow_mut())(bytes)
                            },
                            &mut |bytes| (*released.borrow_mut())(bytes),
                            &mut admitted_owner.borrow_mut(),
                            &mut |token, bytes, component, operation, hint| {
                                navigation_session
                                    .reserve_for(token, bytes, component, operation, hint)
                            },
                        )
                    },
                    &mut || (*source_checkpoints.borrow_mut())(),
                    &mut *before_distance,
                    |event| {
                        apply_hnsw_scratch_acquire(event, &mut |bytes| {
                            // Actual admission fails before LRU reclamation. The scoped
                            // probe releases before the original scratch callback reserves
                            // and observes its unchanged charge; no cache eviction merely
                            // for invoking a callback, and no double retained reservation.
                            let mut admission = owner.scratch();
                            navigation_session
                                .reserve(&mut admission, bytes)
                                .map_err(E::from)?;
                            drop(admission);
                            (*retained.borrow_mut())(bytes)
                        })
                    },
                    |event| {
                        apply_hnsw_scratch_release(event, &mut |bytes| {
                            (*released.borrow_mut())(bytes)
                        })
                    },
                    Some(&navigation_session),
                    search_radius,
                    settled,
                    Some(&mut |position, row_id| {
                        partitions.get(position).and_then(|partition| {
                            sources.get(partition.source_position).and_then(|source| {
                                if membership_error.borrow().is_some() {
                                    return None;
                                }
                                if let Some((_, _, visible)) = last_visible
                                    .get()
                                    .filter(|(at, row, _)| *at == position && *row == row_id)
                                {
                                    return visible;
                                }
                                match source.state.try_directory_visible_lsn_with_copy_admission(
                                    row_id,
                                    snapshot,
                                    &mut admitted_owner.borrow_mut(),
                                    &mut |token, bytes, component, operation, hint| {
                                        navigation_session
                                            .reserve_for(token, bytes, component, operation, hint)
                                    },
                                ) {
                                    Ok(visible) => {
                                        last_visible.set(Some((position, row_id, visible)));
                                        visible
                                    }
                                    Err(error) => {
                                        *membership_error.borrow_mut() = Some(error);
                                        None
                                    }
                                }
                            })
                        })
                    }),
                )
            };
        // Layers are grouped by partition: a partition's base, change and
        // fresh tail jointly hold its rows, so they are searched together;
        // separate partitions are separate graphs and each runs its own search.
        let mut layer_offset = 0usize;
        for (position, partition) in partitions.iter().enumerate() {
            if !partition.layers.is_empty() {
                partition_runs.push((position, layer_offset));
                layer_offset += partition.layers.len();
            }
        }
        let searched = crate::hnsw::union::merge_layer_searches_with_candidate_limit(
            partition_runs.len(),
            budget,
            k,
            crate::hnsw::hnsw_search_candidate_cap_for_ef(ef_search, k),
            |run, remaining, spent, settled| {
                let (position, offset) = partition_runs[run];
                let partition = &partitions[position];
                let (mut layers, descriptor_bytes) = allocate_vector_slots(
                    partition.layers.len(), "active union layer descriptors",
                    &mut |bytes| (*retained.borrow_mut())(bytes),
                    &mut |bytes| (*released.borrow_mut())(bytes),
                )?;
                for layer in &partition.layers {
                    let Some(graph) = layer.graph() else {
                        drop(layers);
                        (*released.borrow_mut())(descriptor_bytes);
                        return Err(E::from(Error::Other("a captured union layer was released".into())));
                    };
                    layers.push(crate::hnsw::HnswUnionLayer {
                        graph, partition: position,
                        allowed_count: partition.graph_allowed_count,
                        live_count: partition.graph_live_count,
                        seed_rows: partition.candidates.map(crate::hnsw::union::UnionSeedRows::Slice),
                        covered_lsn: layer.identity.map(|identity| identity.covered_lsn),
                    });
                }
                let searched = run_layer(&layers, remaining, spent, settled);
                drop(layers);
                (*released.borrow_mut())(descriptor_bytes);
                searched.map(|mut result| {
                    // Keep the per-index rescoring allowance, less stored
                    // scores already read by this walk. Final scoring also
                    // spends the query remainder shared by all partitions.
                    partitions[position]
                        .rescore_credit
                        .set(remaining.saturating_sub(result.rescore_evaluations));
                    for candidate in &mut result.candidates {
                        candidate.layer += offset;
                    }
                    // The walk's own report moves into the admitted flags and
                    // its charge goes back, so the merge carries no list.
                    let reported = std::mem::take(&mut result.disconnected);
                    for partition in reported.iter().copied() {
                        if let Some(slot) = disconnected_slots.borrow_mut().get_mut(partition) {
                            *slot = true;
                        }
                    }
                    let reported_bytes = reported
                        .capacity()
                        .saturating_mul(std::mem::size_of::<usize>());
                    drop(reported);
                    result.retained_bytes = result.retained_bytes.saturating_sub(reported_bytes);
                    (*released.borrow_mut())(reported_bytes);
                    result
                })
            },
            |all, incoming| {
                let required = checked_vector_add(
                    all.candidates.len(),
                    incoming.len(),
                    "union candidate merge",
                )
                .map_err(E::from)?;
                grow_vector_copy(
                    &mut all.candidates,
                    required,
                    Some(&mut all.retained_bytes),
                    "union candidate merge",
                    // Candidate relocation is not another source entry or
                    // distance evaluation; retain the query's work counts.
                    &mut || Ok(()),
                    &mut |bytes| {
                        let mut admission = owner.scratch();
                        navigation_session
                            .reserve(&mut admission, bytes)
                            .map_err(E::from)?;
                        drop(admission);
                        (*retained.borrow_mut())(bytes)
                    },
                    &mut |bytes| (*released.borrow_mut())(bytes),
                )
                .map(|_| ())
            },
            |bytes| (*released.borrow_mut())(bytes),
        );
        drop(run_layer);
        drop(source_checkpoints);
        drop(retained);
        drop(partition_runs);
        (*released.borrow_mut())(partition_run_bytes);
        drop(released);
        // The walk is over. A held tail's read lock goes back now, before
        // resolving takes each partition's publication lock: a publication
        // waiting for that tail's write side already holds it.
        for partition in partitions.iter_mut() {
            for layer in &mut partition.layers {
                layer.release_tail();
            }
        }
        #[cfg(feature = "test-seams")]
        run_after_walk_hook_for_test();
        let partitions = &*partitions;
        drop(admitted_owner);
        drop(navigation_copy_owner);
        drop(navigation_session);
        let disconnected_slots = disconnected_slots.into_inner();
        let membership_error = source_work_error.into_inner()
            .or_else(|| membership_error.into_inner().map(E::from));
        if let Some(error) = membership_error {
            // A bool visibility callback can record an error while the walk
            // still returns a retained result. Dispose of that result before
            // returning the original membership error, which keeps precedence.
            if let Ok(searched) = searched {
                let bytes = searched.retained_bytes;
                drop(searched);
                release_retained(bytes);
            }
            drop(disconnected_slots);
            release_retained(slot_bytes);
            return Err(error);
        }
        let searched = match searched {
            Ok(searched) => searched,
            Err(error) => {
                drop(disconnected_slots);
                release_retained(slot_bytes);
                        return Err(error);
            }
        };
        // The walk alone: final candidate scoring is counted, not timed, here.
        let walk_micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        let resolved = self.resolve_union_candidates(
            index,
            query,
            k,
            snapshot,
            sources,
            partitions,
            &searched,
            crate::hnsw::hnsw_search_candidate_cap_for_ef(ef_search, k),
            &disconnected_slots,
            before_source_entry,
            before_streamed_candidate,
            before_distance,
            before_retain,
            release_retained,
            owner,
            row_check,
        );
        // The walk's rescoring reads and final candidate scoring's, both paid
        // from the allowance the declared breadth sizes; counted after the
        // candidates are resolved, whether they answered or ran out.
        let rescore_evaluations = partitions
            .iter()
            .map(|partition| partition.rescore_spent.get())
            .fold(searched.rescore_evaluations, usize::saturating_add);
        #[cfg(feature = "test-seams")]
        crate::hnsw::record_union_search(crate::hnsw::UnionSearchReceipt {
            budget: searched.budget,
            resolved_ef_search: ef_search,
            admission_evaluations: searched.admission_evaluations,
            expansion_evaluations: searched.expansion_evaluations,
            rescore_evaluations,
            layers_admitted: searched.layers_admitted,
            partitions: partitions.len(),
            frontier_exhausted: searched.frontier_exhausted,
            terminal: searched.terminal.as_str(),
            below_admission_floor: searched.below_admission_floor.is_some(),
            expansion_cap: searched.expansion_cap,
            // What one expanded point can reach: the widest admitted
            // graph's layer-0 maximum degree, read from the graphs.
            layer_zero_degree: serving_layer0_degree,
            allowed_admissions: searched.allowed_admissions,
            disconnected_partitions: disconnected_slots.iter().filter(|slot| **slot).count(),
            unresolved: match &resolved {
                Ok(UnionOutcome::Unresolved(reason)) => Some(*reason),
                _ => None,
            },
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
            count("union_rescore_evaluations", rescore_evaluations as u64);
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
            count("union_walk_micros", walk_micros);
        }
        let receipt = VectorSearchBudgetReceipt {
            global_candidate_budget: searched.budget,
            admission_evaluations: searched.admission_evaluations,
            expansion_evaluations: searched.expansion_evaluations,
            rescore_evaluations,
            layers_admitted: searched.layers_admitted,
            budget_terminal: searched.terminal.as_str(),
        };
        drop(disconnected_slots);
        release_retained(slot_bytes);
        let result_bytes = searched.retained_bytes;
        drop(searched);
        release_retained(result_bytes);
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
        candidate_limit: usize,
        disconnected: &[bool],
        before_source_entry: &mut impl FnMut() -> std::result::Result<(), E>,
        before_streamed_candidate: &mut impl FnMut() -> std::result::Result<(), E>,
        before_distance: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        owner: &mut crate::VectorCopyAdmission,
        row_check: Option<&RefCell<RowCheckLedger<'_, '_, E>>>,
    ) -> std::result::Result<UnionOutcome, E>
    where
        E: From<Error>,
    {
        // A walk whose filter checks ran out treated every row it could no
        // longer check as rejected: whatever it kept is not an answer.
        if let Some(ledger) = row_check {
            let mut ledger = ledger.borrow_mut();
            if let Some(error) = ledger.error.take() {
                return Err(error);
            }
            if ledger.exhausted {
                return Ok(UnionOutcome::Unresolved(
                    HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
                ));
            }
        }
        // The declared query ceiling is shared by graph work, walk rescoring
        // and final scoring across every partition. Empty graph partitions
        // spend this remainder directly; they receive no per-partition grant.
        let evaluation_limit = partitions
            .first()
            .map_or(0, |partition| partition.query_evaluation_limit);
        let Some(mut evaluation_credit) = searched
            .admission_evaluations
            .checked_add(searched.expansion_evaluations)
            .and_then(|spent| spent.checked_add(searched.rescore_evaluations))
            .and_then(|spent| evaluation_limit.checked_sub(spent))
        else {
            return Ok(UnionOutcome::Unresolved(
                HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
            ));
        };
        // A walk over F32 layers reaches a row at the distance its stored
        // vector scores, so the next row's walk distance bounds what it can
        // still score and scoring stops once the page cannot change. A
        // quantized layer's walk distance is its code's, which sits on either
        // side of the score of the vector the row stores: every kept row is
        // scored, and the page is the best of those scores.
        let walk_distance_is_score = matches!(
            self.store
                .with_index_layout(index, |layout| layout.quantization)
                .map_err(E::from)?,
            VectorQuantization::F32
        );
        // keep_reached/keep_members authenticate each visible version before
        // retaining it. A restamped row enters at its resolved stored distance;
        // an unchanged F32 graph-identity member has the same stored bits as
        // its representative (apart from signed zero). Thus these *retained*
        // distances remain score bounds even when other versions/tails exist.
        // This does not change the walk's separate frontier-stop certificate.
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
        // short. Only the candidate-cap proof seam produces one today. A
        // partition whose live rows its graph does not connect is compared
        // the same way, under INDEXED too: its graph cannot say which rows it
        // left out.
        let (mut short, short_bytes) = allocate_vector_slots(
            partitions.len(),
            "union short partitions",
            before_retain,
            release_retained,
        )?;
        short.resize(partitions.len(), None);
        for (slot, disconnected) in short.iter_mut().zip(disconnected) {
            if *disconnected {
                *slot = Some(GRAPH_FRONTIER_DISCONNECTED_REASON);
            }
        }
        #[cfg(feature = "test-seams")]
        for (position, partition) in partitions.iter().enumerate() {
            if let Some(source) = sources.get(partition.source_position)
                && let Some(cap) = self
                    .store
                    .graph_candidate_caps()
                    .graph_candidate_cap(index, Some(&source.partition_key))
                && short[position].is_none()
                && cap < k.min(partition.allowed_count)
            {
                short[position] = Some("maintained_graph_incomplete");
            }
        }
        let required = partitions
            .iter()
            .zip(&short)
            .filter(|(_, short)| short.is_none())
            .map(|(partition, _)| partition.allowed_count)
            .fold(0usize, usize::saturating_add)
            .min(k);
        // Each partition's candidates are scored against the serving pages
        // its walk already read, opened when its first candidate is scored.
        // Beside it, how many of that partition's candidates were considered.
        let (mut scoring, slot_bytes) = match allocate_vector_slots(
            partitions.len(),
            "union scoring slots",
            before_retain,
            release_retained,
        ) {
            Ok(slots) => slots,
            Err(error) => {
                drop(short);
                release_retained(short_bytes);
                return Err(error);
            }
        };
        scoring.resize_with(partitions.len(), || {
            (None::<Option<crate::store::SealedPageScoring>>, 0usize)
        });
        let mut accepted = Vec::<(RowId, f32)>::new();
        let mut accepted_bytes = 0usize;
        // Every row the walk handed back, for the route trace: scored rows at
        // their stored score, the rest at the score their walk distance gives.
        let mut walked = Vec::<(RowId, f32)>::new();
        let mut walked_bytes = 0usize;
        let mut contributing = Vec::<(usize, usize)>::new();
        let mut contributing_bytes = 0usize;
        // Graph rescoring also retains its existing per-walk allowance.
        // Every actual final point load spends the shared query credit once.
        let mut rescore_exhausted = false;
        // The graph candidates are already retained. Remember only the prefix
        // actually resolved, so a suffix row is not point-loaded again even
        // when its first load produced no score. No tail identity set is kept.
        let mut resolved_prefix = 0usize;
        let resolved = (|| -> std::result::Result<(), E> {
            let mut scoring_open = true;
            // Stack-only certificate from one actual stored score. Equal F32
            // graph identities share stored bits (zero signs are immaterial
            // to the cosine fold). A changed version never uses this proof.
            let mut last_stored_identity: Option<(usize, usize, f32)> = None;
            for (position, candidate) in searched.candidates.iter().enumerate() {
                before_source_entry()?;
                // A short partition's candidates are neither scored nor
                // reported: its rows are compared instead.
                if short.get(candidate.partition).copied().flatten().is_some() {
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
                    E::from(Error::Other((
                        "bounded HNSW union walked-row count overflow".to_string()).into()))
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
                    E::from(Error::Other((
                        "bounded HNSW union partition position changed".to_string()).into()))
                })?;
                let source = sources.get(partition.source_position).ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded HNSW union partition position changed".to_string()).into()))
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
                if !state.try_directory_has_visible_row(candidate.row_id, snapshot, owner)?
                    || partition.candidates.is_some_and(|candidates| {
                        candidates.binary_search(&candidate.row_id.0).is_err()
                    })
                {
                    continue;
                }
                // A changed/current private body belongs to the streamed
                // delta. Do not score it once from a saved layer and again
                // from that delta, even when the graph still names its RowId.
                if let Some(tail) = source.serving_selection.as_ref()
                    .and_then(|selection| selection.streamed_tail.as_ref()) {
                    let mut interrupted = None;
                    let included = tail.includes_row(candidate.row_id, snapshot, owner, &mut || {
                        match before_source_entry() {
                            Ok(()) => true, Err(error) => { interrupted = Some(error); false },
                        }
                    });
                    if let Some(error) = interrupted { return Err(error); }
                    if included.map_err(E::from)? {
                        continue;
                    }
                }
                // Layer identities can overlap. Score each logical row once.
                if searched.candidates[..position].iter().any(|earlier| {
                    earlier.partition == candidate.partition && earlier.row_id == candidate.row_id
                }) {
                    continue;
                }
                // This result is consumed inside the same bounded search
                // that supplied the walker its stored-vector scorer: that
                // closure used this snapshot and this operation's admission
                // (including its historical durable route). Graph distances
                // never populate resolved_score and still require a load.
                let resolved_score = candidate.resolved_score;
                let score_checked = candidate.score_checked;
                if walk_distance_is_score && !score_checked
                    && let Some(origin) = candidate.origin
                    && let Some((layer, previous_origin, score)) = last_stored_identity
                    && layer == candidate.layer && previous_origin == origin
                    && accepted.iter().filter(|(row, accepted_score)| {
                        accepted_score.total_cmp(&score) == std::cmp::Ordering::Greater
                            || (accepted_score.total_cmp(&score) == std::cmp::Ordering::Equal
                                && *row <= candidate.row_id)
                    }).count() >= required
                {
                    // Membership/currentness was authenticated above and in
                    // keep_reached/keep_members. This row cannot enter; do not
                    // reread an identical body or manufacture a native score.
                    walked.push((candidate.row_id, score));
                    continue;
                }

                if !owner.historical_durable_reads()
                    && page_scoring.is_none()
                    && !score_checked
                {
                    *page_scoring = Some(state.sealed_page_scoring(
                        snapshot,
                        source.serving_selection.as_deref(),
                        query,
                        before_source_entry,
                        before_retain,
                        release_retained,
                    )?);
                }
                if !score_checked {
                    let credit = partitions[candidate.partition].rescore_credit.get();
                    if credit == 0 || evaluation_credit == 0 {
                        rescore_exhausted = true;
                        return Ok(());
                    }
                    evaluation_credit -= 1;
                    partitions[candidate.partition]
                        .rescore_credit
                        .set(credit - 1);
                    let spent = &partitions[candidate.partition].rescore_spent;
                    spent.set(spent.get() + 1);
                }
                resolved_prefix = position + 1;
                let score = match score_checked {
                    true => resolved_score,
                    false => {
                        let score = state.bounded_score_visible_candidate(
                            index,
                            candidate.row_id,
                            snapshot,
                            query,
                            page_scoring.as_mut().and_then(Option::as_mut),
                            before_source_entry,
                            before_distance,
                            before_retain,
                            release_retained,
                            owner,
                        )?;
                        if score.is_some()
                            && let Some(streamed) = source.streamed_suffix()
                        {
                            if let Some(visible) = state
                                .bounded_visible_directory_entry_by_row_with_copy_admission(
                                    candidate.row_id,
                                    snapshot,
                                    before_source_entry,
                                    owner,
                                    None,
                                )?
                            {
                                if visible.lsn > streamed.covered_lsn() {
                                    streamed.observe_vector_decode(
                                        visible.row_id,
                                        visible.created_tx,
                                        visible.lsn,
                                    );
                                }
                            }
                        }
                        score
                    }
                };
                let Some(score) = score else {
                    walked.push((candidate.row_id, walk_score));
                    continue;
                };
                if walk_distance_is_score && !score_checked
                    && let Some(origin) = candidate.origin
                {
                    last_stored_identity = Some((candidate.layer, origin, score));
                }
                walked.push((candidate.row_id, score));
                let required_capacity = accepted.len().checked_add(1).ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded HNSW union accepted-row count overflow".to_string()).into()))
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
                let mut ordinal = candidate.layer;
                let owner = partitions.iter().enumerate().find_map(|(position, partition)| {
                    if ordinal < partition.layers.len() { Some((position, ordinal)) }
                    else { ordinal -= partition.layers.len(); None }
                });
                if let Some(owner) = owner
                    && !contributing.contains(&owner)
                {
                    let required_capacity = contributing.len().checked_add(1).ok_or_else(|| {
                        E::from(Error::Other("union contributing-layer count overflow".into()))
                    })?;
                    grow_vector_copy(
                        &mut contributing,
                        required_capacity,
                        Some(&mut contributing_bytes),
                        "union contributing layers",
                        &mut || Ok(()),
                        before_retain,
                        release_retained,
                    )?;
                    contributing.push(owner);
                }
            }
            for (position, partition) in partitions.iter().enumerate() {
                if rescore_exhausted {
                    break;
                }
                before_source_entry()?;
                if short.get(position).copied().flatten().is_some() {
                    continue;
                }
                let source = sources.get(partition.source_position).ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded HNSW union partition position changed".to_string()).into()))
                })?;
                let Some(streamed) = source.streamed_suffix() else {
                    continue;
                };
                if source.serving_selection.as_ref().map_or(0, |s| s.suffix_saved_inserts()) == 0 {
                    continue;
                }
                let partition_required = partition.allowed_count.min(k);
                if partition_required == 0 {
                    continue;
                }
                let mut suffix_heap = Vec::<(RowId, f32)>::new();
                let mut suffix_heap_bytes = 0usize;
                let state = &source.state;
                let mut stream_scratch = owner.scratch();
                let mut interrupted: Option<E> = None;
                let stream_result = streamed.stream_tail(
                    &mut |entry| -> Result<bool> {
                        if interrupted.is_some() || rescore_exhausted {
                            return Ok(false);
                        }
                        if entry.visible_from.0 > snapshot.0 {
                            return Ok(true);
                        }
                        if let Some(deleted) = entry.deleted_tx {
                            if deleted.0 <= snapshot.0 {
                                return Ok(true);
                            }
                        }
                        if let Some(candidates) = partition.candidates {
                            if candidates.binary_search(&entry.row_id.0).is_err() {
                                return Ok(true);
                            }
                        }
                        if searched.candidates[..resolved_prefix].iter().any(|candidate| {
                            candidate.partition == position && candidate.row_id == entry.row_id
                        }) {
                            return Ok(true);
                        }
                        if suffix_heap.iter().any(|(id, _)| *id == entry.row_id) {
                            return Ok(true);
                        }
                        let visible = match state.bounded_visible_directory_entry_by_row_with_copy_admission(
                            entry.row_id,
                            snapshot,
                            before_source_entry,
                            owner,
                            None,
                        ) {
                            Ok(Some(v)) => v,
                            Ok(None) => return Ok(true),
                            Err(e) => {
                                interrupted = Some(e);
                                return Ok(false);
                            }
                        };
                        if (visible.created_tx, visible.lsn) != (entry.vector_created_tx, entry.vector_lsn)
                            || visible.membership.owning_row_tx != entry.row_created_tx
                            || visible.membership.row_lsn != entry.row_lsn
                            || visible.membership.visible_from != entry.visible_from
                            // Closing a retained placement does not change
                            // the immutable identity of its journal upsert.
                            || visible.membership.deleted_tx.is_some_and(|tx| tx.0 <= snapshot.0)
                            || visible.deleted_tx.is_some_and(|tx| tx.0 <= snapshot.0)
                        {
                            return Ok(true);
                        }
                        if let Some(ledger) = row_check {
                            let mut ledger_ref = ledger.borrow_mut();
                            let admitted = ledger_ref.admit(
                                partition.source_position,
                                entry.row_id,
                                before_retain,
                                before_source_entry,
                            );
                            if let Some(err) = ledger_ref.error.take() {
                                interrupted = Some(err);
                                return Ok(false);
                            }
                            if ledger_ref.exhausted {
                                rescore_exhausted = true;
                                return Ok(false);
                            }
                            drop(ledger_ref);
                            if !admitted {
                                return Ok(true);
                            }
                        }
                        let credit = partitions[position].rescore_credit.get();
                        if evaluation_credit == 0
                            || (!partition.layers.is_empty() && credit == 0)
                        {
                            rescore_exhausted = true;
                            return Ok(false);
                        }
                        evaluation_credit -= 1;
                        if !partition.layers.is_empty() {
                            partitions[position].rescore_credit.set(credit - 1);
                        }
                        let spent = &partitions[position].rescore_spent;
                        spent.set(spent.get() + 1);
                        let score = match state.bounded_score_visible_candidate(
                            index,
                            entry.row_id,
                            snapshot,
                            query,
                            None,
                            before_source_entry,
                            before_distance,
                            before_retain,
                            release_retained,
                            owner,
                        ) {
                            Ok(Some(s)) => s,
                            Ok(None) => return Ok(true),
                            Err(e) => {
                                interrupted = Some(e);
                                return Ok(false);
                            }
                        };
                        streamed.observe_vector_decode(visible.row_id, visible.created_tx, visible.lsn);
                        if let Err(e) = merge_bounded_vector_score(
                            &mut suffix_heap,
                            &mut suffix_heap_bytes,
                            (entry.row_id, score),
                            partition_required,
                            before_source_entry,
                            before_retain,
                            release_retained,
                        ) {
                            interrupted = Some(e);
                            return Ok(false);
                        }
                        Ok(true)
                    },
                    &mut stream_scratch,
                );

                let partition_res = (|| -> std::result::Result<(), E> {
                    if let Some(err) = interrupted {
                        return Err(err);
                    }
                    stream_result.map_err(E::from)?;
                    if rescore_exhausted {
                        return Ok(());
                    }
                    for (row_id, score) in suffix_heap.iter().copied() {
                        let required_capacity = walked.len().checked_add(1).ok_or_else(|| {
                            E::from(Error::Other((
                                "bounded HNSW union walked-row count overflow".to_string()).into()))
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
                        walked.push((row_id, score));

                        let required_capacity = accepted.len().checked_add(1).ok_or_else(|| {
                            E::from(Error::Other((
                                "bounded HNSW union accepted-row count overflow".to_string()).into()))
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
                        accepted.push((row_id, score));
                    }
                    Ok(())
                })();
                drop(suffix_heap);
                release_retained(suffix_heap_bytes);
                partition_res?;
                if rescore_exhausted {
                    break;
                }
            }
            if partitions.iter().any(|partition| sources.get(partition.source_position)
                .and_then(|source| source.serving_selection.as_ref())
                .is_some_and(|selection| selection.streamed_tail.is_some())) {
                // Reuse the already-admitted candidate allocation as a K heap.
                // Sorted ascending quality is also a valid worst-first heap.
                accepted.sort_unstable_by(|left, right| vector_score_quality(right, left));
                accepted.truncate(k);
                accepted.reverse();
                for (position, partition) in partitions.iter().enumerate() {
                    if short.get(position).copied().flatten().is_some() { continue; }
                    let source = sources.get(partition.source_position).ok_or_else(|| E::from(
                        Error::Other("streamed tail lost its captured source".into())))?;
                    let Some(tail) = source.serving_selection.as_ref()
                        .and_then(|selection| selection.streamed_tail.as_ref()) else { continue; };
                    tail.validate().map_err(E::from)?;
                    let memberships = source.state.durable_membership_source().ok_or_else(|| E::from(
                        Error::Other("streamed working tail has no captured directory".into())))?;
                    let credit = if partition.layers.is_empty() {
                        evaluation_credit
                    } else {
                        evaluation_credit.min(partition.rescore_credit.get())
                    };
                    let exhausted = Cell::new(false);
                    let mut candidate = || {
                        before_streamed_candidate()?;
                        evaluation_credit -= 1;
                        if !partition.layers.is_empty() {
                            partition.rescore_credit.set(partition.rescore_credit.get() - 1);
                        }
                        partition.rescore_spent.set(partition.rescore_spent.get() + 1);
                        Ok(())
                    };
                    bounded_exact_merge_durable(index, &source.state, memberships.as_ref(),
                        Some((&**tail, partition.candidates)), true, query, k, snapshot,
                        &mut accepted, &mut accepted_bytes, &mut crate::store::RawVectorBodyReads::default(),
                        before_source_entry, &mut candidate, before_distance, before_retain,
                        release_retained, owner, partition.source_position, row_check,
                        Some((credit, &exhausted)))?;
                    if exhausted.get() {
                        rescore_exhausted = true;
                        break;
                    }
                    tail.validate().map_err(E::from)?;
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
            drop(contributing);
            release_retained(contributing_bytes);
            drop(short);
            release_retained(short_bytes);
            return Err(error);
        }
        if rescore_exhausted || row_check.is_some_and(|l| l.borrow().exhausted) {
            drop(accepted);
            release_retained(accepted_bytes);
            drop(walked);
            release_retained(walked_bytes);
            drop(contributing);
            release_retained(contributing_bytes);
            drop(short);
            release_retained(short_bytes);
            return Ok(UnionOutcome::Unresolved(
                HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
            ));
        }
        if let Some(ledger) = row_check {
            if let Some(error) = ledger.borrow_mut().error.take() {
                drop(accepted);
                release_retained(accepted_bytes);
                drop(walked);
                release_retained(walked_bytes);
                drop(contributing);
                release_retained(contributing_bytes);
                drop(short);
                release_retained(short_bytes);
                return Err(error);
            }
        }
        if accepted.len() < required {
            drop(accepted);
            release_retained(accepted_bytes);
            drop(walked);
            release_retained(walked_bytes);
            drop(contributing);
            release_retained(contributing_bytes);
            drop(short);
            release_retained(short_bytes);
            return Ok(UnionOutcome::Unresolved(
                HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
            ));
        }
        // Tail candidates compete in the same resolved statement pool. This
        // removes excess actual retained records, not source/work observations.
        sort_vector_scores(&mut walked);
        walked.truncate(candidate_limit);
        Ok(UnionOutcome::Answered {
            accepted,
            accepted_bytes,
            walked,
            walked_bytes,
            contributing,
            contributing_bytes,
            short,
            short_bytes,
        })
    }

    /// Narrow through the captured reverse and forward membership at this snapshot.
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
                        state: state.into(),
                        serving_selection: None,
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
            self.store.visit_query_partition_sources(index, owner, |key, _, _| {
                if selected(key) { count = count.checked_add(1).ok_or_else(|| Error::Other("vector source capacity overflow".into()))?; }
                Ok::<(), Error>(())
            })?;
            owner.reserve(count.checked_mul(std::mem::size_of::<BoundedSnapshotVectorSource>())
                .ok_or_else(|| Error::Other("vector source capacity overflow".into()))?)?;
            let mut result = Vec::with_capacity(count);
            self.store.visit_query_partition_sources(index, owner, |key, state, owner| {
                if selected(key) {
                    if result.len() == count { return Err(Error::Other("vector source selection changed after its admitted count".into())); }
                    owner.reserve(key.estimated_bytes())?;
                    result.push(BoundedSnapshotVectorSource { partition_key: key.clone(), state,
                        serving_selection: None, candidate_ids: None, point_load: false,
                        candidate_ids_cover_all_live: false, allowed_count: 0, live_count: 0 });
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
    ) -> Result<(SearchScores, usize)> {
        let mut scored = SearchScores::new(index, owner.accountant().clone())?.for_exact();
        // Grow only for rows this pass actually authenticates. A historical
        // snapshot needs no preliminary membership-count pass.
        let capacity = k;
        // Evaluate every eligible vector, retaining only the requested best
        // scores. Ranking callers request their complete candidate set here.
        let mut consider = |score: (RowId, f32)| -> Result<()> {
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
            } else if vector_score_quality(&score, &scored[0]) == std::cmp::Ordering::Greater {
                replace_bounded_score_heap_root(&mut scored.rows, score)?;
            }
            Ok(())
        };
        let mut partition_count = 0usize;
        for source in sources {
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
                    source_consider((entry.row_id, entry.vector.cosine_similarity(query)))?;
                }
                Ok::<(), Error>(())
            })?;
            partition_count += usize::from(source_has_rows);
        }
        sort_vector_scores(&mut scored);
        scored.truncate(k);
        Ok((scored, partition_count))
    }

    /// Compare every row one partition's snapshot shows by reading each
    /// row's own stored vector through a point read, admitting each body
    /// against the database's memory before it is read and returning it
    /// straight after, so the comparison never needs room for the partition.
    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    fn stream_source_bodies(
        &self,
        index: &VectorIndexRef,
        source: &BoundedSnapshotVectorSource,
        query: &[f32],
        snapshot: SnapshotId,
        candidates: Option<&RoaringTreemap>,
        consider: &mut impl FnMut((RowId, f32)) -> Result<()>,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<()> {
        let accountant = owner.accountant().clone();
        let mut before_retain = |bytes: usize| -> Result<()> {
            accountant.try_allocate_for(
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
        let mut session = crate::store::OnePassSession::default();
        let _one_pass = if durable.is_none() {
            source.state.one_pass_reads(&mut session)
        } else {
            None
        };
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
            position = position
                .checked_add(1)
                .ok_or_else(|| Error::Other(("exhaustive vector position overflow".to_string()).into()))?;
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
                let states = crate::ScopedVectorCopy::new(metadata.as_mut().unwrap_or(owner), |copies| {
                    let capacity = self.store.query_partition_source_count(index, copies)?;
                    copies.reserve(capacity.checked_mul(std::mem::size_of::<crate::store::VectorQueryState>())
                        .ok_or_else(|| Error::Other("vector source capacity overflow".into()))?)?;
                    let mut states = Vec::with_capacity(capacity);
                    self.store.visit_query_partition_sources(index, copies, |key, state, _| {
                        if selected(key) {
                            if states.len() == capacity { return Err(Error::Other("vector source count changed after admission".into())); }
                            states.push(state);
                        }
                        Ok::<(), Error>(())
                    })?;
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
        owner: &mut crate::VectorCopyAdmission,
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
            owner,
        )?;
        let result =
            self.bounded_rank_candidate_k_grouped(index, limit, &groups, before_source, owner);
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
        owner: &mut crate::VectorCopyAdmission,
    ) -> std::result::Result<usize, E>
    where
        E: From<Error>,
    {
        before_source()?;
        let layout = self
            .store
            .scoped_index_layout(index, owner)
            .map_err(E::from)?;
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
                            owner,
                        )?,
                        limit,
                    )
                    .hnsw_ef_search,
            );
        }
        Ok(breadth.min(allowed).max(limit))
    }

    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    pub fn search_with_mode(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: contextdb_core::VectorSearchMode,
        executing_owner: Arc<dyn MemoryBudget>,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        self.search_with_mode_owned(index, query, k, candidates, snapshot, mode, executing_owner)
    }

    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    pub fn search_with_mode_owned(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        mode: contextdb_core::VectorSearchMode,
        executing_owner: Arc<dyn MemoryBudget>,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
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
    pub fn search_with_mode_admission(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot_mode: (SnapshotId, contextdb_core::VectorSearchMode),
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        self.search_with_mode_route(
            index,
            query,
            k,
            candidates,
            snapshot_mode,
            (owner.funder(), owner.scratch()),
        )
    }

    fn search_with_mode_route(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot_mode: (SnapshotId, contextdb_core::VectorSearchMode),
        owners: (Arc<dyn MemoryBudget>, crate::VectorCopyAdmission),
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        let (snapshot, mode) = snapshot_mode;
        let (executing_owner, mut route_owner) = owners;
        self.store.with_bulk_read(|| {
            if k == 0 {
                return Ok((
                    SearchScores::new(&index, executing_owner.clone())?,
                    OwnedVectorSearchTrace::brute_force(
                        &index,
                        &[],
                        "empty_limit",
                        None,
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
                    SearchScores::new(&index, executing_owner.clone())?,
                    OwnedVectorSearchTrace::brute_force(
                        &index,
                        &[],
                        "empty_candidates",
                        None,
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
                    SearchScores::new(&index, executing_owner.clone())?,
                    OwnedVectorSearchTrace::brute_force(
                        &index,
                        &[],
                        "empty_index",
                        None,
                        Vec::new(),
                        executing_owner.clone(),
                    )?,
                ));
            }
            let exact = |reason: &'static str,
                         hnsw_len: Option<usize>,
                         owner: &mut crate::VectorCopyAdmission|
             -> Result<(SearchScores, OwnedVectorSearchTrace)> {
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
                    };
                    partition_count
                ];
                let trace = OwnedVectorSearchTrace::brute_force_with_owner(
                    &index,
                    &rows,
                    reason,
                    hnsw_len,
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
                return exact("exact_requested", None, &mut route_owner);
            }
            if !query_has_positive_finite_norm(query) {
                if mode == contextdb_core::VectorSearchMode::Indexed {
                    return Err(Error::VectorIndexedRouteUnavailable {
                        index: index.clone(),
                    });
                }
                return exact("non_positive_query_norm", None, &mut route_owner);
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
                        None,
                        &mut route_owner,
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
            let accountant = executing_owner.clone();
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
                        .map_err(|_| Error::Other(("vector candidate count overflow".to_string()).into()))?;
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
                    &mut route_owner,
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
                    &mut route_owner,
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
                    SearchScores::new(&index, executing_owner.clone())?,
                    OwnedVectorSearchTrace::brute_force(
                        &index,
                        &[],
                        "empty_index",
                        None,
                        Vec::new(),
                        executing_owner.clone(),
                    )?,
                )),
                BoundedHnswOutcome::Complete(BoundedHnswResult {
                    mut rows,
                    trace,
                    retained_bytes,
                }) => {
                    let trace_bytes = trace.retained_bytes();
                    let trace =
                        OwnedVectorSearchTrace::from_charged(trace, executing_owner.clone());
                    rows.truncate(k);
                    let scores = SearchScores::from_charged(
                        rows,
                        &index,
                        executing_owner.clone(),
                        retained_bytes.saturating_sub(trace_bytes),
                    )?;
                    Ok((scores, trace))
                }
                BoundedHnswOutcome::ExactRequired { trace, .. } => {
                    let mut trace =
                        OwnedVectorSearchTrace::from_charged(trace, executing_owner.clone());
                    let (rows, _) = self.brute_force_search_sources(
                        &index,
                        &sources,
                        query,
                        k,
                        candidates,
                        snapshot,
                        mode == contextdb_core::VectorSearchMode::Auto,
                        &mut route_owner,
                    )?;
                    trace.replace_final_rows(&rows)?;
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
                    exact("maintained_graph_unavailable", None, &mut route_owner)
                }
                BoundedHnswOutcome::Incomplete { .. } => {
                    if mode == contextdb_core::VectorSearchMode::Indexed {
                        return Err(unavailable(candidates.is_some()));
                    }
                    exact("maintained_graph_incomplete", None, &mut route_owner)
                }
                BoundedHnswOutcome::Disconnected { partition } => {
                    if mode == contextdb_core::VectorSearchMode::Indexed {
                        return Err(Error::VectorIndexedPartitionDisconnected {
                            index: index.clone(),
                            partition: partition as u64,
                        });
                    }
                    exact(GRAPH_FRONTIER_DISCONNECTED_REASON, None, &mut route_owner)
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
        executing_owner: Arc<dyn MemoryBudget>,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
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
    pub fn search_with_strategy_for_test(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        executing_owner: Arc<dyn MemoryBudget>,
    ) -> Result<(SearchScores, OwnedVectorSearchTrace)> {
        self.search_with_strategy(index, query, k, candidates, snapshot, executing_owner)
    }

    /// Compatibility availability probe. Despite its retained name, this is
    /// now an observer: only explicit store maintenance may build a graph.
    pub fn ensure_hnsw_built(&self, index: &VectorIndexRef, snapshot: SnapshotId) -> Result<bool> {
        self.store.with_bulk_read(|| {
            let snapshot_tx = TxId::from_snapshot(snapshot);
            let mut complete = true;
            self.store.with_partition_sources(index, |sources| {
                sources.visit(|_, state| {
                    if state.vector_count() != 0
                        && (state.max_tx() > snapshot_tx || !state.has_complete_hnsw_route())
                    {
                        complete = false;
                    }
                    Ok::<(), Error>(())
                })
            })?;
            Ok(complete)
        })
    }

    pub fn hnsw_eligible_without_build(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        if !self.store.contains_index(index) {
            return Ok(false);
        }
        let mut owner = crate::VectorCopyAdmission::new(self._accountant.clone());
        self.try_hnsw_eligible_without_build(index, candidates, snapshot, &mut owner)
    }

    #[doc(hidden)]
    pub fn try_hnsw_eligible_without_build(
        &self,
        index: &VectorIndexRef,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<bool> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.store.with_bulk_read(|| {
            let sources = self.selected_partition_sources(index, candidates, snapshot, owner)?;
            if Self::aggregate_authorized_source_count(&sources, candidates, snapshot, owner)? == 0
            {
                return Ok(false);
            }
            for source in &sources {
                if source
                    .state
                    .try_directory_visible_entry_count(snapshot, candidates, owner)?
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
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<(bool, bool, bool)> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.store.with_bulk_read(|| {
            let sources = self.selected_partition_sources(index, candidates, snapshot, owner)?;
            let mut base = false;
            let mut change = false;
            let mut tail = false;
            for source in sources {
                if source
                    .state
                    .try_directory_visible_entry_count(snapshot, candidates, owner)?
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
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<(bool, bool, bool)> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.store.with_bulk_read(|| {
            let sources = self.selected_partition_sources(index, None, snapshot, owner)?;
            let mut base = false;
            let mut change = false;
            let mut tail = false;
            for source in sources {
                if source.state.bounded_directory_visible_entry_count(
                    snapshot,
                    candidates,
                    || Ok::<(), Error>(()),
                    owner,
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
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<bool> {
        let mut local_copy_admission = owner.local_scratch();
        let owner = local_copy_admission.as_mut().unwrap_or(owner);
        self.store.with_bulk_read(|| {
            let layout = self.store.scoped_index_layout(index, owner)?;
            let sources = self.selected_partition_sources(index, candidates, snapshot, owner)?;
            let mut nonempty = false;
            for source in sources {
                let allowed = source
                    .state
                    .try_directory_visible_entry_count(snapshot, candidates, owner)?;
                if allowed == 0 {
                    continue;
                }
                nonempty = true;
                let count = source
                    .state
                    .try_directory_visible_entry_count(snapshot, None, owner)?;
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
    ) -> Result<bool> {
        if !self.store.contains_index(index) {
            return Ok(false);
        }
        let mut owner = crate::VectorCopyAdmission::new(self._accountant.clone());
        self.try_hnsw_search_covers_all_without_build(index, k, candidates, snapshot, &mut owner)
    }
}

impl<S: WriteSetApplicator> VectorExecutor for MemVectorExecutor<S> {
    type SearchOwner = Arc<dyn MemoryBudget>;
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
    // Grouping rows by graph identity: per entry, one hash-bucket slot while
    // grouping, one member or group-index entry kept, and one graph-identity
    // key computed at a time (dimension x 4 bytes).
    let graph_identity_grouping = entry_count
        .saturating_mul(crate::hnsw::GRAPH_IDENTITY_ENTRY_BYTES.saturating_mul(2))
        .saturating_add(dimension.saturating_mul(4).saturating_mul(2));
    final_bytes
        .saturating_add(source_snapshot)
        .saturating_add(sorted_entry_refs)
        .saturating_add(cloned_vectors_and_refs)
        .saturating_add(map_and_exact_key_overhead)
        .saturating_add(graph_link_upper_bound)
        .saturating_add(construction_scratch)
        .saturating_add(graph_identity_grouping)
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
    pub fn snapshot(&self) -> SnapshotId { self.snapshot }
    pub fn score(&self) -> f32 { self.score }
    pub fn body_allocation(&self) -> Option<(usize, usize, usize)> { self.body_allocation }
}
#[cfg(feature = "test-seams")]
pub(crate) fn observe_native_score_identity_for_test(row_id: RowId,
    created_tx: contextdb_core::TxId, lsn: contextdb_core::Lsn, snapshot: SnapshotId, score: f32,
    body_allocation: Option<(usize, usize, usize)>) {
    let observer = NATIVE_VECTOR_SCORE_OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.after_native_vector_score(NativeVectorScoreEvent {
            row_id, created_tx, lsn, snapshot, score, body_allocation, _producer: (),
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
            400 * (32 + crate::hnsw::reachability::LAYER0_SLACK),
            "the layer-0 maximum degree of an M = 16 graph is its 32 selected links plus the \
             links reachability owes"
        );
        let walk_nanos = budget as u128 * u128::from(super::AUTO_WALK_EVALUATION_NANOS);
        let crossover = (walk_nanos / u128::from(super::AUTO_EXHAUSTIVE_COMPARISON_NANOS)) as usize;
        assert!(super::auto_statement_prefers_exhaustive(budget, crossover));
        assert!(!super::auto_statement_prefers_exhaustive(
            budget,
            crossover + 1
        ));
    }

    fn resident_partition(allowed: usize, live: usize) -> super::AutoPartitionCost {
        super::AutoPartitionCost {
            allowed,
            live,
            hnsw_m: 16,
            dormant_layers: 0,
            streams_raw_bodies: false,
            flat: false,
        }
    }

    /// A partition served flat costs the walk route its exact comparisons, so
    /// a statement of flat partitions is never priced as a cheaper walk, and a
    /// flat partition beside a graph one adds the same cost to both sides.
    #[test]
    fn a_flat_partition_is_priced_at_its_exact_comparisons() {
        let flat = |allowed| super::AutoPartitionCost {
            flat: true,
            ..resident_partition(allowed, allowed)
        };
        let all_flat = super::auto_statement_cost(
            100,
            K,
            crate::store::AUTO_DEFAULT_EXPANSION_CEILING_PERCENT,
            [flat(150); 3],
        );
        assert!(
            all_flat.prefers_exhaustive,
            "AUTO over flat partitions compares them: {all_flat:?}"
        );
        assert_eq!(all_flat.expected_walk, 0, "a flat partition opens no graph");
        let graph_only = super::auto_statement_cost(
            100,
            K,
            crate::store::AUTO_DEFAULT_EXPANSION_CEILING_PERCENT,
            [resident_partition(1_250, 1_250); 3],
        );
        let mixed = super::auto_statement_cost(
            100,
            K,
            crate::store::AUTO_DEFAULT_EXPANSION_CEILING_PERCENT,
            [
                resident_partition(1_250, 1_250),
                resident_partition(1_250, 1_250),
                resident_partition(1_250, 1_250),
                flat(150),
            ],
        );
        assert_eq!(
            mixed.prefers_exhaustive, graph_only.prefers_exhaustive,
            "a flat partition beside graph ones does not move the decision"
        );
        assert_eq!(mixed.expected_walk, graph_only.expected_walk);
    }

    /// A residual filter makes the walk navigate rows it cannot return. The
    /// frozen multi-Context cell selects three Contexts of 1,250 live rows,
    /// half of them current: there the walk scored 1,866.9 of the 1,875
    /// current rows per query (mean) and answered at 44.7 ms p95 against
    /// EXACT's 22.8 ms, paired and interleaved (debug build, ms-a2,
    /// `indexed_route_does_less_work_than_exact`). AUTO must compare those
    /// rows outright, while the same three partitions with no filter are
    /// still walked.
    #[test]
    fn a_residual_filter_is_priced_into_the_walk_it_widens() {
        let filtered = super::auto_statement_cost(
            100,
            K,
            crate::store::AUTO_DEFAULT_EXPANSION_CEILING_PERCENT,
            [resident_partition(625, 1_250); 3],
        );
        assert!(
            filtered.prefers_exhaustive,
            "a walk through a 50% residual filter over 625 allowed of 1,250 live rows per \
             partition is dearer than comparing the 1,875 allowed rows: {filtered:?}"
        );
        let unfiltered = super::auto_statement_cost(
            100,
            K,
            crate::store::AUTO_DEFAULT_EXPANSION_CEILING_PERCENT,
            [resident_partition(1_250, 1_250); 3],
        );
        assert!(
            !unfiltered.prefers_exhaustive,
            "the same partitions with every live row allowed are still walked: {unfiltered:?}"
        );
    }
}

#[cfg(test)]
mod indexed_row_check_empty_merge_tests {
    use super::*;
    use crate::store::VectorIndexLayout;
    use contextdb_core::read_memory::ReadMemoryBudget;
    use contextdb_core::VectorExecutor;
    use contextdb_tx::WriteSet;

    #[derive(Debug)]
    struct NoLimit;

    impl MemoryBudget for NoLimit {
        fn try_allocate_for(
            &self,
            _bytes: usize,
            _subsystem: &str,
            _operation: &str,
            _hint: &str,
        ) -> Result<()> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    impl ReadMemoryBudget for NoLimit {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> Result<()> {
            MemoryBudget::try_allocate_for(self, bytes, subsystem, operation, hint)
        }

        fn release(&self, bytes: usize) {
            MemoryBudget::release(self, bytes);
        }
    }

    struct TestStore {
        store: Arc<VectorStore>,
    }

    impl WriteSetApplicator for TestStore {
        fn apply(
            &self,
            writes: &WriteSet,
            _read_memory: Arc<dyn ReadMemoryBudget>,
        ) -> Result<()> {
            let owner = Arc::new(NoLimit);
            let admission = crate::VectorCopyAdmission::new(owner);
            let mut bodies = Vec::new();
            for entry in &writes.vector_inserts {
                let mut body = self.store.retained_column_copy_owner(&entry.index, &admission)?;
                body.reserve_for(
                    entry.vector.len().saturating_mul(std::mem::size_of::<f32>()),
                    "test",
                    "caller_vector_body",
                    "test",
                )?;
                bodies.push(body);
            }
            self.store.apply_inserts_with_accountant(
                writes.vector_inserts.clone(),
                None,
                admission,
            )?;
            for body in bodies {
                body.transfer();
            }
            assert!(writes.vector_deletes.is_empty(), "fixture stages inserts only");
            Ok(())
        }

        fn new_row_id(&self) -> RowId {
            RowId(1)
        }
    }

    fn fixture(
        count: usize,
    ) -> (
        MemVectorExecutor<TestStore>,
        VectorIndexRef,
        SnapshotId,
        crate::VectorCopyAdmission,
    ) {
        const M: usize = 8;
        const EF_SEARCH: usize = 1;
        let hnsw = Arc::new(OnceLock::new());
        let store = Arc::new(VectorStore::new(hnsw.clone()));
        let index = VectorIndexRef::new("row_check_empty_merge", "embedding");
        let layout = VectorIndexLayout::unpartitioned(2, VectorQuantization::F32)
            .with_policy(Some(1), Some(M as u32), Some(32), Some(EF_SEARCH as u32), 1)
            .with_flat_below(Some((count + 1) as u32));
        let owner = Arc::new(NoLimit);
        store
            .register_index_with_layout(
                index.clone(),
                layout,
                owner.clone(),
                &mut crate::VectorCopyAdmission::new(owner.clone()),
            )
            .expect("declare the real indexed flat column");
        let transactions = Arc::new(TransactionManager::new(TestStore {
            store: store.clone(),
        }));
        let executor = MemVectorExecutor::new(store, transactions.clone(), hnsw, owner.clone());
        let tx = transactions.begin();
        for row in 0..count {
            executor
                .insert_vector(
                    tx,
                    index.clone(),
                    RowId((row + 1) as u64),
                    vec![1.0, row as f32 + 1.0],
                    owner.clone(),
                )
                .expect("stage a real vector insert");
        }
        transactions
            .commit(tx, owner)
            .expect("commit the staged vector inserts");
        let statement_owner = Arc::new(NoLimit);
        (
            executor,
            index,
            transactions.snapshot(),
            crate::VectorCopyAdmission::new(statement_owner),
        )
    }

    fn grouped(
        executor: &MemVectorExecutor<TestStore>,
        index: &VectorIndexRef,
        snapshot: SnapshotId,
        owner: &mut crate::VectorCopyAdmission,
    ) -> BoundedVectorCandidateGroups {
        let mut groups = executor
            .bounded_candidate_groups::<Error>(
                index,
                None,
                snapshot,
                || Ok(()),
                || Ok(()),
                |_| Ok(()),
                |_| {},
                owner,
            )
            .expect("group the committed visible vectors");
        groups.mark_row_checked();
        groups
    }

    fn rejected_flat_outcome(
        executor: &MemVectorExecutor<TestStore>,
        index: &VectorIndexRef,
        groups: &BoundedVectorCandidateGroups,
        allowance: usize,
        owner: &mut crate::VectorCopyAdmission,
    ) -> BoundedHnswOutcome {
        let mut reject_every_row = |_| Ok::<bool, Error>(false);
        executor
            .bounded_hnsw_search_grouped_with_reader_memory_refusal(
                index,
                &[1.0, 0.0],
                1,
                groups,
                VectorSearchMode::Indexed,
                |_| false,
                HnswAllowedSearchLimits {
                    max_visited_nodes: usize::MAX,
                    max_vector_evaluations: usize::MAX,
                },
                || Ok(()),
                || Ok(()),
                || Ok(()),
                |_| Ok(()),
                |_| {},
                owner,
                Some(RowCheck {
                    allowance,
                    check: &mut reject_every_row,
                }),
            )
            .expect("the direct bounded kernel completes its physical flat scan")
    }

    /// An INDEXED flat scan that reaches the row-check limit before every
    /// rejected row has been classified must refuse; an empty merge is not a
    /// completed search.
    #[test]
    fn indexed_row_check_exhausted_rejected_flat_rows_are_incomplete() {
        let declared = auto_statement_walk_budget(1, 1, 8);
        assert!(declared >= 2, "the production walk budget is positive");
        let (executor, index, snapshot, mut owner) = fixture(declared + 1);
        let groups = grouped(&executor, &index, snapshot, &mut owner);
        let layout = executor.store.scoped_index_layout(&index, &mut owner).unwrap();
        let allowance = groups.row_check_allowance(&layout, 1);
        assert_eq!(allowance, declared, "fixture uses the resolved production allowance");
        let outcome = rejected_flat_outcome(&executor, &index, &groups, allowance, &mut owner);
        assert!(
            matches!(
                outcome,
                BoundedHnswOutcome::Incomplete {
                    reason: HnswAllowedSearchIncomplete::VectorEvaluationLimitReached
                }
            ),
            "an exhausted all-rejected INDEXED flat scan must be incomplete: {outcome:?}"
        );
    }

    /// A false predicate below the same allowance completes its scan and is a
    /// genuine empty result, not a refusal.
    #[test]
    fn indexed_row_check_fewer_than_allowance_rejected_rows_stay_empty() {
        let declared = auto_statement_walk_budget(1, 1, 8);
        assert!(declared >= 2, "the production walk budget is positive");
        let (executor, index, snapshot, mut owner) = fixture(declared - 1);
        let groups = grouped(&executor, &index, snapshot, &mut owner);
        let layout = executor.store.scoped_index_layout(&index, &mut owner).unwrap();
        let allowance = groups.row_check_allowance(&layout, 1);
        assert_eq!(allowance, declared, "control uses the same resolved allowance");
        let outcome = rejected_flat_outcome(&executor, &index, &groups, allowance, &mut owner);
        assert!(
            matches!(outcome, BoundedHnswOutcome::CompleteEmpty),
            "a completed below-allowance all-rejected flat scan is empty: {outcome:?}"
        );
    }
}
