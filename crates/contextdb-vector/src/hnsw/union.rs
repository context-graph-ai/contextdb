//! One best-first search over every admitted layer of every admitted
//! partition.
//!
//! A find over many Contexts asks one question -- the k nearest rows the
//! reader may see -- so it is answered by one search: one frontier, one result
//! heap and one candidate budget, instead of one whole walk per partition
//! whose results are merged afterwards. What a wider scope legitimately adds
//! is ADMISSION. Every non-empty snapshot-compatible layer of every admitted
//! partition is opened, its entry point evaluated and every level, including
//! layer zero, greedily descended before expansion begins, because an entry
//! distance is not a lower bound on what lies behind it: a layer is never
//! skipped for looking far away. Once every layer is admitted, the frontier
//! holds only nodes whose distance has actually been evaluated, and the ordinary stopping rule -- no
//! frontier entry nearer than the worst kept result -- is sound over it.
//!
//! A layer is any of the three a partition's chain holds -- the sealed base,
//! the sealed change beside it and the mutable tail -- on either route: a
//! saved generation the file route reads out of its serving pages, or a graph
//! the owner route holds in memory. The mutable tail is an HNSW graph like the
//! sealed layers, so it joins the walk as one more graph layer rather than
//! being compared exhaustively: its entry is evaluated and descended at
//! admission, and its points are expanded against the same budget, from the
//! same frontier, into the same heap. Every layer scores the same cosine
//! distance, so their points compete directly.
//!
//! Each layer keeps its own filter semantics, exactly as the single-layer walk
//! applies them: its dispersed seed directory, the one-hop bridge through an
//! excluded neighbour where admitted points stop reaching each other, and a
//! second pass that navigates through excluded points when the first is
//! starved. Scratch memory is charged through the caller before it is
//! allocated and sized by the budget, not by the scope.

use super::*;
use hnsw_rs::hnsw::{Point, PointIndexation};
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;

/// One graph layer a union search admits: a generation or mutable tail of one
/// partition, the statement's position for that partition, the rows the
/// partition may admit, and -- for a filtered scope -- the partition's sorted
/// candidate directory.
pub(crate) struct HnswUnionLayer<'a> {
    pub(crate) graph: &'a HnswIndex,
    pub(crate) partition: usize,
    pub(crate) allowed_count: usize,
    /// The logical rows the partition's snapshot sees, however many graph
    /// points hold them: a row whose superseded version still sits in a
    /// sealed layer beside its current one is one row here.
    pub(crate) live_count: usize,
    pub(crate) seed_rows: Option<&'a [u64]>,
}

/// One row a union search hands back, tagged with the partition and layer
/// that produced it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HnswUnionCandidate {
    pub(crate) partition: usize,
    pub(crate) layer: usize,
    pub(crate) row_id: RowId,
    /// The distance the walk reached this row at.
    pub(crate) distance: f32,
    /// The visible stored score, when an early-stop proof already read it.
    pub(crate) resolved_score: Option<f32>,
}

#[derive(Clone, Copy)]
struct KeptRowInfo {
    distance: f32,
    resolved_score: Option<f32>,
}

/// How one union search ended. Exactly one holds, and a short answer is only
/// ever the second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnionTerminal {
    /// At least `k` admitted rows were kept.
    AnswerComplete,
    /// Every admitted row the scope holds was reached and there are fewer than
    /// `k` of them. Established by reaching them all, never inferred from a
    /// spent budget.
    FrontierExhausted,
    /// The budget, a declared limit, or admission alone ran out before either
    /// of the above; or too few admitted rows were reached to establish
    /// either. The caller refuses or, under AUTO, compares the rows instead.
    Refused,
}

impl UnionTerminal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AnswerComplete => "answer_complete",
            Self::FrontierExhausted => "frontier_exhausted",
            Self::Refused => "refused",
        }
    }
}

#[derive(Debug)]
pub(crate) struct HnswUnionSearchResult {
    pub(crate) terminal: UnionTerminal,
    /// Every kept row, nearest first. Rows whose stored vector equals the
    /// query come first, as the single-layer walk's exact tie correction puts
    /// them.
    pub(crate) candidates: Vec<HnswUnionCandidate>,
    pub(crate) retained_bytes: usize,
    pub(crate) status: HnswAllowedSearchStatus,
    /// Opening the layers alone reached the declared limit:
    /// `(admission_evaluations, limit)`.
    pub(crate) below_admission_floor: Option<(usize, usize)>,
    pub(crate) budget: usize,
    pub(crate) admission_evaluations: usize,
    pub(crate) expansion_evaluations: usize,
    /// The most evaluations the walk could spend beyond opening its layers,
    /// as its first pass started.
    #[cfg(feature = "test-seams")]
    pub(crate) expansion_cap: usize,
    pub(crate) layers_admitted: usize,
    /// Every reachable admitted point was evaluated: the frontier ran empty
    /// rather than the walk stopping on its rule or its budget.
    pub(crate) frontier_exhausted: bool,
}

#[derive(Clone, Copy)]
struct Reached {
    distance: f32,
    row_id: RowId,
    layer: u32,
    point: PointId,
    origin: u32,
}

/// One kept result: the point that reached it and the logical row it is.
/// A row can sit in more than one layer of its partition -- a sealed base and
/// the change beside it -- and it is one answer, kept once, at its nearest.
#[derive(Clone, Copy)]
struct Kept {
    reached: Reached,
    partition: usize,
    row_id: RowId,
}

/// Nearest first, for the frontier.
struct Nearest(Reached);

impl PartialEq for Nearest {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}
impl Eq for Nearest {}
impl PartialOrd for Nearest {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for Nearest {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .0
            .distance
            .total_cmp(&self.0.distance)
            .then_with(|| other.0.row_id.cmp(&self.0.row_id))
            .then_with(|| other.0.layer.cmp(&self.0.layer))
            .then_with(|| other.0.origin.cmp(&self.0.origin))
    }
}

/// Farthest first, for the kept results.
struct Farthest(Kept);

impl PartialEq for Farthest {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == CmpOrdering::Equal
    }
}
impl Eq for Farthest {}
impl PartialOrd for Farthest {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for Farthest {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.0
            .reached
            .distance
            .total_cmp(&other.0.reached.distance)
            .then_with(|| self.0.row_id.cmp(&other.0.row_id))
            .then_with(|| self.0.reached.layer.cmp(&other.0.reached.layer))
            .then_with(|| self.0.reached.origin.cmp(&other.0.reached.origin))
    }
}

/// How one admitted layer is read: out of a saved generation's serving pages,
/// or out of a graph held in memory -- a sealed generation on the owner route,
/// or any partition's mutable tail.
enum LayerGraph<'a> {
    Paged {
        paged: &'a PagedHnsw,
        /// This layer's page reader and the walk's own directory of the
        /// pages it has resolved in it, charged before it was allocated.
        reader: ServingPages,
    },
    ResidentF32 {
        points: &'a PointIndexation<'static, f32>,
        distance: &'a HnswF32Distance,
    },
    ResidentQuantized {
        points: &'a PointIndexation<'static, u8>,
        distance: &'a DistQuantizedCosine,
    },
}

/// The query as each kind of layer scores it.
struct UnionQuery<'q> {
    values: &'q [f32],
    /// The query's norm, which every paged distance shares.
    right_norm: f64,
    /// The query encoded the way a quantized layer stores its points, when
    /// the scope has one.
    encoded: Option<&'q [u8]>,
}

/// The neighbours one point keeps at one level: borrowed from a paged
/// topology, which never changes, or copied into the walk's charged scratch
/// out of a graph held in memory, whose lists a concurrent insertion into a
/// mutable tail may be rewriting.
#[derive(Clone, Copy)]
enum Hops<'h> {
    Paged(&'h [PagedNeighbour]),
    Copied(&'h [(PointId, usize)]),
}

impl Hops<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Paged(hops) => hops.len(),
            Self::Copied(hops) => hops.len(),
        }
    }

    /// The neighbour's physical and client identity.
    fn get(&self, position: usize) -> (PointId, usize) {
        match self {
            Self::Paged(hops) => (hops[position].point_id, hops[position].origin_id as usize),
            Self::Copied(hops) => hops[position],
        }
    }
}

/// Where a walk copies one in-memory neighbour list, charged before it grows.
#[derive(Default)]
struct HopScratch {
    ids: Vec<(PointId, usize)>,
    charged: usize,
}

impl HopScratch {
    const ELEMENT_BYTES: usize = std::mem::size_of::<(PointId, usize)>();

    fn fill<'h, T, E, A>(
        &'h mut self,
        point: &Point<'static, T>,
        level: usize,
        acquire: &mut A,
    ) -> std::result::Result<&'h [(PointId, usize)], E>
    where
        T: Clone + Send + Sync,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    {
        let wanted = point.neighbour_count(level);
        if wanted > self.charged {
            acquire(HnswSearchScratchEvent::Reserve {
                container: HnswSearchScratchContainer::Result,
                previous_capacity: self.charged,
                requested_capacity: wanted,
                element_bytes: Self::ELEMENT_BYTES,
            })?;
            self.charged = wanted;
            self.ids.clear();
            self.ids.reserve_exact(wanted);
        }
        self.ids.clear();
        point.copy_neighbour_ids(level, &mut self.ids);
        Ok(&self.ids)
    }

    fn release<R>(self, release: &mut R)
    where
        R: FnMut(HnswSearchScratchEvent),
    {
        let charged = self.charged;
        drop(self.ids);
        if charged != 0 {
            release(HnswSearchScratchEvent::Release {
                container: HnswSearchScratchContainer::Result,
                capacity: charged,
                element_bytes: Self::ELEMENT_BYTES,
            });
        }
    }
}

struct OpenLayer<'a> {
    graph: LayerGraph<'a>,
    id_to_row: parking_lot::RwLockReadGuard<'a, HashMap<usize, RowId>>,
    partition: usize,
    /// This layer's position in the caller's layer list.
    source_layer: usize,
    /// Where this layer's points start in the statement's reached-point bits,
    /// where each of its levels starts within them, and how many points each
    /// level held when the layer was opened. A point a concurrent insertion
    /// appended since lies outside that view and is not walked.
    first_bit: usize,
    level_offsets: Vec<usize>,
    level_lens: Vec<usize>,
    seeds: Vec<PointId>,
}

impl<'a> OpenLayer<'a> {
    fn bit(&self, point: PointId) -> Option<usize> {
        let level = *self.level_offsets.get(point.0 as usize)?;
        let rank = usize::try_from(point.1).ok()?;
        if rank >= *self.level_lens.get(point.0 as usize)? {
            return None;
        }
        Some(self.first_bit + level + rank)
    }

    fn entry(&self) -> Option<PointId> {
        match &self.graph {
            LayerGraph::Paged { paged, .. } => paged.entry_point,
            LayerGraph::ResidentF32 { points, .. } => points.entry_point_id(),
            LayerGraph::ResidentQuantized { points, .. } => points.entry_point_id(),
        }
    }

    /// The client id of one point, or `None` when the layer does not hold it.
    fn origin(&self, point: PointId) -> Option<usize> {
        match &self.graph {
            LayerGraph::Paged { paged, .. } => paged.point(point).map(|point| point.origin_id),
            LayerGraph::ResidentF32 { points, .. } => points
                .resident_point(&point)
                .map(|point| point.get_origin_id()),
            LayerGraph::ResidentQuantized { points, .. } => points
                .resident_point(&point)
                .map(|point| point.get_origin_id()),
        }
    }

    /// The neighbours `point` keeps at `level`, or `None` when the layer does
    /// not hold the point or the level.
    fn neighbours<'h, E, A>(
        &self,
        point: PointId,
        level: usize,
        scratch: &'h mut HopScratch,
        acquire: &mut A,
    ) -> std::result::Result<Option<Hops<'h>>, E>
    where
        'a: 'h,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    {
        match &self.graph {
            LayerGraph::Paged { paged, .. } => {
                let paged: &'a PagedHnsw = paged;
                Ok(paged
                    .point(point)
                    .and_then(|point| point.neighbours.get(level))
                    .map(|hops| Hops::Paged(hops.as_slice())))
            }
            LayerGraph::ResidentF32 { points, .. } => {
                let Some(point) = points.resident_point(&point) else {
                    return Ok(None);
                };
                Ok(Some(Hops::Copied(scratch.fill(&point, level, acquire)?)))
            }
            LayerGraph::ResidentQuantized { points, .. } => {
                let Some(point) = points.resident_point(&point) else {
                    return Ok(None);
                };
                Ok(Some(Hops::Copied(scratch.fill(&point, level, acquire)?)))
            }
        }
    }

    /// Score one point against the query, or `None` when the layer does not
    /// hold it. A paged point is read out of the walk's serving pages; a
    /// point held in memory is scored by its graph's own distance. Both
    /// finish the same cosine walk distance, including a zero-norm query or
    /// stored vector, and report themselves to the same observer.
    #[allow(clippy::too_many_arguments)]
    fn distance<E, S, D, A, R>(
        &mut self,
        point: PointId,
        query: &UnionQuery<'_>,
        before_source_entry: &mut S,
        before_distance: &mut D,
        acquire: &mut A,
        release: &mut R,
    ) -> std::result::Result<Option<f32>, E>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        D: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        match &mut self.graph {
            LayerGraph::Paged { paged, reader } => {
                let Some(stored) = paged.point(point) else {
                    return Ok(None);
                };
                paged
                    .distance(
                        stored,
                        query.values,
                        query.right_norm,
                        before_source_entry,
                        before_distance,
                        acquire,
                        release,
                        reader.as_mut(),
                    )
                    .map(Some)
                    .map_err(map_core_bounded_error)
            }
            LayerGraph::ResidentF32 { points, distance } => {
                let Some(stored) = points.resident_point(&point) else {
                    return Ok(None);
                };
                before_distance()?;
                Ok(Some(distance.eval(query.values, stored.get_v())))
            }
            LayerGraph::ResidentQuantized { points, distance } => {
                let Some(stored) = points.resident_point(&point) else {
                    return Ok(None);
                };
                let encoded = query.encoded.ok_or_else(|| {
                    E::from(Error::Other(
                        "a union search opened a quantized layer without an encoded query"
                            .to_string(),
                    ))
                })?;
                before_distance()?;
                Ok(Some(distance.eval(encoded, stored.get_v())))
            }
        }
    }
}

/// The frontier entries a search is first charged for.
const FRONTIER_FIRST_CHARGE: usize = 256;

enum Stop {
    Limit(HnswAllowedSearchIncomplete),
}

struct Walk {
    reached: Vec<u64>,
    /// One score per physical point touched while admitting layers. `Some`
    /// marks an evaluated point even if the frontier cap later drops it.
    /// Expansion reuses that score at the same traversal step as before.
    admission_scores: Vec<Option<f32>>,
    frontier: BinaryHeap<Nearest>,
    /// The most entries the frontier may ever hold: one per evaluation the
    /// budget allows, and never more than the admitted layers' points.
    frontier_capacity: usize,
    /// The entries the frontier has been charged for so far. It grows on
    /// demand, charged before each growth, so a search pays for the frontier
    /// it actually uses rather than for the most its budget could allow.
    frontier_charged: usize,
    kept: BinaryHeap<Farthest>,
    kept_capacity: usize,
    /// The nearest distance each kept logical row was reached at.
    kept_rows: HashMap<(usize, RowId), KeptRowInfo>,
    /// Rows tied at the heap's worst distance. At an equal frontier score,
    /// all other kept rows are strictly nearer, without scanning the heap.
    worst_ties: usize,
    /// A failed stored-score proof remains failed at this frontier distance
    /// until a graph-nearer row enters the retained set.
    failed_resolved_stop_distance: Option<f32>,
    #[cfg(test)]
    stop_distance_comparisons: std::cell::Cell<usize>,
    evaluations: usize,
    visited: usize,
    admissions: usize,
    evaluation_cap: usize,
    visit_cap: usize,
    /// Where in-memory layers' neighbour lists are copied: one for the point
    /// being expanded and one for the excluded neighbour it bridges through.
    hops: HopScratch,
    bridges: HopScratch,
}

impl Walk {
    fn mark(&mut self, bit: usize) -> bool {
        let word = bit / 64;
        let mask = 1u64 << (bit % 64);
        match self.reached.get_mut(word) {
            Some(value) if *value & mask == 0 => {
                *value |= mask;
                true
            }
            _ => false,
        }
    }

    fn keep(&mut self, kept: Kept) {
        self.admissions += 1;
        let key = (kept.partition, kept.row_id);
        let distance = kept.reached.distance;
        let previous_worst = self.kept.peek().map(|entry| entry.0.reached.distance);
        let mut removed_worst = 0usize;
        let mut added_worst = 0usize;
        let existing = self.kept_rows.get(&key).copied();
        if let Some(existing) = existing {
            if distance >= existing.distance {
                return;
            }
            // The same row, reached nearer through another layer: it replaces
            // its own farther copy rather than taking a second place.
            let mut entries = std::mem::take(&mut self.kept).into_vec();
            entries.retain(|entry| (entry.0.partition, entry.0.row_id) != key);
            self.kept = BinaryHeap::from(entries);
            self.kept_rows.remove(&key);
            if previous_worst
                .is_some_and(|worst| existing.distance.total_cmp(&worst) == CmpOrdering::Equal)
            {
                removed_worst += 1;
            }
        }
        if self
            .failed_resolved_stop_distance
            .is_some_and(|frontier| distance.total_cmp(&frontier) == CmpOrdering::Less)
        {
            self.failed_resolved_stop_distance = None;
        }
        if self.kept.len() < self.kept_capacity {
            self.kept_rows.insert(
                key,
                KeptRowInfo {
                    distance,
                    resolved_score: existing.and_then(|entry| entry.resolved_score),
                },
            );
            self.kept.push(Farthest(kept));
            if previous_worst.is_some_and(|worst| distance.total_cmp(&worst) == CmpOrdering::Equal)
            {
                added_worst += 1;
            }
        } else if self.kept.peek().is_some_and(|worst| {
            distance
                .total_cmp(&worst.0.reached.distance)
                .then_with(|| kept.row_id.cmp(&worst.0.row_id))
                == CmpOrdering::Less
        }) {
            if let Some(Farthest(evicted)) = self.kept.pop() {
                self.kept_rows.remove(&(evicted.partition, evicted.row_id));
                if previous_worst.is_some_and(|worst| {
                    evicted.reached.distance.total_cmp(&worst) == CmpOrdering::Equal
                }) {
                    removed_worst += 1;
                }
            }
            self.kept_rows.insert(
                key,
                KeptRowInfo {
                    distance,
                    resolved_score: existing.and_then(|entry| entry.resolved_score),
                },
            );
            self.kept.push(Farthest(kept));
            if previous_worst.is_some_and(|worst| distance.total_cmp(&worst) == CmpOrdering::Equal)
            {
                added_worst += 1;
            }
        } else {
            return;
        }
        match (previous_worst, self.kept.peek()) {
            (Some(previous), Some(current))
                if previous.total_cmp(&current.0.reached.distance) == CmpOrdering::Equal =>
            {
                self.worst_ties = self.worst_ties - removed_worst + added_worst;
            }
            (_, Some(current)) => {
                let worst = current.0.reached.distance;
                self.worst_ties = self
                    .kept
                    .iter()
                    .filter(|entry| {
                        entry.0.reached.distance.total_cmp(&worst) == CmpOrdering::Equal
                    })
                    .count();
            }
            (_, None) => self.worst_ties = 0,
        }
    }

    fn push_frontier<E, A>(
        &mut self,
        reached: Reached,
        acquire: &mut A,
    ) -> std::result::Result<(), E>
    where
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    {
        if self.frontier.len() >= self.frontier_capacity {
            return Ok(());
        }
        if self.frontier.len() >= self.frontier_charged {
            let grown = self
                .frontier_charged
                .saturating_mul(2)
                .max(FRONTIER_FIRST_CHARGE)
                .min(self.frontier_capacity);
            acquire(HnswSearchScratchEvent::Reserve {
                container: HnswSearchScratchContainer::CandidateHeap,
                previous_capacity: self.frontier_charged,
                requested_capacity: grown,
                element_bytes: std::mem::size_of::<Nearest>(),
            })?;
            self.frontier_charged = grown;
            self.frontier
                .reserve_exact(grown.saturating_sub(self.frontier.len()));
        }
        self.frontier.push(Nearest(reached));
        Ok(())
    }

    fn frontier_is_farther_than_kept(&self, candidate: Reached, required_results: usize) -> bool {
        // A frontier row's ID says nothing about the IDs of its undiscovered
        // neighbours. Equal-distance rows must be expanded until the tie is
        // exhausted, unless enough already-kept rows are strictly nearer
        // than that whole tied plateau.
        self.kept.peek().is_some_and(|worst| {
            match self.compare_stop_distance(candidate.distance, worst.0.reached.distance) {
                CmpOrdering::Greater => true,
                CmpOrdering::Equal => self.kept.len() - self.worst_ties >= required_results,
                CmpOrdering::Less => false,
            }
        })
    }

    fn compare_stop_distance(&self, left: f32, right: f32) -> CmpOrdering {
        #[cfg(test)]
        self.stop_distance_comparisons
            .set(self.stop_distance_comparisons.get() + 1);
        left.total_cmp(&right)
    }

    fn charge(&mut self) -> std::result::Result<(), Stop> {
        if self.visited >= self.visit_cap {
            return Err(Stop::Limit(HnswAllowedSearchIncomplete::VisitLimitReached));
        }
        if self.evaluations >= self.evaluation_cap {
            return Err(Stop::Limit(
                HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
            ));
        }
        Ok(())
    }
}

impl HnswIndex {
    /// The most neighbours a point of this generation keeps on its bottom
    /// layer, read from the graph itself: a walk evaluates at most this many
    /// neighbours of each point it expands there. It is baked into the graph
    /// when it is built, not chosen per query.
    pub(crate) fn layer0_max_degree(&self) -> usize {
        match &self.hnsw {
            HnswInner::F32Mutable(graph) => graph.get_max_nb_connection_layer0(),
            HnswInner::F32Sealed(graph) => graph.get_max_nb_connection_layer0(),
            HnswInner::QuantizedMutable(graph) => graph.get_max_nb_connection_layer0(),
            HnswInner::QuantizedSealed(graph) => graph.get_max_nb_connection_layer0(),
            HnswInner::F32Paged(graph) => graph.layer0_max_degree,
        }
    }
}

/// The candidate budget one statement spends: `cap(ef, k) x M_max0_serving`,
/// the resolved breadth's cap times the widest admitted serving graph's
/// layer-0 maximum degree -- the most neighbours a walk evaluates per point it
/// expands, read from the graphs, never a scaled declared degree. It is one
/// number for the whole search and never multiplied by the number of
/// partitions or layers it admits.
pub(crate) fn union_candidate_budget(
    ef_search: usize,
    k: usize,
    serving_layer0_degree: usize,
) -> usize {
    hnsw_search_candidate_cap(ef_search, k).saturating_mul(serving_layer0_degree.max(1))
}

/// A layer's walk view: how it is read, its levels' sizes and its degree.
fn layer_graph<'a, E, A>(
    graph: &'a HnswIndex,
    acquire: &mut A,
    directory_entries: &mut usize,
) -> std::result::Result<Option<(LayerGraph<'a>, Vec<usize>, usize)>, E>
where
    E: From<Error>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
{
    fn resident_levels<T: Clone + Send + Sync>(points: &PointIndexation<'static, T>) -> Vec<usize> {
        (0..=usize::from(points.get_max_level_observed()))
            .map(|level| points.get_layer_nb_point(level))
            .collect()
    }
    Ok(Some(match &graph.hnsw {
        HnswInner::F32Paged(paged) => {
            if paged.entry_point.is_none() {
                return Ok(None);
            }
            acquire(HnswSearchScratchEvent::Reserve {
                container: HnswSearchScratchContainer::ServingPageDirectory,
                previous_capacity: 0,
                requested_capacity: paged.page_slots,
                element_bytes: SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
            })?;
            *directory_entries = directory_entries.saturating_add(paged.page_slots);
            let reader =
                ServingPages::new(paged.source.reader().map_err(E::from)?, paged.page_slots);
            let levels = paged.points.iter().map(Vec::len).collect();
            (
                LayerGraph::Paged { paged, reader },
                levels,
                paged.max_nb_connection,
            )
        }
        HnswInner::F32Mutable(resident) => (
            LayerGraph::ResidentF32 {
                points: resident.get_point_indexation(),
                distance: resident.get_distance(),
            },
            resident_levels(resident.get_point_indexation()),
            resident.get_max_nb_connection(),
        ),
        HnswInner::F32Sealed(resident) => (
            LayerGraph::ResidentF32 {
                points: resident.get_point_indexation(),
                distance: resident.get_distance(),
            },
            resident_levels(resident.get_point_indexation()),
            resident.get_max_nb_connection(),
        ),
        HnswInner::QuantizedMutable(resident) => (
            LayerGraph::ResidentQuantized {
                points: resident.get_point_indexation(),
                distance: resident.get_distance(),
            },
            resident_levels(resident.get_point_indexation()),
            resident.get_max_nb_connection(),
        ),
        HnswInner::QuantizedSealed(resident) => (
            LayerGraph::ResidentQuantized {
                points: resident.get_point_indexation(),
                distance: resident.get_distance(),
            },
            resident_levels(resident.get_point_indexation()),
            resident.get_max_nb_connection(),
        ),
    }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn search_union_allowed_bounded<E, S, D, A, R, F, V>(
    layers: &[HnswUnionLayer<'_>],
    query: &[f32],
    k: usize,
    ef_search: usize,
    budget: usize,
    limits: HnswAllowedSearchLimits,
    graph_scores_are_current: bool,
    can_certify_resolved_scores: bool,
    query_self_scores: &mut BoundedHnswQuerySelfScores<'_>,
    mut is_allowed: F,
    mut resolve_score: V,
    mut before_source_entry: S,
    mut before_distance: D,
    mut acquire: A,
    mut release: R,
) -> std::result::Result<HnswUnionSearchResult, E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    F: FnMut(usize, RowId) -> bool,
    V: FnMut(usize, RowId, &mut S, &mut D) -> std::result::Result<Option<f32>, E>,
{
    let mut total_allowed = 0usize;
    let mut total_live = 0usize;
    let mut counted_partitions = Vec::<usize>::new();
    for layer in layers {
        if !counted_partitions.contains(&layer.partition) {
            counted_partitions.push(layer.partition);
            total_allowed = total_allowed.saturating_add(layer.allowed_count);
            total_live = total_live.saturating_add(layer.live_count.max(layer.allowed_count));
        }
    }
    let required_results = k.min(total_allowed);
    let ef = hnsw_search_ef(ef_search, required_results);
    let empty = |status| HnswUnionSearchResult {
        terminal: if status == HnswAllowedSearchStatus::Complete {
            UnionTerminal::FrontierExhausted
        } else {
            UnionTerminal::Refused
        },
        candidates: Vec::new(),
        retained_bytes: 0,
        status,
        below_admission_floor: None,
        budget,
        admission_evaluations: 0,
        expansion_evaluations: 0,
        #[cfg(feature = "test-seams")]
        expansion_cap: 0,
        layers_admitted: 0,
        frontier_exhausted: true,
    };
    if required_results == 0 {
        return Ok(empty(HnswAllowedSearchStatus::Complete));
    }

    // Open every layer: how it is read, its identity map, its seed directory.
    let mut open = Vec::<OpenLayer<'_>>::with_capacity(layers.len());
    let mut total_points = 0usize;
    let mut max_degree = 0usize;
    let mut seed_bytes = 0usize;
    // Directory entries charged for the open paged layers' own page
    // directories, given back once those directories are dropped.
    let mut directory_entries = 0usize;
    // The query encoded for quantized layers, charged when the first one
    // opens and given back once the walk is done with it.
    let mut encoded_query = None::<Vec<u8>>;
    let opened = (|| -> std::result::Result<(), E> {
        for (source_layer, layer) in layers.iter().enumerate() {
            before_source_entry()?;
            if layer.graph.is_empty() {
                continue;
            }
            let partition = layer.partition;
            let mut layer_allowed = |row: RowId| is_allowed(partition, row);
            let seed_all = layer.seed_rows.is_none();
            let seed_rows = layer.seed_rows.unwrap_or(&[]);
            if matches!(layer.graph.hnsw, HnswInner::F32Paged(ref paged) if paged.entry_point.is_none())
            {
                continue;
            }
            let directory = layer.graph.allowed_seed_directory(
                k,
                required_results.min(layer.allowed_count),
                layer.allowed_count,
                ef,
                &|| seed_rows.iter().copied(),
                seed_all,
                &mut layer_allowed,
                &mut before_source_entry,
                &mut acquire,
                &mut release,
            )?;
            seed_bytes = seed_bytes.saturating_add(
                directory
                    .points
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PointId>()),
            );
            // A candidate directory with no point in this layer proves the
            // layer contributes nothing; walking excluded points cannot find a
            // missing identity.
            if !seed_all && directory.supplied_seed_rows && directory.points.is_empty() {
                continue;
            }
            let Some((graph, level_lens, degree)) =
                layer_graph(layer.graph, &mut acquire, &mut directory_entries)?
            else {
                continue;
            };
            if matches!(graph, LayerGraph::ResidentQuantized { .. }) && encoded_query.is_none() {
                let mut scratch = VectorSearchScratch {
                    acquire: &mut acquire,
                    release: &mut release,
                };
                encoded_query = Some(encode_bounded_quantized_query(
                    query,
                    layer.graph.quantization,
                    &mut scratch,
                )?);
            }
            let mut level_offsets = Vec::with_capacity(level_lens.len());
            let mut level_start = 0usize;
            for level_len in &level_lens {
                level_offsets.push(level_start);
                level_start = level_start.saturating_add(*level_len);
            }
            max_degree = max_degree.max(degree);
            open.push(OpenLayer {
                graph,
                id_to_row: layer.graph.id_to_row.read(),
                partition,
                source_layer,
                first_bit: total_points,
                level_offsets,
                level_lens,
                seeds: directory.points,
            });
            total_points = total_points.saturating_add(level_start);
        }
        Ok(())
    })();
    let release_seeds = |release: &mut R, bytes: usize| {
        if bytes != 0 {
            release(HnswSearchScratchEvent::Release {
                container: HnswSearchScratchContainer::Result,
                capacity: bytes / std::mem::size_of::<PointId>().max(1),
                element_bytes: std::mem::size_of::<PointId>(),
            });
        }
    };
    let release_directories = |release: &mut R, entries: usize| {
        if entries != 0 {
            release(HnswSearchScratchEvent::Release {
                container: HnswSearchScratchContainer::ServingPageDirectory,
                capacity: entries,
                element_bytes: SERVING_PAGE_DIRECTORY_ENTRY_BYTES,
            });
        }
    };
    let release_encoded = |acquire: &mut A, release: &mut R, encoded: Option<Vec<u8>>| {
        if let Some(encoded) = encoded {
            VectorSearchScratch { acquire, release }
                .free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
        }
    };
    if let Err(error) = opened {
        drop(open);
        release_encoded(&mut acquire, &mut release, encoded_query);
        release_directories(&mut release, directory_entries);
        release_seeds(&mut release, seed_bytes);
        return Err(error);
    }
    if open.is_empty() {
        release_encoded(&mut acquire, &mut release, encoded_query);
        release_directories(&mut release, directory_entries);
        release_seeds(&mut release, seed_bytes);
        return Ok(empty(HnswAllowedSearchStatus::Incomplete(
            HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
        )));
    }

    // Opening the layers is a floor on top of the one budget, bounded only by
    // the declared limit; the walk after it may spend the whole budget. The
    // cap is set again once the layers are open.
    let evaluation_cap = limits.max_vector_evaluations;
    // Every frontier entry is one evaluation, so the frontier never outgrows
    // what the budget can pay for, however many partitions were admitted.
    let frontier_capacity = total_points
        .min(budget.min(evaluation_cap).saturating_add(1))
        .max(1);
    let kept_capacity = ef.max(required_results).min(total_points).max(1);
    let reached_words = total_points.div_ceil(64).max(1);
    let reservations = [
        (
            HnswSearchScratchContainer::ReturnHeap,
            kept_capacity,
            std::mem::size_of::<Farthest>(),
        ),
        (
            HnswSearchScratchContainer::VisitedPoints,
            reached_words,
            std::mem::size_of::<u64>(),
        ),
        (
            HnswSearchScratchContainer::VisitedPoints,
            total_points,
            std::mem::size_of::<Option<f32>>(),
        ),
        // One map entry per kept row, with the table's own control bytes and
        // its load-factor headroom.
        (
            HnswSearchScratchContainer::SeenRows,
            kept_capacity
                .saturating_mul(8)
                .div_ceil(7)
                .next_power_of_two(),
            std::mem::size_of::<((usize, RowId), KeptRowInfo)>() + 1,
        ),
    ];
    for (reserved, (container, requested_capacity, element_bytes)) in
        reservations.into_iter().enumerate()
    {
        if let Err(error) = acquire(HnswSearchScratchEvent::Reserve {
            container,
            previous_capacity: 0,
            requested_capacity,
            element_bytes,
        }) {
            for (container, capacity, element_bytes) in &reservations[..reserved] {
                release(HnswSearchScratchEvent::Release {
                    container: *container,
                    capacity: *capacity,
                    element_bytes: *element_bytes,
                });
            }
            drop(open);
            release_encoded(&mut acquire, &mut release, encoded_query);
            release_directories(&mut release, directory_entries);
            release_seeds(&mut release, seed_bytes);
            return Err(error);
        }
    }
    let mut walk = Walk {
        reached: vec![0u64; reached_words],
        admission_scores: vec![None; total_points],
        frontier: BinaryHeap::new(),
        frontier_capacity,
        frontier_charged: 0,
        kept: BinaryHeap::with_capacity(kept_capacity),
        kept_capacity,
        kept_rows: HashMap::with_capacity(kept_capacity),
        worst_ties: 0,
        failed_resolved_stop_distance: None,
        #[cfg(test)]
        stop_distance_comparisons: std::cell::Cell::new(0),
        evaluations: 0,
        visited: 0,
        admissions: 0,
        evaluation_cap,
        visit_cap: limits.max_visited_nodes,
        hops: HopScratch::default(),
        bridges: HopScratch::default(),
    };
    let union_query = UnionQuery {
        values: query,
        right_norm: PagedHnsw::query_norm(query),
        encoded: encoded_query.as_deref(),
    };

    let searched = (|| -> std::result::Result<HnswUnionSearchResult, E> {
        let mut admission_evaluations = 0usize;
        let mut expansion_evaluations = 0usize;
        #[cfg(feature = "test-seams")]
        let mut expansion_cap = None;
        let mut layers_admitted = 0usize;
        let mut below_floor = None;
        let mut frontier_exhausted = false;
        let mut outcome = None;
        for allow_bridges in [false, true] {
            if allow_bridges {
                // The admitted rows did not reach each other. Walk again,
                // navigating through excluded points, inside what is left of
                // the same budget.
                walk.reached.iter_mut().for_each(|word| *word = 0);
                walk.frontier.clear();
                walk.kept.clear();
                walk.kept_rows.clear();
                walk.worst_ties = 0;
                walk.failed_resolved_stop_distance = None;
                walk.admissions = 0;
            }
            let before_admission = walk.evaluations;
            let admitted = admit_layers(
                &mut open,
                &mut walk,
                &union_query,
                allow_bridges,
                &mut is_allowed,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            )?;
            admission_evaluations += walk.evaluations - before_admission;
            layers_admitted = layers_admitted.max(admitted.layers);
            if let Some(Stop::Limit(reason)) = admitted.stop {
                // Opening every layer is the floor of this search. Whichever
                // declared limit stopped it there, the search did not fit: it
                // is reported as such, never answered from the layers it did
                // open.
                below_floor = Some((walk.evaluations, walk.evaluation_cap.min(walk.visit_cap)));
                outcome = Some(HnswAllowedSearchStatus::Incomplete(reason));
                break;
            }
            let before_expansion = walk.evaluations;
            // The walk spends what is left of the one budget -- all of it on
            // the first pass, whatever opening the layers cost -- and never
            // past the declared limit.
            walk.evaluation_cap = before_expansion
                .saturating_add(budget.saturating_sub(expansion_evaluations))
                .min(limits.max_vector_evaluations);
            #[cfg(feature = "test-seams")]
            expansion_cap.get_or_insert(walk.evaluation_cap.saturating_sub(before_expansion));
            let expanded = expand(
                &mut open,
                &mut walk,
                &union_query,
                total_allowed,
                required_results,
                graph_scores_are_current,
                can_certify_resolved_scores,
                allow_bridges,
                &mut is_allowed,
                &mut resolve_score,
                &mut before_source_entry,
                &mut before_distance,
                &mut acquire,
                &mut release,
            )?;
            expansion_evaluations += walk.evaluations - before_expansion;
            if let Some(Stop::Limit(reason)) = expanded.stop {
                outcome = Some(HnswAllowedSearchStatus::Incomplete(reason));
                break;
            }
            frontier_exhausted = !expanded.converged;
            let fragmented = total_allowed.saturating_mul(max_degree.max(1)) < total_points;
            // The second pass is for rows a filter rejected, counted in
            // logical rows: a superseded version of a row the snapshot sees is
            // that same row reached twice, not a rejection, however many graph
            // points the layers hold for it.
            let starved = !allow_bridges
                && walk.admissions < total_allowed
                && total_allowed < total_live
                && (!expanded.converged || fragmented);
            if walk.kept.len() < required_results || starved {
                outcome = Some(HnswAllowedSearchStatus::Incomplete(
                    HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
                ));
                if !allow_bridges {
                    continue;
                }
            } else {
                outcome = Some(HnswAllowedSearchStatus::Complete);
            }
            break;
        }
        #[cfg(feature = "test-seams")]
        let expansion_cap = expansion_cap.unwrap_or(0);
        let status = outcome.unwrap_or(HnswAllowedSearchStatus::Complete);
        let terminal = match status {
            HnswAllowedSearchStatus::Incomplete(_) => UnionTerminal::Refused,
            HnswAllowedSearchStatus::Complete if walk.kept.len() >= k => {
                UnionTerminal::AnswerComplete
            }
            HnswAllowedSearchStatus::Complete if walk.kept.len() >= total_allowed => {
                UnionTerminal::FrontierExhausted
            }
            HnswAllowedSearchStatus::Complete => UnionTerminal::Refused,
        };

        // The kept results, nearest first, preceded by any row whose stored
        // vector is exactly the query: the walk's float distance cannot order
        // those ties, so the single-layer walk puts them first and so does
        // this. Which stored vectors are exactly the query is decided the
        // way the single-layer walk decides it: by the query's own score in
        // this column's representation, taken once for the statement, and the
        // key that representation files equal vectors under.
        let mut ordered = std::mem::take(&mut walk.kept).into_vec();
        ordered.sort_unstable_by(|left, right| {
            left.0
                .reached
                .distance
                .total_cmp(&right.0.reached.distance)
                .then_with(|| left.0.row_id.cmp(&right.0.row_id))
                .then_with(|| left.0.reached.layer.cmp(&right.0.reached.layer))
                .then_with(|| left.0.reached.origin.cmp(&right.0.reached.origin))
        });
        // Every kept row is handed back, nearest first, with the distance the
        // walk reached it at. The caller re-scores them from their stored
        // vectors in this order and stops once the nearest `k` it accepted
        // cannot be beaten by the next one's walk distance -- normally after
        // exactly `k` -- so a row the walk reached through a superseded point
        // gives its place to the next row instead of leaving the page short
        // or wrong.
        let quantization = layers
            .first()
            .map_or(VectorQuantization::F32, |layer| layer.graph.quantization);
        let mut scratch = VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        };
        let exact_key = if matches!(quantization, VectorQuantization::F32) {
            Some(bounded_f32_exact_key(query, &mut scratch)?)
        } else {
            None
        };
        let key = exact_key.as_deref().or(union_query.encoded);
        // The query's own score is taken only when some layer files a vector
        // equal to the query, so a search with no such row pays nothing for it.
        let equal_vector_filed = key.is_some_and(|key| {
            layers.iter().any(|layer| {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                let fingerprint = hasher.finish().to_le_bytes();
                let lookup_key = if matches!(layer.graph.hnsw, HnswInner::F32Paged(_)) {
                    fingerprint.as_slice()
                } else {
                    key
                };
                layer
                    .graph
                    .exact_rows
                    .read()
                    .get(lookup_key)
                    .is_some_and(|bucket| !bucket.is_empty())
            })
        });
        let exact_score = if equal_vector_filed {
            match query_self_scores.score(
                query,
                quantization,
                union_query.encoded,
                &mut before_distance,
            ) {
                Ok(score) => score,
                Err(error) => {
                    if let Some(exact_key) = exact_key {
                        VectorSearchScratch {
                            acquire: &mut acquire,
                            release: &mut release,
                        }
                        .free_vec(exact_key, HnswSearchScratchContainer::ExactKey);
                    }
                    return Err(error);
                }
            }
        } else {
            None
        };
        let mut scratch = VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        };
        let key = exact_score.and(key);
        let mut ties = match key {
            Some(_) => match scratch.allocate_vec::<(usize, RowId)>(
                HnswSearchScratchContainer::Result,
                kept_capacity,
                "union exact ties",
            ) {
                Ok(ties) => ties,
                Err(error) => {
                    if let Some(exact_key) = exact_key {
                        scratch.free_vec(exact_key, HnswSearchScratchContainer::ExactKey);
                    }
                    return Err(error);
                }
            },
            None => Vec::new(),
        };
        let found = (|| -> std::result::Result<(), E> {
            let Some(key) = key else {
                return Ok(());
            };
            for (position, layer) in layers.iter().enumerate() {
                before_source_entry()?;
                let exact_rows = layer.graph.exact_rows.read();
                if let HnswInner::F32Paged(paged) = &layer.graph.hnsw {
                    paged
                        .visit_exact_rows(
                            key,
                            query,
                            &exact_rows,
                            &layer.graph.row_to_point.read(),
                            kept_capacity,
                            &mut before_source_entry,
                            &mut acquire,
                            &mut release,
                            |row_id, before_source_entry, _, _| {
                                before_source_entry()?;
                                if layer
                                    .seed_rows
                                    .is_some_and(|rows| rows.binary_search(&row_id.0).is_err())
                                    || !is_allowed(layer.partition, row_id)
                                {
                                    return Ok(false);
                                }
                                if ties.len() < kept_capacity {
                                    ties.push((position, row_id));
                                } else if let Some((worst, worst_id)) = ties
                                    .iter()
                                    .enumerate()
                                    .max_by_key(|(_, (_, kept_row))| *kept_row)
                                    .map(|(position, (_, kept_row))| (position, *kept_row))
                                    && row_id < worst_id
                                {
                                    ties[worst] = (position, row_id);
                                }
                                Ok(true)
                            },
                        )
                        .map_err(map_core_bounded_error)?;
                    continue;
                }
                let Some(bucket) = exact_rows.get(key) else {
                    continue;
                };
                for row_id in bucket {
                    before_source_entry()?;
                    if layer
                        .seed_rows
                        .is_some_and(|rows| rows.binary_search(&row_id.0).is_err())
                    {
                        continue;
                    }
                    if is_allowed(layer.partition, *row_id) {
                        if ties.len() < kept_capacity {
                            ties.push((position, *row_id));
                        } else if let Some((worst, worst_id)) = ties
                            .iter()
                            .enumerate()
                            .max_by_key(|(_, (_, kept_row))| *kept_row)
                            .map(|(position, (_, kept_row))| (position, *kept_row))
                            && *row_id < worst_id
                        {
                            ties[worst] = (position, *row_id);
                        }
                    }
                }
            }
            Ok(())
        })();
        let mut scratch = VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        };
        if let Some(exact_key) = exact_key {
            scratch.free_vec(exact_key, HnswSearchScratchContainer::ExactKey);
        }
        if let Err(error) = found {
            scratch.free_vec(ties, HnswSearchScratchContainer::Result);
            return Err(error);
        }
        ties.sort_unstable_by_key(|(_, row_id)| *row_id);
        let capacity = ordered.len().saturating_add(ties.len());
        if let Err(error) = acquire(HnswSearchScratchEvent::Reserve {
            container: HnswSearchScratchContainer::Result,
            previous_capacity: 0,
            requested_capacity: capacity,
            element_bytes: std::mem::size_of::<HnswUnionCandidate>(),
        }) {
            VectorSearchScratch {
                acquire: &mut acquire,
                release: &mut release,
            }
            .free_vec(ties, HnswSearchScratchContainer::Result);
            return Err(error);
        }
        let mut candidates = Vec::with_capacity(capacity);
        // A row equal to the query sits at the distance its own score gives.
        let tie_distance = exact_score.map_or(0.0, |score| 1.0 - score);
        for (position, row_id) in ties.iter().copied() {
            let partition = layers[position].partition;
            candidates.push(HnswUnionCandidate {
                partition,
                layer: position,
                row_id,
                distance: tie_distance,
                resolved_score: walk
                    .kept_rows
                    .get(&(partition, row_id))
                    .and_then(|entry| entry.resolved_score),
            });
        }
        VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        }
        .free_vec(ties, HnswSearchScratchContainer::Result);
        for Farthest(kept) in ordered {
            let open_layer = &open[kept.reached.layer as usize];
            let row_id = kept.row_id;
            if candidates.iter().any(|candidate: &HnswUnionCandidate| {
                candidate.partition == open_layer.partition && candidate.row_id == row_id
            }) {
                continue;
            }
            candidates.push(HnswUnionCandidate {
                partition: open_layer.partition,
                layer: open_layer.source_layer,
                row_id,
                distance: kept.reached.distance,
                resolved_score: walk
                    .kept_rows
                    .get(&(open_layer.partition, row_id))
                    .and_then(|entry| entry.resolved_score),
            });
        }
        Ok(HnswUnionSearchResult {
            terminal,
            retained_bytes: capacity.saturating_mul(std::mem::size_of::<HnswUnionCandidate>()),
            candidates,
            status,
            below_admission_floor: below_floor,
            budget,
            admission_evaluations,
            expansion_evaluations,
            #[cfg(feature = "test-seams")]
            expansion_cap,
            layers_admitted,
            frontier_exhausted,
        })
    })();

    // Each charge goes back only once what it covers has been dropped.
    let frontier_charged = walk.frontier_charged;
    let hops = std::mem::take(&mut walk.hops);
    let bridges = std::mem::take(&mut walk.bridges);
    drop(walk);
    hops.release(&mut release);
    bridges.release(&mut release);
    for (container, capacity, element_bytes) in reservations {
        release(HnswSearchScratchEvent::Release {
            container,
            capacity,
            element_bytes,
        });
    }
    if frontier_charged != 0 {
        release(HnswSearchScratchEvent::Release {
            container: HnswSearchScratchContainer::CandidateHeap,
            capacity: frontier_charged,
            element_bytes: std::mem::size_of::<Nearest>(),
        });
    }
    drop(open);
    release_encoded(&mut acquire, &mut release, encoded_query);
    release_directories(&mut release, directory_entries);
    release_seeds(&mut release, seed_bytes);
    searched
}

struct Admitted {
    layers: usize,
    stop: Option<Stop>,
}

/// Greedy descent may visit the same physical point at several levels. Keep
/// its first distance so neither the rest of admission nor expansion pays
/// for it again. This ledger is separate from `reached`: traversal still
/// admits points at exactly the same step as the original shared walk.
#[allow(clippy::too_many_arguments)]
fn score_admission<E, S, D, A, R>(
    layer: &mut OpenLayer<'_>,
    point: PointId,
    query: &UnionQuery<'_>,
    walk: &mut Walk,
    before_source_entry: &mut S,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<std::result::Result<Option<f32>, Stop>, E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
{
    let Some(bit) = layer.bit(point) else {
        return Ok(Ok(None));
    };
    if let Some(distance) = walk.admission_scores[bit] {
        return Ok(Ok(Some(distance)));
    }
    if let Err(stop) = walk.charge() {
        return Ok(Err(stop));
    }
    let Some(distance) = layer.distance(
        point,
        query,
        before_source_entry,
        before_distance,
        acquire,
        release,
    )?
    else {
        return Ok(Ok(None));
    };
    walk.visited += 1;
    walk.evaluations += 1;
    walk.admission_scores[bit] = Some(distance);
    Ok(Ok(Some(distance)))
}

/// Open every layer: evaluate its entry point, descend through layer zero
/// greedily, and put what it reached -- and its seed directory -- on the one
/// frontier. Every evaluation is charged to the one budget; none is skipped
/// for a layer that looks far away.
#[allow(clippy::too_many_arguments)]
fn admit_layers<E, S, D, A, R, F>(
    open: &mut [OpenLayer<'_>],
    walk: &mut Walk,
    query: &UnionQuery<'_>,
    allow_bridges: bool,
    is_allowed: &mut F,
    before_source_entry: &mut S,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<Admitted, E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    F: FnMut(usize, RowId) -> bool,
{
    // The copies of in-memory neighbour lists live beside the walk while it
    // is borrowed below.
    let mut hops = std::mem::take(&mut walk.hops);
    let admitted = admit_layers_with(
        open,
        walk,
        &mut hops,
        query,
        allow_bridges,
        is_allowed,
        before_source_entry,
        before_distance,
        acquire,
        release,
    );
    walk.hops = hops;
    admitted
}

#[allow(clippy::too_many_arguments)]
fn admit_layers_with<E, S, D, A, R, F>(
    open: &mut [OpenLayer<'_>],
    walk: &mut Walk,
    hops: &mut HopScratch,
    query: &UnionQuery<'_>,
    allow_bridges: bool,
    is_allowed: &mut F,
    before_source_entry: &mut S,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<Admitted, E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    F: FnMut(usize, RowId) -> bool,
{
    let mut layers = 0usize;
    for (position, layer) in open.iter_mut().enumerate() {
        let partition = layer.partition;
        let Some(entry_id) = layer.entry() else {
            continue;
        };
        let admits = |id_to_row: &HashMap<usize, RowId>, origin: usize, is_allowed: &mut F| {
            id_to_row
                .get(&origin)
                .is_some_and(|row| is_allowed(partition, *row))
        };
        let mut pivot_id = entry_id;
        let mut pivot_distance = match score_admission(
            layer,
            entry_id,
            query,
            walk,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )? {
            Ok(Some(distance)) => distance,
            Ok(None) => return Err(E::from(Error::Other("graph entry point".to_string()))),
            Err(stop) => {
                return Ok(Admitted {
                    layers,
                    stop: Some(stop),
                });
            }
        };
        for level in (0..=entry_id.0).rev() {
            loop {
                let mut changed = false;
                let neighbours = layer
                    .neighbours(pivot_id, level as usize, &mut *hops, acquire)?
                    .ok_or_else(|| {
                        E::from(Error::Other("graph admission neighbours".to_string()))
                    })?;
                for hop in 0..neighbours.len() {
                    let (neighbour_id, neighbour_origin) = neighbours.get(hop);
                    if !allow_bridges && !admits(&layer.id_to_row, neighbour_origin, is_allowed) {
                        continue;
                    }
                    let distance = match score_admission(
                        layer,
                        neighbour_id,
                        query,
                        walk,
                        before_source_entry,
                        before_distance,
                        acquire,
                        release,
                    )? {
                        Ok(Some(distance)) => distance,
                        Ok(None) => return Err(E::from(Error::Other("graph edge".to_string()))),
                        Err(stop) => {
                            return Ok(Admitted {
                                layers,
                                stop: Some(stop),
                            });
                        }
                    };
                    if distance < pivot_distance {
                        pivot_id = neighbour_id;
                        pivot_distance = distance;
                        changed = true;
                    }
                }
                if !changed {
                    break;
                }
            }
        }
        let pivot_origin = layer
            .origin(pivot_id)
            .ok_or_else(|| E::from(Error::Other("graph entry point".to_string())))?;
        if let Some(bit) = layer.bit(pivot_id) {
            walk.mark(bit);
        }
        let reached = Reached {
            distance: pivot_distance,
            row_id: layer
                .id_to_row
                .get(&pivot_origin)
                .copied()
                .unwrap_or(RowId(u64::MAX)),
            layer: position as u32,
            point: pivot_id,
            origin: pivot_origin as u32,
        };
        walk.push_frontier(reached, acquire)?;
        if let Some(row_id) = layer.id_to_row.get(&pivot_origin).copied()
            && is_allowed(partition, row_id)
        {
            walk.keep(Kept {
                reached,
                partition,
                row_id,
            });
        }
        let seeds = std::mem::take(&mut layer.seeds);
        let seeded = (|| -> std::result::Result<Option<Stop>, E> {
            for seed in &seeds {
                let Some(bit) = layer.bit(*seed) else {
                    continue;
                };
                if walk
                    .reached
                    .get(bit / 64)
                    .is_some_and(|word| word & (1 << (bit % 64)) != 0)
                {
                    continue;
                }
                let Some(origin) = layer.origin(*seed) else {
                    continue;
                };
                if !admits(&layer.id_to_row, origin, is_allowed) {
                    continue;
                }
                let distance = match score_admission(
                    layer,
                    *seed,
                    query,
                    walk,
                    before_source_entry,
                    before_distance,
                    acquire,
                    release,
                )? {
                    Ok(Some(distance)) => distance,
                    Ok(None) => continue,
                    Err(stop) => return Ok(Some(stop)),
                };
                walk.mark(bit);
                let reached = Reached {
                    distance,
                    row_id: layer
                        .id_to_row
                        .get(&origin)
                        .copied()
                        .unwrap_or(RowId(u64::MAX)),
                    layer: position as u32,
                    point: *seed,
                    origin: origin as u32,
                };
                walk.push_frontier(reached, acquire)?;
                if let Some(row_id) = layer.id_to_row.get(&origin).copied() {
                    walk.keep(Kept {
                        reached,
                        partition,
                        row_id,
                    });
                }
            }
            Ok(None)
        })();
        layer.seeds = seeds;
        if let Some(stop) = seeded? {
            return Ok(Admitted {
                layers,
                stop: Some(stop),
            });
        }
        layers += 1;
    }
    Ok(Admitted { layers, stop: None })
}

struct Expanded {
    converged: bool,
    stop: Option<Stop>,
}

/// Best-first over the one frontier until no frontier entry is nearer than the
/// worst kept result, the frontier is empty, or the budget is spent.
#[allow(clippy::too_many_arguments)]
fn expand<E, S, D, A, R, F, V>(
    open: &mut [OpenLayer<'_>],
    walk: &mut Walk,
    query: &UnionQuery<'_>,
    total_allowed: usize,
    required_results: usize,
    graph_scores_are_current: bool,
    can_certify_resolved_scores: bool,
    allow_bridges: bool,
    is_allowed: &mut F,
    resolve_score: &mut V,
    before_source_entry: &mut S,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<Expanded, E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    F: FnMut(usize, RowId) -> bool,
    V: FnMut(usize, RowId, &mut S, &mut D) -> std::result::Result<Option<f32>, E>,
{
    // The copies of in-memory neighbour lists live beside the walk while it
    // is borrowed below.
    let mut hops = std::mem::take(&mut walk.hops);
    let mut bridges = std::mem::take(&mut walk.bridges);
    let expanded = (|| -> std::result::Result<Expanded, E> {
        while let Some(Nearest(candidate)) = walk.frontier.pop() {
            // Every admitted row is already kept, each once. Admissions are
            // not rows: a row whose superseded version still sits in a graph
            // beside its current one is admitted twice, so counting them
            // would end the walk with rows it never reached.
            if walk.kept.len() >= total_allowed {
                return Ok(Expanded {
                    converged: true,
                    stop: None,
                });
            }
            // Frontier row IDs do not bound the IDs behind them. An equal
            // score can stop only when the requested page is already filled
            // by strictly nearer rows.
            if walk.kept.len() >= walk.kept_capacity
                && walk.frontier_is_farther_than_kept(candidate, required_results)
            {
                let certified = if graph_scores_are_current {
                    true
                } else if can_certify_resolved_scores {
                    if walk.failed_resolved_stop_distance.is_some_and(|failed| {
                        failed.total_cmp(&candidate.distance) == CmpOrdering::Equal
                    }) {
                        false
                    } else {
                        let mut nearer = 0usize;
                        let mut after: Option<(f32, usize, RowId)> = None;
                        while let Some(kept) = walk
                            .kept
                            .iter()
                            .filter(|kept| {
                                after.is_none_or(|(distance, partition, row_id)| {
                                    kept.0
                                        .reached
                                        .distance
                                        .total_cmp(&distance)
                                        .then_with(|| kept.0.partition.cmp(&partition))
                                        .then_with(|| kept.0.row_id.cmp(&row_id))
                                        == CmpOrdering::Greater
                                })
                            })
                            .min_by(|left, right| {
                                left.0
                                    .reached
                                    .distance
                                    .total_cmp(&right.0.reached.distance)
                                    .then_with(|| left.0.partition.cmp(&right.0.partition))
                                    .then_with(|| left.0.row_id.cmp(&right.0.row_id))
                            })
                        {
                            after =
                                Some((kept.0.reached.distance, kept.0.partition, kept.0.row_id));
                            let key = (kept.0.partition, kept.0.row_id);
                            let score = match walk
                                .kept_rows
                                .get(&key)
                                .and_then(|entry| entry.resolved_score)
                            {
                                Some(score) => Some(score),
                                None => resolve_score(
                                    kept.0.partition,
                                    kept.0.row_id,
                                    before_source_entry,
                                    before_distance,
                                )?,
                            };
                            if let Some(score) = score
                                && let Some(entry) = walk.kept_rows.get_mut(&key)
                            {
                                entry.resolved_score = Some(score);
                            }
                            if score.is_some_and(|score| {
                                (1.0 - score).total_cmp(&candidate.distance) == CmpOrdering::Less
                            }) {
                                nearer += 1;
                                if nearer >= required_results {
                                    break;
                                }
                            }
                        }
                        if nearer < required_results {
                            walk.failed_resolved_stop_distance = Some(candidate.distance);
                        }
                        nearer >= required_results
                    }
                } else {
                    false
                };
                if certified {
                    return Ok(Expanded {
                        converged: true,
                        stop: None,
                    });
                }
            }
            let layer = &mut open[candidate.layer as usize];
            let neighbours = layer
                .neighbours(candidate.point, 0, &mut hops, acquire)?
                .ok_or_else(|| E::from(Error::Other("graph layer-zero neighbours".to_string())))?;
            for hop in 0..neighbours.len() {
                let (neighbour_id, neighbour_origin) = neighbours.get(hop);
                let neighbour_is_allowed = layer
                    .id_to_row
                    .get(&neighbour_origin)
                    .is_some_and(|row| is_allowed(layer.partition, *row));
                if let Some(stop) = consider(
                    layer,
                    candidate.layer as usize,
                    neighbour_id,
                    walk,
                    query,
                    allow_bridges,
                    is_allowed,
                    before_source_entry,
                    before_distance,
                    acquire,
                    release,
                )? {
                    return Ok(Expanded {
                        converged: false,
                        stop: Some(stop),
                    });
                }
                if allow_bridges || neighbour_is_allowed {
                    continue;
                }
                // The excluded neighbour is where the admitted points stop
                // reaching each other. Step through it once, as navigation.
                let Some(bridge_hops) = layer.neighbours(neighbour_id, 0, &mut bridges, acquire)?
                else {
                    continue;
                };
                for bridge in 0..bridge_hops.len() {
                    let (bridge_id, _) = bridge_hops.get(bridge);
                    if let Some(stop) = consider(
                        layer,
                        candidate.layer as usize,
                        bridge_id,
                        walk,
                        query,
                        allow_bridges,
                        is_allowed,
                        before_source_entry,
                        before_distance,
                        acquire,
                        release,
                    )? {
                        return Ok(Expanded {
                            converged: false,
                            stop: Some(stop),
                        });
                    }
                }
            }
        }
        Ok(Expanded {
            converged: false,
            stop: None,
        })
    })();
    walk.hops = hops;
    walk.bridges = bridges;
    expanded
}

/// Reach one point: skip it if it was reached or the filter excludes it,
/// otherwise evaluate it, put it on the frontier and keep it if admitted.
#[allow(clippy::too_many_arguments)]
fn consider<E, S, D, A, R, F>(
    layer: &mut OpenLayer<'_>,
    position: usize,
    point_id: PointId,
    walk: &mut Walk,
    query: &UnionQuery<'_>,
    allow_bridges: bool,
    is_allowed: &mut F,
    before_source_entry: &mut S,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<Option<Stop>, E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    F: FnMut(usize, RowId) -> bool,
{
    let Some(bit) = layer.bit(point_id) else {
        return Ok(None);
    };
    if walk
        .reached
        .get(bit / 64)
        .is_none_or(|word| word & (1 << (bit % 64)) != 0)
    {
        return Ok(None);
    }
    let Some(origin) = layer.origin(point_id) else {
        return Ok(None);
    };
    let row_id = layer.id_to_row.get(&origin).copied();
    let admitted = row_id.is_some_and(|row| is_allowed(layer.partition, row));
    if !allow_bridges && !admitted {
        #[cfg(feature = "test-seams")]
        if let Some(row_id) = row_id {
            super::observe_hnsw_residual_rejection(row_id);
        }
        return Ok(None);
    }
    let distance = if let Some(distance) = walk.admission_scores[bit] {
        distance
    } else {
        if let Err(stop) = walk.charge() {
            return Ok(Some(stop));
        }
        let Some(distance) = layer.distance(
            point_id,
            query,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )?
        else {
            return Ok(None);
        };
        walk.visited += 1;
        walk.evaluations += 1;
        distance
    };
    walk.mark(bit);
    let reached = Reached {
        distance,
        row_id: row_id.unwrap_or(RowId(u64::MAX)),
        layer: position as u32,
        point: point_id,
        origin: origin as u32,
    };
    walk.push_frontier(reached, acquire)?;
    if admitted && let Some(row_id) = row_id {
        walk.keep(Kept {
            reached,
            partition: layer.partition,
            row_id,
        });
    }
    Ok(None)
}

/// What one union search spent, recorded for a proof on the thread that ran
/// it.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UnionSearchReceipt {
    pub budget: usize,
    /// Breadth resolved for this statement before multiplying by the widest
    /// admitted graph's stored bottom-layer degree.
    pub resolved_ef_search: usize,
    pub admission_evaluations: usize,
    pub expansion_evaluations: usize,
    pub layers_admitted: usize,
    pub partitions: usize,
    pub frontier_exhausted: bool,
    /// How the search ended: `answer_complete`, `frontier_exhausted` or
    /// `refused`.
    pub terminal: &'static str,
    /// Opening the layers alone needed more than the declared limit allows.
    pub below_admission_floor: bool,
    /// The most evaluations the walk could spend beyond opening its layers.
    pub expansion_cap: usize,
    /// The most neighbours one expanded point can evaluate: the layer-zero
    /// degree of the widest admitted graph.
    pub layer_zero_degree: usize,
}

#[cfg(feature = "test-seams")]
thread_local! {
    static UNION_SEARCH_RECEIPTS: RefCell<Option<Vec<UnionSearchReceipt>>> =
        const { RefCell::new(None) };
}

/// Collect the receipt of every union search `search` runs on this thread.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_union_search_receipts<R>(search: impl FnOnce() -> R) -> (R, Vec<UnionSearchReceipt>) {
    struct Guard(Option<Vec<UnionSearchReceipt>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            UNION_SEARCH_RECEIPTS.with(|slot| {
                slot.replace(self.0.take());
            });
        }
    }
    let prior = UNION_SEARCH_RECEIPTS.with(|slot| slot.replace(Some(Vec::new())));
    let guard = Guard(prior);
    let result = search();
    let receipts = UNION_SEARCH_RECEIPTS
        .with(|slot| slot.borrow_mut().as_mut().map(std::mem::take))
        .unwrap_or_default();
    drop(guard);
    (result, receipts)
}

#[cfg(feature = "test-seams")]
pub(crate) fn record_union_search(receipt: UnionSearchReceipt) {
    UNION_SEARCH_RECEIPTS.with(|slot| {
        if let Some(receipts) = slot.borrow_mut().as_mut() {
            receipts.push(receipt);
        }
    });
}

#[cfg(test)]
mod canonical_tie_tests {
    use super::*;

    #[test]
    fn a_full_result_heap_replaces_larger_row_ids_at_equal_distance() {
        let mut walk = Walk {
            reached: Vec::new(),
            admission_scores: Vec::new(),
            frontier: BinaryHeap::new(),
            frontier_capacity: 0,
            frontier_charged: 0,
            kept: BinaryHeap::new(),
            kept_capacity: 3,
            kept_rows: HashMap::new(),
            worst_ties: 0,
            failed_resolved_stop_distance: None,
            stop_distance_comparisons: std::cell::Cell::new(0),
            evaluations: 0,
            visited: 0,
            admissions: 0,
            evaluation_cap: 0,
            visit_cap: 0,
            hops: HopScratch::default(),
            bridges: HopScratch::default(),
        };
        // Graph traversal sees another Context's higher ids first. None of
        // these vectors equals the query; every score ties at distance one.
        for row_id in [21_u64, 22, 23, 1, 2, 3] {
            let partition = usize::from(row_id < 10);
            walk.keep(Kept {
                reached: Reached {
                    distance: 1.0,
                    row_id: RowId(row_id),
                    layer: partition as u32,
                    point: PointId(0, row_id as i32),
                    origin: row_id as u32,
                },
                partition,
                row_id: RowId(row_id),
            });
        }
        let mut kept = walk
            .kept
            .iter()
            .map(|entry| entry.0.row_id.0)
            .collect::<Vec<_>>();
        kept.sort_unstable();
        assert_eq!(kept, [1, 2, 3]);
        let reached = |distance, row_id| Reached {
            distance,
            row_id: RowId(row_id),
            layer: 0,
            point: PointId(0, row_id as i32),
            origin: row_id as u32,
        };
        assert!(!walk.frontier_is_farther_than_kept(reached(1.0, 4), 3));
        assert!(!walk.frontier_is_farther_than_kept(reached(1.0, 2), 3));
        assert!(!walk.frontier_is_farther_than_kept(reached(0.5, 4), 3));
        assert!(walk.frontier_is_farther_than_kept(reached(1.5, 1), 3));
        walk.keep(Kept {
            reached: reached(0.5, 10),
            partition: 0,
            row_id: RowId(10),
        });
        assert!(walk.frontier_is_farther_than_kept(reached(1.0, 4), 1));
        assert!(!walk.frontier_is_farther_than_kept(reached(1.0, 4), 2));
    }

    #[test]
    fn tied_frontier_stop_checks_do_linear_comparison_work() {
        let reached = |row_id| Reached {
            distance: 1.0,
            row_id: RowId(row_id),
            layer: 0,
            point: PointId(0, row_id as i32),
            origin: row_id as u32,
        };
        let mut observed = Vec::new();
        for breadth in [32_usize, 64, 128, 256] {
            let mut walk = Walk {
                reached: Vec::new(),
                admission_scores: Vec::new(),
                frontier: BinaryHeap::new(),
                frontier_capacity: 0,
                frontier_charged: 0,
                kept: BinaryHeap::new(),
                kept_capacity: breadth,
                kept_rows: HashMap::new(),
                worst_ties: 0,
                failed_resolved_stop_distance: None,
                stop_distance_comparisons: std::cell::Cell::new(0),
                evaluations: 0,
                visited: 0,
                admissions: 0,
                evaluation_cap: 0,
                visit_cap: 0,
                hops: HopScratch::default(),
                bridges: HopScratch::default(),
            };
            for row_id in 0..breadth as u64 {
                walk.keep(Kept {
                    reached: reached(row_id),
                    partition: 0,
                    row_id: RowId(row_id),
                });
            }
            for row_id in breadth as u64..2 * breadth as u64 {
                assert!(!walk.frontier_is_farther_than_kept(reached(row_id), 3));
            }
            observed.push((breadth, walk.stop_distance_comparisons.get()));
        }
        eprintln!("all-tied stop comparisons: {observed:?}");
        for (breadth, comparisons) in observed {
            assert!(
                comparisons <= breadth * 4,
                "a tied plateau of {breadth} needs no retained-heap scan per pop: {comparisons} comparisons"
            );
        }
    }
}
