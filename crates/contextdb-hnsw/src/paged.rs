//! Storage-access version of the seeded, sequential HNSW construction algorithm.
//!
//! A provider owns node persistence and transactions. This module retains only
//! one insertion's finite workspace; it never caches the population of nodes.
use crate::hnsw::{NB_LAYER_MAX, PointId};
use anndists::dist::distances::Distance;
use rand::{Rng, SeedableRng, distr::Uniform};
use rand_chacha::ChaCha12Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

/// One independently loadable point. Adjacency is bounded by the graph degree
/// and layer count, rather than by graph population.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PagedPoint<T> {
    pub id: PointId,
    pub origin_id: usize,
    pub values: Vec<T>,
    pub neighbours: Vec<Vec<PagedEdge>>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct PagedEdge {
    pub point_id: PointId,
    pub origin_id: usize,
    pub distance: f32,
}

impl PartialEq for PagedEdge {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for PagedEdge {}
impl PartialOrd for PagedEdge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PagedEdge {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.point_id.cmp(&other.point_id))
            .then_with(|| self.origin_id.cmp(&other.origin_id))
    }
}
impl PagedEdge {
    fn reversed(self) -> Self {
        Self {
            distance: -self.distance,
            ..self
        }
    }
}

/// Durable generator position. ChaCha12 is rand 0.9's seeded StdRng; retaining
/// the word position resumes the identical stream without replaying old draws.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PagedGraphState {
    pub seed: u64,
    pub rng_word_position: u128,
    pub layer_counts: [usize; NB_LAYER_MAX as usize],
    pub entry_point: Option<PointId>,
    pub points: usize,
    pub layer0_edges: usize,
}
impl PagedGraphState {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            rng_word_position: 0,
            layer_counts: [0; NB_LAYER_MAX as usize],
            entry_point: None,
            points: 0,
            layer0_edges: 0,
        }
    }
    fn next_level(&mut self, m: usize, max_layer: usize) -> usize {
        let mut rng = ChaCha12Rng::seed_from_u64(self.seed);
        rng.set_word_pos(self.rng_word_position);
        let sample = rng.sample(Uniform::<f64>::new(0., 1.).unwrap());
        let mut level = (-sample.ln() * (1. / (m as f64).ln())).floor() as usize;
        if level >= max_layer {
            level = rng.sample(Uniform::<usize>::new(0, max_layer).unwrap());
        }
        self.rng_word_position = rng.get_word_pos();
        level
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PagedGraphPolicy {
    pub m: usize,
    pub ef_construction: usize,
    pub max_layer: usize,
    pub extend_candidates: bool,
    pub keep_pruned: bool,
}
impl PagedGraphPolicy {
    pub fn validate(&self) -> bool {
        self.m > 1
            && self.m <= 256
            && self.ef_construction > 0
            && self.max_layer > 0
            && self.max_layer <= NB_LAYER_MAX as usize
    }
    fn degree(&self, layer: usize) -> usize {
        self.m * if layer == 0 { 2 } else { 1 }
    }
}

/// All reads authenticate identity and payload, and admit their finite node
/// decode buffers. Writes modify only the caller's current transaction. Failed
/// insertion requires dropping that transaction; no partial point is published.
pub trait PagedPointStorage<T> {
    type Error;
    fn load_point(&mut self, id: PointId) -> Result<PagedPoint<T>, Self::Error>;
    fn store_point(&mut self, point: &PagedPoint<T>) -> Result<(), Self::Error>;
    fn checkpoint(&mut self) -> Result<(), Self::Error>;
    /// Statement/maintenance hooks run before a distance or a first visit.
    fn before_distance(&mut self) -> Result<(), Self::Error> {
        self.checkpoint()
    }
    fn before_visit(&mut self) -> Result<(), Self::Error> {
        self.checkpoint()
    }
    /// Filtered serving charges admitted points to the statement's visit and
    /// distance limits. Excluded history remains cancellation/read-budget
    /// checked while it provides graph bridges. Construction uses the ordinary
    /// hooks above and is unaffected by allowed membership.
    fn before_allowed_visit(&mut self, origin: usize) -> Result<(), Self::Error> {
        if self.is_allowed(origin)? {
            self.before_visit()
        } else {
            self.checkpoint()
        }
    }
    fn before_allowed_distance(&mut self, origin: usize) -> Result<(), Self::Error> {
        if self.is_allowed(origin)? {
            self.before_distance()
        } else {
            self.checkpoint()
        }
    }
    /// Search-local spill storage. Providers may place the visited identities
    /// and pending priority queue in transaction-local pages. Returning None
    /// keeps the finite in-memory implementation for small resident adapters.
    fn begin_search_scratch(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn visit_search_scratch(&mut self, _id: PointId) -> Result<Option<bool>, Self::Error> {
        Ok(None)
    }
    fn push_search_candidate(&mut self, _edge: PagedEdge) -> Result<bool, Self::Error> {
        Ok(false)
    }
    fn pop_search_candidate(&mut self) -> Result<Option<Option<PagedEdge>>, Self::Error> {
        Ok(None)
    }
    /// Allowed-set admission affects retained results, while navigation may
    /// pass through excluded nodes. Construction uses unfiltered traversal.
    fn is_allowed(&mut self, _origin: usize) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

#[derive(Debug)]
pub enum PagedGraphError<E> {
    Storage(E),
    InvalidPolicy,
    InvalidState,
    InvalidPoint,
    ScratchExhausted,
    InvalidDistance,
    Encoding,
    InsufficientAllowedCandidates,
}
impl<E> From<E> for PagedGraphError<E> {
    fn from(error: E) -> Self {
        Self::Storage(error)
    }
}

/// Fixed-capacity insertion scratch. Allocate this only after reserving
/// `allocation_bound(capacity)` bytes. All containers refuse growth at their
/// admitted capacity; no population-sized allocation is made implicitly.
pub struct PagedGraphWorkspace {
    capacity: usize,
    visited: HashSet<PointId>,
    candidates: BinaryHeap<PagedEdge>,
    results: BinaryHeap<PagedEdge>,
    selected: Vec<PagedEdge>,
    discarded: BinaryHeap<PagedEdge>,
    extension: Vec<PointId>,
}
impl PagedGraphWorkspace {
    pub fn allocation_bound(capacity: usize) -> Option<usize> {
        let buckets = capacity
            .checked_mul(8)?
            .checked_div(7)?
            .checked_add(1)?
            .checked_next_power_of_two()?
            .max(4);
        capacity
            .checked_mul(8 * std::mem::size_of::<PagedEdge>() + std::mem::size_of::<PointId>())?
            .checked_add(
                buckets
                    .checked_mul(std::mem::size_of::<PointId>() + 1)?
                    .checked_add(32)?,
            )
    }
    /// Allocation bound of a workspace whose provider spills the search
    /// visited set and the search priority queue to transaction pages (every
    /// `visit_search_scratch`/`push_search_candidate` answers `Some`/`true`).
    /// Only the result, selection, pruning and extension containers remain.
    pub fn spilled_allocation_bound(capacity: usize) -> Option<usize> {
        capacity
            .checked_mul(4 * std::mem::size_of::<PagedEdge>() + std::mem::size_of::<PointId>())?
            .checked_add(32)
    }
    /// Exact allocation of `new(capacity)`: every container is allocated at
    /// its admitted capacity once and refuses growth beyond it.
    pub fn exact_allocation_bound(capacity: usize) -> Option<usize> {
        let buckets = capacity
            .checked_mul(8)?
            .checked_div(7)?
            .checked_add(1)?
            .checked_next_power_of_two()?
            .max(4);
        Self::spilled_allocation_bound(capacity)?.checked_add(
            buckets
                .checked_mul(std::mem::size_of::<PointId>() + 1)?
                .checked_add(32)?,
        )
    }
    /// Workspace for a spilling provider: the search visited set is never
    /// allocated. Every container still refuses growth at `capacity`.
    pub fn new_spilled(capacity: usize) -> Self {
        Self {
            capacity,
            visited: HashSet::new(),
            candidates: BinaryHeap::with_capacity(capacity),
            results: BinaryHeap::with_capacity(capacity),
            selected: Vec::with_capacity(capacity),
            discarded: BinaryHeap::with_capacity(capacity),
            extension: Vec::with_capacity(capacity),
        }
    }
    /// A workspace whose containers start empty and grow on demand, each in
    /// steps capped at `capacity`, so actual allocation follows one insertion's
    /// real use and never exceeds `exact_allocation_bound(capacity)`.
    pub fn new_lazy(capacity: usize) -> Self {
        Self {
            capacity,
            visited: HashSet::new(),
            candidates: BinaryHeap::new(),
            results: BinaryHeap::new(),
            selected: Vec::new(),
            discarded: BinaryHeap::new(),
            extension: Vec::new(),
        }
    }
    fn growth(len: usize, capacity: usize) -> usize {
        len.max(8).min(capacity.saturating_sub(len)).max(1)
    }
    fn grow_vec<T>(vec: &mut Vec<T>, capacity: usize) {
        if vec.len() == vec.capacity() {
            vec.reserve_exact(Self::growth(vec.len(), capacity));
        }
    }
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            visited: HashSet::with_capacity(capacity),
            candidates: BinaryHeap::with_capacity(capacity),
            results: BinaryHeap::with_capacity(capacity),
            selected: Vec::with_capacity(capacity),
            discarded: BinaryHeap::with_capacity(capacity),
            extension: Vec::with_capacity(capacity),
        }
    }
    fn clear_search(&mut self) {
        self.visited.clear();
        self.candidates.clear();
        self.results.clear();
    }
    fn push<E>(
        capacity: usize,
        heap: &mut BinaryHeap<PagedEdge>,
        point: PagedEdge,
    ) -> Result<(), PagedGraphError<E>> {
        if heap.len() >= capacity {
            return Err(PagedGraphError::ScratchExhausted);
        }
        if heap.len() == heap.capacity() {
            heap.reserve_exact(Self::growth(heap.len(), capacity));
        }
        heap.push(point);
        Ok(())
    }
    fn visit<E>(&mut self, id: PointId) -> Result<bool, PagedGraphError<E>> {
        if self.visited.contains(&id) {
            return Ok(false);
        }
        if self.visited.len() >= self.capacity {
            return Err(PagedGraphError::ScratchExhausted);
        }
        self.visited.insert(id);
        Ok(true)
    }
}

fn load<T, S: PagedPointStorage<T>>(
    storage: &mut S,
    id: PointId,
    policy: PagedGraphPolicy,
) -> Result<PagedPoint<T>, PagedGraphError<S::Error>> {
    storage.checkpoint()?;
    let point = storage.load_point(id)?;
    if point.id != id
        || id.1 < 0
        || id.0 as usize >= policy.max_layer
        || point.neighbours.len() > policy.max_layer
        || point.neighbours.iter().enumerate().any(|(l, edges)| {
            edges.len() > policy.degree(l)
                || edges
                    .iter()
                    .any(|e| !e.distance.is_finite() || e.distance < 0.)
        })
    {
        return Err(PagedGraphError::InvalidPoint);
    }
    Ok(point)
}
fn distance<T: Send + Sync, D: Distance<T>, E>(
    distance: &D,
    left: &[T],
    right: &[T],
) -> Result<f32, PagedGraphError<E>> {
    let value = distance.eval(left, right);
    if !value.is_finite() || value < 0. {
        return Err(PagedGraphError::InvalidDistance);
    }
    Ok(value)
}

fn visit_search<T, S: PagedPointStorage<T>>(
    storage: &mut S,
    workspace: &mut PagedGraphWorkspace,
    id: PointId,
    allowed_origin: Option<usize>,
) -> Result<bool, PagedGraphError<S::Error>> {
    let fresh = match storage.visit_search_scratch(id)? {
        Some(fresh) => fresh,
        None => workspace.visit(id)?,
    };
    if fresh {
        if let Some(origin) = allowed_origin {
            storage.before_allowed_visit(origin)?;
        } else {
            storage.before_visit()?;
        }
    }
    Ok(fresh)
}
fn push_search<T, S: PagedPointStorage<T>>(
    storage: &mut S,
    workspace: &mut PagedGraphWorkspace,
    edge: PagedEdge,
) -> Result<(), PagedGraphError<S::Error>> {
    if !storage.push_search_candidate(edge)? {
        PagedGraphWorkspace::push(workspace.capacity, &mut workspace.candidates, edge)?;
    }
    Ok(())
}
fn pop_search<T, S: PagedPointStorage<T>>(
    storage: &mut S,
    workspace: &mut PagedGraphWorkspace,
) -> Result<Option<PagedEdge>, PagedGraphError<S::Error>> {
    Ok(match storage.pop_search_candidate()? {
        Some(edge) => edge,
        None => workspace.candidates.pop(),
    })
}

#[allow(clippy::too_many_arguments)]
fn search_layer<T: Send + Sync, S: PagedPointStorage<T>, D: Distance<T>>(
    storage: &mut S,
    policy: PagedGraphPolicy,
    state: &PagedGraphState,
    workspace: &mut PagedGraphWorkspace,
    values: &[T],
    pivot: PointId,
    ef: usize,
    layer: usize,
    metric: &D,
    allowed_work: bool,
) -> Result<(), PagedGraphError<S::Error>> {
    workspace.clear_search();
    storage.begin_search_scratch()?;
    if state.layer_counts[layer] == 0 {
        return Ok(());
    }
    if ef == 0 || ef >= workspace.capacity {
        return Err(PagedGraphError::ScratchExhausted);
    }
    let entry = load(storage, pivot, policy)?;
    let first = PagedEdge {
        point_id: pivot,
        origin_id: entry.origin_id,
        distance: measured_search_distance(
            storage,
            metric,
            values,
            &entry.values,
            entry.origin_id,
            allowed_work,
        )?,
    };
    visit_search(
        storage,
        workspace,
        pivot,
        allowed_work.then_some(entry.origin_id),
    )?;
    push_search(storage, workspace, first.reversed())?;
    PagedGraphWorkspace::push(workspace.capacity, &mut workspace.results, first)?;
    while let Some(candidate) = pop_search(storage, workspace)? {
        if -candidate.distance > workspace.results.peek().unwrap().distance {
            break;
        }
        let point = load(storage, candidate.point_id, policy)?;
        for edge in point.neighbours.get(layer).into_iter().flatten() {
            let neighbour = if allowed_work {
                if workspace.visited.contains(&edge.point_id) {
                    continue;
                }
                let point = load(storage, edge.point_id, policy)?;
                if !visit_search(storage, workspace, edge.point_id, Some(point.origin_id))? {
                    continue;
                }
                point
            } else {
                if !visit_search(storage, workspace, edge.point_id, None)? {
                    continue;
                }
                load(storage, edge.point_id, policy)?
            };
            let score = measured_search_distance(
                storage,
                metric,
                values,
                &neighbour.values,
                neighbour.origin_id,
                allowed_work,
            )?;
            if score < workspace.results.peek().unwrap().distance || workspace.results.len() < ef {
                let scored = PagedEdge {
                    point_id: edge.point_id,
                    origin_id: neighbour.origin_id,
                    distance: score,
                };
                push_search(storage, workspace, scored.reversed())?;
                // Remove first when full: the just-admitted node must be better
                // than the current farthest, so this preserves heap semantics.
                if workspace.results.len() >= ef {
                    workspace.results.pop();
                }
                PagedGraphWorkspace::push(workspace.capacity, &mut workspace.results, scored)?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn select<T: Send + Sync, S: PagedPointStorage<T>, D: Distance<T>>(
    storage: &mut S,
    policy: PagedGraphPolicy,
    workspace: &mut PagedGraphWorkspace,
    values: &[T],
    cap: usize,
    layer: usize,
    extend: bool,
    metric: &D,
) -> Result<(), PagedGraphError<S::Error>> {
    workspace.selected.clear();
    workspace.discarded.clear();
    workspace.extension.clear();
    if workspace.candidates.len() <= cap && !extend {
        while let Some(edge) = workspace.candidates.pop() {
            PagedGraphWorkspace::grow_vec(&mut workspace.selected, workspace.capacity);
            workspace.selected.push(edge.reversed());
        }
        return Ok(());
    }
    if workspace.candidates.len() <= cap && extend {
        for candidate in workspace.candidates.iter() {
            let point = load(storage, candidate.point_id, policy)?;
            for edge in point.neighbours.get(layer).into_iter().flatten() {
                if workspace
                    .candidates
                    .iter()
                    .any(|c| c.point_id == edge.point_id)
                    || workspace.extension.contains(&edge.point_id)
                {
                    continue;
                }
                if workspace.extension.len() >= workspace.capacity {
                    return Err(PagedGraphError::ScratchExhausted);
                }
                PagedGraphWorkspace::grow_vec(&mut workspace.extension, workspace.capacity);
                workspace.extension.push(edge.point_id);
            }
        }
        workspace.extension.sort_unstable();
        for id in &workspace.extension {
            let point = load(storage, *id, policy)?;
            let score = measured_distance(storage, metric, values, &point.values)?;
            PagedGraphWorkspace::push(
                workspace.capacity,
                &mut workspace.candidates,
                PagedEdge {
                    point_id: *id,
                    origin_id: point.origin_id,
                    distance: -score,
                },
            )?;
        }
    }
    while workspace.selected.len() < cap {
        let Some(candidate) = workspace.candidates.pop() else {
            break;
        };
        let point = load(storage, candidate.point_id, policy)?;
        let mut accepted = true;
        for prior in &workspace.selected {
            let previous = load(storage, prior.point_id, policy)?;
            if measured_distance(storage, metric, &point.values, &previous.values)?
                <= -candidate.distance
            {
                accepted = false;
                break;
            }
        }
        if accepted {
            if workspace.selected.len() >= workspace.capacity {
                return Err(PagedGraphError::ScratchExhausted);
            }
            PagedGraphWorkspace::grow_vec(&mut workspace.selected, workspace.capacity);
            workspace.selected.push(candidate.reversed());
        } else if policy.keep_pruned {
            PagedGraphWorkspace::push(workspace.capacity, &mut workspace.discarded, candidate)?;
        }
    }
    if policy.keep_pruned {
        while workspace.selected.len() < cap {
            let Some(candidate) = workspace.discarded.pop() else {
                break;
            };
            if workspace.selected.len() >= workspace.capacity {
                return Err(PagedGraphError::ScratchExhausted);
            }
            PagedGraphWorkspace::grow_vec(&mut workspace.selected, workspace.capacity);
            workspace.selected.push(candidate.reversed());
        }
    }
    Ok(())
}

/// Insert one point using the same descent, diversity selection, reciprocal
/// pruning, tie order and seeded level sampling as sequential resident HNSW.
/// `state` is updated only after success. Transaction rollback must discard all
/// writes on any error, including workspace exhaustion or cancellation.
pub fn insert_paged<T: Send + Sync, S: PagedPointStorage<T>, D: Distance<T>>(
    storage: &mut S,
    policy: PagedGraphPolicy,
    state: &mut PagedGraphState,
    workspace: &mut PagedGraphWorkspace,
    values: Vec<T>,
    origin_id: usize,
    metric: &D,
) -> Result<PointId, PagedGraphError<S::Error>> {
    if !policy.validate() {
        return Err(PagedGraphError::InvalidPolicy);
    }
    let mut successor = state.clone();
    let level = successor.next_level(policy.m, policy.max_layer);
    let rank =
        i32::try_from(successor.layer_counts[level]).map_err(|_| PagedGraphError::InvalidState)?;
    let id = PointId(level as u8, rank);
    successor.layer_counts[level] = successor.layer_counts[level]
        .checked_add(1)
        .ok_or(PagedGraphError::InvalidState)?;
    successor.points = successor
        .points
        .checked_add(1)
        .ok_or(PagedGraphError::InvalidState)?;
    let mut new_point = PagedPoint {
        id,
        origin_id,
        values,
        neighbours: vec![Vec::new(); policy.max_layer],
    };
    let Some(mut pivot) = state.entry_point else {
        if state.points != 0 {
            return Err(PagedGraphError::InvalidState);
        }
        storage.store_point(&new_point)?;
        successor.entry_point = Some(id);
        *state = successor;
        return Ok(id);
    };
    let entry_values = load(storage, pivot, policy)?.values;
    let mut dist_to_entry = measured_distance(storage, metric, &new_point.values, &entry_values)?;
    for layer in ((level + 1)..(pivot.0 as usize + 1)).rev() {
        search_layer(
            storage,
            policy,
            &successor,
            workspace,
            &new_point.values,
            pivot,
            1,
            layer,
            metric,
            false,
        )?;
        if let Some(edge) = workspace.results.pop() {
            new_point.neighbours[layer].push(edge);
            if edge.distance < dist_to_entry {
                pivot = edge.point_id;
                dist_to_entry = edge.distance;
            }
        }
    }
    for layer in (0..=level).rev() {
        search_layer(
            storage,
            policy,
            &successor,
            workspace,
            &new_point.values,
            pivot,
            policy.ef_construction,
            layer,
            metric,
            false,
        )?;
        workspace.candidates.clear();
        while let Some(edge) = workspace.results.pop() {
            PagedGraphWorkspace::push(
                workspace.capacity,
                &mut workspace.candidates,
                edge.reversed(),
            )?;
        }
        if !workspace.candidates.is_empty() {
            select(
                storage,
                policy,
                workspace,
                &new_point.values,
                policy.degree(layer),
                layer,
                layer == 0 && policy.extend_candidates,
                metric,
            )?;
            workspace.selected.sort_unstable();
            new_point.neighbours[layer] = workspace.selected.clone();
            if let Some(edge) = workspace.selected.first() {
                pivot = edge.point_id;
            }
        }
    }
    storage.store_point(&new_point)?;
    successor.layer0_edges = successor
        .layer0_edges
        .checked_add(new_point.neighbours[0].len())
        .ok_or(PagedGraphError::InvalidState)?;
    for layer in (0..=level).rev() {
        for edge in &new_point.neighbours[layer] {
            if edge.point_id == id {
                continue;
            }
            let mut point = load(storage, edge.point_id, policy)?;
            if point.neighbours.len() <= layer {
                point.neighbours.resize_with(layer + 1, Vec::new);
            }
            if point.neighbours[layer].iter().any(|e| e.point_id == id) {
                continue;
            }
            let prior_degree = point.neighbours[layer].len();
            workspace.candidates.clear();
            for old in &point.neighbours[layer] {
                PagedGraphWorkspace::push(
                    workspace.capacity,
                    &mut workspace.candidates,
                    old.reversed(),
                )?;
            }
            PagedGraphWorkspace::push(
                workspace.capacity,
                &mut workspace.candidates,
                PagedEdge {
                    point_id: id,
                    origin_id,
                    distance: -edge.distance,
                },
            )?;
            if workspace.candidates.len() > policy.degree(layer) {
                select(
                    storage,
                    policy,
                    workspace,
                    &point.values,
                    policy.degree(layer),
                    layer,
                    false,
                    metric,
                )?;
                point.neighbours[layer] = workspace.selected.clone();
            } else {
                point.neighbours[layer].push(PagedEdge {
                    point_id: id,
                    origin_id,
                    distance: edge.distance,
                });
            }
            point.neighbours[layer].sort_unstable();
            if layer == 0 {
                successor.layer0_edges = successor
                    .layer0_edges
                    .checked_add(point.neighbours[layer].len())
                    .and_then(|n| n.checked_sub(prior_degree))
                    .ok_or(PagedGraphError::InvalidState)?;
            }
            storage.store_point(&point)?;
        }
    }
    if id.0 > state.entry_point.unwrap().0 {
        successor.entry_point = Some(id);
    }
    *state = successor;
    Ok(id)
}

/// Search an already published graph through point reads. This performs zero
/// build work and uses the same finite workspace as insertion.
pub fn search_paged<T: Send + Sync, S: PagedPointStorage<T>, D: Distance<T>>(
    storage: &mut S,
    policy: PagedGraphPolicy,
    state: &PagedGraphState,
    workspace: &mut PagedGraphWorkspace,
    values: &[T],
    ef: usize,
    metric: &D,
) -> Result<Vec<PagedEdge>, PagedGraphError<S::Error>> {
    if !policy.validate() {
        return Err(PagedGraphError::InvalidPolicy);
    }
    let Some(mut pivot) = state.entry_point else {
        return Ok(Vec::new());
    };
    for layer in (1..=pivot.0 as usize).rev() {
        search_layer(
            storage, policy, state, workspace, values, pivot, 1, layer, metric, false,
        )?;
        if let Some(nearest) = workspace.results.pop() {
            pivot = nearest.point_id;
        }
    }
    search_layer(
        storage, policy, state, workspace, values, pivot, ef, 0, metric, false,
    )?;
    let mut results = Vec::with_capacity(workspace.results.len());
    while let Some(edge) = workspace.results.pop() {
        results.push(edge);
    }
    results.sort_unstable();
    Ok(results)
}

fn measured_distance<T: Send + Sync, S: PagedPointStorage<T>, D: Distance<T>>(
    storage: &mut S,
    metric: &D,
    left: &[T],
    right: &[T],
) -> Result<f32, PagedGraphError<S::Error>> {
    storage.before_distance()?;
    distance(metric, left, right)
}

fn measured_search_distance<T: Send + Sync, S: PagedPointStorage<T>, D: Distance<T>>(
    storage: &mut S,
    metric: &D,
    left: &[T],
    right: &[T],
    origin: usize,
    allowed: bool,
) -> Result<f32, PagedGraphError<S::Error>> {
    if allowed {
        storage.before_allowed_distance(origin)?;
    } else {
        storage.before_distance()?;
    }
    distance(metric, left, right)
}

/// Allowed-result search retains only admitted nodes and refills through
/// excluded history. It refuses a short result unless the authoritative allowed
/// count proves that shortness, rather than filtering a completed top-ef page.
#[allow(clippy::too_many_arguments)]
pub fn search_paged_allowed<T: Send + Sync, S: PagedPointStorage<T>, D: Distance<T>>(
    storage: &mut S,
    policy: PagedGraphPolicy,
    state: &PagedGraphState,
    workspace: &mut PagedGraphWorkspace,
    values: &[T],
    ef: usize,
    required_results: usize,
    allowed_count: usize,
    metric: &D,
) -> Result<Vec<PagedEdge>, PagedGraphError<S::Error>> {
    if !policy.validate() {
        return Err(PagedGraphError::InvalidPolicy);
    }
    if required_results > allowed_count || required_results > ef {
        return Err(PagedGraphError::InvalidState);
    }
    if allowed_count == 0 {
        return Ok(Vec::new());
    }
    let Some(mut pivot) = state.entry_point else {
        return Err(PagedGraphError::InvalidState);
    };
    for layer in (1..=pivot.0 as usize).rev() {
        search_layer(
            storage, policy, state, workspace, values, pivot, 1, layer, metric, true,
        )?;
        if let Some(nearest) = workspace.results.pop() {
            pivot = nearest.point_id;
        }
    }
    if ef == 0 || ef >= workspace.capacity {
        return Err(PagedGraphError::ScratchExhausted);
    }
    workspace.clear_search();
    let entry = load(storage, pivot, policy)?;
    let first = PagedEdge {
        point_id: pivot,
        origin_id: entry.origin_id,
        distance: measured_search_distance(
            storage,
            metric,
            values,
            &entry.values,
            entry.origin_id,
            true,
        )?,
    };
    storage.before_allowed_visit(entry.origin_id)?;
    workspace.visit(pivot)?;
    PagedGraphWorkspace::push(
        workspace.capacity,
        &mut workspace.candidates,
        first.reversed(),
    )?;
    if storage.is_allowed(first.origin_id)? {
        PagedGraphWorkspace::push(workspace.capacity, &mut workspace.results, first)?;
    }
    while let Some(candidate) = workspace.candidates.pop() {
        if workspace.results.len() >= ef
            && -candidate.distance > workspace.results.peek().unwrap().distance
        {
            break;
        }
        let point = load(storage, candidate.point_id, policy)?;
        for edge in point.neighbours.first().into_iter().flatten() {
            if workspace.visited.contains(&edge.point_id) {
                continue;
            }
            let neighbour = load(storage, edge.point_id, policy)?;
            storage.before_allowed_visit(neighbour.origin_id)?;
            workspace.visit(edge.point_id)?;
            let score = measured_search_distance(
                storage,
                metric,
                values,
                &neighbour.values,
                neighbour.origin_id,
                true,
            )?;
            if workspace.results.len() < ef || score < workspace.results.peek().unwrap().distance {
                let scored = PagedEdge {
                    point_id: edge.point_id,
                    origin_id: neighbour.origin_id,
                    distance: score,
                };
                PagedGraphWorkspace::push(
                    workspace.capacity,
                    &mut workspace.candidates,
                    scored.reversed(),
                )?;
                if storage.is_allowed(neighbour.origin_id)? {
                    if workspace.results.len() >= ef {
                        workspace.results.pop();
                    }
                    PagedGraphWorkspace::push(workspace.capacity, &mut workspace.results, scored)?;
                }
            }
        }
    }
    if workspace.results.len() < required_results {
        return Err(PagedGraphError::InsufficientAllowedCandidates);
    }
    let mut results = Vec::with_capacity(workspace.results.len());
    while let Some(edge) = workspace.results.pop() {
        results.push(edge);
    }
    results.sort_unstable();
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::Hnsw;
    use anndists::dist::distances::DistL2;
    use std::collections::BTreeMap;

    #[derive(Default, Clone)]
    struct Storage(BTreeMap<PointId, PagedPoint<f32>>);
    impl PagedPointStorage<f32> for Storage {
        type Error = ();
        fn load_point(&mut self, id: PointId) -> Result<PagedPoint<f32>, ()> {
            self.0.get(&id).cloned().ok_or(())
        }
        fn store_point(&mut self, point: &PagedPoint<f32>) -> Result<(), ()> {
            self.0.insert(point.id, point.clone());
            Ok(())
        }
        fn checkpoint(&mut self) -> Result<(), ()> {
            Ok(())
        }
    }

    #[test]
    fn provider_insertion_and_restart_preserve_resident_seeded_topology() {
        for (extend, keep) in [(false, false), (true, true)] {
            let policy = PagedGraphPolicy {
                m: 8,
                ef_construction: 32,
                max_layer: 16,
                extend_candidates: extend,
                keep_pruned: keep,
            };
            let mut resident = Hnsw::new_with_seed(8, 300, 16, 32, DistL2 {}, 91);
            resident.set_extend_candidates(extend);
            resident.set_keeping_pruned(keep);
            let mut storage = Storage::default();
            let mut state = PagedGraphState::new(91);
            let mut workspace = PagedGraphWorkspace::new(4096);
            for i in 0..300 {
                let values = vec![
                    ((i * 17) % 83) as f32,
                    ((i * 19) % 59) as f32,
                    ((i * 7) % 97) as f32,
                ];
                let id = insert_paged(
                    &mut storage,
                    policy,
                    &mut state,
                    &mut workspace,
                    values.clone(),
                    i,
                    &DistL2 {},
                )
                .unwrap();
                assert_eq!(id, resident.insert_slice_with_point_id((&values, i)));
                // Recover from a durable state representation between insertions.
                state = bincode::deserialize(&bincode::serialize(&state).unwrap()).unwrap();
            }
            for point in resident.get_point_indexation() {
                let saved = &storage.0[&point.get_point_id()];
                let neighbours = point.get_neighborhood_id();
                assert_eq!(saved.neighbours.len(), neighbours.len());
                for (a, b) in saved.neighbours.iter().zip(neighbours) {
                    assert_eq!(
                        a.iter()
                            .map(|e| (e.point_id, e.origin_id, e.distance.to_bits()))
                            .collect::<Vec<_>>(),
                        b.iter()
                            .map(|e| (e.p_id, e.d_id, e.distance.to_bits()))
                            .collect::<Vec<_>>()
                    );
                }
            }
            let mut bytes = Vec::new();
            crate::hnswio::encode_paged_graph_into::<f32, _, _>(
                &mut storage,
                &state,
                policy,
                3,
                &mut bytes,
            )
            .unwrap();
            assert_eq!(bytes, crate::hnswio::encode_owned_graph(&resident).unwrap());
        }
    }

    #[test]
    fn scratch_exhaustion_leaves_recoverable_state_unchanged() {
        let policy = PagedGraphPolicy {
            m: 8,
            ef_construction: 32,
            max_layer: 16,
            extend_candidates: false,
            keep_pruned: false,
        };
        let mut storage = Storage::default();
        let mut state = PagedGraphState::new(1);
        let mut small = PagedGraphWorkspace::new(1);
        insert_paged(
            &mut storage,
            policy,
            &mut state,
            &mut small,
            vec![1., 0.],
            0,
            &DistL2 {},
        )
        .unwrap();
        let before = state.clone();
        let saved = storage.clone();
        assert!(matches!(
            insert_paged(
                &mut storage,
                policy,
                &mut state,
                &mut small,
                vec![0., 1.],
                1,
                &DistL2 {}
            ),
            Err(PagedGraphError::ScratchExhausted)
        ));
        assert_eq!(state, before);
        storage = saved;
        let mut larger = PagedGraphWorkspace::new(256);
        insert_paged(
            &mut storage,
            policy,
            &mut state,
            &mut larger,
            vec![0., 1.],
            1,
            &DistL2 {},
        )
        .unwrap();
        assert_eq!(state.points, 2);
    }
    #[test]
    fn provider_allowed_search_refills_past_excluded_nearest_history() {
        struct Filtered(Storage);
        impl PagedPointStorage<f32> for Filtered {
            type Error = ();
            fn load_point(&mut self, id: PointId) -> Result<PagedPoint<f32>, ()> {
                self.0.load_point(id)
            }
            fn store_point(&mut self, point: &PagedPoint<f32>) -> Result<(), ()> {
                self.0.store_point(point)
            }
            fn checkpoint(&mut self) -> Result<(), ()> {
                Ok(())
            }
            fn is_allowed(&mut self, origin: usize) -> Result<bool, ()> {
                Ok(origin >= 96)
            }
        }
        let policy = PagedGraphPolicy {
            m: 8,
            ef_construction: 32,
            max_layer: 16,
            extend_candidates: false,
            keep_pruned: false,
        };
        let mut source = Filtered(Storage::default());
        let mut state = PagedGraphState::new(91);
        let mut workspace = PagedGraphWorkspace::new(4096);
        for origin in 0..100 {
            insert_paged(
                &mut source,
                policy,
                &mut state,
                &mut workspace,
                vec![origin as f32, 1.],
                origin,
                &DistL2 {},
            )
            .unwrap();
        }
        let rows = search_paged_allowed(
            &mut source,
            policy,
            &state,
            &mut workspace,
            &[0., 1.],
            4,
            4,
            4,
            &DistL2 {},
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|p| p.origin_id).collect::<Vec<_>>(),
            vec![96, 97, 98, 99]
        );
    }
}
