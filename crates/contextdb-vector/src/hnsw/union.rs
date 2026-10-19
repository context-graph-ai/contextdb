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
use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;

/// Optional statement-local decoded navigation, under the same reader funder
/// as mandatory scratch. Layer ordinals namespace captured readers: no entry
/// can escape this invocation or refer to a later root of the same graph.
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
enum NavigationKey {
    Point(usize, PointId),
    Identity(usize, usize),
}
struct NavigationPoint {
    origin: usize,
    neighbours: Vec<Vec<(PointId, usize)>>,
    // Already decoded bodies are optional and admit only from the headroom
    // remaining while the native source/decode window is still charged.
    values: Option<Vec<f32>>,
}
struct NavigationIdentity {
    row: RowId,
    _point: PointId,
    _lsn: contextdb_core::Lsn,
    _created_tx: contextdb_core::TxId,
    _deleted_tx: Option<contextdb_core::TxId>,
}
enum NavigationValue {
    Point(NavigationPoint),
    Identity(NavigationIdentity),
}
struct NavigationEntry {
    value: NavigationValue,
    bytes: usize,
    last_used: usize,
}
struct NavigationCache {
    accountant: Option<Arc<dyn crate::memory_budget::MemoryBudget>>,
    entries: HashMap<NavigationKey, NavigationEntry>,
    tick: usize,
    table_bytes: usize,
    payload_bytes: usize,
    _control_invoice: Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
}
impl NavigationCache {
    fn new(accountant: Option<Arc<dyn crate::memory_budget::MemoryBudget>>) -> Self {
        let control_invoice = accountant.as_ref().and_then(|owner| owner.control_invoice());
        Self {
            accountant,
            _control_invoice: control_invoice,
            entries: HashMap::new(),
            tick: 0,
            table_bytes: 0,
            payload_bytes: 0,
        }
    }
    fn clear(&mut self) {
        // Drop allocations first, then their real operation charge. Removing
        // entries alone would retain the map's buckets and is insufficient.
        let entries = std::mem::take(&mut self.entries);
        drop(entries);
        if let Some(owner) = &self.accountant {
            owner.release(
                self.table_bytes
                    .checked_add(self.payload_bytes)
                    .expect("admitted navigation total"),
            );
        }
        self.table_bytes = 0;
        self.payload_bytes = 0;
    }
    fn evict_lru(&mut self) -> bool {
        let Some(key) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| *key)
        else {
            return false;
        };
        let entry = self.entries.remove(&key).expect("LRU entry");
        let bytes = entry.bytes;
        drop(entry);
        self.payload_bytes -= bytes;
        if let Some(owner) = &self.accountant {
            owner.release(bytes);
        }
        if self.entries.is_empty() {
            let entries = std::mem::take(&mut self.entries);
            drop(entries);
            if let Some(owner) = &self.accountant {
                owner.release(self.table_bytes);
            }
            self.table_bytes = 0;
        }
        true
    }
    fn next_tick(&mut self) -> usize {
        if self.tick == usize::MAX {
            let mut previous = 0usize;
            let mut rank = 0usize;
            while let Some((key, stamp)) = self
                .entries
                .iter()
                .filter(|(_, entry)| entry.last_used > previous)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, entry)| (*key, entry.last_used))
            {
                rank += 1;
                self.entries
                    .get_mut(&key)
                    .expect("navigation recency entry")
                    .last_used = rank;
                previous = stamp;
            }
            self.tick = rank;
        }
        self.tick += 1;
        self.tick
    }
    fn touch(&mut self, key: NavigationKey) -> Option<&NavigationValue> {
        let tick = self.next_tick();
        let entry = self.entries.get_mut(&key)?;
        entry.last_used = tick;
        Some(&entry.value)
    }
    fn insert(&mut self, key: NavigationKey, value: NavigationValue, bytes: usize) {
        let last_used = self.next_tick();
        self.entries.insert(
            key,
            NavigationEntry {
                value,
                bytes,
                last_used,
            },
        );
    }
    fn reserve(&mut self, bytes: usize) -> bool {
        let Some(owner) = &self.accountant else {
            return false;
        };
        owner
            .try_allocate_for(
                bytes,
                "vector_navigation",
                "retain_authenticated_navigation",
                "Optional navigation retention.",
            )
            .is_ok()
    }

    fn table_slot(&mut self) -> bool {
        if self.entries.len() < self.entries.capacity() {
            return true;
        }
        let Some(wanted) = self.entries.capacity().checked_mul(2).map(|n| n.max(4)) else {
            return false;
        };
        let Some(buckets) = wanted
            .checked_mul(8)
            .map(|n| n.div_ceil(7))
            .and_then(|n| n.checked_next_power_of_two())
        else {
            return false;
        };
        let Some(bytes) = buckets
            .checked_mul(std::mem::size_of::<(NavigationKey, NavigationEntry)>() + 1)
            .and_then(|n| n.checked_add(16))
        else {
            return false;
        };
        // New and old table coexist during rehash. Admit the complete new
        // capacity, not merely its delta; payload allocations remain charged.
        if !self.reserve(bytes) {
            return false;
        }
        if self
            .entries
            .try_reserve(wanted.saturating_sub(self.entries.len()))
            .is_err()
        {
            self.accountant
                .as_ref()
                .expect("optional funder")
                .release(bytes);
            return false;
        }
        self.accountant
            .as_ref()
            .expect("optional funder")
            .release(self.table_bytes);
        self.table_bytes = bytes;
        true
    }
    fn row(&mut self, layer: usize, origin: usize) -> Option<RowId> {
        match self.touch(NavigationKey::Identity(layer, origin)) {
            Some(NavigationValue::Identity(identity)) => Some(identity.row),
            _ => None,
        }
    }
    fn row_version(&mut self, layer: usize, origin: usize) -> Option<Lsn> {
        match self.touch(NavigationKey::Identity(layer, origin)) {
            Some(NavigationValue::Identity(identity)) => Some(identity._lsn),
            _ => None,
        }
    }
    fn remember_identity(
        &mut self,
        layer: usize,
        origin: usize,
        identity: &paged_builder::PagedVectorIdentity,
    ) {
        let key = NavigationKey::Identity(layer, origin);
        if self.touch(key).is_some() {
            return;
        }
        if self.table_slot() {
            self.insert(
                key,
                NavigationValue::Identity(NavigationIdentity {
                    row: identity.row_id,
                    _point: identity.point,
                    _lsn: identity.lsn,
                    _created_tx: identity.created_tx,
                    _deleted_tx: identity.deleted_tx,
                }),
                0,
            );
        }
    }
    fn point(&mut self, layer: usize, point: PointId) -> Option<&NavigationPoint> {
        match self.touch(NavigationKey::Point(layer, point)) {
            Some(NavigationValue::Point(point)) => Some(point),
            _ => None,
        }
    }
    fn remember_point(
        &mut self,
        layer: usize,
        id: PointId,
        value: &paged_builder::PagedVectorPoint,
    ) {
        if self.point(layer, id).is_some() {
            return;
        }
        let (origin, neighbours) = match value {
            paged_builder::PagedVectorPoint::F32(point) => (point.origin_id, &point.neighbours),
            paged_builder::PagedVectorPoint::Quantized(point) => {
                (point.origin_id, &point.neighbours)
            }
        };
        let Some(headers) = neighbours
            .len()
            .checked_mul(std::mem::size_of::<Vec<(PointId, usize)>>())
        else {
            return;
        };
        let Some(bytes) = neighbours.iter().try_fold(headers, |sum, edges| {
            sum.checked_add(
                edges
                    .len()
                    .checked_mul(std::mem::size_of::<(PointId, usize)>())?,
            )
        }) else {
            return;
        };
        if self.payload_bytes.checked_add(bytes).is_none() || !self.reserve(bytes) {
            return;
        }
        if !self.table_slot() {
            self.accountant
                .as_ref()
                .expect("optional funder")
                .release(bytes);
            return;
        }
        let mut compact = Vec::with_capacity(neighbours.len());
        for edges in neighbours {
            let mut copied = Vec::with_capacity(edges.len());
            copied.extend(edges.iter().map(|edge| (edge.point_id, edge.origin_id)));
            compact.push(copied);
        }
        self.payload_bytes = self
            .payload_bytes
            .checked_add(bytes)
            .expect("admitted navigation payload");
        let mut cached = NavigationPoint {
            origin,
            neighbours: compact,
            values: None,
        };
        // Upper-layer bodies remain optional. Do not evict the admitted new
        // table while this entry is pending insertion; refusal is a body miss.
        if id.0 != 0
            && let paged_builder::PagedVectorPoint::F32(point) = value
            && let Some(body_bytes) = point.values.len().checked_mul(std::mem::size_of::<f32>())
            && self.payload_bytes.checked_add(body_bytes).is_some()
            && self.accountant.as_ref().is_some_and(|owner| {
                owner
                    .try_allocate_for(
                        body_bytes,
                        "vector_navigation",
                        "retain_authenticated_navigation",
                        "Optional navigation body.",
                    )
                    .is_ok()
            })
        {
            cached.values = Some(point.values.clone());
            self.payload_bytes += body_bytes;
        }
        let retained = bytes
            .checked_add(
                cached
                    .values
                    .as_ref()
                    .map_or(0, |v| v.len() * std::mem::size_of::<f32>()),
            )
            .expect("admitted navigation entry");
        self.insert(
            NavigationKey::Point(layer, id),
            NavigationValue::Point(cached),
            retained,
        );
    }
}
impl Drop for NavigationCache {
    fn drop(&mut self) {
        self.clear();
    }
}

pub(super) fn navigation_memory_refusal(error: &Error) -> bool {
    use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
    match error {
        Error::MemoryBudgetExceeded { .. } => true,
        Error::ReadFailure(failure) => matches!(failure.detail(),
            ReadFailureDetail::OwnerLimitExceeded(detail)
                if detail.limit == ReadFailureLimit::Memory),
        _ => false,
    }
}
// Stack controls need no optional heap charge. Only retained navigation
// allocations compete with mandatory work, and all are reclaimable.
pub(crate) struct NavigationSession {
    navigation: parking_lot::Mutex<NavigationCache>,
}
impl NavigationSession {
    pub(crate) fn new(owner: Option<Arc<dyn crate::memory_budget::MemoryBudget>>) -> Self {
        Self {
            navigation: parking_lot::Mutex::new(NavigationCache::new(owner)),
        }
    }
    fn lock(&self) -> parking_lot::MutexGuard<'_, NavigationCache> {
        self.navigation.lock()
    }
    pub(crate) fn owner(&self) -> Option<Arc<dyn crate::memory_budget::MemoryBudget>> {
        self.lock().accountant.clone()
    }
    pub(crate) fn release_navigation_before_body(&self) {
        // A stored score opens its own authenticated codec/page window, beyond
        // its F32 invoice. Optional navigation must physically leave before
        // that mandatory window; keeping it until the next graph debit can
        // make the real body decode fail despite reclaimable cache capacity.
        self.lock().clear();
    }
    pub(crate) fn reclaim(&self) -> bool {
        // Never evict underneath an active navigation borrow.
        self.navigation
            .try_lock()
            .is_some_and(|mut cache| cache.evict_lru())
    }
    pub(crate) fn reserve_for(
        &self,
        token: &mut crate::memory_budget::VectorCopyAdmission,
        bytes: usize,
        component: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        loop {
            match token.reserve_for(bytes, component, operation, hint) {
                Ok(()) => return Ok(()),
                Err(error) if navigation_memory_refusal(&error) && self.reclaim() => {}
                Err(error) => return Err(error),
            }
        }
    }
    pub(crate) fn reserve(
        &self,
        token: &mut crate::memory_budget::VectorCopyAdmission,
        bytes: usize,
    ) -> contextdb_core::Result<()> {
        self.reserve_for(
            token,
            bytes,
            "vector_copy",
            "materialize",
            "Reduce the selected copy or raise the executing statement's memory limit.",
        )
    }
}

/// One graph layer a union search admits: a generation or mutable tail of one
/// partition, the statement's position for that partition, the rows the
/// partition may admit, and -- for a filtered scope -- the partition's sorted
/// candidate directory.
/// A partition's allowed-row count when its filter is checked row by row as
/// the walk reaches rows, never listed: the count is not known, and the walk
/// never decides anything from it. See `union_search_allowed` for what an
/// unknown count changes.
pub(crate) const ALLOWED_UNKNOWN: usize = usize::MAX;

pub(crate) struct HnswUnionLayer<'a> {
    pub(crate) graph: &'a HnswIndex,
    pub(crate) partition: usize,
    /// The rows its filter allows, or `ALLOWED_UNKNOWN`.
    pub(crate) allowed_count: usize,
    /// The logical rows the partition's snapshot sees, however many graph
    /// points hold them: a row whose superseded version still sits in a
    /// sealed layer beside its current one is one row here.
    pub(crate) live_count: usize,
    pub(crate) seed_rows: Option<UnionSeedRows<'a>>,
    /// For a sealed generation, the frontier it was built at: its points are
    /// the versions visible then, so a point is its row's current version
    /// exactly when that version is no newer. `None` for a tail, whose points
    /// carry their own versions.
    pub(crate) covered_lsn: Option<Lsn>,
}

#[derive(Clone, Copy)]
pub(crate) enum UnionSeedRows<'a> {
    Slice(&'a [u64]),
    Iterator(&'a dyn Fn() -> Box<dyn Iterator<Item = u64> + 'a>),
}
impl<'a> UnionSeedRows<'a> {
    fn iter(self) -> Box<dyn Iterator<Item = u64> + 'a> {
        match self {
            Self::Slice(rows) => Box::new(rows.iter().copied()),
            Self::Iterator(rows) => rows(),
        }
    }
    fn contains(&self, row: u64) -> bool {
        match self {
            Self::Slice(rows) => rows.binary_search(&row).is_ok(),
            Self::Iterator(rows) => rows().any(|value| value == row),
        }
    }
}

/// One row a union search hands back, tagged with the partition and layer
/// that produced it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HnswUnionCandidate {
    pub(crate) partition: usize,
    pub(crate) layer: usize,
    /// Authenticated graph point shared by F32 identity members. Exact-key
    /// ties have no point certificate. This is never a stored-score receipt.
    pub(crate) origin: Option<usize>,
    pub(crate) row_id: RowId,
    /// The distance the walk reached this row at.
    pub(crate) distance: f32,
    /// The visible stored score, when an early-stop proof already read it.
    pub(crate) resolved_score: Option<f32>,
    /// The stored scorer completed, including an authenticated absence.
    pub(crate) score_checked: bool,
}

#[derive(Clone, Copy)]
struct KeptRowInfo {
    distance: f32,
    resolved_score: Option<f32>,
    score_checked: bool,
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
    /// Partitions an unfiltered walk found holding live rows their graph
    /// does not connect: the caller compares their rows instead, and their
    /// candidates here are not an answer. Counted in `retained_bytes`.
    pub(crate) disconnected: Vec<usize>,
    pub(crate) retained_bytes: usize,
    pub(crate) status: HnswAllowedSearchStatus,
    /// Opening the layers alone reached the declared limit:
    /// `(admission_evaluations, limit)`.
    pub(crate) below_admission_floor: Option<(usize, usize)>,
    pub(crate) budget: usize,
    pub(crate) admission_evaluations: usize,
    pub(crate) expansion_evaluations: usize,
    /// Visible stored scores read for restamped rows, from the rescoring
    /// allowance beside the walk's.
    pub(crate) rescore_evaluations: usize,
    pub(crate) allowed_admissions: usize,
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
    origin: usize,
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
    Durable {
        tail: Arc<paged_builder::PagedVectorTail>,
        reader: RefCell<Box<dyn paged_builder::PagedVectorReader>>,
    },
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

struct OpenLayer<'a, 's> {
    graph: LayerGraph<'a>,
    id_to_row: parking_lot::RwLockReadGuard<'a, HashMap<usize, RowId>>,
    /// The rows that share each representative's point (graph-identity
    /// members), by data id.
    members: parking_lot::RwLockReadGuard<'a, super::GraphMembers>,
    /// Each point's own stored version, where the graph records it.
    id_lsn: parking_lot::RwLockReadGuard<'a, HashMap<usize, Lsn>>,
    /// See `HnswUnionLayer::covered_lsn`.
    covered_lsn: Option<Lsn>,
    /// Whether any row of this layer shares another's point. A layer with
    /// none reads no membership at all.
    has_members: bool,
    partition: usize,
    /// This layer's position in the caller's layer list.
    source_layer: usize,
    navigation: &'a NavigationSession,
    /// Where this layer's points start in the statement's reached-point bits,
    /// where each of its levels starts within them, and how many points each
    /// level held when the layer was opened. A point a concurrent insertion
    /// appended since lies outside that view and is not walked.
    first_bit: usize,
    level_ends: Vec<usize>,
    seeds: Vec<PointId>,
    /// The partition's supplied candidate rows, when the caller enumerated
    /// them: every allowed member is one of them, so a point's members are
    /// found by probing each instead of reading the point's whole group.
    supplied: Option<UnionSeedRows<'s>>,
}

impl<'a, 's> OpenLayer<'a, 's> {
    fn bit(&self, point: PointId) -> Option<usize> {
        let index = point.0 as usize;
        let end = *self.level_ends.get(index)?;
        let start = if index == 0 { 0 } else { self.level_ends[index - 1] };
        let rank = usize::try_from(point.1).ok()?;
        if rank >= end.checked_sub(start)? { return None; }
        Some(self.first_bit + start + rank)
    }

    fn entry(&self) -> Option<PointId> {
        match &self.graph {
            LayerGraph::Durable { tail, .. } => tail.progress().graph.entry_point,
            LayerGraph::Paged { paged, .. } => paged.entry_point,
            LayerGraph::ResidentF32 { points, .. } => points.entry_point_id(),
            LayerGraph::ResidentQuantized { points, .. } => points.entry_point_id(),
        }
    }

    /// The client id of one point, or `None` when the layer does not hold it.
    fn origin<E: From<Error>, S: FnMut() -> std::result::Result<(), E>>(
        &self,
        point: PointId,
        before_source: &mut S,
    ) -> std::result::Result<Option<usize>, E> {
        Ok(match &self.graph {
            LayerGraph::Durable { reader, .. } => (|| -> std::result::Result<Option<usize>, E> {
                before_source()?;
                if let Some(cached) = self.navigation.lock().point(self.source_layer, point) {
                    return Ok(Some(cached.origin));
                }
                let value = paged_builder::AdmittedPagedReader::with_reclamation(
                    reader.borrow_mut().as_mut(),
                    self.navigation.owner(),
                    &|| self.navigation.reclaim(),
                )
                .load_point(point)
                .map_err(E::from)?;
                self.navigation
                    .lock()
                    .remember_point(self.source_layer, point, &value);
                Ok(Some(match &*value {
                    paged_builder::PagedVectorPoint::F32(p) => p.origin_id,
                    paged_builder::PagedVectorPoint::Quantized(p) => p.origin_id,
                }))
            })()?,
            LayerGraph::Paged { paged, .. } => paged.point(point).map(|point| point.origin_id),
            LayerGraph::ResidentF32 { points, .. } => points
                .resident_point(&point)
                .map(|point| point.get_origin_id()),
            LayerGraph::ResidentQuantized { points, .. } => points
                .resident_point(&point)
                .map(|point| point.get_origin_id()),
        })
    }
    /// Visit the rows that share `origin`'s point (graph-identity members),
    /// ascending by row id, until `visit` returns `false`. A durable layer
    /// reads them one admitted directory record at a time. With supplied
    /// candidate rows, only those are probed, one seek each, in the caller's
    /// order: the work is bounded by the candidates, never by the group.
    fn for_each_member<E: From<Error>, S: FnMut() -> std::result::Result<(), E>>(
        &self,
        origin: usize,
        before_source: &mut S,
        visit: &mut dyn FnMut(RowId, Lsn, &mut S) -> std::result::Result<bool, E>,
    ) -> std::result::Result<(), E> {
        if !self.has_members {
            return Ok(());
        }
        // A resident point's group is in memory: read it in row order unless
        // the supplied candidates are fewer than its members.
        let probe = match (&self.graph, self.supplied) {
            (LayerGraph::Durable { reader, .. }, Some(_)) => {
                // One directory read decides whether this point has members;
                // only then are the candidates probed.
                before_source()?;
                if paged_builder::AdmittedPagedReader::with_reclamation(
                    reader.borrow_mut().as_mut(),
                    self.navigation.owner(),
                    &|| self.navigation.reclaim(),
                )
                .member_after(origin, None)
                .map_err(E::from)?
                .is_none()
                {
                    return Ok(());
                }
                true
            }
            (LayerGraph::Durable { .. }, None) => false,
            (_, supplied) => {
                let Some(group) = self.members.get(&origin) else {
                    return Ok(());
                };
                match supplied {
                    Some(UnionSeedRows::Slice(rows)) => rows.len() < group.len(),
                    // A caller's iterator names a small restricted set.
                    Some(UnionSeedRows::Iterator(_)) => true,
                    None => false,
                }
            }
        };
        if probe && let Some(supplied) = self.supplied {
            for raw in supplied.iter() {
                before_source()?;
                let row = RowId(raw);
                // A row can share the point in more than one stored version;
                // each is visited, in (row, data id) order.
                let mut after = raw.checked_sub(1).map(|row| (RowId(row), usize::MAX));
                loop {
                    let found = match &self.graph {
                        LayerGraph::Durable { reader, .. } => {
                            paged_builder::AdmittedPagedReader::with_reclamation(
                                reader.borrow_mut().as_mut(),
                                self.navigation.owner(),
                                &|| self.navigation.reclaim(),
                            )
                            .member_after(origin, after)
                            .map_err(E::from)?
                        }
                        _ => self.members.get(&origin).and_then(|group| {
                            let start =
                                after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                            group
                                .range((start, std::ops::Bound::Unbounded))
                                .next()
                                .map(|(&(row, data_id), &lsn)| (row, data_id, lsn))
                        }),
                    };
                    #[cfg(feature = "test-seams")]
                    observe_member_visit();
                    let Some((member, data_id, lsn)) = found.filter(|found| found.0 == row) else {
                        break;
                    };
                    if !visit(member, lsn, before_source)? {
                        return Ok(());
                    }
                    before_source()?;
                    after = Some((member, data_id));
                }
            }
            return Ok(());
        }
        match &self.graph {
            LayerGraph::Durable { reader, .. } => {
                let mut after = None;
                loop {
                    before_source()?;
                    let next = paged_builder::AdmittedPagedReader::with_reclamation(
                        reader.borrow_mut().as_mut(),
                        self.navigation.owner(),
                        &|| self.navigation.reclaim(),
                    )
                    .member_after(origin, after)
                    .map_err(E::from)?;
                    let Some((row, data_id, lsn)) = next else {
                        return Ok(());
                    };
                    #[cfg(feature = "test-seams")]
                    observe_member_visit();
                    if !visit(row, lsn, before_source)? {
                        return Ok(());
                    }
                    after = Some((row, data_id));
                }
            }
            _ => {
                for (&(row, _), &lsn) in self.members.get(&origin).into_iter().flatten() {
                    before_source()?;
                    #[cfg(feature = "test-seams")]
                    observe_member_visit();
                    if !visit(row, lsn, before_source)? {
                        return Ok(());
                    }
                }
                Ok(())
            }
        }
    }

    /// Whether `origin`'s own row or any row sharing its point is allowed.
    fn any_row_allowed<E, S, F>(
        &self,
        origin: usize,
        row: Option<RowId>,
        before_source: &mut S,
        is_allowed: &mut F,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        F: FnMut(usize, RowId) -> bool,
    {
        let partition = self.partition;
        if row.is_some_and(|row| is_allowed(partition, row)) {
            return Ok(true);
        }
        let mut found = false;
        self.for_each_member(origin, before_source, &mut |member, _, _| {
            found = is_allowed(partition, member);
            Ok(!found)
        })?;
        Ok(found)
    }

    fn row<E: From<Error>, S: FnMut() -> std::result::Result<(), E>>(
        &self,
        origin: usize,
        before_source: &mut S,
    ) -> std::result::Result<Option<RowId>, E> {
        match &self.graph {
            LayerGraph::Durable { reader, .. } => (|| {
                before_source()?;
                if let Some(row) = self.navigation.lock().row(self.source_layer, origin) {
                    return Ok(Some(row));
                }
                let identity = paged_builder::AdmittedPagedReader::with_reclamation(
                    reader.borrow_mut().as_mut(),
                    self.navigation.owner(),
                    &|| self.navigation.reclaim(),
                )
                .identity(origin)
                .map_err(E::from)?;
                let row = identity.row_id;
                self.navigation
                    .lock()
                    .remember_identity(self.source_layer, origin, &identity);
                Ok(Some(row))
            })(),
            _ => Ok(self.id_to_row.get(&origin).copied()),
        }
    }

    /// The version `origin`'s own row stores at this point, where the layer
    /// records it; `None` for a decoded sealed point, which is the version
    /// visible at the layer's covered frontier.
    fn row_version<E: From<Error>, S: FnMut() -> std::result::Result<(), E>>(
        &self,
        origin: usize,
        before_source: &mut S,
    ) -> std::result::Result<Option<Lsn>, E> {
        match &self.graph {
            LayerGraph::Durable { reader, .. } => {
                if let Some(lsn) = self
                    .navigation
                    .lock()
                    .row_version(self.source_layer, origin)
                {
                    return Ok(Some(lsn));
                }
                before_source()?;
                let identity = paged_builder::AdmittedPagedReader::with_reclamation(
                    reader.borrow_mut().as_mut(),
                    self.navigation.owner(),
                    &|| self.navigation.reclaim(),
                )
                .identity(origin)
                .map_err(E::from)?;
                self.navigation
                    .lock()
                    .remember_identity(self.source_layer, origin, &identity);
                Ok(Some(identity.lsn))
            }
            _ => Ok(self.id_lsn.get(&origin).copied()),
        }
    }

    /// The neighbours `point` keeps at `level`, or `None` when the layer does
    /// not hold the point or the level.
    fn neighbours<'h, E, A, S>(
        &self,
        point: PointId,
        level: usize,
        scratch: &'h mut HopScratch,
        acquire: &mut A,
        before_source: &mut S,
    ) -> std::result::Result<Option<Hops<'h>>, E>
    where
        'a: 'h,
        E: From<Error>,
        S: FnMut() -> std::result::Result<(), E>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    {
        match &self.graph {
            LayerGraph::Durable { tail, reader } => {
                {
                    // Scratch and the native per-value decode each keep
                    // separate admission while their allocations coexist.
                    let wanted = super::layer0_serving_degree(tail.definition().2.hnsw_m);
                    if wanted > scratch.charged {
                        acquire(HnswSearchScratchEvent::Reserve {
                            container: HnswSearchScratchContainer::Result,
                            previous_capacity: scratch.charged,
                            requested_capacity: wanted,
                            element_bytes: HopScratch::ELEMENT_BYTES,
                        })?;
                        scratch.charged = wanted;
                        scratch.ids.clear();
                        scratch.ids.reserve_exact(wanted);
                    }
                    before_source()?;
                    {
                        let mut navigation = self.navigation.lock();
                        if let Some(cached) = navigation.point(self.source_layer, point) {
                            let Some(edges) = cached.neighbours.get(level) else {
                                return Ok(None);
                            };
                            if edges.len() > wanted {
                                return Err(E::from(Error::Other(
                                    "paged graph degree exceeds its policy".into())));
                            }
                            scratch.ids.clear();
                            scratch.ids.extend_from_slice(edges);
                            return Ok(Some(Hops::Copied(&scratch.ids)));
                        }
                    }
                    let value = paged_builder::AdmittedPagedReader::with_reclamation(
                        reader.borrow_mut().as_mut(),
                        self.navigation.owner(),
                        &|| self.navigation.reclaim(),
                    )
                    .load_point(point)
                    .map_err(E::from)?;
                    self.navigation
                        .lock()
                        .remember_point(self.source_layer, point, &value);
                    let neighbours = match &*value {
                        paged_builder::PagedVectorPoint::F32(p) => &p.neighbours,
                        paged_builder::PagedVectorPoint::Quantized(p) => &p.neighbours,
                    };
                    let Some(edges) = neighbours.get(level) else {
                        return Ok(None);
                    };
                    if edges.len() > wanted {
                        return Err(E::from(Error::Other(
                            "paged graph degree exceeds its policy".into())));
                    }
                    scratch.ids.clear();
                    scratch
                        .ids
                        .extend(edges.iter().map(|edge| (edge.point_id, edge.origin_id)));
                    Ok(Some(Hops::Copied(&scratch.ids)))
                }
            }
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
            LayerGraph::Durable { tail, reader } => (|| {
                before_source_entry()?;
                let cached = {
                    let mut navigation = self.navigation.lock();
                    navigation
                        .point(self.source_layer, point)
                        .and_then(|point| point.values.as_ref())
                        .is_some()
                };
                if cached {
                    before_distance()?;
                    let mut navigation = self.navigation.lock();
                    if let Some(values) = navigation
                        .point(self.source_layer, point)
                        .and_then(|point| point.values.as_ref())
                    {
                        let distance = hnsw_f32_distance().eval(query.values, values);
                        if !distance.is_finite() {
                            return Err(E::from(Error::Other(
                                "invalid paged graph distance".into())));
                        }
                        return Ok(Some(distance));
                    }
                }
                let value = paged_builder::AdmittedPagedReader::with_reclamation(
                    reader.borrow_mut().as_mut(),
                    self.navigation.owner(),
                    &|| self.navigation.reclaim(),
                )
                .load_point(point)
                .map_err(E::from)?;
                self.navigation
                    .lock()
                    .remember_point(self.source_layer, point, &value);
                if !cached {
                    before_distance()?;
                }
                let distance = match &*value {
                    paged_builder::PagedVectorPoint::F32(point) => {
                        hnsw_f32_distance().eval(query.values, &point.values)
                    }
                    paged_builder::PagedVectorPoint::Quantized(point) => DistQuantizedCosine {
                        quantization: tail.definition().1,
                    }
                    .eval(
                        query.encoded.ok_or_else(|| {
                            E::from(Error::Other(
                                "paged quantized graph requires an encoded query".into()))
                        })?,
                        &point.values,
                    ),
                };
                if !distance.is_finite() {
                    return Err(E::from(Error::Other("invalid paged graph distance".into())));
                }
                Ok(Some(distance))
            })(),
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
                    E::from(Error::Other((
                        "a union search opened a quantized layer without an encoded query"
                            .to_string()).into()))
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

#[derive(Default)]
struct PointVisit {
    // An explicit tag avoids Option<f32>'s separate aligned discriminant.
    // Every float bit pattern is preserved, including signed zero and NaN;
    // there is no sentinel value or changed graph/cache behavior.
    score: f32,
    scored: bool,
    marked: bool,
}
impl PointVisit {
    fn distance(&self) -> Option<f32> { self.scored.then_some(self.score) }
    fn cache_distance(&mut self, distance: f32) {
        self.score = distance;
        self.scored = true;
    }
}
const VISIT_ELEMENT_BYTES: usize = std::mem::size_of::<(usize, PointVisit)>() + 1;

/// How a reached point or member stands against the snapshot's version of
/// its row.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RowVersion {
    /// The stored version is the visible one: the graph distance is its score.
    Current,
    /// The row is visible at another version. Its vector may be unchanged
    /// (restamped) or replaced, so it answers at its visible stored score.
    Restamped,
    /// The snapshot sees no version of the row.
    Invisible,
}

/// The distance a reached row answers at, and the visible stored score it was
/// taken from when the graph's distance could not be trusted; `None` when the
/// snapshot sees no version of the row, or when a point of a replaced version
/// reaches a row already kept. The point's graph distance stays its
/// navigation distance either way. A row already kept is not read again.
/// Every visible score read here is a distance evaluation of the walk, paid
/// from its rescoring allowance, declared the same size as its graph
/// allowance; once it is spent the walk stops with its limit, and no score
/// is read.
#[allow(clippy::too_many_arguments)]
fn answer_distance<E, S, D, V>(
    walk: &mut Walk<'_>,
    partition: usize,
    row_id: RowId,
    stored: Option<Lsn>,
    covered: Option<Lsn>,
    graph_distance: f32,
    resolve_score: &mut V,
    before_source_entry: &mut S,
    before_distance: &mut D,
) -> std::result::Result<std::result::Result<Option<(f32, Option<f32>)>, Stop>, E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    V: FnMut(usize, RowId, &mut S, &mut D) -> std::result::Result<Option<f32>, E>,
{
    match walk.version_of(partition, row_id, stored, covered) {
        RowVersion::Current => Ok(Ok(Some((graph_distance, None)))),
        RowVersion::Invisible => Ok(Ok(None)),
        RowVersion::Restamped => {
            // A kept row already competes at a distance its visible version
            // gives it, through that version's own point or a score read
            // before: this point adds nothing to it and reads nothing.
            if walk.kept_rows.contains_key(&(partition, row_id)) {
                return Ok(Ok(None));
            }
            if let Err(stop) = walk.charge_rescore() {
                return Ok(Err(stop));
            }
            let score = resolve_score(partition, row_id, before_source_entry, before_distance)?;
            Ok(Ok(score.map(|score| (1.0 - score, Some(score)))))
        }
    }
}

/// What `Walk::keep` did with one row.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeepOutcome {
    Kept,
    /// The row was already kept at an equal or nearer distance.
    Duplicate,
    /// The heap was full and the row could not displace its worst entry.
    Full,
}

struct Walk<'c> {
    reached: HashMap<usize, PointVisit>,
    visit_bytes: usize,
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
    kept_rows_bytes: usize,
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
    /// Visible stored scores read for rows whose graph score the snapshot
    /// no longer trusts. Their per-index allowance is evaluation_cap, beside
    /// the graph allowance of that size; both also debit the caller's one
    /// remaining request allowance before doing work.
    rescores: usize,
    /// The caller's remaining query allowance, shared by graph evaluations
    /// and stored-score reads independently of their per-index allowances.
    rescore_cap: usize,
    /// Where in-memory layers' neighbour lists are copied: one for the point
    /// being expanded and one for the excluded neighbour it bridges through.
    hops: HopScratch,
    bridges: HopScratch,
    /// The version (LSN) of a row the statement's snapshot sees, if any. A
    /// point or member stores one version of its row; a row replaced since is
    /// answered by its current version's own point, never at this one's
    /// distance. `None` where every stored version is current.
    visible_lsn: Option<&'c mut dyn FnMut(usize, RowId) -> Option<Lsn>>,
}

impl Walk<'_> {
    /// Whether a point or member storing version `stored` of `row` (or, with
    /// `stored` unknown, a sealed point no newer than `covered`) holds the
    /// version the snapshot sees. A version names the row, not its vector: a
    /// scalar update or a late commit restamps an unchanged vector, so a
    /// mismatch only withdraws trust in the graph's distance for that row.
    fn version_of(
        &mut self,
        partition: usize,
        row: RowId,
        stored: Option<Lsn>,
        covered: Option<Lsn>,
    ) -> RowVersion {
        let Some(visible_lsn) = self.visible_lsn.as_mut() else {
            return RowVersion::Current;
        };
        let Some(visible) = visible_lsn(partition, row) else {
            return RowVersion::Invisible;
        };
        let current = match (stored, covered) {
            (Some(stored), _) => visible == stored,
            (None, Some(covered)) => visible <= covered,
            (None, None) => true,
        };
        if current {
            RowVersion::Current
        } else {
            RowVersion::Restamped
        }
    }

    fn visit_entry<E, A, R>(
        &mut self,
        bit: usize,
        acquire: &mut A,
        release: &mut R,
    ) -> std::result::Result<&mut PointVisit, E>
    where
        E: From<Error>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        if !self.reached.contains_key(&bit) && self.reached.len() == self.reached.capacity() {
            let wanted = self.reached.capacity().saturating_mul(2).max(4);
            let buckets = wanted.saturating_mul(8).div_ceil(7).next_power_of_two();
            // Hash-table growth allocates the replacement before freeing the
            // old buckets. Fund both, including the trailing control group.
            let bytes = buckets.checked_mul(VISIT_ELEMENT_BYTES)
                .and_then(|n| n.checked_add(16))
                .ok_or_else(|| E::from(Error::Other("graph visit allocation overflow".into())))?;
            acquire(HnswSearchScratchEvent::Reserve {
                container: HnswSearchScratchContainer::VisitedPoints,
                previous_capacity: 0,
                requested_capacity: bytes,
                element_bytes: 1,
            })?;
            if self.reached.try_reserve(wanted.saturating_sub(self.reached.len())).is_err() {
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::VisitedPoints,
                    capacity: bytes, element_bytes: 1,
                });
                return Err(E::from(Error::Other("cannot allocate admitted graph visit scratch".into())));
            }
            if self.visit_bytes != 0 {
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::VisitedPoints,
                    capacity: self.visit_bytes, element_bytes: 1,
                });
            }
            self.visit_bytes = bytes;
        }
        Ok(self.reached.entry(bit).or_default())
    }
    fn mark<E, A, R>(
        &mut self, bit: usize, acquire: &mut A, release: &mut R,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        let entry = self.visit_entry(bit, acquire, release)?;
        let fresh = !entry.marked;
        entry.marked = true;
        Ok(fresh)
    }

    /// Keep `kept`, reporting whether the heap was full and the row could
    /// not enter it.
    #[cfg(test)]
    fn keep(&mut self, kept: Kept) -> KeepOutcome {
        self.keep_resolved::<Error, _, _>(kept, None, &mut |_| Ok(()), &mut |_| {}).unwrap()
    }

    /// Keep `kept` at its answer distance, recording the visible stored score
    /// that distance was taken from, if it was.
    fn keep_resolved<E, A, R>(&mut self, kept: Kept, resolved: Option<f32>, acquire: &mut A, release: &mut R) -> std::result::Result<KeepOutcome, E>
    where E: From<Error>, A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>, R: FnMut(HnswSearchScratchEvent),
    {
        let key = (kept.partition, kept.row_id);
        let distance = kept.reached.distance;
        let previous_worst = self.kept.peek().map(|entry| entry.0.reached.distance);
        let mut removed_worst = 0usize;
        let mut added_worst = 0usize;
        let existing = self.kept_rows.get(&key).copied();
        // Admissions count logical rows: a kept row reached again, through
        // another layer or another version's point, is not admitted twice.
        if existing.is_none() {
            self.admissions += 1;
        }
        if let Some(existing) = existing {
            if distance >= existing.distance {
                if let Some(score) = resolved
                    && let Some(entry) = self.kept_rows.get_mut(&key)
                {
                    entry.resolved_score = Some(score);
                    entry.score_checked = true;
                }
                return Ok(KeepOutcome::Duplicate);
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
            self.insert_kept_row(
                key,
                KeptRowInfo {
                    distance,
                    resolved_score: resolved.or(existing.and_then(|entry| entry.resolved_score)),
                    score_checked: resolved.is_some() || existing.is_some_and(|entry| entry.score_checked),
                }, acquire, release,
            )?;
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
            self.insert_kept_row(
                key,
                KeptRowInfo {
                    distance,
                    resolved_score: resolved.or(existing.and_then(|entry| entry.resolved_score)),
                    score_checked: resolved.is_some() || existing.is_some_and(|entry| entry.score_checked),
                }, acquire, release,
            )?;
            self.kept.push(Farthest(kept));
            if previous_worst.is_some_and(|worst| distance.total_cmp(&worst) == CmpOrdering::Equal)
            {
                added_worst += 1;
            }
        } else {
            return Ok(KeepOutcome::Full);
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
        Ok(KeepOutcome::Kept)
    }

    fn insert_kept_row<E, A, R>(&mut self, key: (usize, RowId), value: KeptRowInfo,
        acquire: &mut A, release: &mut R) -> std::result::Result<(), E>
    where E: From<Error>, A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>, R: FnMut(HnswSearchScratchEvent),
    {
        if self.kept_rows.len() == self.kept_rows.capacity() && !self.kept_rows.contains_key(&key) {
            let wanted = self.kept_rows.len().saturating_mul(2).max(4);
            let buckets = wanted.saturating_mul(8).div_ceil(7).next_power_of_two();
            let bytes = buckets.checked_mul(std::mem::size_of::<((usize, RowId), KeptRowInfo)>() + 1)
                .and_then(|n| n.checked_add(16))
                .ok_or_else(|| E::from(Error::Other("graph retained-row allocation overflow".into())))?;
            acquire(HnswSearchScratchEvent::Reserve {
                container: HnswSearchScratchContainer::SeenRows,
                previous_capacity: 0, requested_capacity: bytes, element_bytes: 1,
            })?;
            let mut replacement = HashMap::new();
            if replacement.try_reserve(wanted).is_err() {
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::SeenRows, capacity: bytes, element_bytes: 1,
                });
                return Err(E::from(Error::Other("cannot allocate retained-row scratch".into())));
            }
            let old = std::mem::replace(&mut self.kept_rows, replacement);
            self.kept_rows.extend(old);
            // Consuming the old map has physically freed its buckets.
            if self.kept_rows_bytes != 0 {
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::SeenRows,
                    capacity: self.kept_rows_bytes, element_bytes: 1,
                });
            }
            self.kept_rows_bytes = bytes;
        }
        self.kept_rows.insert(key, value);
        Ok(())
    }

    fn push_frontier<E, A, R>(
        &mut self,
        reached: Reached,
        acquire: &mut A,
        release: &mut R,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
        R: FnMut(HnswSearchScratchEvent),
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
                previous_capacity: 0,
                requested_capacity: grown,
                element_bytes: std::mem::size_of::<Nearest>(),
            })?;
            if self.frontier.try_reserve_exact(grown.saturating_sub(self.frontier.len())).is_err() {
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::CandidateHeap,
                    capacity: grown, element_bytes: std::mem::size_of::<Nearest>(),
                });
                return Err(E::from(Error::Other("cannot allocate graph frontier".into())));
            }
            if self.frontier_charged != 0 {
                release(HnswSearchScratchEvent::Release {
                    container: HnswSearchScratchContainer::CandidateHeap,
                    capacity: self.frontier_charged, element_bytes: std::mem::size_of::<Nearest>(),
                });
            }
            self.frontier_charged = grown;
        }
        self.frontier.push(Nearest(reached));
        Ok(())
    }

    #[cfg(test)]
    fn frontier_is_farther_than_kept(&self, candidate: Reached, required_results: usize) -> bool {
        self.frontier_ends_walk(candidate, required_results, 0.0, |_| 0)
    }

    /// Whether `candidate` ends the walk. A head farther than the beam's edge
    /// distance by more than `search_radius` (a fraction of that distance; 0
    /// ends the walk at the edge) ends it. A frontier row's ID
    /// says nothing about the IDs of its undiscovered neighbours, so a head
    /// tied with other kept rows at the edge ends it only when enough rows
    /// are strictly nearer than that whole tied plateau: kept here, or
    /// already returned by the partitions searched before this one
    /// (`settled_nearer`), since the page is merged across them. A head that
    /// is itself the edge's only row is inside any positive radius.
    fn frontier_ends_walk(
        &self,
        candidate: Reached,
        required_results: usize,
        search_radius: f32,
        settled_nearer: impl FnOnce(f32) -> usize,
    ) -> bool {
        self.kept.peek().is_some_and(|worst| {
            let edge = worst.0.reached.distance;
            match self.compare_stop_distance(candidate.distance, edge) {
                CmpOrdering::Greater => {
                    search_radius <= 0.0 || candidate.distance > edge + edge.abs() * search_radius
                }
                CmpOrdering::Equal => {
                    let only_edge_row = self.worst_ties == 1
                        && worst.0.row_id == candidate.row_id
                        && worst.0.reached.layer == candidate.layer;
                    if search_radius > 0.0 && only_edge_row {
                        return false;
                    }
                    let nearer = self.kept.len() - self.worst_ties;
                    nearer >= required_results
                        || nearer.saturating_add(settled_nearer(candidate.distance))
                            >= required_results
                }
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

    /// Admit one stored-score read inside both its per-index allowance and
    /// the request remainder shared with graph evaluations.
    fn charge_rescore(&mut self) -> std::result::Result<(), Stop> {
        if self.rescores >= self.evaluation_cap
            || self.rescores >= self.rescore_cap.saturating_sub(self.evaluations)
        {
            return Err(Stop::Limit(
                HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
            ));
        }
        self.rescores += 1;
        Ok(())
    }

    fn charge(&mut self) -> std::result::Result<(), Stop> {
        if self.visited >= self.visit_cap {
            return Err(Stop::Limit(HnswAllowedSearchIncomplete::VisitLimitReached));
        }
        if self.evaluations >= self.evaluation_cap
            || self.evaluations >= self.rescore_cap.saturating_sub(self.rescores)
        {
            return Err(Stop::Limit(
                HnswAllowedSearchIncomplete::VectorEvaluationLimitReached,
            ));
        }
        Ok(())
    }
}

impl HnswIndex {
    /// The most neighbours a point of this generation keeps on its bottom
    /// layer: the links neighbour selection kept plus the links reachability
    /// owes (`reachability::LAYER0_SLACK`). A walk evaluates at most this many
    /// neighbours of each point it expands there, and its work is priced by
    /// it. It is baked into the graph when it is built, not chosen per query.
    pub(crate) fn layer0_max_degree(&self) -> usize {
        match &self.hnsw {
            HnswInner::F32Mutable(graph) => graph
                .get_max_nb_connection_layer0()
                .saturating_add(super::reachability::LAYER0_SLACK),
            HnswInner::F32Sealed(graph) => graph
                .get_max_nb_connection_layer0()
                .saturating_add(super::reachability::LAYER0_SLACK),
            HnswInner::QuantizedMutable(graph) => graph
                .get_max_nb_connection_layer0()
                .saturating_add(super::reachability::LAYER0_SLACK),
            HnswInner::QuantizedSealed(graph) => graph
                .get_max_nb_connection_layer0()
                .saturating_add(super::reachability::LAYER0_SLACK),
            HnswInner::F32Paged(graph) => graph.layer0_max_degree,
            HnswInner::DurablePages(_) => super::layer0_serving_degree(self.m),
        }
    }
}

/// The candidate budget one statement spends: `cap(ef, k) x M_max0_serving`,
/// the resolved breadth's cap times the widest admitted serving graph's
/// layer-0 maximum degree -- the most neighbours a walk evaluates per point it
/// expands, read from the graphs, never a scaled declared degree. It is one
/// number for the whole search and never multiplied by the number of
/// partitions or layers it admits.
/// AUTO's up-front routing condition (not a proof; the measured gate rows
/// are): graph work is attempted only when its whole cap is below the rows
/// EXACT would score -- at most half of them across several layers, so a
/// cap hit followed by EXACT stays bounded -- and every layer can run its
/// k-floor beam inside the cap.
pub(crate) fn graph_work_cannot_beat_exact(
    cap: usize,
    rows: usize,
    layers: usize,
    k: usize,
    degree: usize,
) -> bool {
    // Only the structural case routes: every search's k-floor beam cannot
    // fit the cap. The cap is an upper bound on work, not the work a search
    // spends, so comparing it with the row count would send searches the dev
    // AUTO contract answers from the graph to EXACT; the measured gate rows
    // (multi-Context p95 <= EXACT, single-Context mean below EXACT) decide.
    let _ = rows;
    layers > 1 && layers.saturating_mul(k).saturating_mul(degree.max(1)) > cap
}

/// Run each partition's own search (its base, change and tail together, as
/// one partition's layers always were) in turn against its declared evaluation
/// cap and merge their kept rows by distance. A layer search that stops on a limit
/// (the cap included) makes the union Incomplete; a refused layer refuses it.
#[cfg(test)]
pub(crate) fn merge_layer_searches<E>(
    layers: usize,
    cap: usize,
    k: usize,
    mut search_layer: impl FnMut(usize, usize, usize) -> std::result::Result<HnswUnionSearchResult, E>,
) -> std::result::Result<HnswUnionSearchResult, E> {
    merge_layer_searches_with_settled(layers, cap, k, |position, remaining, spent, _| {
        search_layer(position, remaining, spent)
    })
}

/// As [`merge_layer_searches`], handing each partition's search the rows the
/// partitions before it returned, so a stop proof can count them.
#[cfg(test)]
pub(crate) fn merge_layer_searches_with_settled<E>(
    layers: usize,
    cap: usize,
    k: usize,
    search_layer: impl FnMut(
        usize,
        usize,
        usize,
        &mut [HnswUnionCandidate],
    ) -> std::result::Result<HnswUnionSearchResult, E>,
) -> std::result::Result<HnswUnionSearchResult, E> {
    // These existing fixtures have no executing reader or merge admission
    // callback. Production uses the custody boundary below with its reader.
    merge_layer_searches_with_custody(
        layers,
        cap,
        k,
        search_layer,
        |all, incoming| {
            let previous = all
                .candidates
                .capacity()
                .saturating_mul(std::mem::size_of::<HnswUnionCandidate>());
            all.candidates.reserve(incoming.len());
            all.retained_bytes = all
                .retained_bytes
                .saturating_sub(previous)
                .saturating_add(
                    all.candidates
                        .capacity()
                        .saturating_mul(std::mem::size_of::<HnswUnionCandidate>()),
                );
            Ok(())
        },
        |_| {},
    )
}

/// Keep both partition results charged until their candidate copy completes.
/// The caller supplies its existing reader's fallible growth operation; on
/// success it updates `all.retained_bytes` for the actual destination capacity.
/// On failure it leaves the original destination and its charge intact.
pub(crate) fn merge_layer_searches_with_custody<E>(
    layers: usize,
    cap: usize,
    k: usize,
    search_layer: impl FnMut(
        usize,
        usize,
        usize,
        &mut [HnswUnionCandidate],
    ) -> std::result::Result<HnswUnionSearchResult, E>,
    grow_candidates: impl FnMut(
        &mut HnswUnionSearchResult,
        &[HnswUnionCandidate],
    ) -> std::result::Result<(), E>,
    release_retained: impl FnMut(usize),
) -> std::result::Result<HnswUnionSearchResult, E> {
    merge_layer_searches_with_candidate_limit(layers, cap, k, usize::MAX,
        search_layer, grow_candidates, release_retained)
}

/// Each partition still searches independently. The merged statement retains
/// the already-resolved EF candidate pool, rather than multiplying that pool
/// by the number of selected partitions. Both incoming buffers stay funded.
pub(crate) fn merge_layer_searches_with_candidate_limit<E>(
    layers: usize,
    cap: usize,
    k: usize,
    candidate_limit: usize,
    mut search_layer: impl FnMut(
        usize,
        usize,
        usize,
        &mut [HnswUnionCandidate],
    ) -> std::result::Result<HnswUnionSearchResult, E>,
    mut grow_candidates: impl FnMut(
        &mut HnswUnionSearchResult,
        &[HnswUnionCandidate],
    ) -> std::result::Result<(), E>,
    mut release_retained: impl FnMut(usize),
) -> std::result::Result<HnswUnionSearchResult, E> {
    let mut merged: Option<HnswUnionSearchResult> = None;
    let mut spent = 0usize;
    for position in 0..layers {
        let remaining = cap;
        let settled = merged
            .as_mut()
            .map_or(&mut [][..], |merged| merged.candidates.as_mut_slice());
        let one = match search_layer(position, remaining, spent, settled) {
            Ok(one) => one,
            Err(error) => {
                if let Some(all) = merged.take() {
                    let bytes = all.retained_bytes;
                    drop(all);
                    release_retained(bytes);
                }
                return Err(error);
            }
        };
        spent = spent
            .saturating_add(one.admission_evaluations)
            .saturating_add(one.expansion_evaluations)
            .saturating_add(one.rescore_evaluations);
        let stop = !matches!(one.status, HnswAllowedSearchStatus::Complete);
        merged = Some(match merged.take() {
            None => one,
            Some(mut all) => {
                let mut one = one;
                if let Err(error) = grow_candidates(&mut all, &one.candidates) {
                    let all_bytes = all.retained_bytes;
                    let one_bytes = one.retained_bytes;
                    drop(all);
                    release_retained(all_bytes);
                    drop(one);
                    release_retained(one_bytes);
                    return Err(error);
                }
                // Growth has admitted the whole replacement while both old
                // buffers still exist. The incoming allocation stays charged
                // until after this copy and its physical drop below.
                all.candidates.extend_from_slice(&one.candidates);
                // A caller that needs the reports takes them out of each
                // walk before the merge; any still here move without growth.
                if all.disconnected.is_empty() {
                    let transferred_bytes = one
                        .disconnected
                        .capacity()
                        .saturating_mul(std::mem::size_of::<usize>());
                    let old = std::mem::replace(
                        &mut all.disconnected,
                        std::mem::take(&mut one.disconnected),
                    );
                    let old_bytes = old.capacity().saturating_mul(std::mem::size_of::<usize>());
                    drop(old);
                    release_retained(old_bytes);
                    all.retained_bytes = all
                        .retained_bytes
                        .saturating_sub(old_bytes)
                        .saturating_add(transferred_bytes);
                    one.retained_bytes = one.retained_bytes.saturating_sub(transferred_bytes);
                } else {
                    debug_assert!(one.disconnected.is_empty());
                }
                all.admission_evaluations += one.admission_evaluations;
                all.expansion_evaluations += one.expansion_evaluations;
                all.rescore_evaluations += one.rescore_evaluations;
                all.allowed_admissions += one.allowed_admissions;
                all.layers_admitted += one.layers_admitted;
                all.frontier_exhausted &= one.frontier_exhausted;
                if all.below_admission_floor.is_none() {
                    all.below_admission_floor = one.below_admission_floor;
                }
                if stop {
                    all.status = one.status;
                }
                if one.terminal == UnionTerminal::Refused {
                    all.terminal = UnionTerminal::Refused;
                }
                let one_bytes = one.retained_bytes;
                drop(one);
                release_retained(one_bytes);
                all
            }
        });
        if let Some(all) = merged.as_mut()
            && all.candidates.len() > candidate_limit
        {
            // Resolve score ties by the public row-id ordering across scopes.
            all.candidates.select_nth_unstable_by(candidate_limit, |left, right| {
                left.distance.total_cmp(&right.distance)
                    .then(left.row_id.cmp(&right.row_id))
                    .then(left.partition.cmp(&right.partition))
            });
            all.candidates.truncate(candidate_limit);
            // The actual capacity still exists and keeps its original invoice.
        }
        if stop {
            break;
        }
    }
    let mut all = merged.unwrap_or_else(|| HnswUnionSearchResult {
        terminal: UnionTerminal::FrontierExhausted,
        candidates: Vec::new(),
        disconnected: Vec::new(),
        retained_bytes: 0,
        status: HnswAllowedSearchStatus::Complete,
        below_admission_floor: None,
        budget: cap,
        admission_evaluations: 0,
        expansion_evaluations: 0,
        rescore_evaluations: 0,
        allowed_admissions: 0,
        #[cfg(feature = "test-seams")]
        expansion_cap: cap,
        layers_admitted: 0,
        frontier_exhausted: true,
    });
    all.budget = cap.saturating_mul(layers);
    // Nearest first across layers, ties by partition then row, exactly as
    // one walk's kept rows are ordered.
    sort_merged_candidates(&mut all.candidates);
    if all.terminal != UnionTerminal::Refused {
        all.terminal = if matches!(all.status, HnswAllowedSearchStatus::Complete)
            && all.candidates.len() >= k
        {
            UnionTerminal::AnswerComplete
        } else if matches!(all.status, HnswAllowedSearchStatus::Complete) {
            UnionTerminal::FrontierExhausted
        } else {
            UnionTerminal::Refused
        };
    }
    Ok(all)
}

/// Stable in-place merge sort. The comparator is unchanged, including full
/// ties: rotations retain each run's order, lower-bound on the right keeps
/// equal left elements first, and upper-bound on the left does the same for
/// a right pivot. No heap scratch or per-element identity is needed.
fn sort_merged_candidates(values: &mut [HnswUnionCandidate]) {
    fn compare(left: &HnswUnionCandidate, right: &HnswUnionCandidate) -> CmpOrdering {
        left.distance
            .total_cmp(&right.distance)
            .then(left.partition.cmp(&right.partition))
            .then(left.row_id.cmp(&right.row_id))
    }
    fn merge(values: &mut [HnswUnionCandidate], middle: usize) {
        if middle == 0 || middle == values.len()
            || compare(&values[middle - 1], &values[middle]) != CmpOrdering::Greater
        {
            return;
        }
        if values.len() == 2 {
            values.swap(0, 1);
            return;
        }
        let (left_cut, right_cut) = if middle > values.len() - middle {
            let left_cut = middle / 2;
            let right_cut = middle + values[middle..].partition_point(|value| {
                compare(value, &values[left_cut]) == CmpOrdering::Less
            });
            (left_cut, right_cut)
        } else {
            let right_cut = middle + (values.len() - middle) / 2;
            let left_cut = values[..middle].partition_point(|value| {
                compare(value, &values[right_cut]) != CmpOrdering::Greater
            });
            (left_cut, right_cut)
        };
        let next_middle = left_cut + (right_cut - middle);
        values[left_cut..right_cut].rotate_left(middle - left_cut);
        let (left, right) = values.split_at_mut(next_middle);
        merge(left, left_cut);
        merge(right, middle - left_cut);
    }
    let mut width = 1usize;
    while width < values.len() {
        let mut start = 0usize;
        while start < values.len() {
            let middle = start.saturating_add(width).min(values.len());
            let end = middle.saturating_add(width).min(values.len());
            merge(&mut values[start..end], middle - start);
            start = end;
        }
        width = width.saturating_mul(2);
    }
}

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
        HnswInner::DurablePages(handle) => {
            let tail = Arc::clone(&handle.read());
            if tail.progress().graph.entry_point.is_none() {
                return Ok(None);
            }
            let levels = tail.progress().graph.layer_counts.to_vec();
            let reader = RefCell::new(tail.graph_reader().map_err(E::from)?);
            (LayerGraph::Durable { tail, reader }, levels, graph.m)
        }
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
    is_allowed: F,
    resolve_score: V,
    before_source_entry: S,
    before_distance: D,
    acquire: A,
    release: R,
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
    search_union_allowed_bounded_with_navigation(
        layers,
        query,
        k,
        ef_search,
        budget,
        limits,
        graph_scores_are_current,
        can_certify_resolved_scores,
        query_self_scores,
        is_allowed,
        resolve_score,
        before_source_entry,
        before_distance,
        acquire,
        release,
        None,
        0.0,
        &mut [],
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn search_union_allowed_bounded_with_navigation<E, S, D, A, R, F, V>(
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
    navigation_session: Option<&NavigationSession>,
    search_radius: f32,
    settled: &mut [HnswUnionCandidate],
    visible_lsn: Option<&mut dyn FnMut(usize, RowId) -> Option<Lsn>>,
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
    #[cfg(feature = "test-seams")]
    let search_radius = search_radius_override().unwrap_or(search_radius);
    let fallback = NavigationSession::new(None);
    let navigation = navigation_session.unwrap_or(&fallback);
    let mut total_allowed = 0usize;
    let mut total_live = 0usize;
    let mut counted_partitions = VectorSearchScratch {
        acquire: &mut acquire,
        release: &mut release,
    }
    .allocate_vec::<usize>(
        HnswSearchScratchContainer::ServingPageDirectory,
        layers.len(),
        "union counted partitions",
    )?;
    for layer in layers {
        if !counted_partitions.contains(&layer.partition) {
            counted_partitions.push(layer.partition);
            total_allowed = total_allowed.saturating_add(layer.allowed_count);
            total_live = total_live.saturating_add(if layer.allowed_count == ALLOWED_UNKNOWN {
                layer.live_count
            } else {
                layer.live_count.max(layer.allowed_count)
            });
        }
    }
    VectorSearchScratch {
        acquire: &mut acquire,
        release: &mut release,
    }
    .free_vec(counted_partitions, HnswSearchScratchContainer::ServingPageDirectory);
    // A filter checked row by row has no allowed count: the walk needs `k`
    // rows, stops only by its proof, its budget or its frontier, and never
    // compares what it reached with a count it does not have.
    let allowed_known = total_allowed != ALLOWED_UNKNOWN;
    let required_results = k.min(total_allowed);
    let ef = hnsw_search_ef(ef_search, required_results);
    let empty = |status| HnswUnionSearchResult {
        terminal: if status == HnswAllowedSearchStatus::Complete {
            UnionTerminal::FrontierExhausted
        } else {
            UnionTerminal::Refused
        },
        candidates: Vec::new(),
        disconnected: Vec::new(),
        retained_bytes: 0,
        status,
        below_admission_floor: None,
        budget,
        admission_evaluations: 0,
        expansion_evaluations: 0,
        rescore_evaluations: 0,
        allowed_admissions: 0,
        #[cfg(feature = "test-seams")]
        expansion_cap: 0,
        layers_admitted: 0,
        frontier_exhausted: true,
    };
    if required_results == 0 {
        return Ok(empty(HnswAllowedSearchStatus::Complete));
    }

    // One cumulative sixteen-level directory supplies both offsets and
    // lengths. Its existing allocation is transformed in place, with no
    // duplicate directory or uncharged replacement during layer opening.
    let open_element_bytes =
        std::mem::size_of::<OpenLayer<'_, '_>>() + 16 * std::mem::size_of::<usize>();
    acquire(HnswSearchScratchEvent::Reserve {
        container: HnswSearchScratchContainer::ServingPageDirectory,
        previous_capacity: 0,
        requested_capacity: layers.len(),
        element_bytes: open_element_bytes,
    })?;
    // Open every layer: how it is read, its identity map, its seed directory.
    let mut open = Vec::<OpenLayer<'_, '_>>::with_capacity(layers.len());
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
            let seed_rows = || {
                layer.seed_rows.map_or_else(
                    || Box::new(std::iter::empty()) as Box<dyn Iterator<Item = u64>>,
                    |rows| rows.iter(),
                )
            };
            if matches!(layer.graph.hnsw, HnswInner::F32Paged(ref paged) if paged.entry_point.is_none())
            {
                continue;
            }
            let Some((graph, level_lens, degree)) =
                layer_graph(layer.graph, &mut acquire, &mut directory_entries)?
            else {
                continue;
            };
            let directory = match &graph {
                LayerGraph::Durable { tail, .. } => layer.graph.paged_seed_directory(
                    tail,
                    Some(navigation),
                    k,
                    required_results.min(layer.allowed_count),
                    layer.allowed_count,
                    ef,
                    &seed_rows,
                    seed_all,
                    &mut layer_allowed,
                    &mut before_source_entry,
                    &mut acquire,
                    &mut release,
                )?,
                _ => layer.graph.allowed_seed_directory(
                    k,
                    required_results.min(layer.allowed_count),
                    layer.allowed_count,
                    ef,
                    &seed_rows,
                    seed_all,
                    &mut layer_allowed,
                    &mut before_source_entry,
                    &mut acquire,
                    &mut release,
                )?,
            };
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
            if (matches!(graph, LayerGraph::ResidentQuantized { .. })
                || matches!(graph, LayerGraph::Durable { .. })
                    && layer.graph.quantization != VectorQuantization::F32)
                && encoded_query.is_none()
            {
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
            let mut level_ends = level_lens;
            let mut level_start = 0usize;
            for end in &mut level_ends {
                level_start = level_start.saturating_add(*end);
                *end = level_start;
            }
            max_degree = max_degree.max(degree);
            // One order for the graph's guards, the order an insertion takes
            // them in: the row map, then the memberships, then the stored
            // versions. Whether the layer has members is read before its
            // member guard is held.
            let id_to_row = layer.graph.id_to_row.read();
            let has_members = layer.graph.has_graph_members();
            let members = layer.graph.members.read();
            let id_lsn = layer.graph.id_lsn.read();
            open.push(OpenLayer {
                navigation,
                graph,
                id_to_row,
                members,
                id_lsn,
                covered_lsn: layer.covered_lsn,
                has_members,
                partition,
                source_layer,
                first_bit: total_points,
                level_ends,
                seeds: directory.points,
                supplied: layer.seed_rows.filter(|_| directory.supplied_seed_rows),
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
    fn release_encoded<E, A, R>(acquire: &mut A, release: &mut R, encoded: Option<Vec<u8>>)
    where
        E: From<Error>,
        A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
        R: FnMut(HnswSearchScratchEvent),
    {
        if let Some(encoded) = encoded {
            VectorSearchScratch { acquire, release }
                .free_vec(encoded, HnswSearchScratchContainer::QueryBytes);
        }
    }
    if let Err(error) = opened {
        drop(open);
        release(HnswSearchScratchEvent::Release {
            container: HnswSearchScratchContainer::ServingPageDirectory,
            capacity: layers.len(),
            element_bytes: open_element_bytes,
        });
        release_encoded(&mut acquire, &mut release, encoded_query);
        release_directories(&mut release, directory_entries);
        release_seeds(&mut release, seed_bytes);
        return Err(error);
    }
    if open.is_empty() {
        drop(open);
        release(HnswSearchScratchEvent::Release {
            container: HnswSearchScratchContainer::ServingPageDirectory,
            capacity: layers.len(),
            element_bytes: open_element_bytes,
        });
        release_encoded(&mut acquire, &mut release, encoded_query);
        release_directories(&mut release, directory_entries);
        release_seeds(&mut release, seed_bytes);
        return Ok(empty(HnswAllowedSearchStatus::Incomplete(
            HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
        )));
    }

    // Opening and expanding the layers share the declared per-index cap,
    // additionally bounded by the request's remaining logical work.
    let evaluation_cap = budget.min(limits.max_vector_evaluations);
    // Every frontier entry is one evaluation, so the frontier never outgrows
    // what the budget can pay for, however many partitions were admitted.
    let frontier_capacity = total_points
        .min(budget.min(evaluation_cap).saturating_add(1))
        .max(1);
    // The return heap holds rows, not points: rows that share a point each
    // take a place, so it is bounded by the rows the scope holds.
    let kept_capacity = ef
        .max(required_results)
        .min(total_points.max(total_live))
        .max(1);
    let mut reservations = [
        (
            HnswSearchScratchContainer::ReturnHeap,
            kept_capacity,
            std::mem::size_of::<Farthest>(),
        ),
        // One map entry per kept row, with the table's own control bytes and
        // its load-factor headroom.
        (
            HnswSearchScratchContainer::SeenRows,
            kept_capacity
                .saturating_mul(8)
                .div_ceil(7)
                .next_power_of_two()
                .saturating_mul(std::mem::size_of::<((usize, RowId), KeptRowInfo)>() + 1)
                .saturating_add(16),
            1,
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
            release(HnswSearchScratchEvent::Release {
                container: HnswSearchScratchContainer::ServingPageDirectory,
                capacity: layers.len(),
                element_bytes: open_element_bytes,
            });
            release_encoded(&mut acquire, &mut release, encoded_query);
            release_directories(&mut release, directory_entries);
            release_seeds(&mut release, seed_bytes);
            return Err(error);
        }
    }
    let mut walk = Walk {
        reached: HashMap::new(),
        visit_bytes: 0,
        frontier: BinaryHeap::new(),
        frontier_capacity,
        frontier_charged: 0,
        kept: BinaryHeap::with_capacity(kept_capacity),
        kept_capacity,
        kept_rows: HashMap::with_capacity(kept_capacity),
        kept_rows_bytes: reservations[1].1,
        worst_ties: 0,
        failed_resolved_stop_distance: None,
        #[cfg(test)]
        stop_distance_comparisons: std::cell::Cell::new(0),
        evaluations: 0,
        visited: 0,
        admissions: 0,
        evaluation_cap,
        visit_cap: limits.max_visited_nodes,
        rescores: 0,
        rescore_cap: limits.max_vector_evaluations,
        hops: HopScratch::default(),
        bridges: HopScratch::default(),
        visible_lsn,
    };
    let union_query = UnionQuery {
        values: query,
        right_norm: PagedHnsw::query_norm(query),
        encoded: encoded_query.as_deref(),
    };

    #[cfg(feature = "test-seams")]
    trace_begin(open[0].partition);
    // Partitions whose live rows the walk's graph does not connect, charged
    // as a result container and handed back with the candidates.
    let mut disconnected = Vec::<usize>::new();
    let searched = (|| -> std::result::Result<HnswUnionSearchResult, E> {
        let mut admission_evaluations = 0usize;
        let mut expansion_evaluations = 0usize;
        #[cfg(feature = "test-seams")]
        let mut expansion_cap = None;
        let mut layers_admitted = 0usize;
        let mut below_floor = None;
        let mut frontier_exhausted = false;
        let mut outcome = None;
        // Every opened layer holds its row map for the whole walk, so an
        // insertion -- its reciprocal pruning and the reachability links put
        // back after it -- publishes before the walk opens or after it ends,
        // never during it.
        #[cfg(feature = "test-seams")]
        crate::test_seam::maybe_pause_walk_open();
        for allow_bridges in [false, true] {
            if allow_bridges {
                // The admitted rows did not reach each other. Walk again,
                // navigating through excluded points, inside what is left of
                // the same budget.
                walk.reached
                    .values_mut()
                    .for_each(|entry| entry.marked = false);
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
                &mut resolve_score,
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
            // Admission and both expansion passes spend the same per-index
            // cap. Advancing the phase must not replenish its spent credit.
            #[cfg(feature = "test-seams")]
            expansion_cap.get_or_insert(
                walk.evaluation_cap.saturating_sub(before_expansion).min(
                    walk.rescore_cap
                        .saturating_sub(before_expansion)
                        .saturating_sub(walk.rescores),
                ),
            );
            let expanded = expand(
                &mut open,
                &mut walk,
                &union_query,
                total_allowed,
                required_results,
                graph_scores_are_current,
                can_certify_resolved_scores,
                search_radius,
                settled,
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
            // A walk whose frontier ran out reached every row its graph
            // connects: an unfiltered walk on its first pass, a filtered one on
            // its second, which also crosses rows the filter excludes. A
            // partition with fewer distinct allowed rows reached than its
            // snapshot allows has live rows its graph does not connect; which
            // ones depends on how the graph was built, so it is not answered
            // from the graph. Rows, not points: a reached row's superseded
            // versions, and the rows sharing its point, are counted once each.
            // A walk that stops by its proof, or by its budget, is not such a
            // walk: the first leaves at most an ordinary recall miss, the
            // second is refused under INDEXED.
            if allowed_known
                && (allow_bridges || total_allowed >= total_live)
                && !expanded.converged
            {
                disconnected_partitions(
                    &open,
                    &walk,
                    layers,
                    &mut disconnected,
                    &mut is_allowed,
                    &mut before_source_entry,
                    &mut acquire,
                    &mut release,
                )?;
                if !disconnected.is_empty() {
                    outcome = Some(HnswAllowedSearchStatus::Complete);
                    break;
                }
            }
            let fragmented = total_allowed.saturating_mul(max_degree.max(1)) < total_points;
            // The second pass is for rows a filter rejected, counted in
            // logical rows: a superseded version of a row the snapshot sees is
            // that same row reached twice, not a rejection, however many graph
            // points the layers hold for it.
            // With no allowed count, a first pass whose frontier ran out may
            // have been starved by rows the filter rejected: the second pass
            // crosses them.
            let starved = if allowed_known {
                !allow_bridges
                    && walk.admissions < total_allowed
                    && total_allowed < total_live
                    && (!expanded.converged || fragmented)
            } else {
                !allow_bridges && !expanded.converged
            };
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
        let mut status = outcome.unwrap_or(HnswAllowedSearchStatus::Complete);
        let mut terminal = match status {
            HnswAllowedSearchStatus::Incomplete(_) => UnionTerminal::Refused,
            // Every connected row was reached; the caller compares the rest.
            HnswAllowedSearchStatus::Complete if !disconnected.is_empty() => {
                UnionTerminal::FrontierExhausted
            }
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
        let equal_vector_filed = (|| -> std::result::Result<bool, E> {
            let mut equal_vector_filed = false;
            if let Some(key) = key {
                for open_layer in &open {
                    if let LayerGraph::Durable { reader, .. } = &open_layer.graph {
                        before_source_entry()?;
                        if paged_builder::AdmittedPagedReader::with_reclamation(
                            reader.borrow_mut().as_mut(),
                            navigation.owner(),
                            &|| navigation.reclaim(),
                        )
                        .exact_graph_row_after(key, None)
                        .map_err(E::from)?
                        .is_some()
                        {
                            equal_vector_filed = true;
                            break;
                        }
                    } else {
                        let graph = layers[open_layer.source_layer].graph;
                        let mut hasher = std::collections::hash_map::DefaultHasher::new();
                        key.hash(&mut hasher);
                        let fingerprint = hasher.finish().to_le_bytes();
                        let lookup = if matches!(graph.hnsw, HnswInner::F32Paged(_)) {
                            fingerprint.as_slice()
                        } else {
                            key
                        };
                        if graph
                            .exact_rows
                            .read()
                            .get(lookup)
                            .is_some_and(|bucket| !bucket.is_empty())
                        {
                            equal_vector_filed = true;
                            break;
                        }
                    }
                }
            }
            Ok(equal_vector_filed)
        })();
        let equal_vector_filed = match equal_vector_filed {
            Ok(found) => found,
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
        };
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
            Some(_) => match scratch.allocate_vec::<(usize, RowId, Option<Option<f32>>) >(
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
                if let Some(open_layer) = open.iter().find(|opened| opened.source_layer == position)
                    && let LayerGraph::Durable { reader, .. } = &open_layer.graph
                {
                    let mut after = None;
                    loop {
                        before_source_entry()?;
                        let Some(row_id) = paged_builder::AdmittedPagedReader::with_reclamation(
                            reader.borrow_mut().as_mut(),
                            navigation.owner(),
                            &|| navigation.reclaim(),
                        )
                        .exact_graph_row_after(key, after)
                        .map_err(E::from)?
                        else {
                            break;
                        };
                        if after.is_some_and(|last| row_id <= last) {
                            return Err(E::from(Error::Other(
                                "paged exact directory did not advance".into())));
                        }
                        after = Some(row_id);
                        if layer.seed_rows.is_some_and(|rows| !rows.contains(row_id.0))
                            || !is_allowed(layer.partition, row_id)
                        {
                            continue;
                        }
                        if ties.len() < kept_capacity {
                            ties.push((position, row_id, None))
                        } else if let Some((worst, worst_id)) = ties
                            .iter()
                            .enumerate()
                            .max_by_key(|(_, (_, row, _))| *row)
                            .map(|(position, (_, row, _))| (position, *row))
                            && row_id < worst_id
                        {
                            ties[worst] = (position, row_id, None)
                        }
                        // Directory is ordered; this layer cannot improve its
                        // retained exact ties after the global heap is full.
                        if ties.len() == kept_capacity
                            && ties
                                .iter()
                                .map(|(_, row, _)| *row)
                                .max()
                                .is_some_and(|worst| row_id >= worst)
                        {
                            break;
                        }
                    }
                    continue;
                }
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
                                if layer.seed_rows.is_some_and(|rows| !rows.contains(row_id.0))
                                    || !is_allowed(layer.partition, row_id)
                                {
                                    return Ok(false);
                                }
                                if ties.len() < kept_capacity {
                                    ties.push((position, row_id, None));
                                } else if let Some((worst, worst_id)) = ties
                                    .iter()
                                    .enumerate()
                                    .max_by_key(|(_, (_, kept_row, _))| *kept_row)
                                    .map(|(position, (_, kept_row, _))| (position, *kept_row))
                                    && row_id < worst_id
                                {
                                    ties[worst] = (position, row_id, None);
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
                // With supplied candidates, each is probed in the sorted
                // bucket: the work is bounded by the candidates, never by the
                // rows (or retained history) that store this vector.
                let probed = layer.seed_rows.map(|rows| {
                    rows.iter()
                        .map(RowId)
                        .filter(|row| bucket.binary_search(row).is_ok())
                });
                let mut supplied = probed.into_iter().flatten();
                let mut stored = bucket.iter().copied();
                loop {
                    let next = if layer.seed_rows.is_some() {
                        supplied.next()
                    } else {
                        stored.next()
                    };
                    let Some(row_id) = next else {
                        break;
                    };
                    let row_id = &row_id;
                    before_source_entry()?;
                    if is_allowed(layer.partition, *row_id) {
                        if ties.len() < kept_capacity {
                            ties.push((position, *row_id, None));
                        } else if let Some((worst, worst_id)) = ties
                            .iter()
                            .enumerate()
                            .max_by_key(|(_, (_, kept_row, _))| *kept_row)
                            .map(|(position, (_, kept_row, _))| (position, *kept_row))
                            && *row_id < worst_id
                        {
                            ties[worst] = (position, *row_id, None);
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
        ties.sort_unstable_by_key(|(_, row_id, _)| *row_id);
        // A row filed under the query's vector may be filed at a version the
        // snapshot no longer sees. Where versions are tracked, a row is a tie
        // only while its visible stored score is the query's own score;
        // otherwise it answers through the walk, at its visible score.
        if walk.visible_lsn.is_some()
            && let Some(exact_score) = exact_score
        {
            let mut retained = Ok(());
            let mut index = 0usize;
            while index < ties.len() {
                let (position, row_id, _) = ties[index];
                if let Err(error) = before_source_entry() {
                    retained = Err(error);
                    break;
                }
                if !is_allowed(layers[position].partition, row_id) {
                    ties.remove(index);
                    continue;
                }
                let partition = layers[position].partition;
                let score = match walk
                    .kept_rows
                    .get(&(partition, row_id))
                    .filter(|entry| entry.score_checked)
                    .map(|entry| entry.resolved_score)
                    .or_else(|| ties[..index].iter().find_map(|(prior, row, score)| {
                        (layers[*prior].partition == partition && *row == row_id)
                            .then_some(*score).flatten()
                    }))
                {
                    Some(score) => score,
                    // Each score read is paid from the walk's rescoring
                    // allowance; once it is spent the search is refused,
                    // never answered from the ties read so far.
                    None => {
                        if let Err(Stop::Limit(reason)) = walk.charge_rescore() {
                            status = HnswAllowedSearchStatus::Incomplete(reason);
                            terminal = UnionTerminal::Refused;
                            ties.clear();
                            break;
                        }
                        match resolve_score(
                            partition,
                            row_id,
                            &mut before_source_entry,
                            &mut before_distance,
                        ) {
                            Ok(score) => score,
                            Err(error) => {
                                retained = Err(error);
                                break;
                            }
                        }
                    }
                };
                if let Some(entry) = walk.kept_rows.get_mut(&(partition, row_id)) {
                    entry.resolved_score = score;
                    entry.score_checked = true;
                }
                // Keep rejected proofs in these bounded slots until every
                // layer duplicate has had a chance to reuse them.
                ties[index].2 = Some(score);
                index += 1;
            }
            ties.retain(|(_, _, score)| {
                score.is_some_and(|score| score.is_some_and(|score| {
                    score.total_cmp(&exact_score) == CmpOrdering::Equal
                }))
            });
            if let Err(error) = retained {
                VectorSearchScratch {
                    acquire: &mut acquire,
                    release: &mut release,
                }
                .free_vec(ties, HnswSearchScratchContainer::Result);
                return Err(error);
            }
        }
        // An authenticated exact directory can supply rows a fragmented
        // traversal missed. Accept its completion proof only when every
        // authoritative allowed row is present, or enough rows have the
        // maximum possible cosine score. Work-limit/floor refusals remain.
        let distinct_ties = ties
            .iter()
            .enumerate()
            .filter(|(ordinal, (position, row, _))| {
                !ties[..*ordinal].iter().any(|(prior_position, prior_row, _)| {
                    layers[*prior_position].partition == layers[*position].partition
                        && prior_row == row
                })
            })
            .count();
        if below_floor.is_none()
            && status
                == HnswAllowedSearchStatus::Incomplete(
                    HnswAllowedSearchIncomplete::InsufficientAllowedCandidates,
                )
            && distinct_ties >= required_results
            && (distinct_ties >= total_allowed || exact_score.is_some_and(|score| score >= 1.0))
        {
            status = HnswAllowedSearchStatus::Complete;
            terminal = if distinct_ties >= k {
                UnionTerminal::AnswerComplete
            } else {
                UnionTerminal::FrontierExhausted
            };
        }
        let capacity = ordered.len().saturating_add(ties.len());
        let allocated = VectorSearchScratch {
            acquire: &mut acquire,
            release: &mut release,
        }
        .allocate_vec::<HnswUnionCandidate>(
            HnswSearchScratchContainer::Result,
            capacity,
            "union candidates",
        );
        let mut candidates = match allocated {
            Ok(candidates) => candidates,
            Err(error) => {
                VectorSearchScratch {
                    acquire: &mut acquire,
                    release: &mut release,
                }
                .free_vec(ties, HnswSearchScratchContainer::Result);
                return Err(error);
            }
        };
        // A row equal to the query sits at the distance its own score gives.
        let tie_distance = exact_score.map_or(0.0, |score| 1.0 - score);
        for (position, row_id, tie_score) in ties.iter().copied() {
            let partition = layers[position].partition;
            candidates.push(HnswUnionCandidate {
                partition,
                layer: position,
                origin: None,
                row_id,
                distance: tie_distance,
                resolved_score: tie_score.flatten().or_else(|| walk.kept_rows.get(&(partition, row_id))
                    .and_then(|entry| entry.resolved_score)),
                score_checked: tie_score.is_some() || walk.kept_rows.get(&(partition, row_id))
                    .is_some_and(|entry| entry.score_checked),
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
                origin: Some(kept.reached.origin),
                row_id,
                distance: kept.reached.distance,
                resolved_score: walk
                    .kept_rows
                    .get(&(open_layer.partition, row_id))
                    .and_then(|entry| entry.resolved_score),
                score_checked: walk.kept_rows.get(&(open_layer.partition, row_id))
                    .is_some_and(|entry| entry.score_checked),
            });
        }
        Ok(HnswUnionSearchResult {
            terminal,
            retained_bytes: candidates
                .capacity()
                .saturating_mul(std::mem::size_of::<HnswUnionCandidate>())
                .saturating_add(
                    disconnected
                        .capacity()
                        .saturating_mul(std::mem::size_of::<usize>()),
                ),
            candidates,
            disconnected: std::mem::take(&mut disconnected),
            status,
            below_admission_floor: below_floor,
            budget,
            admission_evaluations,
            expansion_evaluations,
            rescore_evaluations: walk.rescores,
            allowed_admissions: walk.admissions,
            #[cfg(feature = "test-seams")]
            expansion_cap,
            layers_admitted,
            frontier_exhausted,
        })
    })();
    #[cfg(feature = "test-seams")]
    trace_end();
    if disconnected.capacity() != 0 {
        let capacity = disconnected.capacity();
        drop(disconnected);
        release(HnswSearchScratchEvent::Release {
            container: HnswSearchScratchContainer::Result,
            capacity,
            element_bytes: std::mem::size_of::<usize>(),
        });
    }

    // Each charge goes back only once what it covers has been dropped.
    let frontier_charged = walk.frontier_charged;
    let visit_bytes = walk.visit_bytes;
    reservations[1].1 = walk.kept_rows_bytes;
    let hops = std::mem::take(&mut walk.hops);
    let bridges = std::mem::take(&mut walk.bridges);
    drop(walk);
    if visit_bytes != 0 {
        release(HnswSearchScratchEvent::Release {
            container: HnswSearchScratchContainer::VisitedPoints,
            capacity: visit_bytes,
            element_bytes: 1,
        });
    }
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
    release(HnswSearchScratchEvent::Release {
        container: HnswSearchScratchContainer::ServingPageDirectory,
        capacity: layers.len(),
        element_bytes: open_element_bytes,
    });
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
    layer: &mut OpenLayer<'_, '_>,
    point: PointId,
    query: &UnionQuery<'_>,
    walk: &mut Walk<'_>,
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
    if let Some(distance) = walk.reached.get(&bit).and_then(PointVisit::distance) {
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
    walk.visit_entry(bit, acquire, release)?.cache_distance(distance);
    Ok(Ok(Some(distance)))
}

/// Open every layer: evaluate its entry point, descend through layer zero
/// greedily, and put what it reached -- and its seed directory -- on the one
/// frontier. Every evaluation is charged to the one budget; none is skipped
/// for a layer that looks far away.
#[allow(clippy::too_many_arguments)]
fn admit_layers<E, S, D, A, R, F, V>(
    open: &mut [OpenLayer<'_, '_>],
    walk: &mut Walk<'_>,
    query: &UnionQuery<'_>,
    allow_bridges: bool,
    is_allowed: &mut F,
    resolve_score: &mut V,
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
    V: FnMut(usize, RowId, &mut S, &mut D) -> std::result::Result<Option<f32>, E>,
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
        resolve_score,
        before_source_entry,
        before_distance,
        acquire,
        release,
    );
    walk.hops = hops;
    admitted
}

#[allow(clippy::too_many_arguments)]
fn admit_layers_with<E, S, D, A, R, F, V>(
    open: &mut [OpenLayer<'_, '_>],
    walk: &mut Walk<'_>,
    hops: &mut HopScratch,
    query: &UnionQuery<'_>,
    allow_bridges: bool,
    is_allowed: &mut F,
    resolve_score: &mut V,
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
    V: FnMut(usize, RowId, &mut S, &mut D) -> std::result::Result<Option<f32>, E>,
{
    let mut layers = 0usize;
    for (position, layer) in open.iter_mut().enumerate() {
        let partition = layer.partition;
        let Some(mut entry_id) = layer.entry() else {
            continue;
        };
        // Match the fork's stable allowed entry substitution: inspect only
        // the oldest physical slot in each level, without evaluating excluded
        // history or scanning the population to discover an allowed seed.
        if !allow_bridges {
            let origin = layer
                .origin(entry_id, before_source_entry)?
                .ok_or_else(|| E::from(Error::Other("graph entry identity".into())))?;
            if !layer
                .row(origin, before_source_entry)?
                .is_some_and(|row| is_allowed(partition, row))
            {
                let mut oldest = None;
                for (level, end) in layer.level_ends.iter().copied().enumerate() {
                    let start = if level == 0 { 0 } else { layer.level_ends[level - 1] };
                    if end == start {
                        continue;
                    }
                    let point = PointId(level as u8, 0);
                    let origin = layer
                        .origin(point, before_source_entry)?
                        .ok_or_else(|| E::from(Error::Other("graph oldest identity".into())))?;
                    if oldest.is_none_or(|(_, prior)| origin < prior) {
                        oldest = Some((point, origin));
                    }
                }
                if let Some((point, origin)) = oldest
                    && layer
                        .row(origin, before_source_entry)?
                        .is_some_and(|row| is_allowed(partition, row))
                {
                    entry_id = point;
                }
            }
        }
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
            Ok(None) => return Err(E::from(Error::Other(("graph entry point".to_string()).into()))),
            Err(stop) => {
                return Ok(Admitted {
                    layers,
                    stop: Some(stop),
                });
            }
        };
        // Admission already funded this point's distance. Admit it to the
        // candidate heap/frontier now, rather than discarding every observed
        // point except the final greedy pivot.
        if let Some(stop) = consider(
            layer,
            position,
            entry_id,
            walk,
            query,
            allow_bridges,
            is_allowed,
            resolve_score,
            before_source_entry,
            before_distance,
            acquire,
            release,
        )?
        .stop()
        {
            return Ok(Admitted {
                layers,
                stop: Some(stop),
            });
        }
        for level in (0..=entry_id.0).rev() {
            loop {
                let mut changed = false;
                let neighbours = layer
                    .neighbours(
                        pivot_id,
                        level as usize,
                        &mut *hops,
                        acquire,
                        before_source_entry,
                    )?
                    .ok_or_else(|| {
                        E::from(Error::Other(("graph admission neighbours".to_string()).into()))
                    })?;
                for hop in 0..neighbours.len() {
                    let (neighbour_id, neighbour_origin) = neighbours.get(hop);
                    if !allow_bridges
                        && !layer
                            .row(neighbour_origin, before_source_entry)?
                            .is_some_and(|row| is_allowed(partition, row))
                    {
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
                        Ok(None) => return Err(E::from(Error::Other(("graph edge".to_string()).into()))),
                        Err(stop) => {
                            return Ok(Admitted {
                                layers,
                                stop: Some(stop),
                            });
                        }
                    };
                    if let Some(stop) = consider(
                        layer,
                        position,
                        neighbour_id,
                        walk,
                        query,
                        allow_bridges,
                        is_allowed,
                        resolve_score,
                        before_source_entry,
                        before_distance,
                        acquire,
                        release,
                    )?
                    .stop()
                    {
                        return Ok(Admitted {
                            layers,
                            stop: Some(stop),
                        });
                    }
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
        let seeds = std::mem::take(&mut layer.seeds);
        let seeded = (|| -> std::result::Result<Option<Stop>, E> {
            for seed in &seeds {
                let Some(bit) = layer.bit(*seed) else {
                    continue;
                };
                if walk.reached.get(&bit).is_some_and(|entry| entry.marked) {
                    continue;
                }
                let Some(origin) = layer.origin(*seed, before_source_entry)? else {
                    continue;
                };
                #[cfg(feature = "test-seams")]
                if layer
                    .row(origin, before_source_entry)?
                    .is_some_and(super::hnsw_row_unlinked_for_test)
                {
                    continue;
                }
                let own = layer.row(origin, before_source_entry)?;
                if !layer.any_row_allowed(origin, own, before_source_entry, is_allowed)? {
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
                walk.mark(bit, acquire, release)?;
                let reached = Reached {
                    distance,
                    row_id: layer
                        .row(origin, before_source_entry)?
                        .unwrap_or(RowId(u64::MAX)),
                    layer: position as u32,
                    point: *seed,
                    origin,
                };
                walk.push_frontier(reached, acquire, release)?;
                if let Some(stop) = keep_reached(
                    layer,
                    origin,
                    reached,
                    walk,
                    is_allowed,
                    resolve_score,
                    before_source_entry,
                    before_distance,

                    acquire, release,
                )? {
                    return Ok(Some(stop));
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
    open: &mut [OpenLayer<'_, '_>],
    walk: &mut Walk<'_>,
    query: &UnionQuery<'_>,
    total_allowed: usize,
    required_results: usize,
    graph_scores_are_current: bool,
    can_certify_resolved_scores: bool,
    search_radius: f32,
    settled: &mut [HnswUnionCandidate],
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
    // Earlier partitions' rows, nearest first by walk distance, and their
    // visible stored distances once a stop proof has read them. Both arrays
    // are admitted before they are allocated and given back once dropped; the
    // in-place unstable sort allocates nothing, and its order is total.
    let settled_charges = [
        std::mem::size_of::<usize>(),
        std::mem::size_of::<Option<Option<f32>>>(),
    ];
    if !settled.is_empty() {
        for (admitted, element_bytes) in settled_charges.iter().copied().enumerate() {
            if let Err(error) = acquire(HnswSearchScratchEvent::Reserve {
                container: HnswSearchScratchContainer::MergeResults,
                previous_capacity: 0,
                requested_capacity: settled.len(),
                element_bytes,
            }) {
                for element_bytes in settled_charges[..admitted].iter().copied() {
                    release(HnswSearchScratchEvent::Release {
                        container: HnswSearchScratchContainer::MergeResults,
                        capacity: settled.len(),
                        element_bytes,
                    });
                }
                walk.hops = hops;
                walk.bridges = bridges;
                return Err(error);
            }
        }
    }
    let mut settled_order: Vec<usize> = Vec::with_capacity(settled.len());
    settled_order.extend(0..settled.len());
    settled_order.sort_unstable_by(|left, right| {
        settled[*left]
            .distance
            .total_cmp(&settled[*right].distance)
            .then(settled[*left].partition.cmp(&settled[*right].partition))
            .then(settled[*left].row_id.cmp(&settled[*right].row_id))
    });
    let mut settled_scores: Vec<Option<Option<f32>>> = Vec::with_capacity(settled.len());
    settled_scores.extend(
        settled
            .iter()
            .map(|candidate| candidate.score_checked.then_some(candidate.resolved_score)),
    );
    let expanded = (|| -> std::result::Result<Expanded, E> {
        #[cfg(feature = "test-seams")]
        let mut expansions = 0usize;
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
            // Whether the stop leaned on rows earlier partitions returned. The
            // merged page orders those by their visible stored scores, so they
            // count only once those scores confirm them strictly nearer.
            let mut leaned_on_settled = false;
            if walk.kept.len() >= walk.kept_capacity
                && walk.frontier_ends_walk(candidate, required_results, search_radius, |distance| {
                    leaned_on_settled = true;
                    settled
                        .iter()
                        .filter(|row| row.distance.total_cmp(&distance) == CmpOrdering::Less)
                        .count()
                })
            {
                let certified = if graph_scores_are_current && !leaned_on_settled {
                    true
                } else if graph_scores_are_current {
                    let mut nearer = walk.kept.len() - walk.worst_ties;
                    for position in settled_order.iter().copied() {
                        if nearer >= required_results {
                            break;
                        }
                        let row = &settled[position];
                        before_source_entry()?;
                        if !is_allowed(row.partition, row.row_id) {
                            continue;
                        }
                        let score = match settled_scores[position] {
                            Some(score) => score,
                            None => {
                                // A certifying score is a read of the rescoring allowance.
                                if let Err(stop) = walk.charge_rescore() {
                                    return Ok(Expanded {
                                        converged: false,
                                        stop: Some(stop),
                                    });
                                }
                                let score = resolve_score(
                                    row.partition,
                                    row.row_id,
                                    before_source_entry,
                                    before_distance,
                                )?;
                                settled_scores[position] = Some(score);
                                score
                            }
                        };
                        if score.is_some_and(|score| {
                            (1.0 - score).total_cmp(&candidate.distance) == CmpOrdering::Less
                        }) {
                            nearer += 1;
                        }
                    }
                    nearer >= required_results
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
                            before_source_entry()?;
                            if !is_allowed(key.0, key.1) {
                                continue;
                            }
                            let score = match walk
                                .kept_rows
                                .get(&key)
                                .filter(|entry| entry.score_checked)
                                .map(|entry| entry.resolved_score)
                            {
                                Some(score) => score,
                                None => {
                                    // A certifying score is a read of the rescoring allowance.
                                    if let Err(stop) = walk.charge_rescore() {
                                        return Ok(Expanded {
                                            converged: false,
                                            stop: Some(stop),
                                        });
                                    }
                                    resolve_score(
                                        key.0,
                                        key.1,
                                        before_source_entry,
                                        before_distance,
                                    )?
                                }
                            };
                            if let Some(entry) = walk.kept_rows.get_mut(&key) {
                                entry.resolved_score = score;
                                entry.score_checked = true;
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
                        // Rows earlier partitions returned compete for the
                        // same merged page, scored the same way.
                        for position in settled_order.iter().copied() {
                            if nearer >= required_results {
                                break;
                            }
                            let row = &settled[position];
                            before_source_entry()?;
                            if !is_allowed(row.partition, row.row_id) {
                                continue;
                            }
                            let score = match settled_scores[position] {
                                Some(score) => score,
                                None => {
                                    // A certifying score is a read of the rescoring allowance.
                                    if let Err(stop) = walk.charge_rescore() {
                                        return Ok(Expanded {
                                            converged: false,
                                            stop: Some(stop),
                                        });
                                    }
                                    let score = resolve_score(
                                        row.partition,
                                        row.row_id,
                                        before_source_entry,
                                        before_distance,
                                    )?;
                                    settled_scores[position] = Some(score);
                                    score
                                }
                            };
                            if score.is_some_and(|score| {
                                (1.0 - score).total_cmp(&candidate.distance) == CmpOrdering::Less
                            }) {
                                nearer += 1;
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
                    #[cfg(feature = "test-seams")]
                    trace_stop(
                        open,
                        walk,
                        &mut hops,
                        query,
                        candidate,
                        expansions,
                        is_allowed,
                        before_source_entry,
                        before_distance,
                        acquire,
                        release,
                    )?;
                    return Ok(Expanded {
                        converged: true,
                        stop: None,
                    });
                }
            }
            #[cfg(feature = "test-seams")]
            {
                expansions += 1;
                trace_expansion(expansions, candidate.row_id);
            }
            let layer = &mut open[candidate.layer as usize];
            let neighbours = layer
                .neighbours(candidate.point, 0, &mut hops, acquire, before_source_entry)?
                .ok_or_else(|| E::from(Error::Other(("graph layer-zero neighbours".to_string()).into())))?;
            for hop in 0..neighbours.len() {
                let (neighbour_id, _) = neighbours.get(hop);
                match consider(
                    layer,
                    candidate.layer as usize,
                    neighbour_id,
                    walk,
                    query,
                    allow_bridges,
                    is_allowed,
                    resolve_score,
                    before_source_entry,
                    before_distance,
                    acquire,
                    release,
                )? {
                    Considered::Stopped(stop) => {
                        return Ok(Expanded {
                            converged: false,
                            stop: Some(stop),
                        });
                    }
                    Considered::Passed => continue,
                    Considered::Excluded => {}
                }
                // The excluded neighbour is where the admitted points stop
                // reaching each other. Step through it once, as navigation.
                // Whether it is excluded is the answer the consideration
                // already read; a point reached before is never read again.
                let Some(bridge_hops) = layer.neighbours(
                    neighbour_id,
                    0,
                    &mut bridges,
                    acquire,
                    before_source_entry,
                )?
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
                        resolve_score,
                        before_source_entry,
                        before_distance,
                        acquire,
                        release,
                    )?
                    .stop()
                    {
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
    drop(settled_order);
    // These slots belong to the same captured search, query and snapshot.
    // Preserve proof across expansion phases, later partitions and resolution.
    for (candidate, score) in settled.iter_mut().zip(settled_scores.iter()) {
        if let Some(score) = score {
            candidate.resolved_score = *score;
            candidate.score_checked = true;
        }
    }
    drop(settled_scores);
    if !settled.is_empty() {
        for element_bytes in settled_charges {
            release(HnswSearchScratchEvent::Release {
                container: HnswSearchScratchContainer::MergeResults,
                capacity: settled.len(),
                element_bytes,
            });
        }
    }
    expanded
}

/// Fill `disconnected` with each partition whose distinct reached rows are
/// fewer than its allowed rows, after an unfiltered walk's frontier ran out.
/// In such a walk every marked point is an admitted row, so the rows are read
/// from the marked points: work and scratch scale with what the walk already
/// visited, never with the layers' retained history. The scratch row list and
/// the partition list are reserved before they grow.
#[allow(clippy::too_many_arguments)]
fn disconnected_partitions<E, S, A, R, F>(
    open: &[OpenLayer<'_, '_>],
    walk: &Walk,
    layers: &[HnswUnionLayer<'_>],
    disconnected: &mut Vec<usize>,
    is_allowed: &mut F,
    before_source_entry: &mut S,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<(), E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    F: FnMut(usize, RowId) -> bool,
{
    let marked = walk.reached.values().filter(|visit| visit.marked).count();
    let mut scratch = VectorSearchScratch {
        acquire: &mut *acquire,
        release: &mut *release,
    };
    let mut rows = Vec::<(usize, RowId)>::new();
    scratch.ensure_vec_capacity_compact(
        &mut rows,
        HnswSearchScratchContainer::Result,
        marked,
        "disconnected-partition rows",
        &mut *before_source_entry,
    )?;
    let counted = (|| -> std::result::Result<(), E> {
        for (bit, visit) in &walk.reached {
            if !visit.marked {
                continue;
            }
            before_source_entry()?;
            let Some(layer) = open.iter().rev().find(|layer| layer.first_bit <= *bit) else {
                continue;
            };
            let offset = bit - layer.first_bit;
            let Some(level) = layer.level_ends.iter().position(|end| offset < *end) else {
                continue;
            };
            let start = if level == 0 { 0 } else { layer.level_ends[level - 1] };
            let point = PointId(level as u8, (offset - start) as i32);
            if layer.bit(point) != Some(*bit) {
                continue;
            }
            let Some(origin) = layer.origin(point, before_source_entry)? else {
                continue;
            };
            // The point's own row and every row sharing it, each counted
            // only when the snapshot allows it.
            let partition = layer.partition;
            if let Some(row) = layer.row(origin, before_source_entry)?
                && is_allowed(partition, row)
            {
                let needed = rows.len() + 1;
                scratch.ensure_vec_capacity_compact(
                    &mut rows,
                    HnswSearchScratchContainer::Result,
                    needed,
                    "disconnected-partition rows",
                    &mut *before_source_entry,
                )?;
                rows.push((partition, row));
            }
            // Each allowed member goes straight into the admitted row list.
            // One work control serves every seek of the traversal -- a probe
            // that finds no member included -- and every row a growth of that
            // list copies; any of them can stop the statement.
            let checkpoint = std::cell::RefCell::new(&mut *before_source_entry);
            let mut traversal_checkpoint = || {
                #[cfg(feature = "test-seams")]
                observe_member_checkpoint();
                (*checkpoint.borrow_mut())()
            };
            #[cfg(feature = "test-seams")]
            COUNTING_MEMBERS.with(|counting| counting.set(true));
            let counted =
                layer.for_each_member(origin, &mut traversal_checkpoint, &mut |member, _, _| {
                    if !is_allowed(partition, member) {
                        return Ok(true);
                    }
                    let needed = rows.len() + 1;
                    #[cfg(feature = "test-seams")]
                    if needed > rows.capacity() {
                        MEMBER_GROWTHS.with(|growths| growths.borrow_mut().push(rows.capacity()));
                    }
                    #[cfg(feature = "test-seams")]
                    let needed = if needed > rows.capacity()
                        && !rows.is_empty()
                        && take_member_growth_refusal()
                    {
                        // Asks for far more than any statement admits, so the real
                        // budget refuses this growth.
                        needed.saturating_add(1 << 36)
                    } else {
                        needed
                    };
                    scratch.ensure_vec_capacity_compact(
                        &mut rows,
                        HnswSearchScratchContainer::Result,
                        needed,
                        "disconnected-partition rows",
                        &mut || (*checkpoint.borrow_mut())(),
                    )?;
                    rows.push((partition, member));
                    Ok(true)
                });
            #[cfg(feature = "test-seams")]
            COUNTING_MEMBERS.with(|counting| counting.set(false));
            counted?;
        }
        rows.sort_unstable();
        rows.dedup();
        let mut found = 0usize;
        let mut counted_partitions = 0usize;
        for (position, layer) in layers.iter().enumerate() {
            if layers[..position]
                .iter()
                .any(|earlier| earlier.partition == layer.partition)
            {
                continue;
            }
            counted_partitions += 1;
            let reached = rows.partition_point(|(partition, _)| *partition <= layer.partition)
                - rows.partition_point(|(partition, _)| *partition < layer.partition);
            if reached < layer.allowed_count {
                found += 1;
            }
        }
        if found == 0 {
            return Ok(());
        }
        scratch.ensure_vec_capacity(
            disconnected,
            HnswSearchScratchContainer::Result,
            found.min(counted_partitions),
            "disconnected partitions",
            &mut *before_source_entry,
        )?;
        for (position, layer) in layers.iter().enumerate() {
            if layers[..position]
                .iter()
                .any(|earlier| earlier.partition == layer.partition)
            {
                continue;
            }
            let reached = rows.partition_point(|(partition, _)| *partition <= layer.partition)
                - rows.partition_point(|(partition, _)| *partition < layer.partition);
            if reached < layer.allowed_count {
                disconnected.push(layer.partition);
            }
        }
        Ok(())
    })();
    scratch.free_vec(rows, HnswSearchScratchContainer::Result);
    counted
}

/// What considering one point came to.
enum Considered {
    /// Reached now, reached before, or not a point of the layer.
    Passed,
    /// No row of the point is allowed, and the walk does not step through
    /// excluded points: the caller may bridge across it.
    Excluded,
    /// The walk's allowance ran out.
    Stopped(Stop),
}

impl Considered {
    fn stop(self) -> Option<Stop> {
        match self {
            Considered::Stopped(stop) => Some(stop),
            Considered::Passed | Considered::Excluded => None,
        }
    }
}

/// Reach one point: skip it if it was reached or the filter excludes it,
/// otherwise evaluate it, put it on the frontier and keep it if admitted.
/// A point already reached is passed without reading its rows again.
#[allow(clippy::too_many_arguments)]
fn consider<E, S, D, A, R, F, V>(
    layer: &mut OpenLayer<'_, '_>,
    position: usize,
    point_id: PointId,
    walk: &mut Walk<'_>,
    query: &UnionQuery<'_>,
    allow_bridges: bool,
    is_allowed: &mut F,
    resolve_score: &mut V,
    before_source_entry: &mut S,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<Considered, E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    F: FnMut(usize, RowId) -> bool,
    V: FnMut(usize, RowId, &mut S, &mut D) -> std::result::Result<Option<f32>, E>,
{
    let Some(bit) = layer.bit(point_id) else {
        return Ok(Considered::Passed);
    };
    if walk.reached.get(&bit).is_some_and(|entry| entry.marked) {
        return Ok(Considered::Passed);
    }
    let Some(origin) = layer.origin(point_id, before_source_entry)? else {
        return Ok(Considered::Passed);
    };
    let row_id = layer.row(origin, before_source_entry)?;
    #[cfg(feature = "test-seams")]
    if row_id.is_some_and(super::hnsw_row_unlinked_for_test) {
        return Ok(Considered::Passed);
    }
    let admitted = layer.any_row_allowed(origin, row_id, before_source_entry, is_allowed)?;
    if !allow_bridges && !admitted {
        #[cfg(feature = "test-seams")]
        if let Some(row_id) = row_id {
            super::observe_hnsw_residual_rejection(row_id);
        }
        return Ok(Considered::Excluded);
    }
    let distance = if let Some(distance) = walk.reached.get(&bit).and_then(PointVisit::distance) {
        distance
    } else {
        if let Err(stop) = walk.charge() {
            return Ok(Considered::Stopped(stop));
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
            return Ok(Considered::Passed);
        };
        walk.visited += 1;
        walk.evaluations += 1;
        distance
    };
    walk.mark(bit, acquire, release)?;
    let reached = Reached {
        distance,
        row_id: row_id.unwrap_or(RowId(u64::MAX)),
        layer: position as u32,
        point: point_id,
        origin,
    };
    walk.push_frontier(reached, acquire, release)?;
    keep_reached(
        layer,
        origin,
        reached,
        walk,
        is_allowed,
        resolve_score,
        before_source_entry,
        before_distance,

        acquire, release,
    )
    .map(|stop| stop.map_or(Considered::Passed, Considered::Stopped))
}

/// Keep the reached point's own row and every row sharing it, each at the
/// distance it answers at under the snapshot; the walk's limit when scoring
/// them spent its evaluation allowance.
#[allow(clippy::too_many_arguments)]
fn keep_reached<E, S, D, F, V, A, R>(
    layer: &OpenLayer<'_, '_>,
    origin: usize,
    reached: Reached,
    walk: &mut Walk<'_>,
    is_allowed: &mut F,
    resolve_score: &mut V,
    before_source_entry: &mut S,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<Option<Stop>, E>
where
    E: From<Error>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    F: FnMut(usize, RowId) -> bool,
    V: FnMut(usize, RowId, &mut S, &mut D) -> std::result::Result<Option<f32>, E>,
{
    let partition = layer.partition;
    if let Some(row_id) = layer.row(origin, before_source_entry)?
        && is_allowed(partition, row_id)
    {
        let stored = layer.row_version(origin, before_source_entry)?;
        let answered = match answer_distance(
            walk,
            partition,
            row_id,
            stored,
            layer.covered_lsn,
            reached.distance,
            resolve_score,
            before_source_entry,
            before_distance,
        )? {
            Ok(answered) => answered,
            Err(stop) => return Ok(Some(stop)),
        };
        if let Some((distance, resolved)) = answered {
            #[cfg(feature = "test-seams")]
            trace_evaluation(row_id, distance);
            walk.keep_resolved(
                Kept {
                    reached: Reached {
                        distance,
                        ..reached
                    },
                    partition,
                    row_id,
                },
                resolved, acquire, release,
            )?;
        }
    }
    keep_members(
        layer,
        origin,
        reached,
        walk,
        before_source_entry,
        is_allowed,
        resolve_score,
        before_distance,

        acquire, release,
    )
}

/// Keep every allowed row that shares the reached point, at its distance.
/// A member is filtered by the snapshot exactly as the point's own row is.
#[allow(clippy::too_many_arguments)]
fn keep_members<E, S, D, F, V, A, R>(
    layer: &OpenLayer<'_, '_>,
    origin: usize,
    reached: Reached,
    walk: &mut Walk<'_>,
    before_source_entry: &mut S,
    is_allowed: &mut F,
    resolve_score: &mut V,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<Option<Stop>, E>
where
    E: From<Error>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    F: FnMut(usize, RowId) -> bool,
    V: FnMut(usize, RowId, &mut S, &mut D) -> std::result::Result<Option<f32>, E>,
{
    let partition = layer.partition;
    // Members share the point's distance while every stored version is
    // current. Read in row order, once the heap is full and one cannot enter,
    // none after it can. Where versions are tracked, a later member may
    // answer at its own nearer visible score, so each is still read.
    // Supplied candidates come in the caller's order and are each probed.
    let in_row_order = layer.supplied.is_none() && walk.visible_lsn.is_none();
    let mut stopped = None;
    layer.for_each_member(
        origin,
        before_source_entry,
        &mut |member, lsn, before_source| {
            if !is_allowed(partition, member) {
                return Ok(true);
            }
            let answered = match answer_distance(
                walk,
                partition,
                member,
                Some(lsn),
                None,
                reached.distance,
                resolve_score,
                before_source,
                before_distance,
            )? {
                Ok(answered) => answered,
                Err(stop) => {
                    stopped = Some(stop);
                    return Ok(false);
                }
            };
            let Some((distance, resolved)) = answered else {
                return Ok(true);
            };
            let outcome = walk.keep_resolved(
                Kept {
                    reached: Reached {
                        distance,
                        ..reached
                    },
                    partition,
                    row_id: member,
                },
                resolved, acquire, release,
            )?;
            Ok(!(outcome == KeepOutcome::Full && in_row_order))
        },
    )?;
    Ok(stopped)
}

#[cfg(feature = "test-seams")]
thread_local! {
    static MEMBER_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MEMBER_READS_SINCE_CHECKPOINT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static COUNTING_MEMBERS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static MOST_MEMBER_READS_BETWEEN_CHECKPOINTS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static MEMBER_GROWTH_REFUSAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static MEMBER_GROWTHS: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// The previous capacity of each growth of a disconnected partition's row
/// list a member caused on this thread, since the last call.
#[cfg(all(test, feature = "test-seams"))]
fn take_member_growths() -> Vec<usize> {
    MEMBER_GROWTHS.with(|growths| std::mem::take(&mut *growths.borrow_mut()))
}

#[cfg(feature = "test-seams")]
fn take_member_growth_refusal() -> bool {
    MEMBER_GROWTH_REFUSAL.with(|armed| armed.replace(false))
}

/// Ask the next growth of a disconnected partition's row list that a shared
/// point's member causes, on this thread, to request more than any statement
/// admits, once, so the real budget refuses it.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn refuse_member_growth_once_for_test() {
    MEMBER_GROWTH_REFUSAL.with(|armed| armed.set(true));
}

#[cfg(feature = "test-seams")]
fn observe_member_visit() {
    MEMBER_VISITS.with(|visits| visits.set(visits.get() + 1));
    if !COUNTING_MEMBERS.with(std::cell::Cell::get) {
        return;
    }
    MEMBER_READS_SINCE_CHECKPOINT.with(|reads| {
        reads.set(reads.get() + 1);
        MOST_MEMBER_READS_BETWEEN_CHECKPOINTS.with(|most| most.set(most.get().max(reads.get())));
    });
}

#[cfg(feature = "test-seams")]
fn observe_member_checkpoint() {
    MEMBER_READS_SINCE_CHECKPOINT.with(|reads| reads.set(0));
}

/// The most membership reads (probes, hits or misses) a disconnected count on
/// this thread made between two work checkpoints, since the last call; resets.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn take_most_member_reads_between_checkpoints_for_test() -> u64 {
    MEMBER_READS_SINCE_CHECKPOINT.with(|reads| reads.set(0));
    MOST_MEMBER_READS_BETWEEN_CHECKPOINTS.with(|most| most.replace(0))
}

/// Rows read on this thread as members of a reached point, since the last
/// call; resets the count.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn take_member_visits_for_test() -> u64 {
    MEMBER_VISITS.with(|visits| visits.replace(0))
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
    /// Stored vectors the search read to score rows: the walk's reads for
    /// rows updated since their graph was built, and final candidate
    /// scoring's; both paid from the rescoring allowance.
    pub rescore_evaluations: usize,
    /// Why the search could not answer, when it could not: the walk's
    /// incomplete status or final scoring running out of its allowance.
    pub unresolved: Option<HnswAllowedSearchIncomplete>,
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
    /// Allowed graph points the walk admitted, each version of a row counted.
    pub allowed_admissions: usize,
    /// Partitions the walk found holding live rows their graph does not
    /// connect, handed back to be compared row by row.
    pub disconnected_partitions: usize,
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

/// One row a traced partition walk evaluated: the expansion that reached it
/// (0 while the layers were opened) and the row whose expansion reached it.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UnionWalkEvaluation {
    pub row_id: u64,
    pub distance: f32,
    pub expansion: usize,
    pub parent: Option<u64>,
}

/// The stop check that ended a traced walk.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UnionWalkStop {
    pub head_row: u64,
    pub head_distance: f32,
    pub worst_kept_distance: f32,
    pub worst_ties: usize,
    pub kept: usize,
    pub head_is_farther: bool,
    pub evaluations: usize,
    pub frontier_len: usize,
}

/// What one partition walk did, for a diagnostic: every evaluation in order,
/// how many heads it expanded, the stop it took, the frontier it left and an
/// observer-only continuation past that stop that changes nothing it returned.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UnionWalkTrace {
    pub partition: usize,
    pub expansions: usize,
    pub stop: Option<UnionWalkStop>,
    pub evaluated: Vec<UnionWalkEvaluation>,
    pub frontier_at_stop: Vec<(u64, f32)>,
    pub continuation: Vec<UnionWalkEvaluation>,
}

#[cfg(feature = "test-seams")]
struct WalkTraceState {
    continuation_expansions: usize,
    finished: Vec<UnionWalkTrace>,
    current: Option<UnionWalkTrace>,
    expansion: usize,
    parent: Option<u64>,
}

#[cfg(feature = "test-seams")]
thread_local! {
    static UNION_WALK_TRACES: RefCell<Option<WalkTraceState>> = const { RefCell::new(None) };
}

/// Trace every union partition walk `search` runs on this thread, continuing
/// each past its stop by at most `continuation_expansions` observer-only
/// expansions.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_union_walk_traces<R>(
    continuation_expansions: usize,
    search: impl FnOnce() -> R,
) -> (R, Vec<UnionWalkTrace>) {
    let prior = UNION_WALK_TRACES.with(|slot| {
        slot.replace(Some(WalkTraceState {
            continuation_expansions,
            finished: Vec::new(),
            current: None,
            expansion: 0,
            parent: None,
        }))
    });
    let result = search();
    let traces = UNION_WALK_TRACES
        .with(|slot| slot.replace(prior))
        .map(|state| state.finished)
        .unwrap_or_default();
    (result, traces)
}

#[cfg(feature = "test-seams")]
thread_local! {
    static UNION_SEARCH_RADIUS: std::cell::Cell<Option<f32>> = const { std::cell::Cell::new(None) };
}

/// Run `search` with every union walk on this thread using `radius` in place
/// of the index's resolved search radius, for measuring it.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn with_union_search_radius<R>(radius: f32, search: impl FnOnce() -> R) -> R {
    let prior = UNION_SEARCH_RADIUS.with(|slot| slot.replace(Some(radius)));
    let result = search();
    UNION_SEARCH_RADIUS.with(|slot| slot.set(prior));
    result
}

#[cfg(feature = "test-seams")]
fn search_radius_override() -> Option<f32> {
    UNION_SEARCH_RADIUS.with(|slot| slot.get())
}

#[cfg(feature = "test-seams")]
fn trace_with<T>(change: impl FnOnce(&mut WalkTraceState) -> T) -> Option<T> {
    UNION_WALK_TRACES.with(|slot| slot.borrow_mut().as_mut().map(change))
}

#[cfg(feature = "test-seams")]
fn trace_begin(partition: usize) {
    trace_with(|state| {
        state.expansion = 0;
        state.parent = None;
        state.current = Some(UnionWalkTrace {
            partition,
            ..UnionWalkTrace::default()
        });
    });
}

#[cfg(feature = "test-seams")]
fn trace_end() {
    trace_with(|state| {
        if let Some(trace) = state.current.take() {
            state.finished.push(trace);
        }
    });
}

#[cfg(feature = "test-seams")]
fn trace_expansion(expansion: usize, parent: RowId) {
    trace_with(|state| {
        state.expansion = expansion;
        state.parent = Some(parent.0);
        if let Some(trace) = state.current.as_mut() {
            trace.expansions = expansion;
        }
    });
}

#[cfg(feature = "test-seams")]
fn trace_evaluation(row_id: RowId, distance: f32) {
    trace_with(|state| {
        let (expansion, parent) = (state.expansion, state.parent);
        if let Some(trace) = state.current.as_mut() {
            trace.evaluated.push(UnionWalkEvaluation {
                row_id: row_id.0,
                distance,
                expansion,
                parent,
            });
        }
    });
}

#[cfg(feature = "test-seams")]
fn trace_continuation_expansions() -> usize {
    trace_with(|state| {
        if state.current.is_some() {
            state.continuation_expansions
        } else {
            0
        }
    })
    .unwrap_or(0)
}

/// Record the stop a traced walk took, and continue it past that stop on a
/// copy of its frontier and reached set: nothing it returns, keeps or charges
/// to the walk changes.
#[cfg(feature = "test-seams")]
#[allow(clippy::too_many_arguments)]
fn trace_stop<E, S, D, A, R, F>(
    open: &mut [OpenLayer<'_, '_>],
    walk: &Walk,
    hops: &mut HopScratch,
    query: &UnionQuery<'_>,
    head: Reached,
    expansions: usize,
    is_allowed: &mut F,
    before_source_entry: &mut S,
    before_distance: &mut D,
    acquire: &mut A,
    release: &mut R,
) -> std::result::Result<(), E>
where
    E: From<Error>,
    S: FnMut() -> std::result::Result<(), E>,
    D: FnMut() -> std::result::Result<(), E>,
    A: FnMut(HnswSearchScratchEvent) -> std::result::Result<(), E>,
    R: FnMut(HnswSearchScratchEvent),
    F: FnMut(usize, RowId) -> bool,
{
    let continuation_expansions = trace_continuation_expansions();
    if trace_with(|state| state.current.is_some()) != Some(true) {
        return Ok(());
    }
    let worst = walk
        .kept
        .peek()
        .map_or(f32::INFINITY, |entry| entry.0.reached.distance);
    let mut frontier_at_stop: Vec<(u64, f32)> = walk
        .frontier
        .iter()
        .map(|entry| (entry.0.row_id.0, entry.0.distance))
        .collect();
    frontier_at_stop.sort_by(|left, right| left.1.total_cmp(&right.1).then(left.0.cmp(&right.0)));
    let stop = UnionWalkStop {
        head_row: head.row_id.0,
        head_distance: head.distance,
        worst_kept_distance: worst,
        worst_ties: walk.worst_ties,
        kept: walk.kept.len(),
        head_is_farther: head.distance.total_cmp(&worst) == CmpOrdering::Greater,
        evaluations: walk.evaluations,
        frontier_len: walk.frontier.len(),
    };
    // The head the stop declined is expanded first, as a stop-less walk would.
    let mut frontier: BinaryHeap<Nearest> =
        walk.frontier.iter().map(|entry| Nearest(entry.0)).collect();
    frontier.push(Nearest(head));
    let mut marked: std::collections::HashSet<usize> = walk
        .reached
        .iter()
        .filter(|(_, visit)| visit.marked)
        .map(|(bit, _)| *bit)
        .collect();
    let mut continuation = Vec::new();
    for extra in 0..continuation_expansions {
        let Some(Nearest(candidate)) = frontier.pop() else {
            break;
        };
        let layer = &mut open[candidate.layer as usize];
        let listed: Vec<(PointId, usize)> =
            match layer.neighbours(candidate.point, 0, &mut *hops, acquire, before_source_entry)? {
                Some(neighbours) => (0..neighbours.len())
                    .map(|hop| neighbours.get(hop))
                    .collect(),
                None => continue,
            };
        for (point, origin) in listed {
            let Some(bit) = layer.bit(point) else {
                continue;
            };
            if !marked.insert(bit) {
                continue;
            }
            let Some(row_id) = layer.row(origin, before_source_entry)? else {
                continue;
            };
            if !is_allowed(layer.partition, row_id) {
                continue;
            }
            let distance = match walk.reached.get(&bit).and_then(PointVisit::distance) {
                Some(distance) => distance,
                None => match layer.distance(
                    point,
                    query,
                    before_source_entry,
                    before_distance,
                    acquire,
                    release,
                )? {
                    Some(distance) => distance,
                    None => continue,
                },
            };
            frontier.push(Nearest(Reached {
                distance,
                row_id,
                layer: candidate.layer,
                point,
                origin,
            }));
            continuation.push(UnionWalkEvaluation {
                row_id: row_id.0,
                distance,
                expansion: expansions + 1 + extra,
                parent: Some(candidate.row_id.0),
            });
        }
    }
    trace_with(|state| {
        if let Some(trace) = state.current.as_mut() {
            trace.stop = Some(stop);
            trace.frontier_at_stop = frontier_at_stop;
            trace.continuation = continuation;
        }
    });
    Ok(())
}

#[cfg(test)]
mod canonical_tie_tests {
    use super::*;

    /// A filtered walk that exhausts its graph reaches a shared point whose
    /// own row is allowed and whose many members are not, while the supplied
    /// candidates belong to other points: counting probes every candidate
    /// against that group and finds no member. Every probe -- a miss too -- is
    /// a charged, stoppable work checkpoint: no two probes run without one
    /// between them, a stop at any charge returns the stop with every scratch
    /// byte given back, and a retry completes.
    #[cfg(feature = "test-seams")]
    #[test]
    fn unsuccessful_member_probes_of_the_disconnected_count_are_each_charged() {
        use contextdb_core::{Lsn, TxId};
        let entry = |row: u64, values: Vec<f32>| StoredVectorEntry {
            membership: crate::store::VectorMembershipProvenance::new(
                TxId(1),
                Lsn(row),
                TxId(1),
                None,
            ),
            canonical_deleted_tx: None,
            row_id: RowId(row),
            vector: StoredVector::from_f32(&values, VectorQuantization::F32),
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(row),
        };
        let spread = |row: u64| {
            (0..8)
                .map(|axis| {
                    let mixed = (row + 17)
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .wrapping_add((axis as u64 + 31).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                    ((mixed ^ (mixed >> 29)) % 100_003) as f32 / 100_003.0 - 0.5
                })
                .collect::<Vec<_>>()
        };
        let mut entries = (1..=250)
            .map(|row| entry(row, vec![1.0; 8]))
            .collect::<Vec<_>>();
        entries.extend((251..=300).map(|row| entry(row, spread(row))));
        let graph = HnswIndex::new(&entries, 8, VectorQuantization::F32);
        // The shared point's own row and the distinct rows are allowed; its
        // 249 members are not, and outnumber the candidates.
        let candidates = std::iter::once(1_u64).chain(251..=300).collect::<Vec<_>>();
        let allowed = |row: RowId| row.0 == 1 || row.0 > 250;
        let layers = [HnswUnionLayer {
            covered_lsn: None,
            graph: &graph,
            partition: 0,
            allowed_count: candidates.len(),
            live_count: candidates.len(),
            seed_rows: Some(UnionSeedRows::Slice(&candidates)),
        }];
        let query = spread(1_000);
        let search = |stop_at: Option<usize>| {
            let calls = std::cell::Cell::new(0usize);
            let ledger = std::cell::Cell::new(0isize);
            let mut query_scores = BoundedHnswQuerySelfScores::new(&query);
            let answer = super::super::with_hnsw_unlinked_rows_for_test(&[300], || {
                search_union_allowed_bounded(
                    &layers,
                    &query,
                    10,
                    400,
                    usize::MAX,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: usize::MAX,
                        max_vector_evaluations: usize::MAX,
                    },
                    true,
                    false,
                    &mut query_scores,
                    |_, row| allowed(row),
                    |_, _, _, _| Ok::<_, Error>(None),
                    || {
                        calls.set(calls.get() + 1);
                        if stop_at == Some(calls.get()) {
                            return Err(Error::ReadCancelled);
                        }
                        Ok(())
                    },
                    || Ok::<(), Error>(()),
                    |event| {
                        crate::mem::apply_hnsw_scratch_acquire(event, &mut |bytes| {
                            ledger.set(ledger.get() + bytes as isize);
                            Ok::<(), Error>(())
                        })
                    },
                    |event| {
                        crate::mem::apply_hnsw_scratch_release(event, &mut |bytes| {
                            ledger.set(ledger.get() - bytes as isize)
                        })
                    },
                )
            });
            (answer, calls.get(), ledger.get())
        };
        take_most_member_reads_between_checkpoints_for_test();
        take_member_visits_for_test();
        let (answer, calls, ledger) = search(None);
        let answer = answer.expect("the search completes");
        let probes = take_member_visits_for_test();
        assert!(
            !answer.disconnected.is_empty(),
            "the counting found the cut-off row"
        );
        assert!(
            probes >= candidates.len() as u64,
            "every candidate was probed: {probes}"
        );
        assert_eq!(
            take_most_member_reads_between_checkpoints_for_test(),
            1,
            "a work checkpoint precedes every probe, a miss included"
        );
        assert_eq!(
            ledger, answer.retained_bytes as isize,
            "only the answer stays charged"
        );
        for stop_at in 1..=calls {
            let (stopped, _, ledger) = search(Some(stop_at));
            assert!(
                matches!(stopped, Err(Error::ReadCancelled)),
                "stop at {stop_at}"
            );
            assert_eq!(
                ledger, 0,
                "stop at {stop_at}: every scratch byte given back"
            );
        }
        let (retried, _, _) = search(None);
        assert_eq!(
            retried.expect("a retry completes").candidates.len(),
            answer.candidates.len()
        );
    }

    /// A large shared point beside a row the graph does not connect: the
    /// counting that follows the walk grows its row list member by member.
    /// Every row a growth copies is charged to the statement's work control,
    /// any work charge can stop it with every scratch byte given back, and a
    /// refused growth reservation refuses the search with nothing left charged.
    #[cfg(feature = "test-seams")]
    #[test]
    fn member_growth_of_the_disconnected_count_is_charged_stoppable_and_refusable() {
        use contextdb_core::{Lsn, TxId};
        let entry = |row: u64, values: Vec<f32>| StoredVectorEntry {
            membership: crate::store::VectorMembershipProvenance::new(
                TxId(1),
                Lsn(row),
                TxId(1),
                None,
            ),
            canonical_deleted_tx: None,
            row_id: RowId(row),
            vector: StoredVector::from_f32(&values, VectorQuantization::F32),
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(row),
        };
        let spread = |row: u64| {
            (0..8)
                .map(|axis| {
                    let mixed = (row + 17)
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .wrapping_add((axis as u64 + 31).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                    ((mixed ^ (mixed >> 29)) % 100_003) as f32 / 100_003.0 - 0.5
                })
                .collect::<Vec<_>>()
        };
        let mut entries = (1..=250)
            .map(|row| entry(row, vec![1.0; 8]))
            .collect::<Vec<_>>();
        entries.extend((251..=300).map(|row| entry(row, spread(row))));
        let graph = HnswIndex::new(&entries, 8, VectorQuantization::F32);
        let layers = [HnswUnionLayer {
            covered_lsn: None,
            graph: &graph,
            partition: 0,
            allowed_count: entries.len(),
            live_count: entries.len(),
            seed_rows: None,
        }];
        let query = spread(1_000);
        // One search with the row 300 cut off. `stop_at` fails the work
        // control at that call; `refuse` refuses any reservation above 1 GiB.
        let search = |stop_at: Option<usize>, refuse: bool| {
            let calls = std::cell::Cell::new(0usize);
            let ledger = std::cell::Cell::new(0isize);
            let growths = std::cell::RefCell::new(Vec::<(usize, usize)>::new());
            let mut query_scores = BoundedHnswQuerySelfScores::new(&query);
            let answer = super::super::with_hnsw_unlinked_rows_for_test(&[300], || {
                search_union_allowed_bounded(
                    &layers,
                    &query,
                    10,
                    400,
                    usize::MAX,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: usize::MAX,
                        max_vector_evaluations: usize::MAX,
                    },
                    true,
                    false,
                    &mut query_scores,
                    |_, _| true,
                    |_, _, _, _| Ok::<_, Error>(None),
                    || {
                        calls.set(calls.get() + 1);
                        if stop_at == Some(calls.get()) {
                            return Err(Error::ReadCancelled);
                        }
                        Ok(())
                    },
                    || Ok::<(), Error>(()),
                    |event| {
                        if let HnswSearchScratchEvent::Reserve {
                            container: HnswSearchScratchContainer::Result,
                            previous_capacity,
                            requested_capacity,
                            element_bytes,
                        } = event
                            && element_bytes == std::mem::size_of::<(usize, RowId)>()
                        {
                            // A growth reserves its replacement buffer whole.
                            let _ = previous_capacity;
                            growths.borrow_mut().push((calls.get(), requested_capacity));
                            if refuse
                                && (requested_capacity - previous_capacity) * element_bytes
                                    > 1 << 30
                            {
                                return Err(Error::Other("refused growth".into()));
                            }
                        }
                        crate::mem::apply_hnsw_scratch_acquire(event, &mut |bytes| {
                            ledger.set(ledger.get() + bytes as isize);
                            Ok::<(), Error>(())
                        })
                    },
                    |event| {
                        crate::mem::apply_hnsw_scratch_release(event, &mut |bytes| {
                            ledger.set(ledger.get() - bytes as isize)
                        })
                    },
                )
            });
            (answer, calls.get(), ledger.get(), growths.into_inner())
        };
        take_member_growths();
        take_member_visits_for_test();
        let (answer, calls, ledger, growths) = search(None, false);
        let answer = answer.expect("the search completes");
        // Only the growths a member caused.
        let member_growths = take_member_growths();
        // Each member growth from capacity `previous` is the next reservation
        // of this element size asking for more than `previous`.
        let mut events = growths.into_iter();
        let growths = member_growths
            .iter()
            .filter_map(|previous| {
                events
                    .by_ref()
                    .find(|(_, requested)| requested > previous)
                    .map(|(at, _)| (at, *previous))
            })
            .collect::<Vec<_>>();
        assert!(
            !answer.disconnected.is_empty(),
            "the counting found the cut-off row"
        );
        assert_eq!(
            ledger, answer.retained_bytes as isize,
            "only the answer stays charged"
        );
        assert!(
            growths.len() >= 3,
            "member growths of the row list: {growths:?}"
        );
        // Between two growths the work control is charged for the rows the
        // first growth copied and the members appended since: together, the
        // second growth's previous capacity.
        for pair in growths.windows(2) {
            assert!(
                pair[1].0 - pair[0].0 >= pair[1].1,
                "work charged between growths {pair:?} covers the copied and appended rows"
            );
        }
        // A stop at any work charge across the growths returns the stop and
        // gives back every scratch byte.
        let (first, last) = (
            growths[0].0,
            growths.last().unwrap().0 + growths.last().unwrap().1,
        );
        for stop_at in first..=last.min(calls) {
            let (answer, _, ledger, _) = search(Some(stop_at), false);
            assert!(
                matches!(answer, Err(Error::ReadCancelled)),
                "stop at {stop_at}"
            );
            assert_eq!(
                ledger, 0,
                "stop at {stop_at}: every scratch byte given back"
            );
        }
        // A refused member growth refuses the search, charges nothing, and a
        // retry completes.
        refuse_member_growth_once_for_test();
        let (refused, _, ledger, _) = search(None, true);
        assert!(refused.is_err(), "the refused growth refuses the search");
        assert_eq!(ledger, 0, "the refused search charges nothing");
        let (retried, _, _, _) = search(None, true);
        let retried = retried.expect("a retry completes");
        assert_eq!(retried.candidates.len(), answer.candidates.len());
    }

    #[test]
    fn opening_and_expansion_share_the_original_per_index_cap() {
        use contextdb_core::{Lsn, TxId};

        let vector = |ordinal: usize| {
            (0..16)
                .map(|axis| {
                    let mixed = (ordinal as u64 + 17)
                        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                        .wrapping_add((axis as u64 + 31).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                    ((mixed ^ (mixed >> 29)) % 100_003) as f32 / 100_003.0 - 0.5
                })
                .collect::<Vec<_>>()
        };
        let entries = (0..96)
            .map(|ordinal| StoredVectorEntry {
                membership: crate::store::VectorMembershipProvenance::new(
                    TxId(1),
                    Lsn(1),
                    TxId(1),
                    None,
                ),
                canonical_deleted_tx: None,
                row_id: RowId(ordinal as u64 + 1),
                vector: StoredVector::from_f32(&vector(ordinal), VectorQuantization::F32),
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            })
            .collect::<Vec<_>>();
        let graph = HnswIndex::new(&entries, 16, VectorQuantization::F32);
        let layers = [HnswUnionLayer {
            covered_lsn: None,
            graph: &graph,
            partition: 0,
            allowed_count: entries.len(),
            live_count: entries.len(),
            seed_rows: None,
        }];
        let query = vector(1000);
        let mut saw_expansion = false;
        let mut saw_refusal = false;
        for budget in 1..=96 {
            let mut query_scores = BoundedHnswQuerySelfScores::new(&query);
            let distances = std::cell::Cell::new(0usize);
            let answer = search_union_allowed_bounded(
                &layers,
                &query,
                10,
                32,
                budget,
                HnswAllowedSearchLimits {
                    max_visited_nodes: usize::MAX,
                    max_vector_evaluations: usize::MAX,
                },
                true,
                false,
                &mut query_scores,
                |_, _| true,
                |_, _, _, _| Ok::<_, Error>(None),
                || Ok::<(), Error>(()),
                || {
                    distances.set(distances.get() + 1);
                    Ok::<(), Error>(())
                },
                |_| Ok::<(), Error>(()),
                |_| {},
            )
            .unwrap();
            let spent = answer.admission_evaluations + answer.expansion_evaluations;
            assert!(spent <= budget, "budget={budget}, spent={spent}");
            assert_eq!(distances.get(), spent, "all distances use the same cap");
            saw_expansion |= answer.expansion_evaluations > 0;
            saw_refusal |= answer.terminal == UnionTerminal::Refused;
        }
        assert!(saw_expansion, "the cap includes an actual expansion");
        assert!(saw_refusal, "an unfunded walk refuses");
    }

    #[test]
    fn a_full_result_heap_replaces_larger_row_ids_at_equal_distance() {
        let mut walk = Walk {
            reached: HashMap::new(),
            visit_bytes: 0,
            frontier: BinaryHeap::new(),
            frontier_capacity: 0,
            frontier_charged: 0,
            kept: BinaryHeap::new(),
            kept_capacity: 3,
            kept_rows: HashMap::new(),
            kept_rows_bytes: 0,
            worst_ties: 0,
            failed_resolved_stop_distance: None,
            stop_distance_comparisons: std::cell::Cell::new(0),
            evaluations: 0,
            visited: 0,
            admissions: 0,
            evaluation_cap: 0,
            rescores: 0,
            rescore_cap: 0,
            visit_cap: 0,
            hops: HopScratch::default(),
            bridges: HopScratch::default(),
            visible_lsn: None,
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
                    origin: row_id as usize,
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
            origin: row_id as usize,
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
            origin: row_id as usize,
        };
        let mut observed = Vec::new();
        for breadth in [32_usize, 64, 128, 256] {
            let mut walk = Walk {
                reached: HashMap::new(),
                visit_bytes: 0,
                frontier: BinaryHeap::new(),
                frontier_capacity: 0,
                frontier_charged: 0,
                kept: BinaryHeap::new(),
                kept_capacity: breadth,
                kept_rows: HashMap::new(),
            kept_rows_bytes: 0,
                worst_ties: 0,
                failed_resolved_stop_distance: None,
                stop_distance_comparisons: std::cell::Cell::new(0),
                evaluations: 0,
                visited: 0,
                admissions: 0,
                evaluation_cap: 0,
                rescores: 0,
                rescore_cap: 0,
                visit_cap: 0,
                hops: HopScratch::default(),
                bridges: HopScratch::default(),
                visible_lsn: None,
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

#[cfg(test)]
mod partition_cap_tests {
    use super::*;

    #[test]
    fn each_partition_receives_its_full_declared_cap() {
        let mut grants = Vec::new();
        let result = merge_layer_searches(2, 10, 0, |position, cap, spent| {
            grants.push((position, cap, spent));
            Ok::<_, ()>(HnswUnionSearchResult {
                terminal: UnionTerminal::FrontierExhausted,
                candidates: Vec::new(),
                disconnected: Vec::new(),
                retained_bytes: 0,
                status: HnswAllowedSearchStatus::Complete,
                below_admission_floor: None,
                budget: cap,
                admission_evaluations: 2,
                expansion_evaluations: 6,
                rescore_evaluations: 0,
                allowed_admissions: 2,
                #[cfg(feature = "test-seams")]
                expansion_cap: cap,
                layers_admitted: 1,
                frontier_exhausted: true,
            })
        })
        .unwrap();
        assert_eq!(grants, [(0, 10, 0), (1, 10, 8)]);
        assert_eq!(result.budget, 20);
        assert_eq!(
            result.admission_evaluations + result.expansion_evaluations,
            16
        );
    }
}

#[cfg(test)]
mod search_radius_tests {
    use super::*;

    fn walk(capacity: usize) -> Walk<'static> {
        Walk {
            reached: HashMap::new(),
            visit_bytes: 0,
            frontier: BinaryHeap::new(),
            frontier_capacity: 0,
            frontier_charged: 0,
            kept: BinaryHeap::new(),
            kept_capacity: capacity,
            kept_rows: HashMap::new(),
            kept_rows_bytes: 0,
            worst_ties: 0,
            failed_resolved_stop_distance: None,
            stop_distance_comparisons: std::cell::Cell::new(0),
            evaluations: 0,
            visited: 0,
            admissions: 0,
            evaluation_cap: 0,
            rescores: 0,
            rescore_cap: 0,
            visit_cap: 0,
            hops: HopScratch::default(),
            bridges: HopScratch::default(),
            visible_lsn: None,
        }
    }

    /// A reached point whose row, point and origin all take `ordinal`.
    fn reached(distance: f32, ordinal: u64, layer: u32) -> Reached {
        Reached {
            distance,
            row_id: RowId(ordinal),
            layer,
            point: PointId(0, ordinal as i32),
            origin: ordinal as usize,
        }
    }

    fn keep(walk: &mut Walk<'_>, distance: f32, ordinal: u64) {
        walk.keep(Kept {
            reached: reached(distance, ordinal, 0),
            partition: 0,
            row_id: RowId(ordinal),
        });
    }

    #[test]
    fn only_the_edge_row_itself_is_inside_a_positive_radius() {
        let mut walk = walk(3);
        keep(&mut walk, 0.1, 1);
        keep(&mut walk, 0.2, 2);
        keep(&mut walk, 0.5, 5);
        assert_eq!(walk.worst_ties, 1);
        let mut settled_calls = 0usize;
        let mut settled = |_| {
            settled_calls += 1;
            0
        };

        // The edge's only row, popped again: inside any positive radius,
        // settled by the two strictly nearer rows at radius zero.
        assert!(!walk.frontier_ends_walk(reached(0.5, 5, 0), 2, 0.02, &mut settled));
        assert!(walk.frontier_ends_walk(reached(0.5, 5, 0), 2, 0.0, &mut settled));
        // Another row at the edge's distance is a tie, not the edge row:
        // the tie proof decides it whatever the radius.
        assert!(walk.frontier_ends_walk(reached(0.5, 6, 0), 2, 0.02, &mut settled));
        // The same row id reached through another layer is not the kept point.
        assert!(walk.frontier_ends_walk(reached(0.5, 5, 1), 2, 0.02, &mut settled));
        // The tie proof needs three strictly nearer rows here and has two.
        assert!(!walk.frontier_ends_walk(reached(0.5, 6, 0), 3, 0.02, &mut settled));
        assert_eq!(
            settled_calls, 1,
            "earlier partitions are asked only when kept rows fall short"
        );
        assert_eq!(walk.stop_distance_comparisons.get(), 5);
    }

    #[test]
    fn a_farther_head_ends_the_walk_only_beyond_the_radius() {
        let mut walk = walk(2);
        keep(&mut walk, 0.1, 1);
        keep(&mut walk, 0.5, 2);
        let never = |_| -> usize { panic!("a farther head never consults earlier partitions") };
        assert!(!walk.frontier_ends_walk(reached(0.509, 9, 0), 1, 0.02, never));
        assert!(walk.frontier_ends_walk(reached(0.511, 9, 0), 1, 0.02, never));
        assert!(walk.frontier_ends_walk(reached(0.5001, 9, 0), 1, 0.0, never));
        assert!(!walk.frontier_ends_walk(reached(0.4, 9, 0), 1, 0.02, never));
        assert_eq!(walk.stop_distance_comparisons.get(), 4);
    }

    #[test]
    fn a_tied_plateau_counts_rows_earlier_partitions_returned() {
        let mut walk = walk(3);
        keep(&mut walk, 0.1, 1);
        keep(&mut walk, 0.5, 2);
        keep(&mut walk, 0.5, 3);
        assert_eq!(walk.worst_ties, 2);
        let mut asked = Vec::new();
        // One kept row is strictly nearer; two more are needed.
        assert!(
            !walk.frontier_ends_walk(reached(0.5, 2, 0), 3, 0.02, |distance| {
                asked.push(distance);
                1
            })
        );
        assert!(
            walk.frontier_ends_walk(reached(0.5, 2, 0), 3, 0.02, |distance| {
                asked.push(distance);
                2
            })
        );
        assert_eq!(asked, [0.5, 0.5]);
    }
}

#[cfg(test)]
mod settled_scratch_tests {
    use super::*;
    use contextdb_core::{Lsn, TxId};
    use std::cell::Cell;

    fn vector(ordinal: usize) -> Vec<f32> {
        (0..16)
            .map(|axis| {
                let mixed = (ordinal as u64 + 17)
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    .wrapping_add((axis as u64 + 31).wrapping_mul(0xbf58_476d_1ce4_e5b9));
                ((mixed ^ (mixed >> 29)) % 100_003) as f32 / 100_003.0 - 0.5
            })
            .collect()
    }

    fn entries(first_row: u64) -> Vec<StoredVectorEntry> {
        (0..96)
            .map(|ordinal| StoredVectorEntry {
                membership: crate::store::VectorMembershipProvenance::new(
                    TxId(1),
                    Lsn(1),
                    TxId(1),
                    None,
                ),
                canonical_deleted_tx: None,
                row_id: RowId(first_row + ordinal as u64),
                vector: StoredVector::from_f32(
                    &vector(ordinal + first_row as usize),
                    VectorQuantization::F32,
                ),
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            })
            .collect()
    }

    /// What one two-partition search charged as merge scratch, and whether
    /// its caller refused that charge.
    struct Ledger {
        /// Refuse the merge reservation of this element size.
        refuse_element_bytes: Option<usize>,
        searching: Cell<usize>,
        merge_balance: Cell<isize>,
        merge_reserves_by_partition: [Cell<usize>; 2],
        refused_in_partition: Cell<Option<usize>>,
        partition_balance: [Cell<isize>; 2],
    }

    fn search(refuse_element_bytes: Option<usize>) -> (Result<HnswUnionSearchResult>, Ledger) {
        let first = entries(1);
        let second = entries(1_001);
        let graphs = [
            HnswIndex::new(&first, 16, VectorQuantization::F32),
            HnswIndex::new(&second, 16, VectorQuantization::F32),
        ];
        let ledger = Ledger {
            refuse_element_bytes,
            searching: Cell::new(0),
            merge_balance: Cell::new(0),
            merge_reserves_by_partition: [Cell::new(0), Cell::new(0)],
            refused_in_partition: Cell::new(None),
            partition_balance: [Cell::new(0), Cell::new(0)],
        };
        let query = vector(5_000);
        let result = merge_layer_searches_with_settled(
            2,
            usize::MAX / 4,
            10,
            |position, cap, _, settled| {
                ledger.searching.set(position);
                let layers = [HnswUnionLayer {
                    covered_lsn: None,
                    graph: &graphs[position],
                    partition: position,
                    allowed_count: 96,
                    live_count: 96,
                    seed_rows: None,
                }];
                let mut query_scores = BoundedHnswQuerySelfScores::new(&query);
                search_union_allowed_bounded_with_navigation(
                    &layers,
                    &query,
                    10,
                    32,
                    cap,
                    HnswAllowedSearchLimits {
                        max_visited_nodes: usize::MAX,
                        max_vector_evaluations: usize::MAX,
                    },
                    true,
                    false,
                    &mut query_scores,
                    |_, _| true,
                    |_, _, _, _| Ok::<_, Error>(None),
                    || Ok::<(), Error>(()),
                    || Ok::<(), Error>(()),
                    |event| {
                        let partition = ledger.searching.get();
                        if let HnswSearchScratchEvent::Reserve {
                            container,
                            previous_capacity,
                            requested_capacity,
                            element_bytes,
                        } = event
                        {
                            let bytes =
                                ((requested_capacity - previous_capacity) * element_bytes) as isize;
                            if container == HnswSearchScratchContainer::MergeResults {
                                ledger.merge_reserves_by_partition[partition]
                                    .set(ledger.merge_reserves_by_partition[partition].get() + 1);
                                if ledger.refuse_element_bytes == Some(element_bytes) {
                                    ledger.refused_in_partition.set(Some(partition));
                                    return Err(Error::Other(("merge scratch refused".to_string()).into()));
                                }
                                ledger.merge_balance.set(ledger.merge_balance.get() + bytes);
                            }
                            ledger.partition_balance[partition]
                                .set(ledger.partition_balance[partition].get() + bytes);
                        }
                        Ok(())
                    },
                    |event| {
                        let partition = ledger.searching.get();
                        if let HnswSearchScratchEvent::Release {
                            container,
                            capacity,
                            element_bytes,
                        } = event
                        {
                            let bytes = (capacity * element_bytes) as isize;
                            if container == HnswSearchScratchContainer::MergeResults {
                                ledger.merge_balance.set(ledger.merge_balance.get() - bytes);
                            }
                            ledger.partition_balance[partition]
                                .set(ledger.partition_balance[partition].get() - bytes);
                        }
                    },
                    None,
                    0.02,
                    settled,
                    None,
                )
            },
        );
        (result, ledger)
    }

    /// The second partition's walk copies the rows the first returned into two
    /// arrays its stop proof reads. Both are charged as merge scratch before
    /// they are allocated and given back once they are dropped.
    #[test]
    fn the_settled_rows_copy_is_charged_and_given_back() {
        let (result, ledger) = search(None);
        let result = result.unwrap();
        assert!(!result.candidates.is_empty());
        assert_eq!(
            ledger.merge_reserves_by_partition[0].get(),
            0,
            "the first partition has no earlier rows to copy"
        );
        assert!(
            ledger.merge_reserves_by_partition[1].get() >= 2,
            "the second partition charges both settled arrays"
        );
        assert_eq!(
            ledger.merge_balance.get(),
            0,
            "every settled charge is given back"
        );
    }

    /// A caller that refuses either settled array stops the second partition's
    /// search before it allocates, and that search gives back everything it
    /// had charged.
    #[test]
    fn a_refused_settled_copy_stops_the_second_partition_inside_its_charge() {
        for refused in [
            std::mem::size_of::<usize>(),
            std::mem::size_of::<Option<Option<f32>>>(),
        ] {
            let (result, ledger) = search(Some(refused));
            assert!(result.is_err(), "the refusal reaches the caller");
            assert_eq!(ledger.refused_in_partition.get(), Some(1));
            assert_eq!(
                ledger.merge_balance.get(),
                0,
                "the admitted half of a refused pair is given back (refused {refused} bytes)"
            );
            assert_eq!(
                ledger.partition_balance[1].get(),
                0,
                "the refused partition's search keeps nothing charged (refused {refused} bytes)"
            );
        }
    }
}
