use crate::store::GraphStore;
use contextdb_core::*;
use contextdb_tx::{TransactionManager, WriteSetApplicator};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

/// Production BFS visited-node ceiling. Public so the boundary test can
/// assert the shipped constant while exercising the refusal itself through
/// the small test-only cap seam below.
pub const MAX_VISITED: usize = 100_000;
type AdjPair = (NodeId, NodeId);

#[derive(Debug)]
struct BoundedFrontierEntry {
    node: NodeId,
    depth: u32,
    path: Vec<(NodeId, EdgeType)>,
    retained_bytes: usize,
    /// Whether this node's own adjacency is walked when it is reached. An
    /// edge arriving at a node the traversal has already expanded is still an
    /// edge and is still reported; walking that node a second time would
    /// repeat the work and, on a cycle, never end.
    expand: bool,
}

#[derive(Debug)]
struct BoundedQueueEntry {
    frontier: BoundedFrontierEntry,
    next: Option<Box<BoundedQueueEntry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdjacencyPhase {
    Outgoing,
    /// The outgoing edges the reader's own open transaction has staged and
    /// not yet committed. They are in no adjacency map, so they are walked
    /// after the committed ones rather than among them.
    StagedOutgoing,
    Incoming,
    /// The incoming half of the same staged edges.
    StagedIncoming,
    Done,
}

impl AdjacencyPhase {
    /// The staged edges of a direction are walked straight after that
    /// direction's committed edges, so a transaction's own work is reached at
    /// the same depth as the work it sits beside.
    fn next(self, direction: Direction) -> Self {
        match (self, direction) {
            (AdjacencyPhase::Outgoing, _) => AdjacencyPhase::StagedOutgoing,
            (AdjacencyPhase::StagedOutgoing, Direction::Both) => AdjacencyPhase::Incoming,
            (AdjacencyPhase::Incoming, _) => AdjacencyPhase::StagedIncoming,
            _ => AdjacencyPhase::Done,
        }
    }

    fn is_staged(self) -> bool {
        matches!(
            self,
            AdjacencyPhase::StagedOutgoing | AdjacencyPhase::StagedIncoming
        )
    }

    /// Which end of a staged edge this phase reaches out from.
    fn direction(self) -> Direction {
        match self {
            AdjacencyPhase::Incoming | AdjacencyPhase::StagedIncoming => Direction::Incoming,
            _ => Direction::Outgoing,
        }
    }
}

#[derive(Debug)]
struct BoundedCurrentEntry {
    frontier: BoundedFrontierEntry,
    emitted: bool,
    phase: AdjacencyPhase,
    position: usize,
    /// Identity of the adjacency entry this continuation last inspected,
    /// paired with the slot it occupied. A suspended continuation resumes
    /// from this identity, never from `position` alone.
    anchor: Option<AdjacencyAnchor>,
}

/// Identity of one adjacency entry, owned by a suspended continuation. The
/// entry is named by its own contents -- endpoints, type, creating
/// transaction, LSN -- because adjacency vectors are physically compacted
/// while a continuation is parked: everything that survives beneath the
/// parked slot slides down, so a resume addressed by slot alone reads an
/// entry the caller has never seen and emits the ones in between to nobody.
#[derive(Debug)]
struct AdjacencyAnchor {
    source: NodeId,
    target: NodeId,
    edge_type: EdgeType,
    created_tx: TxId,
    lsn: Lsn,
    /// The slot this entry occupied when it was inspected, which is what a
    /// later relocation measures the slide against.
    position: usize,
    retained_bytes: usize,
}

impl AdjacencyAnchor {
    fn matches(&self, entry: &AdjEntry) -> bool {
        self.source == entry.source
            && self.target == entry.target
            && self.created_tx == entry.created_tx
            && self.lsn == entry.lsn
            && self.edge_type == entry.edge_type
    }
}

/// Where a suspended continuation resumes inside an adjacency vector that
/// may have been compacted beneath it.
enum AdjacencyResume {
    /// The anchored entry still occupies its slot: the held position stands.
    Held,
    /// The anchored entry slid down; the continuation follows it by the same
    /// distance.
    Relocated {
        anchor_position: usize,
        position: usize,
    },
    /// The anchored entry is gone. Only an entry invisible at this
    /// continuation's snapshot is ever physically removed, so re-walking the
    /// key from its first entry re-filters deterministically: a neighbour
    /// already queued is in the visited set and cannot be queued twice, and
    /// nothing this snapshot can see is passed over.
    Restart,
}

/// Owned continuation for the request-bounded graph path. It stores no graph
/// lock or executor borrow, so a cursor may suspend it between fetches.
#[derive(Debug)]
pub struct BoundedBfsCursor {
    edge_types: Option<Vec<EdgeType>>,
    direction: Direction,
    min_depth: u32,
    max_depth: u32,
    snapshot: SnapshotId,
    // Membership does not participate in traversal order. HashSet avoids
    // sorted-vector shifting while exposing its live element capacity.
    visited: HashSet<NodeId>,
    queue_input: Option<Box<BoundedQueueEntry>>,
    queue_output: Option<Box<BoundedQueueEntry>>,
    current: Option<BoundedCurrentEntry>,
    retained_bytes: usize,
    exhausted: bool,
}

/// One staged edge as a traversal needs it: who it joins and by what name.
///
/// Deliberately not the whole entry. A walk never reads an edge's properties,
/// and carrying them would make a bounded read hold memory in proportion to
/// the size of the transaction it is reading inside rather than to the walk it
/// is doing.
#[derive(Debug, Clone)]
pub struct StagedTraversalEdge {
    pub source: NodeId,
    pub target: NodeId,
    pub edge_type: EdgeType,
}

/// One output clone and the exact temporary bytes reserved for it. The caller
/// releases `reserved_bytes` when the projected row is discarded or
/// published; continuation bytes remain owned by [`BoundedBfsCursor`].
#[derive(Debug)]
pub struct BoundedTraversalNode {
    pub node: TraversalNode,
    pub reserved_bytes: usize,
}

impl Drop for BoundedBfsCursor {
    fn drop(&mut self) {
        // The queue is a chain of boxes, so letting it fall on its own drops
        // each entry INSIDE the drop of the entry before it -- one stack frame
        // per queued node. A traversal abandoned while its queue is long (a
        // refusal, a deadline, a caller letting a cursor go) would recurse once
        // per entry and overflow the stack rather than report anything. Both
        // chains are taken apart iteratively instead: each successor is moved
        // into a local and falls there, so the depth is one however long the
        // queue is.
        for head in [self.queue_input.take(), self.queue_output.take()] {
            let mut next = head;
            while let Some(mut entry) = next {
                next = entry.next.take();
            }
        }
    }
}

impl BoundedBfsCursor {
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
        self.exhausted
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.snapshot
    }
}

fn checked_graph_add(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_add(right).ok_or_else(|| {
        Error::Other(format!(
            "bounded graph {operation} memory size exceeds the native address space"
        ))
    })
}

fn checked_graph_mul(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_mul(right).ok_or_else(|| {
        Error::Other(format!(
            "bounded graph {operation} memory size exceeds the native address space"
        ))
    })
}

fn bounded_capacity_bytes<T>(capacity: usize, operation: &str) -> Result<usize> {
    checked_graph_mul(capacity, std::mem::size_of::<T>(), operation)
}

fn reconcile_graph_allocation<E>(
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

fn bounded_graph_string<E>(
    source: &str,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(String, usize), E>
where
    E: From<Error>,
{
    let requested = source.len();
    if requested == 0 {
        return Ok((String::new(), 0));
    }
    before_retain(requested)?;
    let mut owned = String::new();
    if owned.try_reserve_exact(requested).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph edge type allocation failed".to_string(),
        )));
    }
    let actual = owned.capacity();
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph edge type capacity moved backwards".to_string(),
        )));
    }
    let actual = reconcile_graph_allocation(requested, actual, before_retain, release_retained)?;
    owned.push_str(source);
    Ok((owned, actual))
}

/// Find the anchored entry again after the vector beneath it was compacted.
/// Each candidate identity is charged before it is read, so relocation
/// spends the caller's declared adjacency ceiling like any other inspection.
fn relocate_adjacency_anchor<E>(
    anchor: &AdjacencyAnchor,
    position: usize,
    node_entries: &[AdjEntry],
    before_edge: &mut impl FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<AdjacencyResume, E>
where
    E: From<Error>,
{
    if node_entries
        .get(anchor.position)
        .is_some_and(|entry| anchor.matches(entry))
    {
        return Ok(AdjacencyResume::Held);
    }
    // The continuation stands either on the anchored entry itself or on the
    // slot right after it, so the distance to keep is measured from the
    // anchor rather than from the vector's start.
    let Some(offset) = position.checked_sub(anchor.position) else {
        return Ok(AdjacencyResume::Restart);
    };
    let Some(last) = node_entries.len().checked_sub(1) else {
        return Ok(AdjacencyResume::Restart);
    };
    let mut candidate = anchor.position.min(last);
    loop {
        before_edge()?;
        if anchor.matches(&node_entries[candidate]) {
            let position = candidate.checked_add(offset).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded graph adjacency position overflow".to_string(),
                ))
            })?;
            return Ok(AdjacencyResume::Relocated {
                anchor_position: candidate,
                position,
            });
        }
        let Some(next) = candidate.checked_sub(1) else {
            return Ok(AdjacencyResume::Restart);
        };
        candidate = next;
    }
}

/// Drop the continuation's anchor, giving its bytes back and keeping the
/// retained total exact.
fn release_current_anchor<E>(
    cursor: &mut BoundedBfsCursor,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    let Some(current) = cursor.current.as_mut() else {
        return Ok(());
    };
    let Some(anchor) = current.anchor.take() else {
        return Ok(());
    };
    cursor.retained_bytes = cursor
        .retained_bytes
        .checked_sub(anchor.retained_bytes)
        .ok_or_else(|| {
            E::from(Error::Other(
                "bounded graph adjacency anchor accounting underflow".to_string(),
            ))
        })?;
    release_retained(anchor.retained_bytes);
    Ok(())
}

/// Anchor the continuation on the entry it is about to inspect. The
/// replacement is charged before the anchor it supersedes is released, so a
/// refused charge leaves the continuation exactly as it was.
fn install_adjacency_anchor<E>(
    cursor: &mut BoundedBfsCursor,
    entry: &AdjEntry,
    position: usize,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    let (edge_type, anchor_bytes) =
        bounded_graph_string(&entry.edge_type, before_retain, release_retained)?;
    if let Err(error) = release_current_anchor(cursor, release_retained) {
        release_retained(anchor_bytes);
        return Err(error);
    }
    let next_retained = match checked_graph_add(cursor.retained_bytes, anchor_bytes, "anchor") {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(anchor_bytes);
            return Err(E::from(error));
        }
    };
    let Some(current) = cursor.current.as_mut() else {
        release_retained(anchor_bytes);
        return Err(E::from(Error::Other(
            "bounded graph continuation lost its anchored adjacency".to_string(),
        )));
    };
    current.anchor = Some(AdjacencyAnchor {
        source: entry.source,
        target: entry.target,
        edge_type,
        created_tx: entry.created_tx,
        lsn: entry.lsn,
        position,
        retained_bytes: anchor_bytes,
    });
    cursor.retained_bytes = next_retained;
    Ok(())
}

fn bounded_graph_path<E>(
    source: &[(NodeId, EdgeType)],
    appended: Option<(NodeId, &str)>,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<(NodeId, EdgeType)>, usize), E>
where
    E: From<Error>,
{
    let length = source
        .len()
        .checked_add(usize::from(appended.is_some()))
        .ok_or_else(|| {
            E::from(Error::Other(
                "bounded graph path length overflow".to_string(),
            ))
        })?;
    let requested =
        bounded_capacity_bytes::<(NodeId, EdgeType)>(length, "path clone").map_err(E::from)?;
    if requested != 0 {
        before_retain(requested)?;
    }
    let mut path = Vec::new();
    if path.try_reserve_exact(length).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph path allocation failed".to_string(),
        )));
    }
    let actual_vec =
        match bounded_capacity_bytes::<(NodeId, EdgeType)>(path.capacity(), "path clone") {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(requested);
                return Err(E::from(error));
            }
        };
    if actual_vec < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph path capacity moved backwards".to_string(),
        )));
    }
    let actual_vec =
        reconcile_graph_allocation(requested, actual_vec, before_retain, release_retained)?;
    let mut retained = actual_vec;
    for (node, edge_type) in source {
        // A path clone copies one element per traversal step, so each copy is
        // charged before it happens rather than the whole path being copied
        // between two controls.
        if let Err(error) = before_work() {
            release_retained(retained);
            return Err(error);
        }
        let (edge_type, string_bytes) =
            match bounded_graph_string(edge_type, before_retain, release_retained) {
                Ok(owned) => owned,
                Err(error) => {
                    release_retained(retained);
                    return Err(error);
                }
            };
        retained = match checked_graph_add(retained, string_bytes, "path") {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(string_bytes);
                release_retained(retained);
                return Err(E::from(error));
            }
        };
        path.push((*node, edge_type));
    }
    if let Some((node, edge_type)) = appended {
        if let Err(error) = before_work() {
            release_retained(retained);
            return Err(error);
        }
        let (edge_type, string_bytes) =
            match bounded_graph_string(edge_type, before_retain, release_retained) {
                Ok(owned) => owned,
                Err(error) => {
                    release_retained(retained);
                    return Err(error);
                }
            };
        retained = match checked_graph_add(retained, string_bytes, "path") {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(string_bytes);
                release_retained(retained);
                return Err(E::from(error));
            }
        };
        path.push((node, edge_type));
    }
    Ok((path, retained))
}

fn bounded_graph_edge_types<E>(
    source: Option<&[EdgeType]>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Option<Vec<EdgeType>>, usize), E>
where
    E: From<Error>,
{
    let Some(source) = source else {
        return Ok((None, 0));
    };
    let requested =
        bounded_capacity_bytes::<EdgeType>(source.len(), "edge filters").map_err(E::from)?;
    if requested != 0 {
        before_retain(requested)?;
    }
    let mut filters = Vec::new();
    if filters.try_reserve_exact(source.len()).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph edge-filter allocation failed".to_string(),
        )));
    }
    let actual_vec = match bounded_capacity_bytes::<EdgeType>(filters.capacity(), "edge filters") {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(requested);
            return Err(E::from(error));
        }
    };
    if actual_vec < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph edge-filter capacity moved backwards".to_string(),
        )));
    }
    let actual_vec =
        reconcile_graph_allocation(requested, actual_vec, before_retain, release_retained)?;
    let mut retained = actual_vec;
    for edge_type in source {
        let (edge_type, string_bytes) =
            match bounded_graph_string(edge_type, before_retain, release_retained) {
                Ok(owned) => owned,
                Err(error) => {
                    release_retained(retained);
                    return Err(error);
                }
            };
        retained = match checked_graph_add(retained, string_bytes, "edge filters") {
            Ok(bytes) => bytes,
            Err(error) => {
                release_retained(string_bytes);
                release_retained(retained);
                return Err(E::from(error));
            }
        };
        filters.push(edge_type);
    }
    Ok((Some(filters), retained))
}

fn next_graph_capacity(current: usize, required: usize) -> Result<usize> {
    if current == 0 {
        return Ok(required);
    }
    Ok(current
        .checked_mul(2)
        .ok_or_else(|| Error::Other("bounded graph capacity overflow".to_string()))?
        .max(required))
}

fn bounded_graph_visited<E>(
    start: NodeId,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(HashSet<NodeId>, usize), E>
where
    E: From<Error>,
{
    let requested = bounded_capacity_bytes::<NodeId>(1, "visited nodes").map_err(E::from)?;
    before_retain(requested)?;
    let mut visited = HashSet::new();
    if visited.try_reserve(1).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph visited allocation failed".to_string(),
        )));
    }
    let actual = match bounded_capacity_bytes::<NodeId>(visited.capacity(), "visited nodes") {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(requested);
            return Err(E::from(error));
        }
    };
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph visited capacity moved backwards".to_string(),
        )));
    }
    let actual = reconcile_graph_allocation(requested, actual, before_retain, release_retained)?;
    visited.insert(start);
    Ok((visited, actual))
}

fn prepare_graph_visited_growth<E>(
    visited: &HashSet<NodeId>,
    neighbour: NodeId,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<Option<(HashSet<NodeId>, usize, usize)>, E>
where
    E: From<Error>,
{
    if visited.len() < visited.capacity() {
        return Ok(None);
    }
    let required = visited.len().checked_add(1).ok_or_else(|| {
        E::from(Error::Other(
            "bounded graph visited length overflow".to_string(),
        ))
    })?;
    let requested_capacity = next_graph_capacity(visited.capacity(), required).map_err(E::from)?;
    let requested =
        bounded_capacity_bytes::<NodeId>(requested_capacity, "visited nodes").map_err(E::from)?;
    before_retain(requested)?;
    let mut replacement = HashSet::new();
    if replacement.try_reserve(requested_capacity).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph visited allocation failed".to_string(),
        )));
    }
    let actual = match bounded_capacity_bytes::<NodeId>(replacement.capacity(), "visited nodes") {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(requested);
            return Err(E::from(error));
        }
    };
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph visited capacity moved backwards".to_string(),
        )));
    }
    let actual = reconcile_graph_allocation(requested, actual, before_retain, release_retained)?;
    let mut remaining = visited.len();
    let mut source = visited.iter();
    while remaining != 0 {
        if let Err(error) = before_work() {
            release_retained(actual);
            return Err(error);
        }
        let Some(node) = source.next().copied() else {
            release_retained(actual);
            return Err(E::from(Error::Other(
                "bounded graph visited set changed during migration".to_string(),
            )));
        };
        remaining = match remaining.checked_sub(1) {
            Some(remaining) => remaining,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other(
                    "bounded graph visited migration underflow".to_string(),
                )));
            }
        };
        replacement.insert(node);
    }
    if let Err(error) = before_work() {
        release_retained(actual);
        return Err(error);
    }
    replacement.insert(neighbour);
    let previous = match bounded_capacity_bytes::<NodeId>(visited.capacity(), "visited nodes") {
        Ok(bytes) => bytes,
        Err(error) => {
            release_retained(actual);
            return Err(E::from(error));
        }
    };
    Ok(Some((replacement, actual, previous)))
}

fn bounded_queue_pop(cursor: &mut BoundedBfsCursor) -> Option<BoundedFrontierEntry> {
    if cursor.queue_output.is_none() {
        while let Some(mut entry) = cursor.queue_input.take() {
            cursor.queue_input = entry.next.take();
            entry.next = cursor.queue_output.take();
            cursor.queue_output = Some(entry);
        }
    }
    let mut entry = cursor.queue_output.take()?;
    cursor.queue_output = entry.next.take();
    Some(entry.frontier)
}

fn bounded_edge_type_allowed<E>(
    filters: &Option<Vec<EdgeType>>,
    edge_type: &str,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<bool, E>
where
    E: From<Error>,
{
    let Some(filters) = filters.as_ref() else {
        return Ok(true);
    };
    let mut position = 0usize;
    while position < filters.len() {
        before_work()?;
        let filter = filters.get(position).ok_or_else(|| {
            E::from(Error::Other(
                "bounded graph edge-filter position changed".to_string(),
            ))
        })?;
        position = position.checked_add(1).ok_or_else(|| {
            E::from(Error::Other(
                "bounded graph edge-filter position overflow".to_string(),
            ))
        })?;
        if filter.as_str() == edge_type {
            return Ok(true);
        }
    }
    Ok(false)
}

pub struct MemGraphExecutor<S: WriteSetApplicator> {
    store: Arc<GraphStore>,
    tx_mgr: Arc<TransactionManager<S>>,
    dag_edge_types: parking_lot::RwLock<HashSet<String>>,
    // The active BFS visited cap. Production value is always `MAX_VISITED`;
    // only the #[doc(hidden)] test seam below ever lowers it, so the boundary
    // refusal is testable without building 100k edges. Cost: one relaxed
    // atomic load per newly-visited node (declared always-on).
    bfs_visited_cap: std::sync::atomic::AtomicUsize,
}

impl<S: WriteSetApplicator> MemGraphExecutor<S> {
    pub fn new(store: Arc<GraphStore>, tx_mgr: Arc<TransactionManager<S>>) -> Self {
        Self {
            store,
            tx_mgr,
            dag_edge_types: parking_lot::RwLock::new(HashSet::new()),
            bfs_visited_cap: std::sync::atomic::AtomicUsize::new(MAX_VISITED),
        }
    }

    /// Test-only: lower the BFS visited cap so the `BfsVisitedExceeded`
    /// boundary is exercisable without a 100k-edge build. No shipped surface
    /// calls this; production always runs at [`MAX_VISITED`].
    #[doc(hidden)]
    pub fn __set_bfs_visited_cap_for_test(&self, cap: usize) {
        self.bfs_visited_cap
            .store(cap, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn register_dag_edge_types(&self, types: &[String]) {
        let mut set = self.dag_edge_types.write();
        for t in types {
            set.insert(t.clone());
        }
    }

    pub fn edge_count(&self, source: NodeId, edge_type: &str, snapshot: SnapshotId) -> usize {
        let fwd = self.store.forward_adj.read();
        fwd.get(&source)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| entry.edge_type == edge_type && entry.visible_at(snapshot))
                    .count()
            })
            .unwrap_or(0)
    }

    fn bfs_with_write_set(
        &self,
        tx: TxId,
        start: NodeId,
        goal: NodeId,
        edge_type: &str,
    ) -> Result<bool> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        visited.insert(start);
        queue.push_back(start);

        let (ws_inserts, ws_deletes): (Vec<AdjPair>, HashSet<AdjPair>) =
            self.tx_mgr.with_write_set(tx, |ws| {
                let inserts = ws
                    .adj_inserts
                    .iter()
                    .filter(|e| e.edge_type == edge_type)
                    .map(|e| (e.source, e.target))
                    .collect();
                let deletes = ws
                    .adj_deletes
                    .iter()
                    .filter(|(_, et, _, _)| et == edge_type)
                    .map(|(s, _, t, _)| (*s, *t))
                    .collect();
                (inserts, deletes)
            })?;

        while let Some(current) = queue.pop_front() {
            {
                let fwd = self.store.forward_adj.read();
                if let Some(entries) = fwd.get(&current) {
                    for e in entries {
                        if e.edge_type != edge_type || e.deleted_tx.is_some() {
                            continue;
                        }
                        if ws_deletes.contains(&(e.source, e.target)) {
                            continue;
                        }
                        if e.target == goal {
                            return Ok(true);
                        }
                        if visited.insert(e.target) {
                            queue.push_back(e.target);
                        }
                    }
                }
            }

            for (src, tgt) in &ws_inserts {
                if *src == current {
                    if *tgt == goal {
                        return Ok(true);
                    }
                    if visited.insert(*tgt) {
                        queue.push_back(*tgt);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Create an owned traversal continuation. Every allocation is admitted
    /// and reconciled before the cursor is published.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_bfs_cursor<E>(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedBfsCursor, E>
    where
        E: From<Error>,
    {
        let (edge_types, edge_types_bytes) =
            bounded_graph_edge_types(edge_types, &mut before_retain, &mut release_retained)?;
        let (visited, visited_bytes) =
            match bounded_graph_visited(start, &mut before_retain, &mut release_retained) {
                Ok(visited) => visited,
                Err(error) => {
                    release_retained(edge_types_bytes);
                    return Err(error);
                }
            };
        let queue_bytes = std::mem::size_of::<BoundedQueueEntry>();
        let retained_bytes =
            match checked_graph_add(visited_bytes, edge_types_bytes, "initial state")
                .and_then(|bytes| checked_graph_add(bytes, queue_bytes, "initial frontier"))
            {
                Ok(bytes) => bytes,
                Err(error) => {
                    release_retained(visited_bytes);
                    release_retained(edge_types_bytes);
                    return Err(E::from(error));
                }
            };
        if let Err(error) = before_retain(queue_bytes) {
            release_retained(visited_bytes);
            release_retained(edge_types_bytes);
            return Err(error);
        }
        let queue_input = Some(Box::new(BoundedQueueEntry {
            frontier: BoundedFrontierEntry {
                node: start,
                depth: 0,
                path: Vec::new(),
                retained_bytes: 0,
                expand: true,
            },
            next: None,
        }));
        Ok(BoundedBfsCursor {
            edge_types,
            direction,
            min_depth,
            max_depth,
            snapshot,
            visited,
            queue_input,
            queue_output: None,
            current: None,
            retained_bytes,
            exhausted: false,
        })
    }

    /// Pull the next traversal output while retaining exact continuation
    /// state. Every callback is fallible and runs before its corresponding
    /// source inspection, allocation, clone, or insertion.
    /// Advance one bounded traversal over the committed graph.
    ///
    /// The walk a reader with no transaction open does: there is nothing
    /// staged to reach, so it is the same walk with nothing to add.
    pub fn bounded_bfs_next<E>(
        &self,
        cursor: &mut BoundedBfsCursor,
        before_edge: impl FnMut() -> std::result::Result<(), E>,
        before_path_element: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        edge_allowed: impl FnMut(NodeId, NodeId, NodeId, &str) -> std::result::Result<bool, E>,
    ) -> std::result::Result<Option<BoundedTraversalNode>, E>
    where
        E: From<Error>,
    {
        self.bounded_bfs_next_including_staged(
            cursor,
            before_edge,
            before_path_element,
            before_retain,
            release_retained,
            edge_allowed,
            |_node: NodeId,
             _direction: Direction,
             _position: usize|
             -> std::result::Result<Option<(StagedTraversalEdge, usize, usize)>, E> {
                Ok(None)
            },
            // Nobody is counting entries on this door; the walk that wants the
            // figure asks through the staged one.
            || {},
            // …and nobody on this door keeps the standing ceiling: the caller
            // that keeps it asks through the staged walk too.
            None,
        )
    }

    /// The same walk, plus the edges the reader's own open transaction has
    /// staged and not yet committed.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_bfs_next_including_staged<E>(
        &self,
        cursor: &mut BoundedBfsCursor,
        mut before_edge: impl FnMut() -> std::result::Result<(), E>,
        mut before_path_element: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        mut edge_allowed: impl FnMut(NodeId, NodeId, NodeId, &str) -> std::result::Result<bool, E>,
        // The edges the reader's own open transaction has staged, asked for
        // one at a time and RESUMED rather than re-scanned: the index goes in
        // and the index to come back to comes out, so a walk that stops and
        // starts does not begin the transaction again. What comes back is the
        // edge a traversal needs, never its properties, so a walk inside a
        // transaction holds no more than a walk outside one. `None` ends that
        // direction's staged edges.
        mut staged_edge: impl FnMut(
            NodeId,
            Direction,
            usize,
        ) -> std::result::Result<
            Option<(StagedTraversalEdge, usize, usize)>,
            E,
        >,
        // Called once for each adjacency entry this walk actually reads,
        // however many steps it then takes over that entry. What a caller
        // counts as "looked at" is entries, not steps.
        mut entry_read: impl FnMut(),
        // The visited-node ceiling this walk answers to, when it has one.
        // `None` is a walk governed by its caller's own budget and is the
        // shape this cursor was built for. `Some(cap)` is a caller that
        // declared no budget of its own and therefore keeps the standing
        // ceiling, refused with the same typed error and the same number the
        // eager walk refuses with.
        visited_cap: Option<usize>,
    ) -> std::result::Result<Option<BoundedTraversalNode>, E>
    where
        E: From<Error>,
    {
        if cursor.exhausted {
            return Ok(None);
        }

        loop {
            if cursor.current.is_none() {
                if cursor.queue_input.is_none() && cursor.queue_output.is_none() {
                    cursor.exhausted = true;
                    return Ok(None);
                }
                let queue_entry_bytes = std::mem::size_of::<BoundedQueueEntry>();
                let next_retained = cursor
                    .retained_bytes
                    .checked_sub(queue_entry_bytes)
                    .ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph queue-memory accounting underflow".to_string(),
                        ))
                    })?;
                let frontier = bounded_queue_pop(cursor).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph continuation lost its queued node".to_string(),
                    ))
                })?;
                cursor.retained_bytes = next_retained;
                release_retained(queue_entry_bytes);
                let phase = if !frontier.expand {
                    AdjacencyPhase::Done
                } else if matches!(cursor.direction, Direction::Outgoing | Direction::Both) {
                    AdjacencyPhase::Outgoing
                } else if matches!(cursor.direction, Direction::Incoming) {
                    AdjacencyPhase::Incoming
                } else {
                    AdjacencyPhase::Done
                };
                cursor.current = Some(BoundedCurrentEntry {
                    frontier,
                    emitted: false,
                    phase,
                    position: 0,
                    anchor: None,
                });
            }

            let should_emit = cursor.current.as_ref().is_some_and(|current| {
                !current.emitted
                    && current.frontier.depth > 0
                    && current.frontier.depth >= cursor.min_depth
            });
            if should_emit {
                let current = cursor.current.as_mut().ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph continuation lost its current node".to_string(),
                    ))
                })?;
                let (path, output_bytes) = bounded_graph_path(
                    &current.frontier.path,
                    None,
                    &mut before_path_element,
                    &mut before_retain,
                    &mut release_retained,
                )?;
                let node = TraversalNode {
                    id: current.frontier.node,
                    depth: current.frontier.depth,
                    path,
                };
                current.emitted = true;
                return Ok(Some(BoundedTraversalNode {
                    node,
                    reserved_bytes: output_bytes,
                }));
            }

            let at_max_depth = cursor
                .current
                .as_ref()
                .is_some_and(|current| current.frontier.depth >= cursor.max_depth);
            if at_max_depth {
                release_current_anchor(cursor, &mut release_retained)?;
                let finished_bytes = cursor
                    .current
                    .as_ref()
                    .ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph continuation lost its completed node".to_string(),
                        ))
                    })?
                    .frontier
                    .retained_bytes;
                let next_retained = cursor
                    .retained_bytes
                    .checked_sub(finished_bytes)
                    .ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph retained-memory accounting underflow".to_string(),
                        ))
                    })?;
                let finished = cursor.current.take().ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph continuation lost its completed node".to_string(),
                    ))
                })?;
                cursor.retained_bytes = next_retained;
                release_retained(finished.frontier.retained_bytes);
                continue;
            }

            let (node, phase, position) = {
                let current = cursor.current.as_ref().ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph continuation lost its adjacency position".to_string(),
                    ))
                })?;
                (current.frontier.node, current.phase, current.position)
            };

            if phase == AdjacencyPhase::Done {
                release_current_anchor(cursor, &mut release_retained)?;
                let finished_bytes = cursor
                    .current
                    .as_ref()
                    .ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph continuation lost its completed node".to_string(),
                        ))
                    })?
                    .frontier
                    .retained_bytes;
                let next_retained = cursor
                    .retained_bytes
                    .checked_sub(finished_bytes)
                    .ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph retained-memory accounting underflow".to_string(),
                        ))
                    })?;
                let finished = cursor.current.take().ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph continuation lost its completed node".to_string(),
                    ))
                })?;
                cursor.retained_bytes = next_retained;
                release_retained(finished.frontier.retained_bytes);
                continue;
            }

            // What this pass walks: a committed adjacency entry, or one the
            // reader's own open transaction staged and has not committed. A
            // staged edge is in no adjacency map at all, so it cannot be
            // reached by filtering the committed ones -- it is walked here,
            // straight after the committed edges of the same direction, and
            // then judged by exactly the rules below that a committed edge is.
            if phase.is_staged() {
                // One charge covers asking the transaction to resume from
                // where this walk left off, matching what the committed pass
                // is charged for reading the entry at its own position. What
                // the transaction charges for the entries it steps over on the
                // way there is its own to account for.
                before_edge()?;
                let staged = staged_edge(node, phase.direction(), position)?;
                let Some((edge, edge_bytes, resume)) = staged else {
                    release_current_anchor(cursor, &mut release_retained)?;
                    let current = cursor.current.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph continuation lost its staged adjacency".to_string(),
                        ))
                    })?;
                    current.phase = phase.next(cursor.direction);
                    current.position = 0;
                    continue;
                };
                // The continuation resumes at the index the transaction handed
                // back, so a walk that stops and starts steps over no staged
                // edge twice. It needs no anchor the way the committed side
                // does: adjacency vectors are compacted beneath a parked
                // continuation, while a transaction only ever adds to what it
                // staged.
                cursor
                    .current
                    .as_mut()
                    .ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph continuation lost its staged adjacency".to_string(),
                        ))
                    })?
                    .position = resume;
                // Deliberately no snapshot test. A staged edge was written by
                // a transaction that has not committed, so no snapshot can see
                // it -- and the transaction that wrote it is the one reading.
                let type_allowed = bounded_edge_type_allowed(
                    &cursor.edge_types,
                    &edge.edge_type,
                    &mut before_edge,
                )?;
                let neighbor = match phase {
                    AdjacencyPhase::StagedOutgoing if edge.source == node => Some(edge.target),
                    AdjacencyPhase::StagedIncoming if edge.target == node => Some(edge.source),
                    _ => None,
                };
                // Counted on the same rule as a committed entry: an edge whose
                // type this traversal never asked for was stepped over, not
                // examined. A staged edge needs no visibility test -- it is
                // the reader's own uncommitted work, visible to them by
                // definition.
                let Some(neighbor) = neighbor.filter(|_| type_allowed) else {
                    release_retained(edge_bytes);
                    continue;
                };
                if !edge_allowed(neighbor, edge.source, edge.target, &edge.edge_type)? {
                    release_retained(edge_bytes);
                    continue;
                }
                entry_read();
                // Admitted through a borrow of the record this walk owns, so
                // nothing is cloned to hand the shared admission its edge type.
                self.admit_bounded_neighbor(
                    cursor,
                    node,
                    neighbor,
                    &edge.edge_type,
                    &mut before_edge,
                    &mut before_path_element,
                    &mut before_retain,
                    &mut release_retained,
                    visited_cap,
                )?;
                // The record was this walk's to hold and is done being held.
                release_retained(edge_bytes);
                continue;
            }
            {
                let adjacency = match phase {
                    AdjacencyPhase::Outgoing => &self.store.forward_adj,
                    AdjacencyPhase::Incoming => &self.store.reverse_adj,
                    AdjacencyPhase::StagedOutgoing
                    | AdjacencyPhase::StagedIncoming
                    | AdjacencyPhase::Done => {
                        return Err(E::from(Error::Other(
                            "bounded graph continuation reached a completed adjacency".to_string(),
                        )));
                    }
                };
                // One charge covers this pass over the adjacency: taking the lock,
                // looking the node up, and reading the entry at `position`. Nothing
                // in the adjacency map is touched before the caller admits it.
                before_edge()?;
                let entries = adjacency.read();
                let Some(node_entries) = entries.get(&node) else {
                    drop(entries);
                    release_current_anchor(cursor, &mut release_retained)?;
                    let current = cursor.current.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph continuation lost its empty adjacency".to_string(),
                        ))
                    })?;
                    current.phase = phase.next(cursor.direction);
                    current.position = 0;
                    continue;
                };
                // Resume by the identity of the entry this continuation last
                // inspected. Entries removed beneath the parked slot slide every
                // survivor down, so the held slot is re-derived from where that
                // identity now sits before anything at it is read.
                let resume = {
                    let current = cursor.current.as_ref().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph continuation lost its anchored adjacency".to_string(),
                        ))
                    })?;
                    match current.anchor.as_ref() {
                        Some(anchor) => relocate_adjacency_anchor(
                            anchor,
                            position,
                            node_entries,
                            &mut before_edge,
                        )?,
                        None => AdjacencyResume::Held,
                    }
                };
                match resume {
                    AdjacencyResume::Held => {}
                    AdjacencyResume::Relocated {
                        anchor_position,
                        position: resumed,
                    } => {
                        drop(entries);
                        let current = cursor.current.as_mut().ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded graph continuation lost its relocated adjacency"
                                    .to_string(),
                            ))
                        })?;
                        if let Some(anchor) = current.anchor.as_mut() {
                            anchor.position = anchor_position;
                        }
                        current.position = resumed;
                        continue;
                    }
                    AdjacencyResume::Restart => {
                        drop(entries);
                        release_current_anchor(cursor, &mut release_retained)?;
                        let current = cursor.current.as_mut().ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded graph continuation lost its restarted adjacency"
                                    .to_string(),
                            ))
                        })?;
                        current.position = 0;
                        continue;
                    }
                }

                if position >= node_entries.len() {
                    drop(entries);
                    release_current_anchor(cursor, &mut release_retained)?;
                    let current = cursor.current.as_mut().ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph continuation lost its completed adjacency".to_string(),
                        ))
                    })?;
                    current.phase = phase.next(cursor.direction);
                    current.position = 0;
                    continue;
                }

                let entry = node_entries.get(position).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph adjacency changed during a read lock".to_string(),
                    ))
                })?;
                let next_position = position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph adjacency position overflow".to_string(),
                    ))
                })?;
                install_adjacency_anchor(
                    cursor,
                    entry,
                    position,
                    &mut before_retain,
                    &mut release_retained,
                )?;
                let visible = entry.visible_at(cursor.snapshot);
                let type_allowed = bounded_edge_type_allowed(
                    &cursor.edge_types,
                    &entry.edge_type,
                    &mut before_edge,
                )?;
                let neighbor = match phase {
                    AdjacencyPhase::Outgoing if entry.source == node => Some(entry.target),
                    AdjacencyPhase::Incoming if entry.target == node => Some(entry.source),
                    _ => None,
                };
                let source = entry.source;
                let target = entry.target;
                if !visible || !type_allowed {
                    drop(entries);
                    cursor
                        .current
                        .as_mut()
                        .ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded graph continuation lost its charged adjacency".to_string(),
                            ))
                        })?
                        .position = next_position;
                    continue;
                }
                // Counted where the other door counts it: an entry the
                // snapshot hides, or one whose type this traversal never
                // asked for, was stepped over rather than examined, and
                // reporting it makes a probe look like it read a
                // neighbourhood it did not.
                let Some(neighbor) = neighbor else {
                    drop(entries);
                    cursor
                        .current
                        .as_mut()
                        .ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded graph continuation lost its charged adjacency".to_string(),
                            ))
                        })?
                        .position = next_position;
                    continue;
                };
                if !edge_allowed(neighbor, source, target, &entry.edge_type)? {
                    drop(entries);
                    cursor
                        .current
                        .as_mut()
                        .ok_or_else(|| {
                            E::from(Error::Other(
                                "bounded graph continuation lost its charged adjacency".to_string(),
                            ))
                        })?
                        .position = next_position;
                    continue;
                }
                entry_read();
                cursor
                    .current
                    .as_mut()
                    .ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph continuation lost its charged adjacency".to_string(),
                        ))
                    })?
                    .position = next_position;
                // Admitted through a borrow of the entry, with the adjacency
                // guard still held, so nothing is cloned to hand the shared
                // admission its edge type.
                self.admit_bounded_neighbor(
                    cursor,
                    node,
                    neighbor,
                    &entry.edge_type,
                    &mut before_edge,
                    &mut before_path_element,
                    &mut before_retain,
                    &mut release_retained,
                    visited_cap,
                )?;
                drop(entries);
            }
        }
    }

    /// Admit one neighbour into the traversal: its path, its queue entry and
    /// its share of the visited set, each charged before it is taken and each
    /// handed back on any refusal.
    ///
    /// Shared by both sources so that neither has to own its edge type to
    /// reach here. A committed edge lends a borrow that lives as long as the
    /// adjacency guard the caller is holding; a staged edge lends a borrow of
    /// the record the caller is holding. Cloning one to make the two look
    /// alike would put an allocation on every walked edge that nothing had
    /// reserved.
    #[allow(clippy::too_many_arguments)]
    fn admit_bounded_neighbor<E, Edge, Path, Retain, Release>(
        &self,
        cursor: &mut BoundedBfsCursor,
        node: NodeId,
        neighbor: NodeId,
        edge_type: &EdgeType,
        before_edge: &mut Edge,
        before_path_element: &mut Path,
        before_retain: &mut Retain,
        release_retained: &mut Release,
        visited_cap: Option<usize>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
        Edge: FnMut() -> std::result::Result<(), E>,
        Path: FnMut() -> std::result::Result<(), E>,
        Retain: FnMut(usize) -> std::result::Result<(), E>,
        Release: FnMut(usize),
    {
        // An edge that arrives at a node the traversal has already
        // reached is still an edge the caller asked for -- an edge from a
        // node to itself is the plainest case -- so it is reported. What
        // the traversal does not do is walk that node's adjacency again.
        before_edge()?;
        let expand = !cursor.visited.contains(&neighbor);

        let current = cursor.current.as_ref().ok_or_else(|| {
            E::from(Error::Other(
                "bounded graph continuation lost its path source".to_string(),
            ))
        })?;
        let next_depth = current
            .frontier
            .depth
            .checked_add(1)
            .ok_or_else(|| E::from(Error::Other("bounded graph depth overflow".to_string())))?;
        let (path, path_bytes) = bounded_graph_path(
            &current.frontier.path,
            Some((node, edge_type)),
            &mut *before_path_element,
            &mut *before_retain,
            &mut *release_retained,
        )?;
        let queue_bytes = std::mem::size_of::<BoundedQueueEntry>();
        if let Err(error) = before_retain(queue_bytes) {
            release_retained(path_bytes);
            return Err(error);
        }
        let mut prepared_queue = Box::new(BoundedQueueEntry {
            frontier: BoundedFrontierEntry {
                node: neighbor,
                depth: next_depth,
                path,
                retained_bytes: path_bytes,
                expand,
            },
            next: None,
        });
        let prepared_visited = if expand {
            match prepare_graph_visited_growth(
                &cursor.visited,
                neighbor,
                &mut *before_edge,
                &mut *before_retain,
                &mut *release_retained,
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    release_retained(queue_bytes);
                    release_retained(path_bytes);
                    return Err(error);
                }
            }
        } else {
            None
        };
        if prepared_visited.is_none()
            && let Err(error) = before_edge()
        {
            release_retained(queue_bytes);
            release_retained(path_bytes);
            return Err(error);
        }
        let visited_growth = match prepared_visited.as_ref() {
            Some((_, actual, previous)) => actual.checked_sub(*previous).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded graph visited capacity moved backwards".to_string(),
                ))
            }),
            None => Ok(0),
        };
        let visited_growth = match visited_growth {
            Ok(bytes) => bytes,
            Err(error) => {
                if let Some((_, actual, _)) = prepared_visited.as_ref() {
                    release_retained(*actual);
                }
                release_retained(queue_bytes);
                release_retained(path_bytes);
                return Err(error);
            }
        };
        let retained_growth = checked_graph_add(path_bytes, queue_bytes, "neighbor frontier")
            .and_then(|bytes| checked_graph_add(bytes, visited_growth, "neighbor state"));
        let next_retained = match retained_growth
            .and_then(|growth| checked_graph_add(cursor.retained_bytes, growth, "retained state"))
        {
            Ok(bytes) => bytes,
            Err(error) => {
                if let Some((_, actual, _)) = prepared_visited.as_ref() {
                    release_retained(*actual);
                }
                release_retained(queue_bytes);
                release_retained(path_bytes);
                return Err(E::from(error));
            }
        };
        // The adjacency position was advanced where the entry was taken,
        // committed or staged alike, so this node's own bookkeeping is all
        // that is left to settle.
        if cursor.current.is_none() {
            if let Some((_, actual, _)) = prepared_visited.as_ref() {
                release_retained(*actual);
            }
            release_retained(queue_bytes);
            release_retained(path_bytes);
            return Err(E::from(Error::Other(
                "bounded graph continuation lost its admitted adjacency".to_string(),
            )));
        }
        if let Some((replacement, _, previous)) = prepared_visited {
            let old = std::mem::replace(&mut cursor.visited, replacement);
            drop(old);
            release_retained(previous);
        } else if expand {
            cursor.visited.insert(neighbor);
        }
        // The same rule, the same number, and the same typed error the eager
        // walk refuses with -- applied only for a caller that kept the standing
        // ceiling.
        if expand
            && let Some(cap) = visited_cap
            && cursor.visited.len() > cap
        {
            return Err(E::from(Error::BfsVisitedExceeded(cap)));
        }
        prepared_queue.next = cursor.queue_input.take();
        cursor.queue_input = Some(prepared_queue);
        cursor.retained_bytes = next_retained;
        Ok(())
    }

    /// Carry one edge forward with a new property set, as one replacement:
    /// the occurrence that held the old properties is retired and the one
    /// that holds the new set takes its place, so the edge is never twice
    /// live and never briefly absent.
    fn replace_edge_properties(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        properties: std::collections::HashMap<String, Value>,
    ) -> Result<()> {
        let entry = AdjEntry {
            source,
            target,
            edge_type: edge_type.clone(),
            properties,
            created_tx: tx,
            deleted_tx: None,
            lsn: contextdb_core::Lsn(0),
        };
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_inserts.retain(|existing| {
                !(existing.source == source
                    && existing.target == target
                    && existing.edge_type == edge_type)
            });
            if !ws
                .adj_deletes
                .iter()
                .any(|(s, et, t, _)| *s == source && *t == target && et == &edge_type)
            {
                ws.adj_deletes.push((source, edge_type.clone(), target, tx));
            }
            ws.adj_inserts.push(entry);
        })?;
        Ok(())
    }
}

impl<S: WriteSetApplicator> GraphExecutor for MemGraphExecutor<S> {
    fn bfs(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
    ) -> Result<TraversalResult> {
        let mut visited = HashSet::new();
        visited.insert(start);

        type BfsEntry = (NodeId, u32, Vec<(NodeId, EdgeType)>);
        let mut queue: VecDeque<BfsEntry> = VecDeque::new();
        queue.push_back((start, 0, vec![]));

        let mut result_nodes = Vec::new();

        while let Some((current, depth, path)) = queue.pop_front() {
            if depth > 0 && depth >= min_depth {
                result_nodes.push(TraversalNode {
                    id: current,
                    depth,
                    path: path.clone(),
                });
            }

            if depth >= max_depth {
                continue;
            }

            let neighbors = self.neighbors(current, edge_types, direction, snapshot)?;
            for (neighbor_id, edge_type, _) in neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);

                let cap = self
                    .bfs_visited_cap
                    .load(std::sync::atomic::Ordering::Relaxed);
                if visited.len() > cap {
                    return Err(Error::BfsVisitedExceeded(cap));
                }

                let mut new_path = path.clone();
                new_path.push((current, edge_type));
                queue.push_back((neighbor_id, depth + 1, new_path));
            }
        }

        Ok(TraversalResult {
            nodes: result_nodes,
        })
    }

    fn neighbors(
        &self,
        node: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
    ) -> Result<Vec<(NodeId, EdgeType, std::collections::HashMap<String, Value>)>> {
        let mut results = Vec::new();

        if matches!(direction, Direction::Outgoing | Direction::Both) {
            let fwd = self.store.forward_adj.read();
            if let Some(entries) = fwd.get(&node) {
                for e in entries {
                    if !e.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(types) = edge_types
                        && !types.contains(&e.edge_type)
                    {
                        continue;
                    }
                    results.push((e.target, e.edge_type.clone(), e.properties.clone()));
                }
            }
        }

        if matches!(direction, Direction::Incoming | Direction::Both) {
            let rev = self.store.reverse_adj.read();
            if let Some(entries) = rev.get(&node) {
                for e in entries {
                    if !e.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(types) = edge_types
                        && !types.contains(&e.edge_type)
                    {
                        continue;
                    }
                    results.push((e.source, e.edge_type.clone(), e.properties.clone()));
                }
            }
        }

        Ok(results)
    }

    fn insert_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        properties: std::collections::HashMap<String, Value>,
    ) -> Result<bool> {
        let deleted_in_ws = self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_deletes
                .iter()
                .any(|(s, et, t, _)| *s == source && *t == target && et == &edge_type)
        })?;

        {
            let existing = {
                let fwd = self.store.forward_adj.read();
                fwd.get(&source).and_then(|entries| {
                    entries
                        .iter()
                        .rev()
                        .find(|e| {
                            e.target == target && e.edge_type == edge_type && e.deleted_tx.is_none()
                        })
                        .map(|e| e.properties.clone())
                })
            };
            if let Some(existing) = existing
                && !deleted_in_ws
            {
                // The edge is already there, so no edge is inserted -- but the
                // caller still named properties for it, and dropping them
                // would be this call reporting success for work it did not do.
                // Named properties are applied over what the edge holds;
                // anything the caller did not name is left alone, so a caller
                // that passes none is asking only whether the edge exists.
                if properties.is_empty() {
                    return Ok(false);
                }
                let mut merged = existing.clone();
                for (name, value) in properties {
                    merged.insert(name, value);
                }
                if merged == existing {
                    return Ok(false);
                }
                self.replace_edge_properties(tx, source, target, edge_type, merged)?;
                return Ok(false);
            }
        }

        let duplicate_in_ws = self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_inserts
                .iter()
                .any(|e| e.source == source && e.target == target && e.edge_type == edge_type)
        })?;
        if duplicate_in_ws {
            return Ok(false);
        }

        if self.dag_edge_types.read().contains(&edge_type) {
            if source == target {
                return Err(Error::CycleDetected {
                    edge_type: edge_type.clone(),
                    source_node: source,
                    target_node: target,
                });
            }

            if self.bfs_with_write_set(tx, target, source, &edge_type)? {
                return Err(Error::CycleDetected {
                    edge_type: edge_type.clone(),
                    source_node: source,
                    target_node: target,
                });
            }
        }

        let entry = AdjEntry {
            source,
            target,
            edge_type,
            properties,
            created_tx: tx,
            deleted_tx: None,
            lsn: contextdb_core::Lsn(0),
        };

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_inserts.push(entry);
        })?;

        Ok(true)
    }

    fn delete_edge(&self, tx: TxId, source: NodeId, target: NodeId, edge_type: &str) -> Result<()> {
        let committed_edge_exists = {
            let fwd = self.store.forward_adj.read();
            fwd.get(&source).is_some_and(|entries| {
                entries.iter().any(|entry| {
                    entry.target == target
                        && entry.edge_type == edge_type
                        && entry.deleted_tx.is_none()
                })
            })
        };

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_inserts.retain(|entry| {
                !(entry.source == source && entry.target == target && entry.edge_type == edge_type)
            });

            if committed_edge_exists
                && !ws
                    .adj_deletes
                    .iter()
                    .any(|(s, et, t, _)| *s == source && *t == target && et == edge_type)
            {
                ws.adj_deletes
                    .push((source, edge_type.to_string(), target, tx));
            }
        })?;
        Ok(())
    }
}
