use std::collections::VecDeque;
use crate::source::GraphCursor as _;
use crate::source::{GraphCursorHandle, GraphQuery, GraphReadHandle, GraphSnapshot};
use crate::store::{GraphAdjEntry, GraphStore};
use contextdb_core::*;
use contextdb_tx::{OwnedAdjEntry, RowEdgeOwner, TransactionManager, WriteSetApplicator};
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Production BFS visited-node ceiling. Public so the boundary test can
/// assert the shipped constant while exercising the refusal itself through
/// the small test-only cap seam below.
pub const MAX_VISITED: usize = 100_000;

#[derive(Debug)]
struct BoundedFrontierEntry {
    node: NodeId,
    depth: u32,
    path: Option<NonZeroUsize>,
    retained_bytes: usize,
    /// Whether this node's own adjacency is walked when it is reached. An
    /// edge arriving at a node the traversal has already expanded is still an
    /// edge and is still reported; walking that node a second time would
    /// repeat the work and, on a cycle, never end.
    expand: bool,
}

/// One shared predecessor; paths are rebuilt only for emitted rows.
#[derive(Debug)]
struct BoundedPathLink {
    parent: Option<NonZeroUsize>,
    node: NodeId,
    edge_name: usize,
}

#[derive(Debug)]
struct BoundedQueueEntry {
    frontier: BoundedFrontierEntry,
    next: Option<Box<BoundedQueueEntry>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdjacencyPhase {
    Outgoing,
    Incoming,
    Done,
}

impl AdjacencyPhase {
    // A direction completes only after both independently resumed lanes end.
    fn next(self, direction: Direction) -> Self {
        match (self, direction) {
            (AdjacencyPhase::Outgoing, Direction::Both) => AdjacencyPhase::Incoming,
            _ => AdjacencyPhase::Done,
        }
    }

    /// Which end of a staged edge this phase reaches out from.
    fn direction(self) -> Direction {
        match self {
            AdjacencyPhase::Incoming => Direction::Incoming,
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
    source_cursor: Option<GraphCursorHandle>,
    source_head: Option<TraversalWindow>,
    committed_done: bool,
    staged_head: Option<(StagedTraversalEdge, usize)>,
    staged_position: usize,
    staged_done: bool,
    logical_edge: Option<LogicalEdge>,
}

// A file head retains its own typed admission while the staged lane advances.
struct TraversalWindow(
    contextdb_core::read_memory::OwnedReadBox<dyn crate::source::GraphEdgeWindow>,
);
impl std::fmt::Debug for TraversalWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TraversalWindow")
    }
}

fn staged_precedes(
    staged: &StagedTraversalEdge,
    committed: &AdjEntry,
    committed_owner: Option<&RowEdgeOwner>,
    direction: Direction,
) -> bool {
    let (staged_other, committed_other) = if matches!(direction, Direction::Incoming) {
        (staged.source, committed.source)
    } else {
        (staged.target, committed.target)
    };
    (staged_other, &staged.edge_type, staged.row_owner.as_ref())
        < (committed_other, &committed.edge_type, committed_owner)
}

/// One admitted logical triad, never a collection of row owners.
#[derive(Debug)]
struct LogicalEdge {
    source: NodeId,
    target: NodeId,
    edge_type: EdgeType,
    retained_bytes: usize,
}

/// Identity of one adjacency entry, owned by a suspended continuation. The
/// entry is named by its own contents -- endpoints, type, creating
/// transaction, LSN -- because adjacency vectors are physically compacted
/// while a continuation is parked: everything that survives beneath the
/// parked slot slides down, so a resume addressed by slot alone reads an
/// entry the caller has never seen and emits the ones in between to nobody.
#[derive(Debug)]
struct AdjacencyAnchor {
    row_owner: Option<RowEdgeOwner>,
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
    fn matches(&self, entry: &GraphAdjEntry) -> bool {
        self.row_owner == entry.row_owner
            && self.source == entry.source
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
    source_root: Option<GraphReadHandle>,
    // Membership does not participate in traversal order. HashSet avoids
    // sorted-vector shifting while exposing its live element capacity.
    visited: HashSet<NodeId>,
    paths: Vec<BoundedPathLink>,
    path_edge_names: Vec<EdgeType>,
    queue_input: Option<Box<BoundedQueueEntry>>,
    queue_output: Option<Box<BoundedQueueEntry>>,
    current: Option<BoundedCurrentEntry>,
    retained_bytes: usize,
    exhausted: bool,
    physical_single_hop: bool,
}

/// One staged edge as a traversal needs it: who it joins and by what name.
///
/// Deliberately not the whole entry. A walk never reads an edge's properties,
/// and carrying them would make a bounded read hold memory in proportion to
/// the size of the transaction it is reading inside rather than to the walk it
/// is doing.
#[derive(Debug, Clone)]
pub struct StagedTraversalEdge {
    /// Borrowed by the ACL callback before reachability coalesces this edge.
    /// None denotes an explicitly standalone staged edge.
    pub row_owner: Option<RowEdgeOwner>,
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

    /// Preserve every visible physical edge binding for single-hop SQL. This
    /// must be selected before the first pull; reachability keeps coalescing.
    #[doc(hidden)]
    pub fn preserve_single_hop_bindings(&mut self) -> Result<()> {
        if self.min_depth != 1 || self.max_depth != 1 {
            return Err(Error::InvalidStateTransition(
                "physical edge bindings require a single-hop cursor".into(),
            ));
        }
        if self.current.is_some() || !self.paths.is_empty() || self.exhausted {
            return Err(Error::InvalidStateTransition(
                "physical edge bindings must be selected before traversal starts".into(),
            ));
        }
        self.physical_single_hop = true;
        Ok(())
    }
}

fn checked_graph_add(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_add(right).ok_or_else(|| {
        Error::Other((format!(
            "bounded graph {operation} memory size exceeds the native address space"
        )).into())
    })
}

fn checked_graph_mul(left: usize, right: usize, operation: &str) -> Result<usize> {
    left.checked_mul(right).ok_or_else(|| {
        Error::Other((format!(
            "bounded graph {operation} memory size exceeds the native address space"
        )).into())
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
        return Err(E::from(Error::Other((
            "bounded graph edge type allocation failed".to_string()).into())));
    }
    let actual = owned.capacity();
    if actual < requested {
        release_retained(requested);
        return Err(E::from(Error::Other((
            "bounded graph edge type capacity moved backwards".to_string()).into())));
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
    node_entries: &[GraphAdjEntry],
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
                E::from(Error::Other((
                    "bounded graph adjacency position overflow".to_string()).into()))
            })?;
            return Ok(AdjacencyResume::Relocated {
                anchor_position: candidate,
                position,
            });
        }
        let Some(next) = candidate.checked_sub(1) else {
            for (candidate, entry) in node_entries
                .iter()
                .enumerate()
                .skip(anchor.position.saturating_add(1))
            {
                before_edge()?;
                if anchor.matches(entry) {
                    let position = candidate.checked_add(offset).ok_or_else(|| {
                        E::from(Error::Other(
                            "bounded graph adjacency position overflow".into()))
                    })?;
                    return Ok(AdjacencyResume::Relocated {
                        anchor_position: candidate,
                        position,
                    });
                }
            }
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
            E::from(Error::Other((
                "bounded graph adjacency anchor accounting underflow".to_string()).into()))
        })?;
    release_retained(anchor.retained_bytes);
    Ok(())
}

/// Anchor the continuation on the entry it is about to inspect. The
/// replacement is charged before the anchor it supersedes is released, so a
/// refused charge leaves the continuation exactly as it was.
fn install_adjacency_anchor<E>(
    cursor: &mut BoundedBfsCursor,
    entry: &GraphAdjEntry,
    position: usize,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    let (edge_type, anchor_bytes) =
        bounded_graph_string(&entry.edge_type, before_retain, release_retained)?;
    let (row_owner, owner_bytes) = if let Some(owner) = &entry.row_owner {
        match bounded_graph_string(&owner.table, before_retain, release_retained) {
            Ok((table, bytes)) => (
                Some(RowEdgeOwner {
                    table,
                    table_generation: owner.table_generation,
                    row_id: owner.row_id,
                    created_tx: owner.created_tx,
                    lsn: owner.lsn,
                }),
                bytes,
            ),
            Err(error) => {
                drop(edge_type);
                release_retained(anchor_bytes);
                return Err(error);
            }
        }
    } else {
        (None, 0)
    };
    let anchor_bytes = match checked_graph_add(anchor_bytes, owner_bytes, "anchor owner") {
        Ok(bytes) => bytes,
        Err(error) => {
            drop(edge_type);
            drop(row_owner);
            release_retained(anchor_bytes);
            release_retained(owner_bytes);
            return Err(E::from(error));
        }
    };
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
        return Err(E::from(Error::Other((
            "bounded graph continuation lost its anchored adjacency".to_string()).into())));
    };
    current.anchor = Some(AdjacencyAnchor {
        row_owner,
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

fn release_logical_edge<E>(
    cursor: &mut BoundedBfsCursor,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    if let Some(edge) = cursor
        .current
        .as_mut()
        .and_then(|current| current.logical_edge.take())
    {
        let bytes = edge.retained_bytes;
        drop(edge);
        cursor.retained_bytes = cursor.retained_bytes.checked_sub(bytes).ok_or_else(|| {
            E::from(Error::Other(
                "graph logical edge accounting underflow".into()))
        })?;
        release_retained(bytes);
    }
    Ok(())
}

/// Source windows and canonical memory adjacency group a triad's physical
/// owners together. Each is examined and charged, but only the first visible
/// witness adds a reachability queue entry. No owner list is retained.
fn retain_logical_edge<E>(
    cursor: &mut BoundedBfsCursor,
    source: NodeId,
    target: NodeId,
    edge_type: &str,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<bool, E>
where
    E: From<Error>,
{
    let current = cursor.current.as_ref().ok_or_else(|| {
        E::from(Error::Other(
            "graph continuation lost its logical edge".into()))
    })?;
    if current.logical_edge.as_ref().is_some_and(|last| {
        last.source == source && last.target == target && last.edge_type == edge_type
    }) {
        return Ok(false);
    }
    let (edge_type, retained_bytes) =
        bounded_graph_string(edge_type, before_retain, release_retained)?;
    let retained = match checked_graph_add(cursor.retained_bytes, retained_bytes, "logical edge") {
        Ok(bytes) => bytes,
        Err(error) => {
            drop(edge_type);
            release_retained(retained_bytes);
            return Err(E::from(error));
        }
    };
    let old = cursor
        .current
        .as_mut()
        .expect("current edge checked above")
        .logical_edge
        .replace(LogicalEdge {
            source,
            target,
            edge_type,
            retained_bytes,
        });
    let previous = old.as_ref().map_or(0, |edge| edge.retained_bytes);
    drop(old);
    cursor.retained_bytes = retained - previous;
    release_retained(previous);
    Ok(true)
}

/// Admit replacement storage before allocating it; move elements and destroy
/// the previous allocation before returning its charge.
fn grow_bounded_path_storage<T, E>(
    storage: &mut Vec<T>,
    retained_bytes: &mut usize,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    if storage.len() < storage.capacity() {
        return Ok(());
    }
    let capacity = storage.capacity().saturating_mul(2).max(4);
    let requested = checked_graph_add(
        bounded_capacity_bytes::<T>(capacity, "predecessor storage").map_err(E::from)?,
        16,
        "predecessor allocation",
    )
    .map_err(E::from)?;
    before_retain(requested)?;
    let mut replacement = Vec::new();
    if replacement.try_reserve_exact(capacity).is_err() {
        drop(replacement);
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph predecessor allocation failed".into())));
    }
    let actual = replacement.capacity() * std::mem::size_of::<T>() + 16;
    if actual > requested {
        if let Err(error) = before_retain(actual - requested) {
            drop(replacement);
            release_retained(requested);
            return Err(error);
        }
    } else {
        release_retained(requested - actual);
    }
    let previous = if storage.capacity() == 0 {
        0
    } else {
        storage.capacity() * std::mem::size_of::<T>() + 16
    };
    let next = match checked_graph_add(*retained_bytes, actual, "predecessor state") {
        Ok(next) => next - previous,
        Err(error) => {
            drop(replacement);
            release_retained(actual);
            return Err(E::from(error));
        }
    };
    replacement.append(storage);
    let old = std::mem::replace(storage, replacement);
    drop(old);
    release_retained(previous);
    *retained_bytes = next;
    Ok(())
}

fn append_bounded_path<E>(
    cursor: &mut BoundedBfsCursor,
    parent: Option<NonZeroUsize>,
    node: NodeId,
    edge_type: &str,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<Option<NonZeroUsize>, E>
where
    E: From<Error>,
{
    let mut edge_name = None;
    for (position, name) in cursor.path_edge_names.iter().enumerate() {
        before_work()?;
        if name == edge_type {
            edge_name = Some(position);
            break;
        }
    }
    let edge_name = match edge_name {
        Some(position) => position,
        None => {
            grow_bounded_path_storage(
                &mut cursor.path_edge_names,
                &mut cursor.retained_bytes,
                before_retain,
                release_retained,
            )?;
            let (name, bytes) = bounded_graph_string(edge_type, before_retain, release_retained)?;
            let next = match checked_graph_add(cursor.retained_bytes, bytes, "predecessor names") {
                Ok(next) => next,
                Err(error) => {
                    drop(name);
                    release_retained(bytes);
                    return Err(E::from(error));
                }
            };
            let position = cursor.path_edge_names.len();
            cursor.path_edge_names.push(name);
            cursor.retained_bytes = next;
            position
        }
    };
    grow_bounded_path_storage(
        &mut cursor.paths,
        &mut cursor.retained_bytes,
        before_retain,
        release_retained,
    )?;
    before_work()?;
    cursor.paths.push(BoundedPathLink {
        parent,
        node,
        edge_name,
    });
    Ok(NonZeroUsize::new(cursor.paths.len()))
}

fn rebuild_bounded_path<E>(
    cursor: &BoundedBfsCursor,
    mut link: Option<NonZeroUsize>,
    depth: usize,
    before_work: &mut impl FnMut() -> std::result::Result<(), E>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize),
) -> std::result::Result<(Vec<(NodeId, EdgeType)>, usize), E>
where
    E: From<Error>,
{
    let requested =
        bounded_capacity_bytes::<(NodeId, EdgeType)>(depth, "emitted path").map_err(E::from)?;
    before_retain(requested)?;
    let mut path = Vec::new();
    if path.try_reserve_exact(depth).is_err() {
        drop(path);
        release_retained(requested);
        return Err(E::from(Error::Other(
            "bounded graph output path allocation failed".into())));
    }
    let actual = path.capacity() * std::mem::size_of::<(NodeId, EdgeType)>();
    let mut retained =
        match reconcile_graph_allocation(requested, actual, before_retain, release_retained) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(path);
                return Err(error);
            }
        };
    while let Some(index) = link {
        if let Err(error) = before_work() {
            drop(path);
            release_retained(retained);
            return Err(error);
        }
        let predecessor = &cursor.paths[index.get() - 1];
        let (name, bytes) = match bounded_graph_string(
            &cursor.path_edge_names[predecessor.edge_name],
            before_retain,
            release_retained,
        ) {
            Ok(copy) => copy,
            Err(error) => {
                drop(path);
                release_retained(retained);
                return Err(error);
            }
        };
        retained += bytes;
        path.push((predecessor.node, name));
        link = predecessor.parent;
    }
    path.reverse();
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
        return Err(E::from(Error::Other((
            "bounded graph edge-filter allocation failed".to_string()).into())));
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
        return Err(E::from(Error::Other((
            "bounded graph edge-filter capacity moved backwards".to_string()).into())));
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
        .ok_or_else(|| Error::Other(("bounded graph capacity overflow".to_string()).into()))?
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
        return Err(E::from(Error::Other((
            "bounded graph visited allocation failed".to_string()).into())));
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
        return Err(E::from(Error::Other((
            "bounded graph visited capacity moved backwards".to_string()).into())));
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
        E::from(Error::Other((
            "bounded graph visited length overflow".to_string()).into()))
    })?;
    let requested_capacity = next_graph_capacity(visited.capacity(), required).map_err(E::from)?;
    let requested =
        bounded_capacity_bytes::<NodeId>(requested_capacity, "visited nodes").map_err(E::from)?;
    before_retain(requested)?;
    let mut replacement = HashSet::new();
    if replacement.try_reserve(requested_capacity).is_err() {
        release_retained(requested);
        return Err(E::from(Error::Other((
            "bounded graph visited allocation failed".to_string()).into())));
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
        return Err(E::from(Error::Other((
            "bounded graph visited capacity moved backwards".to_string()).into())));
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
            return Err(E::from(Error::Other((
                "bounded graph visited set changed during migration".to_string()).into())));
        };
        remaining = match remaining.checked_sub(1) {
            Some(remaining) => remaining,
            None => {
                release_retained(actual);
                return Err(E::from(Error::Other((
                    "bounded graph visited migration underflow".to_string()).into())));
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
            E::from(Error::Other((
                "bounded graph edge-filter position changed".to_string()).into()))
        })?;
        position = position.checked_add(1).ok_or_else(|| {
            E::from(Error::Other((
                "bounded graph edge-filter position overflow".to_string()).into()))
        })?;
        if filter.as_str() == edge_type {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Construction retains the same lower transaction owner. Public standalone
/// mutations continue through Legacy; Owned mutation needs its admitted producer.
enum ExecutorTransactions<S: WriteSetApplicator> {
    Legacy(Arc<TransactionManager<S>>),
    Owned(contextdb_tx::manager::EngineTransactionHandle<S>),
}
struct GraphStaging<'a> {
    inserts: &'a [AdjEntry],
    deletes: &'a [(NodeId, EdgeType, NodeId, TxId)],
    row_inserts: &'a [OwnedAdjEntry],
    row_deletes: &'a [(OwnedAdjEntry, TxId)],
}

impl<S: WriteSetApplicator> ExecutorTransactions<S> {
    fn snapshot(&self) -> SnapshotId {
        match self {
            Self::Legacy(manager) => manager.snapshot(),
            Self::Owned(handle) => handle.with_transactions(|transactions| transactions.snapshot()),
        }
    }
    fn with_graph_read<R>(&self, tx: TxId, f: impl FnOnce(GraphStaging<'_>) -> R) -> Result<R> {
        match self {
            Self::Legacy(manager) => manager.with_write_set(tx, |ws| {
                f(GraphStaging {
                    inserts: &ws.adj_inserts,
                    deletes: &ws.adj_deletes,
                    row_inserts: &ws.row_adj_inserts,
                    row_deletes: &ws.row_adj_deletes,
                })
            }),
            Self::Owned(handle) => handle.with_transactions(|transactions| {
                transactions.with_read(tx, |ws| {
                    f(GraphStaging {
                        inserts: ws.adj_inserts,
                        deletes: ws.adj_deletes,
                        row_inserts: ws.row_adj_inserts,
                        row_deletes: ws.row_adj_deletes,
                    })
                })
            }),
        }
    }
    fn with_write_set<R>(
        &self,
        tx: TxId,
        f: impl FnOnce(&mut contextdb_tx::WriteSet) -> R,
    ) -> Result<R> {
        match self {
            Self::Legacy(manager) => manager.with_write_set(tx, f),
            Self::Owned(_) => Err(Error::InvalidStateTransition(
                "owned graph mutation requires its admitted engine producer".into(),
            )),
        }
    }
}

pub struct MemGraphExecutor<S: WriteSetApplicator> {
    store: Arc<GraphStore>,
    tx_mgr: ExecutorTransactions<S>,
    dag_edge_types: parking_lot::RwLock<HashSet<String>>,
    // The active BFS visited cap. Production value is always `MAX_VISITED`;
    // only the #[doc(hidden)] test seam below ever lowers it, so the boundary
    // refusal is testable without building 100k edges. Cost: one relaxed
    // atomic load per newly-visited node (declared always-on).
    bfs_visited_cap: std::sync::atomic::AtomicUsize,
}

/// A complete DAG declaration set built before a schema commit becomes durable.
pub struct PreparedDagEdgeTypes {
    edge_types: HashSet<String>,
}

impl<S: WriteSetApplicator> MemGraphExecutor<S> {
    #[doc(hidden)]
    pub fn new_engine(
        store: Arc<GraphStore>,
        transactions: contextdb_tx::manager::EngineTransactionHandle<S>,
    ) -> Self {
        Self {
            store,
            tx_mgr: ExecutorTransactions::Owned(transactions),
            dag_edge_types: parking_lot::RwLock::new(HashSet::new()),
            bfs_visited_cap: std::sync::atomic::AtomicUsize::new(MAX_VISITED),
        }
    }

    pub fn new(store: Arc<GraphStore>, tx_mgr: Arc<TransactionManager<S>>) -> Self {
        Self {
            store,
            tx_mgr: ExecutorTransactions::Legacy(tx_mgr),
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

    pub fn prepare_dag_edge_types(types: impl IntoIterator<Item = String>) -> PreparedDagEdgeTypes {
        PreparedDagEdgeTypes {
            edge_types: types.into_iter().collect(),
        }
    }

    pub fn publish_prepared_dag_edge_types(&self, prepared: PreparedDagEdgeTypes) {
        *self.dag_edge_types.write() = prepared.edge_types;
    }

    /// Validate a row-derived edge before its admitted transaction mutation.
    /// Endpoint deduplication is deliberately absent: ownership is per row.
    pub fn validate_row_edge(
        &self,
        tx: TxId,
        owned: &OwnedAdjEntry,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let entry = &owned.entry;
        if self.dag_edge_types.read().contains(&entry.edge_type)
            && (entry.source == entry.target
                || self.bfs_with_write_set(
                    tx,
                    entry.target,
                    entry.source,
                    &entry.edge_type,
                    read_memory,
                )?)
        {
            return Err(Error::CycleDetected {
                edge_type: entry.edge_type.clone(),
                source_node: entry.source,
                target_node: entry.target,
            });
        }
        Ok(())
    }

    pub fn edge_count(
        &self,
        source: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<usize> {
        self.try_edge_count(source, edge_type, snapshot, read_memory)
    }

    pub fn try_edge_count(
        &self,
        source: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<usize> {
        let root = self.store.capture_source(snapshot, read_memory)?;
        let mut count = 0;
        self.store.visit_edges(
            root.as_ref(),
            GraphQuery {
                node: source,
                direction: Direction::Outgoing,
                identity: None,
            },
            |entry| {
                if entry.edge_type == edge_type && entry.visible_at(snapshot) {
                    count += 1;
                }
                Ok(true)
            },
        )?;
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    fn visit_neighbors(
        &self,
        root: Option<&Arc<dyn GraphSnapshot>>,
        node: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        mut visit: impl FnMut(NodeId, &AdjEntry) -> Result<()>,
    ) -> Result<()> {
        use std::cell::RefCell;
        let credit = RefCell::new(contextdb_core::read_memory::ReadCredit::new(read_memory));
        let mut logical: Option<LogicalEdge> = None;
        for part in [Direction::Outgoing, Direction::Incoming] {
            if !matches!(direction, Direction::Both) && direction != part {
                continue;
            }
            self.store.visit_edges(
                root,
                GraphQuery {
                    node,
                    direction: part,
                    identity: None,
                },
                |entry| {
                    if !entry.visible_at(snapshot)
                        || !edge_types.is_none_or(|types| types.contains(&entry.edge_type))
                        || (matches!(direction, Direction::Both)
                            && matches!(part, Direction::Incoming)
                            && entry.source == entry.target)
                    {
                        return Ok(true);
                    }
                    if logical.as_ref().is_some_and(|last| {
                        last.source == entry.source
                            && last.target == entry.target
                            && last.edge_type == entry.edge_type
                    }) {
                        return Ok(true);
                    }
                    // Canonical adjacency groups each triad with standalone first.
                    // The first visible witness owns the properties of the one
                    // logical neighbor; later owners cannot replace its payload.
                    let (edge_type, retained_bytes) = bounded_graph_string::<Error>(
                        &entry.edge_type,
                        &mut |bytes| {
                            credit.borrow_mut().try_grow_for(
                                bytes,
                                "reader",
                                "graph_neighbors",
                                "Raise the declared reader memory limit.",
                            )
                        },
                        &mut |bytes| {
                            credit
                                .borrow_mut()
                                .try_shrink(bytes)
                                .expect("neighbor scratch releases admitted storage")
                        },
                    )?;
                    let old = logical.replace(LogicalEdge {
                        source: entry.source,
                        target: entry.target,
                        edge_type,
                        retained_bytes,
                    });
                    let previous = old.as_ref().map_or(0, |edge| edge.retained_bytes);
                    drop(old);
                    credit.borrow_mut().try_shrink(previous)?;
                    let neighbor = if matches!(part, Direction::Outgoing) {
                        entry.target
                    } else {
                        entry.source
                    };
                    visit(neighbor, entry)?;
                    Ok(true)
                },
            )?;
        }
        Ok(())
    }

    fn bfs_with_write_set(
        &self,
        tx: TxId,
        start: NodeId,
        goal: NodeId,
        edge_type: &str,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        use std::cell::RefCell;
        let credit = RefCell::new(contextdb_core::read_memory::ReadCredit::new(
            read_memory.clone(),
        ));
        let mut admit = |bytes| {
            credit.borrow_mut().try_grow_for(
                bytes,
                "reader",
                "graph_cycle_validation",
                "Raise the declared reader memory limit.",
            )
        };
        let mut release = |bytes| {
            credit
                .borrow_mut()
                .try_shrink(bytes)
                .expect("cycle validation releases admitted storage")
        };
        let (mut visited, _) = bounded_graph_visited(start, &mut admit, &mut release)?;
        let mut queue = Vec::new();
        let mut queue_bytes = 0;
        grow_bounded_path_storage(&mut queue, &mut queue_bytes, &mut admit, &mut release)?;
        queue.push(start);
        let snapshot = self.tx_mgr.snapshot();
        let root = self.store.capture_source(snapshot, read_memory)?;
        let mut position = 0;
        while let Some(current) = queue.get(position).copied() {
            position += 1;
            let mut enqueue = |target| -> Result<bool> {
                if target == goal {
                    return Ok(true);
                }
                if visited.contains(&target) {
                    return Ok(false);
                }
                let replacement = prepare_graph_visited_growth(
                    &visited,
                    target,
                    &mut || Ok(()),
                    &mut admit,
                    &mut release,
                )?;
                grow_bounded_path_storage(&mut queue, &mut queue_bytes, &mut admit, &mut release)?;
                if let Some((replacement, _, previous)) = replacement {
                    let old = std::mem::replace(&mut visited, replacement);
                    drop(old);
                    release(previous);
                } else {
                    visited.insert(target);
                }
                queue.push(target);
                Ok(false)
            };
            let found = self.tx_mgr.with_graph_read(tx, |ws| -> Result<bool> {
                let mut found = false;
                self.store.visit_edges_with_owner(
                    root.as_ref(),
                    GraphQuery {
                        node: current,
                        direction: Direction::Outgoing,
                        identity: None,
                    },
                    |edge, owner| {
                        if edge.edge_type != edge_type || !edge.visible_at(snapshot) {
                            return Ok(true);
                        }
                        let deleted = match owner {
                            Some(owner) => ws
                                .row_deletes
                                .iter()
                                .any(|(before, _)| before.owner == *owner),
                            None => ws.deletes.iter().any(|(source, kind, target, _)| {
                                *source == edge.source
                                    && *target == edge.target
                                    && *kind == edge.edge_type
                            }),
                        };
                        if deleted {
                            return Ok(true);
                        }
                        found = enqueue(edge.target)?;
                        Ok(!found)
                    },
                )?;
                if found {
                    return Ok(true);
                }
                for edge in ws
                    .inserts
                    .iter()
                    .chain(ws.row_inserts.iter().map(|owned| &owned.entry))
                {
                    if edge.source == current
                        && edge.edge_type == edge_type
                        && enqueue(edge.target)?
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            })??;
            if found {
                return Ok(true);
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
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedBfsCursor, E>
    where
        E: From<Error>,
    {
        let root = self
            .store
            .capture_source(snapshot, read_memory)
            .map_err(E::from)?;
        self.bounded_bfs_cursor_at_source(
            start,
            edge_types,
            direction,
            min_depth,
            max_depth,
            snapshot,
            root,
            before_retain,
            release_retained,
        )
    }

    /// Use the root already captured by the enclosing statement. The engine
    /// supplies the same handle used by relational and vector consumers.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_bfs_cursor_at_source<E>(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
        root: Option<Arc<dyn GraphSnapshot>>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
    ) -> std::result::Result<BoundedBfsCursor, E>
    where
        E: From<Error>,
    {
        if self.store.is_source_backed() && root.is_none() {
            return Err(E::from(Error::Other(
                "graph traversal requires a captured source root".into())));
        }
        let source_root = root.map(GraphReadHandle);
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
        let queue_bytes = std::mem::size_of::<BoundedQueueEntry>() + 16;
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
                path: None,
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
            source_root,
            visited,
            paths: Vec::new(),
            path_edge_names: Vec::new(),
            queue_input,
            queue_output: None,
            current: None,
            retained_bytes,
            exhausted: false,
            physical_single_hop: false,
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

    /// Advance a committed traversal, preserving the physical witness owner
    /// for authorization before logical reachability coalesces sibling edges.
    pub fn bounded_bfs_next_with_owner<E>(
        &self,
        cursor: &mut BoundedBfsCursor,
        before_edge: impl FnMut() -> std::result::Result<(), E>,
        before_path_element: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        edge_allowed: impl FnMut(
            NodeId,
            NodeId,
            NodeId,
            &str,
            Option<&RowEdgeOwner>,
        ) -> std::result::Result<bool, E>,
    ) -> std::result::Result<Option<BoundedTraversalNode>, E>
    where
        E: From<Error>,
    {
        self.bounded_bfs_next_including_staged_with_owner(
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
        before_edge: impl FnMut() -> std::result::Result<(), E>,
        before_path_element: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        mut edge_allowed: impl FnMut(NodeId, NodeId, NodeId, &str) -> std::result::Result<bool, E>,
        // The edges the reader's own open transaction has staged, asked for
        // one at a time and RESUMED rather than re-scanned: the index goes in
        // and the index to come back to comes out, so a walk that stops and
        // starts does not begin the transaction again. What comes back is the
        // edge a traversal needs, never its properties, so a walk inside a
        // transaction holds no more than a walk outside one. `None` ends that
        // direction's staged edges.
        staged_edge: impl FnMut(
            NodeId,
            Direction,
            usize,
        )
            -> std::result::Result<Option<(StagedTraversalEdge, usize, usize)>, E>,
        // Called once for each adjacency entry this walk actually reads,
        // however many steps it then takes over that entry. What a caller
        // counts as "looked at" is entries, not steps.
        entry_read: impl FnMut(),
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
        self.bounded_bfs_next_including_staged_with_owner(
            cursor,
            before_edge,
            before_path_element,
            before_retain,
            release_retained,
            |neighbor, source, target, edge_type, _owner| {
                edge_allowed(neighbor, source, target, edge_type)
            },
            staged_edge,
            entry_read,
            visited_cap,
        )
    }

    /// Authorize each physical committed or staged witness with its exact row
    /// owner. The callback borrows provenance from the admitted window, memory
    /// adjacency, or staged record; no owner is cloned for authorization.
    #[allow(clippy::too_many_arguments)]
    pub fn bounded_bfs_next_including_staged_with_owner<E>(
        &self,
        cursor: &mut BoundedBfsCursor,
        mut before_edge: impl FnMut() -> std::result::Result<(), E>,
        mut before_path_element: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize),
        mut edge_allowed: impl FnMut(
            NodeId,
            NodeId,
            NodeId,
            &str,
            Option<&RowEdgeOwner>,
        ) -> std::result::Result<bool, E>,
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
                    cursor.source_root = None;
                    return Ok(None);
                }
                let queue_entry_bytes = std::mem::size_of::<BoundedQueueEntry>() + 16;
                let next_retained = cursor
                    .retained_bytes
                    .checked_sub(queue_entry_bytes)
                    .ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded graph queue-memory accounting underflow".to_string()).into()))
                    })?;
                let frontier = bounded_queue_pop(cursor).ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded graph continuation lost its queued node".to_string()).into()))
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
                    source_cursor: None,
                    source_head: None,
                    committed_done: false,
                    staged_head: None,
                    staged_position: 0,
                    staged_done: false,
                    logical_edge: None,
                });
            }

            let should_emit = cursor.current.as_ref().is_some_and(|current| {
                !current.emitted
                    && current.frontier.depth > 0
                    && current.frontier.depth >= cursor.min_depth
            });
            if should_emit {
                let current = cursor.current.as_ref().ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded graph continuation lost its current node".to_string()).into()))
                })?;
                let (path, output_bytes) = rebuild_bounded_path(
                    cursor,
                    current.frontier.path,
                    current.frontier.depth as usize,
                    &mut before_path_element,
                    &mut before_retain,
                    &mut release_retained,
                )?;
                let node = TraversalNode {
                    id: current.frontier.node,
                    depth: current.frontier.depth,
                    path,
                };
                cursor
                    .current
                    .as_mut()
                    .expect("current node was inspected above")
                    .emitted = true;
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
                release_logical_edge(cursor, &mut release_retained)?;
                release_current_anchor(cursor, &mut release_retained)?;
                let finished_bytes = cursor
                    .current
                    .as_ref()
                    .ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded graph continuation lost its completed node".to_string()).into()))
                    })?
                    .frontier
                    .retained_bytes;
                let next_retained = cursor
                    .retained_bytes
                    .checked_sub(finished_bytes)
                    .ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded graph retained-memory accounting underflow".to_string()).into()))
                    })?;
                let finished = cursor.current.take().ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded graph continuation lost its completed node".to_string()).into()))
                })?;
                cursor.retained_bytes = next_retained;
                release_retained(finished.frontier.retained_bytes);
                continue;
            }

            let (node, phase, position) = {
                let current = cursor.current.as_ref().ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded graph continuation lost its adjacency position".to_string()).into()))
                })?;
                (current.frontier.node, current.phase, current.position)
            };

            if phase == AdjacencyPhase::Done {
                release_logical_edge(cursor, &mut release_retained)?;
                release_current_anchor(cursor, &mut release_retained)?;
                let finished_bytes = cursor
                    .current
                    .as_ref()
                    .ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded graph continuation lost its completed node".to_string()).into()))
                    })?
                    .frontier
                    .retained_bytes;
                let next_retained = cursor
                    .retained_bytes
                    .checked_sub(finished_bytes)
                    .ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded graph retained-memory accounting underflow".to_string()).into()))
                    })?;
                let finished = cursor.current.take().ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded graph continuation lost its completed node".to_string()).into()))
                })?;
                cursor.retained_bytes = next_retained;
                release_retained(finished.frontier.retained_bytes);
                continue;
            }

            // One admitted staged lookahead is resumed independently of the
            // committed lane. The producer supplies canonical order; its resume
            // is an opaque rank, never a WriteSet slot we reorder here.
            if cursor
                .current
                .as_ref()
                .is_some_and(|current| current.staged_head.is_none() && !current.staged_done)
            {
                before_edge()?;
                let position = cursor.current.as_ref().expect("current").staged_position;
                match staged_edge(node, phase.direction(), position)? {
                    Some((edge, bytes, resume)) => {
                        let retained =
                            match checked_graph_add(cursor.retained_bytes, bytes, "staged head") {
                                Ok(bytes) => bytes,
                                Err(error) => {
                                    drop(edge);
                                    release_retained(bytes);
                                    return Err(E::from(error));
                                }
                            };
                        let current = cursor.current.as_mut().expect("current");
                        current.staged_head = Some((edge, bytes));
                        current.staged_position = resume;
                        cursor.retained_bytes = retained;
                    }
                    None => cursor.current.as_mut().expect("current").staged_done = true,
                }
            }
            if cursor.current.as_ref().expect("current").committed_done {
                if cursor
                    .current
                    .as_ref()
                    .expect("current")
                    .staged_head
                    .is_some()
                {
                    self.consume_staged_head(
                        cursor,
                        node,
                        &mut before_edge,
                        &mut before_path_element,
                        &mut before_retain,
                        &mut release_retained,
                        &mut edge_allowed,
                        &mut entry_read,
                        visited_cap,
                    )?;
                } else {
                    release_current_anchor(cursor, &mut release_retained)?;
                    let current = cursor.current.as_mut().expect("current");
                    current.phase = phase.next(cursor.direction);
                    current.position = 0;
                    current.committed_done = false;
                    current.staged_position = 0;
                    current.staged_done = false;
                }
                continue;
            }
            if let Some(root) = cursor.source_root.as_ref() {
                before_edge()?;
                let current = cursor.current.as_mut().ok_or_else(|| {
                    E::from(Error::Other(
                        "graph continuation lost its source cursor".into()))
                })?;
                if current.source_cursor.is_none() {
                    current.source_cursor = Some(GraphCursorHandle(
                        root.0
                            .cursor(GraphQuery {
                                node,
                                direction: phase.direction(),
                                identity: None,
                            })
                            .map_err(E::from)?,
                    ));
                }
                if current.source_head.is_none() {
                    for _ in 0..current
                        .source_cursor
                        .as_ref()
                        .expect("source cursor installed")
                        .0
                        .next_source_window_bound()
                    {
                        before_edge()?;
                    }
                    current.source_head = current
                        .source_cursor
                        .as_mut()
                        .expect("source cursor installed")
                        .0
                        .next_edge()
                        .map_err(E::from)?
                        .map(TraversalWindow);
                    if current.source_head.is_none() {
                        current.source_cursor = None;
                        current.committed_done = true;
                        continue;
                    }
                }
                let window = &current.source_head.as_ref().expect("source head").0;
                if current.staged_head.as_ref().is_some_and(|(staged, _)| {
                    staged_precedes(staged, window.edge(), window.row_owner(), phase.direction())
                }) {
                    self.consume_staged_head(
                        cursor,
                        node,
                        &mut before_edge,
                        &mut before_path_element,
                        &mut before_retain,
                        &mut release_retained,
                        &mut edge_allowed,
                        &mut entry_read,
                        visited_cap,
                    )?;
                    continue;
                }
                let window = current.source_head.take().expect("source head").0;
                let entry = window.edge();
                let type_allowed = bounded_edge_type_allowed(
                    &cursor.edge_types,
                    &entry.edge_type,
                    &mut before_edge,
                )?;
                if !entry.visible_at(cursor.snapshot)
                    || !type_allowed
                    || (matches!(cursor.direction, Direction::Both)
                        && matches!(phase, AdjacencyPhase::Incoming)
                        && entry.source == entry.target)
                {
                    continue;
                }
                let neighbor = if matches!(phase, AdjacencyPhase::Incoming) {
                    entry.source
                } else {
                    entry.target
                };
                if !edge_allowed(
                    neighbor,
                    entry.source,
                    entry.target,
                    &entry.edge_type,
                    window.row_owner(),
                )? {
                    continue;
                }
                entry_read();
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
                // The admitted window is dropped before advancing or parking.
                continue;
            }
            {
                let adjacency = match phase {
                    AdjacencyPhase::Outgoing => &self.store.forward_adj,
                    AdjacencyPhase::Incoming => &self.store.reverse_adj,
                    AdjacencyPhase::Done => {
                        return Err(E::from(Error::Other((
                            "bounded graph continuation reached a completed adjacency".to_string()).into())));
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
                        E::from(Error::Other((
                            "bounded graph continuation lost its empty adjacency".to_string()).into()))
                    })?;
                    current.committed_done = true;
                    continue;
                };
                // Resume by the identity of the entry this continuation last
                // inspected. Entries removed beneath the parked slot slide every
                // survivor down, so the held slot is re-derived from where that
                // identity now sits before anything at it is read.
                let resume = {
                    let current = cursor.current.as_ref().ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded graph continuation lost its anchored adjacency".to_string()).into()))
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
                            E::from(Error::Other((
                                "bounded graph continuation lost its relocated adjacency"
                                    .to_string()).into()))
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
                            E::from(Error::Other((
                                "bounded graph continuation lost its restarted adjacency"
                                    .to_string()).into()))
                        })?;
                        current.position = 0;
                        continue;
                    }
                }

                if position >= node_entries.len() {
                    drop(entries);
                    release_current_anchor(cursor, &mut release_retained)?;
                    let current = cursor.current.as_mut().ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded graph continuation lost its completed adjacency".to_string()).into()))
                    })?;
                    current.committed_done = true;
                    continue;
                }

                let entry = node_entries.get(position).ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded graph adjacency changed during a read lock".to_string()).into()))
                })?;
                if cursor
                    .current
                    .as_ref()
                    .expect("current")
                    .staged_head
                    .as_ref()
                    .is_some_and(|(staged, _)| {
                        staged_precedes(staged, entry, entry.row_owner.as_ref(), phase.direction())
                    })
                {
                    drop(entries);
                    self.consume_staged_head(
                        cursor,
                        node,
                        &mut before_edge,
                        &mut before_path_element,
                        &mut before_retain,
                        &mut release_retained,
                        &mut edge_allowed,
                        &mut entry_read,
                        visited_cap,
                    )?;
                    continue;
                }
                let next_position = position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other((
                        "bounded graph adjacency position overflow".to_string()).into()))
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
                    AdjacencyPhase::Incoming
                        if entry.target == node
                            && !(matches!(cursor.direction, Direction::Both)
                                && entry.source == entry.target) =>
                    {
                        Some(entry.source)
                    }
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
                            E::from(Error::Other((
                                "bounded graph continuation lost its charged adjacency".to_string()).into()))
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
                            E::from(Error::Other((
                                "bounded graph continuation lost its charged adjacency".to_string()).into()))
                        })?
                        .position = next_position;
                    continue;
                };
                if !edge_allowed(
                    neighbor,
                    source,
                    target,
                    &entry.edge_type,
                    entry.row_owner.as_ref(),
                )? {
                    drop(entries);
                    cursor
                        .current
                        .as_mut()
                        .ok_or_else(|| {
                            E::from(Error::Other((
                                "bounded graph continuation lost its charged adjacency".to_string()).into()))
                        })?
                        .position = next_position;
                    continue;
                }
                entry_read();
                cursor
                    .current
                    .as_mut()
                    .ok_or_else(|| {
                        E::from(Error::Other((
                            "bounded graph continuation lost its charged adjacency".to_string()).into()))
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

    // Taking a staged head transfers its already-admitted bytes out of the
    // continuation. Every exit drops the carrier before releasing those bytes,
    // including type/ACL/admission refusals. Untaken heads stay cursor-owned.
    #[allow(clippy::too_many_arguments)]
    fn consume_staged_head<E>(
        &self,
        cursor: &mut BoundedBfsCursor,
        node: NodeId,
        before_edge: &mut impl FnMut() -> std::result::Result<(), E>,
        before_path_element: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize),
        edge_allowed: &mut impl FnMut(
            NodeId,
            NodeId,
            NodeId,
            &str,
            Option<&RowEdgeOwner>,
        ) -> std::result::Result<bool, E>,
        entry_read: &mut impl FnMut(),
        visited_cap: Option<usize>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let current = cursor.current.as_mut().expect("current staged head");
        let direction = current.phase.direction();
        let (edge, bytes) = current.staged_head.take().expect("staged head selected");
        cursor.retained_bytes -= bytes;
        let result = (|| {
            let type_allowed =
                bounded_edge_type_allowed(&cursor.edge_types, &edge.edge_type, before_edge)?;
            let neighbor = match direction {
                Direction::Outgoing if edge.source == node => Some(edge.target),
                Direction::Incoming
                    if edge.target == node
                        && !(matches!(cursor.direction, Direction::Both)
                            && edge.source == edge.target) =>
                {
                    Some(edge.source)
                }
                _ => None,
            };
            let Some(neighbor) = neighbor.filter(|_| type_allowed) else {
                return Ok(());
            };
            if !edge_allowed(
                neighbor,
                edge.source,
                edge.target,
                &edge.edge_type,
                edge.row_owner.as_ref(),
            )? {
                return Ok(());
            }
            entry_read();
            self.admit_bounded_neighbor(
                cursor,
                node,
                neighbor,
                &edge.edge_type,
                before_edge,
                before_path_element,
                before_retain,
                release_retained,
                visited_cap,
            )
        })();
        drop(edge);
        release_retained(bytes);
        result
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
        let phase = cursor
            .current
            .as_ref()
            .ok_or_else(|| {
                E::from(Error::Other(
                    "graph continuation lost its logical adjacency".into()))
            })?
            .phase;
        let (source, target) = if matches!(phase.direction(), Direction::Incoming) {
            (neighbor, node)
        } else {
            (node, neighbor)
        };
        if !cursor.physical_single_hop
            && !retain_logical_edge(
                cursor,
                source,
                target,
                edge_type,
                before_retain,
                release_retained,
            )?
        {
            return Ok(());
        }
        // An edge that arrives at a node the traversal has already
        // reached is still an edge the caller asked for -- an edge from a
        // node to itself is the plainest case -- so it is reported. What
        // the traversal does not do is walk that node's adjacency again.
        before_edge()?;
        let expand = !cursor.visited.contains(&neighbor);

        let current = cursor.current.as_ref().ok_or_else(|| {
            E::from(Error::Other((
                "bounded graph continuation lost its path source".to_string()).into()))
        })?;
        let next_depth = current
            .frontier
            .depth
            .checked_add(1)
            .ok_or_else(|| E::from(Error::Other(("bounded graph depth overflow".to_string()).into())))?;
        let parent = current.frontier.path;
        let path = append_bounded_path(
            cursor,
            parent,
            node,
            edge_type,
            &mut *before_path_element,
            &mut *before_retain,
            &mut *release_retained,
        )?;
        let path_bytes = 0;
        let queue_bytes = std::mem::size_of::<BoundedQueueEntry>() + 16;
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
                E::from(Error::Other((
                    "bounded graph visited capacity moved backwards".to_string()).into()))
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
            return Err(E::from(Error::Other((
                "bounded graph continuation lost its admitted adjacency".to_string()).into())));
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
            drop(prepared_queue);
            release_retained(queue_bytes);
            cursor.retained_bytes = next_retained - queue_bytes;
            return Err(E::from(Error::BfsVisitedExceeded(cap)));
        }
        prepared_queue.next = cursor.queue_input.take();
        cursor.queue_input = Some(prepared_queue);
        cursor.retained_bytes = next_retained;
        Ok(())
    }

    /// Traverse using the enclosing statement's pinned source root.
    #[allow(clippy::too_many_arguments)]
    pub fn bfs_at_source(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
        root: Option<Arc<dyn GraphSnapshot>>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(TraversalResult, contextdb_core::read_memory::ReadCredit)> {
        use std::cell::RefCell;
        let credit = RefCell::new(contextdb_core::read_memory::ReadCredit::new(read_memory));
        let mut admit = |bytes| {
            credit.borrow_mut().try_grow_for(
                bytes,
                "reader",
                "graph_traversal",
                "Raise the declared reader memory limit.",
            )
        };
        let mut cursor = self.bounded_bfs_cursor_at_source::<Error>(
            start,
            edge_types,
            direction,
            min_depth,
            max_depth,
            snapshot,
            root,
            admit,
            |bytes| {
                credit
                    .borrow_mut()
                    .try_shrink(bytes)
                    .expect("graph kernel releases only admitted storage")
            },
        )?;
        let cap = self
            .bfs_visited_cap
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut nodes = Vec::new();
        let (mut emitted, mut emitted_bytes) =
            bounded_graph_visited(start, &mut admit, &mut |bytes| {
                credit
                    .borrow_mut()
                    .try_shrink(bytes)
                    .expect("graph dedupe releases admitted storage")
            })?;
        while let Some(output) = self.bounded_bfs_next_including_staged::<Error>(
            &mut cursor,
            || Ok(()),
            || Ok(()),
            admit,
            |bytes| {
                credit
                    .borrow_mut()
                    .try_shrink(bytes)
                    .expect("graph kernel releases only admitted storage")
            },
            |_, _, _, _| Ok(true),
            |_, _, _| Ok(None),
            || {},
            Some(cap),
        )? {
            {
                if emitted.contains(&output.node.id) {
                    let bytes = output.reserved_bytes;
                    drop(output);
                    credit.borrow_mut().try_shrink(bytes)?;
                    continue;
                }
                if let Some((replacement, actual, previous)) = prepare_graph_visited_growth(
                    &emitted,
                    output.node.id,
                    &mut || Ok(()),
                    &mut admit,
                    &mut |bytes| {
                        credit
                            .borrow_mut()
                            .try_shrink(bytes)
                            .expect("graph dedupe releases admitted storage")
                    },
                )? {
                    let old = std::mem::replace(&mut emitted, replacement);
                    drop(old);
                    credit.borrow_mut().try_shrink(previous)?;
                    emitted_bytes = actual;
                } else {
                    emitted.insert(output.node.id);
                }
            }
            if nodes.len() == nodes.capacity() {
                let old = nodes.capacity() * std::mem::size_of::<TraversalNode>();
                let capacity = nodes
                    .len()
                    .checked_add(1)
                    .and_then(usize::checked_next_power_of_two)
                    .ok_or_else(|| Error::Other("graph result capacity overflow".into()))?;
                let requested = bounded_capacity_bytes::<TraversalNode>(capacity, "result")?;
                admit(requested)?;
                let mut replacement = Vec::new();
                if replacement.try_reserve_exact(capacity).is_err() {
                    drop(replacement);
                    credit.borrow_mut().try_shrink(requested)?;
                    return Err(Error::Other("graph result allocation failed".into()));
                }
                let actual = replacement.capacity() * std::mem::size_of::<TraversalNode>();
                if actual > requested {
                    admit(actual - requested)?;
                } else {
                    credit.borrow_mut().try_shrink(requested - actual)?;
                }
                replacement.append(&mut nodes);
                let previous = std::mem::replace(&mut nodes, replacement);
                drop(previous);
                credit.borrow_mut().try_shrink(old)?;
            }
            nodes.push(output.node);
        }
        credit
            .borrow_mut()
            .try_shrink(cursor.into_retained_bytes())?;
        drop(emitted);
        credit.borrow_mut().try_shrink(emitted_bytes)?;
        Ok((TraversalResult { nodes }, credit.into_inner()))
    }

    fn current_edge_properties(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<std::collections::HashMap<String, Value>>> {
        let snapshot = self.tx_mgr.snapshot();
        let root = self.store.capture_source(snapshot, read_memory)?;
        let mut existing = None;
        self.store.visit_edges_with_owner(
            root.as_ref(),
            GraphQuery {
                node: source,
                direction: Direction::Outgoing,
                identity: Some((target, edge_type)),
            },
            |entry, owner| {
                if owner.is_none() && entry.visible_at(snapshot) {
                    existing = Some(entry.properties.clone());
                }
                Ok(true)
            },
        )?;
        Ok(existing)
    }

    fn current_owned_edge_properties(
        &self, source: NodeId, target: NodeId, edge_type: &str,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<OwnedEdgePropertyCopy>> {
        let snapshot = self.tx_mgr.snapshot();
        let root = self.store.capture_source(snapshot, reader.clone())?;
        let mut existing = None;
        self.store.visit_edges_with_owner(root.as_ref(), GraphQuery {
            node: source, direction: Direction::Outgoing, identity: Some((target, edge_type)),
        }, |entry, row_owner| {
            if row_owner.is_none() && entry.visible_at(snapshot) {
                let controls = entry.properties.capacity().checked_mul(
                    std::mem::size_of::<(String, Value)>().saturating_add(1).saturating_mul(4))
                    .ok_or_else(|| Error::Other("graph property copy size overflow".into()))?;
                let bytes = controls.checked_add(entry.estimated_bytes())
                    .ok_or_else(|| Error::Other("graph property copy size overflow".into()))?;
                let memory = contextdb_core::read_memory::ReadCredit::try_new(reader.clone(), bytes,
                    "graph", "edge_property_input", "Raise MEMORY_LIMIT to copy this edge's properties.")?;
                existing = Some(OwnedEdgePropertyCopy { values: entry.properties.clone(), _memory: memory });
            }
            Ok(true)
        })?;
        Ok(existing)
    }

    fn owned_dag_reaches(
        &self, handle: &contextdb_tx::manager::EngineTransactionHandle<S>, tx: TxId,
        start: NodeId, goal: NodeId, edge_type: &str,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        let mut memory = contextdb_core::read_memory::ReadCredit::new(reader.clone());
        let snapshot = self.tx_mgr.snapshot();
        let root = self.store.capture_source(snapshot, reader)?;
        let mut visited = std::collections::BTreeSet::new();
        let mut queue = VecDeque::new();
        let mut enqueue = |node: NodeId, visited: &mut std::collections::BTreeSet<NodeId>, queue: &mut VecDeque<NodeId>| -> Result<()> {
            if visited.contains(&node) { return Ok(()); }
            // One worst-case B-tree node plus geometric queue growth and its
            // old/new overlap per newly retained node. No staged-edge directory.
            let bytes = std::mem::size_of::<NodeId>().checked_mul(16)
                .and_then(|n| n.checked_add(128)).ok_or_else(|| Error::Other("graph traversal size overflow".into()))?;
            memory.try_grow_for(bytes, "graph", "dag_cycle_check",
                "Raise MEMORY_LIMIT to validate this DAG insertion.")?;
            visited.insert(node); queue.push_back(node); Ok(())
        };
        enqueue(start, &mut visited, &mut queue)?;
        while let Some(current) = queue.pop_front() {
            let mut found = false;
            self.store.visit_edges(root.as_ref(), GraphQuery {
                node: current, direction: Direction::Outgoing, identity: None,
            }, |entry| {
                if entry.edge_type != edge_type || !entry.visible_at(snapshot) { return Ok(true); }
                let deleted = handle.with_transactions(|transactions| transactions.with_read(tx, |read| {
                    read.adj_deletes.iter().any(|(source, kind, target, _)|
                        *source == entry.source && *target == entry.target && kind == edge_type)
                }))?;
                if deleted { return Ok(true); }
                if entry.target == goal { found = true; return Ok(false); }
                enqueue(entry.target, &mut visited, &mut queue)?;
                Ok(true)
            })?;
            if found { return Ok(true); }
            let found = handle.with_transactions(|transactions| transactions.with_read(tx, |read| -> Result<bool> {
                for entry in read.adj_inserts.iter().filter(|entry| entry.source == current && entry.edge_type == edge_type) {
                    if entry.target == goal { return Ok(true); }
                    enqueue(entry.target, &mut visited, &mut queue)?;
                }
                Ok(false)
            }))??;
            if found { return Ok(true); }
        }
        Ok(false)
    }

    fn stage_owned_graph_entry(
        &self, handle: &contextdb_tx::manager::EngineTransactionHandle<S>, tx: TxId,
        entry: &AdjEntry, replacement: bool,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let _input = contextdb_core::read_memory::ReadCredit::try_new(reader, entry.edge_type.len(),
            "graph", "edge_delete_input", "Raise MEMORY_LIMIT to stage this edge.")?;
        let deletes = [(entry.source, entry.edge_type.clone(), entry.target, tx)];
        handle.with_transactions(|transactions| transactions.with_engine(tx, |held| {
            held.with_tx_checkpoint(|held| {
                if replacement {
                    loop {
                        let position = held.read().adj_inserts.iter().rposition(|old|
                            old.source == entry.source && old.target == entry.target && old.edge_type == entry.edge_type);
                        let Some(position) = position else { break; };
                        let empty = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::AdjInserts(&[]))?;
                        let remove = held.prepare_aux_splice(position, &[position], empty)?;
                        held.publish_tx(remove);
                    }
                    if !held.read().adj_deletes.iter().any(|(source, kind, target, _)|
                        *source == entry.source && *target == entry.target && kind == &entry.edge_type) {
                        let delete = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::AdjDeletes(&deletes))?;
                        let delete = held.prepare_aux_splice(held.read().adj_deletes.len(), &[], delete)?;
                        held.publish_tx(delete);
                    }
                }
                // This is an independent original copy funded by the actual
                // source. The reader's temporary input credit never transfers.
                let insert = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::AdjInserts(std::slice::from_ref(entry)))?;
                let insert = held.prepare_aux_splice(held.read().adj_inserts.len(), &[], insert)?;
                held.publish_tx(insert);
                Ok(())
            })
        }))
    }

    fn insert_owned_edge(
        &self, handle: &contextdb_tx::manager::EngineTransactionHandle<S>, tx: TxId,
        source: NodeId, target: NodeId, edge_type: String,
        properties: std::collections::HashMap<String, Value>,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        let deleted = handle.with_transactions(|transactions| transactions.with_read(tx, |read|
            read.adj_deletes.iter().any(|(s, kind, t, _)| *s == source && *t == target && kind == &edge_type)))?;
        if !deleted && let Some(mut existing) = self.current_owned_edge_properties(source, target, &edge_type, reader.clone())? {
            if properties.is_empty() || properties.iter().all(|(key, value)| existing.values.get(key) == Some(value)) { return Ok(false); }
            let controls = properties.len().checked_add(existing.values.len())
                .and_then(|n| n.checked_mul(std::mem::size_of::<(String, Value)>().saturating_add(1).saturating_mul(8)))
                .ok_or_else(|| Error::Other("graph merged property size overflow".into()))?;
            existing._memory.try_grow_for(controls, "graph", "edge_property_input",
                "Raise MEMORY_LIMIT to merge this edge's properties.")?;
            existing.values.extend(properties);
            let entry = AdjEntry { source, target, edge_type, properties: std::mem::take(&mut existing.values),
                created_tx: tx, deleted_tx: None, lsn: Lsn(0) };
            self.stage_owned_graph_entry(handle, tx, &entry, true, reader)?;
            return Ok(false);
        }
        let duplicate = handle.with_transactions(|transactions| transactions.with_read(tx, |read|
            read.adj_inserts.iter().any(|entry| entry.source == source && entry.target == target && entry.edge_type == edge_type)))?;
        if duplicate { return Ok(false); }
        if self.dag_edge_types.read().contains(&edge_type)
            && (source == target || self.owned_dag_reaches(handle, tx, target, source, &edge_type, reader.clone())?) {
            return Err(Error::CycleDetected { edge_type, source_node: source, target_node: target });
        }
        let entry = AdjEntry { source, target, edge_type, properties, created_tx: tx, deleted_tx: None, lsn: Lsn(0) };
        self.stage_owned_graph_entry(handle, tx, &entry, false, reader)?;
        Ok(true)
    }

    /// Carry one edge forward with a new property set, as one replacement.
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
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<TraversalResult> {
        let root = self.store.capture_source(snapshot, read_memory.clone())?;
        self.bfs_at_source(
            start,
            edge_types,
            direction,
            min_depth,
            max_depth,
            snapshot,
            root,
            read_memory,
        )
        .map(|(result, _credit)| result)
    }

    fn neighbors(
        &self,
        node: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<(NodeId, EdgeType, std::collections::HashMap<String, Value>)>> {
        let root = self.store.capture_source(snapshot, read_memory.clone())?;
        let mut results = Vec::new();
        self.visit_neighbors(
            root.as_ref(),
            node,
            edge_types,
            direction,
            snapshot,
            read_memory,
            |neighbor, entry| {
                results.push((neighbor, entry.edge_type.clone(), entry.properties.clone()));
                Ok(())
            },
        )?;

        Ok(results)
    }

    fn insert_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        properties: std::collections::HashMap<String, Value>,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        if let ExecutorTransactions::Owned(handle) = &self.tx_mgr {
            return self.insert_owned_edge(handle, tx, source, target, edge_type, properties, read_memory);
        }
        let deleted_in_ws = self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_deletes
                .iter()
                .any(|(s, et, t, _)| *s == source && *t == target && et == &edge_type)
        })?;

        {
            let existing =
                self.current_edge_properties(source, target, &edge_type, read_memory.clone())?;
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

            if self.bfs_with_write_set(tx, target, source, &edge_type, read_memory)? {
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

    fn delete_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let snapshot = self.tx_mgr.snapshot();
        let root = self.store.capture_source(snapshot, read_memory.clone())?;
        let mut committed_edge_exists = false;
        self.store.visit_edges_with_owner(
            root.as_ref(),
            GraphQuery {
                node: source,
                direction: Direction::Outgoing,
                identity: Some((target, edge_type)),
            },
            |entry, owner| {
                if owner.is_none() && entry.visible_at(snapshot) {
                    committed_edge_exists = true;
                    return Ok(false);
                }
                Ok(true)
            },
        )?;

        if let ExecutorTransactions::Owned(handle) = &self.tx_mgr {
            let _input = contextdb_core::read_memory::ReadCredit::try_new(read_memory, edge_type.len(),
                "graph", "edge_delete_input", "Raise MEMORY_LIMIT to stage this edge deletion.")?;
            let deletes = [(source, edge_type.to_owned(), target, tx)];
            return handle.with_transactions(|transactions| transactions.with_engine(tx, |held| {
                held.with_tx_checkpoint(|held| {
                    loop {
                        let position = held.read().adj_inserts.iter().rposition(|entry|
                            entry.source == source && entry.target == target && entry.edge_type == edge_type);
                        let Some(position) = position else { break; };
                        let empty = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::AdjInserts(&[]))?;
                        let remove = held.prepare_aux_splice(position, &[position], empty)?;
                        held.publish_tx(remove);
                    }
                    if committed_edge_exists && !held.read().adj_deletes.iter().any(|(s, kind, t, _)|
                        *s == source && *t == target && kind == edge_type) {
                        let delete = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::AdjDeletes(&deletes))?;
                        let delete = held.prepare_aux_splice(held.read().adj_deletes.len(), &[], delete)?;
                        held.publish_tx(delete);
                    }
                    Ok(())
                })
            }));
        }

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

struct OwnedEdgePropertyCopy {
    values: std::collections::HashMap<String, Value>,
    _memory: contextdb_core::read_memory::ReadCredit,
}
