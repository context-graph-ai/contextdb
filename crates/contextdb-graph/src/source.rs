//! Storage-neutral adjacency reads. The engine supplies pinned durable roots;
//! windows keep their existing admission owner until the consumer drops them.
use contextdb_core::read_memory::{OwnedReadBox, ReadCredit};
use contextdb_core::{AdjEntry, Direction, NodeId, Result, SnapshotId};
use std::sync::Arc;

/// A node prefix, or one complete edge identity within that prefix.
/// `Both` is split into two cursors by the consumer.
#[derive(Clone, Copy, Debug)]
pub struct GraphQuery<'a> {
    pub node: NodeId,
    pub direction: Direction,
    pub identity: Option<(NodeId, &'a str)>,
}

/// Captures the engine's already published root without opening another store.
/// A statement-bound adapter must return that statement's root on every capture.
pub trait GraphReadSource: Send + Sync {
    fn capture(
        &self,
        snapshot: SnapshotId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Arc<dyn GraphSnapshot>>;
}

/// One immutable physical root with retained MVCC intervals. Historical logical
/// snapshots filter this root; they never substitute newer visible edges.
pub trait GraphSnapshot: Send + Sync {
    /// Seek the node/identity prefix, retaining a full physical continuation
    /// key. No adjacency list is materialized, including for high-degree nodes.
    fn cursor(&self, query: GraphQuery<'_>) -> Result<GraphCursorOwner>;
}

pub trait GraphCursor: Send {
    /// Return one admitted version window. Advancing must not retain earlier
    /// decoded windows; errors, cancellation and drop release cursor scratch.
    fn next_edge(&mut self) -> Result<Option<OwnedReadBox<dyn GraphEdgeWindow>>>;
}

/// Implement this on the durable window itself, keeping its charge owner with
/// its payload. Consumers borrow the edge rather than extracting its body.
pub trait GraphEdgeWindow: Send {
    fn edge(&self) -> &AdjEntry;
}

/// Borrowed transaction overlay; its owner freezes it for the statement. No
/// committed edge or second copy of a write set is retained by this view.
pub struct GraphOverlay<'a> {
    pub inserts: &'a [AdjEntry],
    pub deletes: &'a [(NodeId, String, NodeId, contextdb_core::TxId)],
}

impl GraphOverlay<'_> {
    pub fn hides(&self, edge: &AdjEntry) -> bool {
        self.deletes.iter().any(|(source, edge_type, target, _)| {
            *source == edge.source && *target == edge.target && *edge_type == edge.edge_type
        })
    }
}

// The root and cursor implementations need not expose storage internals to Debug.
pub(crate) struct GraphReadHandle(pub Arc<dyn GraphSnapshot>);
pub(crate) struct GraphCursorHandle(pub GraphCursorOwner);

impl std::fmt::Debug for GraphReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GraphReadHandle")
    }
}
impl std::fmt::Debug for GraphCursorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GraphCursorHandle")
    }
}

/// Restricted cursor owner: complete Box destruction precedes credit release.
#[doc(hidden)]
pub struct GraphCursorOwner {
    payload: Box<dyn GraphCursor>,
    _credit: ReadCredit,
}
impl GraphCursorOwner {
    pub fn new(payload: Box<dyn GraphCursor>, credit: ReadCredit) -> Self {
        Self {
            payload,
            _credit: credit,
        }
    }
    #[allow(clippy::should_implement_trait)] // The owner exposes its boxed cursor without releasing its credit.
    pub fn as_ref(&self) -> &dyn GraphCursor {
        self.payload.as_ref()
    }
}
impl GraphCursor for GraphCursorOwner {
    fn next_edge(&mut self) -> Result<Option<OwnedReadBox<dyn GraphEdgeWindow>>> {
        self.payload.next_edge()
    }
}
