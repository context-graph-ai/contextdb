//! Letting go of a long traversal costs one stack frame, not one per queued
//! node.
//!
//! A bounded traversal's queue is a chain of boxed entries, and a caller may
//! abandon a traversal at any point: a refusal, a deadline, a cancelled
//! request, or simply dropping the cursor. If that chain fell apart on its
//! own, each entry would be released inside the release of the entry before
//! it -- one stack frame per queued node -- so abandoning a wide traversal
//! would overflow the stack and take the whole process down instead of
//! reporting anything. A reader cannot be told "your query was refused" by a
//! process that is no longer running.
//!
//! So the chain is taken apart iteratively, and this proves it where it
//! matters: a queue holding a million entries is dropped on a thread with a
//! quarter of a megabyte of stack -- far too little to hold a million frames,
//! and ample for the one frame the iterative release needs.

use contextdb_core::{AdjEntry, Direction, Error, Lsn, NodeId, Result, RowId, SnapshotId, TxId};
use contextdb_graph::mem::BoundedBfsCursor;
use contextdb_graph::{GraphStore, MemGraphExecutor};
use contextdb_tx::{TransactionManager, WriteSet, WriteSetApplicator};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Queued entries the abandoned traversal is holding. Chosen far above the
/// number of frames a small stack can hold, so a per-entry release cannot
/// finish by luck.
const QUEUED_ENTRIES: usize = 1_000_000;

/// Edges out of the hub. One pull queues every neighbour it reads and then
/// takes the first of them back off the queue to answer with, so one more edge
/// than the queue is meant to hold leaves exactly that many behind.
const SEEDED_EDGES: usize = QUEUED_ENTRIES + 1;

/// The stack the release runs on. A per-entry release needs a frame for each
/// of a million entries and cannot fit here; an iterative one needs one frame
/// and fits many times over.
const RELEASE_STACK_BYTES: usize = 256 * 1024;

/// Edges are staged into the adjacency in batches, so seeding a wide
/// neighbourhood never holds two copies of the whole thing at once.
const SEED_BATCH: usize = 50_000;

/// The transaction the seeded edges were created by, and a snapshot taken
/// after it: every seeded edge is visible to the traversal.
const SEEDED_TX: TxId = TxId(1);
const SEEDED_SNAPSHOT: SnapshotId = SnapshotId(2);

struct TestStore {
    graph: Arc<GraphStore>,
}

impl WriteSetApplicator for TestStore {
    fn apply(&self, ws: &WriteSet) -> Result<()> {
        self.graph.apply_inserts(ws.adj_inserts.clone());
        self.graph.apply_deletes(ws.adj_deletes.clone());
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        RowId(1)
    }
}

fn node(ordinal: u128) -> NodeId {
    Uuid::from_u128(0x0DDE_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn edge(source: NodeId, target: NodeId) -> AdjEntry {
    AdjEntry {
        source,
        target,
        edge_type: "LINKS".to_string(),
        properties: HashMap::new(),
        created_tx: SEEDED_TX,
        deleted_tx: None,
        lsn: Lsn(1),
    }
}

/// One node with a million edges out of it. A pull reads the whole of a
/// node's adjacency before it answers, so a single pull over this hub queues
/// every neighbour at once -- which is exactly the shape a caller abandons
/// when a wide traversal is refused, deadlined or cancelled on its first
/// answer.
fn hub_with_many_edges() -> (Arc<GraphStore>, MemGraphExecutor<TestStore>, NodeId) {
    let graph = Arc::new(GraphStore::new());
    let transactions = Arc::new(TransactionManager::new(TestStore {
        graph: Arc::clone(&graph),
    }));
    let executor = MemGraphExecutor::new(Arc::clone(&graph), Arc::clone(&transactions));

    let hub = node(0);
    let mut seeded = 0usize;
    while seeded < SEEDED_EDGES {
        let batch: Vec<AdjEntry> = (seeded..(seeded + SEED_BATCH).min(SEEDED_EDGES))
            .map(|ordinal| edge(hub, node(ordinal as u128 + 1)))
            .collect();
        seeded += batch.len();
        graph.apply_inserts(batch);
    }

    (graph, executor, hub)
}

#[test]
fn a_traversal_queue_holding_a_million_entries_is_released_on_a_small_stack() {
    let (_graph, executor, hub) = hub_with_many_edges();

    let mut cursor: BoundedBfsCursor = executor
        .bounded_bfs_cursor::<Error>(
            hub,
            None,
            Direction::Outgoing,
            1,
            1,
            SEEDED_SNAPSHOT,
            |_| Ok(()),
            |_| {},
        )
        .expect("open a traversal continuation on the hub");

    // One pull. It reads every edge out of the hub, queueing a frontier entry
    // for each, and answers with the first neighbour it takes back off the
    // queue -- leaving the rest of them queued in the continuation.
    let emitted = executor
        .bounded_bfs_next::<Error>(
            &mut cursor,
            || Ok(()),
            || Ok(()),
            |_| Ok(()),
            |_| {},
            |_, _, _, _| Ok(true),
        )
        .expect("a pull over the hub's adjacency must succeed");
    assert!(
        emitted.is_some(),
        "the pull must answer a neighbour, which is what leaves the rest of \
         the hub's neighbourhood queued in the continuation"
    );

    // Abandoned here, on a stack that cannot hold a frame per queued entry.
    let release = std::thread::Builder::new()
        .stack_size(RELEASE_STACK_BYTES)
        .spawn(move || drop(cursor))
        .expect("spawn the thread that abandons the traversal");

    assert!(
        release.join().is_ok(),
        "abandoning a traversal holding {QUEUED_ENTRIES} queued entries must \
         return, not exhaust the stack it is released on"
    );
}
