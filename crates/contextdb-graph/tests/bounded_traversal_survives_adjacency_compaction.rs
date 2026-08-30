//! A suspended multi-hop traversal must resume by the identity of the
//! adjacency entry it last consumed, never by a numeric offset into the
//! source's adjacency vector.
//!
//! The bounded traversal continuation is owned by the caller and holds no
//! store lock between pulls, so anything that physically removes an adjacency
//! entry runs freely while the continuation is parked.  Removal preserves the
//! relative order of the entries it keeps, so every surviving entry BELOW the
//! parked point slides down by exactly the number of entries removed under
//! it.  An offset kept across that pause then addresses an entry the reader
//! has never seen, and the entries between the two are emitted to nobody: a
//! silent skip inside a read the caller was promised is complete for its
//! snapshot.
//!
//! Both journeys below park the continuation the only way a caller can — the
//! traversal asks to charge its next adjacency inspection and the caller
//! refuses, which leaves the continuation parked exactly where the refusal
//! landed.  Nothing sleeps and nothing runs on another thread: the removal
//! happens between two pulls on this thread.
//!
//! The first journey removes every entry the continuation had already
//! consumed, so no consumed identity survives the pause at all; the second
//! leaves one consumed entry in place, so the nearest surviving identity is
//! the anchor.  A fixture whose entry offsets survive the removal unchanged
//! would pass without any anchoring at all, so both fixtures are built so
//! that the offsets genuinely move.

use contextdb_core::{Direction, Error, GraphExecutor, NodeId, Result, RowId};
use contextdb_graph::mem::BoundedBfsCursor;
use contextdb_graph::{GraphStore, MemGraphExecutor};
use contextdb_tx::{TransactionManager, WriteSet, WriteSetApplicator};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

/// The promise these journeys protect, quoted in every failure so a reader of
/// the failure does not have to come back here for it.
const TRAVERSAL_PROMISE: &str = "a suspended bounded traversal pages one committed snapshot: \
                                 every neighbour that snapshot can see is emitted exactly once, \
                                 even when adjacency entries the snapshot cannot see are \
                                 physically removed while the traversal is parked";

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

/// What the caller admits, and what the access gate answers. The charge
/// budget is the caller's declared ceiling on adjacency inspections: the
/// traversal charges exactly once before it inspects one entry, so spending
/// the budget parks the continuation at a known entry.
struct Caller {
    remaining: Cell<usize>,
    spent: Cell<usize>,
    denied: HashSet<NodeId>,
}

impl Caller {
    fn new(budget: usize, denied: HashSet<NodeId>) -> Self {
        Self {
            remaining: Cell::new(budget),
            spent: Cell::new(0),
            denied,
        }
    }

    fn charge(&self) -> std::result::Result<(), Error> {
        match self.remaining.get().checked_sub(1) {
            Some(left) => {
                self.remaining.set(left);
                self.spent.set(self.spent.get() + 1);
                Ok(())
            }
            None => Err(Error::Other(
                "declared traversal ceiling reached".to_string(),
            )),
        }
    }

    fn lift_ceiling(&self, budget: usize) {
        self.remaining.set(budget);
    }

    fn spent(&self) -> usize {
        self.spent.get()
    }
}

fn setup() -> (
    Arc<GraphStore>,
    Arc<TransactionManager<TestStore>>,
    MemGraphExecutor<TestStore>,
) {
    let graph = Arc::new(GraphStore::new());
    let tx_mgr = Arc::new(TransactionManager::new(TestStore {
        graph: graph.clone(),
    }));
    let exec = MemGraphExecutor::new(graph.clone(), tx_mgr.clone());
    (graph, tx_mgr, exec)
}

fn node(ordinal: u128) -> NodeId {
    Uuid::from_u128(0x9E11_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn connect(
    tx_mgr: &TransactionManager<TestStore>,
    exec: &MemGraphExecutor<TestStore>,
    source: NodeId,
    target: NodeId,
) {
    let tx = tx_mgr.begin();
    exec.insert_edge(tx, source, target, "LINKS".to_string(), HashMap::new())
        .expect("seed an edge");
    tx_mgr.commit(tx).expect("commit the seeded edge");
}

fn withdraw(
    tx_mgr: &TransactionManager<TestStore>,
    exec: &MemGraphExecutor<TestStore>,
    source: NodeId,
    target: NodeId,
) {
    let tx = tx_mgr.begin();
    exec.delete_edge(tx, source, target, "LINKS")
        .expect("withdraw an edge before any traversal opens");
    tx_mgr.commit(tx).expect("commit the withdrawal");
}

/// One pull of the traversal. The caller's charge runs before every adjacency
/// inspection, and the access gate answers for the neighbour the entry names.
fn pull(
    exec: &MemGraphExecutor<TestStore>,
    cursor: &mut BoundedBfsCursor,
    caller: &Caller,
) -> std::result::Result<Option<NodeId>, Error> {
    exec.bounded_bfs_next::<Error>(
        cursor,
        || caller.charge(),
        || Ok(()),
        |_| Ok(()),
        |_| {},
        |neighbour, _source, _target, _edge_type| Ok(!caller.denied.contains(&neighbour)),
    )
    .map(|emitted| emitted.map(|emitted| emitted.node.id))
}

/// Drain the traversal to exhaustion, collecting every emitted node in order.
fn drain(
    exec: &MemGraphExecutor<TestStore>,
    cursor: &mut BoundedBfsCursor,
    caller: &Caller,
    journey: &str,
) -> Vec<NodeId> {
    let mut emitted = Vec::new();
    loop {
        match pull(exec, cursor, caller) {
            Ok(Some(id)) => emitted.push(id),
            Ok(None) => return emitted,
            Err(error) => panic!(
                "{journey}: {TRAVERSAL_PROMISE}. A pull after the removal failed instead of \
                 continuing the parked traversal: {error:?}"
            ),
        }
    }
}

/// Physically remove every withdrawn adjacency entry on `source`, the way a
/// reclaim of entries no reader can still see does. Returns how many entries
/// were removed, so a journey can prove the offsets really moved.
fn reclaim_withdrawn_entries(store: &GraphStore, source: NodeId) -> usize {
    let mut forward = store.forward_adj.write();
    let mut reverse = store.reverse_adj.write();
    let entries = forward
        .get_mut(&source)
        .expect("the source must carry adjacency entries");
    let before = entries.len();
    entries.retain(|entry| entry.deleted_tx.is_none());
    let removed = before - entries.len();
    for entries in reverse.values_mut() {
        entries.retain(|entry| entry.source != source || entry.deleted_tx.is_none());
    }
    removed
}

fn assert_emitted_exactly_once(emitted: &[NodeId], expected: &[NodeId], journey: &str) {
    let mut sorted = emitted.to_vec();
    sorted.sort();
    let mut expected_sorted = expected.to_vec();
    expected_sorted.sort();
    assert_eq!(
        sorted, expected_sorted,
        "{journey}: {TRAVERSAL_PROMISE}. The traversal emitted {emitted:?} in order; the \
         snapshot's visible neighbourhood is {expected:?}"
    );
}

/// The parked continuation had consumed only entries that the reclaim then
/// removed, so nothing it consumed survives the pause. Every entry below the
/// parked point slides down by two, and the two neighbours that land under
/// the held offset are the ones a numeric resume never reaches.
#[test]
fn a_parked_traversal_emits_every_visible_neighbour_when_all_the_entries_it_consumed_are_reclaimed()
{
    let journey = "parked traversal, every consumed adjacency entry reclaimed under it";
    let (store, tx_mgr, exec) = setup();

    let source = node(0);
    // Written first, so they hold the FRONT of the source's adjacency.
    let withdrawn = [node(1), node(2)];
    let kept = [node(11), node(12), node(13), node(14)];
    // A second hop, so the journey is a real multi-hop traversal rather than
    // one adjacency walk.
    let second_hop = node(21);

    for target in withdrawn {
        connect(&tx_mgr, &exec, source, target);
    }
    for target in kept {
        connect(&tx_mgr, &exec, source, target);
    }
    connect(&tx_mgr, &exec, kept[0], second_hop);
    for target in withdrawn {
        withdraw(&tx_mgr, &exec, source, target);
    }

    // Taken after the withdrawal commits, so the two front entries are
    // invisible here and the traversal must walk straight past them.
    let snapshot = tx_mgr.snapshot();

    // Two inspections: exactly the two withdrawn entries. The third
    // inspection — the first kept entry — is refused, parking the
    // continuation two entries into the vector.
    let caller = Caller::new(withdrawn.len(), HashSet::new());
    let mut cursor = exec
        .bounded_bfs_cursor::<Error>(
            source,
            None,
            Direction::Outgoing,
            1,
            2,
            snapshot,
            |_| Ok(()),
            |_| {},
        )
        .expect("open a bounded traversal over the source's neighbourhood");

    let parked = pull(&exec, &mut cursor, &caller);
    assert!(
        parked.is_err(),
        "{journey}: the fixture must park the traversal inside the source's adjacency, otherwise \
         nothing is held across the reclaim; the pull returned {parked:?}"
    );
    assert_eq!(
        caller.spent(),
        withdrawn.len(),
        "{journey}: the traversal must have inspected exactly the two withdrawn entries before \
         the refusal, so the reclaim below lands strictly beneath the parked point"
    );

    let removed = reclaim_withdrawn_entries(&store, source);
    assert_eq!(
        removed,
        withdrawn.len(),
        "{journey}: both withdrawn entries must really be removed, otherwise the surviving \
         entries never slide and this journey proves nothing"
    );

    caller.lift_ceiling(usize::MAX);
    let emitted = drain(&exec, &mut cursor, &caller, journey);

    let mut expected = kept.to_vec();
    expected.push(second_hop);
    assert_emitted_exactly_once(&emitted, &expected, journey);
}

/// The parked continuation had consumed one entry the reclaim leaves in
/// place — an edge the snapshot can see whose neighbour the access gate
/// refused. That entry is the nearest surviving identity below the parked
/// point, so resuming strictly after it is always possible; a numeric resume
/// still skips the two neighbours the reclaim shifted past the held offset.
#[test]
fn a_parked_traversal_emits_every_visible_neighbour_when_a_consumed_entry_survives_the_reclaim() {
    let journey = "parked traversal, one consumed adjacency entry survives the reclaim";
    let (store, tx_mgr, exec) = setup();

    let source = node(0);
    let withdrawn = [node(1), node(2)];
    // Visible at the snapshot, but its neighbour is refused by the access
    // gate, so the traversal consumes the entry without emitting anything.
    let refused = node(3);
    let kept = [node(11), node(12), node(13), node(14)];
    let second_hop = node(21);

    for target in withdrawn {
        connect(&tx_mgr, &exec, source, target);
    }
    connect(&tx_mgr, &exec, source, refused);
    for target in kept {
        connect(&tx_mgr, &exec, source, target);
    }
    connect(&tx_mgr, &exec, kept[0], second_hop);
    for target in withdrawn {
        withdraw(&tx_mgr, &exec, source, target);
    }

    let snapshot = tx_mgr.snapshot();

    // Three inspections: the two withdrawn entries and the refused one. The
    // fourth — the first kept entry — is refused, parking the continuation
    // three entries into the vector.
    let denied: HashSet<NodeId> = [refused].into_iter().collect();
    let caller = Caller::new(withdrawn.len() + 1, denied);
    let mut cursor = exec
        .bounded_bfs_cursor::<Error>(
            source,
            None,
            Direction::Outgoing,
            1,
            2,
            snapshot,
            |_| Ok(()),
            |_| {},
        )
        .expect("open a bounded traversal over the source's neighbourhood");

    let parked = pull(&exec, &mut cursor, &caller);
    assert!(
        parked.is_err(),
        "{journey}: the fixture must park the traversal inside the source's adjacency, otherwise \
         nothing is held across the reclaim; the pull returned {parked:?}"
    );
    assert_eq!(
        caller.spent(),
        withdrawn.len() + 1,
        "{journey}: the traversal must have inspected the two withdrawn entries and the refused \
         one before the refusal, so one consumed entry survives the reclaim below"
    );

    let removed = reclaim_withdrawn_entries(&store, source);
    assert_eq!(
        removed,
        withdrawn.len(),
        "{journey}: both withdrawn entries must really be removed, otherwise the surviving \
         entries never slide and this journey proves nothing"
    );

    caller.lift_ceiling(usize::MAX);
    let emitted = drain(&exec, &mut cursor, &caller, journey);

    let mut expected = kept.to_vec();
    expected.push(second_hop);
    assert_emitted_exactly_once(&emitted, &expected, journey);
}
