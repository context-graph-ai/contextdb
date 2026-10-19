//! The graph keeps one adjacency copy per row-owning version and one per
//! standalone edge, and every deletion addresses exactly its own owner.
//!
//! Promise (Policy 12 and Policy 32): two SQL rows with the same source,
//! target and edge type are two independent graph artifacts in both directions;
//! deleting or erasing one leaves the other row's edge and a standalone edge
//! with the same triad untouched; a traversal reaches the target once however
//! many owners lead to it; and each owner keeps its own visibility interval, so
//! a later sibling never acquires an earlier one's history.
//!
//! Reason (2026-10-16): the graph store deduplicated by triad, so a second owner
//! of an existing triad left no artifact of its own and an owner-addressed
//! deletion could end the wrong copy. No existing test holds two owners of one
//! triad or a standalone edge beside a row-owned one.

#[path = "../../contextdb-tx/tests/support/read_memory.rs"]
mod test_read_memory;

use contextdb_core::{AdjEntry, Direction, GraphExecutor, Lsn, RowId, TxId, Value};
use contextdb_graph::{GraphQuery, GraphStore, MemGraphExecutor};
use contextdb_tx::{OwnedAdjEntry, RowEdgeOwner, TransactionManager, WriteSet, WriteSetApplicator};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

struct Triad {
    source: Uuid,
    target: Uuid,
}

fn triad() -> Triad {
    Triad {
        source: Uuid::from_u128(1),
        target: Uuid::from_u128(2),
    }
}

fn owner(row_id: u64, created_tx: u64) -> RowEdgeOwner {
    RowEdgeOwner {
        table: "edges".to_string(),
        table_generation: 1,
        row_id: RowId(row_id),
        created_tx: TxId(created_tx),
        lsn: Lsn(created_tx),
    }
}

fn owned(triad: &Triad, owner: RowEdgeOwner) -> OwnedAdjEntry {
    OwnedAdjEntry {
        entry: AdjEntry {
            source: triad.source,
            target: triad.target,
            edge_type: "CITES".to_string(),
            properties: HashMap::from([(
                "owner".to_string(),
                Value::Text(owner.row_id.0.to_string()),
            )]),
            created_tx: owner.created_tx,
            deleted_tx: None,
            lsn: owner.lsn,
        },
        owner,
    }
}

fn standalone(triad: &Triad, created_tx: u64) -> AdjEntry {
    AdjEntry {
        source: triad.source,
        target: triad.target,
        edge_type: "CITES".to_string(),
        properties: HashMap::from([("standalone".to_string(), Value::Bool(true))]),
        created_tx: TxId(created_tx),
        deleted_tx: None,
        lsn: Lsn(created_tx),
    }
}

/// (owner row id or none, created, deleted) for every copy in one direction,
/// in stored order.
fn forward(store: &GraphStore, triad: &Triad) -> Vec<(Option<u64>, u64, Option<u64>)> {
    store
        .forward_adj
        .read()
        .get(&triad.source)
        .map(|entries| entries.as_slice())
        .unwrap_or(&[])
        .iter()
        .map(|entry| {
            (
                entry.row_owner.as_ref().map(|owner| owner.row_id.0),
                entry.created_tx.0,
                entry.deleted_tx.map(|tx| tx.0),
            )
        })
        .collect()
}

fn reverse(store: &GraphStore, triad: &Triad) -> Vec<(Option<u64>, u64, Option<u64>)> {
    store
        .reverse_adj
        .read()
        .get(&triad.target)
        .map(|entries| entries.as_slice())
        .unwrap_or(&[])
        .iter()
        .map(|entry| {
            (
                entry.row_owner.as_ref().map(|owner| owner.row_id.0),
                entry.created_tx.0,
                entry.deleted_tx.map(|tx| tx.0),
            )
        })
        .collect()
}

fn sorted(
    mut copies: Vec<(Option<u64>, u64, Option<u64>)>,
) -> Vec<(Option<u64>, u64, Option<u64>)> {
    copies.sort();
    copies
}

#[test]
fn two_owners_of_one_triad_are_two_copies_in_each_direction() {
    let triad = triad();
    let store = GraphStore::new();
    store.apply_row_inserts_ref(&[owned(&triad, owner(1, 5)), owned(&triad, owner(2, 5))]);
    let expected = vec![(Some(1), 5, None), (Some(2), 5, None)];
    assert_eq!(sorted(forward(&store, &triad)), expected);
    assert_eq!(sorted(reverse(&store, &triad)), expected);
}

#[test]
fn deleting_one_owner_ends_exactly_that_copy_in_both_directions() {
    let triad = triad();
    let store = GraphStore::new();
    let first = owned(&triad, owner(1, 5));
    store.apply_row_inserts_ref(&[first.clone(), owned(&triad, owner(2, 5))]);
    store.apply_row_deletes_ref(&[(first, TxId(20))]);
    let expected = vec![(Some(1), 5, Some(20)), (Some(2), 5, None)];
    assert_eq!(sorted(forward(&store, &triad)), expected);
    assert_eq!(sorted(reverse(&store, &triad)), expected);
}

#[test]
fn a_standalone_edge_and_a_row_owned_edge_with_one_triad_do_not_share_a_deletion() {
    let triad = triad();
    let store = GraphStore::new();
    let row = owned(&triad, owner(1, 5));
    store.apply_inserts_ref(&[standalone(&triad, 4)]);
    store.apply_row_inserts_ref(std::slice::from_ref(&row));
    assert_eq!(
        sorted(forward(&store, &triad)),
        vec![(None, 4, None), (Some(1), 5, None)],
        "both exist side by side"
    );
    // A row-owned deletion ends the row's copy and leaves the standalone one.
    store.apply_row_deletes_ref(&[(row, TxId(20))]);
    assert_eq!(
        sorted(forward(&store, &triad)),
        vec![(None, 4, None), (Some(1), 5, Some(20))]
    );
    // A standalone deletion ends the standalone copy and not a live row's.
    let other = owned(&triad, owner(2, 6));
    store.apply_row_inserts_ref(&[other]);
    store.apply_deletes(vec![(
        triad.source,
        "CITES".to_string(),
        triad.target,
        TxId(30),
    )]);
    assert_eq!(
        sorted(forward(&store, &triad)),
        vec![
            (None, 4, Some(30)),
            (Some(1), 5, Some(20)),
            (Some(2), 6, None)
        ],
        "the standalone deletion reaches only the standalone copy"
    );
    assert_eq!(
        sorted(reverse(&store, &triad)),
        sorted(forward(&store, &triad))
    );
}

#[test]
fn each_owner_keeps_its_own_visibility_interval() {
    let triad = triad();
    let store = GraphStore::new();
    let earlier = owned(&triad, owner(1, 5));
    store.apply_row_inserts_ref(std::slice::from_ref(&earlier));
    store.apply_row_deletes_ref(&[(earlier, TxId(7))]);
    store.apply_row_inserts_ref(&[owned(&triad, owner(2, 9))]);
    assert_eq!(
        sorted(forward(&store, &triad)),
        vec![(Some(1), 5, Some(7)), (Some(2), 9, None)],
        "the later sibling does not acquire the earlier one's interval"
    );
}

#[test]
fn an_exact_row_lookup_visits_only_that_owners_copy() {
    let triad = triad();
    let store = GraphStore::new();
    let first = owned(&triad, owner(1, 5));
    let second = owned(&triad, owner(2, 5));
    store.apply_inserts_ref(&[standalone(&triad, 4)]);
    store.apply_row_inserts_ref(&[first.clone(), second.clone()]);
    for wanted in [&first, &second] {
        let mut visited = Vec::new();
        store
            .visit_row_edges(
                None,
                GraphQuery {
                    node: triad.source,
                    direction: Direction::Outgoing,
                    identity: Some((triad.target, "CITES")),
                },
                &wanted.owner,
                |entry| {
                    visited.push(entry.clone());
                    Ok(true)
                },
            )
            .unwrap();
        assert_eq!(
            visited,
            vec![wanted.entry.clone()],
            "exactly {:?}",
            wanted.owner.row_id
        );
    }
    let unknown = owner(9, 5);
    let mut visited = 0;
    store
        .visit_row_edges(
            None,
            GraphQuery {
                node: triad.source,
                direction: Direction::Outgoing,
                identity: Some((triad.target, "CITES")),
            },
            &unknown,
            |_| {
                visited += 1;
                Ok(true)
            },
        )
        .unwrap();
    assert_eq!(visited, 0, "an owner with no copy visits nothing");
}

#[test]
fn a_full_edge_visit_reports_the_owner_of_every_copy() {
    let triad = triad();
    let store = GraphStore::new();
    store.apply_inserts_ref(&[standalone(&triad, 4)]);
    store.apply_row_inserts_ref(&[owned(&triad, owner(1, 5)), owned(&triad, owner(2, 5))]);
    let mut owners = Vec::new();
    store
        .visit_edges_with_owner(
            None,
            GraphQuery {
                node: triad.source,
                direction: Direction::Outgoing,
                identity: None,
            },
            |_, owner| {
                owners.push(owner.map(|owner| owner.row_id.0));
                Ok(true)
            },
        )
        .unwrap();
    owners.sort();
    assert_eq!(owners, vec![None, Some(1), Some(2)]);
}

struct GraphApplier {
    graph: Arc<GraphStore>,
}

impl WriteSetApplicator for GraphApplier {
    fn apply(
        &self,
        ws: &WriteSet,
        _read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> contextdb_core::Result<()> {
        self.graph.apply_inserts_ref(&ws.adj_inserts);
        self.graph.apply_deletes(ws.adj_deletes.clone());
        self.graph.apply_row_inserts_ref(&ws.row_adj_inserts);
        self.graph.apply_row_deletes_ref(&ws.row_adj_deletes);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        RowId(1)
    }
}

fn reached(
    exec: &MemGraphExecutor<GraphApplier>,
    start: Uuid,
    snapshot: contextdb_core::SnapshotId,
) -> usize {
    exec.bfs(
        start,
        None,
        Direction::Outgoing,
        1,
        3,
        snapshot,
        Arc::new(test_read_memory::TestReadMemory::default()),
    )
    .unwrap()
    .nodes
    .len()
}

#[test]
fn a_traversal_reaches_a_target_once_however_many_owners_lead_to_it() {
    let triad = triad();
    let graph = Arc::new(GraphStore::new());
    let txm = Arc::new(TransactionManager::new(GraphApplier {
        graph: graph.clone(),
    }));
    let exec = MemGraphExecutor::new(graph.clone(), txm.clone());
    let tx = txm.begin();
    txm.with_write_set(tx, |ws| {
        ws.row_adj_inserts.push(owned(&triad, owner(1, tx.0)));
        ws.row_adj_inserts.push(owned(&triad, owner(2, tx.0)));
    })
    .unwrap();
    txm.commit(tx, Arc::new(test_read_memory::TestReadMemory::default()))
        .unwrap();
    let both_live = txm.snapshot();
    assert_eq!(
        reached(&exec, triad.source, both_live),
        1,
        "once, not twice"
    );

    // Delete one owner by the owner the store holds; the other still leads there.
    let stored_first = graph.forward_adj.read()[&triad.source]
        .iter()
        .find(|entry| entry.row_owner.as_ref().map(|o| o.row_id) == Some(RowId(1)))
        .cloned()
        .unwrap();
    let delete = txm.begin();
    txm.with_write_set(delete, |ws| {
        ws.row_adj_deletes.push((
            OwnedAdjEntry {
                owner: stored_first.row_owner.clone().unwrap(),
                entry: stored_first.entry.clone(),
            },
            delete,
        ));
    })
    .unwrap();
    txm.commit(
        delete,
        Arc::new(test_read_memory::TestReadMemory::default()),
    )
    .unwrap();
    assert_eq!(
        reached(&exec, triad.source, txm.snapshot()),
        1,
        "one surviving owner keeps the edge"
    );
    assert_eq!(
        reached(&exec, triad.source, both_live),
        1,
        "the older snapshot is unchanged"
    );

    let stored_second = graph.forward_adj.read()[&triad.source]
        .iter()
        .find(|entry| entry.row_owner.as_ref().map(|o| o.row_id) == Some(RowId(2)))
        .cloned()
        .unwrap();
    let last = txm.begin();
    txm.with_write_set(last, |ws| {
        ws.row_adj_deletes.push((
            OwnedAdjEntry {
                owner: stored_second.row_owner.clone().unwrap(),
                entry: stored_second.entry.clone(),
            },
            last,
        ));
    })
    .unwrap();
    txm.commit(last, Arc::new(test_read_memory::TestReadMemory::default()))
        .unwrap();
    assert_eq!(
        reached(&exec, triad.source, txm.snapshot()),
        0,
        "no owner left, no edge"
    );
    assert_eq!(reached(&exec, triad.source, both_live), 1);
}

// These are rows of the owned-edge traversal fixture above. The old callback
// has no owner parameter and no physical binding selector, so runtime RED is
// established by the named owner-masking and coalescing mutants in the receipt.
#[derive(Clone, Copy)]
enum TraversalRoute {
    Memory,
    Source,
    Staged,
}

struct WitnessSnapshot {
    entries: Arc<Vec<contextdb_graph::GraphAdjEntry>>,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

struct WitnessCursor {
    entries: Arc<Vec<contextdb_graph::GraphAdjEntry>>,
    position: usize,
    memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    reads: Arc<std::sync::atomic::AtomicUsize>,
}

struct WitnessWindow(contextdb_graph::GraphAdjEntry);
impl contextdb_graph::GraphEdgeWindow for WitnessWindow {
    fn edge(&self) -> &AdjEntry {
        &self.0.entry
    }
    fn row_owner(&self) -> Option<&RowEdgeOwner> {
        self.0.row_owner.as_ref()
    }
}

impl contextdb_graph::GraphSnapshot for WitnessSnapshot {
    fn cursor(
        &self,
        query: GraphQuery<'_>,
    ) -> contextdb_core::Result<contextdb_graph::source::GraphCursorOwner> {
        use contextdb_core::read_memory::ReadCredit;
        let first = &self.entries[0];
        assert!(query.identity.is_none());
        let matches = match query.direction {
            Direction::Outgoing => query.node == first.source,
            Direction::Incoming => query.node == first.target,
            Direction::Both => {
                panic!("the production traversal splits Both into directional cursors")
            }
        };
        let mut credit = ReadCredit::new(self.memory.clone());
        credit.try_grow_for(
            std::mem::size_of::<WitnessCursor>(),
            "reader",
            "fixture_cursor",
            "test fixture",
        )?;
        Ok(contextdb_graph::source::GraphCursorOwner::new(
            Box::new(WitnessCursor {
                entries: self.entries.clone(),
                position: if matches { 0 } else { self.entries.len() },
                memory: self.memory.clone(),
                reads: self.reads.clone(),
            }),
            credit,
        ))
    }
}

impl contextdb_graph::GraphCursor for WitnessCursor {
    fn next_edge(
        &mut self,
    ) -> contextdb_core::Result<
        Option<contextdb_core::read_memory::OwnedReadBox<dyn contextdb_graph::GraphEdgeWindow>>,
    > {
        use contextdb_core::read_memory::{OwnedReadBox, ReadCredit};
        let Some(entry) = self.entries.get(self.position) else {
            return Ok(None);
        };
        let mut credit = ReadCredit::new(self.memory.clone());
        credit.try_grow_for(
            entry
                .estimated_bytes()
                .saturating_add(std::mem::size_of::<WitnessWindow>()),
            "reader",
            "fixture_window",
            "test fixture",
        )?;
        let window = WitnessWindow(entry.clone());
        self.position += 1;
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Some(OwnedReadBox::new(Box::new(window), credit)))
    }
}

fn assert_owned_traversal_row(route: TraversalRoute) {
    use contextdb_core::read_memory::ReadCredit;
    use contextdb_core::{Error, SnapshotId};
    use contextdb_graph::mem::StagedTraversalEdge;
    use std::cell::{Cell, RefCell};
    use std::sync::atomic::{AtomicUsize, Ordering};

    for direction in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
        for self_loop in [false, true] {
            for physical in [false, true] {
                for deny_first in [false, true] {
                    for include_standalone in [false, true] {
                        let mut triad = triad();
                        if self_loop {
                            triad.target = triad.source;
                        }
                        let first = owned(&triad, owner(1, 5));
                        let second = owned(&triad, owner(2, 5));
                        assert_eq!(first.owner.created_tx, second.owner.created_tx);
                        assert_eq!(first.owner.lsn, second.owner.lsn);
                        let mut entries = Vec::new();
                        if include_standalone {
                            entries
                                .push(contextdb_graph::GraphAdjEntry::from(standalone(&triad, 4)));
                        }
                        entries.push(first.clone().into());
                        entries.push(second.clone().into());
                        let expected_owners: Vec<_> = entries
                            .iter()
                            .map(|entry| entry.row_owner.clone())
                            .collect();
                        let graph = Arc::new(GraphStore::new());
                        if matches!(route, TraversalRoute::Memory) {
                            if include_standalone {
                                graph.apply_inserts_ref(&[standalone(&triad, 4)]);
                            }
                            graph.apply_row_inserts_ref(&[first, second]);
                        }
                        let txm = Arc::new(TransactionManager::new(GraphApplier {
                            graph: graph.clone(),
                        }));
                        let exec = MemGraphExecutor::new(graph, txm);
                        let memory = Arc::new(test_read_memory::TestReadMemory::default());
                        let credit = RefCell::new(ReadCredit::new(memory.clone()));
                        let reads = Arc::new(AtomicUsize::new(0));
                        let entries = Arc::new(entries);
                        let source: Option<Arc<dyn contextdb_graph::GraphSnapshot>> =
                            if matches!(route, TraversalRoute::Source) {
                                Some(Arc::new(WitnessSnapshot {
                                    entries: entries.clone(),
                                    memory: memory.clone(),
                                    reads: reads.clone(),
                                }))
                            } else {
                                None
                            };
                        let start = if matches!(direction, Direction::Incoming) {
                            triad.target
                        } else {
                            triad.source
                        };
                        let target = if matches!(direction, Direction::Incoming) {
                            triad.source
                        } else {
                            triad.target
                        };
                        let mut cursor = exec
                            .bounded_bfs_cursor_at_source::<Error>(
                                start,
                                None,
                                direction,
                                1,
                                1,
                                SnapshotId(10),
                                source,
                                |bytes| {
                                    credit.borrow_mut().try_grow_for(
                                        bytes,
                                        "reader",
                                        "fixture_traversal",
                                        "test fixture",
                                    )
                                },
                                |bytes| credit.borrow_mut().try_shrink(bytes).unwrap(),
                            )
                            .unwrap();
                        if physical {
                            cursor.preserve_single_hop_bindings().unwrap();
                        }
                        let mut seen = Vec::new();
                        let mut outputs = Vec::new();
                        let staged_reads = Cell::new(0);
                        let examined = Cell::new(0);
                        loop {
                            let next = exec
                                .bounded_bfs_next_including_staged_with_owner::<Error>(
                                    &mut cursor,
                                    || Ok(()),
                                    || Ok(()),
                                    |bytes| {
                                        credit.borrow_mut().try_grow_for(
                                            bytes,
                                            "reader",
                                            "fixture_traversal",
                                            "test fixture",
                                        )
                                    },
                                    |bytes| credit.borrow_mut().try_shrink(bytes).unwrap(),
                                    |neighbor, source, endpoint, edge_type, owner| {
                                        assert_eq!(neighbor, target);
                                        assert_eq!(
                                            (source, endpoint, edge_type),
                                            (triad.source, triad.target, "CITES")
                                        );
                                        seen.push(owner.cloned());
                                        Ok(!deny_first
                                            || owner.is_none_or(|owner| owner.row_id != RowId(1)))
                                    },
                                    |node, part, position| {
                                        if !matches!(route, TraversalRoute::Staged) {
                                            return Ok(None);
                                        }
                                        let matches = match part {
                                            Direction::Outgoing => node == triad.source,
                                            Direction::Incoming => node == triad.target,
                                            Direction::Both => unreachable!(),
                                        };
                                        let Some(entry) = entries.get(position).filter(|_| matches)
                                        else {
                                            return Ok(None);
                                        };
                                        let bytes = std::mem::size_of::<StagedTraversalEdge>()
                                            + entry.edge_type.len()
                                            + entry
                                                .row_owner
                                                .as_ref()
                                                .map_or(0, |owner| owner.table.len());
                                        credit.borrow_mut().try_grow_for(
                                            bytes,
                                            "reader",
                                            "fixture_staged",
                                            "test fixture",
                                        )?;
                                        let staged = StagedTraversalEdge {
                                            source: entry.source,
                                            target: entry.target,
                                            edge_type: entry.edge_type.clone(),
                                            row_owner: entry.row_owner.clone(),
                                        };
                                        staged_reads.set(staged_reads.get() + 1);
                                        Ok(Some((staged, bytes, position + 1)))
                                    },
                                    || examined.set(examined.get() + 1),
                                    None,
                                )
                                .unwrap();
                            let Some(output) = next else {
                                break;
                            };
                            assert_eq!(output.node.depth, 1);
                            outputs.push(output.node.id);
                            let bytes = output.reserved_bytes;
                            drop(output);
                            credit.borrow_mut().try_shrink(bytes).unwrap();
                        }
                        seen.sort();
                        let mut expected_owners = expected_owners;
                        expected_owners.sort();
                        assert_eq!(
                            seen, expected_owners,
                            "authorization receives every complete owner before coalescing, including standalone None"
                        );
                        let permitted = expected_owners
                            .iter()
                            .filter(|owner| {
                                !deny_first
                                    || owner.as_ref().is_none_or(|owner| owner.row_id != RowId(1))
                            })
                            .count();
                        let expected_count = if physical {
                            permitted
                        } else {
                            usize::from(permitted != 0)
                        };
                        assert_eq!(
                            outputs,
                            vec![target; expected_count],
                            "physical single-hop keeps bindings; reachability coalesces"
                        );
                        assert_eq!(
                            examined.get(),
                            permitted,
                            "every allowed physical witness is examined before coalescing"
                        );
                        let physical_reads = entries.len()
                            * if self_loop && matches!(direction, Direction::Both) {
                                2
                            } else {
                                1
                            };
                        if matches!(route, TraversalRoute::Source) {
                            assert_eq!(reads.load(Ordering::SeqCst), physical_reads);
                        }
                        if matches!(route, TraversalRoute::Staged) {
                            assert_eq!(staged_reads.get(), physical_reads);
                        }
                        credit
                            .borrow_mut()
                            .try_shrink(cursor.into_retained_bytes())
                            .unwrap();
                        assert_eq!(
                            credit.borrow().bytes(),
                            0,
                            "all continuation, output and staged charges settle"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn memory_owner_authorization_precedes_logical_or_physical_single_hop_binding() {
    assert_owned_traversal_row(TraversalRoute::Memory);
}

#[test]
fn source_owner_authorization_precedes_logical_or_physical_single_hop_binding() {
    assert_owned_traversal_row(TraversalRoute::Source);
}

#[test]
fn staged_owner_authorization_precedes_logical_or_physical_single_hop_binding() {
    assert_owned_traversal_row(TraversalRoute::Staged);
}

#[test]
fn committed_owner_entrypoint_and_legacy_callback_keep_the_selected_binding_mode() {
    use contextdb_core::{Error, SnapshotId};
    let triad = triad();
    for owner_aware in [false, true] {
        for physical in [false, true] {
            let graph = Arc::new(GraphStore::new());
            graph.apply_row_inserts_ref(&[owned(&triad, owner(1, 5)), owned(&triad, owner(2, 5))]);
            let txm = Arc::new(TransactionManager::new(GraphApplier {
                graph: graph.clone(),
            }));
            let exec = MemGraphExecutor::new(graph, txm);
            let mut cursor = exec
                .bounded_bfs_cursor::<Error>(
                    triad.source,
                    None,
                    Direction::Outgoing,
                    1,
                    1,
                    SnapshotId(10),
                    Arc::new(test_read_memory::TestReadMemory::default()),
                    |_| Ok(()),
                    |_| {},
                )
                .unwrap();
            if physical {
                cursor.preserve_single_hop_bindings().unwrap();
            }
            let mut seen = Vec::new();
            let mut outputs = Vec::new();
            loop {
                let output = if owner_aware {
                    exec.bounded_bfs_next_with_owner::<Error>(
                        &mut cursor,
                        || Ok(()),
                        || Ok(()),
                        |_| Ok(()),
                        |_| {},
                        |_, _, _, _, owner| {
                            seen.push(owner.cloned().expect("a committed row-owned witness"));
                            Ok(true)
                        },
                    )
                } else {
                    exec.bounded_bfs_next::<Error>(
                        &mut cursor,
                        || Ok(()),
                        || Ok(()),
                        |_| Ok(()),
                        |_| {},
                        |_, _, _, _| Ok(true),
                    )
                }
                .unwrap();
                let Some(output) = output else {
                    break;
                };
                outputs.push(output.node.id);
                assert!(
                    cursor.preserve_single_hop_bindings().is_err(),
                    "a mode change after the first pull refuses without changing the mode"
                );
            }
            assert_eq!(outputs, vec![triad.target; if physical { 2 } else { 1 }]);
            if owner_aware {
                seen.sort();
                assert_eq!(seen, vec![owner(1, 5), owner(2, 5)]);
            }
        }
    }
}

struct WitnessSource {
    entries: Arc<Vec<contextdb_graph::GraphAdjEntry>>,
}
impl contextdb_graph::GraphReadSource for WitnessSource {
    fn capture(
        &self,
        _snapshot: contextdb_core::SnapshotId,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> contextdb_core::Result<Arc<dyn contextdb_graph::GraphSnapshot>> {
        Ok(Arc::new(WitnessSnapshot {
            entries: self.entries.clone(),
            memory,
            reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }))
    }
}

// Policy 32 logical adjacency uses a deterministic visible representative;
// unlike SQL single-hop bindings it does not multiply a triad by its owners.
#[test]
fn logical_neighbors_use_the_first_visible_canonical_witness() {
    use contextdb_core::SnapshotId;
    for source_backed in [false, true] {
        for direction in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
            for self_loop in [false, true] {
                let mut triad = triad();
                if self_loop {
                    triad.target = triad.source;
                }
                let mut raw = standalone(&triad, 4);
                raw.deleted_tx = Some(TxId(6));
                let mut first = owned(&triad, owner(1, 5));
                first.entry.deleted_tx = Some(TxId(7));
                let second = owned(&triad, owner(2, 5));
                let entries = vec![
                    contextdb_graph::GraphAdjEntry::from(raw),
                    first.into(),
                    second.into(),
                ];
                let graph = if source_backed {
                    Arc::new(GraphStore::with_source(Arc::new(WitnessSource {
                        entries: Arc::new(entries.clone()),
                    })))
                } else {
                    let graph = Arc::new(GraphStore::new());
                    graph.publish_prepared_received_schema(
                        GraphStore::prepare_received_schema_publication(entries.clone()),
                    );
                    graph
                };
                let txm = Arc::new(TransactionManager::new(GraphApplier {
                    graph: graph.clone(),
                }));
                let exec = MemGraphExecutor::new(graph, txm);
                let start = if matches!(direction, Direction::Incoming) {
                    triad.target
                } else {
                    triad.source
                };
                let target = if matches!(direction, Direction::Incoming) {
                    triad.source
                } else {
                    triad.target
                };
                for snapshot in [3, 4, 5, 6, 7] {
                    let expected: Vec<_> = entries
                        .iter()
                        .find(|entry| entry.visible_at(SnapshotId(snapshot)))
                        .map(|entry| (target, entry.edge_type.clone(), entry.properties.clone()))
                        .into_iter()
                        .collect();
                    let actual = exec
                        .neighbors(
                            start,
                            None,
                            direction,
                            SnapshotId(snapshot),
                            Arc::new(test_read_memory::TestReadMemory::default()),
                        )
                        .unwrap();
                    assert_eq!(
                        actual, expected,
                        "one visible triad, standalone-first properties, then first visible row owner; direction={direction:?}, self_loop={self_loop}, snapshot={snapshot}"
                    );
                }
            }
        }
    }
}

// Received-schema publication selects row artifacts by their owning table,
// even when another table and a standalone edge use exactly the same triad.
// The additive API has no old-source call seam; the decisive mutant routes it
// through the existing type-only API and is recorded beside the RED receipts.
#[test]
fn received_table_publication_preserves_other_tables_and_empty_images_drop_only_selected_owners() {
    use contextdb_graph::GraphAdjEntry;
    use std::collections::HashSet;
    let triad = triad();
    let mut selected = owned(&triad, owner(1, 5));
    selected.owner.table = "selected".to_string();
    let mut kept = owned(&triad, owner(1, 5));
    kept.owner.table = "kept".to_string();
    let mut selected_other = owned(&triad, owner(2, 5));
    selected_other.owner.table = "selected".to_string();
    selected_other.entry.edge_type = "OTHER".to_string();
    let mut raw_other = standalone(&triad, 4);
    raw_other.edge_type = "OTHER".to_string();
    let seed = vec![
        GraphAdjEntry::from(standalone(&triad, 4)),
        selected.into(),
        kept.clone().into(),
        selected_other.into(),
        raw_other.clone().into(),
    ];
    let store = GraphStore::new();
    store.publish_prepared_received_schema(GraphStore::prepare_received_schema_publication(
        seed.clone(),
    ));
    let kept_address = |store: &GraphStore| {
        let forward = store.forward_adj.read();
        forward[&triad.source]
            .iter()
            .find(|entry| {
                entry
                    .row_owner
                    .as_ref()
                    .is_some_and(|owner| owner.table == "kept")
            })
            .unwrap()
            .row_owner
            .as_ref()
            .unwrap()
            .table
            .as_ptr() as usize
    };
    let before_address = kept_address(&store);
    let affected_types = HashSet::from(["CITES".to_string()]);
    let affected_tables = HashSet::from(["selected".to_string()]);
    let mut replacement = owned(&triad, owner(1, 6));
    replacement.owner.table = "selected".to_string();
    replacement.owner.table_generation = 2;
    let raw_replacement = standalone(&triad, 6);
    let image = GraphStore::prepare_received_schema_publication(vec![
        raw_replacement.clone().into(),
        replacement.clone().into(),
    ]);
    store.publish_prepared_received_schema_tables(image, &affected_types, &affected_tables);
    let control = GraphStore::new();
    control.publish_prepared_received_schema(GraphStore::prepare_received_schema_publication(
        vec![
            raw_replacement.into(),
            replacement.into(),
            kept.clone().into(),
            raw_other.clone().into(),
        ],
    ));
    assert_eq!(*store.forward_adj.read(), *control.forward_adj.read());
    assert_eq!(*store.reverse_adj.read(), *control.reverse_adj.read());
    assert_eq!(
        kept_address(&store),
        before_address,
        "unselected table history is retained in place, not copied"
    );

    let empty = GraphStore::prepare_received_schema_publication(Vec::new());
    store.publish_prepared_received_schema_tables(empty, &affected_types, &affected_tables);
    let control = GraphStore::new();
    control.publish_prepared_received_schema(GraphStore::prepare_received_schema_publication(
        vec![kept.into(), raw_other.into()],
    ));
    assert_eq!(
        *store.forward_adj.read(),
        *control.forward_adj.read(),
        "an empty selected image removes the dropped table but preserves a sibling table using the type"
    );
    assert_eq!(*store.reverse_adj.read(), *control.reverse_adj.read());
    assert_eq!(kept_address(&store), before_address);

    // The old type-only API retains its prior selection semantics. Engine
    // received-table publication uses the new table-aware entrypoint above.
    let legacy = GraphStore::new();
    legacy.publish_prepared_received_schema(GraphStore::prepare_received_schema_publication(seed));
    legacy.publish_prepared_received_edge_types(
        GraphStore::prepare_received_schema_publication(Vec::new()),
        &affected_types,
    );
    assert!(
        legacy.forward_adj.read()[&triad.source]
            .iter()
            .all(|entry| entry.edge_type != "CITES")
    );
    assert_eq!(legacy.forward_adj.read()[&triad.source].len(), 2);
}

fn encoded_owner_key(owner: &RowEdgeOwner) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(&u32::try_from(owner.table.len()).unwrap().to_be_bytes());
    key.extend_from_slice(owner.table.as_bytes());
    for field in [
        owner.table_generation,
        owner.row_id.0,
        owner.created_tx.0,
        owner.lsn.0,
    ] {
        key.extend_from_slice(&field.to_be_bytes());
    }
    key
}

// The private durable key is the ordering authority. A lexical table-name
// comparator puts aa before z and selects different neighbor properties.
#[test]
fn different_length_owner_names_have_one_durable_order_in_memory_and_source() {
    use contextdb_core::SnapshotId;
    let triad = triad();
    let mut owners = vec![
        owner(1, 5),
        owner(2, 5),
        owner(1, 5),
        owner(1, 6),
        owner(1, 5),
        owner(1, 5),
    ];
    owners[0].table = "aa".to_string();
    for owner in &mut owners[1..] {
        owner.table = "z".to_string();
    }
    owners[2].table_generation = 2;
    owners[4].lsn = Lsn(7);
    let mut entries: Vec<contextdb_graph::GraphAdjEntry> = owners
        .into_iter()
        .map(|owner| {
            let table = owner.table.clone();
            let mut edge = owned(&triad, owner);
            edge.entry
                .properties
                .insert("table".to_string(), Value::Text(table));
            edge.into()
        })
        .collect();
    entries.sort_by_cached_key(|entry| encoded_owner_key(entry.row_owner.as_ref().unwrap()));
    let expected_owners: Vec<_> = entries
        .iter()
        .map(|entry| entry.row_owner.clone())
        .collect();
    assert_eq!(expected_owners[0].as_ref().unwrap().table, "z");
    assert_eq!(
        expected_owners.last().unwrap().as_ref().unwrap().table,
        "aa"
    );
    for source_backed in [false, true] {
        let graph = if source_backed {
            Arc::new(GraphStore::with_source(Arc::new(WitnessSource {
                entries: Arc::new(entries.clone()),
            })))
        } else {
            let graph = Arc::new(GraphStore::new());
            graph.publish_prepared_received_schema(
                GraphStore::prepare_received_schema_publication(entries.clone()),
            );
            graph
        };
        let memory = Arc::new(test_read_memory::TestReadMemory::default());
        let root = graph
            .capture_source(SnapshotId(10), memory.clone())
            .unwrap();
        let mut observed = Vec::new();
        graph
            .visit_edges_with_owner(
                root.as_ref(),
                GraphQuery {
                    node: triad.source,
                    direction: Direction::Outgoing,
                    identity: None,
                },
                |_, owner| {
                    observed.push(owner.cloned());
                    Ok(true)
                },
            )
            .unwrap();
        assert_eq!(
            observed, expected_owners,
            "memory and source windows use the exact length-prefixed durable key order"
        );
        let txm = Arc::new(TransactionManager::new(GraphApplier {
            graph: graph.clone(),
        }));
        let exec = MemGraphExecutor::new(graph, txm);
        let actual = exec
            .neighbors(
                triad.source,
                None,
                Direction::Outgoing,
                SnapshotId(10),
                memory,
            )
            .unwrap();
        assert_eq!(
            actual,
            vec![(
                triad.target,
                "CITES".to_string(),
                entries[0].properties.clone()
            )],
            "the canonical representative and properties agree across routes"
        );
    }
}

// Committed and staged witnesses must be merged before last-triad coalescing;
// separately sorted A,B then A,B must not report the two triads twice.
#[test]
fn mixed_committed_and_staged_streams_coalesce_only_after_canonical_merge() {
    use contextdb_core::read_memory::ReadCredit;
    use contextdb_core::{Error, SnapshotId};
    use contextdb_graph::{GraphAdjEntry, mem::StagedTraversalEdge};
    use std::cell::{Cell, RefCell};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for source_backed in [false, true] {
        for direction in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
            for self_loop in [false, true] {
                for physical in [false, true] {
                    for deny_committed in [false, true] {
                        let mut triad = triad();
                        if self_loop {
                            triad.target = triad.source;
                        }
                        let mut committed = Vec::<GraphAdjEntry>::new();
                        let mut staged = Vec::<GraphAdjEntry>::new();
                        for name in ["A", "B"] {
                            let mut first = owned(&triad, owner(1, 5));
                            first.entry.edge_type = name.into();
                            let mut second = owned(&triad, owner(2, 5));
                            second.entry.edge_type = name.into();
                            let mut raw = standalone(&triad, 4);
                            raw.edge_type = name.into();
                            if name == "A" {
                                committed.push(raw.into());
                            } else {
                                staged.push(raw.into());
                            }
                            if name == "A" {
                                // An earlier owner in key order is invisible to
                                // this snapshot and cannot suppress visible owners.
                                let mut hidden = owned(&triad, owner(0, 20));
                                hidden.entry.edge_type = name.into();
                                committed.push(hidden.into());
                            }
                            committed.push(first.into());
                            staged.push(second.into());
                        }
                        let graph = Arc::new(GraphStore::new());
                        if !source_backed {
                            graph.publish_prepared_received_schema(
                                GraphStore::prepare_received_schema_publication(committed.clone()),
                            );
                        }
                        let txm = Arc::new(TransactionManager::new(GraphApplier {
                            graph: graph.clone(),
                        }));
                        let exec = MemGraphExecutor::new(graph, txm);
                        let memory = Arc::new(test_read_memory::TestReadMemory::default());
                        let credit = RefCell::new(ReadCredit::new(memory.clone()));
                        let reads = Arc::new(AtomicUsize::new(0));
                        let source: Option<Arc<dyn contextdb_graph::GraphSnapshot>> = source_backed
                            .then(|| {
                                Arc::new(WitnessSnapshot {
                                    entries: Arc::new(committed),
                                    memory,
                                    reads: reads.clone(),
                                })
                                    as Arc<dyn contextdb_graph::GraphSnapshot>
                            });
                        let start = if matches!(direction, Direction::Incoming) {
                            triad.target
                        } else {
                            triad.source
                        };
                        let target = if matches!(direction, Direction::Incoming) {
                            triad.source
                        } else {
                            triad.target
                        };
                        let mut cursor = exec
                            .bounded_bfs_cursor_at_source::<Error>(
                                start,
                                None,
                                direction,
                                1,
                                1,
                                SnapshotId(10),
                                source,
                                |bytes| {
                                    credit.borrow_mut().try_grow_for(
                                        bytes,
                                        "reader",
                                        "mixed_cursor",
                                        "fixture",
                                    )
                                },
                                |bytes| credit.borrow_mut().try_shrink(bytes).unwrap(),
                            )
                            .unwrap();
                        if physical {
                            cursor.preserve_single_hop_bindings().unwrap();
                        }
                        let staged_reads = Cell::new(0);
                        let mut seen = Vec::new();
                        let mut outputs = Vec::new();
                        loop {
                            let output = exec
                                .bounded_bfs_next_including_staged_with_owner::<Error>(
                                    &mut cursor,
                                    || Ok(()),
                                    || Ok(()),
                                    |bytes| {
                                        credit.borrow_mut().try_grow_for(
                                            bytes,
                                            "reader",
                                            "mixed_cursor",
                                            "fixture",
                                        )
                                    },
                                    |bytes| credit.borrow_mut().try_shrink(bytes).unwrap(),
                                    |neighbor, _, _, name, owner| {
                                        assert_eq!(neighbor, target);
                                        seen.push((name.to_string(), owner.cloned()));
                                        Ok(!deny_committed
                                            || owner.is_none_or(|owner| owner.row_id != RowId(1)))
                                    },
                                    |node, part, position| {
                                        let matches = match part {
                                            Direction::Outgoing => node == triad.source,
                                            Direction::Incoming => node == triad.target,
                                            Direction::Both => unreachable!(),
                                        };
                                        let Some(entry) = staged.get(position).filter(|_| matches)
                                        else {
                                            return Ok(None);
                                        };
                                        let bytes = std::mem::size_of::<StagedTraversalEdge>()
                                            + entry.edge_type.len()
                                            + entry
                                                .row_owner
                                                .as_ref()
                                                .map_or(0, |owner| owner.table.len());
                                        credit.borrow_mut().try_grow_for(
                                            bytes,
                                            "reader",
                                            "mixed_staged",
                                            "fixture",
                                        )?;
                                        staged_reads.set(staged_reads.get() + 1);
                                        Ok(Some((
                                            StagedTraversalEdge {
                                                source: entry.source,
                                                target: entry.target,
                                                edge_type: entry.edge_type.clone(),
                                                row_owner: entry.row_owner.clone(),
                                            },
                                            bytes,
                                            position + 1,
                                        )))
                                    },
                                    || {},
                                    None,
                                )
                                .unwrap();
                            let Some(output) = output else {
                                break;
                            };
                            outputs.push(output.node.id);
                            let bytes = output.reserved_bytes;
                            drop(output);
                            credit.borrow_mut().try_shrink(bytes).unwrap();
                        }
                        let expected = vec![
                            ("A".to_string(), None),
                            ("A".to_string(), Some(owner(1, 5))),
                            ("A".to_string(), Some(owner(2, 5))),
                            ("B".to_string(), None),
                            ("B".to_string(), Some(owner(1, 5))),
                            ("B".to_string(), Some(owner(2, 5))),
                        ];
                        assert_eq!(
                            seen, expected,
                            "ACL sees canonical merged physical owners before coalescing; source={source_backed}, direction={direction:?}, loop={self_loop}"
                        );
                        let count = if physical {
                            if deny_committed { 4 } else { 6 }
                        } else {
                            2
                        };
                        assert_eq!(
                            outputs,
                            vec![target; count],
                            "only logical triads coalesce across lanes"
                        );
                        let physical_reads = if self_loop && matches!(direction, Direction::Both) {
                            6
                        } else {
                            3
                        };
                        assert_eq!(
                            staged_reads.get(),
                            physical_reads,
                            "one read per staged physical witness"
                        );
                        if source_backed {
                            assert_eq!(
                                reads.load(Ordering::SeqCst),
                                physical_reads / 3 * 4,
                                "source paging remains linear"
                            );
                        }
                        credit
                            .borrow_mut()
                            .try_shrink(cursor.into_retained_bytes())
                            .unwrap();
                        assert_eq!(credit.borrow().bytes(), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn mixed_lane_lookaheads_release_on_refusal_and_cursor_drop() {
    use contextdb_core::read_memory::ReadCredit;
    use contextdb_core::{Error, SnapshotId};
    use contextdb_graph::{GraphAdjEntry, mem::StagedTraversalEdge};
    use std::cell::{Cell, RefCell};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for fail_at in 0..40 {
        for seam in ["work", "retain", "acl", "path"] {
            let triad = triad();
            let committed: Vec<GraphAdjEntry> = ["A", "B"]
                .into_iter()
                .map(|name| {
                    let mut edge = owned(&triad, owner(2, 5));
                    edge.entry.edge_type = name.into();
                    edge.into()
                })
                .collect();
            let staged: Vec<GraphAdjEntry> = ["A", "B"]
                .into_iter()
                .map(|name| {
                    let mut edge = owned(&triad, owner(1, 5));
                    edge.entry.edge_type = name.into();
                    edge.into()
                })
                .collect();
            let graph = Arc::new(GraphStore::new());
            let exec = MemGraphExecutor::new(
                graph.clone(),
                Arc::new(TransactionManager::new(GraphApplier { graph })),
            );
            let memory = Arc::new(test_read_memory::TestReadMemory::default());
            let credit = RefCell::new(ReadCredit::new(memory.clone()));
            let reads = Arc::new(AtomicUsize::new(0));
            let source = Arc::new(WitnessSnapshot {
                entries: Arc::new(committed),
                memory,
                reads: reads.clone(),
            });
            let mut cursor = exec
                .bounded_bfs_cursor_at_source::<Error>(
                    triad.source,
                    None,
                    Direction::Outgoing,
                    1,
                    1,
                    SnapshotId(10),
                    Some(source),
                    |bytes| {
                        credit
                            .borrow_mut()
                            .try_grow_for(bytes, "reader", "mixed_cursor", "fixture")
                    },
                    |bytes| credit.borrow_mut().try_shrink(bytes).unwrap(),
                )
                .unwrap();
            let ordinal = Cell::new(0);
            let probe = |name: &str| -> Result<(), Error> {
                if name == seam {
                    let position = ordinal.get();
                    ordinal.set(position + 1);
                    if position == fail_at {
                        return Err(Error::Other("injected mixed-lane refusal".into()));
                    }
                }
                Ok(())
            };
            let staged_reads = Cell::new(0);
            // Drop at each emitted page as well as each injected refusal.
            let result = exec.bounded_bfs_next_including_staged_with_owner::<Error>(
                &mut cursor,
                || probe("work"),
                || probe("path"),
                |bytes| {
                    probe("retain")?;
                    credit
                        .borrow_mut()
                        .try_grow_for(bytes, "reader", "mixed_cursor", "fixture")
                },
                |bytes| credit.borrow_mut().try_shrink(bytes).unwrap(),
                |_, _, _, _, _| {
                    probe("acl")?;
                    Ok(true)
                },
                |node, direction, position| {
                    assert_eq!(direction, Direction::Outgoing);
                    let Some(entry) = staged.get(position).filter(|_| node == triad.source) else {
                        return Ok(None);
                    };
                    let bytes = std::mem::size_of::<StagedTraversalEdge>()
                        + entry.edge_type.len()
                        + entry.row_owner.as_ref().unwrap().table.len();
                    credit
                        .borrow_mut()
                        .try_grow_for(bytes, "reader", "mixed_staged", "fixture")?;
                    staged_reads.set(staged_reads.get() + 1);
                    Ok(Some((
                        StagedTraversalEdge {
                            source: entry.source,
                            target: entry.target,
                            edge_type: entry.edge_type.clone(),
                            row_owner: entry.row_owner.clone(),
                        },
                        bytes,
                        position + 1,
                    )))
                },
                || {},
                None,
            );
            if let Ok(Some(output)) = result {
                let bytes = output.reserved_bytes;
                drop(output);
                credit.borrow_mut().try_shrink(bytes).unwrap();
            }
            assert!(
                reads.load(Ordering::SeqCst) <= 2,
                "source windows fetched at most once"
            );
            assert!(staged_reads.get() <= 2, "staged ranks fetched at most once");
            let bytes = cursor.into_retained_bytes();
            credit.borrow_mut().try_shrink(bytes).unwrap();
            assert_eq!(
                credit.borrow().bytes(),
                0,
                "all manually held lookahead credits released: seam={seam}, fail_at={fail_at}"
            );
        }
    }
}
