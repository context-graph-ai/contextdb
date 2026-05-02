use crate::store::GraphStore;
use contextdb_core::*;
use contextdb_tx::{TxManager, WriteSetApplicator};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

const MAX_VISITED: usize = 100_000;
type AdjPair = (NodeId, NodeId);

pub struct MemGraphExecutor<S: WriteSetApplicator> {
    store: Arc<GraphStore>,
    tx_mgr: Arc<TxManager<S>>,
    dag_edge_types: parking_lot::RwLock<HashSet<String>>,
}

impl<S: WriteSetApplicator> MemGraphExecutor<S> {
    pub fn new(store: Arc<GraphStore>, tx_mgr: Arc<TxManager<S>>) -> Self {
        Self {
            store,
            tx_mgr,
            dag_edge_types: parking_lot::RwLock::new(HashSet::new()),
        }
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

                if visited.len() > MAX_VISITED {
                    return Err(Error::BfsVisitedExceeded(MAX_VISITED));
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
            let fwd = self.store.forward_adj.read();
            if let Some(entries) = fwd.get(&source) {
                let live_in_committed = entries.iter().any(|e| {
                    e.target == target && e.edge_type == edge_type && e.deleted_tx.is_none()
                });
                if live_in_committed && !deleted_in_ws {
                    return Ok(false);
                }
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
