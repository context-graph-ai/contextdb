use crate::store::GraphStore;
use contextdb_core::*;
use contextdb_tx::{TxManager, WriteSetApplicator};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

const MAX_VISITED: usize = 100_000;

pub struct MemGraphExecutor<S: WriteSetApplicator> {
    store: Arc<GraphStore>,
    tx_mgr: Arc<TxManager<S>>,
}

impl<S: WriteSetApplicator> MemGraphExecutor<S> {
    pub fn new(store: Arc<GraphStore>, tx_mgr: Arc<TxManager<S>>) -> Self {
        Self { store, tx_mgr }
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

        let mut queue: VecDeque<(NodeId, u32, Vec<(NodeId, EdgeType)>)> = VecDeque::new();
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
    ) -> Result<()> {
        let entry = AdjEntry {
            source,
            target,
            edge_type,
            properties,
            created_tx: tx,
            deleted_tx: None,
        };

        self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_inserts.push(entry);
        })?;

        Ok(())
    }

    fn delete_edge(&self, tx: TxId, source: NodeId, target: NodeId, edge_type: &str) -> Result<()> {
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_deletes
                .push((source, edge_type.to_string(), target, tx));
        })?;
        Ok(())
    }
}
