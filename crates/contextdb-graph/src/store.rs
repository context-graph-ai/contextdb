use contextdb_core::{AdjEntry, EdgeType, NodeId};
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct GraphStore {
    pub forward_adj: RwLock<HashMap<NodeId, Vec<AdjEntry>>>,
    pub reverse_adj: RwLock<HashMap<NodeId, Vec<AdjEntry>>>,
}

impl Default for GraphStore {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphStore {
    pub fn new() -> Self {
        Self {
            forward_adj: RwLock::new(HashMap::new()),
            reverse_adj: RwLock::new(HashMap::new()),
        }
    }

    pub fn apply_inserts(&self, inserts: Vec<AdjEntry>) {
        let mut fwd = self.forward_adj.write();
        let mut rev = self.reverse_adj.write();

        for entry in inserts {
            rev.entry(entry.target).or_default().push(entry.clone());
            fwd.entry(entry.source).or_default().push(entry);
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(NodeId, EdgeType, NodeId, u64)>) {
        let mut fwd = self.forward_adj.write();
        let mut rev = self.reverse_adj.write();

        for (source, edge_type, target, deleted_tx) in deletes {
            if let Some(entries) = fwd.get_mut(&source) {
                for e in entries.iter_mut() {
                    if e.target == target && e.edge_type == edge_type && e.deleted_tx.is_none() {
                        e.deleted_tx = Some(deleted_tx);
                    }
                }
            }

            if let Some(entries) = rev.get_mut(&target) {
                for e in entries.iter_mut() {
                    if e.source == source && e.edge_type == edge_type && e.deleted_tx.is_none() {
                        e.deleted_tx = Some(deleted_tx);
                    }
                }
            }
        }
    }

    pub fn insert_loaded_edge(&self, entry: AdjEntry) {
        let mut fwd = self.forward_adj.write();
        let mut rev = self.reverse_adj.write();
        rev.entry(entry.target).or_default().push(entry.clone());
        fwd.entry(entry.source).or_default().push(entry);
    }
}
