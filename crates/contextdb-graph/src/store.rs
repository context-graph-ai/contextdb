use contextdb_core::{AdjEntry, EdgeType, NodeId, TxId};
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct GraphStore {
    pub forward_adj: RwLock<HashMap<NodeId, Vec<AdjEntry>>>,
    pub reverse_adj: RwLock<HashMap<NodeId, Vec<AdjEntry>>>,
}

/// Both adjacency directions, constructed together before received-schema
/// durability.  Publishing this payload only swaps the two maps.
pub struct PreparedGraphPublication {
    forward_adj: HashMap<NodeId, Vec<AdjEntry>>,
    reverse_adj: HashMap<NodeId, Vec<AdjEntry>>,
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

    pub fn prepare_received_schema_publication(entries: Vec<AdjEntry>) -> PreparedGraphPublication {
        let mut forward_adj = HashMap::new();
        let mut reverse_adj = HashMap::new();
        for entry in entries {
            reverse_adj
                .entry(entry.target)
                .or_insert_with(Vec::new)
                .push(entry.clone());
            forward_adj
                .entry(entry.source)
                .or_insert_with(Vec::new)
                .push(entry);
        }
        PreparedGraphPublication {
            forward_adj,
            reverse_adj,
        }
    }

    pub fn publish_prepared_received_schema(&self, publication: PreparedGraphPublication) {
        let mut forward = self.forward_adj.write();
        let mut reverse = self.reverse_adj.write();
        *forward = publication.forward_adj;
        *reverse = publication.reverse_adj;
    }

    pub fn apply_inserts(&self, inserts: Vec<AdjEntry>) {
        self.apply_inserts_ref(&inserts);
    }

    pub fn apply_inserts_ref(&self, inserts: &[AdjEntry]) {
        // Stage owned copies BEFORE acquiring the adjacency write guards. Each
        // entry is retained in both the forward and reverse maps, so one copy
        // per side is unavoidable; staging outside the lock lets us move one of
        // them in (rather than clone both under the guard) and keeps the
        // writer's lock-hold window off the per-entry copy cost.
        let staged: Vec<AdjEntry> = inserts.to_vec();
        let mut fwd = self.forward_adj.write();
        let mut rev = self.reverse_adj.write();

        for entry in staged {
            rev.entry(entry.target).or_default().push(entry.clone());
            fwd.entry(entry.source).or_default().push(entry);
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(NodeId, EdgeType, NodeId, TxId)>) {
        self.apply_deletes_ref(&deletes);
    }

    pub fn apply_deletes_ref(&self, deletes: &[(NodeId, EdgeType, NodeId, TxId)]) {
        let mut fwd = self.forward_adj.write();
        let mut rev = self.reverse_adj.write();

        for (source, edge_type, target, deleted_tx) in deletes {
            if let Some(entries) = fwd.get_mut(source) {
                for e in entries.iter_mut() {
                    if e.target == *target && e.edge_type == *edge_type && e.deleted_tx.is_none() {
                        e.deleted_tx = Some(*deleted_tx);
                    }
                }
            }

            if let Some(entries) = rev.get_mut(target) {
                for e in entries.iter_mut() {
                    if e.source == *source && e.edge_type == *edge_type && e.deleted_tx.is_none() {
                        e.deleted_tx = Some(*deleted_tx);
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
