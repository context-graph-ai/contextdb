use crate::source::{GraphCursor, GraphQuery, GraphReadSource, GraphSnapshot};
use contextdb_core::{AdjEntry, Direction, EdgeType, NodeId, Result, SnapshotId, TxId};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct GraphStore {
    source: Option<Arc<dyn GraphReadSource>>,
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
            source: None,
            forward_adj: RwLock::new(HashMap::new()),
            reverse_adj: RwLock::new(HashMap::new()),
        }
    }

    /// File-backed stores retain no committed adjacency. The source owns the
    /// published root; the engine keeps staged mutations in its write set.
    pub fn with_source(source: Arc<dyn GraphReadSource>) -> Self {
        Self {
            source: Some(source),
            ..Self::new()
        }
    }

    pub fn is_source_backed(&self) -> bool {
        self.source.is_some()
    }

    pub fn capture_source(
        &self,
        snapshot: SnapshotId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Arc<dyn GraphSnapshot>>> {
        self.source
            .as_ref()
            .map(|source| source.capture(snapshot, read_memory))
            .transpose()
    }

    /// Visit one version at a time; false stops at the current window. This
    /// helper never collects adjacency, and an identity query seeks only that
    /// identity in the durable implementation.
    pub fn visit_edges(
        &self,
        root: Option<&Arc<dyn GraphSnapshot>>,
        query: GraphQuery<'_>,
        mut visit: impl FnMut(&AdjEntry) -> Result<bool>,
    ) -> Result<()> {
        if let Some(root) = root {
            let mut cursor = root.cursor(query)?;
            while let Some(window) = cursor.next_edge()? {
                if !visit(window.edge())? {
                    break;
                }
            }
            return Ok(());
        }
        if self.is_source_backed() {
            return Err(contextdb_core::Error::Other(
                "graph read requires a captured source root".into(),
            ));
        }
        let adjacency = match query.direction {
            Direction::Outgoing => &self.forward_adj,
            Direction::Incoming => &self.reverse_adj,
            Direction::Both => {
                return Err(contextdb_core::Error::Other(
                    "graph cursor requires one direction".into(),
                ));
            }
        };
        let entries = adjacency.read();
        if let Some(entries) = entries.get(&query.node) {
            for entry in entries {
                if let Some((other, edge_type)) = query.identity {
                    let endpoint = if matches!(query.direction, Direction::Incoming) {
                        entry.source
                    } else {
                        entry.target
                    };
                    if endpoint != other || entry.edge_type != edge_type {
                        continue;
                    }
                }
                if !visit(entry)? {
                    break;
                }
            }
        }
        Ok(())
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
        if self.is_source_backed() {
            return;
        }
        let mut forward = self.forward_adj.write();
        let mut reverse = self.reverse_adj.write();
        *forward = publication.forward_adj;
        *reverse = publication.reverse_adj;
    }

    pub fn publish_prepared_received_edge_types(
        &self,
        publication: PreparedGraphPublication,
        affected_edge_types: &std::collections::HashSet<String>,
    ) {
        if self.is_source_backed() || affected_edge_types.is_empty() {
            return;
        }
        let mut forward = self.forward_adj.write();
        let mut reverse = self.reverse_adj.write();
        for entries in forward.values_mut() {
            entries.retain(|edge| !affected_edge_types.contains(&edge.edge_type));
        }
        forward.retain(|_, entries| !entries.is_empty());
        for entries in reverse.values_mut() {
            entries.retain(|edge| !affected_edge_types.contains(&edge.edge_type));
        }
        reverse.retain(|_, entries| !entries.is_empty());
        for (node, mut entries) in publication.forward_adj {
            forward.entry(node).or_default().append(&mut entries);
        }
        for (node, mut entries) in publication.reverse_adj {
            reverse.entry(node).or_default().append(&mut entries);
        }
    }

    pub fn apply_inserts(&self, inserts: Vec<AdjEntry>) {
        self.apply_inserts_ref(&inserts);
    }

    pub fn apply_inserts_ref(&self, inserts: &[AdjEntry]) {
        if self.is_source_backed() {
            return;
        }
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
        if self.is_source_backed() {
            return;
        }
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
        if self.is_source_backed() {
            return;
        }
        let mut fwd = self.forward_adj.write();
        let mut rev = self.reverse_adj.write();
        rev.entry(entry.target).or_default().push(entry.clone());
        fwd.entry(entry.source).or_default().push(entry);
    }
}
