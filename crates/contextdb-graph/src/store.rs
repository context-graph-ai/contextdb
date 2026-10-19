use crate::source::{GraphCursor, GraphQuery, GraphReadSource, GraphSnapshot};
use contextdb_core::{AdjEntry, Direction, EdgeType, NodeId, Result, SnapshotId, TxId};
use contextdb_tx::{OwnedAdjEntry, RowEdgeOwner};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Canonical memory adjacency, retaining provenance beside the public edge view.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct GraphAdjEntry {
    pub entry: AdjEntry,
    pub row_owner: Option<RowEdgeOwner>,
}
impl std::ops::Deref for GraphAdjEntry {
    type Target = AdjEntry;
    fn deref(&self) -> &AdjEntry {
        &self.entry
    }
}
impl std::ops::DerefMut for GraphAdjEntry {
    fn deref_mut(&mut self) -> &mut AdjEntry {
        &mut self.entry
    }
}
impl From<AdjEntry> for GraphAdjEntry {
    fn from(entry: AdjEntry) -> Self {
        Self {
            entry,
            row_owner: None,
        }
    }
}
impl From<OwnedAdjEntry> for GraphAdjEntry {
    fn from(owned: OwnedAdjEntry) -> Self {
        Self {
            entry: owned.entry,
            row_owner: Some(owned.owner),
        }
    }
}
impl GraphAdjEntry {
    pub fn estimated_bytes(&self) -> usize {
        self.entry.estimated_bytes().saturating_add(
            std::mem::size_of::<Option<RowEdgeOwner>>()
                .saturating_add(
                    self.row_owner
                        .as_ref()
                        .map_or(0, |owner| owner.table.capacity()),
                )
                .saturating_mul(2),
        )
    }
}

pub struct GraphStore {
    source: Option<Arc<dyn GraphReadSource>>,
    pub forward_adj: RwLock<HashMap<NodeId, Vec<GraphAdjEntry>>>,
    pub reverse_adj: RwLock<HashMap<NodeId, Vec<GraphAdjEntry>>>,
    // External to the source Arc it prices. Factory Arc clones retain the
    // same paired charge; this store never exports a source Arc or Weak.
    _source_control: Option<contextdb_tx::SourceCharge>,
}

/// Both adjacency directions, constructed together before received-schema
/// durability.  Publishing this payload only swaps the two maps.
pub struct PreparedGraphPublication {
    forward_adj: HashMap<NodeId, Vec<GraphAdjEntry>>,
    reverse_adj: HashMap<NodeId, Vec<GraphAdjEntry>>,
}

fn adjacency_order(
    left: &GraphAdjEntry,
    right: &GraphAdjEntry,
    incoming: bool,
) -> std::cmp::Ordering {
    let left_node = if incoming { left.source } else { left.target };
    let right_node = if incoming { right.source } else { right.target };
    left_node
        .cmp(&right_node)
        .then_with(|| left.edge_type.cmp(&right.edge_type))
        .then_with(|| left.row_owner.cmp(&right.row_owner))
        .then_with(|| left.created_tx.cmp(&right.created_tx))
        .then_with(|| left.lsn.cmp(&right.lsn))
}
fn group_adjacency(entries: &mut [GraphAdjEntry], incoming: bool) {
    entries.sort_unstable_by(|left, right| adjacency_order(left, right, incoming));
}
fn insert_adjacency(entries: &mut Vec<GraphAdjEntry>, entry: GraphAdjEntry, incoming: bool) {
    let position =
        entries.partition_point(|prior| adjacency_order(prior, &entry, incoming).is_le());
    entries.insert(position, entry);
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
            _source_control: None,
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

    /// Internal assembly of an already admitted source. Clone the matching
    /// external charge before cloning a source Arc outside this store.
    #[doc(hidden)]
    pub fn with_funded_source(
        source: Arc<dyn GraphReadSource>,
        control: contextdb_tx::SourceCharge,
    ) -> Self {
        Self { source: Some(source), _source_control: Some(control), ..Self::new() }
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
        self.visit_edges_with_owner(root, query, |entry, _| visit(entry))
    }

    pub fn visit_edges_with_owner(
        &self,
        root: Option<&Arc<dyn GraphSnapshot>>,
        query: GraphQuery<'_>,
        mut visit: impl FnMut(&AdjEntry, Option<&RowEdgeOwner>) -> Result<bool>,
    ) -> Result<()> {
        if let Some(root) = root {
            let mut cursor = root.cursor(query)?;
            while let Some(window) = cursor.next_edge()? {
                if !visit(window.edge(), window.row_owner())? {
                    break;
                }
            }
            return Ok(());
        }
        if self.is_source_backed() {
            return Err(contextdb_core::Error::Other(
                "graph read requires a captured source root".into()));
        }
        let adjacency = match query.direction {
            Direction::Outgoing => &self.forward_adj,
            Direction::Incoming => &self.reverse_adj,
            Direction::Both => {
                return Err(contextdb_core::Error::Other(
                    "graph cursor requires one direction".into()));
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
                if !visit(&entry.entry, entry.row_owner.as_ref())? {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn visit_row_edges(
        &self,
        root: Option<&Arc<dyn GraphSnapshot>>,
        query: GraphQuery<'_>,
        owner: &RowEdgeOwner,
        mut visit: impl FnMut(&AdjEntry) -> Result<bool>,
    ) -> Result<()> {
        if let Some(root) = root {
            let mut cursor = root.row_cursor(query, owner)?;
            while let Some(window) = cursor.next_edge()? {
                if window.row_owner() != Some(owner) {
                    return Err(contextdb_core::Error::Other(
                        "row graph cursor returned another owner".into()));
                }
                if !visit(window.edge())? {
                    break;
                }
            }
            return Ok(());
        }
        if self.is_source_backed() {
            return Err(contextdb_core::Error::Other(
                "graph read requires a captured source root".into()));
        }
        let Some((other, edge_type)) = query.identity else {
            return Err(contextdb_core::Error::Other(
                "row graph lookup requires its complete edge identity".into()));
        };
        let incoming = match query.direction {
            Direction::Incoming => true,
            Direction::Outgoing => false,
            Direction::Both => {
                return Err(contextdb_core::Error::Other(
                    "graph cursor requires one direction".into()));
            }
        };
        let adjacency = if incoming {
            &self.reverse_adj
        } else {
            &self.forward_adj
        };
        let adjacency = adjacency.read();
        let Some(entries) = adjacency.get(&query.node) else {
            return Ok(());
        };
        let compare = |candidate: &GraphAdjEntry| {
            let endpoint = if incoming {
                candidate.source
            } else {
                candidate.target
            };
            endpoint
                .cmp(&other)
                .then_with(|| candidate.edge_type.as_str().cmp(edge_type))
                .then_with(|| candidate.row_owner.as_ref().cmp(&Some(owner)))
        };
        let start = entries.partition_point(|entry| compare(entry).is_lt());
        for entry in &entries[start..] {
            if !compare(entry).is_eq() || !visit(&entry.entry)? {
                break;
            }
        }
        Ok(())
    }

    pub fn prepare_received_schema_publication(
        entries: Vec<GraphAdjEntry>,
    ) -> PreparedGraphPublication {
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
        for entries in forward_adj.values_mut() {
            group_adjacency(entries, false);
        }
        for entries in reverse_adj.values_mut() {
            group_adjacency(entries, true);
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

    /// Publish an already admitted selected image. Standalone artifacts are
    /// selected by type; row-owned artifacts strictly by the owning table.
    /// The prepared image contains only these replacements in both directions,
    /// in canonical order. An empty image removes the selected old artifacts.
    #[doc(hidden)]
    pub fn publish_prepared_received_schema_tables(
        &self,
        publication: PreparedGraphPublication,
        affected_edge_types: &std::collections::HashSet<String>,
        affected_tables: &std::collections::HashSet<String>,
    ) {
        if affected_edge_types.is_empty() && affected_tables.is_empty() {
            return;
        }
        self.publish_prepared_received_selection(publication, |edge| match &edge.row_owner {
            None => affected_edge_types.contains(&edge.edge_type),
            Some(owner) => affected_tables.contains(&owner.table),
        });
    }

    pub fn publish_prepared_received_edge_types(
        &self,
        publication: PreparedGraphPublication,
        affected_edge_types: &std::collections::HashSet<String>,
    ) {
        if affected_edge_types.is_empty() {
            return;
        }
        self.publish_prepared_received_selection(publication, |edge| {
            affected_edge_types.contains(&edge.edge_type)
        });
    }

    fn publish_prepared_received_selection(
        &self,
        publication: PreparedGraphPublication,
        selected: impl Fn(&GraphAdjEntry) -> bool,
    ) {
        if self.is_source_backed() {
            return;
        }
        let mut forward = self.forward_adj.write();
        let mut reverse = self.reverse_adj.write();
        for entries in forward.values_mut() {
            entries.retain(|edge| !selected(edge));
        }
        forward.retain(|_, entries| !entries.is_empty());
        for entries in reverse.values_mut() {
            entries.retain(|edge| !selected(edge));
        }
        reverse.retain(|_, entries| !entries.is_empty());
        for (node, mut entries) in publication.forward_adj {
            let affected = forward.entry(node).or_default();
            affected.append(&mut entries);
            group_adjacency(affected, false);
        }
        for (node, mut entries) in publication.reverse_adj {
            let affected = reverse.entry(node).or_default();
            affected.append(&mut entries);
            group_adjacency(affected, true);
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
        let staged: Vec<GraphAdjEntry> = inserts.iter().cloned().map(GraphAdjEntry::from).collect();
        let mut fwd = self.forward_adj.write();
        let mut rev = self.reverse_adj.write();

        for entry in staged {
            insert_adjacency(rev.entry(entry.target).or_default(), entry.clone(), true);
            insert_adjacency(fwd.entry(entry.source).or_default(), entry, false);
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
                    if e.row_owner.is_none()
                        && e.target == *target
                        && e.edge_type == *edge_type
                        && e.deleted_tx.is_none()
                    {
                        e.deleted_tx = Some(*deleted_tx);
                    }
                }
            }

            if let Some(entries) = rev.get_mut(target) {
                for e in entries.iter_mut() {
                    if e.row_owner.is_none()
                        && e.source == *source
                        && e.edge_type == *edge_type
                        && e.deleted_tx.is_none()
                    {
                        e.deleted_tx = Some(*deleted_tx);
                    }
                }
            }
        }
    }

    /// Row-derived inserts retain every independently owned version, even
    /// when another row or a standalone edge has the same endpoints.
    pub fn apply_row_inserts_ref(&self, inserts: &[OwnedAdjEntry]) {
        if self.is_source_backed() {
            return;
        }
        let staged: Vec<GraphAdjEntry> = inserts.iter().cloned().map(GraphAdjEntry::from).collect();
        let mut forward = self.forward_adj.write();
        let mut reverse = self.reverse_adj.write();
        for entry in staged {
            insert_adjacency(
                reverse.entry(entry.target).or_default(),
                entry.clone(),
                true,
            );
            insert_adjacency(forward.entry(entry.source).or_default(), entry, false);
        }
    }

    /// The immutable before-owner is the selection key; sibling endpoints do
    /// not authorize deleting another row's artifact.
    pub fn apply_row_deletes_ref(&self, deletes: &[(OwnedAdjEntry, TxId)]) {
        if self.is_source_backed() {
            return;
        }
        let mut forward = self.forward_adj.write();
        let mut reverse = self.reverse_adj.write();
        for (before, deleted_tx) in deletes {
            for (adjacency, node, incoming) in [
                (&mut *forward, before.entry.source, false),
                (&mut *reverse, before.entry.target, true),
            ] {
                if let Some(entries) = adjacency.get_mut(&node) {
                    let other = if incoming {
                        before.entry.source
                    } else {
                        before.entry.target
                    };
                    let compare = |candidate: &GraphAdjEntry| {
                        let endpoint = if incoming {
                            candidate.source
                        } else {
                            candidate.target
                        };
                        endpoint
                            .cmp(&other)
                            .then_with(|| candidate.edge_type.cmp(&before.entry.edge_type))
                            .then_with(|| candidate.row_owner.as_ref().cmp(&Some(&before.owner)))
                    };
                    let start = entries.partition_point(|entry| compare(entry).is_lt());
                    for entry in &mut entries[start..] {
                        if !compare(entry).is_eq() {
                            break;
                        }
                        if entry.deleted_tx.is_none() {
                            entry.deleted_tx = Some(*deleted_tx);
                        }
                    }
                }
            }
        }
    }

    pub fn insert_loaded_edge(&self, entry: AdjEntry) {
        self.insert_loaded_graph_entry(entry.into());
    }

    pub fn insert_loaded_graph_entry(&self, entry: GraphAdjEntry) {
        if self.is_source_backed() {
            return;
        }
        let mut fwd = self.forward_adj.write();
        let mut rev = self.reverse_adj.write();
        insert_adjacency(rev.entry(entry.target).or_default(), entry.clone(), true);
        insert_adjacency(fwd.entry(entry.source).or_default(), entry, false);
    }
}
