use crate::composite_store::CompositeStore;
use contextdb_core::*;
use contextdb_graph::{GraphStore, MemGraphExecutor};
use contextdb_relational::{MemRelationalExecutor, RelationalStore};
use contextdb_tx::TxManager;
use contextdb_vector::{MemVectorExecutor, VectorStore};
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Database {
    tx_mgr: Arc<TxManager<CompositeStore>>,
    relational: MemRelationalExecutor<CompositeStore>,
    graph: MemGraphExecutor<CompositeStore>,
    vector: MemVectorExecutor<CompositeStore>,
}

impl Database {
    pub fn open_memory() -> Self {
        let relational = Arc::new(RelationalStore::new());
        let graph = Arc::new(GraphStore::new());
        let vector = Arc::new(VectorStore::new());

        let store = CompositeStore::new(relational.clone(), graph.clone(), vector.clone());
        let tx_mgr = Arc::new(TxManager::new(store));

        Self {
            relational: MemRelationalExecutor::new(relational, tx_mgr.clone()),
            graph: MemGraphExecutor::new(graph, tx_mgr.clone()),
            vector: MemVectorExecutor::new(vector, tx_mgr.clone()),
            tx_mgr,
        }
    }

    pub fn begin(&self) -> TxId {
        self.tx_mgr.begin()
    }

    pub fn commit(&self, tx: TxId) -> Result<()> {
        self.tx_mgr.commit(tx)
    }

    pub fn rollback(&self, tx: TxId) -> Result<()> {
        self.tx_mgr.rollback(tx)
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.tx_mgr.snapshot()
    }

    pub fn insert_row(&self, tx: TxId, table: &str, values: HashMap<ColName, Value>) -> Result<RowId> {
        self.relational.insert(tx, table, values)
    }

    pub fn upsert_row(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<UpsertResult> {
        self.relational
            .upsert(tx, table, conflict_col, values, self.snapshot())
    }

    pub fn delete_row(&self, tx: TxId, table: &str, row_id: RowId) -> Result<()> {
        self.relational.delete(tx, table, row_id)
    }

    pub fn scan(&self, table: &str, snapshot: SnapshotId) -> Result<Vec<VersionedRow>> {
        self.relational.scan(table, snapshot)
    }

    pub fn scan_filter(
        &self,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
    ) -> Result<Vec<VersionedRow>> {
        self.relational.scan_filter(table, snapshot, predicate)
    }

    pub fn point_lookup(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        self.relational.point_lookup(table, col, value, snapshot)
    }

    pub fn insert_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        properties: HashMap<String, Value>,
    ) -> Result<()> {
        self.graph
            .insert_edge(tx, source, target, edge_type, properties)
    }

    pub fn query_bfs(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        max_depth: u32,
        snapshot: SnapshotId,
    ) -> Result<TraversalResult> {
        self.graph.bfs(start, edge_types, direction, 1, max_depth, snapshot)
    }

    pub fn insert_vector(&self, tx: TxId, row_id: RowId, vector: Vec<f32>) -> Result<()> {
        self.vector.insert_vector(tx, row_id, vector)
    }

    pub fn query_vector(
        &self,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        self.vector.search(query, k, candidates, snapshot)
    }
}
