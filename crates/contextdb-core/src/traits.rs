use crate::error::Result;
use crate::types::*;
use std::collections::HashMap;

pub trait RelationalExecutor: Send + Sync {
    fn scan(&self, table: &str, snapshot: SnapshotId) -> Result<Vec<VersionedRow>>;
    fn scan_filter(
        &self,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
    ) -> Result<Vec<VersionedRow>>;
    fn point_lookup(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>>;
    fn insert(&self, tx: TxId, table: &str, values: HashMap<ColName, Value>) -> Result<RowId>;
    fn upsert(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<UpsertResult>;
    fn delete(&self, tx: TxId, table: &str, row_id: RowId) -> Result<()>;
}

pub trait GraphExecutor: Send + Sync {
    fn bfs(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
    ) -> Result<TraversalResult>;
    fn neighbors(
        &self,
        node: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
    ) -> Result<Vec<(NodeId, EdgeType, HashMap<String, Value>)>>;
    fn insert_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        properties: HashMap<String, Value>,
    ) -> Result<()>;
    fn delete_edge(&self, tx: TxId, source: NodeId, target: NodeId, edge_type: &str) -> Result<()>;
}

pub trait VectorExecutor: Send + Sync {
    fn search(
        &self,
        query: &[f32],
        k: usize,
        candidates: Option<&roaring::RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>>;
    fn insert_vector(&self, tx: TxId, row_id: RowId, vector: Vec<f32>) -> Result<()>;
    fn delete_vector(&self, tx: TxId, row_id: RowId) -> Result<()>;
}

pub trait TransactionManager: Send + Sync {
    fn begin(&self) -> TxId;
    fn commit(&self, tx: TxId) -> Result<()>;
    fn rollback(&self, tx: TxId) -> Result<()>;
    fn snapshot(&self) -> SnapshotId;
}
