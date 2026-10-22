use crate::error::Result;
use crate::types::*;
use std::collections::HashMap;

pub trait RelationalExecutor: Send + Sync {
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn scan(&self, table: &str, snapshot: SnapshotId) -> Result<Vec<VersionedRow>>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn scan_filter(
        &self,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
    ) -> Result<Vec<VersionedRow>>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn point_lookup(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn insert(&self, tx: TxId, table: &str, values: HashMap<ColName, Value>) -> Result<RowId>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn upsert(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<UpsertResult>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn delete(&self, tx: TxId, table: &str, row_id: RowId) -> Result<()>;
}

pub trait GraphExecutor: Send + Sync {
    #[allow(clippy::too_many_arguments)] // Keep this established input list explicit without changing its API.
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn bfs(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
        read_memory: crate::read_memory::ReadMemoryOwner,
    ) -> Result<TraversalResult>;
    #[allow(clippy::type_complexity)]
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn neighbors(
        &self,
        node: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
        read_memory: crate::read_memory::ReadMemoryOwner,
    ) -> Result<Vec<(NodeId, EdgeType, HashMap<String, Value>)>>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn insert_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        properties: HashMap<String, Value>,
        read_memory: crate::read_memory::ReadMemoryOwner,
    ) -> Result<bool>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn delete_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        read_memory: crate::read_memory::ReadMemoryOwner,
    ) -> Result<()>;
}

pub trait VectorExecutor: Send + Sync {
    type SearchOwner;
    type SearchOutput: IntoIterator<Item = (RowId, f32)> + std::ops::Deref<Target = [(RowId, f32)]>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn search(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&roaring::RoaringTreemap>,
        snapshot: SnapshotId,
        owner: Self::SearchOwner,
    ) -> Result<Self::SearchOutput>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn insert_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
        executing_owner: Self::SearchOwner,
    ) -> Result<()>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn delete_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        executing_owner: Self::SearchOwner,
    ) -> Result<()>;
}

pub trait TransactionManager: Send + Sync {
    fn begin(&self) -> TxId;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn commit(
        &self,
        tx: TxId,
        read_memory: crate::read_memory::ReadMemoryOwner,
    ) -> Result<()>;
    #[expect(clippy::result_large_err, reason = "Public Error retains inline NativeRefusal diagnostics so exhausted memory budgets can refuse without allocating.")]
    fn rollback(&self, tx: TxId) -> Result<()>;
    fn snapshot(&self) -> SnapshotId;
}
