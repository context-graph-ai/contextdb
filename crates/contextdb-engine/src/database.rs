use crate::composite_store::CompositeStore;
use crate::executor::execute_plan;
use crate::schema_enforcer::validate_dml;
use contextdb_core::*;
use contextdb_graph::{GraphStore, MemGraphExecutor};
use contextdb_parser::Statement;
use contextdb_planner::PhysicalPlan;
use contextdb_relational::{MemRelationalExecutor, RelationalStore};
use contextdb_tx::TxManager;
use contextdb_vector::{MemVectorExecutor, VectorStore};
use parking_lot::Mutex;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        }
    }

    pub fn empty_with_affected(rows_affected: u64) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected,
        }
    }
}

pub struct Database {
    tx_mgr: Arc<TxManager<CompositeStore>>,
    relational_store: Arc<RelationalStore>,
    relational: MemRelationalExecutor<CompositeStore>,
    graph: MemGraphExecutor<CompositeStore>,
    vector: MemVectorExecutor<CompositeStore>,
    session_tx: Mutex<Option<TxId>>,
}

impl Database {
    pub fn open_memory() -> Self {
        let relational = Arc::new(RelationalStore::new());
        let graph = Arc::new(GraphStore::new());
        let vector = Arc::new(VectorStore::new());

        let store = CompositeStore::new(relational.clone(), graph.clone(), vector.clone());
        let tx_mgr = Arc::new(TxManager::new(store));

        Self {
            relational_store: relational.clone(),
            relational: MemRelationalExecutor::new(relational, tx_mgr.clone()),
            graph: MemGraphExecutor::new(graph, tx_mgr.clone()),
            vector: MemVectorExecutor::new(vector, tx_mgr.clone()),
            tx_mgr,
            session_tx: Mutex::new(None),
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

    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let stmt = contextdb_parser::parse(sql)?;

        match stmt {
            Statement::Begin => {
                let mut session = self.session_tx.lock();
                if session.is_none() {
                    *session = Some(self.begin());
                }
                return Ok(QueryResult::empty());
            }
            Statement::Commit => {
                let mut session = self.session_tx.lock();
                if let Some(tx) = *session {
                    self.commit(tx)?;
                    *session = None;
                }
                return Ok(QueryResult::empty());
            }
            Statement::Rollback => {
                let mut session = self.session_tx.lock();
                if let Some(tx) = *session {
                    self.rollback(tx)?;
                    *session = None;
                }
                return Ok(QueryResult::empty());
            }
            _ => {}
        }

        let plan = contextdb_planner::plan(&stmt)?;
        validate_dml(&plan, self, params)?;
        let active_tx = *self.session_tx.lock();
        match active_tx {
            Some(tx) => execute_plan(self, &plan, params, Some(tx)),
            None => self.execute_autocommit(&plan, params),
        }
    }

    fn execute_autocommit(
        &self,
        plan: &PhysicalPlan,
        params: &HashMap<String, Value>,
    ) -> Result<QueryResult> {
        match plan {
            PhysicalPlan::Insert(_) | PhysicalPlan::Delete(_) | PhysicalPlan::Update(_) => {
                let tx = self.begin();
                let result = execute_plan(self, plan, params, Some(tx));
                match result {
                    Ok(qr) => {
                        self.commit(tx)?;
                        Ok(qr)
                    }
                    Err(e) => {
                        let _ = self.rollback(tx);
                        Err(e)
                    }
                }
            }
            _ => execute_plan(self, plan, params, None),
        }
    }

    pub fn explain(&self, sql: &str) -> Result<String> {
        let stmt = contextdb_parser::parse(sql)?;
        let plan = contextdb_planner::plan(&stmt)?;
        Ok(plan.explain())
    }

    pub fn execute_in_tx(
        &self,
        tx: TxId,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> Result<QueryResult> {
        let stmt = contextdb_parser::parse(sql)?;
        let plan = contextdb_planner::plan(&stmt)?;
        validate_dml(&plan, self, params)?;
        execute_plan(self, &plan, params, Some(tx))
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

    pub fn table_names(&self) -> Vec<String> {
        self.relational_store.table_names()
    }

    pub fn table_meta(&self, table: &str) -> Option<TableMeta> {
        self.relational_store.table_meta(table)
    }

    pub(crate) fn graph(&self) -> &MemGraphExecutor<CompositeStore> {
        &self.graph
    }

    pub(crate) fn relational_store(&self) -> &Arc<RelationalStore> {
        &self.relational_store
    }
}
