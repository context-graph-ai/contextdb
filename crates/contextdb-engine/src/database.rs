use crate::composite_store::{ChangeLogEntry, CompositeStore};
use crate::executor::execute_plan;
use crate::schema_enforcer::validate_dml;
use crate::sync_types::{
    ApplyResult, ChangeSet, Conflict, ConflictPolicies, ConflictPolicy, DdlChange, EdgeChange,
    NaturalKey, RowChange, VectorChange,
};
use contextdb_core::*;
use contextdb_graph::{GraphStore, MemGraphExecutor};
use contextdb_parser::Statement;
use contextdb_planner::PhysicalPlan;
use contextdb_relational::{MemRelationalExecutor, RelationalStore};
use contextdb_tx::TxManager;
use contextdb_vector::{MemVectorExecutor, VectorStore};
use parking_lot::Mutex;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
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
    instance_id: uuid::Uuid,
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
            instance_id: uuid::Uuid::new_v4(),
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

    pub fn insert_row(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<RowId> {
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

    pub fn delete_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
    ) -> Result<()> {
        self.graph.delete_edge(tx, source, target, edge_type)
    }

    pub fn query_bfs(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        max_depth: u32,
        snapshot: SnapshotId,
    ) -> Result<TraversalResult> {
        self.graph
            .bfs(start, edge_types, direction, 1, max_depth, snapshot)
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

    pub fn instance_id(&self) -> uuid::Uuid {
        self.instance_id
    }

    pub(crate) fn graph(&self) -> &MemGraphExecutor<CompositeStore> {
        &self.graph
    }

    pub(crate) fn relational_store(&self) -> &Arc<RelationalStore> {
        &self.relational_store
    }

    pub(crate) fn next_lsn_for_ddl(&self) -> u64 {
        self.tx_mgr.next_lsn()
    }

    pub(crate) fn log_create_table_ddl(&self, name: &str, meta: &TableMeta, lsn: u64) {
        self.tx_mgr.store().log_create_table(name, meta, lsn);
    }

    pub(crate) fn log_drop_table_ddl(&self, name: &str, lsn: u64) {
        self.tx_mgr.store().log_drop_table(name, lsn);
    }

    pub fn change_log_since(&self, since_lsn: u64) -> Vec<ChangeLogEntry> {
        let log = self.tx_mgr.store().change_log.read();
        let start = log.partition_point(|e| e.lsn() <= since_lsn);
        log[start..].to_vec()
    }

    pub fn ddl_log_since(&self, since_lsn: u64) -> Vec<DdlChange> {
        let ddl = self.tx_mgr.store().ddl_log.read();
        let start = ddl.partition_point(|(lsn, _)| *lsn <= since_lsn);
        ddl[start..].iter().map(|(_, c)| c.clone()).collect()
    }

    /// Extracts changes from this database since the given LSN.
    pub fn changes_since(&self, since_lsn: u64) -> ChangeSet {
        let mut rows = Vec::new();
        let mut edges = Vec::new();
        let mut vectors = Vec::new();
        let ddl = self.ddl_log_since(since_lsn);

        for entry in self.change_log_since(since_lsn) {
            match entry {
                ChangeLogEntry::RowInsert { table, row_id, lsn } => {
                    if let Some((natural_key, values)) = self.row_change_values(&table, row_id) {
                        rows.push(RowChange {
                            table,
                            natural_key,
                            values,
                            lsn,
                        });
                    }
                }
                ChangeLogEntry::RowDelete {
                    table,
                    natural_key,
                    lsn,
                    ..
                } => {
                    let mut values = HashMap::new();
                    values.insert("__deleted".to_string(), Value::Bool(true));
                    rows.push(RowChange {
                        table,
                        natural_key,
                        values,
                        lsn,
                    });
                }
                ChangeLogEntry::EdgeInsert {
                    source,
                    target,
                    edge_type,
                    lsn,
                } => {
                    let properties = self
                        .edge_properties(source, target, &edge_type, lsn)
                        .unwrap_or_default();
                    edges.push(EdgeChange {
                        source,
                        target,
                        edge_type,
                        properties,
                        lsn,
                    });
                }
                ChangeLogEntry::EdgeDelete {
                    source,
                    target,
                    edge_type,
                    lsn,
                } => {
                    let mut properties = HashMap::new();
                    properties.insert("__deleted".to_string(), Value::Bool(true));
                    edges.push(EdgeChange {
                        source,
                        target,
                        edge_type,
                        properties,
                        lsn,
                    });
                }
                ChangeLogEntry::VectorInsert { row_id, lsn } => {
                    if let Some(vector) = self.vector_for_row_lsn(row_id, lsn) {
                        vectors.push(VectorChange {
                            row_id,
                            vector,
                            lsn,
                        });
                    }
                }
                ChangeLogEntry::VectorDelete { row_id, lsn } => vectors.push(VectorChange {
                    row_id,
                    vector: Vec::new(),
                    lsn,
                }),
            }
        }

        // Deduplicate upserts: when a RowDelete is followed by a RowInsert for the same
        // (table, natural_key), the delete is part of an upsert — remove it.
        // Build a set of (table, natural_key) that have a non-delete entry.
        let insert_keys: HashSet<(String, String, String)> = rows
            .iter()
            .filter(|r| !matches!(r.values.get("__deleted"), Some(Value::Bool(true))))
            .map(|r| {
                (
                    r.table.clone(),
                    r.natural_key.column.clone(),
                    format!("{:?}", r.natural_key.value),
                )
            })
            .collect();
        rows.retain(|r| {
            if matches!(r.values.get("__deleted"), Some(Value::Bool(true))) {
                // Keep the delete only if there's no subsequent insert for this key
                let key = (
                    r.table.clone(),
                    r.natural_key.column.clone(),
                    format!("{:?}", r.natural_key.value),
                );
                !insert_keys.contains(&key)
            } else {
                true
            }
        });

        ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
        }
    }

    /// Returns the current LSN of this database.
    pub fn current_lsn(&self) -> u64 {
        self.tx_mgr.current_lsn()
    }

    /// Applies a ChangeSet to this database with the given conflict policies.
    pub fn apply_changes(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<ApplyResult> {
        let mut tx = self.begin();
        let mut result = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: Vec::new(),
            new_lsn: self.current_lsn(),
        };
        let vector_row_ids = changes
            .vectors
            .iter()
            .filter(|v| !v.vector.is_empty())
            .map(|v| v.row_id)
            .collect::<Vec<_>>();
        let mut vector_row_map: HashMap<u64, u64> = HashMap::new();
        let mut vector_row_idx = 0usize;
        let mut failed_row_ids: HashSet<u64> = HashSet::new();

        for ddl in changes.ddl {
            match ddl {
                DdlChange::CreateTable {
                    name,
                    columns,
                    constraints,
                } => {
                    if self.table_meta(&name).is_some() {
                        continue;
                    }
                    let mut sql = format!(
                        "CREATE TABLE {} ({})",
                        name,
                        columns
                            .iter()
                            .map(|(col, ty)| format!("{col} {ty}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    if !constraints.is_empty() {
                        sql.push(' ');
                        sql.push_str(&constraints.join(" "));
                    }
                    self.execute_in_tx(tx, &sql, &HashMap::new())?;
                }
            }
        }

        for row in changes.rows {
            if row.values.is_empty() {
                result.skipped_rows += 1;
                self.commit(tx)?;
                tx = self.begin();
                continue;
            }

            let policy = policies
                .per_table
                .get(&row.table)
                .copied()
                .unwrap_or(policies.default);

            let existing = self.point_lookup(
                &row.table,
                &row.natural_key.column,
                &row.natural_key.value,
                self.snapshot(),
            )?;
            let is_delete = matches!(row.values.get("__deleted"), Some(Value::Bool(true)));

            if is_delete {
                if let Some(local) = existing {
                    if let Err(err) = self.delete_row(tx, &row.table, local.row_id) {
                        result.conflicts.push(Conflict {
                            natural_key: row.natural_key.clone(),
                            resolution: policy,
                            reason: Some(format!("delete failed: {err}")),
                        });
                        result.skipped_rows += 1;
                    } else {
                        result.applied_rows += 1;
                    }
                } else {
                    result.skipped_rows += 1;
                }
                self.commit(tx)?;
                tx = self.begin();
                continue;
            }

            let mut values = row.values.clone();
            values.remove("__deleted");

            match (existing, policy) {
                (None, _) => match self.insert_row(tx, &row.table, values.clone()) {
                    Ok(new_row_id) => {
                        result.applied_rows += 1;
                        if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                            vector_row_map.insert(*remote_row_id, new_row_id);
                            vector_row_idx += 1;
                        }
                    }
                    Err(err) => {
                        result.skipped_rows += 1;
                        if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                            failed_row_ids.insert(*remote_row_id);
                            vector_row_idx += 1;
                        }
                        result.conflicts.push(Conflict {
                            natural_key: row.natural_key.clone(),
                            resolution: policy,
                            reason: Some(format!("{err}")),
                        });
                    }
                },
                (Some(local), ConflictPolicy::InsertIfNotExists) => {
                    if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                        vector_row_map.insert(*remote_row_id, local.row_id);
                        vector_row_idx += 1;
                    }
                    result.skipped_rows += 1;
                }
                (Some(_), ConflictPolicy::ServerWins) => {
                    result.skipped_rows += 1;
                    if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                        failed_row_ids.insert(*remote_row_id);
                        vector_row_idx += 1;
                    }
                    result.conflicts.push(Conflict {
                        natural_key: row.natural_key.clone(),
                        resolution: ConflictPolicy::ServerWins,
                        reason: Some("server_wins".to_string()),
                    });
                }
                (Some(local), ConflictPolicy::LatestWins) => {
                    if row.lsn <= local.lsn {
                        result.skipped_rows += 1;
                        if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                            failed_row_ids.insert(*remote_row_id);
                            vector_row_idx += 1;
                        }
                        result.conflicts.push(Conflict {
                            natural_key: row.natural_key.clone(),
                            resolution: ConflictPolicy::LatestWins,
                            reason: Some("local_lsn_newer_or_equal".to_string()),
                        });
                    } else {
                        match self.upsert_row(
                            tx,
                            &row.table,
                            &row.natural_key.column,
                            values.clone(),
                        ) {
                            Ok(_) => {
                                result.applied_rows += 1;
                                if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                                    if let Ok(Some(found)) = self.point_lookup(
                                        &row.table,
                                        &row.natural_key.column,
                                        &row.natural_key.value,
                                        self.snapshot(),
                                    ) {
                                        vector_row_map.insert(*remote_row_id, found.row_id);
                                    }
                                    vector_row_idx += 1;
                                }
                            }
                            Err(err) => {
                                result.skipped_rows += 1;
                                if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                                    failed_row_ids.insert(*remote_row_id);
                                    vector_row_idx += 1;
                                }
                                result.conflicts.push(Conflict {
                                    natural_key: row.natural_key.clone(),
                                    resolution: ConflictPolicy::LatestWins,
                                    reason: Some(format!("state_machine_or_constraint: {err}")),
                                });
                            }
                        }
                    }
                }
                (Some(_), ConflictPolicy::EdgeWins) => {
                    result.conflicts.push(Conflict {
                        natural_key: row.natural_key.clone(),
                        resolution: ConflictPolicy::EdgeWins,
                        reason: Some("edge_wins".to_string()),
                    });
                    match self.upsert_row(tx, &row.table, &row.natural_key.column, values.clone()) {
                        Ok(_) => {
                            result.applied_rows += 1;
                            if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                                if let Ok(Some(found)) = self.point_lookup(
                                    &row.table,
                                    &row.natural_key.column,
                                    &row.natural_key.value,
                                    self.snapshot(),
                                ) {
                                    vector_row_map.insert(*remote_row_id, found.row_id);
                                }
                                vector_row_idx += 1;
                            }
                        }
                        Err(err) => {
                            result.skipped_rows += 1;
                            if let Some(remote_row_id) = vector_row_ids.get(vector_row_idx) {
                                failed_row_ids.insert(*remote_row_id);
                                vector_row_idx += 1;
                            }
                            if let Some(last) = result.conflicts.last_mut() {
                                last.reason = Some(format!("state_machine_or_constraint: {err}"));
                            }
                        }
                    }
                }
            }

            self.commit(tx)?;
            tx = self.begin();
        }

        for edge in changes.edges {
            let is_delete = matches!(edge.properties.get("__deleted"), Some(Value::Bool(true)));
            if is_delete {
                let _ = self.delete_edge(tx, edge.source, edge.target, &edge.edge_type);
            } else {
                let _ = self.insert_edge(
                    tx,
                    edge.source,
                    edge.target,
                    edge.edge_type,
                    edge.properties,
                );
            }
        }

        for vector in changes.vectors {
            if failed_row_ids.contains(&vector.row_id) {
                continue; // skip vectors for rows that failed to insert
            }
            let local_row_id = vector_row_map
                .get(&vector.row_id)
                .copied()
                .unwrap_or(vector.row_id);
            if vector.vector.is_empty() {
                let _ = self.vector.delete_vector(tx, local_row_id);
            } else {
                let _ = self.insert_vector(tx, local_row_id, vector.vector);
            }
        }

        self.commit(tx)?;
        result.new_lsn = self.current_lsn();
        Ok(result)
    }

    fn row_change_values(
        &self,
        table: &str,
        row_id: RowId,
    ) -> Option<(NaturalKey, HashMap<String, Value>)> {
        let tables = self.relational_store.tables.read();
        let meta = self.relational_store.table_meta.read();
        let rows = tables.get(table)?;
        let row = rows.iter().find(|r| r.row_id == row_id)?;
        let key_col = meta
            .get(table)
            .and_then(|m| m.natural_key_column.clone())
            .or_else(|| {
                meta.get(table).and_then(|m| {
                    m.columns
                        .iter()
                        .find(|c| c.name == "id" && c.column_type == ColumnType::Uuid)
                        .map(|_| "id".to_string())
                })
            })?;

        let key_val = row.values.get(&key_col)?.clone();
        let values = row
            .values
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<_, _>>();
        Some((
            NaturalKey {
                column: key_col,
                value: key_val,
            },
            values,
        ))
    }

    fn edge_properties(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        lsn: u64,
    ) -> Option<HashMap<String, Value>> {
        self.tx_mgr
            .store()
            .graph
            .forward_adj
            .read()
            .get(&source)
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|e| e.target == target && e.edge_type == edge_type && e.lsn == lsn)
                    .map(|e| e.properties.clone())
            })
    }

    fn vector_for_row_lsn(&self, row_id: RowId, lsn: u64) -> Option<Vec<f32>> {
        self.tx_mgr
            .store()
            .vector
            .vectors
            .read()
            .iter()
            .find(|v| v.row_id == row_id && v.lsn == lsn)
            .map(|v| v.vector.clone())
    }
}
