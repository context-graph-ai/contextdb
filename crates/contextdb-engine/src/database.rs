use crate::composite_store::{ChangeLogEntry, CompositeStore};
use crate::executor::execute_plan;
use crate::persistence::RedbPersistence;
use crate::persistent_store::PersistentCompositeStore;
use crate::plugin::{CommitSource, CorePlugin, DatabasePlugin, PluginHealth, QueryOutcome};
use crate::schema_enforcer::validate_dml;
use crate::sync_types::{
    ApplyResult, ChangeSet, Conflict, ConflictPolicies, ConflictPolicy, DdlChange, EdgeChange,
    NaturalKey, RowChange, VectorChange,
};
use contextdb_core::*;
use contextdb_graph::{GraphStore, MemGraphExecutor};
use contextdb_parser::Statement;
use contextdb_parser::ast::{AlterAction, CreateTable, DataType};
use contextdb_planner::PhysicalPlan;
use contextdb_relational::{MemRelationalExecutor, RelationalStore};
use contextdb_tx::{TxManager, WriteSetApplicator};
use contextdb_vector::{HnswIndex, MemVectorExecutor, VectorStore};
use parking_lot::{Mutex, RwLock};
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

type DynStore = Box<dyn WriteSetApplicator>;

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
    tx_mgr: Arc<TxManager<DynStore>>,
    relational_store: Arc<RelationalStore>,
    graph_store: Arc<GraphStore>,
    vector_store: Arc<VectorStore>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    ddl_log: Arc<RwLock<Vec<(u64, DdlChange)>>>,
    persistence: Option<Arc<RedbPersistence>>,
    relational: MemRelationalExecutor<DynStore>,
    graph: MemGraphExecutor<DynStore>,
    vector: MemVectorExecutor<DynStore>,
    session_tx: Mutex<Option<TxId>>,
    instance_id: uuid::Uuid,
    plugin: Arc<dyn DatabasePlugin>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
struct PropagationQueueEntry {
    table: String,
    uuid: uuid::Uuid,
    target_state: String,
    depth: u32,
    abort_on_failure: bool,
}

#[derive(Debug, Clone, Copy)]
struct PropagationSource<'a> {
    table: &'a str,
    uuid: uuid::Uuid,
    state: &'a str,
    depth: u32,
}

#[derive(Debug, Clone, Copy)]
struct PropagationContext<'a> {
    tx: TxId,
    snapshot: SnapshotId,
    metas: &'a HashMap<String, TableMeta>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let persistence = if path.exists() {
            Arc::new(RedbPersistence::open(path)?)
        } else {
            Arc::new(RedbPersistence::create(path)?)
        };

        let all_meta = persistence.load_all_table_meta()?;

        let relational = Arc::new(RelationalStore::new());
        for (name, meta) in &all_meta {
            relational.create_table(name, meta.clone());
            for row in persistence.load_relational_table(name)? {
                relational.insert_loaded_row(name, row);
            }
        }

        let graph = Arc::new(GraphStore::new());
        for edge in persistence.load_forward_edges()? {
            graph.insert_loaded_edge(edge);
        }

        let hnsw = Arc::new(OnceLock::new());
        let vector = Arc::new(VectorStore::new(hnsw.clone()));
        for meta in all_meta.values() {
            for column in &meta.columns {
                if let ColumnType::Vector(dimension) = column.column_type {
                    vector.set_dimension(dimension);
                    break;
                }
            }
        }
        for entry in persistence.load_vectors()? {
            vector.insert_loaded_vector(entry);
        }

        let max_row_id = relational.max_row_id();
        let max_tx = max_tx_across_all(&relational, &graph, &vector);
        let max_lsn = max_lsn_across_all(&relational, &graph, &vector);
        relational.set_next_row_id(max_row_id.saturating_add(1));

        let change_log = Arc::new(RwLock::new(Vec::new()));
        let ddl_log = Arc::new(RwLock::new(Vec::new()));
        let composite = CompositeStore::new(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            ddl_log.clone(),
        );
        let persistent = PersistentCompositeStore::new(composite, persistence.clone());
        let store: DynStore = Box::new(persistent);
        let tx_mgr = Arc::new(TxManager::new_with_counters(
            store,
            max_tx.saturating_add(1),
            max_lsn.saturating_add(1),
            max_tx,
        ));

        let db = Self {
            tx_mgr: tx_mgr.clone(),
            relational_store: relational.clone(),
            graph_store: graph.clone(),
            vector_store: vector.clone(),
            change_log,
            ddl_log,
            persistence: Some(persistence),
            relational: MemRelationalExecutor::new(relational, tx_mgr.clone()),
            graph: MemGraphExecutor::new(graph, tx_mgr.clone()),
            vector: MemVectorExecutor::new(vector, tx_mgr.clone(), hnsw),
            session_tx: Mutex::new(None),
            instance_id: uuid::Uuid::new_v4(),
            plugin: Arc::new(CorePlugin),
        };

        for meta in all_meta.values() {
            if !meta.dag_edge_types.is_empty() {
                db.graph.register_dag_edge_types(&meta.dag_edge_types);
            }
        }

        maybe_prebuild_hnsw(&db.vector_store);

        Ok(db)
    }

    pub fn open_memory() -> Self {
        let relational = Arc::new(RelationalStore::new());
        let graph = Arc::new(GraphStore::new());
        let hnsw = Arc::new(OnceLock::new());
        let vector = Arc::new(VectorStore::new(hnsw.clone()));
        let change_log = Arc::new(RwLock::new(Vec::new()));
        let ddl_log = Arc::new(RwLock::new(Vec::new()));
        let store: DynStore = Box::new(CompositeStore::new(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            ddl_log.clone(),
        ));
        let tx_mgr = Arc::new(TxManager::new(store));

        let db = Self {
            tx_mgr: tx_mgr.clone(),
            relational_store: relational.clone(),
            graph_store: graph.clone(),
            vector_store: vector.clone(),
            change_log,
            ddl_log,
            persistence: None,
            relational: MemRelationalExecutor::new(relational, tx_mgr.clone()),
            graph: MemGraphExecutor::new(graph, tx_mgr.clone()),
            vector: MemVectorExecutor::new(vector, tx_mgr.clone(), hnsw),
            session_tx: Mutex::new(None),
            instance_id: uuid::Uuid::new_v4(),
            plugin: Arc::new(CorePlugin),
        };
        maybe_prebuild_hnsw(&db.vector_store);
        db
    }

    pub fn begin(&self) -> TxId {
        self.tx_mgr.begin()
    }

    pub fn commit(&self, tx: TxId) -> Result<()> {
        self.commit_with_source(tx, CommitSource::User)
    }

    pub fn rollback(&self, tx: TxId) -> Result<()> {
        self.tx_mgr.rollback(tx)
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.tx_mgr.snapshot()
    }

    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let stmt = contextdb_parser::parse(sql)?;

        match &stmt {
            Statement::Begin => {
                let mut session = self.session_tx.lock();
                if session.is_none() {
                    *session = Some(self.begin());
                }
                return Ok(QueryResult::empty());
            }
            Statement::Commit => {
                let mut session = self.session_tx.lock();
                if let Some(tx) = session.take() {
                    self.commit_with_source(tx, CommitSource::User)?;
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

        let active_tx = *self.session_tx.lock();
        self.execute_statement(&stmt, sql, params, active_tx)
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
                        self.commit_with_source(tx, CommitSource::AutoCommit)?;
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
        let mut output = plan.explain();
        if self.vector_store.vector_count() >= 1000 {
            output = output.replace("VectorSearch(", "HNSWSearch(");
        }
        Ok(output)
    }

    pub fn execute_in_tx(
        &self,
        tx: TxId,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> Result<QueryResult> {
        let stmt = contextdb_parser::parse(sql)?;
        self.execute_statement(&stmt, sql, params, Some(tx))
    }

    fn commit_with_source(&self, tx: TxId, source: CommitSource) -> Result<()> {
        let mut ws = self.tx_mgr.cloned_write_set(tx)?;

        if !ws.is_empty()
            && let Err(err) = self.plugin.pre_commit(&ws, source)
        {
            let _ = self.rollback(tx);
            return Err(err);
        }

        let lsn = self.tx_mgr.commit_with_lsn(tx)?;

        if !ws.is_empty() {
            ws.stamp_lsn(lsn);
            self.plugin.post_commit(&ws, source);
        }

        Ok(())
    }

    fn execute_statement(
        &self,
        stmt: &Statement,
        sql: &str,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
    ) -> Result<QueryResult> {
        self.plugin.on_query(sql)?;

        if let Some(change) = self.ddl_change_for_statement(stmt).as_ref() {
            self.plugin.on_ddl(change)?;
        }

        let started = Instant::now();
        let result = (|| {
            // Pre-resolve InSubquery expressions with CTE context before planning
            let stmt = self.pre_resolve_cte_subqueries(stmt, params, tx)?;
            let plan = contextdb_planner::plan(&stmt)?;
            validate_dml(&plan, self, params)?;
            let result = match tx {
                Some(tx) => execute_plan(self, &plan, params, Some(tx)),
                None => self.execute_autocommit(&plan, params),
            };
            if result.is_ok()
                && let Statement::CreateTable(ct) = &stmt
                && !ct.dag_edge_types.is_empty()
            {
                self.graph.register_dag_edge_types(&ct.dag_edge_types);
            }
            result
        })();
        let duration = started.elapsed();
        let outcome = query_outcome_from_result(&result);
        self.plugin.post_query(sql, duration, &outcome);
        result.map(strip_internal_row_id)
    }

    fn ddl_change_for_statement(&self, stmt: &Statement) -> Option<DdlChange> {
        match stmt {
            Statement::CreateTable(ct) => Some(ddl_change_from_create_table(ct)),
            Statement::DropTable(dt) => Some(DdlChange::DropTable {
                name: dt.name.clone(),
            }),
            Statement::AlterTable(at) => {
                let mut meta = self.table_meta(&at.table).unwrap_or_default();
                // Simulate the alter action on a cloned meta to get post-alteration columns
                match &at.action {
                    AlterAction::AddColumn(col) => {
                        meta.columns.push(contextdb_core::ColumnDef {
                            name: col.name.clone(),
                            column_type: crate::executor::map_column_type(&col.data_type),
                            nullable: col.nullable,
                            primary_key: col.primary_key,
                            unique: col.unique,
                            default: col.default.as_ref().map(|expr| format!("{expr:?}")),
                        });
                    }
                    AlterAction::DropColumn(name) => {
                        meta.columns.retain(|c| c.name != *name);
                    }
                    AlterAction::RenameColumn { from, to } => {
                        if let Some(c) = meta.columns.iter_mut().find(|c| c.name == *from) {
                            c.name = to.clone();
                        }
                    }
                }
                Some(DdlChange::AlterTable {
                    name: at.table.clone(),
                    columns: meta
                        .columns
                        .iter()
                        .map(|c| {
                            (
                                c.name.clone(),
                                sql_type_for_meta_column(c, &meta.propagation_rules),
                            )
                        })
                        .collect(),
                    constraints: create_table_constraints_from_meta(&meta),
                })
            }
            _ => None,
        }
    }

    /// Pre-resolve InSubquery expressions within SELECT statements that have CTEs.
    /// This allows CTE-backed subqueries in WHERE clauses to be evaluated before planning.
    fn pre_resolve_cte_subqueries(
        &self,
        stmt: &Statement,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
    ) -> Result<Statement> {
        if let Statement::Select(sel) = stmt
            && !sel.ctes.is_empty()
            && sel.body.where_clause.is_some()
        {
            use crate::executor::resolve_in_subqueries_with_ctes;
            let resolved_where = sel
                .body
                .where_clause
                .as_ref()
                .map(|expr| resolve_in_subqueries_with_ctes(self, expr, params, tx, &sel.ctes))
                .transpose()?;
            let mut new_body = sel.body.clone();
            new_body.where_clause = resolved_where;
            Ok(Statement::Select(contextdb_parser::ast::SelectStatement {
                ctes: sel.ctes.clone(),
                body: new_body,
            }))
        } else {
            Ok(stmt.clone())
        }
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
        let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
        let meta = self.table_meta(table);
        let new_state = meta
            .as_ref()
            .and_then(|m| m.state_machine.as_ref())
            .and_then(|sm| values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);

        let result = self
            .relational
            .upsert(tx, table, conflict_col, values, self.snapshot())?;

        if let (Some(uuid), Some(state), Some(_meta)) =
            (row_uuid, new_state.as_deref(), meta.as_ref())
            && matches!(result, UpsertResult::Updated)
        {
            let already_propagating = self
                .tx_mgr
                .with_write_set(tx, |ws| ws.propagation_in_progress)?;
            if !already_propagating {
                self.tx_mgr
                    .with_write_set(tx, |ws| ws.propagation_in_progress = true)?;
                let propagate_result = self.propagate(tx, table, uuid, state);
                self.tx_mgr
                    .with_write_set(tx, |ws| ws.propagation_in_progress = false)?;
                propagate_result?;
            }
        }

        Ok(result)
    }

    fn propagate(
        &self,
        tx: TxId,
        table: &str,
        row_uuid: uuid::Uuid,
        new_state: &str,
    ) -> Result<()> {
        let snapshot = self.snapshot();
        let metas = self.relational_store().table_meta.read().clone();
        let mut queue: VecDeque<PropagationQueueEntry> = VecDeque::new();
        let mut visited: HashSet<(String, uuid::Uuid)> = HashSet::new();
        let mut abort_violation: Option<Error> = None;
        let ctx = PropagationContext {
            tx,
            snapshot,
            metas: &metas,
        };
        let root = PropagationSource {
            table,
            uuid: row_uuid,
            state: new_state,
            depth: 0,
        };

        self.enqueue_fk_children(&ctx, &mut queue, root);
        self.enqueue_edge_children(&ctx, &mut queue, root)?;
        self.apply_vector_exclusions(&ctx, root)?;

        while let Some(entry) = queue.pop_front() {
            if !visited.insert((entry.table.clone(), entry.uuid)) {
                continue;
            }

            let Some(meta) = metas.get(&entry.table) else {
                continue;
            };

            let Some(state_machine) = &meta.state_machine else {
                let msg = format!(
                    "warning: propagation target table {} has no state machine",
                    entry.table
                );
                eprintln!("{msg}");
                if entry.abort_on_failure && abort_violation.is_none() {
                    abort_violation = Some(Error::PropagationAborted {
                        table: entry.table.clone(),
                        column: String::new(),
                        from: String::new(),
                        to: entry.target_state.clone(),
                    });
                }
                continue;
            };

            let state_column = state_machine.column.clone();
            let Some(existing) = self.relational.point_lookup_with_tx(
                Some(tx),
                &entry.table,
                "id",
                &Value::Uuid(entry.uuid),
                snapshot,
            )?
            else {
                continue;
            };

            let from_state = existing
                .values
                .get(&state_column)
                .and_then(Value::as_text)
                .unwrap_or("")
                .to_string();

            let mut next_values = existing.values.clone();
            next_values.insert(
                state_column.clone(),
                Value::Text(entry.target_state.clone()),
            );

            let upsert_outcome =
                self.relational
                    .upsert(tx, &entry.table, "id", next_values, snapshot);

            let reached_state = match upsert_outcome {
                Ok(UpsertResult::Updated) => entry.target_state.as_str(),
                Ok(UpsertResult::NoOp) | Ok(UpsertResult::Inserted) => continue,
                Err(Error::InvalidStateTransition(_)) => {
                    eprintln!(
                        "warning: skipped invalid propagated transition {}.{} {} -> {}",
                        entry.table, state_column, from_state, entry.target_state
                    );
                    if entry.abort_on_failure && abort_violation.is_none() {
                        abort_violation = Some(Error::PropagationAborted {
                            table: entry.table.clone(),
                            column: state_column.clone(),
                            from: from_state,
                            to: entry.target_state.clone(),
                        });
                    }
                    continue;
                }
                Err(err) => return Err(err),
            };

            self.enqueue_edge_children(
                &ctx,
                &mut queue,
                PropagationSource {
                    table: &entry.table,
                    uuid: entry.uuid,
                    state: reached_state,
                    depth: entry.depth,
                },
            )?;
            self.apply_vector_exclusions(
                &ctx,
                PropagationSource {
                    table: &entry.table,
                    uuid: entry.uuid,
                    state: reached_state,
                    depth: entry.depth,
                },
            )?;

            self.enqueue_fk_children(
                &ctx,
                &mut queue,
                PropagationSource {
                    table: &entry.table,
                    uuid: entry.uuid,
                    state: reached_state,
                    depth: entry.depth,
                },
            );
        }

        if let Some(err) = abort_violation {
            return Err(err);
        }

        Ok(())
    }

    fn enqueue_fk_children(
        &self,
        ctx: &PropagationContext<'_>,
        queue: &mut VecDeque<PropagationQueueEntry>,
        source: PropagationSource<'_>,
    ) {
        for (owner_table, owner_meta) in ctx.metas {
            for rule in &owner_meta.propagation_rules {
                let PropagationRule::ForeignKey {
                    fk_column,
                    referenced_table,
                    trigger_state,
                    target_state,
                    max_depth,
                    abort_on_failure,
                    ..
                } = rule
                else {
                    continue;
                };

                if referenced_table != source.table || trigger_state != source.state {
                    continue;
                }

                if source.depth >= *max_depth {
                    continue;
                }

                let rows = match self.relational.scan_filter_with_tx(
                    Some(ctx.tx),
                    owner_table,
                    ctx.snapshot,
                    &|row| row.values.get(fk_column) == Some(&Value::Uuid(source.uuid)),
                ) {
                    Ok(rows) => rows,
                    Err(err) => {
                        eprintln!(
                            "warning: propagation scan failed for {owner_table}.{fk_column}: {err}"
                        );
                        continue;
                    }
                };

                for row in rows {
                    if let Some(id) = row.values.get("id").and_then(Value::as_uuid).copied() {
                        queue.push_back(PropagationQueueEntry {
                            table: owner_table.clone(),
                            uuid: id,
                            target_state: target_state.clone(),
                            depth: source.depth + 1,
                            abort_on_failure: *abort_on_failure,
                        });
                    }
                }
            }
        }
    }

    fn enqueue_edge_children(
        &self,
        ctx: &PropagationContext<'_>,
        queue: &mut VecDeque<PropagationQueueEntry>,
        source: PropagationSource<'_>,
    ) -> Result<()> {
        let Some(meta) = ctx.metas.get(source.table) else {
            return Ok(());
        };

        for rule in &meta.propagation_rules {
            let PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } = rule
            else {
                continue;
            };

            if trigger_state != source.state || source.depth >= *max_depth {
                continue;
            }

            let bfs = self.query_bfs(
                source.uuid,
                Some(std::slice::from_ref(edge_type)),
                *direction,
                1,
                ctx.snapshot,
            )?;

            for node in bfs.nodes {
                if self
                    .relational
                    .point_lookup_with_tx(
                        Some(ctx.tx),
                        source.table,
                        "id",
                        &Value::Uuid(node.id),
                        ctx.snapshot,
                    )?
                    .is_some()
                {
                    queue.push_back(PropagationQueueEntry {
                        table: source.table.to_string(),
                        uuid: node.id,
                        target_state: target_state.clone(),
                        depth: source.depth + 1,
                        abort_on_failure: *abort_on_failure,
                    });
                }
            }
        }

        Ok(())
    }

    fn apply_vector_exclusions(
        &self,
        ctx: &PropagationContext<'_>,
        source: PropagationSource<'_>,
    ) -> Result<()> {
        let Some(meta) = ctx.metas.get(source.table) else {
            return Ok(());
        };

        for rule in &meta.propagation_rules {
            let PropagationRule::VectorExclusion { trigger_state } = rule else {
                continue;
            };
            if trigger_state != source.state {
                continue;
            }
            if let Some(row) = self.relational.point_lookup_with_tx(
                Some(ctx.tx),
                source.table,
                "id",
                &Value::Uuid(source.uuid),
                ctx.snapshot,
            )? {
                self.delete_vector(ctx.tx, row.row_id)?;
            }
        }

        Ok(())
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

    pub fn edge_count(
        &self,
        source: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<usize> {
        Ok(self.graph.edge_count(source, edge_type, snapshot))
    }

    pub fn get_edge_properties(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<HashMap<String, Value>>> {
        let props = self
            .graph_store
            .forward_adj
            .read()
            .get(&source)
            .and_then(|entries| {
                entries
                    .iter()
                    .rev()
                    .find(|entry| {
                        entry.target == target
                            && entry.edge_type == edge_type
                            && entry.visible_at(snapshot)
                    })
                    .map(|entry| entry.properties.clone())
            });
        Ok(props)
    }

    pub fn insert_vector(&self, tx: TxId, row_id: RowId, vector: Vec<f32>) -> Result<()> {
        self.vector.insert_vector(tx, row_id, vector)
    }

    pub fn delete_vector(&self, tx: TxId, row_id: RowId) -> Result<()> {
        self.vector.delete_vector(tx, row_id)
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

    pub fn open_memory_with_plugin(plugin: Arc<dyn DatabasePlugin>) -> Result<Self> {
        let relational = Arc::new(RelationalStore::new());
        let graph = Arc::new(GraphStore::new());
        let hnsw = Arc::new(OnceLock::new());
        let vector = Arc::new(VectorStore::new(hnsw.clone()));
        let change_log = Arc::new(RwLock::new(Vec::new()));
        let ddl_log = Arc::new(RwLock::new(Vec::new()));
        let store: DynStore = Box::new(CompositeStore::new(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            ddl_log.clone(),
        ));
        let tx_mgr = Arc::new(TxManager::new(store));
        let db = Self {
            tx_mgr: tx_mgr.clone(),
            relational_store: relational.clone(),
            graph_store: graph.clone(),
            vector_store: vector.clone(),
            change_log,
            ddl_log,
            persistence: None,
            relational: MemRelationalExecutor::new(relational, tx_mgr.clone()),
            graph: MemGraphExecutor::new(graph, tx_mgr.clone()),
            vector: MemVectorExecutor::new(vector, tx_mgr.clone(), hnsw),
            session_tx: Mutex::new(None),
            instance_id: uuid::Uuid::new_v4(),
            plugin,
        };
        maybe_prebuild_hnsw(&db.vector_store);
        db.plugin.on_open()?;
        Ok(db)
    }

    pub fn close(&self) -> Result<()> {
        let tx = self.session_tx.lock().take();
        if let Some(tx) = tx {
            self.rollback(tx)?;
        }
        if let Some(persistence) = &self.persistence {
            persistence.close();
        }
        self.plugin.on_close()
    }

    pub fn plugin(&self) -> &dyn DatabasePlugin {
        self.plugin.as_ref()
    }

    pub fn plugin_health(&self) -> PluginHealth {
        self.plugin.health()
    }

    pub fn plugin_describe(&self) -> serde_json::Value {
        self.plugin.describe()
    }

    pub(crate) fn graph(&self) -> &MemGraphExecutor<DynStore> {
        &self.graph
    }

    pub(crate) fn relational_store(&self) -> &Arc<RelationalStore> {
        &self.relational_store
    }

    pub(crate) fn next_lsn_for_ddl(&self) -> u64 {
        self.tx_mgr.next_lsn()
    }

    pub(crate) fn log_create_table_ddl(&self, name: &str, meta: &TableMeta, lsn: u64) {
        self.ddl_log
            .write()
            .push((lsn, ddl_change_from_meta(name, meta)));
    }

    pub(crate) fn log_drop_table_ddl(&self, name: &str, lsn: u64) {
        self.ddl_log.write().push((
            lsn,
            DdlChange::DropTable {
                name: name.to_string(),
            },
        ));
    }

    pub(crate) fn log_alter_table_ddl(&self, name: &str, meta: &TableMeta, lsn: u64) {
        self.ddl_log.write().push((
            lsn,
            DdlChange::AlterTable {
                name: name.to_string(),
                columns: meta
                    .columns
                    .iter()
                    .map(|c| {
                        (
                            c.name.clone(),
                            sql_type_for_meta_column(c, &meta.propagation_rules),
                        )
                    })
                    .collect(),
                constraints: create_table_constraints_from_meta(meta),
            },
        ));
    }

    pub(crate) fn persist_table_meta(&self, name: &str, meta: &TableMeta) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.flush_table_meta(name, meta)?;
        }
        Ok(())
    }

    pub(crate) fn persist_table_rows(&self, name: &str) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            let tables = self.relational_store.tables.read();
            if let Some(rows) = tables.get(name) {
                persistence.rewrite_table_rows(name, rows)?;
            }
        }
        Ok(())
    }

    pub(crate) fn remove_persisted_table(&self, name: &str) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.remove_table_meta(name)?;
            persistence.remove_table_data(name)?;
        }
        Ok(())
    }

    pub fn change_log_since(&self, since_lsn: u64) -> Vec<ChangeLogEntry> {
        let log = self.change_log.read();
        let start = log.partition_point(|e| e.lsn() <= since_lsn);
        log[start..].to_vec()
    }

    pub fn ddl_log_since(&self, since_lsn: u64) -> Vec<DdlChange> {
        let ddl = self.ddl_log.read();
        let start = ddl.partition_point(|(lsn, _)| *lsn <= since_lsn);
        ddl[start..].iter().map(|(_, c)| c.clone()).collect()
    }

    /// Builds a complete snapshot of all live data as a ChangeSet.
    /// Used as fallback when change_log/ddl_log cannot serve a watermark.
    fn full_state_snapshot(&self) -> ChangeSet {
        let mut rows = Vec::new();
        let mut edges = Vec::new();
        let mut vectors = Vec::new();
        let mut ddl = Vec::new();

        let meta_guard = self.relational_store.table_meta.read();
        let tables_guard = self.relational_store.tables.read();

        // DDL
        for (name, meta) in meta_guard.iter() {
            ddl.push(ddl_change_from_meta(name, meta));
        }

        // Rows (live only) — collect row_ids that have live rows for orphan vector filtering
        let mut live_row_ids: HashSet<RowId> = HashSet::new();
        for (table_name, table_rows) in tables_guard.iter() {
            let meta = match meta_guard.get(table_name) {
                Some(m) => m,
                None => continue,
            };
            let key_col = meta.natural_key_column.clone().unwrap_or_else(|| {
                if meta
                    .columns
                    .iter()
                    .any(|c| c.name == "id" && c.column_type == ColumnType::Uuid)
                {
                    "id".to_string()
                } else {
                    String::new()
                }
            });
            if key_col.is_empty() {
                continue;
            }
            for row in table_rows.iter().filter(|r| r.deleted_tx.is_none()) {
                let key_val = match row.values.get(&key_col) {
                    Some(v) => v.clone(),
                    None => continue,
                };
                live_row_ids.insert(row.row_id);
                rows.push(RowChange {
                    table: table_name.clone(),
                    natural_key: NaturalKey {
                        column: key_col.clone(),
                        value: key_val,
                    },
                    values: row.values.clone(),
                    deleted: false,
                    lsn: row.lsn,
                });
            }
        }

        drop(tables_guard);
        drop(meta_guard);

        // Edges (live only)
        let fwd = self.graph_store.forward_adj.read();
        for (_source, entries) in fwd.iter() {
            for entry in entries.iter().filter(|e| e.deleted_tx.is_none()) {
                edges.push(EdgeChange {
                    source: entry.source,
                    target: entry.target,
                    edge_type: entry.edge_type.clone(),
                    properties: entry.properties.clone(),
                    lsn: entry.lsn,
                });
            }
        }
        drop(fwd);

        // Vectors (live only, skip orphans, first entry per row_id)
        let mut seen_vector_rows: HashSet<RowId> = HashSet::new();
        let vecs = self.vector_store.vectors.read();
        for entry in vecs.iter().filter(|v| v.deleted_tx.is_none()) {
            if !live_row_ids.contains(&entry.row_id) {
                continue; // skip orphan vectors
            }
            if !seen_vector_rows.insert(entry.row_id) {
                continue; // first live entry per row_id only
            }
            vectors.push(VectorChange {
                row_id: entry.row_id,
                vector: entry.vector.clone(),
                lsn: entry.lsn,
            });
        }
        drop(vecs);

        ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
        }
    }

    /// Extracts changes from this database since the given LSN.
    pub fn changes_since(&self, since_lsn: u64) -> ChangeSet {
        // Future watermark guard
        if since_lsn > self.current_lsn() {
            return ChangeSet::default();
        }

        // Check if the ephemeral logs can serve the requested watermark.
        // After restart, both logs are empty but stores may have data — fall back to snapshot.
        let log = self.change_log.read();
        let change_first_lsn = log.first().map(|e| e.lsn());
        let change_log_empty = log.is_empty();
        drop(log);

        let ddl = self.ddl_log.read();
        let ddl_first_lsn = ddl.first().map(|(lsn, _)| *lsn);
        let ddl_log_empty = ddl.is_empty();
        drop(ddl);

        let has_table_data = !self
            .relational_store
            .tables
            .read()
            .values()
            .all(|rows| rows.is_empty());
        let has_table_meta = !self.relational_store.table_meta.read().is_empty();

        // If both logs are empty but stores have data → post-restart, fall back
        if change_log_empty && ddl_log_empty && (has_table_data || has_table_meta) {
            return self.full_state_snapshot();
        }

        // If logs have entries, check the minimum first-LSN across both covers since_lsn
        let min_first_lsn = match (change_first_lsn, ddl_first_lsn) {
            (Some(c), Some(d)) => Some(c.min(d)),
            (Some(c), None) => Some(c),
            (None, Some(d)) => Some(d),
            (None, None) => None, // both empty, stores empty — nothing to serve
        };

        if min_first_lsn.is_some_and(|min_lsn| min_lsn > since_lsn + 1) {
            // Log doesn't cover since_lsn — fall back to snapshot
            return self.full_state_snapshot();
        }

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
                            deleted: false,
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
                        deleted: true,
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
            .filter(|r| !r.deleted)
            .map(|r| {
                (
                    r.table.clone(),
                    r.natural_key.column.clone(),
                    format!("{:?}", r.natural_key.value),
                )
            })
            .collect();
        rows.retain(|r| {
            if r.deleted {
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
        mut changes: ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<ApplyResult> {
        self.plugin.on_sync_pull(&mut changes)?;

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
                        // Check for schema divergence
                        if let Some(local_meta) = self.table_meta(&name) {
                            let local_col_count = local_meta.columns.len();
                            if local_col_count != columns.len() {
                                tracing::warn!(
                                    table = name,
                                    local_columns = local_col_count,
                                    remote_columns = columns.len(),
                                    "schema divergence detected — keeping local schema"
                                );
                            }
                        }
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
                DdlChange::DropTable { name } => {
                    if self.table_meta(&name).is_some() {
                        self.relational_store().drop_table(&name);
                        self.remove_persisted_table(&name)?;
                    }
                }
                DdlChange::AlterTable {
                    name,
                    columns,
                    constraints,
                } => {
                    if self.table_meta(&name).is_none() {
                        continue;
                    }
                    self.relational_store().table_meta.write().remove(&name);
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
                self.commit_with_source(tx, CommitSource::SyncPull)?;
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
            let is_delete = row.deleted;

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
                self.commit_with_source(tx, CommitSource::SyncPull)?;
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

            self.commit_with_source(tx, CommitSource::SyncPull)?;
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

        let existing_vector_rows: HashSet<RowId> = self
            .vector_store
            .vectors
            .read()
            .iter()
            .filter(|v| v.deleted_tx.is_none())
            .map(|v| v.row_id)
            .collect();

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
                if existing_vector_rows.contains(&local_row_id) {
                    continue;
                }
                let _ = self.insert_vector(tx, local_row_id, vector.vector);
            }
        }

        self.commit_with_source(tx, CommitSource::SyncPull)?;
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
        self.graph_store
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
        self.vector_store
            .vectors
            .read()
            .iter()
            .find(|v| v.row_id == row_id && v.lsn == lsn)
            .map(|v| v.vector.clone())
    }
}

fn strip_internal_row_id(mut qr: QueryResult) -> QueryResult {
    if let Some(pos) = qr.columns.iter().position(|c| c == "row_id") {
        qr.columns.remove(pos);
        for row in &mut qr.rows {
            if pos < row.len() {
                row.remove(pos);
            }
        }
    }
    qr
}

fn query_outcome_from_result(result: &Result<QueryResult>) -> QueryOutcome {
    match result {
        Ok(query_result) => QueryOutcome::Success {
            row_count: if query_result.rows.is_empty() {
                query_result.rows_affected as usize
            } else {
                query_result.rows.len()
            },
        },
        Err(error) => QueryOutcome::Error {
            error: error.to_string(),
        },
    }
}

fn maybe_prebuild_hnsw(vector_store: &VectorStore) {
    if vector_store.vector_count() >= 1000 {
        let entries = vector_store.all_entries();
        let dim = vector_store.dimension().unwrap_or(0);
        let hnsw_opt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            HnswIndex::new(&entries, dim)
        }))
        .ok();
        let _ = vector_store.hnsw.set(RwLock::new(hnsw_opt));
    }
}

fn max_tx_across_all(
    relational: &RelationalStore,
    graph: &GraphStore,
    vector: &VectorStore,
) -> TxId {
    let relational_max = relational
        .tables
        .read()
        .values()
        .flat_map(|rows| rows.iter())
        .flat_map(|row| std::iter::once(row.created_tx).chain(row.deleted_tx))
        .max()
        .unwrap_or(0);
    let graph_max = graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter())
        .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx))
        .max()
        .unwrap_or(0);
    let vector_max = vector
        .vectors
        .read()
        .iter()
        .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx))
        .max()
        .unwrap_or(0);

    relational_max.max(graph_max).max(vector_max)
}

fn max_lsn_across_all(
    relational: &RelationalStore,
    graph: &GraphStore,
    vector: &VectorStore,
) -> u64 {
    let relational_max = relational
        .tables
        .read()
        .values()
        .flat_map(|rows| rows.iter().map(|row| row.lsn))
        .max()
        .unwrap_or(0);
    let graph_max = graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter().map(|entry| entry.lsn))
        .max()
        .unwrap_or(0);
    let vector_max = vector
        .vectors
        .read()
        .iter()
        .map(|entry| entry.lsn)
        .max()
        .unwrap_or(0);

    relational_max.max(graph_max).max(vector_max)
}

fn ddl_change_from_create_table(ct: &CreateTable) -> DdlChange {
    DdlChange::CreateTable {
        name: ct.name.clone(),
        columns: ct
            .columns
            .iter()
            .map(|col| {
                (
                    col.name.clone(),
                    sql_type_for_ast_column(col, &ct.propagation_rules),
                )
            })
            .collect(),
        constraints: create_table_constraints_from_ast(ct),
    }
}

fn ddl_change_from_meta(name: &str, meta: &TableMeta) -> DdlChange {
    DdlChange::CreateTable {
        name: name.to_string(),
        columns: meta
            .columns
            .iter()
            .map(|col| {
                (
                    col.name.clone(),
                    sql_type_for_meta_column(col, &meta.propagation_rules),
                )
            })
            .collect(),
        constraints: create_table_constraints_from_meta(meta),
    }
}

fn sql_type_for_ast(data_type: &DataType) -> String {
    match data_type {
        DataType::Uuid => "UUID".to_string(),
        DataType::Text => "TEXT".to_string(),
        DataType::Integer => "INTEGER".to_string(),
        DataType::Real => "REAL".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Json => "JSON".to_string(),
        DataType::Vector(dim) => format!("VECTOR({dim})"),
    }
}

fn sql_type_for_ast_column(
    col: &contextdb_parser::ast::ColumnDef,
    _rules: &[contextdb_parser::ast::AstPropagationRule],
) -> String {
    let mut ty = sql_type_for_ast(&col.data_type);
    if let Some(reference) = &col.references {
        ty.push_str(&format!(
            " REFERENCES {}({})",
            reference.table, reference.column
        ));
        for rule in &reference.propagation_rules {
            if let contextdb_parser::ast::AstPropagationRule::FkState {
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } = rule
            {
                ty.push_str(&format!(
                    " ON STATE {} PROPAGATE SET {}",
                    trigger_state, target_state
                ));
                if max_depth.unwrap_or(10) != 10 {
                    ty.push_str(&format!(" MAX DEPTH {}", max_depth.unwrap_or(10)));
                }
                if *abort_on_failure {
                    ty.push_str(" ABORT ON FAILURE");
                }
            }
        }
    }
    ty
}

fn sql_type_for_meta_column(col: &contextdb_core::ColumnDef, rules: &[PropagationRule]) -> String {
    let mut ty = match col.column_type {
        ColumnType::Integer => "INTEGER".to_string(),
        ColumnType::Real => "REAL".to_string(),
        ColumnType::Text => "TEXT".to_string(),
        ColumnType::Boolean => "BOOLEAN".to_string(),
        ColumnType::Json => "JSON".to_string(),
        ColumnType::Uuid => "UUID".to_string(),
        ColumnType::Vector(dim) => format!("VECTOR({dim})"),
    };

    let fk_rules = rules
        .iter()
        .filter_map(|rule| match rule {
            PropagationRule::ForeignKey {
                fk_column,
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } if fk_column == &col.name => Some((
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                *max_depth,
                *abort_on_failure,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();

    if let Some((referenced_table, referenced_column, ..)) = fk_rules.first() {
        ty.push_str(&format!(
            " REFERENCES {}({})",
            referenced_table, referenced_column
        ));
        for (_, _, trigger_state, target_state, max_depth, abort_on_failure) in fk_rules {
            ty.push_str(&format!(
                " ON STATE {} PROPAGATE SET {}",
                trigger_state, target_state
            ));
            if max_depth != 10 {
                ty.push_str(&format!(" MAX DEPTH {max_depth}"));
            }
            if abort_on_failure {
                ty.push_str(" ABORT ON FAILURE");
            }
        }
    }

    ty
}

fn create_table_constraints_from_ast(ct: &CreateTable) -> Vec<String> {
    let mut constraints = Vec::new();

    if ct.immutable {
        constraints.push("IMMUTABLE".to_string());
    }

    if let Some(sm) = &ct.state_machine {
        let transitions = sm
            .transitions
            .iter()
            .map(|(from, tos)| format!("{from} -> [{}]", tos.join(", ")))
            .collect::<Vec<_>>()
            .join(", ");
        constraints.push(format!("STATE MACHINE ({}: {})", sm.column, transitions));
    }

    if !ct.dag_edge_types.is_empty() {
        let edge_types = ct
            .dag_edge_types
            .iter()
            .map(|edge_type| format!("'{edge_type}'"))
            .collect::<Vec<_>>()
            .join(", ");
        constraints.push(format!("DAG({edge_types})"));
    }

    for rule in &ct.propagation_rules {
        match rule {
            contextdb_parser::ast::AstPropagationRule::EdgeState {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => {
                let mut clause = format!(
                    "PROPAGATE ON EDGE {} {} STATE {} SET {}",
                    edge_type, direction, trigger_state, target_state
                );
                if max_depth.unwrap_or(10) != 10 {
                    clause.push_str(&format!(" MAX DEPTH {}", max_depth.unwrap_or(10)));
                }
                if *abort_on_failure {
                    clause.push_str(" ABORT ON FAILURE");
                }
                constraints.push(clause);
            }
            contextdb_parser::ast::AstPropagationRule::VectorExclusion { trigger_state } => {
                constraints.push(format!(
                    "PROPAGATE ON STATE {} EXCLUDE VECTOR",
                    trigger_state
                ));
            }
            contextdb_parser::ast::AstPropagationRule::FkState { .. } => {}
        }
    }

    constraints
}

fn create_table_constraints_from_meta(meta: &TableMeta) -> Vec<String> {
    let mut constraints = Vec::new();

    if meta.immutable {
        constraints.push("IMMUTABLE".to_string());
    }

    if let Some(sm) = &meta.state_machine {
        let states = sm
            .transitions
            .iter()
            .map(|(from, to)| format!("{from} -> [{}]", to.join(", ")))
            .collect::<Vec<_>>()
            .join(", ");
        constraints.push(format!("STATE MACHINE ({}: {})", sm.column, states));
    }

    if !meta.dag_edge_types.is_empty() {
        let edge_types = meta
            .dag_edge_types
            .iter()
            .map(|edge_type| format!("'{edge_type}'"))
            .collect::<Vec<_>>()
            .join(", ");
        constraints.push(format!("DAG({edge_types})"));
    }

    for rule in &meta.propagation_rules {
        match rule {
            PropagationRule::Edge {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => {
                let dir = match direction {
                    Direction::Incoming => "INCOMING",
                    Direction::Outgoing => "OUTGOING",
                    Direction::Both => "BOTH",
                };
                let mut clause = format!(
                    "PROPAGATE ON EDGE {} {} STATE {} SET {}",
                    edge_type, dir, trigger_state, target_state
                );
                if *max_depth != 10 {
                    clause.push_str(&format!(" MAX DEPTH {max_depth}"));
                }
                if *abort_on_failure {
                    clause.push_str(" ABORT ON FAILURE");
                }
                constraints.push(clause);
            }
            PropagationRule::VectorExclusion { trigger_state } => {
                constraints.push(format!(
                    "PROPAGATE ON STATE {} EXCLUDE VECTOR",
                    trigger_state
                ));
            }
            PropagationRule::ForeignKey { .. } => {}
        }
    }

    constraints
}
