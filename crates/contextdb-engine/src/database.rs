use crate::composite_store::{ChangeLogEntry, CompositeStore};
use crate::executor::execute_plan;
use crate::persistence::RedbPersistence;
use crate::persistent_store::PersistentCompositeStore;
use crate::plugin::{
    CommitEvent, CommitSource, CorePlugin, DatabasePlugin, PluginHealth, QueryOutcome,
    SubscriptionMetrics,
};
use crate::rank_formula::{FormulaEvalError, RankFormula};
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
use contextdb_vector::{HnswGraphStats, HnswIndex, MemVectorExecutor, VectorStore};
use parking_lot::{Mutex, RwLock};
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type DynStore = Box<dyn WriteSetApplicator>;
const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 64;
const MAX_STATEMENT_CACHE_ENTRIES: usize = 1024;
// redb may need a small metadata page on the next write, especially for a new
// file with the format metadata table. Keep the disk-limit error deterministic
// instead of starting a write that cannot commit cleanly.
const MIN_DISK_WRITE_HEADROOM_BYTES: u64 = 1024;

#[derive(Debug, Clone)]
pub struct IndexCandidate {
    pub name: String,
    pub rejected_reason: std::borrow::Cow<'static, str>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryTrace {
    pub physical_plan: &'static str,
    pub index_used: Option<String>,
    pub predicates_pushed: smallvec::SmallVec<[std::borrow::Cow<'static, str>; 4]>,
    pub indexes_considered: smallvec::SmallVec<[IndexCandidate; 4]>,
    pub sort_elided: bool,
}

impl QueryTrace {
    /// Stub default: scan-labeled trace with no plan data. The no-op writes
    /// this everywhere. Impl must replace construction sites with real plan
    /// inspection.
    pub fn scan() -> Self {
        Self {
            physical_plan: "Scan",
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct CascadeReport {
    pub dropped_indexes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
    pub trace: QueryTrace,
    pub cascade: Option<CascadeReport>,
}

#[derive(Debug, Clone)]
pub struct SemanticQuery {
    pub table: String,
    pub vector_column: String,
    pub query: Vec<f32>,
    pub limit: usize,
    pub sort_key: Option<String>,
    pub min_similarity: Option<f32>,
    pub where_clause: Option<String>,
}

impl SemanticQuery {
    pub fn new(
        table: impl Into<String>,
        vector_column: impl Into<String>,
        query: Vec<f32>,
        limit: usize,
    ) -> Self {
        Self {
            table: table.into(),
            vector_column: vector_column.into(),
            query,
            limit,
            sort_key: None,
            min_similarity: None,
            where_clause: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub row_id: RowId,
    pub values: HashMap<String, Value>,
    pub vector_score: f32,
    /// Always populated. Equals the formula's computed value when the search
    /// uses a rank policy via `sort_key`, and equals `vector_score` (raw
    /// cosine) in all other cases. Callers never unwrap this field.
    pub rank: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronAuditEntry {
    pub schedule_name: String,
    pub kind: CronAuditKind,
    pub at_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronAuditKind {
    Fired,
    MissedSkipped,
    MissedCaughtUp { ticks: u32 },
    Failed(String),
}

#[derive(Debug)]
pub struct CronPauseGuard {
    _private: (),
}

#[derive(Debug)]
pub struct ApplyPhasePauseGuard {
    _private: (),
}

impl ApplyPhasePauseGuard {
    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        let _ = timeout;
        false
    }

    pub fn release(&self) {}
}

#[derive(Debug, Clone, PartialEq)]
pub struct SinkEvent {
    pub event_type: String,
    pub table: String,
    pub row_values: HashMap<String, Value>,
    pub severity: String,
    pub at_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkError {
    Transient(String),
    Permanent(String),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkMetrics {
    pub delivered: u64,
    pub queued: u64,
    pub retried: u64,
    pub permanent_failures: u64,
}

#[derive(Debug, Clone)]
struct CachedStatement {
    stmt: Statement,
    plan: PhysicalPlan,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            trace: QueryTrace::scan(),
            cascade: None,
        }
    }

    pub fn empty_with_affected(rows_affected: u64) -> Self {
        Self {
            columns: vec![],
            rows: vec![],
            rows_affected,
            trace: QueryTrace::scan(),
            cascade: None,
        }
    }
}

thread_local! {
    static SNAPSHOT_OVERRIDE: std::cell::RefCell<Option<SnapshotId>> =
        const { std::cell::RefCell::new(None) };
}

pub struct Database {
    tx_mgr: Arc<TxManager<DynStore>>,
    relational_store: Arc<RelationalStore>,
    graph_store: Arc<GraphStore>,
    vector_store: Arc<VectorStore>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    persistence: Option<Arc<RedbPersistence>>,
    relational: MemRelationalExecutor<DynStore>,
    graph: MemGraphExecutor<DynStore>,
    vector: MemVectorExecutor<DynStore>,
    session_tx: Mutex<Option<TxId>>,
    instance_id: uuid::Uuid,
    plugin: Arc<dyn DatabasePlugin>,
    accountant: Arc<MemoryAccountant>,
    conflict_policies: RwLock<ConflictPolicies>,
    subscriptions: Mutex<SubscriptionState>,
    pruning_runtime: Mutex<PruningRuntime>,
    pruning_guard: Arc<Mutex<()>>,
    disk_limit: AtomicU64,
    disk_limit_startup_ceiling: AtomicU64,
    sync_watermark: Arc<AtomicLsn>,
    closed: AtomicBool,
    rows_examined: AtomicU64,
    statement_cache: RwLock<HashMap<String, Arc<CachedStatement>>>,
    rank_formula_cache: RwLock<HashMap<(String, String), Arc<RankFormula>>>,
    rank_policy_eval_count: AtomicU64,
    rank_policy_formula_parse_count: AtomicU64,
    corrupt_joined_values: RwLock<HashSet<(String, RowId, String)>>,
}

pub(crate) enum InsertRowResult {
    Inserted(RowId),
    NoOp,
}

#[derive(Debug, Default)]
pub(crate) struct IndexScanTxOverlay {
    pub deleted_row_ids: std::collections::HashSet<RowId>,
    pub matching_inserts: Vec<VersionedRow>,
}

enum RowConstraintCheck {
    Valid,
    DuplicateUniqueNoOp,
}

enum ConstraintProbe {
    NoIndex,
    NoMatch,
    Match(RowId),
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

#[derive(Debug)]
struct SubscriptionState {
    subscribers: Vec<SyncSender<CommitEvent>>,
    events_sent: u64,
    events_dropped: u64,
}

impl SubscriptionState {
    fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            events_sent: 0,
            events_dropped: 0,
        }
    }
}

#[derive(Debug)]
struct PruningRuntime {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl PruningRuntime {
    fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    fn build_db(
        tx_mgr: Arc<TxManager<DynStore>>,
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector_store: Arc<VectorStore>,
        hnsw: Arc<OnceLock<parking_lot::RwLock<Option<HnswIndex>>>>,
        change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
        ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
        persistence: Option<Arc<RedbPersistence>>,
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
        disk_limit: Option<u64>,
        disk_limit_startup_ceiling: Option<u64>,
    ) -> Self {
        Self {
            tx_mgr: tx_mgr.clone(),
            relational_store: relational.clone(),
            graph_store: graph.clone(),
            vector_store: vector_store.clone(),
            change_log,
            ddl_log,
            persistence,
            relational: MemRelationalExecutor::new(relational, tx_mgr.clone()),
            graph: MemGraphExecutor::new(graph, tx_mgr.clone()),
            vector: MemVectorExecutor::new_with_accountant(
                vector_store,
                tx_mgr.clone(),
                hnsw,
                accountant.clone(),
            ),
            session_tx: Mutex::new(None),
            instance_id: uuid::Uuid::new_v4(),
            plugin,
            accountant,
            conflict_policies: RwLock::new(ConflictPolicies::uniform(ConflictPolicy::LatestWins)),
            subscriptions: Mutex::new(SubscriptionState::new()),
            pruning_runtime: Mutex::new(PruningRuntime::new()),
            pruning_guard: Arc::new(Mutex::new(())),
            disk_limit: AtomicU64::new(disk_limit.unwrap_or(0)),
            disk_limit_startup_ceiling: AtomicU64::new(disk_limit_startup_ceiling.unwrap_or(0)),
            sync_watermark: Arc::new(AtomicLsn::new(Lsn(0))),
            closed: AtomicBool::new(false),
            rows_examined: AtomicU64::new(0),
            statement_cache: RwLock::new(HashMap::new()),
            rank_formula_cache: RwLock::new(HashMap::new()),
            rank_policy_eval_count: AtomicU64::new(0),
            rank_policy_formula_parse_count: AtomicU64::new(0),
            corrupt_joined_values: RwLock::new(HashSet::new()),
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(
            path,
            Arc::new(CorePlugin),
            Arc::new(MemoryAccountant::no_limit()),
        )
    }

    pub fn open_memory() -> Self {
        Self::open_memory_with_plugin_and_accountant(
            Arc::new(CorePlugin),
            Arc::new(MemoryAccountant::no_limit()),
        )
        .expect("failed to open in-memory database")
    }

    pub fn open_with_contexts<P: AsRef<Path>>(
        path: P,
        contexts: std::collections::BTreeSet<contextdb_core::types::ContextId>,
    ) -> Result<Self> {
        let _ = contexts;
        Self::open(path)
    }

    pub fn open_memory_with_contexts(
        contexts: std::collections::BTreeSet<contextdb_core::types::ContextId>,
    ) -> Self {
        let _ = contexts;
        Self::open_memory()
    }

    pub fn open_with_scope_labels<P: AsRef<Path>>(
        path: P,
        labels: std::collections::BTreeSet<contextdb_core::types::ScopeLabel>,
    ) -> Result<Self> {
        let _ = labels;
        Self::open(path)
    }

    pub fn open_memory_with_scope_labels(
        labels: std::collections::BTreeSet<contextdb_core::types::ScopeLabel>,
    ) -> Self {
        let _ = labels;
        Self::open_memory()
    }

    pub fn open_as_principal<P: AsRef<Path>>(
        path: P,
        principal: contextdb_core::types::Principal,
    ) -> Result<Self> {
        let _ = principal;
        Self::open(path)
    }

    pub fn open_memory_as_principal(principal: contextdb_core::types::Principal) -> Self {
        let _ = principal;
        Self::open_memory()
    }

    pub fn open_with_constraints<P: AsRef<Path>>(
        path: P,
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> Result<Self> {
        let _ = (contexts, scope_labels, principal);
        Self::open(path)
    }

    pub fn open_memory_with_constraints(
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> Self {
        let _ = (contexts, scope_labels, principal);
        Self::open_memory()
    }

    fn open_loaded(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
        mut accountant: Arc<MemoryAccountant>,
        startup_disk_limit: Option<u64>,
    ) -> Result<Self> {
        let path = path.as_ref();
        let persistence = if path.exists() {
            Arc::new(RedbPersistence::open(path)?)
        } else {
            Arc::new(RedbPersistence::create(path)?)
        };
        if accountant.usage().limit.is_none()
            && let Some(limit) = persistence.load_config_value::<usize>("memory_limit")?
        {
            accountant = Arc::new(MemoryAccountant::with_budget(limit));
        }
        let persisted_disk_limit = persistence.load_config_value::<u64>("disk_limit")?;
        let startup_disk_ceiling = startup_disk_limit;
        let effective_disk_limit = match (persisted_disk_limit, startup_disk_limit) {
            (Some(persisted), Some(ceiling)) => Some(persisted.min(ceiling)),
            (Some(persisted), None) => Some(persisted),
            (None, Some(ceiling)) => Some(ceiling),
            (None, None) => None,
        };

        let all_meta = persistence.load_all_table_meta()?;

        let relational = Arc::new(RelationalStore::new());
        for (name, meta) in &all_meta {
            relational.create_table(name, meta.clone());
            // Register EVERY index declared in TableMeta.indexes — this
            // includes auto-indexes (kind=Auto) synthesized at CREATE TABLE
            // time AND user-declared indexes (kind=UserDeclared).
            for decl in &meta.indexes {
                relational.create_index_storage(name, &decl.name, decl.columns.clone());
            }
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
        for (table_name, meta) in &all_meta {
            for column in &meta.columns {
                if let ColumnType::Vector(dimension) = column.column_type {
                    vector.register_index(
                        VectorIndexRef::new(table_name, column.name.clone()),
                        dimension,
                        column.quantization,
                    );
                }
            }
        }
        let loaded_vectors = persistence.load_vectors()?;
        for entry in &loaded_vectors {
            vector.insert_loaded_vector(entry.clone());
        }
        hydrate_relational_vector_values(&relational, &loaded_vectors);

        let max_row_id = relational.max_row_id();
        let max_tx = max_tx_across_all(&relational, &graph, &vector);
        let max_lsn = max_lsn_across_all(&relational, &graph, &vector);
        relational.set_next_row_id(RowId(max_row_id.0.saturating_add(1)));

        let change_log = Arc::new(RwLock::new(persistence.load_change_log()?));
        let ddl_log = Arc::new(RwLock::new(persistence.load_ddl_log()?));
        let composite = CompositeStore::new(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            ddl_log.clone(),
            accountant.clone(),
        );
        let persistent = PersistentCompositeStore::new(composite, persistence.clone());
        let store: DynStore = Box::new(persistent);
        let tx_mgr = Arc::new(TxManager::new_with_counters(
            store,
            TxId(max_tx.0.saturating_add(1)),
            Lsn(max_lsn.0.saturating_add(1)),
            max_tx,
        ));

        let db = Self::build_db(
            tx_mgr,
            relational,
            graph,
            vector,
            hnsw,
            change_log,
            ddl_log,
            Some(persistence),
            plugin,
            accountant,
            effective_disk_limit,
            startup_disk_ceiling,
        );

        for meta in all_meta.values() {
            if !meta.dag_edge_types.is_empty() {
                db.graph.register_dag_edge_types(&meta.dag_edge_types);
            }
        }
        db.rebuild_rank_formula_cache_from_meta(&all_meta)?;

        db.account_loaded_state()?;
        maybe_prebuild_hnsw(&db.vector_store, db.accountant());

        Ok(db)
    }

    fn open_memory_internal(
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
    ) -> Result<Self> {
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
            accountant.clone(),
        ));
        let tx_mgr = Arc::new(TxManager::new(store));

        let db = Self::build_db(
            tx_mgr, relational, graph, vector, hnsw, change_log, ddl_log, None, plugin, accountant,
            None, None,
        );
        maybe_prebuild_hnsw(&db.vector_store, db.accountant());
        Ok(db)
    }

    pub fn begin(&self) -> TxId {
        self.tx_mgr.begin()
    }

    pub fn commit(&self, tx: TxId) -> Result<()> {
        self.commit_with_source(tx, CommitSource::User)
    }

    pub fn rollback(&self, tx: TxId) -> Result<()> {
        let ws = self.tx_mgr.cloned_write_set(tx)?;
        self.release_insert_allocations(&ws);
        self.tx_mgr.rollback(tx)
    }

    pub fn snapshot(&self) -> SnapshotId {
        self.tx_mgr.snapshot()
    }

    pub fn snapshot_at(&self, lsn: Lsn) -> SnapshotId {
        let _ = lsn;
        self.snapshot()
    }

    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        if let Some(cached) = self.cached_statement(sql) {
            let active_tx = *self.session_tx.lock();
            return self.execute_statement_with_plan(
                &cached.stmt,
                sql,
                params,
                active_tx,
                Some(&cached.plan),
            );
        }

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

    fn cached_statement(&self, sql: &str) -> Option<Arc<CachedStatement>> {
        self.statement_cache.read().get(sql).cloned()
    }

    fn cache_statement_if_eligible(&self, sql: &str, stmt: &Statement, plan: &PhysicalPlan) {
        if !Self::is_statement_cache_eligible(stmt, plan) {
            return;
        }

        let mut cache = self.statement_cache.write();
        if cache.contains_key(sql) {
            return;
        }
        if cache.len() >= MAX_STATEMENT_CACHE_ENTRIES {
            return;
        }
        cache.insert(
            sql.to_string(),
            Arc::new(CachedStatement {
                stmt: stmt.clone(),
                plan: plan.clone(),
            }),
        );
    }

    fn is_statement_cache_eligible(stmt: &Statement, plan: &PhysicalPlan) -> bool {
        matches!((stmt, plan), (Statement::Insert(ins), PhysicalPlan::Insert(_))
            if !ins.table.eq_ignore_ascii_case("GRAPH")
                && !ins.table.eq_ignore_ascii_case("__edges"))
    }

    pub(crate) fn clear_statement_cache(&self) {
        self.statement_cache.write().clear();
    }

    #[doc(hidden)]
    pub fn __statement_cache_len(&self) -> usize {
        self.statement_cache.read().len()
    }

    fn execute_autocommit(
        &self,
        plan: &PhysicalPlan,
        params: &HashMap<String, Value>,
    ) -> Result<QueryResult> {
        // Reset per-query rows_examined once at the entry point so every
        // sub-plan (union, CTE, subquery IndexScan) accumulates into the
        // shared counter rather than overwriting prior counts.
        self.__reset_rows_examined();
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
        let vector_index = vector_index_from_plan(&plan);
        if let Some(index) = &vector_index
            && let Some(state) = self.vector_store.try_state(index)
            && state.vector_count() >= 1000
            && state.max_tx() <= TxId::from_snapshot(self.snapshot())
            && state.hnsw_len().is_none()
        {
            let query = vec![0.0_f32; state.dimension()];
            let _ = self.query_vector_strict(index.clone(), &query, 1, None, self.snapshot());
        }
        let mut output = plan.explain();
        let uses_hnsw = vector_index
            .as_ref()
            .is_some_and(|index| self.vector_store.has_hnsw_index_for(index));
        if uses_hnsw {
            output = output.replace("VectorSearch(", "HNSWSearch(");
            output = output.replace("VectorSearch {", "HNSWSearch {");
        } else {
            output = output.replace("VectorSearch(", "VectorSearch(strategy=BruteForce, ");
            output = output.replace("VectorSearch {", "VectorSearch { strategy: BruteForce,");
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
            && let Err(err) = self.validate_foreign_keys_in_tx(tx)
        {
            let _ = self.rollback(tx);
            return Err(err);
        }

        if !ws.is_empty()
            && let Err(err) = self.plugin.pre_commit(&ws, source)
        {
            let _ = self.rollback(tx);
            return Err(err);
        }

        let lsn = self.tx_mgr.commit_with_lsn(tx)?;

        if !ws.is_empty() {
            self.release_delete_allocations(&ws);
            ws.stamp_lsn(lsn);
            self.plugin.post_commit(&ws, source);
            self.publish_commit_event_if_subscribers(&ws, source, lsn);
        }

        Ok(())
    }

    fn build_commit_event(
        ws: &contextdb_tx::WriteSet,
        source: CommitSource,
        lsn: Lsn,
    ) -> CommitEvent {
        let mut tables_changed: Vec<String> = ws
            .relational_inserts
            .iter()
            .map(|(table, _)| table.clone())
            .chain(
                ws.relational_deletes
                    .iter()
                    .map(|(table, _, _)| table.clone()),
            )
            .chain(
                ws.vector_inserts
                    .iter()
                    .map(|entry| entry.index.table.clone()),
            )
            .chain(
                ws.vector_deletes
                    .iter()
                    .map(|(index, _, _)| index.table.clone()),
            )
            .chain(
                ws.vector_moves
                    .iter()
                    .map(|(index, _, _, _)| index.table.clone()),
            )
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        tables_changed.sort();

        CommitEvent {
            source,
            lsn,
            tables_changed,
            row_count: ws.relational_inserts.len()
                + ws.relational_deletes.len()
                + ws.adj_inserts.len()
                + ws.adj_deletes.len()
                + ws.vector_inserts.len()
                + ws.vector_deletes.len()
                + ws.vector_moves.len(),
        }
    }

    fn publish_commit_event_if_subscribers(
        &self,
        ws: &contextdb_tx::WriteSet,
        source: CommitSource,
        lsn: Lsn,
    ) {
        let mut subscriptions = self.subscriptions.lock();
        if subscriptions.subscribers.is_empty() {
            return;
        }
        let event = Self::build_commit_event(ws, source, lsn);
        let subscribers = std::mem::take(&mut subscriptions.subscribers);
        let mut live_subscribers = Vec::with_capacity(subscribers.len());

        for sender in subscribers {
            match sender.try_send(event.clone()) {
                Ok(()) => {
                    subscriptions.events_sent += 1;
                    live_subscribers.push(sender);
                }
                Err(TrySendError::Full(_)) => {
                    subscriptions.events_dropped += 1;
                    live_subscribers.push(sender);
                }
                Err(TrySendError::Disconnected(_)) => {}
            }
        }

        subscriptions.subscribers = live_subscribers;
    }

    fn stop_pruning_thread(&self) {
        let handle = {
            let mut runtime = self.pruning_runtime.lock();
            runtime.shutdown.store(true, Ordering::SeqCst);
            let handle = runtime.handle.take();
            runtime.shutdown = Arc::new(AtomicBool::new(false));
            handle
        };

        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    fn execute_statement(
        &self,
        stmt: &Statement,
        sql: &str,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
    ) -> Result<QueryResult> {
        self.execute_statement_with_plan(stmt, sql, params, tx, None)
    }

    fn execute_statement_with_plan(
        &self,
        stmt: &Statement,
        sql: &str,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        cached_plan: Option<&PhysicalPlan>,
    ) -> Result<QueryResult> {
        self.plugin.on_query(sql)?;

        if let Some(change) = self.ddl_change_for_statement(stmt).as_ref() {
            self.plugin.on_ddl(change)?;
        }

        // Handle INSERT INTO GRAPH / __edges as a virtual table routing to the graph store.
        if let Statement::Insert(ins) = stmt
            && (ins.table.eq_ignore_ascii_case("GRAPH")
                || ins.table.eq_ignore_ascii_case("__edges"))
        {
            return self.execute_graph_insert(ins, params, tx);
        }

        let started = Instant::now();
        let result = (|| {
            if let Some(plan) = cached_plan {
                return self.run_planned_statement(stmt, plan, params, tx);
            }

            let (stmt, plan) = {
                // Pre-resolve InSubquery expressions with CTE context before planning.
                let stmt = self.pre_resolve_cte_subqueries(stmt, params, tx)?;
                let plan = contextdb_planner::plan(&stmt)?;
                self.cache_statement_if_eligible(sql, &stmt, &plan);
                (stmt, plan)
            };
            self.run_planned_statement(&stmt, &plan, params, tx)
        })();
        let duration = started.elapsed();
        let outcome = query_outcome_from_result(&result);
        self.plugin.post_query(sql, duration, &outcome);
        result.map(strip_internal_row_id)
    }

    fn run_planned_statement(
        &self,
        stmt: &Statement,
        plan: &PhysicalPlan,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
    ) -> Result<QueryResult> {
        validate_dml(plan, self, params)?;
        let result = match tx {
            Some(tx) => {
                // Reset rows_examined at the top of an in-tx statement so
                // sub-plans accumulate rather than overwrite.
                self.__reset_rows_examined();
                execute_plan(self, plan, params, Some(tx))
            }
            None => self.execute_autocommit(plan, params),
        };
        if result.is_ok()
            && let Statement::CreateTable(ct) = stmt
            && !ct.dag_edge_types.is_empty()
        {
            self.graph.register_dag_edge_types(&ct.dag_edge_types);
        }
        result
    }

    /// Handle `INSERT INTO GRAPH (source_id, target_id, edge_type) VALUES (...)`.
    fn execute_graph_insert(
        &self,
        ins: &contextdb_parser::ast::Insert,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
    ) -> Result<QueryResult> {
        use crate::executor::resolve_expr;

        let col_index = |name: &str| {
            ins.columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case(name))
        };
        let source_idx = col_index("source_id")
            .ok_or_else(|| Error::PlanError("GRAPH INSERT requires source_id column".into()))?;
        let target_idx = col_index("target_id")
            .ok_or_else(|| Error::PlanError("GRAPH INSERT requires target_id column".into()))?;
        let edge_type_idx = col_index("edge_type")
            .ok_or_else(|| Error::PlanError("GRAPH INSERT requires edge_type column".into()))?;

        let auto_commit = tx.is_none();
        let tx = tx.unwrap_or_else(|| self.begin());
        let mut count = 0u64;
        for row_exprs in &ins.values {
            let source = resolve_expr(&row_exprs[source_idx], params)?;
            let target = resolve_expr(&row_exprs[target_idx], params)?;
            let edge_type = resolve_expr(&row_exprs[edge_type_idx], params)?;

            let source_uuid = match &source {
                Value::Uuid(u) => *u,
                Value::Text(t) => uuid::Uuid::parse_str(t)
                    .map_err(|e| Error::PlanError(format!("invalid source_id uuid: {e}")))?,
                _ => return Err(Error::PlanError("source_id must be UUID".into())),
            };
            let target_uuid = match &target {
                Value::Uuid(u) => *u,
                Value::Text(t) => uuid::Uuid::parse_str(t)
                    .map_err(|e| Error::PlanError(format!("invalid target_id uuid: {e}")))?,
                _ => return Err(Error::PlanError("target_id must be UUID".into())),
            };
            let edge_type_str = match &edge_type {
                Value::Text(t) => t.clone(),
                _ => return Err(Error::PlanError("edge_type must be TEXT".into())),
            };

            self.insert_edge(
                tx,
                source_uuid,
                target_uuid,
                edge_type_str,
                Default::default(),
            )?;
            count += 1;
        }

        if auto_commit {
            self.commit_with_source(tx, CommitSource::AutoCommit)?;
        }

        Ok(QueryResult::empty_with_affected(count))
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
                            default: col
                                .default
                                .as_ref()
                                .map(crate::executor::stored_default_expr),
                            references: col.references.as_ref().map(|reference| {
                                contextdb_core::ForeignKeyReference {
                                    table: reference.table.clone(),
                                    column: reference.column.clone(),
                                }
                            }),
                            expires: col.expires,
                            immutable: col.immutable,
                            quantization: match col.quantization {
                                contextdb_parser::ast::VectorQuantization::F32 => {
                                    contextdb_core::VectorQuantization::F32
                                }
                                contextdb_parser::ast::VectorQuantization::SQ8 => {
                                    contextdb_core::VectorQuantization::SQ8
                                }
                                contextdb_parser::ast::VectorQuantization::SQ4 => {
                                    contextdb_core::VectorQuantization::SQ4
                                }
                            },
                            rank_policy: col
                                .rank_policy
                                .as_deref()
                                .map(crate::executor::map_rank_policy),
                            context_id: col.context_id,
                            scope_label: col.scope_label.as_deref().map(|scope| match scope {
                                contextdb_parser::ast::ScopeLabelConstraint::Simple { labels } => {
                                    contextdb_core::ScopeLabelKind::Simple {
                                        write_labels: labels.clone(),
                                    }
                                }
                                contextdb_parser::ast::ScopeLabelConstraint::Split {
                                    read,
                                    write,
                                } => contextdb_core::ScopeLabelKind::Split {
                                    read_labels: read.clone(),
                                    write_labels: write.clone(),
                                },
                            }),
                            acl_ref: col.acl_ref.as_ref().map(|acl| contextdb_core::AclRef {
                                ref_table: acl.ref_table.clone(),
                                ref_column: acl.ref_column.clone(),
                            }),
                        });
                        if col.expires {
                            meta.expires_column = Some(col.name.clone());
                        }
                    }
                    AlterAction::DropColumn {
                        column: name,
                        cascade: _,
                    } => {
                        meta.columns.retain(|c| c.name != *name);
                        if meta.expires_column.as_deref() == Some(name.as_str()) {
                            meta.expires_column = None;
                        }
                    }
                    AlterAction::RenameColumn { from, to } => {
                        if let Some(c) = meta.columns.iter_mut().find(|c| c.name == *from) {
                            c.name = to.clone();
                        }
                        if meta.expires_column.as_deref() == Some(from.as_str()) {
                            meta.expires_column = Some(to.clone());
                        }
                    }
                    AlterAction::SetRetain {
                        duration_seconds,
                        sync_safe,
                    } => {
                        meta.default_ttl_seconds = Some(*duration_seconds);
                        meta.sync_safe = *sync_safe;
                    }
                    AlterAction::DropRetain => {
                        meta.default_ttl_seconds = None;
                        meta.sync_safe = false;
                    }
                    AlterAction::SetSyncConflictPolicy(_) | AlterAction::DropSyncConflictPolicy => { /* handled in executor */
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
        // Statement-scoped bound: `Value::TxId(n)` must satisfy
        // `n <= max(committed_watermark, tx)` so writes inside an active
        // transaction can reference their own allocated TxId. The error,
        // when fired, still reports `committed_watermark` per plan B7.
        let values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        self.validate_row_constraints(tx, table, &values, None)?;
        self.relational.insert(tx, table, values)
    }

    /// UPDATE-aware insert: the UPDATE path first deletes the old row and
    /// then re-inserts. The constraint probe must skip the old row_id so the
    /// same PK does not self-collide. The old row's index entry still looks
    /// visible at the committed-watermark snapshot because its `deleted_tx`
    /// equals the current (uncommitted) `tx`.
    pub(crate) fn insert_row_replacing(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        old_row_id: RowId,
    ) -> Result<RowId> {
        let values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        self.validate_row_constraints(tx, table, &values, Some(old_row_id))?;
        self.relational.insert(tx, table, values)
    }

    /// Internal variant used by sync-apply: skips the TXID bound check because
    /// peer TxIds may legitimately exceed the local watermark. Still enforces
    /// wrong-variant + reverse-direction TXID column rules.
    pub(crate) fn insert_row_for_sync(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<RowId> {
        let values = self.coerce_row_for_insert(table, values, None, None)?;
        self.validate_row_constraints(tx, table, &values, None)?;
        self.relational.insert(tx, table, values)
    }

    pub(crate) fn upsert_row_for_sync(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<UpsertResult> {
        let values = self.coerce_row_for_insert(table, values, None, None)?;
        self.upsert_row(tx, table, conflict_col, values)
    }

    /// Route each row cell through `coerce_value_for_column` for variant
    /// compatibility. The one concession to historical `insert_row` behavior
    /// is that `Value::Vector` payloads are accepted regardless of declared
    /// dimension — prior integration suites (e.g. the 3-component probe into
    /// a VECTOR(384) embedding column) depend on the library API NOT enforcing
    /// dim equality there. SQL execution (`exec_insert`/`exec_update`) still
    /// performs the full dim check because it always threads through the
    /// executor module's coercion helpers.
    fn coerce_row_for_insert(
        &self,
        table: &str,
        values: HashMap<ColName, Value>,
        current_tx_max: Option<TxId>,
        active_tx: Option<TxId>,
    ) -> Result<HashMap<ColName, Value>> {
        let meta = self.table_meta(table);
        let mut out: HashMap<ColName, Value> = HashMap::with_capacity(values.len());
        for (col, v) in values {
            // Vector + Value::Vector: pass straight through (dim check happens on SQL path).
            let is_vector_bypass = matches!(&v, Value::Vector(_))
                && meta
                    .as_ref()
                    .and_then(|m| m.columns.iter().find(|c| c.name == col))
                    .map(|c| matches!(c.column_type, contextdb_core::ColumnType::Vector(_)))
                    .unwrap_or(false);

            let coerced = if is_vector_bypass {
                v
            } else {
                crate::executor::coerce_into_column(
                    self,
                    table,
                    &col,
                    v,
                    current_tx_max,
                    active_tx,
                )?
            };
            out.insert(col, coerced);
        }
        Ok(out)
    }

    pub(crate) fn insert_row_with_unique_noop(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<InsertRowResult> {
        match self.check_row_constraints(tx, table, &values, None, true)? {
            RowConstraintCheck::Valid => self
                .relational
                .insert(tx, table, values)
                .map(InsertRowResult::Inserted),
            RowConstraintCheck::DuplicateUniqueNoOp => Ok(InsertRowResult::NoOp),
        }
    }

    pub fn upsert_row(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<UpsertResult> {
        let snapshot = self.snapshot_for_read();
        let existing_row = values
            .get(conflict_col)
            .map(|conflict_value| {
                self.point_lookup_in_tx(tx, table, conflict_col, conflict_value, snapshot)
            })
            .transpose()?
            .flatten();
        let existing_row_id = existing_row.as_ref().map(|row| row.row_id);
        // Diff-respecting column-level IMMUTABLE check: reject any upsert whose
        // flagged-column value differs from the existing local value. Idempotent
        // replay (same-value) succeeds; new rows (no existing match) apply normally.
        if let (Some(existing), Some(meta)) = (existing_row.as_ref(), self.table_meta(table)) {
            for col_def in meta.columns.iter().filter(|c| c.immutable) {
                let Some(incoming) = values.get(&col_def.name) else {
                    continue;
                };
                let existing_value = existing.values.get(&col_def.name);
                if existing_value != Some(incoming) {
                    return Err(Error::ImmutableColumn {
                        table: table.to_string(),
                        column: col_def.name.clone(),
                    });
                }
            }
        }
        self.validate_row_constraints(tx, table, &values, existing_row_id)?;

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
            .upsert(tx, table, conflict_col, values, snapshot)?;

        if let (Some(uuid), Some(state), Some(_meta)) =
            (row_uuid, new_state.as_deref(), meta.as_ref())
            && matches!(result, UpsertResult::Updated)
        {
            self.propagate_state_change_if_needed(tx, table, Some(uuid), Some(state))?;
        }

        Ok(result)
    }

    fn validate_row_constraints(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        skip_row_id: Option<RowId>,
    ) -> Result<()> {
        match self.check_row_constraints(tx, table, values, skip_row_id, false)? {
            RowConstraintCheck::Valid => Ok(()),
            RowConstraintCheck::DuplicateUniqueNoOp => {
                unreachable!("strict constraint validation cannot return no-op")
            }
        }
    }

    fn check_row_constraints(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        skip_row_id: Option<RowId>,
        allow_duplicate_unique_noop: bool,
    ) -> Result<RowConstraintCheck> {
        let metas = self.relational_store.table_meta.read();
        let meta = metas
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        // Constraint probes MUST see the current committed watermark, not any
        // thread-local override. A PK/UNIQUE violation on a committed row must
        // be detected even if the caller pinned a pre-violation snapshot for
        // read visibility.
        let snapshot = self.snapshot();

        // Scan the whole table only when no index covers any PK / UNIQUE
        // column we need to probe. Pulled lazily so the fast path skips it.
        let mut visible_rows_cache: Option<Vec<VersionedRow>> = None;

        for column in meta.columns.iter().filter(|column| column.primary_key) {
            let Some(value) = values.get(&column.name) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            match self.probe_column_for_constraint(
                tx,
                table,
                &column.name,
                value,
                snapshot,
                skip_row_id,
            )? {
                ConstraintProbe::Match(_) => {
                    return Err(Error::UniqueViolation {
                        table: table.to_string(),
                        column: column.name.clone(),
                    });
                }
                ConstraintProbe::NoMatch => {}
                ConstraintProbe::NoIndex => {
                    // Fallback to full scan for PK columns without an index.
                    if visible_rows_cache.is_none() {
                        visible_rows_cache = Some(self.relational.scan_filter_with_tx(
                            Some(tx),
                            table,
                            snapshot,
                            &|row| skip_row_id.is_none_or(|row_id| row.row_id != row_id),
                        )?);
                    }
                    let rows = visible_rows_cache.as_deref().unwrap();
                    if rows
                        .iter()
                        .any(|existing| existing.values.get(&column.name) == Some(value))
                    {
                        return Err(Error::UniqueViolation {
                            table: table.to_string(),
                            column: column.name.clone(),
                        });
                    }
                }
            }
        }

        let mut duplicate_unique_row_id = None;

        for column in meta
            .columns
            .iter()
            .filter(|column| column.unique && !column.primary_key)
        {
            let Some(value) = values.get(&column.name) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            let matching_row_ids: Vec<RowId> = match self.probe_column_for_constraint(
                tx,
                table,
                &column.name,
                value,
                snapshot,
                skip_row_id,
            )? {
                ConstraintProbe::Match(rid) => vec![rid],
                ConstraintProbe::NoMatch => Vec::new(),
                ConstraintProbe::NoIndex => {
                    if visible_rows_cache.is_none() {
                        visible_rows_cache = Some(self.relational.scan_filter_with_tx(
                            Some(tx),
                            table,
                            snapshot,
                            &|row| skip_row_id.is_none_or(|row_id| row.row_id != row_id),
                        )?);
                    }
                    let rows = visible_rows_cache.as_deref().unwrap();
                    rows.iter()
                        .filter(|existing| existing.values.get(&column.name) == Some(value))
                        .map(|existing| existing.row_id)
                        .collect()
                }
            };
            self.merge_unique_conflict(
                table,
                &column.name,
                &matching_row_ids,
                allow_duplicate_unique_noop,
                &mut duplicate_unique_row_id,
            )?;
        }

        for unique_constraint in &meta.unique_constraints {
            let mut candidate_values = Vec::with_capacity(unique_constraint.len());
            let mut has_null = false;

            for column_name in unique_constraint {
                match values.get(column_name) {
                    Some(Value::Null) | None => {
                        has_null = true;
                        break;
                    }
                    Some(value) => candidate_values.push(value.clone()),
                }
            }

            if has_null {
                continue;
            }

            let matching_row_ids: Vec<RowId> = if let Some(rid) = self.probe_composite_unique(
                tx,
                table,
                unique_constraint,
                &candidate_values,
                snapshot,
                skip_row_id,
            )? {
                vec![rid]
            } else if self.index_covers_composite(table, unique_constraint) {
                Vec::new()
            } else {
                if visible_rows_cache.is_none() {
                    visible_rows_cache = Some(self.relational.scan_filter_with_tx(
                        Some(tx),
                        table,
                        snapshot,
                        &|row| skip_row_id.is_none_or(|row_id| row.row_id != row_id),
                    )?);
                }
                let rows = visible_rows_cache.as_deref().unwrap();
                rows.iter()
                    .filter(|existing| {
                        unique_constraint.iter().zip(candidate_values.iter()).all(
                            |(column_name, value)| existing.values.get(column_name) == Some(value),
                        )
                    })
                    .map(|existing| existing.row_id)
                    .collect()
            };
            // Report composite UNIQUE violations using the first column name,
            // matching the plan's single-column error convention.
            let column_label = unique_constraint.first().map(|s| s.as_str()).unwrap_or("");
            self.merge_unique_conflict(
                table,
                column_label,
                &matching_row_ids,
                allow_duplicate_unique_noop,
                &mut duplicate_unique_row_id,
            )?;
        }

        if duplicate_unique_row_id.is_some() {
            Ok(RowConstraintCheck::DuplicateUniqueNoOp)
        } else {
            Ok(RowConstraintCheck::Valid)
        }
    }

    fn merge_unique_conflict(
        &self,
        table: &str,
        column: &str,
        matching_row_ids: &[RowId],
        allow_duplicate_unique_noop: bool,
        duplicate_unique_row_id: &mut Option<RowId>,
    ) -> Result<()> {
        if matching_row_ids.is_empty() {
            return Ok(());
        }

        if !allow_duplicate_unique_noop || matching_row_ids.len() != 1 {
            return Err(Error::UniqueViolation {
                table: table.to_string(),
                column: column.to_string(),
            });
        }

        let matched_row_id = matching_row_ids[0];

        if let Some(existing_row_id) = duplicate_unique_row_id {
            if *existing_row_id != matched_row_id {
                return Err(Error::UniqueViolation {
                    table: table.to_string(),
                    column: column.to_string(),
                });
            }
        } else {
            *duplicate_unique_row_id = Some(matched_row_id);
        }

        Ok(())
    }

    /// Returns true if `table` has any single-column index covering `column`.
    fn index_covers_column(&self, table: &str, column: &str) -> bool {
        let indexes = self.relational_store.indexes.read();
        indexes
            .iter()
            .any(|((t, _), idx)| t == table && idx.columns.len() == 1 && idx.columns[0].0 == column)
    }

    /// Returns true if `table` has an index whose first-column prefix contains
    /// exactly the columns in `cols` (same order).
    fn index_covers_composite(&self, table: &str, cols: &[String]) -> bool {
        let indexes = self.relational_store.indexes.read();
        indexes.iter().any(|((t, _), idx)| {
            t == table
                && idx.columns.len() >= cols.len()
                && idx
                    .columns
                    .iter()
                    .zip(cols.iter())
                    .all(|((c, _), want)| c == want)
        })
    }

    /// Look up `(table, column) = value` using a single-column index when one
    /// exists, layered with the tx's staged inserts and deletes.
    fn probe_column_for_constraint(
        &self,
        tx: TxId,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_row_id: Option<RowId>,
    ) -> Result<ConstraintProbe> {
        use contextdb_core::{DirectedValue, TotalOrdAsc};
        let (tx_staged_deletes, staged_overlap) = self.tx_mgr.with_write_set(tx, |ws| {
            // Rows this tx has already staged for delete must not be treated as
            // obstructions by the constraint probe. The old index entry still
            // looks visible at the committed-watermark snapshot until commit.
            let deletes = if ws.relational_deletes.is_empty() {
                std::collections::HashSet::new()
            } else {
                ws.relational_deletes
                    .iter()
                    .filter(|(t, _, _)| t == table)
                    .map(|(_, row_id, _)| *row_id)
                    .collect()
            };
            let overlap = ws.relational_inserts.iter().find_map(|(t, row)| {
                if t != table {
                    return None;
                }
                if let Some(sid) = skip_row_id
                    && row.row_id == sid
                {
                    return None;
                }
                if row.values.get(column) == Some(value) {
                    Some(row.row_id)
                } else {
                    None
                }
            });
            (deletes, overlap)
        })?;
        let indexes = self.relational_store.indexes.read();
        // Auto constraint indexes have stable names. Try those directly before
        // falling back to user-declared single-column indexes.
        let table_key = table.to_string();
        let pk_key = (table_key.clone(), format!("__pk_{column}"));
        let unique_key = (table_key, format!("__unique_{column}"));
        let storage = indexes
            .get(&pk_key)
            .or_else(|| indexes.get(&unique_key))
            .or_else(|| {
                indexes.iter().find_map(|((t, _), idx)| {
                    if t == table && idx.columns.len() == 1 && idx.columns[0].0 == column {
                        Some(idx)
                    } else {
                        None
                    }
                })
            });
        let Some(storage) = storage else {
            return Ok(ConstraintProbe::NoIndex);
        };
        let key = vec![DirectedValue::Asc(TotalOrdAsc(value.clone()))];
        if let Some(entries) = storage.tree.get(&key) {
            for entry in entries {
                if let Some(sid) = skip_row_id
                    && entry.row_id == sid
                {
                    continue;
                }
                if tx_staged_deletes.contains(&entry.row_id) {
                    continue;
                }
                if entry.visible_at(snapshot) {
                    return Ok(ConstraintProbe::Match(entry.row_id));
                }
            }
        }
        drop(indexes);
        Ok(match staged_overlap {
            Some(row_id) => ConstraintProbe::Match(row_id),
            None => ConstraintProbe::NoMatch,
        })
    }

    /// Probe a composite UNIQUE (a, b, ...) using the first index whose
    /// leading prefix matches `cols`. The probe walks the range for the full
    /// key prefix.
    fn probe_composite_unique(
        &self,
        tx: TxId,
        table: &str,
        cols: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_row_id: Option<RowId>,
    ) -> Result<Option<RowId>> {
        use contextdb_core::{DirectedValue, TotalOrdAsc};
        if cols.is_empty() || values.is_empty() || cols.len() != values.len() {
            return Ok(None);
        }
        // Rows this tx has already staged for delete are not obstructions.
        let tx_staged_deletes: std::collections::HashSet<RowId> =
            self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_deletes
                    .iter()
                    .filter(|(t, _, _)| t == table)
                    .map(|(_, row_id, _)| *row_id)
                    .collect()
            })?;
        let indexes = self.relational_store.indexes.read();
        let storage_entry = indexes.iter().find(|((t, _), idx)| {
            t == table
                && idx.columns.len() >= cols.len()
                && idx
                    .columns
                    .iter()
                    .zip(cols.iter())
                    .all(|((c, _), w)| c == w)
        });
        let (_, storage) = match storage_entry {
            Some(e) => e,
            None => return Ok(None),
        };
        // Full-prefix walk: range from (val1,...,valN, -inf) to (val1,...,valN, +inf).
        // For simplicity, iterate and filter by prefix match.
        for (key, entries) in storage.tree.iter() {
            if key.len() < cols.len() {
                continue;
            }
            let prefix_match = values.iter().enumerate().all(|(i, v)| match &key[i] {
                DirectedValue::Asc(TotalOrdAsc(k)) => k == v,
                DirectedValue::Desc(contextdb_core::TotalOrdDesc(k)) => k == v,
            });
            if !prefix_match {
                continue;
            }
            for entry in entries {
                if let Some(sid) = skip_row_id
                    && entry.row_id == sid
                {
                    continue;
                }
                if tx_staged_deletes.contains(&entry.row_id) {
                    continue;
                }
                if entry.visible_at(snapshot) {
                    return Ok(Some(entry.row_id));
                }
            }
        }
        drop(indexes);
        // Tx-staged inserts.
        let overlap = self.tx_mgr.with_write_set(tx, |ws| {
            for (t, row) in &ws.relational_inserts {
                if t != table {
                    continue;
                }
                if let Some(sid) = skip_row_id
                    && row.row_id == sid
                {
                    continue;
                }
                let matches = cols
                    .iter()
                    .zip(values.iter())
                    .all(|(c, v)| row.values.get(c) == Some(v));
                if matches {
                    return Some(row.row_id);
                }
            }
            None
        })?;
        Ok(overlap)
    }

    fn validate_foreign_keys_in_tx(&self, tx: TxId) -> Result<()> {
        // Constraint probe — FK existence checks must see the committed
        // watermark, not any read-side snapshot override.
        let snapshot = self.snapshot();
        let relational_inserts = self
            .tx_mgr
            .with_write_set(tx, |ws| ws.relational_inserts.clone())?;

        for (table, row) in relational_inserts {
            let meta = self
                .table_meta(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            for column in &meta.columns {
                let Some(reference) = &column.references else {
                    continue;
                };
                let Some(value) = row.values.get(&column.name) else {
                    continue;
                };
                if *value == Value::Null {
                    continue;
                }
                if self
                    .point_lookup_in_tx(tx, &reference.table, &reference.column, value, snapshot)?
                    .is_none()
                {
                    return Err(Error::ForeignKeyViolation {
                        table: table.clone(),
                        column: column.name.clone(),
                        ref_table: reference.table.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    pub(crate) fn propagate_state_change_if_needed(
        &self,
        tx: TxId,
        table: &str,
        row_uuid: Option<uuid::Uuid>,
        new_state: Option<&str>,
    ) -> Result<()> {
        if let (Some(uuid), Some(state)) = (row_uuid, new_state) {
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

        Ok(())
    }

    fn propagate(
        &self,
        tx: TxId,
        table: &str,
        row_uuid: uuid::Uuid,
        new_state: &str,
    ) -> Result<()> {
        let snapshot = self.snapshot_for_read();
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
            for row_id in self.logical_row_ids_for_uuid(ctx.tx, source.table, source.uuid) {
                let index = self
                    .table_meta(source.table)
                    .and_then(|meta| {
                        meta.columns
                            .iter()
                            .find(|column| {
                                matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                            })
                            .map(|column| VectorIndexRef::new(source.table, column.name.clone()))
                    })
                    .unwrap_or_default();
                self.delete_vector(ctx.tx, index, row_id)?;
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

    /// Scan a table with visibility over the tx's in-flight write-set layered on
    /// top of the committed snapshot.
    pub(crate) fn scan_in_tx(
        &self,
        tx: TxId,
        table: &str,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        self.relational.scan_with_tx(Some(tx), table, snapshot)
    }

    /// Compute the in-tx overlay (deleted row_ids + matching staged inserts)
    /// for an index-driven scan of `table` matching `shape` on `column`.
    /// Internal helper for the IndexScan executor arm.
    pub(crate) fn index_scan_tx_overlay(
        &self,
        tx: TxId,
        table: &str,
        column: &str,
        shape: &crate::executor::IndexPredicateShape,
    ) -> Result<IndexScanTxOverlay> {
        use crate::executor::{IndexPredicateShape, range_includes};
        let mut overlay = IndexScanTxOverlay::default();
        self.tx_mgr.with_write_set(tx, |ws| {
            for (t, _row_id, _) in &ws.relational_deletes {
                if t == table {
                    overlay.deleted_row_ids.insert(*_row_id);
                }
            }
            for (t, row) in &ws.relational_inserts {
                if t != table {
                    continue;
                }
                let v = row.values.get(column).cloned().unwrap_or(Value::Null);
                let include = match shape {
                    IndexPredicateShape::Equality(target) => v == *target,
                    IndexPredicateShape::NotEqual(target) => v != *target,
                    IndexPredicateShape::InList(list) => list.contains(&v),
                    IndexPredicateShape::Range { lower, upper } => range_includes(&v, lower, upper),
                    IndexPredicateShape::IsNull => v == Value::Null,
                    IndexPredicateShape::IsNotNull => v != Value::Null,
                };
                if include {
                    overlay.matching_inserts.push(row.clone());
                }
            }
        })?;
        Ok(overlay)
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

    pub(crate) fn point_lookup_in_tx(
        &self,
        tx: TxId,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        self.relational
            .point_lookup_with_tx(Some(tx), table, col, value, snapshot)
    }

    pub(crate) fn logical_row_ids_for_uuid(
        &self,
        tx: TxId,
        table: &str,
        uuid: uuid::Uuid,
    ) -> Vec<RowId> {
        let mut row_ids = HashSet::new();

        if let Some(rows) = self.relational_store.tables.read().get(table) {
            for row in rows {
                if row.values.get("id") == Some(&Value::Uuid(uuid)) {
                    row_ids.insert(row.row_id);
                }
            }
        }

        let _ = self.tx_mgr.with_write_set(tx, |ws| {
            for (insert_table, row) in &ws.relational_inserts {
                if insert_table == table && row.values.get("id") == Some(&Value::Uuid(uuid)) {
                    row_ids.insert(row.row_id);
                }
            }
        });

        row_ids.into_iter().collect()
    }

    pub fn insert_edge(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        properties: HashMap<String, Value>,
    ) -> Result<bool> {
        let bytes = estimate_edge_bytes(source, target, &edge_type, &properties);
        self.accountant.try_allocate_for(
            bytes,
            "graph_insert",
            "insert_edge",
            "Reduce edge fan-out or raise MEMORY_LIMIT before inserting more graph edges.",
        )?;

        match self
            .graph
            .insert_edge(tx, source, target, edge_type, properties)
        {
            Ok(inserted) => {
                if !inserted {
                    self.accountant.release(bytes);
                }
                Ok(inserted)
            }
            Err(err) => {
                self.accountant.release(bytes);
                Err(err)
            }
        }
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

    pub fn insert_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
    ) -> Result<()> {
        self.vector_store.state(&index)?;
        if let Some(expected) = self.pending_vector_dimension(tx, &index)?
            && expected != vector.len()
        {
            return Err(self.direct_vector_dimension_error(&index, expected, vector.len()));
        }
        self.insert_vector_strict(tx, index.clone(), row_id, vector)
            .map_err(|err| match err {
                Error::VectorIndexDimensionMismatch {
                    expected, actual, ..
                } => self.direct_vector_dimension_error(&index, expected, actual),
                other => other,
            })
    }

    pub(crate) fn insert_vector_strict(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        vector: Vec<f32>,
    ) -> Result<()> {
        self.vector_store.validate_vector(&index, vector.len())?;
        let bytes = self.vector_insert_accounted_bytes(&index, vector.len());
        self.accountant.try_allocate_for(
            bytes,
            "insert",
            &format!("vector_insert@{}.{}", index.table, index.column),
            "Reduce vector dimensionality, insert fewer rows, or raise MEMORY_LIMIT.",
        )?;
        let existing_live = self
            .vector_store
            .live_entry_for_row(&index, row_id, self.snapshot())
            .is_some();
        let entry = VectorEntry {
            index: index.clone(),
            row_id,
            vector,
            created_tx: tx,
            deleted_tx: None,
            lsn: Lsn(0),
        };
        let replaced_inserts = match self.tx_mgr.with_write_set(tx, |ws| {
            let mut replaced_inserts = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_inserts.len() {
                if ws.vector_inserts[pos].index == index && ws.vector_inserts[pos].row_id == row_id
                {
                    replaced_inserts.push(ws.vector_inserts.remove(pos));
                } else {
                    pos += 1;
                }
            }

            let mut moved_sources = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_moves.len() {
                let (move_index, old_row_id, new_row_id, _) = &ws.vector_moves[pos];
                if *move_index == index && *new_row_id == row_id {
                    moved_sources.push(*old_row_id);
                    ws.vector_moves.remove(pos);
                } else {
                    pos += 1;
                }
            }
            for old_row_id in moved_sources {
                if !ws
                    .vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == old_row_id
                    })
                {
                    ws.vector_deletes.push((index.clone(), old_row_id, tx));
                }
            }

            let already_deleted =
                ws.vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == row_id
                    });
            if existing_live && !already_deleted {
                ws.vector_deletes.push((index.clone(), row_id, tx));
            }
            ws.vector_inserts.push(entry);
            replaced_inserts
        }) {
            Ok(replaced_inserts) => replaced_inserts,
            Err(err) => {
                self.accountant.release(bytes);
                return Err(err);
            }
        };
        for replaced in replaced_inserts {
            self.accountant.release(
                self.vector_insert_accounted_bytes(&replaced.index, replaced.vector.len()),
            );
        }
        Ok(())
    }

    pub fn delete_vector(&self, tx: TxId, index: VectorIndexRef, row_id: RowId) -> Result<()> {
        self.vector_store.state(&index)?;
        let existing_live = self
            .vector_store
            .live_entry_for_row(&index, row_id, self.snapshot())
            .is_some();
        let canceled_inserts = self.tx_mgr.with_write_set(tx, |ws| {
            let mut canceled_inserts = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_inserts.len() {
                if ws.vector_inserts[pos].index == index && ws.vector_inserts[pos].row_id == row_id
                {
                    canceled_inserts.push(ws.vector_inserts.remove(pos));
                } else {
                    pos += 1;
                }
            }
            let mut moved_sources = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_moves.len() {
                let (move_index, old_row_id, new_row_id, _) = &ws.vector_moves[pos];
                if *move_index == index && *new_row_id == row_id {
                    moved_sources.push(*old_row_id);
                    ws.vector_moves.remove(pos);
                } else {
                    pos += 1;
                }
            }
            let pending_move_from_row =
                ws.vector_moves
                    .iter()
                    .any(|(move_index, old_row_id, _, _)| {
                        *move_index == index && *old_row_id == row_id
                    });
            let canceled_move_to_row = !moved_sources.is_empty();
            for old_row_id in moved_sources {
                if !ws
                    .vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == old_row_id
                    })
                {
                    ws.vector_deletes.push((index.clone(), old_row_id, tx));
                }
            }
            let already_deleted =
                ws.vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == row_id
                    });
            if !pending_move_from_row
                && ((canceled_inserts.is_empty() && !canceled_move_to_row) || existing_live)
                && !already_deleted
            {
                ws.vector_deletes.push((index, row_id, tx));
            }
            canceled_inserts
        })?;
        for entry in canceled_inserts {
            self.accountant
                .release(self.vector_insert_accounted_bytes(&entry.index, entry.vector.len()));
        }
        Ok(())
    }

    pub(crate) fn move_vector(
        &self,
        tx: TxId,
        index: VectorIndexRef,
        old_row_id: RowId,
        new_row_id: RowId,
    ) -> Result<()> {
        self.vector_store.state(&index)?;
        let existing_live = self
            .vector_store
            .live_entry_for_row(&index, old_row_id, self.snapshot())
            .is_some();
        let replaced_inserts = self.tx_mgr.with_write_set(tx, |ws| {
            let old_row_deleted =
                ws.vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == old_row_id
                    });

            let mut moving_insert = None;
            let mut replaced_inserts = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_inserts.len() {
                if ws.vector_inserts[pos].index == index
                    && ws.vector_inserts[pos].row_id == old_row_id
                {
                    let entry = ws.vector_inserts.remove(pos);
                    if let Some(previous) = moving_insert.replace(entry) {
                        replaced_inserts.push(previous);
                    }
                } else {
                    pos += 1;
                }
            }

            let mut moved_any = moving_insert.is_some();
            let mut has_move_from_old = false;
            for (move_index, source_row_id, destination_row_id, _) in &mut ws.vector_moves {
                if *move_index != index {
                    continue;
                }
                if *destination_row_id == old_row_id {
                    *destination_row_id = new_row_id;
                    moved_any = true;
                }
                if *source_row_id == old_row_id {
                    *destination_row_id = new_row_id;
                    has_move_from_old = true;
                    moved_any = true;
                }
            }

            if !moved_any && existing_live && !old_row_deleted {
                ws.vector_moves
                    .push((index.clone(), old_row_id, new_row_id, tx));
                moved_any = true;
                has_move_from_old = true;
            }

            if moved_any {
                let mut pos = 0;
                while pos < ws.vector_inserts.len() {
                    if ws.vector_inserts[pos].index == index
                        && ws.vector_inserts[pos].row_id == new_row_id
                    {
                        replaced_inserts.push(ws.vector_inserts.remove(pos));
                    } else {
                        pos += 1;
                    }
                }
            }

            if let Some(mut entry) = moving_insert {
                entry.row_id = new_row_id;
                ws.vector_inserts.push(entry);
            } else if has_move_from_old {
                let mut seen_move_from_old = false;
                ws.vector_moves.retain(|(move_index, source_row_id, _, _)| {
                    if *move_index == index && *source_row_id == old_row_id {
                        if seen_move_from_old {
                            false
                        } else {
                            seen_move_from_old = true;
                            true
                        }
                    } else {
                        true
                    }
                });
            }

            replaced_inserts
        })?;
        for replaced in replaced_inserts {
            self.accountant.release(
                self.vector_insert_accounted_bytes(&replaced.index, replaced.vector.len()),
            );
        }
        Ok(())
    }

    pub fn query_vector(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        if self.vector_store.try_state(&index).is_none() {
            return Err(Error::UnknownVectorIndex { index });
        }
        self.vector.search(index, query, k, candidates, snapshot)
    }

    pub fn semantic_search(&self, query: SemanticQuery) -> Result<Vec<SearchResult>> {
        self.semantic_search_with_candidates(query, None)
    }

    pub(crate) fn semantic_search_with_candidates(
        &self,
        query: SemanticQuery,
        candidates: Option<RoaringTreemap>,
    ) -> Result<Vec<SearchResult>> {
        let index = VectorIndexRef::new(query.table.clone(), query.vector_column.clone());
        let snapshot = self.snapshot_for_read();
        let meta = self
            .table_meta(&query.table)
            .ok_or_else(|| Error::TableNotFound(query.table.clone()))?;
        let vector_column = meta
            .columns
            .iter()
            .find(|column| column.name == query.vector_column)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;

        let mut candidate_bitmap = candidates;
        if let Some(where_clause) = &query.where_clause {
            let where_bitmap =
                self.semantic_where_candidate_bitmap(&query.table, where_clause, snapshot)?;
            candidate_bitmap = Some(match candidate_bitmap {
                Some(mut existing) => {
                    existing &= where_bitmap;
                    existing
                }
                None => where_bitmap,
            });
        }

        let Some(sort_key) = query.sort_key.as_deref() else {
            let raw_k = if query.min_similarity.is_some() || candidate_bitmap.is_some() {
                self.vector_entry_count(&index).max(query.limit)
            } else {
                query.limit
            };
            let mut rows = self.query_vector_strict(
                index.clone(),
                &query.query,
                raw_k,
                candidate_bitmap.as_ref(),
                snapshot,
            )?;
            if let Some(min_similarity) = query.min_similarity {
                rows.retain(|(_, score)| *score >= min_similarity);
                rows.truncate(query.limit);
            }
            return rows
                .into_iter()
                .map(|(row_id, vector_score)| {
                    let anchor = self.find_row_by_id_at(&query.table, row_id, snapshot)?;
                    let values = self.search_result_values(&index, row_id, snapshot, anchor.values);
                    Ok(SearchResult {
                        row_id,
                        values,
                        vector_score,
                        rank: vector_score,
                    })
                })
                .collect();
        };

        let Some(policy) = vector_column.rank_policy.as_ref() else {
            return Err(Error::RankPolicyNotFound {
                index: rank_index_name(&query.table, &query.vector_column),
                sort_key: sort_key.to_string(),
            });
        };
        if policy.sort_key != sort_key {
            return Err(Error::RankPolicyNotFound {
                index: rank_index_name(&query.table, &query.vector_column),
                sort_key: sort_key.to_string(),
            });
        }
        let formula = self.rank_formula(&query.table, &query.vector_column)?;
        let entry_count = self.vector_entry_count(&index);
        let internal_k = self.rank_policy_candidate_k(entry_count, query.limit);
        let mut raw = self.query_vector_strict(
            index.clone(),
            &query.query,
            internal_k,
            candidate_bitmap.as_ref(),
            snapshot,
        )?;
        if let Some(min_similarity) = query.min_similarity {
            raw.retain(|(_, score)| *score >= min_similarity);
        }

        let mut ranked = Vec::with_capacity(raw.len());
        for (row_id, vector_score) in raw {
            let anchor = self.find_row_by_id_at(&query.table, row_id, snapshot)?;
            let joined = self.joined_row_for_rank_policy(policy, &anchor, snapshot)?;
            self.rank_policy_eval_count.fetch_add(1, Ordering::SeqCst);
            let eval = formula.eval_with_resolver(vector_score, |column| {
                self.resolve_rank_formula_column(policy, &anchor, joined.as_ref(), column)
            });
            let rank = match eval {
                Ok(Some(rank)) => rank,
                Ok(None) => f32::NAN,
                Err(err) => {
                    let error_row_id =
                        if matches!(err, FormulaEvalError::CorruptJoinedColumn { .. }) {
                            joined.as_ref().map(|row| row.row_id).unwrap_or(row_id)
                        } else {
                            row_id
                        };
                    self.warn_rank_eval_error(
                        &query.table,
                        &query.vector_column,
                        error_row_id,
                        &err,
                    );
                    continue;
                }
            };
            let values = self.search_result_values(
                &index,
                row_id,
                snapshot,
                merged_rank_values(&anchor, joined.as_ref()),
            );
            ranked.push(SearchResult {
                row_id,
                values,
                vector_score,
                rank,
            });
        }
        ranked.sort_by(compare_ranked_results);
        ranked.truncate(query.limit);
        Ok(ranked)
    }

    #[doc(hidden)]
    pub fn __rank_policy_eval_count(&self) -> u64 {
        self.rank_policy_eval_count.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn __reset_rank_policy_eval_count(&self) {
        self.rank_policy_eval_count.store(0, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn __rank_policy_formula_parse_count(&self) -> u64 {
        self.rank_policy_formula_parse_count.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn __inject_raw_joined_row_value_for_test(
        &self,
        table: &str,
        row_id: RowId,
        column: &str,
        _raw_bytes: Vec<u8>,
    ) -> Result<()> {
        self.corrupt_joined_values
            .write()
            .insert((table.to_string(), row_id, column.to_string()));
        Ok(())
    }

    pub(crate) fn query_vector_strict(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<Vec<(RowId, f32)>> {
        self.vector_store.validate_vector(&index, query.len())?;
        self.vector.search(index, query, k, candidates, snapshot)
    }

    pub(crate) fn register_rank_formula(
        &self,
        table: &str,
        column: &str,
        formula: Arc<RankFormula>,
    ) {
        let mut cache = self.rank_formula_cache.write();
        cache.insert((table.to_string(), column.to_string()), formula);
        self.rank_policy_formula_parse_count
            .store(cache.len() as u64, Ordering::SeqCst);
    }

    pub(crate) fn remove_rank_formula(&self, table: &str, column: &str) {
        let mut cache = self.rank_formula_cache.write();
        cache.remove(&(table.to_string(), column.to_string()));
        self.rank_policy_formula_parse_count
            .store(cache.len() as u64, Ordering::SeqCst);
    }

    pub(crate) fn remove_rank_formulas_for_table(&self, table: &str) {
        let mut cache = self.rank_formula_cache.write();
        cache.retain(|(policy_table, _), _| policy_table != table);
        self.rank_policy_formula_parse_count
            .store(cache.len() as u64, Ordering::SeqCst);
    }

    fn rebuild_rank_formula_cache_from_meta(
        &self,
        metas: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        let mut cache = self.rank_formula_cache.write();
        cache.clear();
        for (table, meta) in metas {
            for column in &meta.columns {
                if let Some(policy) = &column.rank_policy {
                    let formula = RankFormula::compile_for_index(
                        &rank_index_name(table, &column.name),
                        &policy.formula,
                    )?;
                    cache.insert((table.clone(), column.name.clone()), Arc::new(formula));
                }
            }
        }
        self.rank_policy_formula_parse_count
            .store(cache.len() as u64, Ordering::SeqCst);
        Ok(())
    }

    fn rank_formula(&self, table: &str, column: &str) -> Result<Arc<RankFormula>> {
        self.rank_formula_cache
            .read()
            .get(&(table.to_string(), column.to_string()))
            .cloned()
            .ok_or_else(|| {
                Error::Other(format!(
                    "rank policy formula cache missing for {}",
                    rank_index_name(table, column)
                ))
            })
    }

    fn vector_entry_count(&self, index: &VectorIndexRef) -> usize {
        self.vector_store
            .try_state(index)
            .map(|state| state.entry_count())
            .unwrap_or(0)
    }

    fn rank_policy_candidate_k(&self, entry_count: usize, limit: usize) -> usize {
        if entry_count == 0 || limit == 0 {
            return limit;
        }
        if entry_count < 1000 {
            return entry_count;
        }
        entry_count
            .saturating_sub(1)
            .min(limit.saturating_mul(30).max(1500))
            .max(limit)
    }

    fn semantic_where_candidate_bitmap(
        &self,
        table: &str,
        where_clause: &str,
        snapshot: SnapshotId,
    ) -> Result<RoaringTreemap> {
        let sql = format!("SELECT * FROM {table} WHERE {where_clause}");
        let stmt = contextdb_parser::parse(&sql)?;
        let expr = match stmt {
            Statement::Select(select) => select
                .body
                .where_clause
                .ok_or_else(|| Error::ParseError("semantic WHERE missing expression".into()))?,
            _ => return Err(Error::ParseError("semantic WHERE parse failed".into())),
        };
        let mut bitmap = RoaringTreemap::new();
        for row in self.scan(table, snapshot)? {
            if crate::executor::row_matches(&row, &expr, &HashMap::new())? {
                bitmap.insert(row.row_id.0);
            }
        }
        Ok(bitmap)
    }

    fn find_row_by_id_at(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<VersionedRow> {
        self.relational_store
            .row_by_id(table, row_id, snapshot)
            .ok_or_else(|| Error::NotFound(format!("row {row_id} in table {table}")))
    }

    fn joined_row_for_rank_policy(
        &self,
        policy: &RankPolicy,
        anchor: &VersionedRow,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        if policy.anchor_column.is_empty() {
            return Err(Error::Other(format!(
                "rank policy on index {}.{} has no resolved anchor join column",
                policy.joined_table, policy.joined_column
            )));
        }
        let join_value = anchor
            .values
            .get(&policy.anchor_column)
            .cloned()
            .unwrap_or(Value::Null);
        if join_value == Value::Null {
            return Ok(None);
        }

        let indexes = self.relational_store.indexes.read();
        let storage = indexes
            .get(&(policy.joined_table.clone(), policy.protected_index.clone()))
            .ok_or_else(|| {
                Error::Other(format!(
                    "rank policy protected index `{}` missing on table `{}`",
                    policy.protected_index, policy.joined_table
                ))
            })?;
        let Some((first_column, direction)) = storage.columns.first() else {
            return Err(Error::Other(format!(
                "rank policy protected index `{}` on `{}` has no columns",
                policy.protected_index, policy.joined_table
            )));
        };
        if first_column != &policy.joined_column {
            return Err(Error::Other(format!(
                "rank policy protected index `{}` on `{}` no longer leads with `{}`",
                policy.protected_index, policy.joined_table, policy.joined_column
            )));
        }

        let key_component = match direction {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(join_value.clone())),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(join_value.clone())),
        };
        let mut best_row_id: Option<RowId> = None;
        let mut consider = |entries: &[contextdb_relational::IndexEntry]| {
            for entry in entries {
                if entry.visible_at(snapshot)
                    && best_row_id.is_none_or(|current| current < entry.row_id)
                {
                    best_row_id = Some(entry.row_id);
                }
            }
        };

        if storage.columns.len() == 1 {
            if let Some(entries) = storage.tree.get(&vec![key_component.clone()]) {
                consider(entries);
            }
        } else {
            for (key, entries) in storage.tree.range(vec![key_component.clone()]..) {
                if key.first() != Some(&key_component) {
                    break;
                }
                consider(entries);
            }
        }
        drop(indexes);

        let Some(row_id) = best_row_id else {
            return Ok(None);
        };
        let Some(row) = self
            .relational_store
            .row_by_id(&policy.joined_table, row_id, snapshot)
        else {
            return Ok(None);
        };
        let Some(value) = row.values.get(&policy.joined_column) else {
            return Ok(None);
        };
        if !values_equal_for_rank_join(value, &join_value) {
            return Ok(None);
        }
        Ok(Some(row))
    }

    fn resolve_rank_formula_column(
        &self,
        policy: &RankPolicy,
        anchor: &VersionedRow,
        joined: Option<&VersionedRow>,
        column: &str,
    ) -> std::result::Result<Option<f32>, FormulaEvalError> {
        if let Some(value) = anchor.values.get(column) {
            return rank_value_to_number(value, column);
        }
        let Some(joined) = joined else {
            return Ok(None);
        };
        if self.corrupt_joined_values.read().contains(&(
            policy.joined_table.clone(),
            joined.row_id,
            column.to_string(),
        )) {
            return Err(FormulaEvalError::CorruptJoinedColumn {
                column: column.to_string(),
            });
        }
        let value = joined.values.get(column).unwrap_or(&Value::Null);
        rank_value_to_number(value, column)
    }

    fn warn_rank_eval_error(
        &self,
        table: &str,
        column: &str,
        row_id: RowId,
        err: &FormulaEvalError,
    ) {
        let mut reason = err.reason();
        if reason.len() > 256 {
            reason.truncate(253);
            reason.push_str("...");
        }
        tracing::warn!(
            name: "rank_policy_eval_error",
            target: "rank_policy_eval_error",
            index = %rank_index_name(table, column),
            row_id = row_id.0,
            reason = %reason,
            "rank_policy_eval_error"
        );
    }

    fn search_result_values(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        mut values: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        if let Some(entry) = self.vector_store_live_entry_for_row(index, row_id, snapshot) {
            values.insert(index.column.clone(), Value::Vector(entry.vector));
        }
        values
    }

    fn pending_vector_dimension(&self, tx: TxId, index: &VectorIndexRef) -> Result<Option<usize>> {
        Ok(self
            .tx_mgr
            .cloned_write_set(tx)?
            .vector_inserts
            .iter()
            .rev()
            .find(|entry| entry.index == *index && entry.deleted_tx.is_none())
            .map(|entry| entry.vector.len()))
    }

    fn direct_vector_dimension_error(
        &self,
        index: &VectorIndexRef,
        expected: usize,
        actual: usize,
    ) -> Error {
        Error::VectorIndexDimensionMismatch {
            index: index.clone(),
            expected,
            actual,
        }
    }

    pub(crate) fn vector_insert_accounted_bytes(
        &self,
        index: &VectorIndexRef,
        dimension: usize,
    ) -> usize {
        self.vector_store
            .try_state(index)
            .map(|state| state.quantization().storage_bytes(dimension))
            .unwrap_or_else(|| 24 + dimension.saturating_mul(std::mem::size_of::<f32>()))
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_len(&self, index: VectorIndexRef) -> Option<usize> {
        self.vector_store
            .try_state(&index)
            .and_then(|state| state.hnsw_len())
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_stats(&self, index: VectorIndexRef) -> Option<HnswGraphStats> {
        self.vector_store
            .try_state(&index)
            .and_then(|state| state.hnsw_stats())
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_raw_search_for_test(
        &self,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
    ) -> Option<Vec<(RowId, f32)>> {
        self.vector_store
            .raw_hnsw_search(&index, query, k)
            .and_then(Result::ok)
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_raw_entry_count_for_row_for_test(
        &self,
        index: VectorIndexRef,
        row_id: RowId,
    ) -> Option<usize> {
        self.vector_store
            .raw_hnsw_entry_count_for_row(&index, row_id)
    }

    #[doc(hidden)]
    pub fn __debug_vector_storage_bytes_per_entry(
        &self,
        index: VectorIndexRef,
    ) -> Result<Vec<usize>> {
        self.vector_store.storage_bytes_per_entry(&index)
    }

    pub fn has_live_vector(&self, row_id: RowId, snapshot: SnapshotId) -> bool {
        !self
            .vector_store
            .live_entries_for_row(row_id, snapshot)
            .is_empty()
    }

    pub fn live_vector_entry(&self, row_id: RowId, snapshot: SnapshotId) -> Option<VectorEntry> {
        self.vector_store
            .live_entries_for_row(row_id, snapshot)
            .into_iter()
            .next()
    }

    pub(crate) fn vector_store_live_entry_for_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Option<VectorEntry> {
        self.vector_store
            .live_entry_for_row(index, row_id, snapshot)
    }

    pub(crate) fn drop_table_aux_state(&self, table: &str) {
        let snapshot = self.snapshot_for_read();
        let rows = self.scan(table, snapshot).unwrap_or_default();
        let edge_keys: HashSet<(NodeId, EdgeType, NodeId)> = rows
            .iter()
            .filter_map(|row| {
                match (
                    row.values.get("source_id").and_then(Value::as_uuid),
                    row.values.get("target_id").and_then(Value::as_uuid),
                    row.values.get("edge_type").and_then(Value::as_text),
                ) {
                    (Some(source), Some(target), Some(edge_type)) => {
                        Some((*source, edge_type.to_string(), *target))
                    }
                    _ => None,
                }
            })
            .collect();

        if !edge_keys.is_empty() {
            {
                let mut forward = self.graph_store.forward_adj.write();
                for entries in forward.values_mut() {
                    entries.retain(|entry| {
                        !edge_keys.contains(&(entry.source, entry.edge_type.clone(), entry.target))
                    });
                }
                forward.retain(|_, entries| !entries.is_empty());
            }
            {
                let mut reverse = self.graph_store.reverse_adj.write();
                for entries in reverse.values_mut() {
                    entries.retain(|entry| {
                        !edge_keys.contains(&(entry.source, entry.edge_type.clone(), entry.target))
                    });
                }
                reverse.retain(|_, entries| !entries.is_empty());
            }
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        self.relational_store.table_names()
    }

    pub fn table_meta(&self, table: &str) -> Option<TableMeta> {
        self.relational_store.table_meta(table)
    }

    /// Execute at a specific snapshot. Threads `snapshot` through via a
    /// thread-local override so scans / IndexScans filter visibility using
    /// the caller-provided snapshot, not the live committed watermark.
    #[doc(hidden)]
    pub fn execute_at_snapshot(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        snapshot: SnapshotId,
    ) -> Result<QueryResult> {
        SNAPSHOT_OVERRIDE.with(|cell| {
            let prior = cell.replace(Some(snapshot));
            let r = self.execute(sql, params);
            cell.replace(prior);
            r
        })
    }

    pub(crate) fn snapshot_for_read(&self) -> SnapshotId {
        SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().unwrap_or_else(|| self.snapshot()))
    }

    /// Return the row changes since `since`. Walks `change_log` for
    /// `RowInsert` / `RowDelete` entries whose LSN exceeds `since`, fetches
    /// the row values out of the live relational store, and emits a
    /// `RowChange` the receiver can replay. row_id order is preserved.
    #[doc(hidden)]
    pub fn change_log_rows_since(&self, since: Lsn) -> Result<Vec<RowChange>> {
        let entries = self.change_log_since(since);
        let tables = self.relational_store.tables.read();
        let mut out = Vec::new();
        for e in entries {
            match e {
                ChangeLogEntry::RowInsert { table, row_id, lsn } => {
                    let Some(rows) = tables.get(&table) else {
                        continue;
                    };
                    let Some(row) = rows.iter().find(|r| r.row_id == row_id) else {
                        continue;
                    };
                    let natural_key = row
                        .values
                        .get("id")
                        .cloned()
                        .map(|value| NaturalKey {
                            column: "id".to_string(),
                            value,
                        })
                        .unwrap_or_else(|| NaturalKey {
                            column: "id".to_string(),
                            value: Value::Int64(row_id.0 as i64),
                        });
                    out.push(RowChange {
                        table,
                        natural_key,
                        values: row.values.clone(),
                        deleted: row.deleted_tx.is_some(),
                        lsn,
                    });
                }
                ChangeLogEntry::RowDelete {
                    table,
                    row_id: _,
                    natural_key,
                    lsn,
                } => {
                    out.push(RowChange {
                        table,
                        natural_key,
                        values: HashMap::new(),
                        deleted: true,
                        lsn,
                    });
                }
                _ => {}
            }
        }
        Ok(out)
    }

    /// Count of base rows the executor touched during the most recent query.
    #[doc(hidden)]
    pub fn __rows_examined(&self) -> u64 {
        self.rows_examined.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn __reset_rows_examined(&self) {
        self.rows_examined.store(0, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn __bump_rows_examined(&self, delta: u64) {
        self.rows_examined.fetch_add(delta, Ordering::SeqCst);
    }

    /// Count of batch-level `indexes.write()` lock acquisitions since startup.
    /// `apply_changes` bumps this once per batch; per-row commits do not.
    #[doc(hidden)]
    pub fn __index_write_lock_count(&self) -> u64 {
        self.relational_store.index_write_lock_count()
    }

    /// Total entries across every registered index's BTreeMap.
    #[doc(hidden)]
    pub fn __introspect_indexes_total_entries(&self) -> u64 {
        self.relational_store.introspect_indexes_total_entries()
    }

    /// Probe the constraint-check path for a specific table/column/value.
    /// Returns a QueryResult whose trace reflects whether the probe went
    /// through an index (IndexScan) or a full scan. Accepts either a
    /// single-column index or a composite leading-column match.
    #[doc(hidden)]
    pub fn __probe_constraint_check(
        &self,
        table: &str,
        column: &str,
        value: Value,
    ) -> Result<QueryResult> {
        let covered = self.index_covers_column(table, column)
            || self
                .relational_store
                .indexes
                .read()
                .iter()
                .any(|((t, _), idx)| {
                    t == table && idx.columns.first().is_some_and(|(c, _)| c == column)
                });
        let trace = if covered {
            QueryTrace {
                physical_plan: "IndexScan",
                index_used: None,
                predicates_pushed: Default::default(),
                indexes_considered: Default::default(),
                sort_elided: false,
            }
        } else {
            QueryTrace::scan()
        };
        let _ = value;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            trace,
            cascade: None,
        })
    }

    /// Run one pruning cycle. Called by the background loop or manually in tests.
    pub fn run_pruning_cycle(&self) -> u64 {
        let _guard = self.pruning_guard.lock();
        prune_expired_rows(
            &self.relational_store,
            &self.graph_store,
            &self.vector_store,
            self.accountant(),
            self.persistence.as_ref(),
            self.sync_watermark(),
        )
    }

    /// Set the pruning loop interval. Test-only API.
    pub fn set_pruning_interval(&self, interval: Duration) {
        self.stop_pruning_thread();

        let shutdown = Arc::new(AtomicBool::new(false));
        let relational = self.relational_store.clone();
        let graph = self.graph_store.clone();
        let vector = self.vector_store.clone();
        let accountant = self.accountant.clone();
        let persistence = self.persistence.clone();
        let sync_watermark = self.sync_watermark.clone();
        let pruning_guard = self.pruning_guard.clone();
        let thread_shutdown = shutdown.clone();

        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::SeqCst) {
                {
                    let _guard = pruning_guard.lock();
                    let _ = prune_expired_rows(
                        &relational,
                        &graph,
                        &vector,
                        accountant.as_ref(),
                        persistence.as_ref(),
                        sync_watermark.load(Ordering::SeqCst),
                    );
                }
                sleep_with_shutdown(&thread_shutdown, interval);
            }
        });

        let mut runtime = self.pruning_runtime.lock();
        runtime.shutdown = shutdown;
        runtime.handle = Some(handle);
    }

    pub fn sync_watermark(&self) -> Lsn {
        self.sync_watermark.load(Ordering::SeqCst)
    }

    pub fn set_sync_watermark(&self, watermark: Lsn) {
        self.sync_watermark.store(watermark, Ordering::SeqCst);
    }

    pub fn instance_id(&self) -> uuid::Uuid {
        self.instance_id
    }

    pub fn open_memory_with_plugin_and_accountant(
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
    ) -> Result<Self> {
        Self::open_memory_internal(plugin, accountant)
    }

    pub fn open_memory_with_plugin(plugin: Arc<dyn DatabasePlugin>) -> Result<Self> {
        let db = Self::open_memory_with_plugin_and_accountant(
            plugin,
            Arc::new(MemoryAccountant::no_limit()),
        )?;
        db.plugin.on_open()?;
        Ok(db)
    }

    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let tx = self.session_tx.lock().take();
        if let Some(tx) = tx {
            self.rollback(tx)?;
        }
        self.stop_pruning_thread();
        self.subscriptions.lock().subscribers.clear();
        if let Some(persistence) = &self.persistence {
            persistence.close();
        }
        self.plugin.on_close()
    }

    /// File-backed database with custom plugin.
    pub fn open_with_plugin(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
    ) -> Result<Self> {
        let db = Self::open_loaded(path, plugin, Arc::new(MemoryAccountant::no_limit()), None)?;
        db.plugin.on_open()?;
        Ok(db)
    }

    /// Full constructor with budget.
    pub fn open_with_config(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
    ) -> Result<Self> {
        Self::open_with_config_and_disk_limit(path, plugin, accountant, None)
    }

    pub fn open_with_config_and_disk_limit(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
        startup_disk_limit: Option<u64>,
    ) -> Result<Self> {
        let path = path.as_ref();
        if path.as_os_str() == ":memory:" {
            return Self::open_memory_with_plugin_and_accountant(plugin, accountant);
        }
        let accountant = if let Some(limit) = persisted_memory_limit(path)? {
            let usage = accountant.usage();
            if usage.limit.is_none() && usage.startup_ceiling.is_none() {
                Arc::new(MemoryAccountant::with_budget(limit))
            } else {
                accountant
            }
        } else {
            accountant
        };
        let db = Self::open_loaded(path, plugin, accountant, startup_disk_limit)?;
        db.plugin.on_open()?;
        Ok(db)
    }

    /// In-memory database with budget.
    pub fn open_memory_with_accountant(accountant: Arc<MemoryAccountant>) -> Self {
        Self::open_memory_internal(Arc::new(CorePlugin), accountant)
            .expect("failed to open in-memory database with accountant")
    }

    /// Access the memory accountant.
    pub fn accountant(&self) -> &MemoryAccountant {
        &self.accountant
    }

    pub(crate) fn register_vector_index_for_column(&self, table: &str, column: &ColumnDef) {
        if let ColumnType::Vector(dimension) = column.column_type {
            self.vector_store.register_index(
                VectorIndexRef::new(table, column.name.clone()),
                dimension,
                column.quantization,
            );
        }
    }

    pub(crate) fn deregister_vector_index(&self, table: &str, column: &str) {
        self.vector_store
            .deregister_index(&VectorIndexRef::new(table, column), self.accountant());
    }

    pub(crate) fn rename_vector_index(&self, table: &str, from: &str, to: &str) -> Result<()> {
        self.vector_store.rename_index(
            &VectorIndexRef::new(table, from),
            VectorIndexRef::new(table, to),
        )
    }

    pub(crate) fn vector_store_deregister_table(&self, table: &str) {
        self.vector_store.deregister_table(table, self.accountant());
    }

    pub(crate) fn vector_index_infos(&self) -> Vec<contextdb_vector::store::VectorIndexInfo> {
        self.vector_store.index_infos()
    }

    fn account_loaded_state(&self) -> Result<()> {
        let metadata_bytes = self
            .relational_store
            .table_meta
            .read()
            .values()
            .fold(0usize, |acc, meta| {
                acc.saturating_add(meta.estimated_bytes())
            });
        self.accountant.try_allocate_for(
            metadata_bytes,
            "open",
            "load_table_metadata",
            "Open the database with a larger MEMORY_LIMIT or reduce stored schema metadata.",
        )?;

        let row_bytes =
            self.relational_store
                .tables
                .read()
                .iter()
                .fold(0usize, |acc, (table, rows)| {
                    let meta = self.table_meta(table);
                    acc.saturating_add(rows.iter().fold(0usize, |inner, row| {
                        inner.saturating_add(meta.as_ref().map_or_else(
                            || row.estimated_bytes(),
                            |meta| estimate_row_bytes_for_meta(&row.values, meta, false),
                        ))
                    }))
                });
        self.accountant.try_allocate_for(
            row_bytes,
            "open",
            "load_rows",
            "Open the database with a larger MEMORY_LIMIT or prune retained rows first.",
        )?;

        let edge_bytes = self
            .graph_store
            .forward_adj
            .read()
            .values()
            .flatten()
            .filter(|edge| edge.deleted_tx.is_none())
            .fold(0usize, |acc, edge| {
                acc.saturating_add(edge.estimated_bytes())
            });
        self.accountant.try_allocate_for(
            edge_bytes,
            "open",
            "load_edges",
            "Open the database with a larger MEMORY_LIMIT or reduce graph edge volume.",
        )?;

        let vector_bytes = self
            .vector_store
            .index_infos()
            .into_iter()
            .fold(0usize, |acc, info| acc.saturating_add(info.bytes));
        self.accountant.try_allocate_for(
            vector_bytes,
            "open",
            "load_vectors",
            "Open the database with a larger MEMORY_LIMIT or reduce stored vector data.",
        )?;

        Ok(())
    }

    fn release_insert_allocations(&self, ws: &contextdb_tx::WriteSet) {
        for (table, row) in &ws.relational_inserts {
            let bytes = self
                .table_meta(table)
                .map(|meta| estimate_row_bytes_for_meta(&row.values, &meta, false))
                .unwrap_or_else(|| row.estimated_bytes());
            self.accountant.release(bytes);
        }

        for edge in &ws.adj_inserts {
            self.accountant.release(edge.estimated_bytes());
        }

        for entry in &ws.vector_inserts {
            self.accountant
                .release(self.vector_insert_accounted_bytes(&entry.index, entry.vector.len()));
        }
    }

    fn release_delete_allocations(&self, ws: &contextdb_tx::WriteSet) {
        for (table, row_id, _) in &ws.relational_deletes {
            if let Some(row) = self.find_row_by_id(table, *row_id) {
                let bytes = self
                    .table_meta(table)
                    .map(|meta| estimate_row_bytes_for_meta(&row.values, &meta, false))
                    .unwrap_or_else(|| row.estimated_bytes());
                self.accountant.release(bytes);
            }
        }

        for (source, edge_type, target, _) in &ws.adj_deletes {
            if let Some(edge) = self.find_edge(source, target, edge_type) {
                self.accountant.release(edge.estimated_bytes());
            }
        }

        for (index, row_id, _) in &ws.vector_deletes {
            if let Some(vector) = self.find_vector_by_index_and_row(index, *row_id) {
                self.accountant
                    .release(self.vector_insert_accounted_bytes(index, vector.vector.len()));
            }
        }

        if !ws.vector_deletes.is_empty() {
            self.vector_store.clear_hnsw(self.accountant());
        }
    }

    fn find_row_by_id(&self, table: &str, row_id: RowId) -> Option<VersionedRow> {
        self.relational_store
            .tables
            .read()
            .get(table)
            .and_then(|rows| rows.iter().find(|row| row.row_id == row_id))
            .cloned()
    }

    fn find_vector_by_index_and_row(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
    ) -> Option<VectorEntry> {
        self.vector_store
            .try_state(index)
            .and_then(|state| state.find_by_row_id(index, row_id))
    }

    fn find_edge(&self, source: &NodeId, target: &NodeId, edge_type: &str) -> Option<AdjEntry> {
        self.graph_store
            .forward_adj
            .read()
            .get(source)
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| entry.target == *target && entry.edge_type == edge_type)
                    .cloned()
            })
    }

    pub(crate) fn write_set_checkpoint(
        &self,
        tx: TxId,
    ) -> Result<(usize, usize, usize, usize, usize, usize)> {
        self.tx_mgr.with_write_set(tx, |ws| {
            (
                ws.relational_inserts.len(),
                ws.relational_deletes.len(),
                ws.adj_inserts.len(),
                ws.vector_inserts.len(),
                ws.vector_deletes.len(),
                ws.vector_moves.len(),
            )
        })
    }

    pub(crate) fn restore_write_set_checkpoint(
        &self,
        tx: TxId,
        checkpoint: (usize, usize, usize, usize, usize, usize),
    ) -> Result<()> {
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_inserts.truncate(checkpoint.0);
            ws.relational_deletes.truncate(checkpoint.1);
            ws.adj_inserts.truncate(checkpoint.2);
            ws.vector_inserts.truncate(checkpoint.3);
            ws.vector_deletes.truncate(checkpoint.4);
            ws.vector_moves.truncate(checkpoint.5);
        })
    }

    /// Get a clone of the current conflict policies.
    pub fn conflict_policies(&self) -> ConflictPolicies {
        self.conflict_policies.read().clone()
    }

    /// Set the default conflict policy.
    pub fn set_default_conflict_policy(&self, policy: ConflictPolicy) {
        self.conflict_policies.write().default = policy;
    }

    /// Set a per-table conflict policy.
    pub fn set_table_conflict_policy(&self, table: &str, policy: ConflictPolicy) {
        self.conflict_policies
            .write()
            .per_table
            .insert(table.to_string(), policy);
    }

    /// Remove a per-table conflict policy override.
    pub fn drop_table_conflict_policy(&self, table: &str) {
        self.conflict_policies.write().per_table.remove(table);
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

    pub(crate) fn allocate_ddl_lsn<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Lsn) -> R,
    {
        self.tx_mgr.allocate_ddl_lsn(f)
    }

    pub(crate) fn with_commit_lock<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.tx_mgr.with_commit_lock(f)
    }

    pub(crate) fn log_create_table_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        lsn: Lsn,
    ) -> Result<()> {
        let change = ddl_change_from_meta(name, meta);
        self.ddl_log.write().push((lsn, change.clone()));
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        Ok(())
    }

    pub(crate) fn log_drop_table_ddl(&self, name: &str, lsn: Lsn) -> Result<()> {
        let change = DdlChange::DropTable {
            name: name.to_string(),
        };
        self.ddl_log.write().push((lsn, change.clone()));
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        Ok(())
    }

    pub(crate) fn log_alter_table_ddl(&self, name: &str, meta: &TableMeta, lsn: Lsn) -> Result<()> {
        let change = DdlChange::AlterTable {
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
        };
        self.ddl_log.write().push((lsn, change.clone()));
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        Ok(())
    }

    pub(crate) fn log_create_index_ddl(
        &self,
        table: &str,
        name: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        lsn: Lsn,
    ) -> Result<()> {
        let change = DdlChange::CreateIndex {
            table: table.to_string(),
            name: name.to_string(),
            columns: columns.to_vec(),
        };
        self.ddl_log.write().push((lsn, change.clone()));
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        Ok(())
    }

    pub(crate) fn log_drop_index_ddl(&self, table: &str, name: &str, lsn: Lsn) -> Result<()> {
        let change = DdlChange::DropIndex {
            table: table.to_string(),
            name: name.to_string(),
        };
        self.ddl_log.write().push((lsn, change.clone()));
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        Ok(())
    }

    pub(crate) fn persist_table_meta(&self, name: &str, meta: &TableMeta) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.flush_table_meta(name, meta)?;
        }
        Ok(())
    }

    pub(crate) fn persist_memory_limit(&self, limit: Option<usize>) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            match limit {
                Some(limit) => persistence.flush_config_value("memory_limit", &limit)?,
                None => persistence.remove_config_value("memory_limit")?,
            }
        }
        Ok(())
    }

    pub fn set_disk_limit(&self, limit: Option<u64>) -> Result<()> {
        if self.persistence.is_none() {
            self.disk_limit.store(0, Ordering::SeqCst);
            return Ok(());
        }

        let ceiling = self.disk_limit_startup_ceiling();
        if let Some(ceiling) = ceiling {
            match limit {
                Some(bytes) if bytes > ceiling => {
                    return Err(Error::Other(format!(
                        "disk limit {bytes} exceeds startup ceiling {ceiling}"
                    )));
                }
                None => {
                    return Err(Error::Other(
                        "cannot remove disk limit when a startup ceiling is set".to_string(),
                    ));
                }
                _ => {}
            }
        }

        self.disk_limit.store(limit.unwrap_or(0), Ordering::SeqCst);
        Ok(())
    }

    pub fn disk_limit(&self) -> Option<u64> {
        match self.disk_limit.load(Ordering::SeqCst) {
            0 => None,
            bytes => Some(bytes),
        }
    }

    pub fn disk_limit_startup_ceiling(&self) -> Option<u64> {
        match self.disk_limit_startup_ceiling.load(Ordering::SeqCst) {
            0 => None,
            bytes => Some(bytes),
        }
    }

    pub fn disk_file_size(&self) -> Option<u64> {
        self.persistence
            .as_ref()
            .map(|persistence| std::fs::metadata(persistence.path()).map(|meta| meta.len()))
            .transpose()
            .ok()
            .flatten()
    }

    pub(crate) fn persist_disk_limit(&self, limit: Option<u64>) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            match limit {
                Some(limit) => persistence.flush_config_value("disk_limit", &limit)?,
                None => persistence.remove_config_value("disk_limit")?,
            }
        }
        Ok(())
    }

    pub fn check_disk_budget(&self, operation: &str) -> Result<()> {
        let Some(limit) = self.disk_limit() else {
            return Ok(());
        };
        let Some(current_bytes) = self.disk_file_size() else {
            return Ok(());
        };
        if current_bytes.saturating_add(MIN_DISK_WRITE_HEADROOM_BYTES) <= limit {
            return Ok(());
        }
        Err(Error::DiskBudgetExceeded {
            operation: operation.to_string(),
            current_bytes,
            budget_limit_bytes: limit,
            hint: "Reduce retained file-backed data or raise DISK_LIMIT before writing more data."
                .to_string(),
        })
    }

    pub fn persisted_sync_watermarks(&self, tenant_id: &str) -> Result<(Lsn, Lsn)> {
        let Some(persistence) = &self.persistence else {
            return Ok((Lsn(0), Lsn(0)));
        };
        let push = persistence
            .load_config_value::<u64>(&format!("sync_push_watermark:{tenant_id}"))?
            .map(Lsn)
            .unwrap_or(Lsn(0));
        let pull = persistence
            .load_config_value::<u64>(&format!("sync_pull_watermark:{tenant_id}"))?
            .map(Lsn)
            .unwrap_or(Lsn(0));
        Ok((push, pull))
    }

    pub fn persist_sync_push_watermark(&self, tenant_id: &str, watermark: Lsn) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence
                .flush_config_value(&format!("sync_push_watermark:{tenant_id}"), &watermark.0)?;
        }
        Ok(())
    }

    pub fn persist_sync_pull_watermark(&self, tenant_id: &str, watermark: Lsn) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence
                .flush_config_value(&format!("sync_pull_watermark:{tenant_id}"), &watermark.0)?;
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

    pub(crate) fn persist_vectors(&self) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            let vectors = self.vector_store.all_entries();
            persistence.rewrite_vectors(&vectors)?;
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

    pub fn change_log_since(&self, since_lsn: Lsn) -> Vec<ChangeLogEntry> {
        let log = self.change_log.read();
        let start = log.partition_point(|e| e.lsn() <= since_lsn);
        log[start..].to_vec()
    }

    pub fn ddl_log_since(&self, since_lsn: Lsn) -> Vec<DdlChange> {
        let ddl = self.ddl_log.read();
        let start = ddl.partition_point(|(lsn, _)| *lsn <= since_lsn);
        ddl[start..].iter().map(|(_, c)| c.clone()).collect()
    }

    /// Builds a complete snapshot of all live data as a ChangeSet.
    /// Used as fallback when change_log/ddl_log cannot serve a watermark.
    #[allow(dead_code)]
    fn full_state_snapshot(&self) -> ChangeSet {
        let mut rows = Vec::new();
        let mut edges = Vec::new();
        let mut vectors = Vec::new();
        let mut ddl = Vec::new();

        let meta_guard = self.relational_store.table_meta.read();
        let tables_guard = self.relational_store.tables.read();

        // DDL. A full snapshot must be directly applyable to an empty peer:
        // create joined tables and their user indexes before any table whose
        // rank policy validates against them.
        ddl.extend(full_snapshot_ddl(&meta_guard));

        // Rows (live only) — collect row_ids that have live rows for orphan vector filtering
        let mut live_row_ids: HashSet<RowId> = HashSet::new();
        for (table_name, table_rows) in tables_guard.iter() {
            let meta = match meta_guard.get(table_name) {
                Some(m) => m,
                None => continue,
            };
            let key_col = meta
                .natural_key_column
                .clone()
                .or_else(|| {
                    if meta
                        .columns
                        .iter()
                        .any(|c| c.name == "id" && c.column_type == ColumnType::Uuid)
                    {
                        Some("id".to_string())
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    meta.columns
                        .iter()
                        .find(|c| c.primary_key && c.column_type == ColumnType::Uuid)
                        .map(|c| c.name.clone())
                })
                .unwrap_or_default();
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

        // Vectors (live only, skip orphans)
        for entry in self
            .vector_store
            .all_entries()
            .into_iter()
            .filter(|v| v.deleted_tx.is_none())
        {
            if !live_row_ids.contains(&entry.row_id) {
                continue; // skip orphan vectors
            }
            vectors.push(VectorChange {
                index: entry.index.clone(),
                row_id: entry.row_id,
                vector: entry.vector,
                lsn: entry.lsn,
            });
        }

        ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
        }
    }

    fn persisted_state_since(&self, since_lsn: Lsn) -> ChangeSet {
        if since_lsn == Lsn(0) {
            return self.full_state_snapshot();
        }

        let mut rows = Vec::new();
        let mut edges = Vec::new();
        let mut vectors = Vec::new();
        let ddl = Vec::new();

        let meta_guard = self.relational_store.table_meta.read();
        let tables_guard = self.relational_store.tables.read();

        let mut live_row_ids: HashSet<RowId> = HashSet::new();
        for (table_name, table_rows) in tables_guard.iter() {
            let meta = match meta_guard.get(table_name) {
                Some(meta) => meta,
                None => continue,
            };
            let key_col = meta
                .natural_key_column
                .clone()
                .or_else(|| {
                    if meta
                        .columns
                        .iter()
                        .any(|c| c.name == "id" && c.column_type == ColumnType::Uuid)
                    {
                        Some("id".to_string())
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    meta.columns
                        .iter()
                        .find(|c| c.primary_key && c.column_type == ColumnType::Uuid)
                        .map(|c| c.name.clone())
                })
                .unwrap_or_default();
            if key_col.is_empty() {
                continue;
            }
            for row in table_rows.iter().filter(|row| row.deleted_tx.is_none()) {
                live_row_ids.insert(row.row_id);
                if row.lsn <= since_lsn {
                    continue;
                }
                let key_val = match row.values.get(&key_col) {
                    Some(value) => value.clone(),
                    None => continue,
                };
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

        let fwd = self.graph_store.forward_adj.read();
        for entries in fwd.values() {
            for entry in entries
                .iter()
                .filter(|entry| entry.deleted_tx.is_none() && entry.lsn > since_lsn)
            {
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

        for entry in self
            .vector_store
            .all_entries()
            .into_iter()
            .filter(|entry| entry.deleted_tx.is_none() && entry.lsn > since_lsn)
        {
            if !live_row_ids.contains(&entry.row_id) {
                continue;
            }
            vectors.push(VectorChange {
                index: entry.index.clone(),
                row_id: entry.row_id,
                vector: entry.vector,
                lsn: entry.lsn,
            });
        }

        ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
        }
    }

    fn preflight_sync_apply_memory(
        &self,
        changes: &ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<()> {
        let usage = self.accountant.usage();
        let Some(limit) = usage.limit else {
            return Ok(());
        };
        let available = usage.available.unwrap_or(limit);
        let mut required = 0usize;

        for row in &changes.rows {
            if row.deleted || row.values.is_empty() {
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

            if existing.is_some()
                && matches!(
                    policy,
                    ConflictPolicy::InsertIfNotExists | ConflictPolicy::ServerWins
                )
            {
                continue;
            }

            required = required.saturating_add(
                self.table_meta(&row.table)
                    .map(|meta| estimate_row_bytes_for_meta(&row.values, &meta, false))
                    .unwrap_or_else(|| estimate_row_value_bytes(&row.values)),
            );
        }

        for edge in &changes.edges {
            required = required.saturating_add(
                96 + edge.edge_type.len().saturating_mul(16)
                    + estimate_row_value_bytes(&edge.properties),
            );
        }

        for vector in &changes.vectors {
            if vector.vector.is_empty() {
                continue;
            }
            required = required.saturating_add(
                24 + vector
                    .vector
                    .len()
                    .saturating_mul(std::mem::size_of::<f32>()),
            );
        }

        if required > available {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: "sync".to_string(),
                operation: "apply_changes".to_string(),
                requested_bytes: required,
                available_bytes: available,
                budget_limit_bytes: limit,
                hint:
                    "Reduce sync batch size, split the push, or raise MEMORY_LIMIT on the server."
                        .to_string(),
            });
        }

        Ok(())
    }

    /// Extracts changes from this database since the given LSN.
    pub fn changes_since(&self, since_lsn: Lsn) -> ChangeSet {
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

        // If both logs are empty but stores have data → post-restart, derive deltas from
        // persisted row/edge/vector LSNs instead of replaying a full snapshot.
        if change_log_empty && ddl_log_empty && (has_table_data || has_table_meta) {
            return self.persisted_state_since(since_lsn);
        }

        // If logs have entries, check the minimum first-LSN across both covers since_lsn
        let min_first_lsn = match (change_first_lsn, ddl_first_lsn) {
            (Some(c), Some(d)) => Some(c.min(d)),
            (Some(c), None) => Some(c),
            (None, Some(d)) => Some(d),
            (None, None) => None, // both empty, stores empty — nothing to serve
        };

        if min_first_lsn.is_some_and(|min_lsn| min_lsn.0 > since_lsn.0 + 1) {
            // Log doesn't cover since_lsn — derive the delta from persisted state.
            return self.persisted_state_since(since_lsn);
        }

        let (ddl, change_entries) = self.with_commit_lock(|| {
            let ddl = self.ddl_log_since(since_lsn);
            let changes = self.change_log_since(since_lsn);
            (ddl, changes)
        });

        let mut rows = Vec::new();
        let mut edges = Vec::new();
        let mut vectors = Vec::new();

        for entry in change_entries {
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
                ChangeLogEntry::VectorInsert { index, row_id, lsn } => {
                    if let Some(vector) = self.vector_for_row_lsn(&index, row_id, lsn) {
                        vectors.push(VectorChange {
                            index,
                            row_id,
                            vector,
                            lsn,
                        });
                    }
                }
                ChangeLogEntry::VectorDelete { index, row_id, lsn } => vectors.push(VectorChange {
                    index,
                    row_id,
                    vector: Vec::new(),
                    lsn,
                }),
            }
        }

        // Deduplicate upserts: when a RowDelete is followed by a RowInsert for the same
        // (table, natural_key), the delete is part of an upsert — remove it.
        // Only remove a delete if there is a non-delete entry with a HIGHER LSN
        // (i.e., the insert came after the delete, indicating an upsert).
        // If the insert has a lower LSN, the delete is genuine and must be kept.
        let insert_max_lsn: HashMap<(String, String, String), Lsn> = {
            let mut map: HashMap<(String, String, String), Lsn> = HashMap::new();
            for r in rows.iter().filter(|r| !r.deleted) {
                let key = (
                    r.table.clone(),
                    r.natural_key.column.clone(),
                    format!("{:?}", r.natural_key.value),
                );
                let entry = map.entry(key).or_insert(Lsn(0));
                if r.lsn > *entry {
                    *entry = r.lsn;
                }
            }
            map
        };
        rows.retain(|r| {
            if r.deleted {
                let key = (
                    r.table.clone(),
                    r.natural_key.column.clone(),
                    format!("{:?}", r.natural_key.value),
                );
                // Keep the delete unless there is a subsequent insert (higher or equal LSN).
                // Equal LSN means the delete+insert are part of the same upsert transaction.
                match insert_max_lsn.get(&key) {
                    Some(&insert_lsn) => insert_lsn < r.lsn,
                    None => true,
                }
            } else {
                true
            }
        });

        let vector_reinserts: HashSet<(VectorIndexRef, RowId, Lsn)> = vectors
            .iter()
            .filter(|v| !v.vector.is_empty())
            .map(|v| (v.index.clone(), v.row_id, v.lsn))
            .collect();
        vectors.retain(|v| {
            !v.vector.is_empty() || !vector_reinserts.contains(&(v.index.clone(), v.row_id, v.lsn))
        });

        ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
        }
    }

    /// Returns the current LSN of this database.
    pub fn current_lsn(&self) -> Lsn {
        self.tx_mgr.current_lsn()
    }

    /// Returns the highest-committed TxId on this database.
    pub fn committed_watermark(&self) -> TxId {
        self.tx_mgr.current_tx_max()
    }

    /// Returns the next TxId the allocator will issue on this database.
    pub fn next_tx(&self) -> TxId {
        self.tx_mgr.peek_next_tx()
    }

    pub fn register_cron_callback<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database) -> Result<()> + Send + Sync + 'static,
    {
        let _ = (name, callback);
        Ok(())
    }

    pub fn cron_run_due_now_for_test(&self) -> Result<u64> {
        Ok(0)
    }

    pub fn pause_cron_tickler_for_test(&self) -> CronPauseGuard {
        CronPauseGuard { _private: () }
    }

    pub fn pause_after_relational_apply_for_test(&self) -> ApplyPhasePauseGuard {
        ApplyPhasePauseGuard { _private: () }
    }

    pub fn cron_audit_log_for_test(&self) -> Vec<CronAuditEntry> {
        Vec::new()
    }

    /// Subscribe to commit events. Returns a receiver that yields a `CommitEvent`
    /// after each commit.
    pub fn subscribe(&self) -> Receiver<CommitEvent> {
        self.subscribe_with_capacity(DEFAULT_SUBSCRIPTION_CAPACITY)
    }

    /// Subscribe with a custom channel capacity.
    pub fn subscribe_with_capacity(&self, capacity: usize) -> Receiver<CommitEvent> {
        let (tx, rx) = mpsc::sync_channel(capacity.max(1));
        self.subscriptions.lock().subscribers.push(tx);
        rx
    }

    pub fn register_sink<F>(
        &self,
        name: &str,
        principal: Option<contextdb_core::types::Principal>,
        deliver: F,
    ) -> Result<()>
    where
        F: Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync + 'static,
    {
        let _ = (name, principal, deliver);
        Ok(())
    }

    #[doc(hidden)]
    pub fn __debug_register_sink_with_constraints_for_test<F>(
        &self,
        name: &str,
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
        deliver: F,
    ) -> Result<()>
    where
        F: Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync + 'static,
    {
        let _ = (name, contexts, scope_labels, principal, deliver);
        Ok(())
    }

    pub fn sink_metrics_for_test(&self, sink: &str) -> SinkMetrics {
        let _ = sink;
        SinkMetrics::default()
    }

    /// Returns health metrics for the subscription system.
    pub fn subscription_health(&self) -> SubscriptionMetrics {
        let subscriptions = self.subscriptions.lock();
        SubscriptionMetrics {
            active_channels: subscriptions.subscribers.len(),
            events_sent: subscriptions.events_sent,
            events_dropped: subscriptions.events_dropped,
        }
    }

    /// Applies a ChangeSet to this database with the given conflict policies.
    pub fn apply_changes(
        &self,
        mut changes: ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<ApplyResult> {
        // Per I14: the whole batch takes the index-maintenance lock once.
        // Per-row commits reuse the same guard via the per-row apply that
        // runs inside the tx manager's commit_mutex, so no second write
        // acquisition happens for the scope of this call.
        self.relational_store.bump_index_write_lock_count();
        self.plugin.on_sync_pull(&mut changes)?;
        self.check_disk_budget("sync_pull")?;
        self.preflight_sync_apply_memory(&changes, policies)?;

        // Pre-scan for TxId overflow so the allocator is untouched on rejection.
        for row in &changes.rows {
            for v in row.values.values() {
                if let Value::TxId(incoming) = v
                    && incoming.0 == u64::MAX
                {
                    return Err(Error::TxIdOverflow {
                        table: row.table.clone(),
                        incoming: u64::MAX,
                    });
                }
            }
        }

        let mut tx = self.begin();
        let batch_row_commits = changes.rows.len() > 128;
        let mut result = ApplyResult {
            applied_rows: 0,
            skipped_rows: 0,
            conflicts: Vec::new(),
            new_lsn: self.current_lsn(),
        };
        let vector_row_ids = changes.vectors.iter().map(|v| v.row_id).collect::<Vec<_>>();
        let mut vector_row_map: HashMap<RowId, RowId> = HashMap::new();
        let mut vector_row_idx = 0usize;
        let mut failed_row_ids: HashSet<RowId> = HashSet::new();
        let mut table_meta_cache: HashMap<String, Option<TableMeta>> = HashMap::new();
        let mut visible_rows_cache: HashMap<String, Vec<VersionedRow>> = HashMap::new();

        for ddl in changes.ddl.clone() {
            match ddl {
                DdlChange::CreateTable {
                    name,
                    columns,
                    constraints,
                } => {
                    if self.table_meta(&name).is_some() {
                        if let Some(local_meta) = self.table_meta(&name) {
                            let local_cols: Vec<(String, String)> = local_meta
                                .columns
                                .iter()
                                .map(|c| {
                                    (
                                        c.name.clone(),
                                        normalize_schema_type(&sql_type_for_meta_column(
                                            c,
                                            &local_meta.propagation_rules,
                                        )),
                                    )
                                })
                                .collect();
                            let remote_cols: Vec<(String, String)> = columns
                                .iter()
                                .map(|(col_name, col_type)| {
                                    (col_name.clone(), normalize_schema_type(col_type))
                                })
                                .collect();
                            let mut local_sorted = local_cols.clone();
                            local_sorted.sort();
                            let mut remote_sorted = remote_cols.clone();
                            remote_sorted.sort();
                            if local_sorted != remote_sorted {
                                result.conflicts.push(Conflict {
                                    natural_key: NaturalKey {
                                        column: "table".to_string(),
                                        value: Value::Text(name.clone()),
                                    },
                                    resolution: ConflictPolicy::ServerWins,
                                    reason: Some(format!(
                                        "schema mismatch: local columns {:?} differ from remote {:?}",
                                        local_cols, remote_cols
                                    )),
                                });
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
                    self.clear_statement_cache();
                    table_meta_cache.remove(&name);
                    visible_rows_cache.remove(&name);
                }
                DdlChange::DropTable { name } => {
                    if self.table_meta(&name).is_some() {
                        if let Some(block) =
                            crate::executor::rank_policy_drop_table_blocker(self, &name)
                        {
                            return Err(block);
                        }
                        self.drop_table_aux_state(&name);
                        self.remove_rank_formulas_for_table(&name);
                        self.relational_store().drop_table(&name);
                        self.remove_persisted_table(&name)?;
                        self.clear_statement_cache();
                    }
                    table_meta_cache.remove(&name);
                    visible_rows_cache.remove(&name);
                }
                DdlChange::AlterTable {
                    name,
                    columns,
                    constraints,
                } => {
                    if self.table_meta(&name).is_none() {
                        continue;
                    }
                    let existing = self.table_meta(&name).unwrap_or_default();
                    let existing_cols: HashSet<String> =
                        existing.columns.iter().map(|c| c.name.clone()).collect();
                    for (col, ty) in columns {
                        if existing_cols.contains(&col) {
                            continue;
                        }
                        let sql = format!("ALTER TABLE {} ADD COLUMN {} {}", name, col, ty);
                        self.execute_in_tx(tx, &sql, &HashMap::new())?;
                    }
                    let _ = constraints;
                    self.clear_statement_cache();
                    table_meta_cache.remove(&name);
                    visible_rows_cache.remove(&name);
                }
                DdlChange::CreateIndex {
                    table,
                    name,
                    columns,
                } => {
                    // Apply at the receiver: write IndexDecl into
                    // TableMeta.indexes, register storage, rebuild over
                    // locally-resident rows. Emit a matching DDL log entry.
                    // Silently skipping on missing table would hide sync
                    // divergence; surface it as TableNotFound so the caller
                    // can see which index couldn't land.
                    if self.table_meta(&table).is_none() {
                        return Err(Error::TableNotFound(table.clone()));
                    }
                    let already = self
                        .table_meta(&table)
                        .map(|m| m.indexes.iter().any(|i| i.name == name))
                        .unwrap_or(false);
                    if !already {
                        {
                            let store = self.relational_store();
                            let mut metas = store.table_meta.write();
                            if let Some(m) = metas.get_mut(&table) {
                                m.indexes.push(contextdb_core::IndexDecl {
                                    name: name.clone(),
                                    columns: columns.clone(),
                                    kind: contextdb_core::IndexKind::UserDeclared,
                                });
                            }
                        }
                        self.relational_store().create_index_storage(
                            &table,
                            &name,
                            columns.clone(),
                        );
                        self.relational_store().rebuild_index(&table, &name);
                        if let Some(table_meta) = self.table_meta(&table) {
                            self.persist_table_meta(&table, &table_meta)?;
                        }
                        self.allocate_ddl_lsn(|lsn| {
                            self.log_create_index_ddl(&table, &name, &columns, lsn)
                        })?;
                        self.clear_statement_cache();
                    }
                    table_meta_cache.remove(&table);
                }
                DdlChange::DropIndex { table, name } => {
                    if self.table_meta(&table).is_some() {
                        let exists = self
                            .table_meta(&table)
                            .map(|m| m.indexes.iter().any(|i| i.name == name))
                            .unwrap_or(false);
                        if exists {
                            if let Some(block) =
                                crate::executor::rank_policy_drop_index_blocker(self, &table, &name)
                            {
                                return Err(block);
                            }
                            {
                                let store = self.relational_store();
                                let mut metas = store.table_meta.write();
                                if let Some(m) = metas.get_mut(&table) {
                                    m.indexes.retain(|i| i.name != name);
                                }
                            }
                            self.relational_store().drop_index_storage(&table, &name);
                            if let Some(table_meta) = self.table_meta(&table) {
                                self.persist_table_meta(&table, &table_meta)?;
                            }
                            self.allocate_ddl_lsn(|lsn| {
                                self.log_drop_index_ddl(&table, &name, lsn)
                            })?;
                            self.clear_statement_cache();
                        }
                    }
                    table_meta_cache.remove(&table);
                }
            }
        }

        self.preflight_sync_apply_memory(&changes, policies)?;

        for row in changes.rows {
            if row.values.is_empty() {
                result.skipped_rows += 1;
                if !batch_row_commits {
                    self.commit_with_source(tx, CommitSource::SyncPull)?;
                    tx = self.begin();
                }
                continue;
            }

            let policy = policies
                .per_table
                .get(&row.table)
                .copied()
                .unwrap_or(policies.default);

            let existing = cached_point_lookup(
                self,
                &mut visible_rows_cache,
                &row.table,
                &row.natural_key.column,
                &row.natural_key.value,
            )?;
            let is_delete = row.deleted;
            let row_has_vector = cached_table_meta(self, &mut table_meta_cache, &row.table)
                .is_some_and(|meta| {
                    meta.columns
                        .iter()
                        .any(|col| matches!(col.column_type, ColumnType::Vector(_)))
                });

            if is_delete {
                if let Some(local) = existing {
                    if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                        consume_vector_row_group(
                            &vector_row_ids,
                            &mut vector_row_idx,
                            local.row_id,
                            &mut vector_row_map,
                        );
                    }
                    if let Err(err) = self.delete_row(tx, &row.table, local.row_id) {
                        result.conflicts.push(Conflict {
                            natural_key: row.natural_key.clone(),
                            resolution: policy,
                            reason: Some(format!("delete failed: {err}")),
                        });
                        result.skipped_rows += 1;
                    } else {
                        remove_cached_row(&mut visible_rows_cache, &row.table, local.row_id);
                        result.applied_rows += 1;
                    }
                } else {
                    result.skipped_rows += 1;
                }
                if !batch_row_commits {
                    self.commit_with_source(tx, CommitSource::SyncPull)?;
                    tx = self.begin();
                }
                continue;
            }

            let mut values = row.values.clone();
            values.remove("__deleted");

            match (existing, policy) {
                (None, _) => {
                    if let Some(meta) = cached_table_meta(self, &mut table_meta_cache, &row.table) {
                        let mut constraint_error: Option<String> = None;

                        for col_def in &meta.columns {
                            if !col_def.nullable
                                && !col_def.primary_key
                                && col_def.default.is_none()
                            {
                                match values.get(&col_def.name) {
                                    None | Some(Value::Null) => {
                                        constraint_error = Some(format!(
                                            "NOT NULL constraint violated: {}.{}",
                                            row.table, col_def.name
                                        ));
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }

                        let has_unique = meta.columns.iter().any(|c| c.unique && !c.primary_key);
                        if constraint_error.is_none() && has_unique {
                            for col_def in &meta.columns {
                                if col_def.unique
                                    && !col_def.primary_key
                                    && let Some(new_val) = values.get(&col_def.name)
                                    && *new_val != Value::Null
                                    && cached_visible_rows(
                                        self,
                                        &mut visible_rows_cache,
                                        &row.table,
                                    )?
                                    .iter()
                                    .any(|r| r.values.get(&col_def.name) == Some(new_val))
                                {
                                    constraint_error = Some(format!(
                                        "UNIQUE constraint violated: {}.{}",
                                        row.table, col_def.name
                                    ));
                                    break;
                                }
                            }
                        }

                        if let Some(err_msg) = constraint_error {
                            result.skipped_rows += 1;
                            if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                                consume_failed_vector_row_group(
                                    &vector_row_ids,
                                    &mut vector_row_idx,
                                    &mut failed_row_ids,
                                );
                            }
                            result.conflicts.push(Conflict {
                                natural_key: row.natural_key.clone(),
                                resolution: policy,
                                reason: Some(err_msg),
                            });
                            if !batch_row_commits {
                                self.commit_with_source(tx, CommitSource::SyncPull)?;
                                tx = self.begin();
                            }
                            continue;
                        }
                    }

                    // Sync-apply overflow guard + allocator/watermark advance for Value::TxId cells.
                    let mut overflow: Option<Error> = None;
                    for v in values.values() {
                        if let Value::TxId(incoming) = v
                            && let Err(err) = self.tx_mgr.advance_for_sync(&row.table, *incoming)
                        {
                            overflow = Some(err);
                            break;
                        }
                    }
                    if let Some(err) = overflow {
                        return Err(err);
                    }

                    match self.insert_row_for_sync(tx, &row.table, values.clone()) {
                        Ok(new_row_id) => {
                            record_cached_insert(
                                &mut visible_rows_cache,
                                &row.table,
                                VersionedRow {
                                    row_id: new_row_id,
                                    values: values.clone(),
                                    created_tx: tx,
                                    deleted_tx: None,
                                    lsn: row.lsn,
                                    created_at: None,
                                },
                            );
                            result.applied_rows += 1;
                            if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                                consume_vector_row_group(
                                    &vector_row_ids,
                                    &mut vector_row_idx,
                                    new_row_id,
                                    &mut vector_row_map,
                                );
                            }
                        }
                        Err(err) => {
                            if is_fatal_sync_apply_error(&err) {
                                return Err(err);
                            }
                            result.skipped_rows += 1;
                            if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                                consume_failed_vector_row_group(
                                    &vector_row_ids,
                                    &mut vector_row_idx,
                                    &mut failed_row_ids,
                                );
                            }
                            result.conflicts.push(Conflict {
                                natural_key: row.natural_key.clone(),
                                resolution: policy,
                                reason: Some(format!("{err}")),
                            });
                        }
                    }
                }
                (Some(local), ConflictPolicy::InsertIfNotExists) => {
                    if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                        consume_vector_row_group(
                            &vector_row_ids,
                            &mut vector_row_idx,
                            local.row_id,
                            &mut vector_row_map,
                        );
                    }
                    result.skipped_rows += 1;
                }
                (Some(_), ConflictPolicy::ServerWins) => {
                    result.skipped_rows += 1;
                    if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                        consume_failed_vector_row_group(
                            &vector_row_ids,
                            &mut vector_row_idx,
                            &mut failed_row_ids,
                        );
                    }
                    result.conflicts.push(Conflict {
                        natural_key: row.natural_key.clone(),
                        resolution: ConflictPolicy::ServerWins,
                        reason: Some("server_wins".to_string()),
                    });
                }
                (Some(local), ConflictPolicy::LatestWins) => {
                    // Deterministic tie-break when LSNs match: if both rows carry a
                    // `Value::TxId` cell under the same column name, the row with the
                    // strictly greater raw u64 wins. Otherwise fall back to the strict
                    // "incoming must exceed local" rule.
                    let incoming_wins = if row.lsn == local.lsn {
                        let mut winner = false;
                        for (col, incoming_val) in values.iter() {
                            if let (Value::TxId(incoming_tx), Some(Value::TxId(local_tx))) =
                                (incoming_val, local.values.get(col))
                            {
                                if incoming_tx.0 > local_tx.0 {
                                    winner = true;
                                    break;
                                } else if incoming_tx.0 < local_tx.0 {
                                    winner = false;
                                    break;
                                }
                            }
                        }
                        winner
                    } else {
                        row.lsn > local.lsn
                    };

                    if !incoming_wins {
                        result.skipped_rows += 1;
                        if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                            consume_failed_vector_row_group(
                                &vector_row_ids,
                                &mut vector_row_idx,
                                &mut failed_row_ids,
                            );
                        }
                        result.conflicts.push(Conflict {
                            natural_key: row.natural_key.clone(),
                            resolution: ConflictPolicy::LatestWins,
                            reason: Some("local_lsn_newer_or_equal".to_string()),
                        });
                    } else {
                        // State machine conflict detection
                        if let Some(meta) = self.table_meta(&row.table)
                            && let Some(sm) = &meta.state_machine
                        {
                            let sm_col = sm.column.clone();
                            let transitions = sm.transitions.clone();
                            let incoming_state = values.get(&sm_col).and_then(|v| match v {
                                Value::Text(s) => Some(s.clone()),
                                _ => None,
                            });
                            let local_state = local.values.get(&sm_col).and_then(|v| match v {
                                Value::Text(s) => Some(s.clone()),
                                _ => None,
                            });

                            if let (Some(incoming), Some(current)) = (incoming_state, local_state) {
                                // Check if the transition from current to incoming is valid
                                let valid = transitions
                                    .get(&current)
                                    .is_some_and(|targets| targets.contains(&incoming));
                                if !valid && incoming != current {
                                    result.skipped_rows += 1;
                                    if row_has_vector
                                        && vector_row_ids.get(vector_row_idx).is_some()
                                    {
                                        consume_failed_vector_row_group(
                                            &vector_row_ids,
                                            &mut vector_row_idx,
                                            &mut failed_row_ids,
                                        );
                                    }
                                    result.conflicts.push(Conflict {
                                        natural_key: row.natural_key.clone(),
                                        resolution: ConflictPolicy::LatestWins,
                                        reason: Some(format!(
                                            "state_machine: invalid transition {} -> {} (current: {})",
                                            current, incoming, current
                                        )),
                                    });
                                    if !batch_row_commits {
                                        self.commit_with_source(tx, CommitSource::SyncPull)?;
                                        tx = self.begin();
                                    }
                                    continue;
                                }
                            }
                        }

                        // Sync-apply overflow guard + allocator/watermark advance.
                        let mut overflow: Option<Error> = None;
                        for v in values.values() {
                            if let Value::TxId(incoming) = v
                                && let Err(err) =
                                    self.tx_mgr.advance_for_sync(&row.table, *incoming)
                            {
                                overflow = Some(err);
                                break;
                            }
                        }
                        if let Some(err) = overflow {
                            return Err(err);
                        }

                        match self.upsert_row_for_sync(
                            tx,
                            &row.table,
                            &row.natural_key.column,
                            values.clone(),
                        ) {
                            Ok(_) => {
                                visible_rows_cache.remove(&row.table);
                                result.applied_rows += 1;
                                if row_has_vector
                                    && vector_row_ids.get(vector_row_idx).is_some()
                                    && let Ok(Some(found)) = self.point_lookup_in_tx(
                                        tx,
                                        &row.table,
                                        &row.natural_key.column,
                                        &row.natural_key.value,
                                        self.snapshot(),
                                    )
                                {
                                    consume_vector_row_group(
                                        &vector_row_ids,
                                        &mut vector_row_idx,
                                        found.row_id,
                                        &mut vector_row_map,
                                    );
                                }
                            }
                            Err(err) => {
                                if is_fatal_sync_apply_error(&err) {
                                    return Err(err);
                                }
                                result.skipped_rows += 1;
                                if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                                    consume_failed_vector_row_group(
                                        &vector_row_ids,
                                        &mut vector_row_idx,
                                        &mut failed_row_ids,
                                    );
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
                    let mut overflow: Option<Error> = None;
                    for v in values.values() {
                        if let Value::TxId(incoming) = v
                            && let Err(err) = self.tx_mgr.advance_for_sync(&row.table, *incoming)
                        {
                            overflow = Some(err);
                            break;
                        }
                    }
                    if let Some(err) = overflow {
                        return Err(err);
                    }

                    match self.upsert_row_for_sync(
                        tx,
                        &row.table,
                        &row.natural_key.column,
                        values.clone(),
                    ) {
                        Ok(_) => {
                            visible_rows_cache.remove(&row.table);
                            result.applied_rows += 1;
                            if row_has_vector
                                && vector_row_ids.get(vector_row_idx).is_some()
                                && let Ok(Some(found)) = self.point_lookup_in_tx(
                                    tx,
                                    &row.table,
                                    &row.natural_key.column,
                                    &row.natural_key.value,
                                    self.snapshot(),
                                )
                            {
                                consume_vector_row_group(
                                    &vector_row_ids,
                                    &mut vector_row_idx,
                                    found.row_id,
                                    &mut vector_row_map,
                                );
                            }
                        }
                        Err(err) => {
                            if is_fatal_sync_apply_error(&err) {
                                return Err(err);
                            }
                            result.skipped_rows += 1;
                            if row_has_vector && vector_row_ids.get(vector_row_idx).is_some() {
                                consume_failed_vector_row_group(
                                    &vector_row_ids,
                                    &mut vector_row_idx,
                                    &mut failed_row_ids,
                                );
                            }
                            if let Some(last) = result.conflicts.last_mut() {
                                last.reason = Some(format!("state_machine_or_constraint: {err}"));
                            }
                        }
                    }
                }
            }

            if !batch_row_commits {
                self.commit_with_source(tx, CommitSource::SyncPull)?;
                tx = self.begin();
            }
        }

        if batch_row_commits {
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

        for vector in changes.vectors {
            if failed_row_ids.contains(&vector.row_id) {
                continue; // skip vectors for rows that failed to insert
            }
            let local_row_id = vector_row_map
                .get(&vector.row_id)
                .copied()
                .unwrap_or(vector.row_id);
            if vector.vector.is_empty() {
                self.delete_vector(tx, vector.index.clone(), local_row_id)?;
            } else {
                if self.has_live_vector(local_row_id, self.snapshot()) {
                    let _ = self.delete_vector(tx, vector.index.clone(), local_row_id);
                }
                self.insert_vector_strict(tx, vector.index.clone(), local_row_id, vector.vector)?;
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
            })
            .or_else(|| {
                meta.get(table).and_then(|m| {
                    m.columns
                        .iter()
                        .find(|c| c.primary_key && c.column_type == ColumnType::Uuid)
                        .map(|c| c.name.clone())
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
        lsn: Lsn,
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

    fn vector_for_row_lsn(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        lsn: Lsn,
    ) -> Option<Vec<f32>> {
        self.vector_store.vector_for_row_lsn(index, row_id, lsn)
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

fn cached_table_meta(
    db: &Database,
    cache: &mut HashMap<String, Option<TableMeta>>,
    table: &str,
) -> Option<TableMeta> {
    cache
        .entry(table.to_string())
        .or_insert_with(|| db.table_meta(table))
        .clone()
}

pub(crate) fn rank_index_name(table: &str, column: &str) -> String {
    format!("{table}.{column}")
}

fn rank_value_to_number(
    value: &Value,
    column: &str,
) -> std::result::Result<Option<f32>, FormulaEvalError> {
    match value {
        Value::Null => Ok(None),
        Value::Float64(value) => Ok(Some(*value as f32)),
        Value::Int64(value) => Ok(Some(*value as f32)),
        Value::Bool(value) => Ok(Some(if *value { 1.0 } else { 0.0 })),
        Value::Text(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TEXT",
        }),
        Value::Json(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "JSON",
        }),
        Value::Uuid(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "UUID",
        }),
        Value::Vector(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "VECTOR",
        }),
        Value::Timestamp(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TIMESTAMP",
        }),
        Value::TxId(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TXID",
        }),
    }
}

fn merged_rank_values(
    anchor: &VersionedRow,
    joined: Option<&VersionedRow>,
) -> HashMap<String, Value> {
    let mut values = anchor.values.clone();
    if let Some(joined) = joined {
        for (key, value) in &joined.values {
            values.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    values
}

fn values_equal_for_rank_join(left: &Value, right: &Value) -> bool {
    if matches!((left, right), (Value::Null, _) | (_, Value::Null)) {
        return false;
    }
    left == right
}

fn compare_ranked_results(left: &SearchResult, right: &SearchResult) -> std::cmp::Ordering {
    rank_float_desc(left.rank, right.rank)
        .then_with(|| rank_float_desc(left.vector_score, right.vector_score))
        .then_with(|| right.row_id.cmp(&left.row_id))
}

fn rank_float_desc(left: f32, right: f32) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => right.total_cmp(&left),
    }
}

fn cached_visible_rows<'a>(
    db: &Database,
    cache: &'a mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
) -> Result<&'a mut Vec<VersionedRow>> {
    if !cache.contains_key(table) {
        let rows = db.scan(table, db.snapshot())?;
        cache.insert(table.to_string(), rows);
    }
    Ok(cache.get_mut(table).expect("cached visible rows"))
}

fn cached_point_lookup(
    db: &Database,
    cache: &mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
    col: &str,
    value: &Value,
) -> Result<Option<VersionedRow>> {
    let rows = cached_visible_rows(db, cache, table)?;
    Ok(rows
        .iter()
        .find(|r| r.values.get(col) == Some(value))
        .cloned())
}

fn record_cached_insert(
    cache: &mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
    row: VersionedRow,
) {
    if let Some(rows) = cache.get_mut(table) {
        rows.push(row);
    }
}

fn consume_vector_row_group(
    remote_row_ids: &[RowId],
    cursor: &mut usize,
    local_row_id: RowId,
    map: &mut HashMap<RowId, RowId>,
) {
    let Some(remote_row_id) = remote_row_ids.get(*cursor).copied() else {
        return;
    };
    while remote_row_ids.get(*cursor).copied() == Some(remote_row_id) {
        map.insert(remote_row_id, local_row_id);
        *cursor += 1;
    }
}

fn consume_failed_vector_row_group(
    remote_row_ids: &[RowId],
    cursor: &mut usize,
    failed: &mut HashSet<RowId>,
) {
    let Some(remote_row_id) = remote_row_ids.get(*cursor).copied() else {
        return;
    };
    while remote_row_ids.get(*cursor).copied() == Some(remote_row_id) {
        failed.insert(remote_row_id);
        *cursor += 1;
    }
}

fn vector_index_from_plan(plan: &PhysicalPlan) -> Option<VectorIndexRef> {
    match plan {
        PhysicalPlan::VectorSearch { table, column, .. }
        | PhysicalPlan::HnswSearch { table, column, .. } => {
            Some(VectorIndexRef::new(table.clone(), column.clone()))
        }
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. } => vector_index_from_plan(input),
        PhysicalPlan::Join { left, right, .. } => {
            vector_index_from_plan(left).or_else(|| vector_index_from_plan(right))
        }
        PhysicalPlan::Pipeline(plans) => plans.iter().find_map(vector_index_from_plan),
        _ => None,
    }
}

fn hydrate_relational_vector_values(relational: &RelationalStore, vectors: &[VectorEntry]) {
    if vectors.is_empty() {
        return;
    }
    let mut tables = relational.tables.write();
    for entry in vectors {
        let Some(rows) = tables.get_mut(&entry.index.table) else {
            continue;
        };
        if let Some(row) = rows.iter_mut().find(|row| row.row_id == entry.row_id) {
            row.values.insert(
                entry.index.column.clone(),
                Value::Vector(entry.vector.clone()),
            );
        }
    }
}

fn remove_cached_row(cache: &mut HashMap<String, Vec<VersionedRow>>, table: &str, row_id: RowId) {
    if let Some(rows) = cache.get_mut(table) {
        rows.retain(|row| row.row_id != row_id);
    }
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

fn maybe_prebuild_hnsw(vector_store: &VectorStore, accountant: &MemoryAccountant) {
    let _ = (vector_store, accountant);
}

fn estimate_row_bytes_for_meta(
    values: &HashMap<ColName, Value>,
    meta: &TableMeta,
    include_vectors: bool,
) -> usize {
    let mut bytes = 96usize;
    for column in &meta.columns {
        let Some(value) = values.get(&column.name) else {
            continue;
        };
        if !include_vectors && matches!(column.column_type, ColumnType::Vector(_)) {
            continue;
        }
        bytes = bytes.saturating_add(32 + column.name.len() * 8 + value.estimated_bytes());
    }
    bytes
}

fn estimate_edge_bytes(
    source: NodeId,
    target: NodeId,
    edge_type: &str,
    properties: &HashMap<String, Value>,
) -> usize {
    AdjEntry {
        source,
        target,
        edge_type: edge_type.to_string(),
        properties: properties.clone(),
        created_tx: TxId(0),
        deleted_tx: None,
        lsn: Lsn(0),
    }
    .estimated_bytes()
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        let runtime = self.pruning_runtime.get_mut();
        runtime.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = runtime.handle.take() {
            let _ = handle.join();
        }
        self.subscriptions.lock().subscribers.clear();
        if let Some(persistence) = &self.persistence {
            persistence.close();
        }
    }
}

fn sleep_with_shutdown(shutdown: &AtomicBool, interval: Duration) {
    let deadline = Instant::now() + interval;
    while !shutdown.load(Ordering::SeqCst) {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn prune_expired_rows(
    relational_store: &Arc<RelationalStore>,
    graph_store: &Arc<GraphStore>,
    vector_store: &Arc<VectorStore>,
    accountant: &MemoryAccountant,
    persistence: Option<&Arc<RedbPersistence>>,
    sync_watermark: Lsn,
) -> u64 {
    let now = Wallclock::now();
    let metas = relational_store.table_meta.read().clone();
    let mut pruned_by_table: HashMap<String, Vec<RowId>> = HashMap::new();
    let mut pruned_node_ids = HashSet::new();
    let mut released_row_bytes = 0usize;

    {
        let mut tables = relational_store.tables.write();
        for (table_name, rows) in tables.iter_mut() {
            let Some(meta) = metas.get(table_name) else {
                continue;
            };
            if meta.default_ttl_seconds.is_none() {
                continue;
            }

            rows.retain(|row| {
                if !row_is_prunable(row, meta, now, sync_watermark) {
                    return true;
                }

                pruned_by_table
                    .entry(table_name.clone())
                    .or_default()
                    .push(row.row_id);
                released_row_bytes = released_row_bytes
                    .saturating_add(estimate_row_bytes_for_meta(&row.values, meta, false));
                if let Some(Value::Uuid(id)) = row.values.get("id") {
                    pruned_node_ids.insert(*id);
                }
                false
            });
        }
    }

    let pruned_row_ids: HashSet<RowId> = pruned_by_table
        .values()
        .flat_map(|rows| rows.iter().copied())
        .collect();
    if pruned_row_ids.is_empty() {
        return 0;
    }

    let released_vector_bytes = vector_store.prune_row_ids(&pruned_row_ids, accountant);

    let mut released_edge_bytes = 0usize;
    {
        let mut forward = graph_store.forward_adj.write();
        for entries in forward.values_mut() {
            entries.retain(|entry| {
                if pruned_node_ids.contains(&entry.source)
                    || pruned_node_ids.contains(&entry.target)
                {
                    released_edge_bytes =
                        released_edge_bytes.saturating_add(entry.estimated_bytes());
                    false
                } else {
                    true
                }
            });
        }
        forward.retain(|_, entries| !entries.is_empty());
    }
    {
        let mut reverse = graph_store.reverse_adj.write();
        for entries in reverse.values_mut() {
            entries.retain(|entry| {
                !pruned_node_ids.contains(&entry.source) && !pruned_node_ids.contains(&entry.target)
            });
        }
        reverse.retain(|_, entries| !entries.is_empty());
    }

    if let Some(persistence) = persistence {
        for table_name in pruned_by_table.keys() {
            let rows = relational_store
                .tables
                .read()
                .get(table_name)
                .cloned()
                .unwrap_or_default();
            let _ = persistence.rewrite_table_rows(table_name, &rows);
        }

        let vectors = vector_store.all_entries();
        let edges = graph_store
            .forward_adj
            .read()
            .values()
            .flat_map(|entries| entries.iter().cloned())
            .collect::<Vec<_>>();
        let _ = persistence.rewrite_vectors(&vectors);
        let _ = persistence.rewrite_graph_edges(&edges);
    }

    accountant.release(
        released_row_bytes
            .saturating_add(released_vector_bytes)
            .saturating_add(released_edge_bytes),
    );

    pruned_row_ids.len() as u64
}

fn row_is_prunable(
    row: &VersionedRow,
    meta: &TableMeta,
    now: Wallclock,
    sync_watermark: Lsn,
) -> bool {
    if meta.sync_safe && row.lsn >= sync_watermark {
        return false;
    }

    let Some(default_ttl_seconds) = meta.default_ttl_seconds else {
        return false;
    };

    if let Some(expires_column) = &meta.expires_column {
        match row.values.get(expires_column) {
            Some(Value::Timestamp(millis)) if *millis == i64::MAX => return false,
            Some(Value::Timestamp(millis)) if *millis < 0 => return true,
            Some(Value::Timestamp(millis)) => return (*millis as u64) <= now.0,
            Some(Value::Null) | None => {}
            Some(_) => {}
        }
    }

    let ttl_millis = default_ttl_seconds.saturating_mul(1000);
    row.created_at
        .map(|created_at| now.0.saturating_sub(created_at.0) > ttl_millis)
        .unwrap_or(false)
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
        .unwrap_or(TxId(0));
    let graph_max = graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter())
        .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx))
        .max()
        .unwrap_or(TxId(0));
    let vector_max = vector
        .all_entries()
        .into_iter()
        .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx))
        .max()
        .unwrap_or(TxId(0));

    relational_max.max(graph_max).max(vector_max)
}

fn max_lsn_across_all(
    relational: &RelationalStore,
    graph: &GraphStore,
    vector: &VectorStore,
) -> Lsn {
    let relational_max = relational
        .tables
        .read()
        .values()
        .flat_map(|rows| rows.iter().map(|row| row.lsn))
        .max()
        .unwrap_or(Lsn(0));
    let graph_max = graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter().map(|entry| entry.lsn))
        .max()
        .unwrap_or(Lsn(0));
    let vector_max = vector
        .all_entries()
        .into_iter()
        .map(|entry| entry.lsn)
        .max()
        .unwrap_or(Lsn(0));

    relational_max.max(graph_max).max(vector_max)
}

fn persisted_memory_limit(path: &Path) -> Result<Option<usize>> {
    if !path.exists() {
        return Ok(None);
    }
    let persistence = RedbPersistence::open(path)?;
    let limit = persistence.load_config_value::<usize>("memory_limit")?;
    persistence.close();
    Ok(limit)
}

fn is_fatal_sync_apply_error(err: &Error) -> bool {
    matches!(
        err,
        Error::MemoryBudgetExceeded { .. } | Error::DiskBudgetExceeded { .. }
    )
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
    ddl_change_from_meta_excluding(name, meta, &HashSet::new())
}

fn ddl_change_from_meta_excluding(
    name: &str,
    meta: &TableMeta,
    excluded_columns: &HashSet<String>,
) -> DdlChange {
    DdlChange::CreateTable {
        name: name.to_string(),
        columns: meta
            .columns
            .iter()
            .filter(|col| !excluded_columns.contains(&col.name))
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

fn full_snapshot_ddl(metas: &HashMap<String, TableMeta>) -> Vec<DdlChange> {
    let mut names = metas.keys().cloned().collect::<Vec<_>>();
    names.sort();

    let mut emitted = HashSet::new();
    let mut ddl = Vec::new();
    while emitted.len() < names.len() {
        let before = emitted.len();
        for name in &names {
            if emitted.contains(name) {
                continue;
            }
            let Some(meta) = metas.get(name) else {
                continue;
            };
            let deps_ready = meta
                .columns
                .iter()
                .filter_map(|column| column.rank_policy.as_ref())
                .all(|policy| {
                    policy.joined_table == *name
                        || !metas.contains_key(&policy.joined_table)
                        || emitted.contains(&policy.joined_table)
                });
            if deps_ready {
                push_snapshot_table_ddl(&mut ddl, name, meta);
                emitted.insert(name.clone());
            }
        }
        if emitted.len() == before {
            for name in &names {
                if !emitted.contains(name)
                    && let Some(meta) = metas.get(name)
                {
                    push_snapshot_table_ddl(&mut ddl, name, meta);
                    emitted.insert(name.clone());
                }
            }
        }
    }
    ddl
}

fn push_snapshot_table_ddl(ddl: &mut Vec<DdlChange>, name: &str, meta: &TableMeta) {
    let deferred_self_rank_columns = meta
        .columns
        .iter()
        .filter(|column| {
            column
                .rank_policy
                .as_ref()
                .is_some_and(|policy| policy.joined_table == name)
        })
        .map(|column| column.name.clone())
        .collect::<HashSet<_>>();
    ddl.push(ddl_change_from_meta_excluding(
        name,
        meta,
        &deferred_self_rank_columns,
    ));
    for index in &meta.indexes {
        if index.kind == contextdb_core::IndexKind::UserDeclared {
            ddl.push(DdlChange::CreateIndex {
                table: name.to_string(),
                name: index.name.clone(),
                columns: index.columns.clone(),
            });
        }
    }
    if !deferred_self_rank_columns.is_empty() {
        ddl.push(DdlChange::AlterTable {
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
        });
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
        DataType::TxId => "TXID".to_string(),
    }
}

fn sql_type_for_ast_column(
    col: &contextdb_parser::ast::ColumnDef,
    _rules: &[contextdb_parser::ast::AstPropagationRule],
) -> String {
    let mut ty = sql_type_for_ast(&col.data_type);
    append_ast_quantization(&mut ty, col.quantization);
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
    if col.primary_key {
        ty.push_str(" PRIMARY KEY");
    }
    if !col.nullable && !col.primary_key {
        ty.push_str(" NOT NULL");
    }
    if col.unique {
        ty.push_str(" UNIQUE");
    }
    if col.immutable {
        ty.push_str(" IMMUTABLE");
    }
    if let Some(policy) = col.rank_policy.as_deref() {
        ty.push_str(&format!(
            " RANK_POLICY (JOIN {} ON {}, FORMULA '{}', SORT_KEY {})",
            policy.joined_table,
            policy.joined_column,
            sql_quote(&policy.formula),
            policy.sort_key
        ));
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
        ColumnType::Timestamp => "TIMESTAMP".to_string(),
        ColumnType::TxId => "TXID".to_string(),
    };
    append_core_quantization(&mut ty, col.quantization);

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

    if let Some(reference) = &col.references {
        ty.push_str(&format!(
            " REFERENCES {}({})",
            reference.table, reference.column
        ));
    } else if let Some((referenced_table, referenced_column, ..)) = fk_rules.first() {
        ty.push_str(&format!(
            " REFERENCES {}({})",
            referenced_table, referenced_column
        ));
    }

    if col.references.is_some() || !fk_rules.is_empty() {
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
    if col.primary_key {
        ty.push_str(" PRIMARY KEY");
    }
    if !col.nullable && !col.primary_key {
        ty.push_str(" NOT NULL");
    }
    if col.unique {
        ty.push_str(" UNIQUE");
    }
    if col.expires {
        ty.push_str(" EXPIRES");
    }
    if col.immutable {
        ty.push_str(" IMMUTABLE");
    }
    if let Some(policy) = &col.rank_policy {
        ty.push_str(&format!(
            " RANK_POLICY (JOIN {} ON {}, FORMULA '{}', SORT_KEY {})",
            policy.joined_table,
            policy.joined_column,
            sql_quote(&policy.formula),
            policy.sort_key
        ));
    }

    ty
}

fn append_ast_quantization(
    ty: &mut String,
    quantization: contextdb_parser::ast::VectorQuantization,
) {
    let quantization = match quantization {
        contextdb_parser::ast::VectorQuantization::F32 => return,
        contextdb_parser::ast::VectorQuantization::SQ8 => "SQ8",
        contextdb_parser::ast::VectorQuantization::SQ4 => "SQ4",
    };
    ty.push_str(&format!(" WITH (quantization = '{quantization}')"));
}

fn append_core_quantization(ty: &mut String, quantization: contextdb_core::VectorQuantization) {
    if !matches!(quantization, contextdb_core::VectorQuantization::F32) {
        ty.push_str(&format!(
            " WITH (quantization = '{}')",
            quantization.as_str()
        ));
    }
}

fn sql_quote(value: &str) -> String {
    value.replace('\'', "''")
}

fn normalize_schema_type(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
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

    if let Some(retain) = &ct.retain {
        let mut clause = format!("RETAIN {}", ttl_seconds_to_sql(retain.duration_seconds));
        if retain.sync_safe {
            clause.push_str(" SYNC SAFE");
        }
        constraints.push(clause);
    }

    for unique_constraint in &ct.unique_constraints {
        constraints.push(format!("UNIQUE ({})", unique_constraint.join(", ")));
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

    if let Some(ttl_seconds) = meta.default_ttl_seconds {
        let mut clause = format!("RETAIN {}", ttl_seconds_to_sql(ttl_seconds));
        if meta.sync_safe {
            clause.push_str(" SYNC SAFE");
        }
        constraints.push(clause);
    }

    for unique_constraint in &meta.unique_constraints {
        constraints.push(format!("UNIQUE ({})", unique_constraint.join(", ")));
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

fn ttl_seconds_to_sql(seconds: u64) -> String {
    if seconds.is_multiple_of(24 * 60 * 60) {
        format!("{} DAYS", seconds / (24 * 60 * 60))
    } else if seconds.is_multiple_of(60 * 60) {
        format!("{} HOURS", seconds / (60 * 60))
    } else if seconds.is_multiple_of(60) {
        format!("{} MINUTES", seconds / 60)
    } else {
        format!("{seconds} SECONDS")
    }
}
