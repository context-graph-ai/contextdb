use crate::composite_store::{ApplyPhasePause, ChangeLogEntry, CompositeStore};
use crate::executor::{apply_on_conflict_updates, execute_plan};
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
    NaturalKey, RowChange, VectorChange, natural_key_column_for_meta,
};
use contextdb_core::*;
use contextdb_graph::{GraphStore, MemGraphExecutor};
use contextdb_parser::Statement;
use contextdb_parser::ast::{AlterAction, CreateTable, DataType, Expr};
use contextdb_planner::{OnConflictPlan, PhysicalPlan};
use contextdb_relational::{MemRelationalExecutor, RelationalStore, index_key_from_values};
use contextdb_tx::{TxManager, WriteSet, WriteSetApplicator};
use contextdb_vector::{HnswGraphStats, HnswIndex, MemVectorExecutor, VectorStore};
use parking_lot::{Condvar, Mutex, RwLock};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type DynStore = Box<dyn WriteSetApplicator>;
type GatedBfsEntry = (NodeId, u32, Vec<(NodeId, EdgeType)>);
type GatedGraphNeighbor = (NodeId, EdgeType, HashMap<String, Value>, NodeId, NodeId);
const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 1024;
const MAX_STATEMENT_CACHE_ENTRIES: usize = 1024;
const SAME_PROCESS_REOPEN_RETRY: Duration = Duration::from_millis(500);
// redb may need a small metadata page on the next write, especially for a new
// file with the format metadata table. Keep the disk-limit error deterministic
// instead of starting a write that cannot commit cleanly.
const MIN_DISK_WRITE_HEADROOM_BYTES: u64 = 1024;

mod cron;
pub(crate) mod event_bus;
pub(crate) mod gate;
pub(crate) mod trigger;
use cron::CronState;
use event_bus::EventBusState;
use trigger::TriggerState;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronAuditEntry {
    pub schedule_name: String,
    pub kind: CronAuditKind,
    pub at_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CronAuditKind {
    Fired,
    MissedSkipped,
    MissedCaughtUp { ticks: u32 },
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerDeclaration {
    pub name: String,
    pub table: String,
    pub on_events: Vec<TriggerEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriggerContext {
    pub trigger_name: String,
    pub table: String,
    pub event: TriggerEvent,
    pub tx: TxId,
    pub depth: u32,
    pub row_values: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerAuditEntry {
    pub trigger_name: String,
    pub firing_tx: TxId,
    pub firing_lsn: Lsn,
    pub depth: u32,
    pub cascade_row_count: u32,
    pub status: TriggerAuditStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerAuditStatus {
    Fired,
    RolledBack { reason: String },
    DepthExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerAuditStatusFilter {
    Fired,
    RolledBack,
    DepthExceeded,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TriggerAuditFilter {
    pub trigger_name: Option<String>,
    pub status: Option<TriggerAuditStatusFilter>,
}

pub struct CronPauseGuard {
    cron: Arc<CronState>,
}

impl std::fmt::Debug for CronPauseGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronPauseGuard").finish_non_exhaustive()
    }
}

impl Drop for CronPauseGuard {
    fn drop(&mut self) {
        self.cron.resume_tickler();
    }
}

#[derive(Debug)]
pub struct ApplyPhasePauseGuard {
    inner: Arc<ApplyPhasePause>,
    generation: u64,
}

impl ApplyPhasePauseGuard {
    pub fn wait_until_reached(&self, timeout: Duration) -> bool {
        self.inner.wait_until_reached(self.generation, timeout)
    }

    pub fn release(&self) {
        self.inner.release(self.generation);
    }
}

impl Drop for ApplyPhasePauseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    static CRON_LSN_OVERRIDE: std::cell::RefCell<Option<Lsn>> =
        const { std::cell::RefCell::new(None) };
    static CRON_CALLBACK_TX: std::cell::Cell<Option<TxId>> =
        const { std::cell::Cell::new(None) };
    static CRON_CALLBACK_DB: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static CRON_CALLBACK_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static TRIGGER_CALLBACK_TX: std::cell::Cell<Option<TxId>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_CALLBACK_DB: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_CALLBACK_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH: std::cell::Cell<u32> =
        const { std::cell::Cell::new(0) };
    static DB_OPERATION_STACK: std::cell::RefCell<Vec<usize>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

struct SyncApplyTriggerGateGuard;

impl Drop for SyncApplyTriggerGateGuard {
    fn drop(&mut self) {
        SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

pub struct Database {
    tx_mgr: Arc<TxManager<DynStore>>,
    relational_store: Arc<RelationalStore>,
    graph_store: Arc<GraphStore>,
    vector_store: Arc<VectorStore>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    persistence: Option<Arc<RedbPersistence>>,
    open_registry_path: Mutex<Option<PathBuf>>,
    operation_gate: RwLock<()>,
    apply_phase_pause: Arc<ApplyPhasePause>,
    relational: MemRelationalExecutor<DynStore>,
    graph: MemGraphExecutor<DynStore>,
    vector: MemVectorExecutor<DynStore>,
    session_tx: Mutex<Option<TxId>>,
    instance_id: uuid::Uuid,
    owner_thread: thread::ThreadId,
    plugin: Arc<dyn DatabasePlugin>,
    access: AccessConstraints,
    accountant: Arc<MemoryAccountant>,
    conflict_policies: RwLock<ConflictPolicies>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    pruning_runtime: Mutex<PruningRuntime>,
    pruning_guard: Arc<Mutex<()>>,
    cron: Arc<CronState>,
    event_bus: Arc<EventBusState>,
    trigger: Arc<TriggerState>,
    pending_event_bus_ddl: Mutex<HashMap<TxId, Vec<DdlChange>>>,
    pending_commit_metadata: Mutex<HashMap<TxId, PendingCommitMetadata>>,
    disk_limit: AtomicU64,
    disk_limit_startup_ceiling: AtomicU64,
    sync_watermark: Arc<AtomicLsn>,
    closed: AtomicBool,
    rows_examined: AtomicU64,
    statement_cache: RwLock<HashMap<String, Arc<CachedStatement>>>,
    rank_formula_cache: RwLock<HashMap<(String, String), Arc<RankFormula>>>,
    acl_grant_cache: RwLock<HashMap<AclGrantCacheKey, Arc<HashSet<uuid::Uuid>>>>,
    rank_policy_eval_count: AtomicU64,
    rank_policy_formula_parse_count: AtomicU64,
    fk_indexed_tuple_probes: AtomicU64,
    fk_full_scan_fallbacks: AtomicU64,
    corrupt_joined_values: RwLock<HashSet<(String, RowId, String)>>,
    resource_owner: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AccessConstraints {
    contexts: Option<BTreeSet<ContextId>>,
    scope_labels: Option<BTreeSet<ScopeLabel>>,
    principal: Option<Principal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AclGrantCacheKey {
    principal: Principal,
    ref_table: String,
    ref_column: String,
    snapshot: SnapshotId,
}

pub(crate) enum InsertRowResult {
    Inserted(RowId),
    NoOp,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct WriteSetCounts {
    relational_inserts: usize,
    relational_deletes: usize,
    adj_inserts: usize,
    adj_deletes: usize,
    vector_inserts: usize,
    vector_deletes: usize,
    vector_moves: usize,
}

#[derive(Debug, Default, Clone)]
struct PendingCommitMetadata {
    conditional_update_guards: Vec<PendingConditionalUpdateGuard>,
    upsert_intents: Vec<PendingUpsertIntent>,
}

#[derive(Debug, Clone)]
pub(crate) struct UpsertIntentDetails {
    pub insert_values: HashMap<ColName, Value>,
    pub conflict_columns: Vec<ColName>,
    pub update_columns: Vec<(ColName, Expr)>,
    pub params: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
struct PendingConditionalUpdateGuard {
    table: TableName,
    row_id: RowId,
    predicates: Vec<(ColName, Value)>,
    before: WriteSetCounts,
    after: WriteSetCounts,
}

#[derive(Debug, Clone)]
struct PendingUpsertIntent {
    table: TableName,
    row_id: RowId,
    active_tx: TxId,
    insert_values: HashMap<ColName, Value>,
    conflict_columns: Vec<ColName>,
    update_columns: Vec<(ColName, Expr)>,
    params: HashMap<String, Value>,
}

#[derive(Debug, Default)]
struct CommitValidationOutcome {
    conditional_noop_count: u64,
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

static OPEN_FILE_DATABASES: OnceLock<OpenFileRegistry> = OnceLock::new();

fn open_file_registry() -> &'static OpenFileRegistry {
    OPEN_FILE_DATABASES.get_or_init(|| OpenFileRegistry {
        entries: Mutex::new(BTreeMap::new()),
        waiters: Condvar::new(),
    })
}

fn canonical_database_path(path: &Path) -> Result<PathBuf> {
    canonical_database_path_inner(path, 0)
}

fn canonical_database_path_inner(path: &Path, depth: usize) -> Result<PathBuf> {
    if depth > 32 {
        return Err(Error::Other(format!(
            "too many symlink levels while canonicalizing {}",
            path.display()
        )));
    }
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && metadata.file_type().is_symlink()
    {
        let target = std::fs::read_link(path)
            .map_err(|err| Error::Other(format!("read_link {}: {err}", path.display())))?;
        let resolved = if target.is_absolute() {
            target
        } else {
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .join(target)
        };
        return canonical_database_path_inner(&resolved, depth + 1);
    }
    if path.exists() {
        return std::fs::canonicalize(path)
            .map_err(|err| Error::Other(format!("canonicalize {}: {err}", path.display())));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path.file_name().ok_or_else(|| {
        Error::Other(format!(
            "database path must include a file name: {}",
            path.display()
        ))
    })?;
    let canonical_parent = std::fs::canonicalize(parent)
        .map_err(|err| Error::Other(format!("canonicalize {}: {err}", parent.display())))?;
    Ok(canonical_parent.join(file_name))
}

struct OpenRegistryReservation {
    path: PathBuf,
    active: bool,
}

struct OpenFileRegistry {
    entries: Mutex<BTreeMap<PathBuf, OpenRegistryState>>,
    waiters: Condvar,
}

#[derive(Clone, Copy)]
enum OpenRegistryState {
    Opening { pid: u32 },
    Open { pid: u32 },
}

impl OpenRegistryState {
    fn pid(self) -> u32 {
        match self {
            Self::Opening { pid } | Self::Open { pid } => pid,
        }
    }

    fn is_opening(self) -> bool {
        matches!(self, Self::Opening { .. })
    }
}

struct DatabaseOperationGuard<'a> {
    db_id: usize,
    _lock: Option<parking_lot::RwLockReadGuard<'a, ()>>,
}

impl Drop for DatabaseOperationGuard<'_> {
    fn drop(&mut self) {
        DB_OPERATION_STACK.with(|stack| {
            let popped = stack.borrow_mut().pop();
            debug_assert_eq!(
                popped,
                Some(self.db_id),
                "database operation stack mismatch"
            );
        });
    }
}

impl OpenRegistryReservation {
    fn acquire(path: PathBuf) -> Result<Self> {
        let registry = open_file_registry();
        let mut entries = registry.entries.lock();
        loop {
            match entries.get(&path).copied() {
                Some(state) if state.is_opening() => {
                    registry.waiters.wait(&mut entries);
                }
                Some(state) => {
                    return Err(Error::DatabaseLocked {
                        holder_pid: state.pid(),
                        path,
                    });
                }
                None => {
                    entries.insert(
                        path.clone(),
                        OpenRegistryState::Opening {
                            pid: std::process::id(),
                        },
                    );
                    return Ok(Self { path, active: true });
                }
            }
        }
    }

    fn disarm(mut self) -> PathBuf {
        let registry = open_file_registry();
        let mut entries = registry.entries.lock();
        entries.insert(
            self.path.clone(),
            OpenRegistryState::Open {
                pid: std::process::id(),
            },
        );
        registry.waiters.notify_all();
        self.active = false;
        self.path.clone()
    }
}

impl Drop for OpenRegistryReservation {
    fn drop(&mut self) {
        if self.active {
            let registry = open_file_registry();
            let mut entries = registry.entries.lock();
            if entries
                .get(&self.path)
                .copied()
                .is_some_and(|state| state.is_opening() && state.pid() == std::process::id())
            {
                entries.remove(&self.path);
                registry.waiters.notify_all();
            }
        }
    }
}

fn acquire_registry_and_persistence(
    canonical_path: &Path,
) -> Result<(OpenRegistryReservation, Arc<RedbPersistence>)> {
    let retry_deadline = Instant::now() + SAME_PROCESS_REOPEN_RETRY;

    loop {
        let registry_reservation =
            match OpenRegistryReservation::acquire(canonical_path.to_path_buf()) {
                Ok(reservation) => reservation,
                Err(Error::DatabaseLocked { holder_pid, path })
                    if holder_pid == std::process::id()
                        && path == canonical_path
                        && Instant::now() < retry_deadline =>
                {
                    thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(err) => return Err(err),
            };

        let persistence = if canonical_path.exists() {
            RedbPersistence::open(canonical_path)
        } else {
            RedbPersistence::create(canonical_path)
        };

        match persistence {
            Ok(persistence) => return Ok((registry_reservation, Arc::new(persistence))),
            Err(Error::DatabaseLocked { holder_pid, path })
                if holder_pid == std::process::id()
                    && path == canonical_path
                    && Instant::now() < retry_deadline =>
            {
                drop(registry_reservation);
                thread::sleep(Duration::from_millis(1));
            }
            Err(err) => return Err(err),
        }
    }
}

fn release_open_registry_path(path: &Path) {
    let registry = open_file_registry();
    let mut entries = registry.entries.lock();
    if entries
        .get(path)
        .copied()
        .is_some_and(|state| state.pid() == std::process::id())
    {
        entries.remove(path);
        registry.waiters.notify_all();
    }
}

fn closed_database_error() -> Error {
    Error::Other("database handle is closed".to_string())
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PruningReport {
    pub pruned_rows: u64,
    pub blocked_count: u64,
    pub blocked: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FkProbeStats {
    pub indexed_tuple_probes: u64,
    pub full_scan_fallbacks: u64,
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
        open_registry_path: Option<PathBuf>,
        apply_phase_pause: Arc<ApplyPhasePause>,
        plugin: Arc<dyn DatabasePlugin>,
        accountant: Arc<MemoryAccountant>,
        disk_limit: Option<u64>,
        disk_limit_startup_ceiling: Option<u64>,
        event_bus: Arc<EventBusState>,
        trigger: Arc<TriggerState>,
    ) -> Self {
        Self {
            tx_mgr: tx_mgr.clone(),
            relational_store: relational.clone(),
            graph_store: graph.clone(),
            vector_store: vector_store.clone(),
            change_log,
            ddl_log,
            persistence,
            open_registry_path: Mutex::new(open_registry_path),
            operation_gate: RwLock::new(()),
            apply_phase_pause,
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
            owner_thread: thread::current().id(),
            plugin,
            access: AccessConstraints::default(),
            accountant,
            conflict_policies: RwLock::new(ConflictPolicies::uniform(ConflictPolicy::LatestWins)),
            subscriptions: Arc::new(Mutex::new(SubscriptionState::new())),
            pruning_runtime: Mutex::new(PruningRuntime::new()),
            pruning_guard: Arc::new(Mutex::new(())),
            cron: Arc::new(CronState::new()),
            event_bus,
            trigger,
            pending_event_bus_ddl: Mutex::new(HashMap::new()),
            pending_commit_metadata: Mutex::new(HashMap::new()),
            disk_limit: AtomicU64::new(disk_limit.unwrap_or(0)),
            disk_limit_startup_ceiling: AtomicU64::new(disk_limit_startup_ceiling.unwrap_or(0)),
            sync_watermark: Arc::new(AtomicLsn::new(Lsn(0))),
            closed: AtomicBool::new(false),
            rows_examined: AtomicU64::new(0),
            statement_cache: RwLock::new(HashMap::new()),
            rank_formula_cache: RwLock::new(HashMap::new()),
            acl_grant_cache: RwLock::new(HashMap::new()),
            rank_policy_eval_count: AtomicU64::new(0),
            rank_policy_formula_parse_count: AtomicU64::new(0),
            fk_indexed_tuple_probes: AtomicU64::new(0),
            fk_full_scan_fallbacks: AtomicU64::new(0),
            corrupt_joined_values: RwLock::new(HashSet::new()),
            resource_owner: true,
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
        Self::open_with_constraints(path, Some(contexts), None, None)
    }

    pub fn open_memory_with_contexts(
        contexts: std::collections::BTreeSet<contextdb_core::types::ContextId>,
    ) -> Self {
        Self::open_memory_with_constraints(Some(contexts), None, None)
    }

    pub fn open_with_scope_labels<P: AsRef<Path>>(
        path: P,
        labels: std::collections::BTreeSet<contextdb_core::types::ScopeLabel>,
    ) -> Result<Self> {
        Self::open_with_constraints(path, None, Some(labels), None)
    }

    pub fn open_memory_with_scope_labels(
        labels: std::collections::BTreeSet<contextdb_core::types::ScopeLabel>,
    ) -> Self {
        Self::open_memory_with_constraints(None, Some(labels), None)
    }

    pub fn open_as_principal<P: AsRef<Path>>(
        path: P,
        principal: contextdb_core::types::Principal,
    ) -> Result<Self> {
        Self::open_with_constraints(path, None, None, Some(principal))
    }

    pub fn open_memory_as_principal(principal: contextdb_core::types::Principal) -> Self {
        Self::open_memory_with_constraints(None, None, Some(principal))
    }

    pub fn open_with_constraints<P: AsRef<Path>>(
        path: P,
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> Result<Self> {
        let access = AccessConstraints {
            contexts,
            scope_labels,
            principal,
        };
        let path = path.as_ref();
        let db = if path.as_os_str() == ":memory:" {
            Self::open_memory_internal(
                Arc::new(CorePlugin),
                Arc::new(MemoryAccountant::no_limit()),
            )?
        } else {
            let db = Self::open_loaded(
                path,
                Arc::new(CorePlugin),
                Arc::new(MemoryAccountant::no_limit()),
                None,
            )?;
            db.plugin.on_open()?;
            db
        };
        let db = db.with_access_constraints(access);
        db.start_cron_tickler_if_schedules_present();
        Ok(db)
    }

    pub fn open_memory_with_constraints(
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> Self {
        Self::open_memory().with_access_constraints(AccessConstraints {
            contexts,
            scope_labels,
            principal,
        })
    }

    fn with_access_constraints(mut self, access: AccessConstraints) -> Self {
        self.access = access;
        self
    }

    fn open_loaded(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
        mut accountant: Arc<MemoryAccountant>,
        startup_disk_limit: Option<u64>,
    ) -> Result<Self> {
        let canonical_path = canonical_database_path(path.as_ref())?;
        let (registry_reservation, persistence) =
            acquire_registry_and_persistence(&canonical_path)?;
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

        let loaded_change_log = persistence.load_change_log()?;
        let loaded_ddl_log = persistence.load_ddl_log()?;
        let mut commit_index = persistence.load_commit_index()?;
        let reconstructed_commit_index =
            commit_index_across_all(&relational, &graph, &vector, &loaded_change_log);
        let mut missing_commit_index = BTreeMap::new();
        for (lsn, tx) in reconstructed_commit_index {
            if let std::collections::btree_map::Entry::Vacant(slot) = commit_index.entry(lsn) {
                slot.insert(tx);
                missing_commit_index.insert(lsn, tx);
            }
        }
        let mut loaded_vectors = loaded_vectors;
        let repaired_visibility_order = repair_visibility_tx_order_if_needed(
            &relational,
            &graph,
            &vector,
            loaded_vectors.as_mut_slice(),
            &all_meta,
            &persistence,
            &mut commit_index,
        )?;
        if repaired_visibility_order {
            persistence.rewrite_commit_index(&commit_index)?;
        } else if !missing_commit_index.is_empty() {
            persistence.flush_commit_index_entries(&missing_commit_index)?;
        }
        hydrate_relational_vector_values(&relational, &loaded_vectors);

        let max_row_id = relational.max_row_id();
        let max_tx = max_tx_across_all(&relational, &graph, &vector);
        let commit_index_max_lsn = commit_index.keys().next_back().copied().unwrap_or(Lsn(0));
        let ddl_max_lsn = loaded_ddl_log
            .iter()
            .map(|(lsn, _)| *lsn)
            .max()
            .unwrap_or(Lsn(0));
        let max_lsn = max_lsn_across_all(&relational, &graph, &vector)
            .max(commit_index_max_lsn)
            .max(ddl_max_lsn);
        relational.set_next_row_id(RowId(max_row_id.0.saturating_add(1)));

        let change_log = Arc::new(RwLock::new(loaded_change_log));
        let ddl_log = Arc::new(RwLock::new(loaded_ddl_log));
        let apply_phase_pause = Arc::new(ApplyPhasePause::new());
        let composite = CompositeStore::new_with_apply_phase_pause(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            ddl_log.clone(),
            accountant.clone(),
            apply_phase_pause.clone(),
        );
        let event_bus = Arc::new(EventBusState::new());
        let trigger = Arc::new(TriggerState::new());
        let persistent = PersistentCompositeStore::new(
            composite,
            persistence.clone(),
            Some(event_bus.clone()),
            Some(trigger.clone()),
        );
        let store: DynStore = Box::new(persistent);
        let tx_mgr = Arc::new(TxManager::new_with_counters_and_commit_index(
            store,
            TxId(max_tx.0.saturating_add(1)),
            Lsn(max_lsn.0.saturating_add(1)),
            max_tx,
            commit_index,
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
            Some(registry_reservation.disarm()),
            apply_phase_pause,
            plugin,
            accountant,
            effective_disk_limit,
            startup_disk_ceiling,
            event_bus,
            trigger,
        );

        for meta in all_meta.values() {
            if !meta.dag_edge_types.is_empty() {
                db.graph.register_dag_edge_types(&meta.dag_edge_types);
            }
        }
        db.rebuild_rank_formula_cache_from_meta(&all_meta)?;

        db.account_loaded_state()?;
        maybe_prebuild_hnsw(&db.vector_store, db.accountant());
        db.load_cron_state_from_persistence()?;
        db.load_event_bus_state_from_persistence()?;
        db.load_trigger_state_from_persistence()?;

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
        let apply_phase_pause = Arc::new(ApplyPhasePause::new());
        let trigger = Arc::new(TriggerState::new());
        let store: DynStore = Box::new(CompositeStore::new_with_apply_phase_pause(
            relational.clone(),
            graph.clone(),
            vector.clone(),
            change_log.clone(),
            ddl_log.clone(),
            accountant.clone(),
            apply_phase_pause.clone(),
        ));
        let tx_mgr = Arc::new(TxManager::new(store));
        let event_bus = Arc::new(EventBusState::new());

        let db = Self::build_db(
            tx_mgr,
            relational,
            graph,
            vector,
            hnsw,
            change_log,
            ddl_log,
            None,
            None,
            apply_phase_pause,
            plugin,
            accountant,
            None,
            None,
            event_bus,
            trigger,
        );
        maybe_prebuild_hnsw(&db.vector_store, db.accountant());
        Ok(db)
    }

    pub fn begin(&self) -> TxId {
        let _operation = self.assert_open_operation();
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            panic!("transaction control is not allowed inside cron callbacks");
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) {
            panic!("transaction control is not allowed inside trigger callbacks");
        }
        if self.trigger_active_on_this_handle() {
            panic!("transaction control is not allowed inside trigger callbacks");
        }
        if self.cron.callback_active_on_other_thread() {
            panic!(
                "transaction control is not allowed from another thread while a cron callback is active"
            );
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            panic!(
                "transaction control is not allowed from another thread while a trigger callback is active"
            );
        }
        self.tx_mgr.begin()
    }

    pub fn commit(&self, tx: TxId) -> Result<()> {
        let _operation = self.open_operation()?;
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::Other(
                "transaction control is not allowed inside cron callbacks".to_string(),
            ));
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::Other(
                "transaction control is not allowed inside trigger callbacks".to_string(),
            ));
        }
        if self.trigger_active_on_this_handle() {
            return Err(Error::Other(
                "transaction control is not allowed inside trigger callbacks".to_string(),
            ));
        }
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::Other(
                "transaction control is not allowed from another thread while a cron callback is active"
                    .to_string(),
            ));
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return Err(Error::Other(
                "transaction control is not allowed from another thread while a trigger callback is active"
                    .to_string(),
            ));
        }
        self.commit_with_source(tx, CommitSource::User)
    }

    pub fn rollback(&self, tx: TxId) -> Result<()> {
        let _operation = self.open_operation()?;
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::Other(
                "transaction control is not allowed inside cron callbacks".to_string(),
            ));
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::Other(
                "transaction control is not allowed inside trigger callbacks".to_string(),
            ));
        }
        if self.trigger_active_on_this_handle() {
            return Err(Error::Other(
                "transaction control is not allowed inside trigger callbacks".to_string(),
            ));
        }
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::Other(
                "transaction control is not allowed from another thread while a cron callback is active"
                    .to_string(),
            ));
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return Err(Error::Other(
                "transaction control is not allowed from another thread while a trigger callback is active"
                    .to_string(),
            ));
        }
        let ws = self.tx_mgr.rollback_write_set(tx)?;
        self.pending_event_bus_ddl.lock().remove(&tx);
        self.pending_commit_metadata.lock().remove(&tx);
        self.release_insert_allocations(&ws);
        Ok(())
    }

    pub fn snapshot(&self) -> SnapshotId {
        let _operation = self.assert_open_operation();
        self.tx_mgr.snapshot()
    }

    pub fn snapshot_at(&self, lsn: Lsn) -> SnapshotId {
        let _operation = self.assert_open_operation();
        self.tx_mgr.snapshot_at_lsn(lsn)
    }

    fn enter_sync_apply_trigger_gate_bypass(&self) -> SyncApplyTriggerGateGuard {
        SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_add(1));
        });
        SyncApplyTriggerGateGuard
    }

    fn sync_apply_trigger_gate_bypass_active() -> bool {
        SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH.with(|depth| depth.get() > 0)
    }

    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let _operation = self.open_operation()?;
        if let Some(cached) = self.cached_statement(sql) {
            self.assert_statement_allowed_inside_cron_callback(&cached.stmt)?;
            self.assert_statement_allowed_during_cross_thread_callback(&cached.stmt)?;
            let active_tx = self.active_session_tx();
            return self.execute_statement_with_plan(
                &cached.stmt,
                sql,
                params,
                active_tx,
                Some(&cached.plan),
            );
        }

        let stmt = contextdb_parser::parse(sql)?;
        self.assert_statement_allowed_inside_cron_callback(&stmt)?;
        self.assert_statement_allowed_during_cross_thread_callback(&stmt)?;

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

        match &stmt {
            Statement::CreateSchedule {
                name,
                every,
                callback,
                missed_tick_policy,
                catch_up_within_seconds,
            } => {
                self.create_cron_schedule(
                    name,
                    every,
                    callback,
                    missed_tick_policy.as_deref(),
                    *catch_up_within_seconds,
                )?;
                return Ok(QueryResult::empty());
            }
            Statement::DropSchedule { name } => {
                self.drop_cron_schedule(name)?;
                return Ok(QueryResult::empty());
            }
            Statement::CreateTrigger { .. } | Statement::DropTrigger { .. } => {
                let ddl = self
                    .ddl_change_for_statement(&stmt, self.active_session_tx())
                    .expect("trigger statement has DDL change");
                self.apply_trigger_ddl_from_user(ddl)?;
                return Ok(QueryResult::empty());
            }
            _ => {}
        }

        let active_tx = self.active_session_tx();
        self.execute_statement(&stmt, sql, params, active_tx)
    }

    fn active_session_tx(&self) -> Option<TxId> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            let this_db = self as *const Self as usize;
            if CRON_CALLBACK_DB.with(|slot| slot.get()) == Some(this_db) {
                return CRON_CALLBACK_TX.with(|slot| slot.get());
            }
        }
        if let Some(tx) = self.active_trigger_tx_for_this_handle() {
            return Some(tx);
        }
        *self.session_tx.lock()
    }

    fn assert_statement_allowed_inside_cron_callback(&self, stmt: &Statement) -> Result<()> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get())
            && Self::statement_forbidden_inside_cron_callback(stmt)
        {
            return Err(Error::Other(
                "DDL and transaction control are not allowed inside cron callbacks".to_string(),
            ));
        }
        if CRON_CALLBACK_ACTIVE.with(|active| active.get())
            && Self::statement_requires_cron_bound_handle(stmt)
        {
            let active_tx = self.active_session_tx();
            let cron_tx = CRON_CALLBACK_TX.with(|slot| slot.get());
            let cron_db = CRON_CALLBACK_DB.with(|slot| slot.get());
            let this_db = self as *const Self as usize;
            if active_tx.is_none() || active_tx != cron_tx || cron_db != Some(this_db) {
                return Err(Error::Other(
                    "cron callback writes must use the supplied tx-bound database handle"
                        .to_string(),
                ));
            }
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get())
            && Self::statement_forbidden_inside_cron_callback(stmt)
            && !matches!(
                stmt,
                Statement::CreateTrigger { .. } | Statement::DropTrigger { .. }
            )
        {
            return Err(Error::Other(
                "DDL and transaction control are not allowed inside trigger callbacks".to_string(),
            ));
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get())
            && Self::statement_requires_cron_bound_handle(stmt)
        {
            let active_tx = self.active_session_tx();
            let trigger_tx = TRIGGER_CALLBACK_TX.with(|slot| slot.get());
            let trigger_db = TRIGGER_CALLBACK_DB.with(|slot| slot.get());
            let this_db = self as *const Self as usize;
            if active_tx.is_none() || active_tx != trigger_tx || trigger_db != Some(this_db) {
                return Err(Error::Other(
                    "trigger callback writes must use the supplied tx-bound database handle"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    fn assert_statement_allowed_during_cross_thread_callback(
        &self,
        stmt: &Statement,
    ) -> Result<()> {
        let is_trigger_ddl = matches!(
            stmt,
            Statement::CreateTrigger { .. } | Statement::DropTrigger { .. }
        );
        if self.cron.callback_active_on_other_thread()
            && (Self::statement_forbidden_inside_cron_callback(stmt)
                || Self::statement_requires_cron_bound_handle(stmt))
        {
            return Err(Error::Other(
                "database writes from other threads are not allowed while a cron callback is active"
                    .to_string(),
            ));
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
            && !is_trigger_ddl
            && (Self::statement_forbidden_inside_cron_callback(stmt)
                || Self::statement_requires_cron_bound_handle(stmt))
        {
            return Err(Error::Other(
                "database writes from other threads are not allowed while a trigger callback is active"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn assert_cron_callback_tx_bound_handle(&self, tx: TxId) -> Result<()> {
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::Other(
                "database writes from other threads are not allowed while a cron callback is active"
                    .to_string(),
            ));
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return Err(Error::Other(
                "database writes from other threads are not allowed while a trigger callback is active"
                    .to_string(),
            ));
        }
        if !CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            return self.assert_trigger_callback_tx_bound_handle(tx);
        }
        let cron_tx = CRON_CALLBACK_TX.with(|slot| slot.get());
        let cron_db = CRON_CALLBACK_DB.with(|slot| slot.get());
        let this_db = self as *const Self as usize;
        if cron_tx == Some(tx) && cron_db == Some(this_db) {
            self.assert_trigger_callback_tx_bound_handle(tx)
        } else {
            Err(Error::Other(
                "cron callback writes must use the supplied tx-bound database handle".to_string(),
            ))
        }
    }

    fn assert_trigger_callback_tx_bound_handle(&self, tx: TxId) -> Result<()> {
        if !TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Ok(());
        }
        let trigger_tx = TRIGGER_CALLBACK_TX.with(|slot| slot.get());
        let trigger_db = TRIGGER_CALLBACK_DB.with(|slot| slot.get());
        let this_db = self as *const Self as usize;
        if trigger_tx == Some(tx) && trigger_db == Some(this_db) {
            Ok(())
        } else {
            Err(Error::Other(
                "trigger callback writes must use the supplied tx-bound database handle"
                    .to_string(),
            ))
        }
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

    fn statement_forbidden_inside_cron_callback(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Begin
                | Statement::Commit
                | Statement::Rollback
                | Statement::CreateTable(_)
                | Statement::AlterTable(_)
                | Statement::DropTable(_)
                | Statement::CreateIndex(_)
                | Statement::DropIndex(_)
                | Statement::CreateSchedule { .. }
                | Statement::DropSchedule { .. }
                | Statement::CreateTrigger { .. }
                | Statement::DropTrigger { .. }
                | Statement::CreateEventType { .. }
                | Statement::CreateSink { .. }
                | Statement::CreateRoute { .. }
                | Statement::DropRoute { .. }
                | Statement::SetMemoryLimit(_)
                | Statement::SetDiskLimit(_)
                | Statement::SetSyncConflictPolicy(_)
        )
    }

    fn statement_requires_cron_bound_handle(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_)
        )
    }

    pub(crate) fn clear_statement_cache(&self) {
        self.statement_cache.write().clear();
    }

    #[doc(hidden)]
    pub fn __statement_cache_len(&self) -> usize {
        let _operation = self.assert_open_operation();
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
                    Ok(mut qr) => {
                        let event_bus_ddl = self.take_pending_event_bus_ddl(tx);
                        let validation = self.commit_with_source_and_event_bus_ddl(
                            tx,
                            CommitSource::AutoCommit,
                            &event_bus_ddl,
                        )?;
                        qr.rows_affected = qr
                            .rows_affected
                            .saturating_sub(validation.conditional_noop_count);
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
        let _operation = self.open_operation()?;
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
        let _operation = self.open_operation()?;
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::Other(
                "execute_in_tx is not allowed inside cron callbacks".to_string(),
            ));
        }
        let stmt = contextdb_parser::parse(sql)?;
        self.assert_statement_allowed_inside_cron_callback(&stmt)?;
        self.assert_statement_allowed_during_cross_thread_callback(&stmt)?;
        self.assert_trigger_callback_tx_bound_handle(tx)?;
        self.execute_statement(&stmt, sql, params, Some(tx))
    }

    fn commit_with_source(&self, tx: TxId, source: CommitSource) -> Result<()> {
        let event_bus_ddl = self.take_pending_event_bus_ddl(tx);
        self.commit_with_source_and_event_bus_ddl(tx, source, &event_bus_ddl)
            .map(|_| ())
    }

    fn commit_with_source_and_event_bus_ddl(
        &self,
        tx: TxId,
        source: CommitSource,
        event_bus_ddl: &[DdlChange],
    ) -> Result<CommitValidationOutcome> {
        self.commit_with_source_and_sync_ddl(tx, source, event_bus_ddl, &[])
    }

    fn commit_with_source_and_sync_ddl(
        &self,
        tx: TxId,
        source: CommitSource,
        event_bus_ddl: &[DdlChange],
        trigger_ddl: &[DdlChange],
    ) -> Result<CommitValidationOutcome> {
        self.commit_with_source_and_sync_ddl_and_trigger_audit_projection(
            tx,
            source,
            event_bus_ddl,
            trigger_ddl,
            None,
        )
    }

    fn commit_with_source_and_sync_ddl_and_trigger_audit_projection(
        &self,
        tx: TxId,
        source: CommitSource,
        event_bus_ddl: &[DdlChange],
        trigger_ddl: &[DdlChange],
        sync_pull_trigger_audit_projection: Option<&BTreeMap<String, TriggerDeclaration>>,
    ) -> Result<CommitValidationOutcome> {
        let pending_trigger_audits = std::cell::RefCell::new(Vec::new());
        let mut pending_sink_events = Vec::new();
        let mut committed_trigger_audit_entries = Vec::new();
        let validation_noop_count = std::cell::Cell::new(0_u64);
        let (lsn, ws) = match self.tx_mgr.commit_with_lsn_active_prepare_and_applied_mut(
            tx,
            |_| {
                if source != CommitSource::SyncPull {
                    let outcome = self.prepare_active_trigger_write_set_for_dispatch(tx)?;
                    validation_noop_count.set(
                        validation_noop_count
                            .get()
                            .saturating_add(outcome.conditional_noop_count),
                    );
                    let audits = self
                        .dispatch_triggers_for_tx(tx)
                        .map_err(|failure| failure.error)?;
                    *pending_trigger_audits.borrow_mut() = audits;
                    let outcome = self.prepare_active_trigger_write_set_for_dispatch(tx)?;
                    validation_noop_count.set(
                        validation_noop_count
                            .get()
                            .saturating_add(outcome.conditional_noop_count),
                    );
                }
                Ok(())
            },
            |ws| {
                if !ws.is_empty() {
                    if source != CommitSource::SyncPull {
                        self.rewrite_txid_placeholders(tx, ws)?;
                    }
                    let final_validation = self.commit_validate(tx, ws)?;
                    validation_noop_count.set(
                        validation_noop_count
                            .get()
                            .saturating_add(final_validation.conditional_noop_count),
                    );
                    if let Some(lsn) = ws.commit_lsn {
                        self.stage_event_bus_ddl_for_commit(lsn, event_bus_ddl)?;
                        self.stage_trigger_ddl_for_commit(lsn, trigger_ddl)?;
                    }
                    self.plugin.pre_commit(ws, source)?;
                    pending_sink_events = self
                        .prepare_sink_events_for_write_set_with_event_bus_ddl(ws, event_bus_ddl)?;
                    if self.persistence.is_some()
                        && let Some(lsn) = ws.commit_lsn
                    {
                        self.event_bus
                            .stage_sink_events_for_persistence(lsn, pending_sink_events.clone());
                    }
                    if let Some(lsn) = ws.commit_lsn {
                        committed_trigger_audit_entries = if source == CommitSource::SyncPull {
                            let projected_declarations =
                                self.staged_trigger_declarations_for_commit(lsn);
                            let audit_projection = sync_pull_trigger_audit_projection
                                .or(projected_declarations.as_ref());
                            self.committed_sync_pull_trigger_audits_for_write_set(
                                ws,
                                lsn,
                                audit_projection,
                            )?
                        } else {
                            let pending = pending_trigger_audits.borrow();
                            self.committed_trigger_audits_for_pending(&pending, ws, lsn)
                        };
                        self.stage_trigger_audits_for_persistence(
                            lsn,
                            &committed_trigger_audit_entries,
                        );
                    }
                }
                Ok(())
            },
            |lsn, ws| {
                if !ws.is_empty() {
                    self.publish_staged_event_bus_ddl_commit(lsn);
                    self.publish_staged_trigger_ddl_commit(lsn);
                }
            },
        ) {
            Ok(committed) => committed,
            Err(failure) => {
                if let Some(lsn) = failure.write_set.as_ref().and_then(|ws| ws.commit_lsn) {
                    self.event_bus.take_staged_sink_events_for_persistence(lsn);
                    self.discard_staged_event_bus_ddl_commit(lsn);
                    self.discard_staged_trigger_ddl_commit(lsn);
                    self.discard_staged_trigger_audits_for_persistence(lsn);
                }
                if let Some(ws) = &failure.write_set {
                    self.release_insert_allocations(ws);
                }
                if let Err(audit_error) = self.append_rolled_back_trigger_audits(
                    &pending_trigger_audits.borrow(),
                    tx,
                    &failure.error.to_string(),
                ) {
                    self.pending_commit_metadata.lock().remove(&tx);
                    return Err(audit_error);
                }
                self.pending_commit_metadata.lock().remove(&tx);
                return Err(failure.error);
            }
        };
        self.pending_commit_metadata.lock().remove(&tx);

        if !ws.is_empty() {
            self.release_delete_allocations(&ws);
            self.plugin.post_commit(&ws, source);
            self.publish_prepared_sink_events_to_memory(pending_sink_events);
            self.append_trigger_audits_to_memory(committed_trigger_audit_entries);
            self.publish_commit_event_if_subscribers(&ws, source, lsn);
        } else {
            if !event_bus_ddl.is_empty() {
                self.apply_event_bus_ddl_batch(event_bus_ddl.to_vec())?;
            }
            if !trigger_ddl.is_empty() {
                self.apply_trigger_ddl_batch(trigger_ddl.to_vec())?;
            }
        }

        Ok(CommitValidationOutcome {
            conditional_noop_count: validation_noop_count.get(),
        })
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

        let relational_row_count = ws
            .relational_inserts
            .iter()
            .map(|(table, row)| (table.clone(), row.row_id))
            .chain(
                ws.relational_deletes
                    .iter()
                    .map(|(table, row_id, _)| (table.clone(), *row_id)),
            )
            .collect::<HashSet<_>>()
            .len();

        CommitEvent {
            source,
            lsn,
            tables_changed,
            row_count: relational_row_count
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

        if let Some(change) = self.ddl_change_for_statement(stmt, tx).as_ref() {
            self.plugin.on_ddl(change)?;
        }

        let started = Instant::now();
        if let Some(result) = self.execute_event_bus_statement(stmt, tx) {
            let outcome = query_outcome_from_result(&result);
            self.plugin.post_query(sql, started.elapsed(), &outcome);
            return result;
        }

        // Handle INSERT INTO GRAPH / __edges as a virtual table routing to the graph store.
        if let Statement::Insert(ins) = stmt
            && (ins.table.eq_ignore_ascii_case("GRAPH")
                || ins.table.eq_ignore_ascii_case("__edges"))
        {
            return self.execute_graph_insert(ins, params, tx);
        }

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

    fn execute_event_bus_statement(
        &self,
        stmt: &Statement,
        tx: Option<TxId>,
    ) -> Option<Result<QueryResult>> {
        if !matches!(
            stmt,
            Statement::CreateEventType { .. }
                | Statement::CreateSink { .. }
                | Statement::CreateRoute { .. }
                | Statement::DropRoute { .. }
        ) {
            return None;
        }
        let change = self
            .ddl_change_for_statement(stmt, tx)
            .expect("event bus statement has DDL change");
        Some(match tx {
            Some(tx) => self
                .stage_event_bus_ddl_in_tx(tx, change)
                .map(|()| QueryResult::empty()),
            None => self
                .apply_event_bus_ddl_from_user(vec![change])
                .map(|()| QueryResult::empty()),
        })
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

    fn ddl_change_for_statement(&self, stmt: &Statement, tx: Option<TxId>) -> Option<DdlChange> {
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
                    foreign_keys: single_column_foreign_keys_from_meta(&meta, &HashSet::new()),
                    composite_foreign_keys: meta.composite_foreign_keys.clone(),
                    composite_unique: meta.unique_constraints.clone(),
                })
            }
            Statement::CreateEventType { name, when, table } => Some(DdlChange::CreateEventType {
                name: name.clone(),
                trigger: match when {
                    contextdb_parser::ast::EventTypeTrigger::Insert => "INSERT",
                    contextdb_parser::ast::EventTypeTrigger::Update => "UPDATE",
                    contextdb_parser::ast::EventTypeTrigger::Delete => "DELETE",
                }
                .to_string(),
                table: table.clone(),
            }),
            Statement::CreateTrigger {
                name,
                table,
                on_events,
            } => Some(DdlChange::CreateTrigger {
                name: name.clone(),
                table: table.clone(),
                on_events: on_events
                    .iter()
                    .map(|event| match event {
                        contextdb_parser::ast::TriggerEvent::Insert => "INSERT",
                        contextdb_parser::ast::TriggerEvent::Update => "UPDATE",
                        contextdb_parser::ast::TriggerEvent::Delete => "DELETE",
                    })
                    .map(str::to_string)
                    .collect(),
            }),
            Statement::DropTrigger { name } => Some(DdlChange::DropTrigger { name: name.clone() }),
            Statement::CreateSink {
                name,
                sink_type,
                url,
            } => Some(DdlChange::CreateSink {
                name: name.clone(),
                sink_type: match sink_type {
                    contextdb_parser::ast::SinkType::Webhook => "WEBHOOK",
                    contextdb_parser::ast::SinkType::Callback => "CALLBACK",
                }
                .to_string(),
                url: url.clone(),
            }),
            Statement::CreateRoute {
                name,
                event_type,
                sink,
                where_in,
            } => Some(DdlChange::CreateRoute {
                name: name.clone(),
                event_type: event_type.clone(),
                sink: sink.clone(),
                table: self
                    .event_bus_table_for_event_type_with_pending(tx, event_type)
                    .unwrap_or_default(),
                where_in: where_in
                    .as_ref()
                    .map(|where_in| (where_in.column.clone(), where_in.values.clone())),
            }),
            Statement::DropRoute { name } => Some(DdlChange::DropRoute {
                name: name.clone(),
                table: self
                    .event_bus_table_for_route_with_pending(tx, name)
                    .unwrap_or_default(),
            }),
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
        let _operation = self.open_operation()?;
        self.assert_cron_callback_tx_bound_handle(tx)?;
        self.ensure_trigger_table_ready(table, "insert_row")?;
        // Statement-scoped bound: `Value::TxId(n)` must satisfy
        // `n <= max(committed_watermark, tx)` so writes inside an active
        // transaction can reference their own allocated TxId. The error,
        // when fired, still reports `committed_watermark` per plan B7.
        let mut values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        self.complete_insert_access_values(table, &mut values)?;
        self.validate_row_constraints(tx, table, &values, None)?;
        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(table, row_id, &values, self.snapshot())?;
        self.relational
            .insert_with_row_id(tx, table, row_id, values, self.snapshot())
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
        self.ensure_trigger_table_ready(table, "insert_row_replacing")?;
        let mut values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        self.complete_insert_access_values(table, &mut values)?;
        self.validate_row_constraints(tx, table, &values, Some(old_row_id))?;
        self.assert_row_write_allowed(table, old_row_id, &values, self.snapshot())?;
        self.relational
            .insert_with_row_id(tx, table, old_row_id, values, self.snapshot())
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
        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(table, row_id, &values, self.snapshot())?;
        self.relational
            .insert_with_row_id(tx, table, row_id, values, self.snapshot())
    }

    pub(crate) fn upsert_row_for_sync(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
    ) -> Result<UpsertResult> {
        let values = self.coerce_row_for_insert(table, values, None, None)?;
        let snapshot = self.snapshot_for_read();
        let existing_row = values
            .get(conflict_col)
            .map(|conflict_value| {
                self.point_lookup_in_tx(tx, table, conflict_col, conflict_value, snapshot)
            })
            .transpose()?
            .flatten();
        let existing_row_id = existing_row.as_ref().map(|row| row.row_id);

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

        if let Some(existing) = existing_row.as_ref() {
            self.assert_row_write_allowed(table, existing.row_id, &existing.values, snapshot)?;
            self.assert_row_write_allowed(table, existing.row_id, &values, snapshot)?;
            let changed = values
                .iter()
                .any(|(column, value)| existing.values.get(column) != Some(value));
            if !changed {
                return Ok(UpsertResult::NoOp);
            }
            self.validate_commit_time_upsert_state_transition(table, existing, &values)?;
            self.relational.delete(tx, table, existing.row_id)?;
            self.relational
                .insert_with_row_id(tx, table, existing.row_id, values, snapshot)?;
            if let (Some(uuid), Some(state), Some(_meta)) =
                (row_uuid, new_state.as_deref(), meta.as_ref())
            {
                self.propagate_state_change_if_needed(tx, table, Some(uuid), Some(state))?;
            }
            return Ok(UpsertResult::Updated);
        }

        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(table, row_id, &values, snapshot)?;
        self.relational
            .insert_with_row_id(tx, table, row_id, values, snapshot)?;
        if let (Some(uuid), Some(state), Some(_meta)) =
            (row_uuid, new_state.as_deref(), meta.as_ref())
        {
            self.propagate_state_change_if_needed(tx, table, Some(uuid), Some(state))?;
        }
        Ok(UpsertResult::Inserted)
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
        mut values: HashMap<ColName, Value>,
    ) -> Result<InsertRowResult> {
        self.ensure_trigger_table_ready(table, "insert_row")?;
        self.complete_insert_access_values(table, &mut values)?;
        let allow_duplicate_unique_noop =
            !self.table_meta(table).is_some_and(|meta| meta.immutable);
        match self.check_row_constraints(tx, table, &values, None, allow_duplicate_unique_noop)? {
            RowConstraintCheck::Valid => {
                let row_id = self.relational_store.new_row_id();
                self.assert_row_write_allowed(table, row_id, &values, self.snapshot())?;
                self.relational
                    .insert_with_row_id(tx, table, row_id, values, self.snapshot())
                    .map(InsertRowResult::Inserted)
            }
            RowConstraintCheck::DuplicateUniqueNoOp => Ok(InsertRowResult::NoOp),
        }
    }

    pub fn upsert_row(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        mut values: HashMap<ColName, Value>,
    ) -> Result<UpsertResult> {
        let _operation = self.open_operation()?;
        self.assert_cron_callback_tx_bound_handle(tx)?;
        self.ensure_trigger_table_ready(table, "upsert_row")?;
        self.complete_insert_access_values(table, &mut values)?;
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
        let Some(existing) = existing_row.as_ref() else {
            let row_id = self.relational_store.new_row_id();
            self.assert_row_write_allowed(table, row_id, &values, snapshot)?;

            let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
            let meta = self.table_meta(table);
            let new_state = meta
                .as_ref()
                .and_then(|m| m.state_machine.as_ref())
                .and_then(|sm| values.get(&sm.column))
                .and_then(Value::as_text)
                .map(std::borrow::ToOwned::to_owned);

            self.relational
                .insert_with_row_id(tx, table, row_id, values, snapshot)?;
            if let (Some(uuid), Some(state), Some(_meta)) =
                (row_uuid, new_state.as_deref(), meta.as_ref())
            {
                self.propagate_state_change_if_needed(tx, table, Some(uuid), Some(state))?;
            }
            return Ok(UpsertResult::Inserted);
        };
        {
            self.assert_row_write_allowed(table, existing.row_id, &existing.values, snapshot)?;
            self.assert_row_write_allowed(table, existing.row_id, &values, snapshot)?;
        }

        let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
        let meta = self.table_meta(table);
        let new_state = meta
            .as_ref()
            .and_then(|m| m.state_machine.as_ref())
            .and_then(|sm| values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);

        let changed = values
            .iter()
            .any(|(column, value)| existing.values.get(column) != Some(value));
        if !changed {
            return Ok(UpsertResult::NoOp);
        }
        self.validate_commit_time_upsert_state_transition(table, existing, &values)?;
        self.relational.delete(tx, table, existing.row_id)?;
        self.relational
            .insert_with_row_id(tx, table, existing.row_id, values, snapshot)?;

        if let (Some(uuid), Some(state), Some(_meta)) =
            (row_uuid, new_state.as_deref(), meta.as_ref())
        {
            self.propagate_state_change_if_needed(tx, table, Some(uuid), Some(state))?;
        }

        Ok(UpsertResult::Updated)
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
        // Constraint probes MUST see the current committed watermark, not any
        // thread-local override. A PK/UNIQUE violation on a committed row must
        // be detected even if the caller pinned a pre-violation snapshot for
        // read visibility.
        let snapshot = self.snapshot();
        self.check_row_constraints_at_snapshot(
            tx,
            table,
            values,
            skip_row_id,
            allow_duplicate_unique_noop,
            snapshot,
        )
    }

    fn check_row_constraints_at_snapshot(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        skip_row_id: Option<RowId>,
        allow_duplicate_unique_noop: bool,
        snapshot: SnapshotId,
    ) -> Result<RowConstraintCheck> {
        let metas = self.relational_store.table_meta.read();
        let meta = metas
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

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
                tx,
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
                tx,
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
        tx: TxId,
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
        if self.row_id_is_staged_insert(tx, table, matched_row_id)? {
            return Err(Error::UniqueViolation {
                table: table.to_string(),
                column: column.to_string(),
            });
        }

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

    fn row_id_is_staged_insert(&self, tx: TxId, table: &str, row_id: RowId) -> Result<bool> {
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_inserts
                .iter()
                .any(|(staged_table, staged_row)| {
                    staged_table == table && staged_row.row_id == row_id
                })
        })
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

    pub(crate) fn rewrite_txid_placeholders(
        &self,
        origin_tx: TxId,
        ws: &mut WriteSet,
    ) -> Result<()> {
        for (table, row) in &mut ws.relational_inserts {
            self.rewrite_txid_placeholders_in_values(
                table,
                origin_tx,
                row.created_tx,
                &mut row.values,
            );
        }
        Ok(())
    }

    fn rewrite_txid_placeholders_in_values(
        &self,
        table: &str,
        origin_tx: TxId,
        canonical_tx: TxId,
        values: &mut HashMap<ColName, Value>,
    ) {
        let Some(meta) = self.table_meta(table) else {
            return;
        };
        for column in meta
            .columns
            .iter()
            .filter(|column| !column.nullable && matches!(column.column_type, ColumnType::TxId))
        {
            if matches!(values.get(&column.name), Some(Value::TxId(tx)) if *tx == origin_tx) {
                values.insert(column.name.clone(), Value::TxId(canonical_tx));
            }
        }
    }

    fn commit_validate(
        &self,
        origin_tx: TxId,
        ws: &mut WriteSet,
    ) -> Result<CommitValidationOutcome> {
        let metadata = self
            .pending_commit_metadata
            .lock()
            .remove(&origin_tx)
            .unwrap_or_default();
        let snapshot = self.snapshot();
        let conditional_noop_count =
            self.revalidate_conditional_updates(ws, snapshot, &metadata.conditional_update_guards)?;
        self.validate_unique_constraints_in_write_set(ws, snapshot, &metadata.upsert_intents)?;
        self.validate_foreign_keys_in_write_set(ws, snapshot)?;
        self.validate_composite_foreign_keys_in_write_set(ws, snapshot)?;
        Ok(CommitValidationOutcome {
            conditional_noop_count,
        })
    }

    fn take_conditional_update_guards_for_tx(
        &self,
        tx: TxId,
    ) -> Vec<PendingConditionalUpdateGuard> {
        self.pending_commit_metadata
            .lock()
            .get_mut(&tx)
            .map(|metadata| std::mem::take(&mut metadata.conditional_update_guards))
            .unwrap_or_default()
    }

    fn pending_upsert_intents_for_tx(&self, tx: TxId) -> Vec<PendingUpsertIntent> {
        self.pending_commit_metadata
            .lock()
            .get(&tx)
            .map(|metadata| metadata.upsert_intents.clone())
            .unwrap_or_default()
    }

    fn rewrite_commit_time_upserts_for_write_set(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        upsert_intents: &[PendingUpsertIntent],
    ) -> Result<()> {
        let mut index = 0;
        while index < ws.relational_inserts.len() {
            let table = ws.relational_inserts[index].0.clone();
            let skip_deleted = Self::deleted_row_ids_for_table(ws, &table);
            if self.apply_original_commit_time_upsert_if_needed(
                ws,
                index,
                &skip_deleted,
                upsert_intents,
                snapshot,
            )? {
                index = 0;
                continue;
            }
            index += 1;
        }
        Ok(())
    }

    fn deleted_row_ids_for_table(ws: &WriteSet, table: &str) -> HashSet<RowId> {
        ws.relational_deletes
            .iter()
            .filter(|(t, _, _)| t == table)
            .map(|(_, row_id, _)| *row_id)
            .collect()
    }

    fn committed_unique_conflict(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
    ) -> Result<Option<VersionedRow>> {
        if columns.is_empty() || columns.len() != values.len() {
            return Ok(None);
        }

        fn visible_posting(
            entries: &[contextdb_relational::IndexEntry],
            snapshot: SnapshotId,
            skip_deleted: &HashSet<RowId>,
        ) -> Option<RowId> {
            entries
                .iter()
                .find(|entry| !skip_deleted.contains(&entry.row_id) && entry.visible_at(snapshot))
                .map(|entry| entry.row_id)
        }

        let (index_checked, indexed_row_id) = {
            let indexes = self.relational_store.indexes.read();
            if columns.len() == 1 {
                let column = &columns[0];
                let table_key = table.to_string();
                let pk_key = (table_key.clone(), format!("__pk_{column}"));
                let unique_key = (table_key, format!("__unique_{column}"));
                let storage = indexes
                    .get(&pk_key)
                    .or_else(|| indexes.get(&unique_key))
                    .or_else(|| {
                        indexes.iter().find_map(|((t, _), idx)| {
                            if t == table && idx.columns.len() == 1 && idx.columns[0].0 == *column {
                                Some(idx)
                            } else {
                                None
                            }
                        })
                    });
                match storage {
                    Some(storage) => {
                        let key = index_key_from_values(&storage.columns[..1], values);
                        (
                            true,
                            storage.tree.get(&key).and_then(|entries| {
                                visible_posting(entries, snapshot, skip_deleted)
                            }),
                        )
                    }
                    None => (false, None),
                }
            } else {
                let storage = indexes.iter().find_map(|((t, _), idx)| {
                    if t == table
                        && idx.columns.len() >= columns.len()
                        && idx
                            .columns
                            .iter()
                            .zip(columns.iter())
                            .all(|((have, _), want)| have == want)
                    {
                        Some(idx)
                    } else {
                        None
                    }
                });
                match storage {
                    Some(storage) => {
                        let prefix =
                            index_key_from_values(&storage.columns[..columns.len()], values);
                        let row_id = if storage.columns.len() == columns.len() {
                            storage.tree.get(&prefix).and_then(|entries| {
                                visible_posting(entries, snapshot, skip_deleted)
                            })
                        } else {
                            storage
                                .tree
                                .range(prefix.clone()..)
                                .take_while(|(key, _)| {
                                    key.len() >= prefix.len() && key[..prefix.len()] == prefix[..]
                                })
                                .find_map(|(_, entries)| {
                                    visible_posting(entries, snapshot, skip_deleted)
                                })
                        };
                        (true, row_id)
                    }
                    None => (false, None),
                }
            }
        };

        if let Some(row_id) = indexed_row_id {
            return Ok(self.relational_store.row_by_id(table, row_id, snapshot));
        }
        if index_checked {
            return Ok(None);
        }

        let rows = self
            .relational
            .scan_filter_with_tx(None, table, snapshot, &|row| {
                !skip_deleted.contains(&row.row_id)
                    && columns
                        .iter()
                        .zip(values.iter())
                        .all(|(column, value)| row.values.get(column) == Some(value))
            })?;
        Ok(rows.into_iter().next())
    }

    fn staged_unique_conflict_in_write_set(
        ws: &WriteSet,
        insert_index: usize,
        table: &str,
        columns: &[String],
        values: &[Value],
    ) -> Option<RowId> {
        ws.relational_inserts.iter().enumerate().find_map(
            |(other_index, (other_table, other_row))| {
                if other_index == insert_index || other_table != table {
                    return None;
                }
                if ws.relational_inserts[insert_index].1.row_id == other_row.row_id {
                    return None;
                }
                columns
                    .iter()
                    .zip(values.iter())
                    .all(|(column, value)| other_row.values.get(column) == Some(value))
                    .then_some(other_row.row_id)
            },
        )
    }

    fn indexed_visible_row_exists(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
    ) -> Option<bool> {
        use contextdb_core::{DirectedValue, TotalOrdAsc, TotalOrdDesc};

        let indexes = self.relational_store.indexes.read();
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
            })?;
        let key = match storage.columns.first().map(|(_, direction)| direction) {
            Some(SortDirection::Desc) => vec![DirectedValue::Desc(TotalOrdDesc(value.clone()))],
            _ => vec![DirectedValue::Asc(TotalOrdAsc(value.clone()))],
        };
        Some(storage.tree.get(&key).is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| !skip_deleted.contains(&entry.row_id) && entry.visible_at(snapshot))
        }))
    }

    fn visible_row_exists_by_column(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
    ) -> Result<bool> {
        if let Some(exists) =
            self.indexed_visible_row_exists(table, column, value, snapshot, skip_deleted)
        {
            return Ok(exists);
        }

        let rows = self
            .relational
            .scan_filter_with_tx(None, table, snapshot, &|row| {
                !skip_deleted.contains(&row.row_id) && row.values.get(column) == Some(value)
            })?;
        Ok(!rows.is_empty())
    }

    fn visible_row_by_column(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
    ) -> Result<Option<VersionedRow>> {
        let rows = self
            .relational
            .scan_filter_with_tx(None, table, snapshot, &|row| {
                !skip_deleted.contains(&row.row_id) && row.values.get(column) == Some(value)
            })?;
        Ok(rows.into_iter().next())
    }

    fn indexed_visible_row_exists_by_columns(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
    ) -> Result<bool> {
        if columns.is_empty() || columns.len() != values.len() {
            return Ok(false);
        }

        fn visible_posting(
            entries: &[contextdb_relational::IndexEntry],
            snapshot: SnapshotId,
            skip_deleted: &HashSet<RowId>,
        ) -> Option<RowId> {
            entries
                .iter()
                .find(|entry| !skip_deleted.contains(&entry.row_id) && entry.visible_at(snapshot))
                .map(|entry| entry.row_id)
        }

        let row_id = {
            let indexes = self.relational_store.indexes.read();
            let Some(storage) = indexes.iter().find_map(|((indexed_table, _), storage)| {
                if indexed_table == table
                    && storage.columns.len() >= columns.len()
                    && storage
                        .columns
                        .iter()
                        .zip(columns.iter())
                        .all(|((indexed_column, _), wanted)| indexed_column == wanted)
                {
                    Some(storage)
                } else {
                    None
                }
            }) else {
                self.fk_full_scan_fallbacks.fetch_add(1, Ordering::SeqCst);
                return Err(Error::Other(format!(
                    "composite foreign key probe on {table}({}) has no covering index",
                    columns.join(", ")
                )));
            };

            self.fk_indexed_tuple_probes.fetch_add(1, Ordering::SeqCst);
            let prefix = index_key_from_values(&storage.columns[..columns.len()], values);
            if storage.columns.len() == columns.len() {
                storage
                    .tree
                    .get(&prefix)
                    .and_then(|entries| visible_posting(entries, snapshot, skip_deleted))
            } else {
                storage
                    .tree
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| {
                        key.len() >= prefix.len() && key[..prefix.len()] == prefix[..]
                    })
                    .find_map(|(_, entries)| visible_posting(entries, snapshot, skip_deleted))
            }
        };
        Ok(row_id.is_some_and(|row_id| {
            self.relational_store
                .row_by_id(table, row_id, snapshot)
                .is_some()
        }))
    }

    fn apply_commit_time_upsert(
        &self,
        ws: &mut WriteSet,
        insert_index: usize,
        columns: &[String],
        conflict_row: &VersionedRow,
        upsert_intents: &[PendingUpsertIntent],
        snapshot: SnapshotId,
    ) -> Result<bool> {
        let Some(intent) = upsert_intents.iter().find(|intent| {
            intent.table == ws.relational_inserts[insert_index].0
                && intent.row_id == ws.relational_inserts[insert_index].1.row_id
                && intent.conflict_columns == columns
        }) else {
            return Ok(false);
        };

        let (table, incoming) = ws.relational_inserts[insert_index].clone();
        let on_conflict = OnConflictPlan {
            columns: intent.conflict_columns.clone(),
            update_columns: intent.update_columns.clone(),
        };
        let mut original_insert_values = intent.insert_values.clone();
        self.rewrite_txid_placeholders_in_values(
            &table,
            intent.active_tx,
            incoming.created_tx,
            &mut original_insert_values,
        );
        let mut values = apply_on_conflict_updates(
            self,
            &table,
            original_insert_values.clone(),
            conflict_row,
            &on_conflict,
            &intent.params,
            Some(intent.active_tx),
        )?;
        self.rewrite_txid_placeholders_in_values(
            &table,
            intent.active_tx,
            incoming.created_tx,
            &mut values,
        );
        for (column, incoming_value) in &incoming.values {
            if original_insert_values.get(column) != Some(incoming_value) {
                values.insert(column.clone(), incoming_value.clone());
            }
        }
        self.validate_commit_time_upsert_replacement(&table, conflict_row, &values, snapshot)?;

        let incoming_row_bytes = self
            .table_meta(&table)
            .map(|meta| estimate_row_bytes_for_meta(&incoming.values, &meta, false))
            .unwrap_or_else(|| incoming.estimated_bytes());
        let replacement_row_bytes = self
            .table_meta(&table)
            .map(|meta| estimate_row_bytes_for_meta(&values, &meta, false))
            .unwrap_or_else(|| {
                let mut replacement = incoming.clone();
                replacement.values = values.clone();
                replacement.estimated_bytes()
            });
        let extra_row_bytes = replacement_row_bytes.saturating_sub(incoming_row_bytes);
        if extra_row_bytes > 0 {
            self.accountant.try_allocate_for(
                extra_row_bytes,
                "insert",
                "commit_time_upsert_row_rewrite",
                "Reduce row size or raise MEMORY_LIMIT before committing this UPSERT.",
            )?;
        }

        let mut replacement = incoming;
        let incoming_row_id = replacement.row_id;
        replacement.row_id = conflict_row.row_id;
        replacement.values = values;
        if let Err(err) = self.reconcile_commit_time_upsert_vectors(
            ws,
            &table,
            incoming_row_id,
            conflict_row.row_id,
            &replacement.values,
            replacement.created_tx,
            replacement.lsn,
            snapshot,
        ) {
            if extra_row_bytes > 0 {
                self.accountant.release(extra_row_bytes);
            }
            return Err(err);
        }
        if incoming_row_bytes > replacement_row_bytes {
            self.accountant
                .release(incoming_row_bytes - replacement_row_bytes);
        }
        let row_uuid = replacement
            .values
            .get("id")
            .and_then(Value::as_uuid)
            .copied();
        let new_state = self
            .table_meta(&table)
            .and_then(|meta| meta.state_machine)
            .and_then(|sm| replacement.values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);
        let changed = replacement
            .values
            .iter()
            .any(|(column, value)| conflict_row.values.get(column) != Some(value));
        ws.relational_inserts[insert_index] = (table.clone(), replacement);
        if !ws
            .relational_deletes
            .iter()
            .any(|(t, row_id, _)| t == &table && *row_id == conflict_row.row_id)
        {
            ws.relational_deletes.push((
                table.clone(),
                conflict_row.row_id,
                ws.relational_inserts[insert_index].1.created_tx,
            ));
        }
        if changed && let (Some(uuid), Some(state)) = (row_uuid, new_state.as_deref()) {
            let replacement_tx = ws.relational_inserts[insert_index].1.created_tx;
            self.propagate_state_change_in_prepared_write_set(
                ws,
                replacement_tx,
                &table,
                Some(uuid),
                Some(state),
                snapshot,
            )?;
        }
        Ok(true)
    }

    fn validate_commit_time_upsert_replacement(
        &self,
        table: &str,
        conflict_row: &VersionedRow,
        values: &HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<()> {
        if self.relational_store().is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }
        self.assert_row_write_allowed(table, conflict_row.row_id, &conflict_row.values, snapshot)?;
        self.assert_row_write_allowed(table, conflict_row.row_id, values, snapshot)?;
        self.validate_commit_time_upsert_state_transition(table, conflict_row, values)?;
        self.validate_commit_time_upsert_vectors(table, values)
    }

    fn validate_commit_time_upsert_state_transition(
        &self,
        table: &str,
        conflict_row: &VersionedRow,
        values: &HashMap<ColName, Value>,
    ) -> Result<()> {
        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };
        let Some(state_machine) = meta.state_machine else {
            return Ok(());
        };

        let old_state = conflict_row
            .values
            .get(&state_machine.column)
            .and_then(Value::as_text);
        let new_state = values.get(&state_machine.column).and_then(Value::as_text);
        let (Some(old_state), Some(new_state)) = (old_state, new_state) else {
            return Ok(());
        };

        if self.relational_store().validate_state_transition(
            table,
            &state_machine.column,
            old_state,
            new_state,
        ) {
            return Ok(());
        }

        Err(Error::InvalidStateTransition(format!(
            "{old_state} -> {new_state}"
        )))
    }

    fn validate_commit_time_upsert_vectors(
        &self,
        table: &str,
        values: &HashMap<ColName, Value>,
    ) -> Result<()> {
        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };

        for column in &meta.columns {
            if let ColumnType::Vector(expected) = column.column_type
                && let Some(Value::Vector(vector)) = values.get(&column.name)
            {
                let got = vector.len();
                if got != expected {
                    return Err(self.direct_vector_dimension_error(
                        &VectorIndexRef::new(table, column.name.clone()),
                        expected,
                        got,
                    ));
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn reconcile_commit_time_upsert_vectors(
        &self,
        ws: &mut WriteSet,
        table: &str,
        incoming_row_id: RowId,
        conflict_row_id: RowId,
        replacement_values: &HashMap<ColName, Value>,
        replacement_tx: TxId,
        replacement_lsn: Lsn,
        snapshot: SnapshotId,
    ) -> Result<()> {
        let Some(meta) = self.table_meta(table) else {
            return Ok(());
        };
        let vector_columns = meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        if vector_columns.is_empty() {
            return Ok(());
        }

        let mut pending_by_column = HashMap::<ColName, VectorEntry>::new();
        let mut pos = 0;
        while pos < ws.vector_inserts.len() {
            let entry = &ws.vector_inserts[pos];
            if entry.index.table == table && entry.row_id == incoming_row_id {
                let entry = ws.vector_inserts.remove(pos);
                if let Some(replaced) = pending_by_column.insert(entry.index.column.clone(), entry)
                {
                    self.accountant.release(
                        self.vector_insert_accounted_bytes(&replaced.index, replaced.vector.len()),
                    );
                }
            } else {
                pos += 1;
            }
        }

        for column in vector_columns {
            let index = VectorIndexRef::new(table, column.clone());
            let existing = self.vector_store_live_entry_for_row(&index, conflict_row_id, snapshot);
            let final_vector = match replacement_values.get(&column) {
                Some(Value::Vector(vector)) => Some(vector.clone()),
                _ => None,
            };
            let Some(final_vector) = final_vector else {
                if let Some(pending) = pending_by_column.remove(&column) {
                    self.accountant.release(
                        self.vector_insert_accounted_bytes(&pending.index, pending.vector.len()),
                    );
                }
                if existing.is_some()
                    && !ws
                        .vector_deletes
                        .iter()
                        .any(|(pending_index, pending_row_id, _)| {
                            *pending_index == index && *pending_row_id == conflict_row_id
                        })
                {
                    ws.vector_deletes
                        .push((index.clone(), conflict_row_id, replacement_tx));
                }
                continue;
            };

            if existing
                .as_ref()
                .is_some_and(|entry| entry.vector == final_vector)
            {
                if let Some(pending) = pending_by_column.remove(&column) {
                    self.accountant.release(
                        self.vector_insert_accounted_bytes(&pending.index, pending.vector.len()),
                    );
                }
                continue;
            }

            if existing.is_some()
                && !ws
                    .vector_deletes
                    .iter()
                    .any(|(pending_index, pending_row_id, _)| {
                        *pending_index == index && *pending_row_id == conflict_row_id
                    })
            {
                ws.vector_deletes
                    .push((index.clone(), conflict_row_id, replacement_tx));
            }

            let mut entry = match pending_by_column.remove(&column) {
                Some(mut pending) if pending.vector == final_vector => {
                    pending.row_id = conflict_row_id;
                    pending.created_tx = replacement_tx;
                    pending.deleted_tx = None;
                    pending.lsn = replacement_lsn;
                    pending
                }
                Some(pending) => {
                    self.accountant.release(
                        self.vector_insert_accounted_bytes(&pending.index, pending.vector.len()),
                    );
                    let bytes = self.vector_insert_accounted_bytes(&index, final_vector.len());
                    self.accountant.try_allocate_for(
                        bytes,
                        "insert",
                        &format!("vector_insert@{}.{}", index.table, index.column),
                        "Reduce vector dimensionality, insert fewer rows, or raise MEMORY_LIMIT.",
                    )?;
                    VectorEntry {
                        index: index.clone(),
                        row_id: conflict_row_id,
                        vector: final_vector.clone(),
                        created_tx: replacement_tx,
                        deleted_tx: None,
                        lsn: replacement_lsn,
                    }
                }
                None => {
                    let bytes = self.vector_insert_accounted_bytes(&index, final_vector.len());
                    self.accountant.try_allocate_for(
                        bytes,
                        "insert",
                        &format!("vector_insert@{}.{}", index.table, index.column),
                        "Reduce vector dimensionality, insert fewer rows, or raise MEMORY_LIMIT.",
                    )?;
                    VectorEntry {
                        index: index.clone(),
                        row_id: conflict_row_id,
                        vector: final_vector.clone(),
                        created_tx: replacement_tx,
                        deleted_tx: None,
                        lsn: replacement_lsn,
                    }
                }
            };
            entry.index = index;

            let mut replaced_entries = Vec::new();
            let mut pos = 0;
            while pos < ws.vector_inserts.len() {
                if ws.vector_inserts[pos].index == entry.index
                    && ws.vector_inserts[pos].row_id == conflict_row_id
                {
                    replaced_entries.push(ws.vector_inserts.remove(pos));
                } else {
                    pos += 1;
                }
            }
            for replaced in replaced_entries {
                self.accountant.release(
                    self.vector_insert_accounted_bytes(&replaced.index, replaced.vector.len()),
                );
            }
            ws.vector_inserts.push(entry);
        }

        for pending in pending_by_column.into_values() {
            self.accountant
                .release(self.vector_insert_accounted_bytes(&pending.index, pending.vector.len()));
        }
        Ok(())
    }

    fn apply_original_commit_time_upsert_if_needed(
        &self,
        ws: &mut WriteSet,
        insert_index: usize,
        skip_deleted: &HashSet<RowId>,
        upsert_intents: &[PendingUpsertIntent],
        snapshot: SnapshotId,
    ) -> Result<bool> {
        let (table, row) = ws.relational_inserts[insert_index].clone();
        for intent in upsert_intents.iter().filter(|intent| {
            intent.table == table
                && intent.row_id == row.row_id
                && !intent.conflict_columns.is_empty()
        }) {
            let mut insert_values = intent.insert_values.clone();
            self.rewrite_txid_placeholders_in_values(
                &table,
                intent.active_tx,
                row.created_tx,
                &mut insert_values,
            );
            let mut conflict_values = Vec::with_capacity(intent.conflict_columns.len());
            let mut has_null = false;
            for column in &intent.conflict_columns {
                match insert_values.get(column) {
                    Some(Value::Null) | None => {
                        has_null = true;
                        break;
                    }
                    Some(value) => conflict_values.push(value.clone()),
                }
            }
            if has_null {
                continue;
            }

            let Some(conflict) = self.committed_unique_conflict(
                &table,
                &intent.conflict_columns,
                &conflict_values,
                snapshot,
                skip_deleted,
            )?
            else {
                continue;
            };
            if self.apply_commit_time_upsert(
                ws,
                insert_index,
                &intent.conflict_columns,
                &conflict,
                upsert_intents,
                snapshot,
            )? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn validate_unique_constraints_in_write_set(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        upsert_intents: &[PendingUpsertIntent],
    ) -> Result<()> {
        let mut index = 0;
        while index < ws.relational_inserts.len() {
            let (table, row) = ws.relational_inserts[index].clone();
            let meta = self
                .table_meta(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            let skip_deleted = Self::deleted_row_ids_for_table(ws, &table);
            let mut transformed_upsert = false;

            if self.apply_original_commit_time_upsert_if_needed(
                ws,
                index,
                &skip_deleted,
                upsert_intents,
                snapshot,
            )? {
                // Re-check the rewritten post-image against every UNIQUE
                // constraint before moving on. Prepared state propagation can
                // remove earlier staged rows, so restart rather than trusting
                // the old vector index.
                index = 0;
                continue;
            }

            for column in meta
                .columns
                .iter()
                .filter(|column| column.primary_key || column.unique)
            {
                let Some(value) = row.values.get(&column.name) else {
                    continue;
                };
                if *value == Value::Null {
                    continue;
                }
                let columns = vec![column.name.clone()];
                let values = vec![value.clone()];
                if Self::staged_unique_conflict_in_write_set(ws, index, &table, &columns, &values)
                    .is_some()
                {
                    return Err(Error::UniqueViolation {
                        table,
                        column: column.name.clone(),
                    });
                }
                if let Some(conflict) = self.committed_unique_conflict(
                    &table,
                    &columns,
                    &values,
                    snapshot,
                    &skip_deleted,
                )? {
                    if self.apply_commit_time_upsert(
                        ws,
                        index,
                        &columns,
                        &conflict,
                        upsert_intents,
                        snapshot,
                    )? {
                        transformed_upsert = true;
                        break;
                    }
                    return Err(Error::UniqueViolation {
                        table,
                        column: column.name.clone(),
                    });
                }
            }
            if transformed_upsert {
                // Re-check the rewritten post-image against every UNIQUE
                // constraint before moving on. Prepared state propagation can
                // remove earlier staged rows, so restart rather than trusting
                // the old vector index.
                index = 0;
                continue;
            }

            for unique_constraint in &meta.unique_constraints {
                let mut values = Vec::with_capacity(unique_constraint.len());
                let mut has_null = false;
                for column in unique_constraint {
                    match row.values.get(column) {
                        Some(Value::Null) | None => {
                            has_null = true;
                            break;
                        }
                        Some(value) => values.push(value.clone()),
                    }
                }
                if has_null {
                    continue;
                }
                if Self::staged_unique_conflict_in_write_set(
                    ws,
                    index,
                    &table,
                    unique_constraint,
                    &values,
                )
                .is_some()
                {
                    return Err(Error::UniqueViolation {
                        table,
                        column: unique_constraint.first().cloned().unwrap_or_default(),
                    });
                }
                if let Some(conflict) = self.committed_unique_conflict(
                    &table,
                    unique_constraint,
                    &values,
                    snapshot,
                    &skip_deleted,
                )? {
                    if self.apply_commit_time_upsert(
                        ws,
                        index,
                        unique_constraint,
                        &conflict,
                        upsert_intents,
                        snapshot,
                    )? {
                        transformed_upsert = true;
                        break;
                    }
                    return Err(Error::UniqueViolation {
                        table,
                        column: unique_constraint.first().cloned().unwrap_or_default(),
                    });
                }
            }
            if transformed_upsert {
                // Re-check the rewritten post-image against every UNIQUE
                // constraint before moving on. Prepared state propagation can
                // remove earlier staged rows, so restart rather than trusting
                // the old vector index.
                index = 0;
                continue;
            }

            index += 1;
        }
        Ok(())
    }

    fn validate_foreign_keys_in_write_set(
        &self,
        ws: &WriteSet,
        snapshot: SnapshotId,
    ) -> Result<()> {
        for (table, row) in &ws.relational_inserts {
            let meta = self
                .table_meta(table)
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
                let staged_deletes = Self::deleted_row_ids_for_table(ws, &reference.table);
                let staged_match = ws.relational_inserts.iter().any(|(insert_table, row)| {
                    insert_table == &reference.table
                        && row.values.get(&reference.column) == Some(value)
                });
                let committed_match = self.visible_row_exists_by_column(
                    &reference.table,
                    &reference.column,
                    value,
                    snapshot,
                    &staged_deletes,
                )?;
                if !staged_match && !committed_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: table.clone(),
                        child_columns: vec![column.name.clone()],
                        parent_table: reference.table.clone(),
                        parent_columns: vec![reference.column.clone()],
                    });
                }
            }
        }

        let reverse_refs = {
            let metas = self.relational_store.table_meta.read();
            metas
                .iter()
                .flat_map(|(child_table, meta)| {
                    meta.columns.iter().filter_map(|column| {
                        column.references.as_ref().map(|reference| {
                            (
                                reference.table.clone(),
                                reference.column.clone(),
                                child_table.clone(),
                                column.name.clone(),
                            )
                        })
                    })
                })
                .collect::<Vec<_>>()
        };

        for (parent_table, parent_row_id, _) in &ws.relational_deletes {
            let Some(parent_row) =
                self.relational_store
                    .row_by_id(parent_table, *parent_row_id, snapshot)
            else {
                continue;
            };
            for (ref_table, ref_column, child_table, child_column) in &reverse_refs {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_value) = parent_row.values.get(ref_column) else {
                    continue;
                };
                let parent_replaced_with_same_key =
                    ws.relational_inserts.iter().any(|(insert_table, row)| {
                        insert_table == parent_table
                            && row.values.get(ref_column) == Some(parent_value)
                    });
                if parent_replaced_with_same_key {
                    continue;
                }
                let child_deletes = Self::deleted_row_ids_for_table(ws, child_table);
                if self.visible_row_exists_by_column(
                    child_table,
                    child_column,
                    parent_value,
                    snapshot,
                    &child_deletes,
                )? {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: vec![child_column.clone()],
                        parent_table: parent_table.clone(),
                        parent_columns: vec![ref_column.clone()],
                    });
                }
            }
        }

        Ok(())
    }

    fn validate_composite_foreign_keys_in_write_set(
        &self,
        ws: &WriteSet,
        snapshot: SnapshotId,
    ) -> Result<()> {
        for (table, row) in &ws.relational_inserts {
            let meta = self
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            for fk in &meta.composite_foreign_keys {
                let Some(values) = tuple_values_for_row(row, &fk.child_columns) else {
                    continue;
                };
                let parent_deletes = Self::deleted_row_ids_for_table(ws, &fk.parent_table);
                let staged_match =
                    staged_tuple_exists(ws, &fk.parent_table, &fk.parent_columns, &values);
                let committed_match = self.indexed_visible_row_exists_by_columns(
                    &fk.parent_table,
                    &fk.parent_columns,
                    &values,
                    snapshot,
                    &parent_deletes,
                )?;
                if !staged_match && !committed_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: table.clone(),
                        child_columns: fk.child_columns.clone(),
                        parent_table: fk.parent_table.clone(),
                        parent_columns: fk.parent_columns.clone(),
                    });
                }
            }
        }

        let reverse_refs = {
            let metas = self.relational_store.table_meta.read();
            metas
                .iter()
                .flat_map(|(child_table, meta)| {
                    meta.composite_foreign_keys.iter().map(|fk| {
                        (
                            fk.parent_table.clone(),
                            fk.parent_columns.clone(),
                            child_table.clone(),
                            fk.child_columns.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        for (parent_table, parent_row_id, _) in &ws.relational_deletes {
            let Some(parent_row) =
                self.relational_store
                    .row_by_id(parent_table, *parent_row_id, snapshot)
            else {
                continue;
            };
            for (ref_table, ref_columns, child_table, child_columns) in &reverse_refs {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_values) = tuple_values_for_row(&parent_row, ref_columns) else {
                    continue;
                };
                let parent_replaced_with_same_key =
                    staged_tuple_exists(ws, parent_table, ref_columns, &parent_values);
                if parent_replaced_with_same_key {
                    continue;
                }

                if staged_tuple_exists(ws, child_table, child_columns, &parent_values) {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: child_columns.clone(),
                        parent_table: parent_table.clone(),
                        parent_columns: ref_columns.clone(),
                    });
                }

                let child_deletes = Self::deleted_row_ids_for_table(ws, child_table);
                if self.indexed_visible_row_exists_by_columns(
                    child_table,
                    child_columns,
                    &parent_values,
                    snapshot,
                    &child_deletes,
                )? {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: child_columns.clone(),
                        parent_table: parent_table.clone(),
                        parent_columns: ref_columns.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    fn revalidate_conditional_updates(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        guards: &[PendingConditionalUpdateGuard],
    ) -> Result<u64> {
        let mut conditional_noop_count = 0_u64;
        for guard in guards.iter().rev() {
            let matches = self
                .relational_store
                .row_by_id(&guard.table, guard.row_id, snapshot)
                .is_some_and(|row| {
                    guard
                        .predicates
                        .iter()
                        .all(|(column, value)| row.values.get(column) == Some(value))
                });
            if matches {
                continue;
            }

            self.release_insert_allocations_for_slice(ws, guard.before, guard.after);
            remove_write_set_slice(ws, guard.before, guard.after);
            conditional_noop_count = conditional_noop_count.saturating_add(1);
        }
        Ok(conditional_noop_count)
    }

    fn release_insert_allocations_for_slice(
        &self,
        ws: &WriteSet,
        before: WriteSetCounts,
        after: WriteSetCounts,
    ) {
        for (table, row) in ws
            .relational_inserts
            .iter()
            .skip(before.relational_inserts)
            .take(
                after
                    .relational_inserts
                    .saturating_sub(before.relational_inserts),
            )
        {
            let bytes = self
                .table_meta(table)
                .map(|meta| estimate_row_bytes_for_meta(&row.values, &meta, false))
                .unwrap_or_else(|| row.estimated_bytes());
            self.accountant.release(bytes);
        }

        for edge in ws
            .adj_inserts
            .iter()
            .skip(before.adj_inserts)
            .take(after.adj_inserts.saturating_sub(before.adj_inserts))
        {
            self.accountant.release(edge.estimated_bytes());
        }

        for entry in ws
            .vector_inserts
            .iter()
            .skip(before.vector_inserts)
            .take(after.vector_inserts.saturating_sub(before.vector_inserts))
        {
            self.accountant
                .release(self.vector_insert_accounted_bytes(&entry.index, entry.vector.len()));
        }
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

            self.assert_row_write_allowed(
                &entry.table,
                existing.row_id,
                &existing.values,
                snapshot,
            )?;
            self.assert_row_write_allowed(&entry.table, existing.row_id, &next_values, snapshot)?;

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

    fn propagate_state_change_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        tx: TxId,
        table: &str,
        row_uuid: Option<uuid::Uuid>,
        new_state: Option<&str>,
        snapshot: SnapshotId,
    ) -> Result<()> {
        let (Some(uuid), Some(state)) = (row_uuid, new_state) else {
            return Ok(());
        };
        if ws.propagation_in_progress {
            return Ok(());
        }

        ws.propagation_in_progress = true;
        let result = self.propagate_in_prepared_write_set(ws, tx, table, uuid, state, snapshot);
        ws.propagation_in_progress = false;
        result
    }

    fn propagate_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        tx: TxId,
        table: &str,
        row_uuid: uuid::Uuid,
        new_state: &str,
        snapshot: SnapshotId,
    ) -> Result<()> {
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

        self.enqueue_fk_children_in_prepared_write_set(ws, &ctx, &mut queue, root);
        self.enqueue_edge_children_in_prepared_write_set(ws, &ctx, &mut queue, root)?;
        self.apply_vector_exclusions_in_prepared_write_set(ws, &ctx, root)?;

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
            let Some(existing) = self.point_lookup_in_prepared_write_set(
                ws,
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

            self.assert_row_write_allowed(
                &entry.table,
                existing.row_id,
                &existing.values,
                snapshot,
            )?;
            self.assert_row_write_allowed(&entry.table, existing.row_id, &next_values, snapshot)?;

            let reached_state = match self.upsert_row_in_prepared_write_set(
                ws,
                tx,
                &entry.table,
                &state_column,
                &existing,
                next_values,
            ) {
                Ok(true) => entry.target_state.as_str(),
                Ok(false) => continue,
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

            let propagated = PropagationSource {
                table: &entry.table,
                uuid: entry.uuid,
                state: reached_state,
                depth: entry.depth,
            };
            self.enqueue_edge_children_in_prepared_write_set(ws, &ctx, &mut queue, propagated)?;
            self.apply_vector_exclusions_in_prepared_write_set(ws, &ctx, propagated)?;
            self.enqueue_fk_children_in_prepared_write_set(ws, &ctx, &mut queue, propagated);
        }

        if let Some(err) = abort_violation {
            return Err(err);
        }

        Ok(())
    }

    fn enqueue_fk_children_in_prepared_write_set(
        &self,
        ws: &WriteSet,
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

                let rows = match self.scan_filter_in_prepared_write_set(
                    ws,
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

    fn enqueue_edge_children_in_prepared_write_set(
        &self,
        ws: &WriteSet,
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
                    .point_lookup_in_prepared_write_set(
                        ws,
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

    fn apply_vector_exclusions_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
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
            let Some(index) = self.table_meta(source.table).and_then(|meta| {
                meta.columns
                    .iter()
                    .find(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    .map(|column| VectorIndexRef::new(source.table, column.name.clone()))
            }) else {
                continue;
            };
            for row_id in self.logical_row_ids_for_uuid_in_prepared_write_set(
                ws,
                source.table,
                source.uuid,
                ctx.snapshot,
            )? {
                self.delete_vector_in_prepared_write_set(
                    ws,
                    ctx.tx,
                    index.clone(),
                    row_id,
                    ctx.snapshot,
                )?;
            }
        }

        Ok(())
    }

    fn scan_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        let tables = self.relational_store.tables.read();
        let rows = tables
            .get(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        let mut result: Vec<VersionedRow> = rows
            .iter()
            .filter(|row| row.visible_at(snapshot))
            .cloned()
            .collect();

        let committed_row_ids: HashSet<RowId> = result.iter().map(|row| row.row_id).collect();
        let deleted_row_ids: HashSet<RowId> = ws
            .relational_deletes
            .iter()
            .filter(|(delete_table, _, _)| delete_table == table)
            .map(|(_, row_id, _)| *row_id)
            .collect();
        result.retain(|row| !deleted_row_ids.contains(&row.row_id));

        let mut seen_inserts = HashSet::new();
        let mut inserts = ws
            .relational_inserts
            .iter()
            .rev()
            .filter(|(insert_table, row)| {
                insert_table == table
                    && seen_inserts.insert(row.row_id)
                    && (!deleted_row_ids.contains(&row.row_id)
                        || committed_row_ids.contains(&row.row_id))
            })
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>();
        inserts.reverse();
        result.extend(inserts);

        Ok(result)
    }

    fn scan_filter_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,
    ) -> Result<Vec<VersionedRow>> {
        Ok(self
            .scan_in_prepared_write_set(ws, table, snapshot)?
            .into_iter()
            .filter(predicate)
            .collect())
    }

    fn point_lookup_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        Ok(self
            .scan_in_prepared_write_set(ws, table, snapshot)?
            .into_iter()
            .find(|row| row.values.get(col) == Some(value)))
    }

    fn row_by_id_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        Ok(self
            .scan_in_prepared_write_set(ws, table, snapshot)?
            .into_iter()
            .find(|row| row.row_id == row_id))
    }

    fn logical_row_ids_for_uuid_in_prepared_write_set(
        &self,
        ws: &WriteSet,
        table: &str,
        uuid: uuid::Uuid,
        snapshot: SnapshotId,
    ) -> Result<Vec<RowId>> {
        Ok(self
            .scan_in_prepared_write_set(ws, table, snapshot)?
            .into_iter()
            .filter(|row| row.values.get("id") == Some(&Value::Uuid(uuid)))
            .map(|row| row.row_id)
            .collect())
    }

    fn upsert_row_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        tx: TxId,
        table: &str,
        state_column: &str,
        existing: &VersionedRow,
        next_values: HashMap<ColName, Value>,
    ) -> Result<bool> {
        if self.relational_store().is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }

        let old_state = existing
            .values
            .get(state_column)
            .and_then(Value::as_text)
            .unwrap_or("");
        let new_state = next_values
            .get(state_column)
            .and_then(Value::as_text)
            .unwrap_or("");
        if !self.relational_store().validate_state_transition(
            table,
            state_column,
            old_state,
            new_state,
        ) {
            return Err(Error::InvalidStateTransition(format!(
                "{old_state} -> {new_state}"
            )));
        }

        let changed = next_values
            .iter()
            .any(|(column, value)| existing.values.get(column) != Some(value));
        if !changed {
            return Ok(false);
        }

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        let row_bytes = estimate_row_bytes_for_meta(&next_values, &meta, false);
        self.accountant.try_allocate_for(
            row_bytes,
            "update",
            "prepared_state_propagation_row_replace",
            "Reduce row growth or raise MEMORY_LIMIT before committing this propagated update.",
        )?;

        if let Err(err) = self.delete_row_in_prepared_write_set(ws, tx, table, existing.row_id) {
            self.accountant.release(row_bytes);
            return Err(err);
        }

        ws.relational_inserts.push((
            table.to_string(),
            VersionedRow {
                row_id: existing.row_id,
                values: next_values,
                created_tx: tx,
                deleted_tx: None,
                lsn: ws.commit_lsn.unwrap_or(Lsn(0)),
                created_at: Some(current_wallclock()),
            },
        ));
        Ok(true)
    }

    fn delete_row_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        tx: TxId,
        table: &str,
        row_id: RowId,
    ) -> Result<()> {
        if !self.relational_store.table_meta.read().contains_key(table) {
            return Err(Error::TableNotFound(table.to_string()));
        }
        if self.relational_store().is_immutable(table) {
            return Err(Error::ImmutableTable(table.to_string()));
        }

        let mut removed_inserts = Vec::new();
        let mut pos = 0;
        while pos < ws.relational_inserts.len() {
            if ws.relational_inserts[pos].0 == table
                && ws.relational_inserts[pos].1.row_id == row_id
            {
                removed_inserts.push(ws.relational_inserts.remove(pos));
            } else {
                pos += 1;
            }
        }
        for (removed_table, row) in removed_inserts {
            let bytes = self
                .table_meta(&removed_table)
                .map(|meta| estimate_row_bytes_for_meta(&row.values, &meta, false))
                .unwrap_or_else(|| row.estimated_bytes());
            self.accountant.release(bytes);
        }

        let committed_row_exists = self
            .relational_store
            .row_by_id(table, row_id, SnapshotId::from_raw_wire(u64::MAX))
            .is_some();
        if committed_row_exists
            && !ws
                .relational_deletes
                .iter()
                .any(|(delete_table, deleted_row_id, _)| {
                    delete_table == table && *deleted_row_id == row_id
                })
        {
            ws.relational_deletes.push((table.to_string(), row_id, tx));
        }

        Ok(())
    }

    fn delete_vector_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        tx: TxId,
        index: VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<()> {
        self.vector_store.state(&index)?;
        let Some(row) = self.row_by_id_in_prepared_write_set(ws, &index.table, row_id, snapshot)?
        else {
            return Err(Error::NotFound(format!(
                "row {row_id} in table {}",
                index.table
            )));
        };
        self.assert_row_write_allowed(&index.table, row.row_id, &row.values, snapshot)?;
        let existing_live = self
            .vector_store
            .live_entry_for_row(&index, row_id, snapshot)
            .is_some();
        let mut canceled_inserts = Vec::new();
        let mut pos = 0;
        while pos < ws.vector_inserts.len() {
            if ws.vector_inserts[pos].index == index && ws.vector_inserts[pos].row_id == row_id {
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
        let pending_move_from_row = ws
            .vector_moves
            .iter()
            .any(|(move_index, old_row_id, _, _)| *move_index == index && *old_row_id == row_id);
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

        let already_deleted = ws
            .vector_deletes
            .iter()
            .any(|(pending_index, pending_row_id, _)| {
                *pending_index == index && *pending_row_id == row_id
            });
        if !pending_move_from_row
            && ((canceled_inserts.is_empty() && !canceled_move_to_row) || existing_live)
            && !already_deleted
        {
            ws.vector_deletes.push((index.clone(), row_id, tx));
        }
        for entry in canceled_inserts {
            self.accountant
                .release(self.vector_insert_accounted_bytes(&entry.index, entry.vector.len()));
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
        let _operation = self.open_operation()?;
        self.assert_cron_callback_tx_bound_handle(tx)?;
        self.ensure_trigger_table_ready(table, "delete_row")?;
        self.assert_row_id_write_allowed(Some(tx), table, row_id, self.snapshot())?;
        self.relational.delete(tx, table, row_id)
    }

    pub fn scan(&self, table: &str, snapshot: SnapshotId) -> Result<Vec<VersionedRow>> {
        let _operation = self.open_operation()?;
        let rows = self.relational.scan(table, snapshot)?;
        self.filter_rows_for_read(table, rows, snapshot)
    }

    pub(crate) fn scan_in_tx_raw(
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
        let _operation = self.open_operation()?;
        let rows = self.relational.scan(table, snapshot)?;
        let rows = self.filter_rows_for_read(table, rows, snapshot)?;
        Ok(rows.into_iter().filter(|row| predicate(row)).collect())
    }

    pub fn point_lookup(
        &self,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        let _operation = self.open_operation()?;
        self.assert_table_read_allowed(table)?;
        let Some(row) = self.relational.point_lookup(table, col, value, snapshot)? else {
            return Ok(None);
        };
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        if self.read_allowed_for_row(table, &meta, &row, snapshot)? {
            Ok(Some(row))
        } else {
            Ok(None)
        }
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

    pub(crate) fn conflict_lookup_in_tx(
        &self,
        tx: TxId,
        table: &str,
        columns: &[ColName],
        values: &[Value],
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        if columns.is_empty() || columns.len() != values.len() {
            return Err(Error::Other(
                "ON CONFLICT target must include matching columns and values".to_string(),
            ));
        }
        if let ([column], [value]) = (columns, values) {
            return self.point_lookup_in_tx(tx, table, column, value, snapshot);
        }
        let rows = self.scan_in_tx_raw(tx, table, snapshot)?;
        Ok(rows.into_iter().find(|row| {
            columns
                .iter()
                .zip(values.iter())
                .all(|(column, value)| row.values.get(column) == Some(value))
        }))
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
        let _operation = self.open_operation()?;
        self.assert_cron_callback_tx_bound_handle(tx)?;
        let snapshot = self.snapshot();
        self.assert_node_write_allowed(source, snapshot)?;
        self.assert_node_write_allowed(target, snapshot)?;
        self.assert_graph_edge_write_allowed(Some(tx), source, target, &edge_type, snapshot)?;
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
        let _operation = self.open_operation()?;
        self.assert_cron_callback_tx_bound_handle(tx)?;
        let snapshot = self.snapshot();
        self.assert_node_write_allowed(source, snapshot)?;
        self.assert_node_write_allowed(target, snapshot)?;
        self.assert_graph_edge_write_allowed(Some(tx), source, target, edge_type, snapshot)?;
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
        let _operation = self.open_operation()?;
        if self.access_is_admin() {
            self.graph
                .bfs(start, edge_types, direction, 1, max_depth, snapshot)
        } else {
            self.query_bfs_gated(start, edge_types, direction, 1, max_depth, snapshot)
        }
    }

    pub fn edge_count(
        &self,
        source: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<usize> {
        let _operation = self.open_operation()?;
        if self.access_is_admin() {
            return Ok(self.graph.edge_count(source, edge_type, snapshot));
        }
        if !self.node_read_allowed(source, snapshot)? {
            return Ok(0);
        }
        let edge_types = [edge_type.to_string()];
        let mut count = 0;
        for (target, edge_type, _, edge_source, edge_target) in self
            .graph_neighbors_with_orientation(
                source,
                Some(&edge_types),
                Direction::Outgoing,
                snapshot,
            )?
        {
            if self.node_read_allowed(target, snapshot)?
                && self.edge_read_allowed(edge_source, edge_target, &edge_type, snapshot)?
            {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn get_edge_properties(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<HashMap<String, Value>>> {
        let _operation = self.open_operation()?;
        if !self.access_is_admin()
            && (!self.node_read_allowed(source, snapshot)?
                || !self.node_read_allowed(target, snapshot)?
                || !self.edge_read_allowed(source, target, edge_type, snapshot)?)
        {
            return Ok(None);
        }
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
        let _operation = self.open_operation()?;
        self.ensure_trigger_table_ready(&index.table, "insert_vector")?;
        self.assert_cron_callback_tx_bound_handle(tx)?;
        self.vector_store.state(&index)?;
        self.assert_existing_row_id_write_allowed(Some(tx), &index.table, row_id, self.snapshot())?;
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
        self.ensure_trigger_table_ready(&index.table, "insert_vector")?;
        self.vector_store.validate_vector(&index, vector.len())?;
        self.assert_row_id_write_allowed(Some(tx), &index.table, row_id, self.snapshot())?;
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
        let _operation = self.open_operation()?;
        self.ensure_trigger_table_ready(&index.table, "delete_vector")?;
        self.assert_cron_callback_tx_bound_handle(tx)?;
        self.vector_store.state(&index)?;
        self.assert_row_id_write_allowed(Some(tx), &index.table, row_id, self.snapshot())?;
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
        self.ensure_trigger_table_ready(&index.table, "move_vector")?;
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
        let _operation = self.open_operation()?;
        if self.vector_store.try_state(&index).is_none() {
            return Err(Error::UnknownVectorIndex { index });
        }
        let effective_candidates =
            self.effective_read_candidates(&index.table, snapshot, candidates)?;
        self.vector
            .search(index, query, k, effective_candidates.as_ref(), snapshot)
    }

    pub fn semantic_search(&self, query: SemanticQuery) -> Result<Vec<SearchResult>> {
        let _operation = self.open_operation()?;
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
        let _operation = self.assert_open_operation();
        self.rank_policy_eval_count.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn __reset_rank_policy_eval_count(&self) {
        let _operation = self.assert_open_operation();
        self.rank_policy_eval_count.store(0, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn __rank_policy_formula_parse_count(&self) -> u64 {
        let _operation = self.assert_open_operation();
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
        let _operation = self.open_operation()?;
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
        let effective_candidates =
            self.effective_read_candidates(&index.table, snapshot, candidates)?;
        self.vector
            .search(index, query, k, effective_candidates.as_ref(), snapshot)
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
        if !self.row_read_allowed_for_change(&policy.joined_table, &row, snapshot) {
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
        let _operation = self.assert_open_operation();
        self.vector_store
            .try_state(&index)
            .and_then(|state| state.hnsw_len())
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_stats(&self, index: VectorIndexRef) -> Option<HnswGraphStats> {
        let _operation = self.assert_open_operation();
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
        let _operation = self.assert_open_operation();
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
        let _operation = self.assert_open_operation();
        self.vector_store
            .raw_hnsw_entry_count_for_row(&index, row_id)
    }

    #[doc(hidden)]
    pub fn __debug_vector_storage_bytes_per_entry(
        &self,
        index: VectorIndexRef,
    ) -> Result<Vec<usize>> {
        let _operation = self.open_operation()?;
        self.vector_store.storage_bytes_per_entry(&index)
    }

    pub fn has_live_vector(&self, row_id: RowId, snapshot: SnapshotId) -> bool {
        let _operation = self.assert_open_operation();
        self.vector_store
            .live_entries_for_row(row_id, snapshot)
            .into_iter()
            .any(|entry| self.row_id_read_allowed_for_change(&entry.index.table, row_id, snapshot))
    }

    pub fn live_vector_entry(&self, row_id: RowId, snapshot: SnapshotId) -> Option<VectorEntry> {
        let _operation = self.assert_open_operation();
        self.vector_store
            .live_entries_for_row(row_id, snapshot)
            .into_iter()
            .find(|entry| self.row_id_read_allowed_for_change(&entry.index.table, row_id, snapshot))
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
        let edges = self.graph_edges_after_table_drop(table);
        let mut forward_next: HashMap<NodeId, Vec<AdjEntry>> = HashMap::new();
        let mut reverse_next: HashMap<NodeId, Vec<AdjEntry>> = HashMap::new();
        for edge in edges {
            forward_next
                .entry(edge.source)
                .or_default()
                .push(edge.clone());
            reverse_next.entry(edge.target).or_default().push(edge);
        }
        *self.graph_store.forward_adj.write() = forward_next;
        *self.graph_store.reverse_adj.write() = reverse_next;
    }

    pub(crate) fn graph_edges_after_table_drop(&self, table: &str) -> Vec<AdjEntry> {
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

        self.graph_store
            .forward_adj
            .read()
            .values()
            .flat_map(|entries| entries.iter().cloned())
            .filter(|entry| {
                !edge_keys.contains(&(entry.source, entry.edge_type.clone(), entry.target))
            })
            .collect()
    }

    pub(crate) fn vector_entries_after_table_drop(&self, table: &str) -> Vec<VectorEntry> {
        self.vector_store
            .all_entries()
            .into_iter()
            .filter(|entry| entry.index.table != table)
            .collect()
    }

    pub fn table_names(&self) -> Vec<String> {
        let _operation = self.assert_open_operation();
        self.relational_store.table_names()
    }

    pub fn table_meta(&self, table: &str) -> Option<TableMeta> {
        let _operation = self.assert_open_operation();
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
        let _operation = self.open_operation()?;
        let changes = self.changes_since(since);
        if !self.access_is_admin() {
            return Ok(changes.rows);
        }
        let entries = self.change_log_since(since);
        let tables = self.relational_store.tables.read();
        let mut out = Vec::new();
        for e in entries {
            match e {
                ChangeLogEntry::RowInsert { table, row_id, lsn } => {
                    let Some(rows) = tables.get(&table) else {
                        continue;
                    };
                    let Some(row) = rows
                        .iter()
                        .rev()
                        .find(|r| r.row_id == row_id && r.lsn == lsn)
                        .or_else(|| rows.iter().rev().find(|r| r.row_id == row_id))
                    else {
                        continue;
                    };
                    let Some((natural_key, _)) = self.row_change_values_from_row(&table, row)
                    else {
                        continue;
                    };
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
        let _operation = self.assert_open_operation();
        self.rows_examined.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn __reset_rows_examined(&self) {
        let _operation = self.assert_open_operation();
        self.rows_examined.store(0, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn __bump_rows_examined(&self, delta: u64) {
        let _operation = self.assert_open_operation();
        self.rows_examined.fetch_add(delta, Ordering::SeqCst);
    }

    /// Count of batch-level `indexes.write()` lock acquisitions since startup.
    /// `apply_changes` bumps this once per batch; per-row commits do not.
    #[doc(hidden)]
    pub fn __index_write_lock_count(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.index_write_lock_count()
    }

    /// Total entries across every registered index's BTreeMap.
    #[doc(hidden)]
    pub fn __introspect_indexes_total_entries(&self) -> u64 {
        let _operation = self.assert_open_operation();
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
        let _operation = self.open_operation()?;
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
        let _operation = self.assert_open_operation();
        let _guard = self.pruning_guard.lock();
        match self.run_pruning_cycle_checked_inner() {
            Ok(report) => report.pruned_rows,
            Err(err) => {
                log_pruning_error(&err);
                0
            }
        }
    }

    pub fn run_pruning_cycle_checked(&self) -> Result<PruningReport> {
        let _operation = self.assert_open_operation();
        let _guard = self.pruning_guard.lock();
        self.run_pruning_cycle_checked_inner()
    }

    fn run_pruning_cycle_checked_inner(&self) -> Result<PruningReport> {
        self.with_commit_lock(|| {
            checked_prune_expired_rows(
                &self.relational_store,
                &self.graph_store,
                &self.vector_store,
                self.accountant(),
                self.persistence.as_ref(),
                self.sync_watermark(),
            )
        })
    }

    pub fn __fk_probe_stats(&self) -> FkProbeStats {
        FkProbeStats {
            indexed_tuple_probes: self.fk_indexed_tuple_probes.load(Ordering::SeqCst),
            full_scan_fallbacks: self.fk_full_scan_fallbacks.load(Ordering::SeqCst),
        }
    }

    pub fn __reset_fk_probe_stats(&self) {
        let _operation = self.assert_open_operation();
        self.fk_indexed_tuple_probes.store(0, Ordering::SeqCst);
        self.fk_full_scan_fallbacks.store(0, Ordering::SeqCst);
    }

    /// Set the pruning loop interval. Test-only API.
    pub fn set_pruning_interval(&self, interval: Duration) {
        let _operation = self.assert_open_operation();
        self.stop_pruning_thread();

        let shutdown = Arc::new(AtomicBool::new(false));
        let relational = self.relational_store.clone();
        let graph = self.graph_store.clone();
        let vector = self.vector_store.clone();
        let accountant = self.accountant.clone();
        let persistence = self.persistence.clone();
        let sync_watermark = self.sync_watermark.clone();
        let tx_mgr = self.tx_mgr.clone();
        let pruning_guard = self.pruning_guard.clone();
        let thread_shutdown = shutdown.clone();

        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::SeqCst) {
                {
                    let _guard = pruning_guard.lock();
                    if let Err(err) = tx_mgr.with_commit_lock(|| {
                        checked_prune_expired_rows(
                            &relational,
                            &graph,
                            &vector,
                            accountant.as_ref(),
                            persistence.as_ref(),
                            sync_watermark.load(Ordering::SeqCst),
                        )
                    }) {
                        log_pruning_error(&err);
                    }
                }
                sleep_with_shutdown(&thread_shutdown, interval);
            }
        });

        let mut runtime = self.pruning_runtime.lock();
        runtime.shutdown = shutdown;
        runtime.handle = Some(handle);
    }

    pub fn sync_watermark(&self) -> Lsn {
        let _operation = self.assert_open_operation();
        self.sync_watermark.load(Ordering::SeqCst)
    }

    pub fn set_sync_watermark(&self, watermark: Lsn) {
        let _operation = self.assert_open_operation();
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
        let db_id = self as *const Self as usize;
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::Other(
                "cannot close database from inside a cron callback".to_string(),
            ));
        }
        if DB_OPERATION_STACK.with(|stack| stack.borrow().contains(&db_id)) {
            return Err(Error::Other(
                "cannot close database from inside an active operation".to_string(),
            ));
        }
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::Other(
                "cannot close database from another thread while a cron callback is active"
                    .to_string(),
            ));
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return Err(Error::Other(
                "cannot close database from another thread while a trigger callback is active"
                    .to_string(),
            ));
        }
        let _operation_barrier = self.operation_gate.write();
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let tx = self.session_tx.lock().take();
        if let Some(tx) = tx {
            if let Ok(ws) = self.tx_mgr.cloned_write_set(tx) {
                self.release_insert_allocations(&ws);
            }
            self.pending_event_bus_ddl.lock().remove(&tx);
            self.pending_commit_metadata.lock().remove(&tx);
            let _ = self.tx_mgr.rollback(tx);
        }
        self.stop_cron_tickler();
        self.stop_event_bus_threads();
        self.stop_pruning_thread();
        if self.resource_owner {
            self.subscriptions.lock().subscribers.clear();
            if let Some(persistence) = &self.persistence {
                persistence.close();
            }
            self.release_open_registry();
        }
        self.plugin.on_close()
    }

    fn open_operation(&self) -> Result<DatabaseOperationGuard<'_>> {
        let db_id = self as *const Self as usize;
        let nested = DB_OPERATION_STACK.with(|stack| stack.borrow().contains(&db_id));
        if nested {
            DB_OPERATION_STACK.with(|stack| stack.borrow_mut().push(db_id));
            return Ok(DatabaseOperationGuard { db_id, _lock: None });
        }

        let lock = self.operation_gate.read();
        if self.closed.load(Ordering::SeqCst) {
            return Err(closed_database_error());
        }
        DB_OPERATION_STACK.with(|stack| stack.borrow_mut().push(db_id));
        Ok(DatabaseOperationGuard {
            db_id,
            _lock: Some(lock),
        })
    }

    fn assert_open_operation(&self) -> DatabaseOperationGuard<'_> {
        self.open_operation().expect("database handle is closed")
    }

    fn release_open_registry(&self) {
        if let Some(path) = self.open_registry_path.lock().take() {
            release_open_registry_path(&path);
        }
    }

    /// File-backed database with custom plugin.
    pub fn open_with_plugin(
        path: impl AsRef<Path>,
        plugin: Arc<dyn DatabasePlugin>,
    ) -> Result<Self> {
        let db = Self::open_loaded(path, plugin, Arc::new(MemoryAccountant::no_limit()), None)?;
        db.plugin.on_open()?;
        db.start_cron_tickler_if_schedules_present();
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
        let db = Self::open_loaded(path, plugin, accountant, startup_disk_limit)?;
        db.plugin.on_open()?;
        db.start_cron_tickler_if_schedules_present();
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

    pub(crate) fn write_set_counts(&self, tx: TxId) -> Result<WriteSetCounts> {
        self.tx_mgr
            .with_write_set(tx, |ws| current_write_set_counts(ws))
    }

    pub(crate) fn record_conditional_update_guard(
        &self,
        tx: TxId,
        table: TableName,
        row_id: RowId,
        predicates: Vec<(ColName, Value)>,
        before: WriteSetCounts,
        after: WriteSetCounts,
    ) -> Result<()> {
        self.tx_mgr.with_write_set(tx, |_| ())?;
        self.pending_commit_metadata
            .lock()
            .entry(tx)
            .or_default()
            .conditional_update_guards
            .push(PendingConditionalUpdateGuard {
                table,
                row_id,
                predicates,
                before,
                after,
            });
        Ok(())
    }

    pub(crate) fn record_upsert_intent(
        &self,
        tx: TxId,
        table: TableName,
        row_id: RowId,
        details: UpsertIntentDetails,
    ) -> Result<()> {
        self.tx_mgr.with_write_set(tx, |_| ())?;
        self.pending_commit_metadata
            .lock()
            .entry(tx)
            .or_default()
            .upsert_intents
            .push(PendingUpsertIntent {
                table,
                row_id,
                active_tx: tx,
                insert_values: details.insert_values,
                conflict_columns: details.conflict_columns,
                update_columns: details.update_columns,
                params: details.params,
            });
        Ok(())
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
        let _operation = self.assert_open_operation();
        self.conflict_policies.read().clone()
    }

    /// Set the default conflict policy.
    pub fn set_default_conflict_policy(&self, policy: ConflictPolicy) {
        let _operation = self.assert_open_operation();
        self.conflict_policies.write().default = policy;
    }

    /// Set a per-table conflict policy.
    pub fn set_table_conflict_policy(&self, table: &str, policy: ConflictPolicy) {
        let _operation = self.assert_open_operation();
        self.conflict_policies
            .write()
            .per_table
            .insert(table.to_string(), policy);
    }

    /// Remove a per-table conflict policy override.
    pub fn drop_table_conflict_policy(&self, table: &str) {
        let _operation = self.assert_open_operation();
        self.conflict_policies.write().per_table.remove(table);
    }

    pub fn plugin(&self) -> &dyn DatabasePlugin {
        assert!(
            !self.closed.load(Ordering::SeqCst),
            "database handle is closed"
        );
        self.plugin.as_ref()
    }

    pub fn plugin_health(&self) -> PluginHealth {
        let _operation = self.assert_open_operation();
        self.plugin.health()
    }

    pub fn plugin_describe(&self) -> serde_json::Value {
        let _operation = self.assert_open_operation();
        self.plugin.describe()
    }

    pub(crate) fn graph(&self) -> &MemGraphExecutor<DynStore> {
        &self.graph
    }

    pub(crate) fn relational_store(&self) -> &Arc<RelationalStore> {
        &self.relational_store
    }

    pub(crate) fn allocate_ddl_lsn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(Lsn) -> Result<R>,
    {
        self.tx_mgr.allocate_ddl_lsn(f)
    }

    pub(crate) fn allocate_ddl_lsn_maybe<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(Lsn) -> Result<Option<R>>,
    {
        self.tx_mgr.allocate_ddl_lsn_maybe(f)
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
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        self.ddl_log.write().push((lsn, change));
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
            foreign_keys: single_column_foreign_keys_from_meta(meta, &HashSet::new()),
            composite_foreign_keys: meta.composite_foreign_keys.clone(),
            composite_unique: meta.unique_constraints.clone(),
        };
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        self.ddl_log.write().push((lsn, change));
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
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        self.ddl_log.write().push((lsn, change));
        Ok(())
    }

    pub(crate) fn log_drop_index_ddl(&self, table: &str, name: &str, lsn: Lsn) -> Result<()> {
        let change = DdlChange::DropIndex {
            table: table.to_string(),
            name: name.to_string(),
        };
        if let Some(persistence) = &self.persistence {
            persistence.append_ddl_log(lsn, &change)?;
        }
        self.ddl_log.write().push((lsn, change));
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
        let _operation = self.open_operation()?;
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
        let _operation = self.assert_open_operation();
        match self.disk_limit.load(Ordering::SeqCst) {
            0 => None,
            bytes => Some(bytes),
        }
    }

    pub fn disk_limit_startup_ceiling(&self) -> Option<u64> {
        let _operation = self.assert_open_operation();
        match self.disk_limit_startup_ceiling.load(Ordering::SeqCst) {
            0 => None,
            bytes => Some(bytes),
        }
    }

    pub fn disk_file_size(&self) -> Option<u64> {
        let _operation = self.assert_open_operation();
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
        let _operation = self.open_operation()?;
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
        let _operation = self.open_operation()?;
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
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            persistence
                .flush_config_value(&format!("sync_push_watermark:{tenant_id}"), &watermark.0)?;
        }
        Ok(())
    }

    pub fn persist_sync_pull_watermark(&self, tenant_id: &str, watermark: Lsn) -> Result<()> {
        let _operation = self.open_operation()?;
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

    pub fn change_log_since(&self, since_lsn: Lsn) -> Vec<ChangeLogEntry> {
        let _operation = self.assert_open_operation();
        if !self.access_is_admin() {
            return self.gated_change_log_since(since_lsn);
        }
        self.with_commit_lock(|| self.change_log_since_unlocked(since_lsn))
    }

    fn gated_change_log_since(&self, since_lsn: Lsn) -> Vec<ChangeLogEntry> {
        let changes = self.changes_since(since_lsn);
        let mut entries = Vec::new();
        for row in changes.rows {
            if row.deleted {
                entries.push(ChangeLogEntry::RowDelete {
                    table: row.table,
                    row_id: RowId(0),
                    natural_key: row.natural_key,
                    lsn: row.lsn,
                });
            } else if let Some(row_id) = self.row_id_for_natural_key(
                &row.table,
                &row.natural_key.column,
                &row.natural_key.value,
                self.snapshot_at(row.lsn),
            ) {
                entries.push(ChangeLogEntry::RowInsert {
                    table: row.table,
                    row_id,
                    lsn: row.lsn,
                });
            }
        }
        for edge in changes.edges {
            if matches!(edge.properties.get("__deleted"), Some(Value::Bool(true))) {
                entries.push(ChangeLogEntry::EdgeDelete {
                    source: edge.source,
                    target: edge.target,
                    edge_type: edge.edge_type,
                    lsn: edge.lsn,
                });
            } else {
                entries.push(ChangeLogEntry::EdgeInsert {
                    source: edge.source,
                    target: edge.target,
                    edge_type: edge.edge_type,
                    lsn: edge.lsn,
                });
            }
        }
        for vector in changes.vectors {
            if vector.vector.is_empty() {
                entries.push(ChangeLogEntry::VectorDelete {
                    index: vector.index,
                    row_id: vector.row_id,
                    lsn: vector.lsn,
                });
            } else {
                entries.push(ChangeLogEntry::VectorInsert {
                    index: vector.index,
                    row_id: vector.row_id,
                    lsn: vector.lsn,
                });
            }
        }
        entries.sort_by_key(ChangeLogEntry::lsn);
        entries
    }

    fn change_log_since_unlocked(&self, since_lsn: Lsn) -> Vec<ChangeLogEntry> {
        let log = self.change_log.read();
        let start = log.partition_point(|e| e.lsn() <= since_lsn);
        log[start..].to_vec()
    }

    pub fn ddl_log_since(&self, since_lsn: Lsn) -> Vec<DdlChange> {
        let _operation = self.assert_open_operation();
        if !self.access_is_admin() {
            return Vec::new();
        }
        self.with_commit_lock(|| self.ddl_log_since_unlocked(since_lsn))
    }

    fn ddl_log_since_unlocked(&self, since_lsn: Lsn) -> Vec<DdlChange> {
        self.ddl_log_entries_since_unlocked(since_lsn)
            .into_iter()
            .map(|(_, change)| change)
            .collect()
    }

    fn ddl_log_entries_since_unlocked(&self, since_lsn: Lsn) -> Vec<(Lsn, DdlChange)> {
        let ddl = self.ddl_log.read();
        let mut entries = ddl
            .iter()
            .filter(|(lsn, _)| *lsn > since_lsn)
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by_key(|(lsn, _)| *lsn);
        entries
            .into_iter()
            .map(|(lsn, change)| (lsn, self.enrich_table_ddl_from_current_meta(change)))
            .collect()
    }

    fn enrich_table_ddl_from_current_meta(&self, change: DdlChange) -> DdlChange {
        match change {
            DdlChange::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => {
                // Sequence-form bincode table DDL is legacy-shaped after
                // deserialization. Current TableMeta is the durable authority
                // for structured constraints, so use it to serve complete sync
                // payloads after reopen.
                if foreign_keys.is_empty()
                    && composite_foreign_keys.is_empty()
                    && composite_unique.is_empty()
                    && let Some(meta) = self.table_meta(&name)
                {
                    return DdlChange::CreateTable {
                        name,
                        columns,
                        constraints,
                        foreign_keys: single_column_foreign_keys_from_meta(&meta, &HashSet::new()),
                        composite_foreign_keys: meta.composite_foreign_keys,
                        composite_unique: meta.unique_constraints,
                    };
                }
                DdlChange::CreateTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                }
            }
            DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => {
                // See CreateTable arm: enrich legacy-shaped decoded DDL from
                // the current table metadata before exposing it to sync.
                if foreign_keys.is_empty()
                    && composite_foreign_keys.is_empty()
                    && composite_unique.is_empty()
                    && let Some(meta) = self.table_meta(&name)
                {
                    return DdlChange::AlterTable {
                        name,
                        columns,
                        constraints,
                        foreign_keys: single_column_foreign_keys_from_meta(&meta, &HashSet::new()),
                        composite_foreign_keys: meta.composite_foreign_keys,
                        composite_unique: meta.unique_constraints,
                    };
                }
                DdlChange::AlterTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                }
            }
            other => other,
        }
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
        if self.access_is_admin() {
            ddl.extend(full_snapshot_ddl(&meta_guard));
            let live_tables = meta_guard.keys().cloned().collect::<HashSet<_>>();
            ddl.extend(self.trigger_snapshot_ddl_for_tables(&live_tables));
            ddl.extend(self.event_bus_snapshot_ddl_for_tables(&live_tables));
        }

        // Rows (live only) — collect row_ids that have live rows for orphan vector filtering
        let mut live_row_ids: HashSet<RowId> = HashSet::new();
        for (table_name, table_rows) in tables_guard.iter() {
            let meta = match meta_guard.get(table_name) {
                Some(m) => m,
                None => continue,
            };
            let key_col = natural_key_column_for_meta(meta).unwrap_or_default();
            if key_col.is_empty() {
                continue;
            }
            for row in table_rows.iter().filter(|r| r.deleted_tx.is_none()) {
                if !self.row_read_allowed_for_change(table_name, row, self.snapshot()) {
                    continue;
                }
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
                if !self.graph_edge_read_allowed_for_change(
                    entry.source,
                    entry.target,
                    &entry.edge_type,
                    self.snapshot(),
                ) {
                    continue;
                }
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

        let first_data_lsn = rows
            .iter()
            .map(|row| row.lsn)
            .chain(edges.iter().map(|edge| edge.lsn))
            .chain(vectors.iter().map(|vector| vector.lsn))
            .min()
            .unwrap_or_else(|| self.current_lsn());
        let snapshot_schema_lsn = if rows.is_empty() && edges.is_empty() && vectors.is_empty() {
            self.current_lsn()
        } else {
            Lsn(first_data_lsn.0.saturating_sub(1))
        };
        let ddl_lsn = vec![snapshot_schema_lsn; ddl.len()];

        ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
            ddl_lsn,
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
            let key_col = natural_key_column_for_meta(meta).unwrap_or_default();
            if key_col.is_empty() {
                continue;
            }
            for row in table_rows.iter().filter(|row| row.deleted_tx.is_none()) {
                if !self.row_read_allowed_for_change(table_name, row, self.snapshot()) {
                    continue;
                }
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
                if !self.graph_edge_read_allowed_for_change(
                    entry.source,
                    entry.target,
                    &entry.edge_type,
                    SnapshotId(entry.lsn.0),
                ) {
                    continue;
                }
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
            ddl_lsn: Vec::new(),
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
        let projected_meta = self.projected_sync_table_meta(&changes.ddl);
        required = required.saturating_add(self.projected_sync_ddl_metadata_bytes(&changes.ddl));

        for row in &changes.rows {
            if row.deleted || row.values.is_empty() {
                continue;
            }
            let table_meta = projected_meta
                .get(&row.table)
                .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;

            let policy = policies
                .per_table
                .get(&row.table)
                .copied()
                .unwrap_or(policies.default);
            let existing = if self.table_meta(&row.table).is_some() {
                self.point_lookup(
                    &row.table,
                    &row.natural_key.column,
                    &row.natural_key.value,
                    self.snapshot(),
                )?
            } else {
                None
            };

            if existing.is_some()
                && matches!(
                    policy,
                    ConflictPolicy::InsertIfNotExists | ConflictPolicy::ServerWins
                )
            {
                continue;
            }

            required = required.saturating_add(estimate_row_bytes_for_meta(
                &row.values,
                table_meta,
                false,
            ));
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
            let bytes = projected_meta
                .get(&vector.index.table)
                .and_then(|meta| {
                    meta.columns
                        .iter()
                        .find(|column| column.name == vector.index.column)
                        .map(|column| column.quantization.storage_bytes(vector.vector.len()))
                })
                .unwrap_or_else(|| {
                    vector
                        .vector
                        .len()
                        .saturating_mul(std::mem::size_of::<f32>())
                });
            required = required.saturating_add(24 + bytes);
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

    fn projected_sync_table_meta(&self, ddl: &[DdlChange]) -> HashMap<String, TableMeta> {
        let mut projected = self.relational_store.table_meta.read().clone();

        for change in ddl {
            Self::apply_sync_table_ddl_to_projected_meta(&mut projected, change);
        }

        projected
    }

    fn apply_sync_table_ddl_to_projected_meta(
        projected: &mut HashMap<String, TableMeta>,
        change: &DdlChange,
    ) {
        match change {
            DdlChange::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => {
                projected.entry(name.clone()).or_insert_with(|| {
                    rough_sync_table_meta(
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    )
                });
            }
            DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => {
                if let Some(meta) = projected.get_mut(name) {
                    let incoming = rough_sync_table_meta(
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    );
                    let mut existing = meta
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect::<HashSet<_>>();
                    for column in incoming.columns {
                        if existing.insert(column.name.clone()) {
                            meta.columns.push(column);
                        } else if let Some(current) = meta
                            .columns
                            .iter_mut()
                            .find(|current| current.name == column.name)
                        {
                            current.references = column.references;
                        }
                    }
                    if !incoming.unique_constraints.is_empty() {
                        meta.unique_constraints = incoming.unique_constraints;
                    }
                    if !incoming.composite_foreign_keys.is_empty() {
                        meta.composite_foreign_keys = incoming.composite_foreign_keys;
                    }
                    let user_indexes = meta
                        .indexes
                        .iter()
                        .filter(|index| index.kind == IndexKind::UserDeclared)
                        .cloned()
                        .collect::<Vec<_>>();
                    meta.indexes = crate::executor::auto_indexes_for_table_meta(meta);
                    meta.indexes.extend(user_indexes);
                }
            }
            DdlChange::DropTable { name } => {
                projected.remove(name);
            }
            DdlChange::CreateIndex {
                table,
                name,
                columns,
            } => {
                if let Some(meta) = projected.get_mut(table)
                    && !meta.indexes.iter().any(|index| index.name == *name)
                {
                    meta.indexes.push(IndexDecl {
                        name: name.clone(),
                        columns: columns.clone(),
                        kind: IndexKind::UserDeclared,
                    });
                }
            }
            DdlChange::DropIndex { table, name } => {
                if let Some(meta) = projected.get_mut(table) {
                    meta.indexes.retain(|index| index.name != *name);
                }
            }
            DdlChange::CreateEventType { .. }
            | DdlChange::CreateTrigger { .. }
            | DdlChange::DropTrigger { .. }
            | DdlChange::CreateSink { .. }
            | DdlChange::CreateRoute { .. }
            | DdlChange::DropRoute { .. } => {}
        }
    }

    fn projected_sync_ddl_metadata_bytes(&self, ddl: &[DdlChange]) -> usize {
        let mut projected = self.relational_store.table_meta.read().clone();
        let mut required = 0usize;

        for change in ddl {
            match change {
                DdlChange::CreateTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } => {
                    if !projected.contains_key(name) {
                        let meta = rough_sync_table_meta(
                            columns,
                            constraints,
                            foreign_keys,
                            composite_foreign_keys,
                            composite_unique,
                        );
                        required = required.saturating_add(meta.estimated_bytes());
                        projected.insert(name.clone(), meta);
                    }
                }
                DdlChange::AlterTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } => {
                    if let Some(meta) = projected.get_mut(name) {
                        let before = meta.estimated_bytes();
                        let incoming = rough_sync_table_meta(
                            columns,
                            constraints,
                            foreign_keys,
                            composite_foreign_keys,
                            composite_unique,
                        );
                        let mut existing = meta
                            .columns
                            .iter()
                            .map(|column| column.name.clone())
                            .collect::<HashSet<_>>();
                        for column in incoming.columns {
                            if existing.insert(column.name.clone()) {
                                meta.columns.push(column);
                            } else if let Some(current) = meta
                                .columns
                                .iter_mut()
                                .find(|current| current.name == column.name)
                            {
                                current.references = column.references;
                            }
                        }
                        if !incoming.unique_constraints.is_empty() {
                            meta.unique_constraints = incoming.unique_constraints;
                        }
                        if !incoming.composite_foreign_keys.is_empty() {
                            meta.composite_foreign_keys = incoming.composite_foreign_keys;
                        }
                        let user_indexes = meta
                            .indexes
                            .iter()
                            .filter(|index| index.kind == IndexKind::UserDeclared)
                            .cloned()
                            .collect::<Vec<_>>();
                        meta.indexes = crate::executor::auto_indexes_for_table_meta(meta);
                        meta.indexes.extend(user_indexes);
                        required =
                            required.saturating_add(meta.estimated_bytes().saturating_sub(before));
                    }
                }
                DdlChange::DropTable { name } => {
                    projected.remove(name);
                }
                DdlChange::CreateIndex {
                    table,
                    name,
                    columns,
                } => {
                    if let Some(meta) = projected.get_mut(table)
                        && !meta.indexes.iter().any(|index| index.name == *name)
                    {
                        let before = meta.estimated_bytes();
                        meta.indexes.push(IndexDecl {
                            name: name.clone(),
                            columns: columns.clone(),
                            kind: IndexKind::UserDeclared,
                        });
                        required =
                            required.saturating_add(meta.estimated_bytes().saturating_sub(before));
                    }
                }
                DdlChange::DropIndex { table, name } => {
                    if let Some(meta) = projected.get_mut(table) {
                        meta.indexes.retain(|index| index.name != *name);
                    }
                }
                DdlChange::CreateEventType { .. }
                | DdlChange::CreateTrigger { .. }
                | DdlChange::DropTrigger { .. }
                | DdlChange::CreateSink { .. }
                | DdlChange::CreateRoute { .. }
                | DdlChange::DropRoute { .. } => {}
            }
        }

        required
    }

    fn preflight_sync_ddl_mixed_apply(
        &self,
        changes: &ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<()> {
        if changes.ddl.is_empty() {
            return Ok(());
        }

        let callback_required_triggers = self.preflight_sync_trigger_callback_required(changes)?;
        let mut ddl_prefix = Vec::new();
        let mut event_bus_ddl_prefix = Vec::new();
        let mut incoming_fk_values = HashMap::<String, Vec<HashMap<String, Value>>>::new();
        let mut deleted_committed_fk_row_ids = HashMap::<String, HashSet<RowId>>::new();
        let mut projected_table_meta = self.relational_store.table_meta.read().clone();
        let mut projected_trigger_declarations = self.trigger.declarations.lock().clone();
        let snapshot = self.snapshot();
        for group in changes.clone().split_by_data_lsn() {
            self.preflight_sync_trigger_data_gate(
                &group.rows,
                &group.edges,
                &group.vectors,
                &projected_trigger_declarations,
                &callback_required_triggers,
            )?;
            ddl_prefix.extend(group.ddl.iter().cloned());
            event_bus_ddl_prefix.extend(group.ddl.iter().filter_map(|ddl| match ddl {
                DdlChange::CreateEventType { .. }
                | DdlChange::CreateSink { .. }
                | DdlChange::CreateRoute { .. }
                | DdlChange::DropRoute { .. } => Some(ddl.clone()),
                _ => None,
            }));
            for ddl in &group.ddl {
                Self::validate_sync_table_ddl_against_projected_meta(&projected_table_meta, ddl)?;
                Self::apply_sync_table_ddl_to_projected_meta(&mut projected_table_meta, ddl);
                self.apply_sync_trigger_ddl_to_projection(
                    &mut projected_trigger_declarations,
                    ddl,
                    &projected_table_meta,
                )?;
            }

            self.preflight_sync_rows_against_projected_schema(&group.rows, &projected_table_meta)?;
            self.preflight_sync_vectors_against_projected_schema(
                &group.vectors,
                &projected_table_meta,
            )?;
            self.preflight_sync_foreign_keys_against_lsn_prefix(
                &group.rows,
                &projected_table_meta,
                snapshot,
                &mut incoming_fk_values,
                &mut deleted_committed_fk_row_ids,
                policies,
            )?;

            self.preflight_sync_trigger_data_gate(
                &group.rows,
                &group.edges,
                &group.vectors,
                &projected_trigger_declarations,
                &callback_required_triggers,
            )?;

            let dropped_tables = Self::sync_dropped_tables(&ddl_prefix);
            let projected_event_bus_ddl = event_bus_ddl_prefix
                .iter()
                .filter(|ddl| {
                    !Self::skip_event_bus_ddl_for_dropped_table(ddl, &dropped_tables, |table| {
                        projected_table_meta.contains_key(table)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            self.validate_event_bus_ddl_batch_with_table_lookup(
                &projected_event_bus_ddl,
                |table| projected_table_meta.contains_key(table),
            )?;
        }

        let projected_dag_edge_types = self.projected_sync_dag_edge_types(&changes.ddl);
        self.preflight_sync_edge_cycles(changes, &projected_dag_edge_types)?;

        Ok(())
    }

    fn preflight_sync_trigger_callback_required(
        &self,
        changes: &ChangeSet,
    ) -> Result<HashSet<String>> {
        let projected_trigger_declarations = self.preflight_sync_trigger_projection(changes)?;
        Ok(self.sync_triggers_requiring_ready_gate(&projected_trigger_declarations))
    }

    fn preflight_sync_trigger_projection(
        &self,
        changes: &ChangeSet,
    ) -> Result<BTreeMap<String, TriggerDeclaration>> {
        let mut projected_table_meta = self.relational_store.table_meta.read().clone();
        let mut projected_trigger_declarations = self.trigger.declarations.lock().clone();
        for group in changes.clone().split_by_data_lsn() {
            for ddl in &group.ddl {
                Self::validate_sync_table_ddl_against_projected_meta(&projected_table_meta, ddl)?;
                Self::apply_sync_table_ddl_to_projected_meta(&mut projected_table_meta, ddl);
                self.apply_sync_trigger_ddl_to_projection(
                    &mut projected_trigger_declarations,
                    ddl,
                    &projected_table_meta,
                )?;
            }
        }
        Ok(projected_trigger_declarations)
    }

    fn validate_sync_table_ddl_against_projected_meta(
        projected: &HashMap<String, TableMeta>,
        change: &DdlChange,
    ) -> Result<()> {
        match change {
            DdlChange::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => Self::validate_sync_table_shape_ddl(
                projected,
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            ),
            DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => Self::validate_sync_table_shape_ddl(
                projected,
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            ),
            DdlChange::CreateIndex {
                table,
                name,
                columns,
            } => {
                let Some(meta) = projected.get(table) else {
                    return Err(Error::TableNotFound(table.clone()));
                };
                for prefix in ["__pk_", "__unique_", "__fk_"] {
                    if name.starts_with(prefix) {
                        return Err(Error::ReservedIndexName {
                            table: table.clone(),
                            name: name.clone(),
                            prefix: prefix.to_string(),
                        });
                    }
                }
                for (column, _) in columns {
                    if !meta
                        .columns
                        .iter()
                        .any(|candidate| candidate.name == *column)
                    {
                        return Err(Error::ColumnNotFound {
                            table: table.clone(),
                            column: column.clone(),
                        });
                    }
                }
                for (column, _) in columns {
                    let column_meta = meta
                        .columns
                        .iter()
                        .find(|candidate| candidate.name == *column)
                        .expect("column existence verified above");
                    if matches!(
                        column_meta.column_type,
                        ColumnType::Json | ColumnType::Vector(_)
                    ) {
                        return Err(Error::ColumnNotIndexable {
                            table: table.clone(),
                            column: column.clone(),
                            column_type: column_meta.column_type.clone(),
                        });
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn validate_sync_table_shape_ddl(
        projected: &HashMap<String, TableMeta>,
        name: &str,
        columns: &[(String, String)],
        constraints: &[String],
        foreign_keys: &[SingleColumnForeignKey],
        composite_foreign_keys: &[CompositeForeignKey],
        composite_unique: &[Vec<String>],
    ) -> Result<()> {
        let sql = sync_create_table_sql(
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        );
        let stmt = contextdb_parser::parse(&sql)?;
        let _ = contextdb_planner::plan(&stmt)?;
        let candidate = rough_sync_table_meta(
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        );
        let candidate_lookup = candidate.clone();
        crate::executor::validate_composite_foreign_keys_for_meta(name, &candidate, |parent| {
            if parent == name {
                Some(candidate_lookup.clone())
            } else {
                projected.get(parent).cloned()
            }
        })?;
        Ok(())
    }

    fn preflight_sync_rows_against_projected_schema(
        &self,
        rows: &[RowChange],
        projected_table_meta: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        for row in rows {
            if row.values.is_empty() {
                continue;
            }
            let table_meta = projected_table_meta
                .get(&row.table)
                .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;
            let expected_natural_key = natural_key_column_for_meta(table_meta)
                .ok_or_else(|| Error::NotSyncEligible(row.table.clone()))?;
            if row.natural_key.column != expected_natural_key {
                let column_exists = table_meta
                    .columns
                    .iter()
                    .any(|column| column.name == row.natural_key.column);
                if column_exists {
                    return Err(Error::SyncError(format!(
                        "sync row natural key column mismatch for {}: got {}, expected {}",
                        row.table, row.natural_key.column, expected_natural_key
                    )));
                }
                return Err(Error::ColumnNotFound {
                    table: row.table.clone(),
                    column: row.natural_key.column.clone(),
                });
            }
            if !row.deleted {
                match row.values.get(&expected_natural_key) {
                    Some(value) if value == &row.natural_key.value => {}
                    _ => {
                        return Err(Error::SyncError(format!(
                            "sync row natural key value mismatch for {}.{}",
                            row.table, expected_natural_key
                        )));
                    }
                }
                for (column, value) in &row.values {
                    let is_vector_bypass = matches!(value, Value::Vector(_))
                        && table_meta
                            .columns
                            .iter()
                            .find(|candidate| candidate.name == *column)
                            .map(|candidate| matches!(candidate.column_type, ColumnType::Vector(_)))
                            .unwrap_or(false);
                    if !is_vector_bypass {
                        crate::executor::coerce_into_column_with_meta(
                            &row.table,
                            table_meta,
                            column,
                            value.clone(),
                            None,
                            None,
                        )?;
                    }
                }
                for column in &table_meta.columns {
                    if column.nullable || column.primary_key || column.default.is_some() {
                        continue;
                    }
                    match row.values.get(&column.name) {
                        None | Some(Value::Null) => {
                            return Err(Error::ColumnNotNullable {
                                table: row.table.clone(),
                                column: column.name.clone(),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    fn preflight_sync_vectors_against_projected_schema(
        &self,
        vectors: &[VectorChange],
        projected_table_meta: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        for vector in vectors {
            let Some(table_meta) = projected_table_meta.get(&vector.index.table) else {
                return Err(Error::UnknownVectorIndex {
                    index: vector.index.clone(),
                });
            };
            let Some(column) = table_meta
                .columns
                .iter()
                .find(|column| column.name == vector.index.column)
            else {
                return Err(Error::UnknownVectorIndex {
                    index: vector.index.clone(),
                });
            };
            let ColumnType::Vector(expected) = column.column_type else {
                return Err(Error::UnknownVectorIndex {
                    index: vector.index.clone(),
                });
            };
            if !vector.vector.is_empty() && vector.vector.len() != expected {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: vector.index.clone(),
                    expected,
                    actual: vector.vector.len(),
                });
            }
        }
        Ok(())
    }

    fn preflight_sync_foreign_keys_against_lsn_prefix(
        &self,
        rows: &[RowChange],
        projected_table_meta: &HashMap<String, TableMeta>,
        snapshot: SnapshotId,
        incoming_values: &mut HashMap<String, Vec<HashMap<String, Value>>>,
        deleted_committed_row_ids: &mut HashMap<String, HashSet<RowId>>,
        policies: &ConflictPolicies,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut deleted_projected_rows = Vec::<(String, HashMap<String, Value>)>::new();
        let mut current_deleted_committed_row_ids = Vec::<(String, RowId)>::new();
        let mut applying_rows = Vec::<&RowChange>::new();
        for row in rows.iter().filter(|row| row.deleted) {
            let empty_deleted = HashSet::new();
            let skip_deleted = deleted_committed_row_ids
                .get(&row.table)
                .unwrap_or(&empty_deleted);
            if self.table_meta(&row.table).is_some()
                && let Some(committed) = self.visible_row_by_column(
                    &row.table,
                    &row.natural_key.column,
                    &row.natural_key.value,
                    snapshot,
                    skip_deleted,
                )?
            {
                current_deleted_committed_row_ids.push((row.table.clone(), committed.row_id));
                deleted_projected_rows.push((row.table.clone(), committed.values));
                continue;
            }
            if let Some(incoming) = incoming_values.get(&row.table).and_then(|table_values| {
                table_values.iter().find(|values| {
                    values.get(&row.natural_key.column) == Some(&row.natural_key.value)
                })
            }) {
                deleted_projected_rows.push((row.table.clone(), incoming.clone()));
            }
        }

        let mut post_incoming_values = incoming_values.clone();
        for row in rows.iter().filter(|row| row.deleted) {
            if let Some(table_values) = post_incoming_values.get_mut(&row.table) {
                table_values.retain(|values| {
                    values.get(&row.natural_key.column) != Some(&row.natural_key.value)
                });
            }
        }
        let mut post_deleted_committed_row_ids = deleted_committed_row_ids.clone();
        for (table, row_id) in current_deleted_committed_row_ids {
            post_deleted_committed_row_ids
                .entry(table)
                .or_default()
                .insert(row_id);
        }
        for row in rows {
            if row.deleted || row.values.is_empty() {
                continue;
            }
            let policy = Self::sync_conflict_policy_for_table(policies, &row.table);
            let mut replaces_incoming_prefix = false;
            if let Some(table_values) = post_incoming_values.get_mut(&row.table) {
                let mut retained = Vec::with_capacity(table_values.len());
                for values in table_values.drain(..) {
                    if values.get(&row.natural_key.column) == Some(&row.natural_key.value) {
                        replaces_incoming_prefix = true;
                        deleted_projected_rows.push((row.table.clone(), values));
                    } else {
                        retained.push(values);
                    }
                }
                *table_values = retained;
            }
            let empty_deleted = HashSet::new();
            let skip_deleted = post_deleted_committed_row_ids
                .get(&row.table)
                .unwrap_or(&empty_deleted);
            if self.table_meta(&row.table).is_some()
                && let Some(committed) = self.visible_row_by_column(
                    &row.table,
                    &row.natural_key.column,
                    &row.natural_key.value,
                    snapshot,
                    skip_deleted,
                )?
            {
                if !replaces_incoming_prefix
                    && !Self::sync_row_applies_over_committed(row, &committed, policy)
                {
                    continue;
                }
                post_deleted_committed_row_ids
                    .entry(row.table.clone())
                    .or_default()
                    .insert(committed.row_id);
                deleted_projected_rows.push((row.table.clone(), committed.values));
            }
            post_incoming_values
                .entry(row.table.clone())
                .or_default()
                .push(row.values.clone());
            applying_rows.push(row);
        }

        for row in applying_rows {
            let table_meta = projected_table_meta
                .get(&row.table)
                .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;
            for column in &table_meta.columns {
                let Some(reference) = &column.references else {
                    continue;
                };
                let Some(value) = row.values.get(&column.name) else {
                    continue;
                };
                if *value == Value::Null {
                    continue;
                }
                if !projected_table_meta.contains_key(&reference.table) {
                    return Err(Error::ForeignKeyViolation {
                        child_table: row.table.clone(),
                        child_columns: vec![column.name.clone()],
                        parent_table: reference.table.clone(),
                        parent_columns: vec![reference.column.clone()],
                    });
                }
                let incoming_match =
                    post_incoming_values
                        .get(&reference.table)
                        .is_some_and(|rows| {
                            rows.iter()
                                .any(|values| values.get(&reference.column) == Some(value))
                        });
                if incoming_match {
                    continue;
                }
                let empty_deleted = HashSet::new();
                let deleted_parent_row_ids = post_deleted_committed_row_ids
                    .get(&reference.table)
                    .unwrap_or(&empty_deleted);
                let committed_match = if self.table_meta(&reference.table).is_some() {
                    self.visible_row_exists_by_column(
                        &reference.table,
                        &reference.column,
                        value,
                        snapshot,
                        deleted_parent_row_ids,
                    )?
                } else {
                    false
                };
                if !committed_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: row.table.clone(),
                        child_columns: vec![column.name.clone()],
                        parent_table: reference.table.clone(),
                        parent_columns: vec![reference.column.clone()],
                    });
                }
            }
        }
        let reverse_refs = projected_table_meta
            .iter()
            .flat_map(|(child_table, meta)| {
                meta.columns.iter().filter_map(|column| {
                    column.references.as_ref().map(|reference| {
                        (
                            reference.table.clone(),
                            reference.column.clone(),
                            child_table.clone(),
                            column.name.clone(),
                        )
                    })
                })
            })
            .collect::<Vec<_>>();

        for (parent_table, parent_values) in &deleted_projected_rows {
            for (ref_table, ref_column, child_table, child_column) in &reverse_refs {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_value) = parent_values.get(ref_column) else {
                    continue;
                };
                let parent_replaced_with_same_key = post_incoming_values
                    .get(parent_table)
                    .is_some_and(|table_values| {
                        table_values
                            .iter()
                            .any(|values| values.get(ref_column) == Some(parent_value))
                    });
                if parent_replaced_with_same_key {
                    continue;
                }
                let incoming_child_match =
                    post_incoming_values
                        .get(child_table)
                        .is_some_and(|table_values| {
                            table_values
                                .iter()
                                .any(|values| values.get(child_column) == Some(parent_value))
                        });
                if incoming_child_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: vec![child_column.clone()],
                        parent_table: parent_table.clone(),
                        parent_columns: vec![ref_column.clone()],
                    });
                }
                let empty_deleted = HashSet::new();
                let deleted_child_row_ids = post_deleted_committed_row_ids
                    .get(child_table)
                    .unwrap_or(&empty_deleted);
                if self.table_meta(child_table).is_some()
                    && self.visible_row_exists_by_column(
                        child_table,
                        child_column,
                        parent_value,
                        snapshot,
                        deleted_child_row_ids,
                    )?
                {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: vec![child_column.clone()],
                        parent_table: parent_table.clone(),
                        parent_columns: vec![ref_column.clone()],
                    });
                }
            }
        }

        *incoming_values = post_incoming_values;
        *deleted_committed_row_ids = post_deleted_committed_row_ids;
        Ok(())
    }

    fn sync_composite_fk_violation_for_values(
        &self,
        tx: TxId,
        child_table: &str,
        child_meta: &TableMeta,
        values: &HashMap<String, Value>,
        incoming_batch_values: &HashMap<String, Vec<HashMap<String, Value>>>,
        projected_deleted_committed_row_ids: &HashMap<String, HashSet<RowId>>,
    ) -> Result<Option<Error>> {
        for fk in &child_meta.composite_foreign_keys {
            let mut tuple = Vec::with_capacity(fk.child_columns.len());
            let mut has_null = false;
            for column in &fk.child_columns {
                match values.get(column) {
                    Some(Value::Null) | None => {
                        has_null = true;
                        break;
                    }
                    Some(value) => tuple.push(value.clone()),
                }
            }
            if has_null {
                continue;
            }

            let incoming_match =
                incoming_batch_values
                    .get(&fk.parent_table)
                    .is_some_and(|table_values| {
                        table_values.iter().any(|row_values| {
                            fk.parent_columns
                                .iter()
                                .zip(tuple.iter())
                                .all(|(column, value)| row_values.get(column) == Some(value))
                        })
                    });
            if incoming_match {
                continue;
            }

            let staged_match = self.tx_mgr.with_write_set(tx, |ws| {
                staged_tuple_exists(ws, &fk.parent_table, &fk.parent_columns, &tuple)
            })?;
            if staged_match {
                continue;
            }

            let mut parent_deletes = projected_deleted_committed_row_ids
                .get(&fk.parent_table)
                .cloned()
                .unwrap_or_default();
            let staged_parent_deletes = self.tx_mgr.with_write_set(tx, |ws| {
                Self::deleted_row_ids_for_table(ws, &fk.parent_table)
            })?;
            parent_deletes.extend(staged_parent_deletes);
            let committed_match = self.indexed_visible_row_exists_by_columns(
                &fk.parent_table,
                &fk.parent_columns,
                &tuple,
                self.snapshot(),
                &parent_deletes,
            )?;
            if !committed_match {
                return Ok(Some(Error::ForeignKeyViolation {
                    child_table: child_table.to_string(),
                    child_columns: fk.child_columns.clone(),
                    parent_table: fk.parent_table.clone(),
                    parent_columns: fk.parent_columns.clone(),
                }));
            }
        }

        Ok(None)
    }

    fn sync_conflict_policy_for_table(policies: &ConflictPolicies, table: &str) -> ConflictPolicy {
        policies
            .per_table
            .get(table)
            .copied()
            .unwrap_or(policies.default)
    }

    fn sync_row_applies_over_committed(
        row: &RowChange,
        committed: &VersionedRow,
        policy: ConflictPolicy,
    ) -> bool {
        match policy {
            ConflictPolicy::InsertIfNotExists | ConflictPolicy::ServerWins => false,
            ConflictPolicy::EdgeWins => true,
            ConflictPolicy::LatestWins => Self::sync_latest_wins_incoming(row, committed),
        }
    }

    fn sync_latest_wins_incoming(row: &RowChange, committed: &VersionedRow) -> bool {
        if row.lsn != committed.lsn {
            return row.lsn > committed.lsn;
        }
        for (col, incoming_val) in &row.values {
            if let (Value::TxId(incoming_tx), Some(Value::TxId(local_tx))) =
                (incoming_val, committed.values.get(col))
            {
                if incoming_tx.0 > local_tx.0 {
                    return true;
                }
                if incoming_tx.0 < local_tx.0 {
                    return false;
                }
            }
        }
        false
    }

    fn projected_sync_incoming_values_for_applied_rows(
        &self,
        rows: &[RowChange],
        policies: &ConflictPolicies,
    ) -> Result<ProjectedSyncApply> {
        let mut projected_rows = Vec::<ProjectedSyncRow>::new();
        let mut visible_rows_cache = HashMap::new();
        let mut deleted_committed_row_ids = HashMap::<String, HashSet<RowId>>::new();
        let mut synthetic_row_ids = HashSet::<RowId>::new();
        let mut next_synthetic_row_id = u64::MAX;

        for row in rows {
            if row.values.is_empty() {
                continue;
            }

            let existing = cached_point_lookup(
                self,
                &mut visible_rows_cache,
                &row.table,
                &row.natural_key.column,
                &row.natural_key.value,
            )?;

            if row.deleted {
                if let Some(local) = existing {
                    remove_cached_row(&mut visible_rows_cache, &row.table, local.row_id);
                    if !synthetic_row_ids.remove(&local.row_id) {
                        deleted_committed_row_ids
                            .entry(row.table.clone())
                            .or_default()
                            .insert(local.row_id);
                    }
                    projected_rows.retain(|projected| {
                        projected.table != row.table || projected.natural_key != row.natural_key
                    });
                }
                continue;
            }

            let mut values = row.values.clone();
            values.remove("__deleted");
            let policy = Self::sync_conflict_policy_for_table(policies, &row.table);
            let applies = match existing.as_ref() {
                None => self
                    .sync_insert_constraint_error_for_values(
                        &row.table,
                        &values,
                        &mut visible_rows_cache,
                    )?
                    .is_none(),
                Some(local) => {
                    Self::sync_row_applies_over_committed(row, local, policy)
                        && self
                            .sync_upsert_constraint_error_for_values(
                                &row.table,
                                &values,
                                local,
                                &mut visible_rows_cache,
                            )?
                            .is_none()
                }
            };
            if !applies {
                continue;
            }

            if let Some(local) = existing.as_ref()
                && !synthetic_row_ids.contains(&local.row_id)
            {
                deleted_committed_row_ids
                    .entry(row.table.clone())
                    .or_default()
                    .insert(local.row_id);
            }
            projected_rows.retain(|projected| {
                projected.table != row.table || projected.natural_key != row.natural_key
            });
            let row_id = existing.as_ref().map(|row| row.row_id).unwrap_or_else(|| {
                let row_id = RowId(next_synthetic_row_id);
                synthetic_row_ids.insert(row_id);
                next_synthetic_row_id = next_synthetic_row_id.saturating_sub(1);
                row_id
            });
            upsert_cached_projection(
                &mut visible_rows_cache,
                &row.table,
                row_id,
                values.clone(),
                row.lsn,
            );
            projected_rows.push(ProjectedSyncRow {
                table: row.table.clone(),
                natural_key: row.natural_key.clone(),
                values,
            });
        }

        self.retain_projected_rows_with_valid_composite_fks(
            &mut projected_rows,
            &deleted_committed_row_ids,
        )?;
        Ok(ProjectedSyncApply {
            incoming_values: projected_sync_rows_by_table(&projected_rows),
            deleted_committed_row_ids,
        })
    }

    fn sync_insert_constraint_error_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        visible_rows_cache: &mut HashMap<String, Vec<VersionedRow>>,
    ) -> Result<Option<String>> {
        self.sync_projected_row_constraint_error_for_values(table, values, visible_rows_cache, None)
    }

    fn sync_projected_row_constraint_error_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        visible_rows_cache: &mut HashMap<String, Vec<VersionedRow>>,
        skip_row_id: Option<RowId>,
    ) -> Result<Option<String>> {
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        for col_def in &meta.columns {
            if !col_def.nullable && !col_def.primary_key && col_def.default.is_none() {
                match values.get(&col_def.name) {
                    None | Some(Value::Null) => {
                        return Ok(Some(format!(
                            "NOT NULL constraint violated: {}.{}",
                            table, col_def.name
                        )));
                    }
                    _ => {}
                }
            }
        }

        let visible_rows = cached_visible_rows(self, visible_rows_cache, table)?;
        for col_def in &meta.columns {
            if (col_def.primary_key || col_def.unique)
                && let Some(new_value) = values.get(&col_def.name)
                && *new_value != Value::Null
                && visible_rows.iter().any(|row| {
                    skip_row_id != Some(row.row_id)
                        && row.values.get(&col_def.name) == Some(new_value)
                })
            {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}.{}",
                    table, col_def.name
                )));
            }
        }

        for unique_columns in &meta.unique_constraints {
            let Some(tuple) = tuple_values_from_map(values, unique_columns) else {
                continue;
            };
            if visible_rows.iter().any(|row| {
                skip_row_id != Some(row.row_id)
                    && tuple_values_for_row(row, unique_columns).as_ref() == Some(&tuple)
            }) {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}({})",
                    table,
                    unique_columns.join(", ")
                )));
            }
        }

        Ok(None)
    }

    fn sync_upsert_constraint_error_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        existing: &VersionedRow,
        visible_rows_cache: &mut HashMap<String, Vec<VersionedRow>>,
    ) -> Result<Option<String>> {
        let values = match self.coerce_row_for_insert(table, values.clone(), None, None) {
            Ok(values) => values,
            Err(err) if is_fatal_sync_apply_error(&err) => return Err(err),
            Err(err) => return Ok(Some(format!("{err}"))),
        };

        if let Some(meta) = self.table_meta(table) {
            for col_def in meta.columns.iter().filter(|column| column.immutable) {
                let Some(incoming) = values.get(&col_def.name) else {
                    continue;
                };
                if existing.values.get(&col_def.name) != Some(incoming) {
                    return Ok(Some(format!(
                        "{}",
                        Error::ImmutableColumn {
                            table: table.to_string(),
                            column: col_def.name.clone(),
                        }
                    )));
                }
            }
        }

        if let Some(err) = self.sync_projected_row_constraint_error_for_values(
            table,
            &values,
            visible_rows_cache,
            Some(existing.row_id),
        )? {
            return Ok(Some(err));
        }

        let snapshot = self.snapshot_for_read();
        for check in [
            self.assert_row_write_allowed(table, existing.row_id, &existing.values, snapshot),
            self.assert_row_write_allowed(table, existing.row_id, &values, snapshot),
            self.validate_commit_time_upsert_state_transition(table, existing, &values),
        ] {
            if let Err(err) = check {
                if is_fatal_sync_apply_error(&err) {
                    return Err(err);
                }
                return Ok(Some(format!("{err}")));
            }
        }

        Ok(None)
    }

    fn retain_projected_rows_with_valid_composite_fks(
        &self,
        projected_rows: &mut Vec<ProjectedSyncRow>,
        deleted_committed_row_ids: &HashMap<String, HashSet<RowId>>,
    ) -> Result<()> {
        loop {
            let incoming_values = projected_sync_rows_by_table(projected_rows);
            let mut remove_indexes = Vec::new();
            for (idx, row) in projected_rows.iter().enumerate() {
                let meta = self
                    .table_meta(&row.table)
                    .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;
                if self.projected_composite_fk_violation_for_values(
                    &meta,
                    &row.values,
                    &incoming_values,
                    deleted_committed_row_ids,
                )? {
                    remove_indexes.push(idx);
                }
            }
            if remove_indexes.is_empty() {
                return Ok(());
            }
            for idx in remove_indexes.into_iter().rev() {
                projected_rows.remove(idx);
            }
        }
    }

    fn projected_composite_fk_violation_for_values(
        &self,
        child_meta: &TableMeta,
        values: &HashMap<String, Value>,
        incoming_values: &HashMap<String, Vec<HashMap<String, Value>>>,
        deleted_committed_row_ids: &HashMap<String, HashSet<RowId>>,
    ) -> Result<bool> {
        for fk in &child_meta.composite_foreign_keys {
            let Some(tuple) = tuple_values_from_map(values, &fk.child_columns) else {
                continue;
            };
            let incoming_match =
                incoming_values
                    .get(&fk.parent_table)
                    .is_some_and(|table_values| {
                        table_values.iter().any(|row_values| {
                            fk.parent_columns
                                .iter()
                                .zip(tuple.iter())
                                .all(|(column, value)| row_values.get(column) == Some(value))
                        })
                    });
            if incoming_match {
                continue;
            }

            let empty_deleted = HashSet::new();
            let deleted_parent_row_ids = deleted_committed_row_ids
                .get(&fk.parent_table)
                .unwrap_or(&empty_deleted);
            let committed_match = self.indexed_visible_row_exists_by_columns(
                &fk.parent_table,
                &fk.parent_columns,
                &tuple,
                self.snapshot(),
                deleted_parent_row_ids,
            )?;
            if !committed_match {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn projected_sync_dag_edge_types(&self, ddl: &[DdlChange]) -> HashSet<EdgeType> {
        let mut edge_types = HashSet::new();
        {
            let meta = self.relational_store.table_meta.read();
            for table_meta in meta.values() {
                edge_types.extend(table_meta.dag_edge_types.iter().cloned());
            }
        }

        for change in ddl {
            match change {
                DdlChange::CreateTable { constraints, .. }
                | DdlChange::AlterTable { constraints, .. } => {
                    edge_types.extend(ddl_dag_edge_types(constraints));
                }
                DdlChange::DropTable { .. }
                | DdlChange::CreateIndex { .. }
                | DdlChange::DropIndex { .. }
                | DdlChange::CreateTrigger { .. }
                | DdlChange::DropTrigger { .. }
                | DdlChange::CreateEventType { .. }
                | DdlChange::CreateSink { .. }
                | DdlChange::CreateRoute { .. }
                | DdlChange::DropRoute { .. } => {}
            }
        }

        edge_types
    }

    fn sync_dropped_tables(ddl: &[DdlChange]) -> HashSet<String> {
        ddl.iter()
            .filter_map(|change| match change {
                DdlChange::DropTable { name } => Some(name.clone()),
                _ => None,
            })
            .collect()
    }

    fn skip_event_bus_ddl_for_dropped_table(
        ddl: &DdlChange,
        dropped_tables: &HashSet<String>,
        table_exists_after_apply: impl Fn(&str) -> bool,
    ) -> bool {
        match ddl {
            DdlChange::CreateEventType { table, .. } | DdlChange::CreateRoute { table, .. } => {
                !table.is_empty()
                    && !table_exists_after_apply(table)
                    && dropped_tables.contains(table)
            }
            _ => false,
        }
    }

    fn preflight_sync_edge_cycles(
        &self,
        changes: &ChangeSet,
        dag_edge_types: &HashSet<EdgeType>,
    ) -> Result<()> {
        if dag_edge_types.is_empty() || changes.edges.is_empty() {
            return Ok(());
        }

        let snapshot = self.snapshot_for_read();
        let mut adjacency: HashMap<EdgeType, HashMap<NodeId, HashSet<NodeId>>> = HashMap::new();
        {
            let forward = self.graph_store.forward_adj.read();
            for entries in forward.values() {
                for edge in entries {
                    if edge.visible_at(snapshot) && dag_edge_types.contains(&edge.edge_type) {
                        adjacency
                            .entry(edge.edge_type.clone())
                            .or_default()
                            .entry(edge.source)
                            .or_default()
                            .insert(edge.target);
                    }
                }
            }
        }

        for edge in &changes.edges {
            if !dag_edge_types.contains(&edge.edge_type) {
                continue;
            }

            let by_type = adjacency.entry(edge.edge_type.clone()).or_default();
            let is_delete = matches!(edge.properties.get("__deleted"), Some(Value::Bool(true)));
            if is_delete {
                if let Some(targets) = by_type.get_mut(&edge.source) {
                    targets.remove(&edge.target);
                    if targets.is_empty() {
                        by_type.remove(&edge.source);
                    }
                }
                continue;
            }

            if edge.source == edge.target
                || sync_projected_edge_has_path(by_type, edge.target, edge.source)
            {
                return Err(Error::CycleDetected {
                    edge_type: edge.edge_type.clone(),
                    source_node: edge.source,
                    target_node: edge.target,
                });
            }

            by_type.entry(edge.source).or_default().insert(edge.target);
        }

        Ok(())
    }

    /// Extracts changes from this database since the given LSN.
    pub fn changes_since(&self, since_lsn: Lsn) -> ChangeSet {
        let _operation = self.assert_open_operation();
        // Future watermark guard
        if since_lsn > self.current_lsn() {
            return ChangeSet::default();
        }

        // Check if the ephemeral logs can serve the requested watermark.
        // After restart, both logs are empty but stores may have data — fall back to snapshot.
        let (change_first_lsn, change_log_empty, ddl_first_lsn, ddl_log_empty) = self
            .with_commit_lock(|| {
                let log = self.change_log.read();
                let change_first_lsn = log.first().map(|e| e.lsn());
                let change_log_empty = log.is_empty();
                drop(log);

                let ddl = self.ddl_log.read();
                let ddl_first_lsn = ddl.first().map(|(lsn, _)| *lsn);
                let ddl_log_empty = ddl.is_empty();

                (
                    change_first_lsn,
                    change_log_empty,
                    ddl_first_lsn,
                    ddl_log_empty,
                )
            });

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

        let (mut ddl_entries, change_entries) = self.with_commit_lock(|| {
            let ddl = self.ddl_log_entries_since_unlocked(since_lsn);
            let changes = self.change_log_since_unlocked(since_lsn);
            (ddl, changes)
        });
        if !self.access_is_admin() {
            ddl_entries.clear();
        }
        let (ddl_lsn, ddl): (Vec<_>, Vec<_>) = ddl_entries.into_iter().unzip();

        let mut rows = Vec::new();
        let mut edges = Vec::new();
        let mut vectors = Vec::new();

        for entry in change_entries {
            match entry {
                ChangeLogEntry::RowInsert { table, row_id, lsn } => {
                    let snapshot = self.snapshot_at(lsn);
                    if let Some(row) = self.row_for_change(&table, row_id, lsn)
                        && self.row_read_allowed_for_change(&table, &row, snapshot)
                        && let Some((natural_key, values)) =
                            self.row_change_values_from_row(&table, &row)
                    {
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
                    row_id,
                } => {
                    let snapshot = self.snapshot_before_lsn(lsn);
                    if !self.access_is_admin() {
                        let Some(row) = self.row_visible_at_snapshot(&table, row_id, snapshot)
                        else {
                            continue;
                        };
                        if !self.row_read_allowed_for_change(&table, &row, snapshot) {
                            continue;
                        }
                    }
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
                    if !self.graph_edge_read_allowed_for_change(
                        source,
                        target,
                        &edge_type,
                        self.snapshot_at(lsn),
                    ) {
                        continue;
                    }
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
                    if !self.graph_edge_read_allowed_for_change(
                        source,
                        target,
                        &edge_type,
                        self.snapshot_before_lsn(lsn),
                    ) {
                        continue;
                    }
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
                    if self.row_id_read_allowed_for_change(
                        &index.table,
                        row_id,
                        self.snapshot_at(lsn),
                    ) && let Some(vector) = self.vector_for_row_lsn(&index, row_id, lsn)
                    {
                        vectors.push(VectorChange {
                            index,
                            row_id,
                            vector,
                            lsn,
                        });
                    }
                }
                ChangeLogEntry::VectorDelete { index, row_id, lsn } => {
                    if self.row_id_read_allowed_for_change(
                        &index.table,
                        row_id,
                        self.snapshot_before_lsn(lsn),
                    ) {
                        vectors.push(VectorChange {
                            index,
                            row_id,
                            vector: Vec::new(),
                            lsn,
                        });
                    }
                }
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
            ddl_lsn,
        }
    }

    /// Returns the current LSN of this database.
    pub fn current_lsn(&self) -> Lsn {
        let _operation = self.assert_open_operation();
        if let Some(lsn) = CRON_LSN_OVERRIDE.with(|slot| *slot.borrow()) {
            return lsn;
        }
        self.tx_mgr.current_lsn()
    }

    /// Returns the highest-committed TxId on this database.
    pub fn committed_watermark(&self) -> TxId {
        let _operation = self.assert_open_operation();
        self.tx_mgr.current_tx_max()
    }

    /// Returns the next TxId the allocator will issue on this database.
    pub fn next_tx(&self) -> TxId {
        let _operation = self.assert_open_operation();
        self.tx_mgr.peek_next_tx()
    }

    pub fn pause_after_relational_apply_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = self.apply_phase_pause.clone();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    /// Subscribe to commit events. Returns a receiver that yields a `CommitEvent`
    /// after each commit.
    pub fn subscribe(&self) -> Receiver<CommitEvent> {
        let _operation = self.assert_open_operation();
        self.subscribe_with_capacity(DEFAULT_SUBSCRIPTION_CAPACITY)
    }

    /// Subscribe with a custom channel capacity.
    pub fn subscribe_with_capacity(&self, capacity: usize) -> Receiver<CommitEvent> {
        let _operation = self.assert_open_operation();
        let (tx, rx) = mpsc::sync_channel(capacity.max(1));
        self.subscriptions.lock().subscribers.push(tx);
        rx
    }

    /// Returns health metrics for the subscription system.
    pub fn subscription_health(&self) -> SubscriptionMetrics {
        let _operation = self.assert_open_operation();
        let subscriptions = self.subscriptions.lock();
        SubscriptionMetrics {
            active_channels: subscriptions.subscribers.len(),
            events_sent: subscriptions.events_sent,
            events_dropped: subscriptions.events_dropped,
        }
    }

    pub fn complete_initialization(&self) -> Result<()> {
        let _operation = self.open_operation()?;
        let declarations = self.trigger.declarations.lock();
        let callbacks = self.trigger.callbacks.read();
        for name in declarations.keys() {
            if !callbacks.contains_key(name) {
                return Err(Error::TriggerCallbackMissing {
                    trigger_name: name.clone(),
                });
            }
        }
        self.trigger.ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn register_trigger_callback<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static,
    {
        let _operation = self.open_operation()?;
        if !self.trigger.declarations.lock().contains_key(name) {
            return Err(Error::TriggerNotDeclared {
                trigger_name: name.to_string(),
            });
        }
        let mut callbacks = self.trigger.callbacks.write();
        if callbacks.contains_key(name) {
            return Err(Error::TriggerAlreadyRegistered {
                trigger_name: name.to_string(),
            });
        }
        callbacks.insert(name.to_string(), Arc::new(callback));
        Ok(())
    }

    pub fn list_triggers(&self) -> Vec<TriggerDeclaration> {
        let _operation = self.assert_open_operation();
        self.persisted_trigger_declarations()
    }

    pub fn registered_trigger_callbacks(&self) -> Vec<String> {
        let _operation = self.assert_open_operation();
        let mut names = self
            .trigger
            .callbacks
            .read()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn trigger_cascade_depth_cap(&self) -> u32 {
        let _operation = self.assert_open_operation();
        trigger::TRIGGER_CASCADE_DEPTH_CAP
    }

    pub fn trigger_audit_ring_capacity(&self) -> usize {
        let _operation = self.assert_open_operation();
        trigger::TRIGGER_AUDIT_RING_CAPACITY
    }

    pub fn trigger_audit_log(&self) -> Vec<TriggerAuditEntry> {
        let _operation = self.assert_open_operation();
        self.trigger.audit_ring.lock().iter().cloned().collect()
    }

    pub fn trigger_audit_history(
        &self,
        filter: TriggerAuditFilter,
    ) -> Result<Vec<TriggerAuditEntry>> {
        let _operation = self.open_operation()?;
        let history = if let Some(persistence) = &self.persistence {
            persistence.load_trigger_audit_history()?
        } else {
            self.trigger.volatile_audit_history.lock().clone()
        };
        Ok(history
            .into_iter()
            .filter(|entry| {
                filter
                    .trigger_name
                    .as_ref()
                    .is_none_or(|name| entry.trigger_name == *name)
                    && filter
                        .status
                        .is_none_or(|status| entry.status.matches_filter(status))
            })
            .collect())
    }

    /// Applies a ChangeSet to this database with the given conflict policies.
    pub fn apply_changes(
        &self,
        mut changes: ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<ApplyResult> {
        let _operation = self.open_operation()?;
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::Other(
                "apply_changes is not allowed inside cron callbacks".to_string(),
            ));
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::Other(
                "apply_changes is not allowed inside trigger callbacks".to_string(),
            ));
        }
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::Other(
                "apply_changes is not allowed from another thread while a cron callback is active"
                    .to_string(),
            ));
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return Err(Error::Other(
                "apply_changes is not allowed from another thread while a trigger callback is active"
                    .to_string(),
            ));
        }
        // Per I14: the whole batch takes the index-maintenance lock once.
        // Per-row commits reuse the same guard via the per-row apply that
        // runs inside the tx manager's commit_mutex, so no second write
        // acquisition happens for the scope of this call.
        self.relational_store.bump_index_write_lock_count();
        Self::validate_public_changeset_ddl_lsn(&changes)?;
        self.plugin.on_sync_pull(&mut changes)?;
        Self::validate_public_changeset_ddl_lsn(&changes)?;
        if !self.access_is_admin() && !changes.ddl.is_empty() {
            if let Some(trigger_ddl) = changes.ddl.iter().find(|ddl| {
                matches!(
                    ddl,
                    DdlChange::CreateTrigger { .. } | DdlChange::DropTrigger { .. }
                )
            }) {
                let operation = match trigger_ddl {
                    DdlChange::CreateTrigger { .. } => "apply_changes CREATE TRIGGER",
                    DdlChange::DropTrigger { .. } => "apply_changes DROP TRIGGER",
                    _ => "apply_changes trigger DDL",
                };
                return Err(Error::TriggerRequiresAdmin {
                    operation: operation.to_string(),
                });
            }
            return Err(Error::Other(
                "sync DDL apply requires an admin database handle".to_string(),
            ));
        }
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
        self.preflight_sync_ddl_mixed_apply(&changes, policies)?;
        self.preflight_sync_apply_trigger_ready(&changes)?;
        let _sync_trigger_gate = self.enter_sync_apply_trigger_gate_bypass();

        let lsn_groups = changes.split_by_data_lsn();
        if lsn_groups.len() > 1 {
            let mut total = ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: Vec::new(),
                new_lsn: self.current_lsn(),
            };
            for group in lsn_groups {
                let result = self.apply_changes_single_lsn_group(group, policies)?;
                total.applied_rows += result.applied_rows;
                total.skipped_rows += result.skipped_rows;
                total.conflicts.extend(result.conflicts);
                total.new_lsn = result.new_lsn;
            }
            return Ok(total);
        }

        self.apply_changes_single_lsn_group(
            lsn_groups
                .into_iter()
                .next()
                .expect("split_by_data_lsn always returns at least one group"),
            policies,
        )
    }

    fn validate_public_changeset_ddl_lsn(changes: &ChangeSet) -> Result<()> {
        changes
            .validate_ddl_lsn_cardinality()
            .map_err(Error::SyncError)
    }

    fn preflight_sync_apply_trigger_ready(&self, changes: &ChangeSet) -> Result<()> {
        if self.trigger.ready.load(Ordering::SeqCst) || changes.is_empty() {
            return Ok(());
        }

        if self.sync_changeset_is_trigger_tombstone_only(changes) {
            return Ok(());
        }

        if !changes.ddl.is_empty() {
            let projected_triggers = self.preflight_sync_trigger_projection(changes)?;
            if projected_triggers.is_empty() {
                return Ok(());
            }
        }

        self.ensure_sync_apply_ready()
    }

    fn apply_changes_single_lsn_group(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<ApplyResult> {
        let mut tx = self.begin();
        let commit_each_row = false;
        let batch_row_commits = false;
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
        let mut event_bus_ddl = Vec::new();
        let mut trigger_ddl = Vec::new();
        let mut sync_trigger_projection = self.trigger.declarations.lock().clone();
        let group_has_data = changes.data_entry_count() != 0;
        let dropped_tables = Self::sync_dropped_tables(&changes.ddl);
        let ddl_result = (|| -> Result<()> {
            for ddl in changes.ddl.clone() {
                match ddl {
                    DdlChange::CreateTable {
                        name,
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    } => {
                        if self.table_meta(&name).is_some() {
                            if let Some(local_meta) = self.table_meta(&name) {
                                if !sync_table_shape_matches(
                                    &local_meta,
                                    &columns,
                                    &constraints,
                                    &foreign_keys,
                                    &composite_foreign_keys,
                                    &composite_unique,
                                ) {
                                    return Err(Error::SchemaInvalid {
                                        reason: format!(
                                            "schema mismatch for table {name}: structured constraints differ"
                                        ),
                                    });
                                }
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
                        let sql = sync_create_table_sql(
                            &name,
                            &columns,
                            &constraints,
                            &foreign_keys,
                            &composite_foreign_keys,
                            &composite_unique,
                        );
                        self.execute_in_tx(tx, &sql, &HashMap::new())?;
                        self.clear_statement_cache();
                        table_meta_cache.remove(&name);
                        visible_rows_cache.remove(&name);
                    }
                    DdlChange::DropTable { name } => {
                        let projected_table_triggers = sync_trigger_projection
                            .values()
                            .filter(|declaration| declaration.table == name)
                            .map(|declaration| declaration.name.clone())
                            .collect::<Vec<_>>();
                        if !projected_table_triggers.is_empty() {
                            let current_trigger_names = self
                                .trigger
                                .declarations
                                .lock()
                                .keys()
                                .cloned()
                                .collect::<HashSet<_>>();
                            for trigger_name in &projected_table_triggers {
                                let already_queued_drop = trigger_ddl.iter().any(|change| {
                                    matches!(change, DdlChange::DropTrigger { name } if name == trigger_name)
                                });
                                if !current_trigger_names.contains(trigger_name)
                                    && !already_queued_drop
                                {
                                    trigger_ddl.push(DdlChange::DropTrigger {
                                        name: trigger_name.clone(),
                                    });
                                }
                            }
                            for trigger_name in projected_table_triggers {
                                sync_trigger_projection.remove(&trigger_name);
                            }
                        }
                        let table_had_triggers = self
                            .trigger
                            .declarations
                            .lock()
                            .values()
                            .any(|declaration| declaration.table == name);
                        if self.table_meta(&name).is_some() {
                            if let Some(block) =
                                crate::executor::rank_policy_drop_table_blocker(self, &name)
                            {
                                return Err(block);
                            }
                            let bytes_to_release =
                                crate::executor::estimate_drop_table_bytes(self, &name);
                            let prefix_trigger_ddl = std::mem::take(&mut trigger_ddl);
                            self.allocate_ddl_lsn(|lsn| {
                                self.log_drop_table_ddl_and_remove_triggers_with_prefix(
                                    &name,
                                    lsn,
                                    &prefix_trigger_ddl,
                                )
                            })?;
                            self.accountant().release(bytes_to_release);
                            self.clear_statement_cache();
                        } else if table_had_triggers {
                            let prefix_trigger_ddl = std::mem::take(&mut trigger_ddl);
                            self.allocate_ddl_lsn(|lsn| {
                                self.log_drop_table_ddl_and_remove_triggers_with_prefix(
                                    &name,
                                    lsn,
                                    &prefix_trigger_ddl,
                                )
                            })?;
                            self.clear_statement_cache();
                        } else if !group_has_data && !trigger_ddl.is_empty() {
                            self.apply_trigger_ddl_batch(std::mem::take(&mut trigger_ddl))?;
                        }
                        table_meta_cache.remove(&name);
                        visible_rows_cache.remove(&name);
                    }
                    DdlChange::AlterTable {
                        name,
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    } => {
                        if self.table_meta(&name).is_none() {
                            continue;
                        }
                        let existing = self.table_meta(&name).unwrap_or_default();
                        let existing_cols: HashSet<String> =
                            existing.columns.iter().map(|c| c.name.clone()).collect();
                        for (col, ty) in &columns {
                            if existing_cols.contains(col.as_str()) {
                                continue;
                            }
                            let sql = format!(
                                "ALTER TABLE {} ADD COLUMN {} {}",
                                name,
                                col,
                                sync_column_type_with_foreign_key(col, ty, &foreign_keys)
                            );
                            self.execute_in_tx(tx, &sql, &HashMap::new())?;
                        }
                        if let Some(mut meta) = self.table_meta(&name) {
                            let incoming = rough_sync_table_meta(
                                &columns,
                                &constraints,
                                &foreign_keys,
                                &composite_foreign_keys,
                                &composite_unique,
                            );
                            for incoming_column in incoming.columns {
                                if let Some(existing_column) = meta
                                    .columns
                                    .iter_mut()
                                    .find(|column| column.name == incoming_column.name)
                                {
                                    existing_column.references = incoming_column.references;
                                }
                            }
                            if !incoming.unique_constraints.is_empty() {
                                meta.unique_constraints = incoming.unique_constraints;
                            }
                            if !incoming.composite_foreign_keys.is_empty() {
                                meta.composite_foreign_keys = incoming.composite_foreign_keys;
                            }
                            let user_indexes = meta
                                .indexes
                                .iter()
                                .filter(|index| index.kind == IndexKind::UserDeclared)
                                .cloned()
                                .collect::<Vec<_>>();
                            meta.indexes = crate::executor::auto_indexes_for_table_meta(&meta);
                            meta.indexes.extend(user_indexes);
                            {
                                let store = self.relational_store();
                                store.table_meta.write().insert(name.clone(), meta.clone());
                                for index in meta
                                    .indexes
                                    .iter()
                                    .filter(|index| index.kind == IndexKind::Auto)
                                {
                                    store.create_index_storage(
                                        &name,
                                        &index.name,
                                        index.columns.clone(),
                                    );
                                    store.rebuild_index(&name, &index.name);
                                }
                            }
                            self.persist_table_meta(&name, &meta)?;
                        }
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
                                if let Some(block) = crate::executor::rank_policy_drop_index_blocker(
                                    self, &table, &name,
                                ) {
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
                    trigger_change @ (DdlChange::CreateTrigger { .. }
                    | DdlChange::DropTrigger { .. }) => {
                        let trigger_table = match &trigger_change {
                            DdlChange::CreateTrigger { table, .. } => Some(table.clone()),
                            DdlChange::DropTrigger { .. } => None,
                            _ => None,
                        };
                        self.require_admin_trigger_ddl(match &trigger_change {
                            DdlChange::CreateTrigger { .. } => "apply_changes CREATE TRIGGER",
                            DdlChange::DropTrigger { .. } => "apply_changes DROP TRIGGER",
                            _ => "apply_changes trigger DDL",
                        })?;
                        let projected_table_meta = self.relational_store.table_meta.read().clone();
                        self.apply_sync_trigger_ddl_to_projection(
                            &mut sync_trigger_projection,
                            &trigger_change,
                            &projected_table_meta,
                        )?;
                        trigger_ddl.push(trigger_change);
                        if let Some(table) = trigger_table {
                            table_meta_cache.remove(&table);
                            visible_rows_cache.remove(&table);
                        }
                    }
                    event_ddl @ (DdlChange::CreateEventType { .. }
                    | DdlChange::CreateSink { .. }
                    | DdlChange::CreateRoute { .. }
                    | DdlChange::DropRoute { .. }) => {
                        event_bus_ddl.push(event_ddl);
                    }
                }
            }
            event_bus_ddl.retain(|ddl| {
                !Self::skip_event_bus_ddl_for_dropped_table(ddl, &dropped_tables, |table| {
                    self.table_meta(table).is_some()
                })
            });
            Ok(())
        })();
        if let Err(err) = ddl_result {
            let _ = self.rollback(tx);
            return Err(err);
        }
        let projected_sync_apply =
            match self.projected_sync_incoming_values_for_applied_rows(&changes.rows, policies) {
                Ok(projection) => projection,
                Err(err) => {
                    let _ = self.rollback(tx);
                    return Err(err);
                }
            };
        for row in changes.rows {
            if row.values.is_empty() {
                result.skipped_rows += 1;
                if commit_each_row {
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

            let existing = match cached_point_lookup(
                self,
                &mut visible_rows_cache,
                &row.table,
                &row.natural_key.column,
                &row.natural_key.value,
            ) {
                Ok(existing) => existing,
                Err(err) => {
                    let _ = self.rollback(tx);
                    return Err(err);
                }
            };
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
                if commit_each_row {
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
                                let visible_rows = match cached_visible_rows(
                                    self,
                                    &mut visible_rows_cache,
                                    &row.table,
                                ) {
                                    Ok(rows) => rows,
                                    Err(err) => {
                                        let _ = self.rollback(tx);
                                        return Err(err);
                                    }
                                };
                                if col_def.unique
                                    && !col_def.primary_key
                                    && let Some(new_val) = values.get(&col_def.name)
                                    && *new_val != Value::Null
                                    && visible_rows
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
                            if commit_each_row {
                                self.commit_with_source(tx, CommitSource::SyncPull)?;
                                tx = self.begin();
                            }
                            continue;
                        }

                        if let Some(err) = self.sync_composite_fk_violation_for_values(
                            tx,
                            &row.table,
                            &meta,
                            &values,
                            &projected_sync_apply.incoming_values,
                            &projected_sync_apply.deleted_committed_row_ids,
                        )? {
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
                            if commit_each_row {
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
                            && let Err(err) =
                                self.tx_mgr.advance_for_sync(tx, &row.table, *incoming)
                        {
                            overflow = Some(err);
                            break;
                        }
                    }
                    if let Some(err) = overflow {
                        let _ = self.rollback(tx);
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
                                let _ = self.rollback(tx);
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
                                    if commit_each_row {
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
                                    self.tx_mgr.advance_for_sync(tx, &row.table, *incoming)
                            {
                                overflow = Some(err);
                                break;
                            }
                        }
                        if let Some(err) = overflow {
                            let _ = self.rollback(tx);
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
                                    let _ = self.rollback(tx);
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
                            && let Err(err) =
                                self.tx_mgr.advance_for_sync(tx, &row.table, *incoming)
                        {
                            overflow = Some(err);
                            break;
                        }
                    }
                    if let Some(err) = overflow {
                        let _ = self.rollback(tx);
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
                                let _ = self.rollback(tx);
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

            if commit_each_row {
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
                if let Err(err) = self.delete_edge(tx, edge.source, edge.target, &edge.edge_type) {
                    let _ = self.rollback(tx);
                    return Err(err);
                }
            } else {
                if let Err(err) = self.insert_edge(
                    tx,
                    edge.source,
                    edge.target,
                    edge.edge_type,
                    edge.properties,
                ) {
                    let _ = self.rollback(tx);
                    return Err(err);
                }
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
                if let Err(err) = self.delete_vector(tx, vector.index.clone(), local_row_id) {
                    let _ = self.rollback(tx);
                    return Err(err);
                }
            } else {
                if self.has_live_vector(local_row_id, self.snapshot()) {
                    let _ = self.delete_vector(tx, vector.index.clone(), local_row_id);
                }
                if let Err(err) =
                    self.insert_vector(tx, vector.index.clone(), local_row_id, vector.vector)
                {
                    let _ = self.rollback(tx);
                    return Err(err);
                }
            }
        }

        let sync_pull_trigger_audit_projection = if trigger_ddl.is_empty() {
            None
        } else {
            Some(self.sync_pull_trigger_audit_projection(&trigger_ddl)?)
        };

        self.commit_with_source_and_sync_ddl_and_trigger_audit_projection(
            tx,
            CommitSource::SyncPull,
            &event_bus_ddl,
            &trigger_ddl,
            sync_pull_trigger_audit_projection.as_ref(),
        )?;
        let committed_lsn = self.current_lsn();
        result.new_lsn = committed_lsn;
        Ok(result)
    }

    fn row_for_change(&self, table: &str, row_id: RowId, lsn: Lsn) -> Option<VersionedRow> {
        let tables = self.relational_store.tables.read();
        let rows = tables.get(table)?;
        rows.iter()
            .rev()
            .find(|r| r.row_id == row_id && r.lsn == lsn)
            .or_else(|| rows.iter().rev().find(|r| r.row_id == row_id))
            .cloned()
    }

    fn row_visible_at_snapshot(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Option<VersionedRow> {
        let tables = self.relational_store.tables.read();
        let rows = tables.get(table)?;
        rows.iter()
            .rev()
            .find(|row| row.row_id == row_id && row.visible_at(snapshot))
            .cloned()
    }

    fn snapshot_before_lsn(&self, lsn: Lsn) -> SnapshotId {
        self.snapshot_at(Lsn(lsn.0.saturating_sub(1)))
    }

    fn row_change_values_from_row(
        &self,
        table: &str,
        row: &VersionedRow,
    ) -> Option<(NaturalKey, HashMap<String, Value>)> {
        let meta = self.relational_store.table_meta.read();
        let key_col = meta.get(table).and_then(natural_key_column_for_meta)?;

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

    fn row_id_for_natural_key(
        &self,
        table: &str,
        key_col: &str,
        key_value: &Value,
        snapshot: SnapshotId,
    ) -> Option<RowId> {
        self.relational_store
            .tables
            .read()
            .get(table)?
            .iter()
            .rev()
            .find(|row| row.visible_at(snapshot) && row.values.get(key_col) == Some(key_value))
            .map(|row| row.row_id)
    }

    fn row_read_allowed_for_change(
        &self,
        table: &str,
        row: &VersionedRow,
        snapshot: SnapshotId,
    ) -> bool {
        let Some(meta) = self.table_meta(table) else {
            return false;
        };
        self.read_allowed_for_row(table, &meta, row, snapshot)
            .unwrap_or(false)
    }

    fn row_id_read_allowed_for_change(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> bool {
        let Some(row) = self.row_visible_at_snapshot(table, row_id, snapshot) else {
            return false;
        };
        self.row_read_allowed_for_change(table, &row, snapshot)
    }

    fn graph_edge_read_allowed_for_change(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> bool {
        self.node_read_allowed(source, snapshot).unwrap_or(false)
            && self.node_read_allowed(target, snapshot).unwrap_or(false)
            && self
                .edge_read_allowed(source, target, edge_type, snapshot)
                .unwrap_or(false)
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

fn current_write_set_counts(ws: &WriteSet) -> WriteSetCounts {
    WriteSetCounts {
        relational_inserts: ws.relational_inserts.len(),
        relational_deletes: ws.relational_deletes.len(),
        adj_inserts: ws.adj_inserts.len(),
        adj_deletes: ws.adj_deletes.len(),
        vector_inserts: ws.vector_inserts.len(),
        vector_deletes: ws.vector_deletes.len(),
        vector_moves: ws.vector_moves.len(),
    }
}

fn current_wallclock() -> Wallclock {
    Wallclock(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    )
}

fn remove_write_set_slice(ws: &mut WriteSet, before: WriteSetCounts, after: WriteSetCounts) {
    fn drain_range<T>(items: &mut Vec<T>, start: usize, end: usize) {
        if start >= items.len() {
            return;
        }
        let end = end.min(items.len());
        if start < end {
            items.drain(start..end);
        }
    }

    drain_range(
        &mut ws.relational_inserts,
        before.relational_inserts,
        after.relational_inserts,
    );
    drain_range(
        &mut ws.relational_deletes,
        before.relational_deletes,
        after.relational_deletes,
    );
    drain_range(&mut ws.adj_inserts, before.adj_inserts, after.adj_inserts);
    drain_range(&mut ws.adj_deletes, before.adj_deletes, after.adj_deletes);
    drain_range(
        &mut ws.vector_inserts,
        before.vector_inserts,
        after.vector_inserts,
    );
    drain_range(
        &mut ws.vector_deletes,
        before.vector_deletes,
        after.vector_deletes,
    );
    drain_range(
        &mut ws.vector_moves,
        before.vector_moves,
        after.vector_moves,
    );
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

fn upsert_cached_projection(
    cache: &mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
    row_id: RowId,
    values: HashMap<String, Value>,
    lsn: Lsn,
) {
    if let Some(rows) = cache.get_mut(table) {
        if let Some(row) = rows.iter_mut().find(|row| row.row_id == row_id) {
            row.values = values;
            row.lsn = lsn;
        } else {
            rows.push(VersionedRow {
                row_id,
                values,
                created_tx: TxId(0),
                deleted_tx: None,
                lsn,
                created_at: None,
            });
        }
    }
}

fn projected_sync_rows_by_table(
    rows: &[ProjectedSyncRow],
) -> HashMap<String, Vec<HashMap<String, Value>>> {
    let mut by_table = HashMap::<String, Vec<HashMap<String, Value>>>::new();
    for row in rows {
        by_table
            .entry(row.table.clone())
            .or_default()
            .push(row.values.clone());
    }
    by_table
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
        if let Some(row) = rows.iter_mut().find(|row| {
            row.row_id == entry.row_id && row.created_tx == entry.created_tx && row.lsn == entry.lsn
        }) {
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
        let _operation_barrier = self.operation_gate.write();
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.stop_cron_tickler();
        self.stop_event_bus_threads();
        let runtime = self.pruning_runtime.get_mut();
        runtime.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = runtime.handle.take() {
            let _ = handle.join();
        }
        if self.resource_owner {
            self.subscriptions.lock().subscribers.clear();
            if let Some(persistence) = &self.persistence {
                persistence.close();
            }
            self.release_open_registry();
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

fn tuple_values_from_map(
    values_by_column: &HashMap<String, Value>,
    columns: &[String],
) -> Option<Vec<Value>> {
    let mut values = Vec::with_capacity(columns.len());
    for column in columns {
        match values_by_column.get(column) {
            Some(Value::Null) | None => return None,
            Some(value) => values.push(value.clone()),
        }
    }
    Some(values)
}

fn tuple_values_for_row(row: &VersionedRow, columns: &[String]) -> Option<Vec<Value>> {
    tuple_values_from_map(&row.values, columns)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RetentionRowKey {
    table: String,
    row_id: RowId,
}

impl RetentionRowKey {
    fn new(table: &str, row_id: RowId) -> Self {
        Self {
            table: table.to_string(),
            row_id,
        }
    }
}

#[derive(Debug, Clone)]
struct ProjectedSyncRow {
    table: String,
    natural_key: NaturalKey,
    values: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
struct ProjectedSyncApply {
    incoming_values: HashMap<String, Vec<HashMap<String, Value>>>,
    deleted_committed_row_ids: HashMap<String, HashSet<RowId>>,
}

fn staged_tuple_exists(ws: &WriteSet, table: &str, columns: &[String], values: &[Value]) -> bool {
    ws.relational_inserts.iter().any(|(insert_table, row)| {
        insert_table == table
            && columns
                .iter()
                .zip(values.iter())
                .all(|(column, value)| row.values.get(column) == Some(value))
    })
}

fn prune_expired_rows(
    relational_store: &Arc<RelationalStore>,
    graph_store: &Arc<GraphStore>,
    vector_store: &Arc<VectorStore>,
    accountant: &MemoryAccountant,
    persistence: Option<&Arc<RedbPersistence>>,
    sync_watermark: Lsn,
) -> Result<PruningReport> {
    let now = Wallclock::now();
    let metas = relational_store.table_meta.read().clone();
    let mut pruned_by_table: HashMap<String, Vec<RowId>> = HashMap::new();
    let mut pruned_node_ids = HashSet::new();
    let mut released_row_bytes = 0usize;
    let mut blocked = Vec::new();

    let table_snapshot = relational_store.tables.read().clone();
    let prune_candidates = retention_prune_candidates(&metas, &table_snapshot, now, sync_watermark);
    let protected_prune_candidates =
        fk_protected_prune_candidates(&metas, &table_snapshot, &prune_candidates);
    for (table_name, rows) in &table_snapshot {
        let Some(meta) = metas.get(table_name) else {
            continue;
        };
        if meta.default_ttl_seconds.is_none() {
            continue;
        }

        for row in rows {
            let row_key = RetentionRowKey::new(table_name, row.row_id);
            if !prune_candidates.contains(&row_key) {
                continue;
            }
            if let Some(reason) = prune_blocker_for_referenced_parent(
                table_name,
                row,
                &metas,
                &table_snapshot,
                &prune_candidates,
                &protected_prune_candidates,
            ) {
                blocked.push(reason);
                continue;
            }

            pruned_by_table
                .entry(table_name.clone())
                .or_default()
                .push(row.row_id);
            released_row_bytes = released_row_bytes.saturating_add(estimate_row_bytes_for_meta(
                &row.values,
                meta,
                false,
            ));
            if let Some(Value::Uuid(id)) = row.values.get("id") {
                pruned_node_ids.insert(*id);
            }
        }
    }

    let pruned_row_ids: HashSet<RowId> = pruned_by_table
        .values()
        .flat_map(|rows| rows.iter().copied())
        .collect();
    if pruned_row_ids.is_empty() {
        return Ok(PruningReport {
            pruned_rows: 0,
            blocked_count: blocked.len() as u64,
            blocked,
        });
    }

    let pruned_by_table_sets = pruned_by_table
        .iter()
        .map(|(table, row_ids)| {
            (
                table.clone(),
                row_ids.iter().copied().collect::<HashSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let post_prune_table_rows = pruned_by_table_sets
        .iter()
        .map(|(table_name, row_ids)| {
            let rows = table_snapshot
                .get(table_name)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|row| !row_ids.contains(&row.row_id))
                .collect::<Vec<_>>();
            (table_name.clone(), rows)
        })
        .collect::<HashMap<_, _>>();
    let post_prune_vectors = vector_store
        .all_entries()
        .into_iter()
        .filter(|entry| !pruned_row_ids.contains(&entry.row_id))
        .collect::<Vec<_>>();
    let post_prune_edges = graph_store
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter().cloned())
        .filter(|entry| {
            !pruned_node_ids.contains(&entry.source) && !pruned_node_ids.contains(&entry.target)
        })
        .collect::<Vec<_>>();

    if let Some(persistence) = persistence {
        persistence.rewrite_pruned_state(
            &post_prune_table_rows,
            &post_prune_vectors,
            &post_prune_edges,
        )?;
    }

    {
        let mut tables = relational_store.tables.write();
        for (table_name, row_ids) in &pruned_by_table_sets {
            if let Some(rows) = tables.get_mut(table_name) {
                rows.retain(|row| !row_ids.contains(&row.row_id));
            }
        }
    }

    for table_name in pruned_by_table.keys() {
        if let Some(meta) = metas.get(table_name) {
            for index in &meta.indexes {
                relational_store.rebuild_index(table_name, &index.name);
            }
        }
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

    accountant.release(
        released_row_bytes
            .saturating_add(released_vector_bytes)
            .saturating_add(released_edge_bytes),
    );

    Ok(PruningReport {
        pruned_rows: pruned_row_ids.len() as u64,
        blocked_count: blocked.len() as u64,
        blocked,
    })
}

fn checked_prune_expired_rows(
    relational_store: &Arc<RelationalStore>,
    graph_store: &Arc<GraphStore>,
    vector_store: &Arc<VectorStore>,
    accountant: &MemoryAccountant,
    persistence: Option<&Arc<RedbPersistence>>,
    sync_watermark: Lsn,
) -> Result<PruningReport> {
    prune_expired_rows(
        relational_store,
        graph_store,
        vector_store,
        accountant,
        persistence,
        sync_watermark,
    )
}

fn log_pruning_error(err: &Error) {
    tracing::warn!(
        name: "retention_pruning_error",
        target: "retention_pruning",
        error = %err,
        "retention_pruning_error"
    );
}

fn retention_prune_candidates(
    metas: &HashMap<String, TableMeta>,
    tables: &HashMap<String, Vec<VersionedRow>>,
    now: Wallclock,
    sync_watermark: Lsn,
) -> HashSet<RetentionRowKey> {
    let mut candidates = HashSet::new();
    for (table_name, rows) in tables {
        let Some(meta) = metas.get(table_name) else {
            continue;
        };
        if meta.default_ttl_seconds.is_none() {
            continue;
        }
        for row in rows {
            if row_is_prunable(row, meta, now, sync_watermark) {
                candidates.insert(RetentionRowKey::new(table_name, row.row_id));
            }
        }
    }
    candidates
}

fn fk_protected_prune_candidates(
    metas: &HashMap<String, TableMeta>,
    tables: &HashMap<String, Vec<VersionedRow>>,
    prune_candidates: &HashSet<RetentionRowKey>,
) -> HashSet<RetentionRowKey> {
    let mut protected = HashSet::new();
    loop {
        let mut changed = false;
        for (parent_table, rows) in tables {
            for parent_row in rows {
                let parent_key = RetentionRowKey::new(parent_table, parent_row.row_id);
                if !prune_candidates.contains(&parent_key) || parent_row.deleted_tx.is_some() {
                    continue;
                }
                if prune_blocker_for_referenced_parent(
                    parent_table,
                    parent_row,
                    metas,
                    tables,
                    prune_candidates,
                    &protected,
                )
                .is_some()
                {
                    changed |= protected.insert(parent_key);
                }
            }
        }
        if !changed {
            return protected;
        }
    }
}

fn prune_blocker_for_referenced_parent(
    parent_table: &str,
    parent_row: &VersionedRow,
    metas: &HashMap<String, TableMeta>,
    tables: &HashMap<String, Vec<VersionedRow>>,
    prune_candidates: &HashSet<RetentionRowKey>,
    protected_prune_candidates: &HashSet<RetentionRowKey>,
) -> Option<String> {
    if parent_row.deleted_tx.is_some() {
        return None;
    }

    for (child_table, child_meta) in metas {
        for column in &child_meta.columns {
            let Some(reference) = &column.references else {
                continue;
            };
            if reference.table != parent_table {
                continue;
            }
            let Some(parent_value) = parent_row.values.get(&reference.column) else {
                continue;
            };
            if *parent_value == Value::Null {
                continue;
            }
            if child_rows_contain_live_reference(
                child_table,
                tables,
                std::slice::from_ref(&column.name),
                std::slice::from_ref(parent_value),
                prune_candidates,
                protected_prune_candidates,
            ) {
                return Some(format!(
                    "blocked pruning {}({}) because live {}({}) references it",
                    parent_table, reference.column, child_table, column.name
                ));
            }
        }

        for fk in &child_meta.composite_foreign_keys {
            if fk.parent_table != parent_table {
                continue;
            }
            let Some(parent_values) = tuple_values_for_row(parent_row, &fk.parent_columns) else {
                continue;
            };
            if child_rows_contain_live_reference(
                child_table,
                tables,
                &fk.child_columns,
                &parent_values,
                prune_candidates,
                protected_prune_candidates,
            ) {
                return Some(format!(
                    "blocked pruning {}({}) because live {}({}) references it",
                    parent_table,
                    fk.parent_columns.join(", "),
                    child_table,
                    fk.child_columns.join(", ")
                ));
            }
        }
    }

    None
}

fn child_rows_contain_live_reference(
    child_table: &str,
    tables: &HashMap<String, Vec<VersionedRow>>,
    child_columns: &[String],
    parent_values: &[Value],
    prune_candidates: &HashSet<RetentionRowKey>,
    protected_prune_candidates: &HashSet<RetentionRowKey>,
) -> bool {
    tables.get(child_table).is_some_and(|rows| {
        rows.iter().any(|row| {
            row.deleted_tx.is_none()
                && child_columns
                    .iter()
                    .zip(parent_values.iter())
                    .all(|(column, value)| row.values.get(column) == Some(value))
                && {
                    let child_key = RetentionRowKey::new(child_table, row.row_id);
                    !prune_candidates.contains(&child_key)
                        || protected_prune_candidates.contains(&child_key)
                }
        })
    })
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

fn commit_index_across_all(
    relational: &RelationalStore,
    graph: &GraphStore,
    vector: &VectorStore,
    change_log: &[ChangeLogEntry],
) -> BTreeMap<Lsn, TxId> {
    let mut index = BTreeMap::new();
    let mut add_entry = |lsn: Lsn, tx: TxId| {
        if lsn != Lsn(0) {
            index
                .entry(lsn)
                .and_modify(|current: &mut TxId| *current = (*current).max(tx))
                .or_insert(tx);
        }
    };
    let relational_rows = relational
        .tables
        .read()
        .iter()
        .map(|(table, rows)| (table.clone(), rows.clone()))
        .collect::<Vec<_>>();
    for row in relational
        .tables
        .read()
        .values()
        .flat_map(|rows| rows.iter().cloned())
    {
        add_entry(row.lsn, row.created_tx);
    }
    let graph_entries = graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter().cloned())
        .collect::<Vec<_>>();
    for entry in graph
        .forward_adj
        .read()
        .values()
        .flat_map(|entries| entries.iter().cloned())
    {
        add_entry(entry.lsn, entry.created_tx);
    }
    let vector_entries = vector.all_entries();
    for entry in &vector_entries {
        add_entry(entry.lsn, entry.created_tx);
    }

    for entry in change_log {
        match entry {
            ChangeLogEntry::RowInsert { .. }
            | ChangeLogEntry::EdgeInsert { .. }
            | ChangeLogEntry::VectorInsert { .. } => {}
            ChangeLogEntry::RowDelete {
                table, row_id, lsn, ..
            } => {
                if let Some(deleted_tx) = relational_rows
                    .iter()
                    .find(|(candidate, _)| candidate == table)
                    .and_then(|(_, rows)| {
                        rows.iter()
                            .filter(|row| {
                                row.row_id == *row_id && row.lsn < *lsn && row.deleted_tx.is_some()
                            })
                            .max_by_key(|row| row.lsn)
                            .and_then(|row| row.deleted_tx)
                    })
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
            ChangeLogEntry::EdgeDelete {
                source,
                target,
                edge_type,
                lsn,
            } => {
                if let Some(deleted_tx) = graph_entries
                    .iter()
                    .filter(|edge| {
                        edge.source == *source
                            && edge.target == *target
                            && edge.edge_type == *edge_type
                            && edge.lsn < *lsn
                            && edge.deleted_tx.is_some()
                    })
                    .max_by_key(|edge| edge.lsn)
                    .and_then(|edge| edge.deleted_tx)
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
            ChangeLogEntry::VectorDelete {
                index: vector_index,
                row_id,
                lsn,
            } => {
                if let Some(deleted_tx) = vector_entries
                    .iter()
                    .filter(|vector| {
                        vector.index == *vector_index
                            && vector.row_id == *row_id
                            && vector.lsn < *lsn
                            && vector.deleted_tx.is_some()
                    })
                    .max_by_key(|vector| vector.lsn)
                    .and_then(|vector| vector.deleted_tx)
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
        }
    }
    index
}

fn repair_visibility_tx_order_if_needed(
    relational: &RelationalStore,
    graph: &GraphStore,
    vector: &VectorStore,
    loaded_vectors: &mut [VectorEntry],
    table_meta: &HashMap<String, TableMeta>,
    persistence: &RedbPersistence,
    commit_index: &mut BTreeMap<Lsn, TxId>,
) -> Result<bool> {
    let mut previous = TxId(0);
    let mut needs_repair = false;
    for tx in commit_index.values() {
        if *tx < previous {
            needs_repair = true;
            break;
        }
        previous = *tx;
    }
    if !needs_repair {
        return Ok(false);
    }

    let mut tx_remap = HashMap::new();
    for (idx, tx) in commit_index.values().copied().enumerate() {
        tx_remap.entry(tx).or_insert(TxId(idx as u64 + 1));
    }

    for tx in commit_index.values_mut() {
        if let Some(mapped) = tx_remap.get(tx) {
            *tx = *mapped;
        }
    }

    let mut table_rows = {
        let mut tables = relational.tables.write();
        for rows in tables.values_mut() {
            for row in rows.iter_mut() {
                remap_tx_id(&mut row.created_tx, &tx_remap);
                if let Some(deleted_tx) = &mut row.deleted_tx {
                    remap_tx_id(deleted_tx, &tx_remap);
                }
            }
        }
        tables
            .iter()
            .map(|(table, rows)| (table.clone(), rows.clone()))
            .collect::<Vec<_>>()
    };
    for (table, meta) in table_meta {
        for decl in &meta.indexes {
            relational.rebuild_index(table, &decl.name);
        }
    }

    let graph_edges = {
        let mut edges = graph
            .forward_adj
            .read()
            .values()
            .flat_map(|entries| entries.iter().cloned())
            .collect::<Vec<_>>();
        for edge in &mut edges {
            remap_tx_id(&mut edge.created_tx, &tx_remap);
            if let Some(deleted_tx) = &mut edge.deleted_tx {
                remap_tx_id(deleted_tx, &tx_remap);
            }
        }

        let mut forward = HashMap::new();
        let mut reverse = HashMap::new();
        for edge in &edges {
            forward
                .entry(edge.source)
                .or_insert_with(Vec::new)
                .push(edge.clone());
            reverse
                .entry(edge.target)
                .or_insert_with(Vec::new)
                .push(edge.clone());
        }
        *graph.forward_adj.write() = forward;
        *graph.reverse_adj.write() = reverse;
        edges
    };

    for entry in loaded_vectors.iter_mut() {
        remap_tx_id(&mut entry.created_tx, &tx_remap);
        if let Some(deleted_tx) = &mut entry.deleted_tx {
            remap_tx_id(deleted_tx, &tx_remap);
        }
    }
    vector.replace_loaded_vectors(loaded_vectors.to_vec());

    for (table, rows) in table_rows.drain(..) {
        persistence.rewrite_table_rows(&table, &rows)?;
    }
    persistence.rewrite_graph_edges(&graph_edges)?;
    persistence.rewrite_vectors(loaded_vectors)?;

    Ok(true)
}

fn remap_tx_id(tx: &mut TxId, tx_remap: &HashMap<TxId, TxId>) {
    if let Some(mapped) = tx_remap.get(tx) {
        *tx = *mapped;
    }
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
        foreign_keys: single_column_foreign_keys_from_ast(ct),
        composite_foreign_keys: ct
            .composite_foreign_keys
            .iter()
            .map(|fk| CompositeForeignKey {
                child_columns: fk.child_columns.clone(),
                parent_table: fk.parent_table.clone(),
                parent_columns: fk.parent_columns.clone(),
            })
            .collect(),
        composite_unique: ct.unique_constraints.clone(),
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
        foreign_keys: single_column_foreign_keys_from_meta(meta, excluded_columns),
        composite_foreign_keys: meta
            .composite_foreign_keys
            .iter()
            .filter(|fk| {
                fk.child_columns
                    .iter()
                    .all(|column| !excluded_columns.contains(column))
            })
            .cloned()
            .collect(),
        composite_unique: meta
            .unique_constraints
            .iter()
            .filter(|columns| {
                columns
                    .iter()
                    .all(|column| !excluded_columns.contains(column))
            })
            .cloned()
            .collect(),
    }
}

fn single_column_foreign_keys_from_ast(ct: &CreateTable) -> Vec<SingleColumnForeignKey> {
    ct.columns
        .iter()
        .filter_map(|column| {
            column
                .references
                .as_ref()
                .map(|reference| SingleColumnForeignKey {
                    child_column: column.name.clone(),
                    parent_table: reference.table.clone(),
                    parent_column: reference.column.clone(),
                })
        })
        .collect()
}

fn single_column_foreign_keys_from_meta(
    meta: &TableMeta,
    excluded_columns: &HashSet<String>,
) -> Vec<SingleColumnForeignKey> {
    meta.columns
        .iter()
        .filter(|column| !excluded_columns.contains(&column.name))
        .filter_map(|column| {
            column
                .references
                .as_ref()
                .map(|reference| SingleColumnForeignKey {
                    child_column: column.name.clone(),
                    parent_table: reference.table.clone(),
                    parent_column: reference.column.clone(),
                })
        })
        .collect()
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
                })
                && meta.columns.iter().all(|column| {
                    column.references.as_ref().is_none_or(|reference| {
                        reference.table == *name
                            || !metas.contains_key(&reference.table)
                            || emitted.contains(&reference.table)
                    })
                })
                && meta.composite_foreign_keys.iter().all(|fk| {
                    fk.parent_table == *name
                        || !metas.contains_key(&fk.parent_table)
                        || emitted.contains(&fk.parent_table)
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
            foreign_keys: single_column_foreign_keys_from_meta(meta, &HashSet::new()),
            composite_foreign_keys: meta.composite_foreign_keys.clone(),
            composite_unique: meta.unique_constraints.clone(),
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

fn sync_create_table_sql(
    name: &str,
    columns: &[(String, String)],
    constraints: &[String],
    foreign_keys: &[SingleColumnForeignKey],
    composite_foreign_keys: &[CompositeForeignKey],
    composite_unique: &[Vec<String>],
) -> String {
    let mut table_elements = columns
        .iter()
        .map(|(column, ty)| {
            format!(
                "{column} {}",
                sync_column_type_with_foreign_key(column, ty, foreign_keys)
            )
        })
        .collect::<Vec<_>>();
    let mut table_options = Vec::new();
    let mut seen_table_elements = table_elements
        .iter()
        .map(|element| schema_clause_key(element))
        .collect::<HashSet<_>>();

    for unique in composite_unique {
        let element = format!("UNIQUE ({})", unique.join(", "));
        if seen_table_elements.insert(schema_clause_key(&element)) {
            table_elements.push(element);
        }
    }
    for fk in composite_foreign_keys {
        let element = format!(
            "FOREIGN KEY ({}) REFERENCES {}({})",
            fk.child_columns.join(", "),
            fk.parent_table,
            fk.parent_columns.join(", ")
        );
        if seen_table_elements.insert(schema_clause_key(&element)) {
            table_elements.push(element);
        }
    }
    for constraint in constraints {
        if sync_constraint_is_table_element(constraint) {
            if seen_table_elements.insert(schema_clause_key(constraint)) {
                table_elements.push(constraint.clone());
            }
        } else {
            table_options.push(constraint.clone());
        }
    }

    let mut sql = format!("CREATE TABLE {} ({})", name, table_elements.join(", "));
    if !table_options.is_empty() {
        sql.push(' ');
        sql.push_str(&table_options.join(" "));
    }
    sql
}

fn sync_column_type_with_foreign_key(
    column: &str,
    ty: &str,
    foreign_keys: &[SingleColumnForeignKey],
) -> String {
    if normalize_schema_type(ty)
        .to_ascii_uppercase()
        .contains(" REFERENCES ")
    {
        return ty.to_string();
    }
    let Some(fk) = foreign_keys.iter().find(|fk| fk.child_column == column) else {
        return ty.to_string();
    };
    format!(
        "{} REFERENCES {}({})",
        ty, fk.parent_table, fk.parent_column
    )
}

fn sync_constraint_is_table_element(constraint: &str) -> bool {
    let upper = normalize_schema_type(constraint).to_ascii_uppercase();
    upper.starts_with("UNIQUE") || upper.starts_with("FOREIGN KEY")
}

fn schema_clause_key(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .flat_map(|ch| ch.to_uppercase())
        .collect()
}

fn ddl_vector_dimension(value: &str) -> Option<usize> {
    let upper = value.to_ascii_uppercase();
    let start = upper.find("VECTOR(")? + "VECTOR(".len();
    let end = upper[start..].find(')')? + start;
    upper[start..end].trim().parse().ok()
}

fn ddl_dag_edge_types(constraints: &[String]) -> Vec<EdgeType> {
    let mut edge_types = Vec::new();
    for constraint in constraints {
        let upper = constraint.to_ascii_uppercase();
        let Some(dag_start) = upper.find("DAG") else {
            continue;
        };
        let Some(paren_offset) = upper[dag_start..].find('(') else {
            continue;
        };
        let values_start = dag_start + paren_offset + 1;
        let Some(end_offset) = upper[values_start..].find(')') else {
            continue;
        };
        let values_end = values_start + end_offset;
        for raw in constraint[values_start..values_end].split(',') {
            let edge_type = raw
                .trim()
                .trim_matches(|ch: char| ch == '\'' || ch == '"' || ch.is_whitespace());
            if !edge_type.is_empty() {
                edge_types.push(edge_type.to_string());
            }
        }
    }

    edge_types
}

fn ddl_unique_constraints(constraints: &[String]) -> Vec<Vec<String>> {
    constraints
        .iter()
        .filter_map(|constraint| {
            let upper = normalize_schema_type(constraint).to_ascii_uppercase();
            if !upper.starts_with("UNIQUE") {
                return None;
            }
            let start = constraint.find('(')? + 1;
            let end = constraint[start..].find(')')? + start;
            let columns = constraint[start..end]
                .split(',')
                .map(|column| column.trim().to_string())
                .filter(|column| !column.is_empty())
                .collect::<Vec<_>>();
            (!columns.is_empty()).then_some(columns)
        })
        .collect()
}

fn ddl_column_reference(ty: &str) -> Option<ForeignKeyReference> {
    let normalized = normalize_schema_type(ty);
    let upper = normalized.to_ascii_uppercase();
    let reference_start = upper.find("REFERENCES ")? + "REFERENCES ".len();
    let rest = normalized[reference_start..].trim();
    let paren = rest.find('(')?;
    let close = rest[paren + 1..].find(')')? + paren + 1;
    let table = rest[..paren].trim();
    let column = rest[paren + 1..close].trim();
    if table.is_empty() || column.is_empty() {
        return None;
    }
    Some(ForeignKeyReference {
        table: table.to_string(),
        column: column.to_string(),
    })
}

fn sync_table_shape_matches(
    local_meta: &TableMeta,
    remote_columns: &[(String, String)],
    remote_constraints: &[String],
    remote_foreign_keys: &[SingleColumnForeignKey],
    remote_composite_foreign_keys: &[CompositeForeignKey],
    remote_composite_unique: &[Vec<String>],
) -> bool {
    let remote_meta = rough_sync_table_meta(
        remote_columns,
        remote_constraints,
        remote_foreign_keys,
        remote_composite_foreign_keys,
        remote_composite_unique,
    );

    descriptor_multiset(single_column_foreign_keys_from_meta(
        local_meta,
        &HashSet::new(),
    )) == descriptor_multiset(single_column_foreign_keys_from_meta(
        &remote_meta,
        &HashSet::new(),
    )) && descriptor_multiset(local_meta.composite_foreign_keys.clone())
        == descriptor_multiset(remote_meta.composite_foreign_keys)
        && descriptor_multiset(local_meta.unique_constraints.clone())
            == descriptor_multiset(remote_meta.unique_constraints)
}

fn descriptor_multiset<T>(mut values: Vec<T>) -> Vec<T>
where
    T: Ord,
{
    values.sort();
    values
}

fn rough_sync_table_meta(
    columns: &[(String, String)],
    constraints: &[String],
    foreign_keys: &[SingleColumnForeignKey],
    composite_foreign_keys: &[CompositeForeignKey],
    composite_unique: &[Vec<String>],
) -> TableMeta {
    let mut column_defs = columns
        .iter()
        .map(|(name, ty)| rough_sync_column_def(name, ty))
        .collect::<Vec<_>>();
    for fk in foreign_keys {
        if let Some(column) = column_defs
            .iter_mut()
            .find(|column| column.name == fk.child_column)
        {
            column.references = Some(ForeignKeyReference {
                table: fk.parent_table.clone(),
                column: fk.parent_column.clone(),
            });
        }
    }
    let mut unique_constraints = ddl_unique_constraints(constraints);
    for unique in composite_unique {
        if !unique_constraints
            .iter()
            .any(|candidate| candidate == unique)
        {
            unique_constraints.push(unique.clone());
        }
    }

    let mut meta = TableMeta {
        columns: column_defs,
        immutable: constraints
            .iter()
            .any(|constraint| constraint.eq_ignore_ascii_case("IMMUTABLE")),
        state_machine: None,
        dag_edge_types: ddl_dag_edge_types(constraints),
        unique_constraints,
        natural_key_column: None,
        propagation_rules: Vec::new(),
        default_ttl_seconds: None,
        sync_safe: false,
        expires_column: None,
        indexes: Vec::new(),
        composite_foreign_keys: composite_foreign_keys.to_vec(),
    };
    meta.indexes = crate::executor::auto_indexes_for_table_meta(&meta);
    meta
}

fn rough_sync_column_def(name: &str, ty: &str) -> ColumnDef {
    let upper = normalize_schema_type(ty).to_ascii_uppercase();
    let primary_key = upper.contains("PRIMARY KEY");
    let unique = upper.contains("UNIQUE");
    let expires = upper.contains("EXPIRES");
    let immutable = upper.contains("IMMUTABLE");
    let quantization = if upper.contains("SQ4") {
        VectorQuantization::SQ4
    } else if upper.contains("SQ8") {
        VectorQuantization::SQ8
    } else {
        VectorQuantization::F32
    };
    let column_type = if let Some(dimension) = ddl_vector_dimension(ty) {
        ColumnType::Vector(dimension)
    } else if upper.starts_with("UUID") {
        ColumnType::Uuid
    } else if upper.starts_with("TEXT") {
        ColumnType::Text
    } else if upper.starts_with("INTEGER") || upper.starts_with("INT") {
        ColumnType::Integer
    } else if upper.starts_with("REAL") || upper.starts_with("FLOAT") || upper.starts_with("DOUBLE")
    {
        ColumnType::Real
    } else if upper.starts_with("BOOLEAN") || upper.starts_with("BOOL") {
        ColumnType::Boolean
    } else if upper.starts_with("TIMESTAMP") {
        ColumnType::Timestamp
    } else if upper.starts_with("TXID") {
        ColumnType::TxId
    } else if upper.starts_with("JSON") {
        ColumnType::Json
    } else {
        ColumnType::Text
    };

    ColumnDef {
        name: name.to_string(),
        column_type,
        nullable: !primary_key && !upper.contains("NOT NULL"),
        primary_key,
        unique,
        default: None,
        references: ddl_column_reference(ty),
        expires,
        immutable,
        quantization,
        rank_policy: None,
        context_id: upper.contains("CONTEXT_ID"),
        scope_label: None,
        acl_ref: None,
    }
}

fn sync_projected_edge_has_path(
    adjacency: &HashMap<NodeId, HashSet<NodeId>>,
    start: NodeId,
    goal: NodeId,
) -> bool {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    visited.insert(start);
    queue.push_back(start);

    while let Some(current) = queue.pop_front() {
        let Some(targets) = adjacency.get(&current) else {
            continue;
        };
        for target in targets {
            if *target == goal {
                return true;
            }
            if visited.insert(*target) {
                queue.push_back(*target);
            }
        }
    }

    false
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

    for fk in &ct.composite_foreign_keys {
        constraints.push(format!(
            "FOREIGN KEY ({}) REFERENCES {}({})",
            fk.child_columns.join(", "),
            fk.parent_table,
            fk.parent_columns.join(", ")
        ));
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

    for fk in &meta.composite_foreign_keys {
        constraints.push(format!(
            "FOREIGN KEY ({}) REFERENCES {}({})",
            fk.child_columns.join(", "),
            fk.parent_table,
            fk.parent_columns.join(", ")
        ));
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

#[cfg(test)]
mod retention_prune_persistence_tests {
    use super::*;
    use tempfile::TempDir;

    fn params() -> HashMap<String, Value> {
        HashMap::new()
    }

    fn visible_rows(db: &Database, table: &str) -> usize {
        db.execute(&format!("SELECT * FROM {table}"), &params())
            .unwrap()
            .rows
            .len()
    }

    fn db_with_expired_row() -> Database {
        let tmp = TempDir::new().unwrap();
        let path = tmp.keep().join("retention-persist-failure.db");
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE obs (id INTEGER PRIMARY KEY, note TEXT) RETAIN 1 SECONDS",
            &params(),
        )
        .unwrap();
        db.execute("INSERT INTO obs (id, note) VALUES (1, 'old')", &params())
            .unwrap();
        std::thread::sleep(Duration::from_secs(2));
        db
    }

    #[test]
    fn checked_prune_reports_persistence_failure_without_mutating_memory() {
        let db = db_with_expired_row();

        db.persistence.as_ref().unwrap().close();
        let err = db.run_pruning_cycle_checked().unwrap_err();

        assert!(
            err.to_string().contains("database persistence is closed"),
            "checked pruning must surface persistence rewrite failure; got {err:?}"
        );
        assert_eq!(
            visible_rows(&db, "obs"),
            1,
            "failed persistence must leave in-memory rows unchanged for retry"
        );
    }

    #[test]
    fn compatibility_prune_keeps_memory_when_persistence_fails() {
        let db = db_with_expired_row();

        db.persistence.as_ref().unwrap().close();
        let pruned = db.run_pruning_cycle();

        assert_eq!(
            pruned, 0,
            "compatibility pruning cannot surface errors and must report no durable prune"
        );
        assert_eq!(
            visible_rows(&db, "obs"),
            1,
            "failed persistence must leave in-memory rows unchanged for retry"
        );
    }
}
