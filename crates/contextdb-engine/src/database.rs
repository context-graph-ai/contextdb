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
use contextdb_tx::{
    RelationalDeletePredicate, TxManager, WriteSet, WriteSetApplicator,
    row_matches_delete_predicates,
};
use contextdb_vector::{
    HnswGraphStats, HnswIndex, MemVectorExecutor, VectorSearchDebugTrace, VectorStore,
    cosine_similarity,
};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, Condvar, Mutex, RwLock};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

type DynStore = Box<dyn WriteSetApplicator>;
type GatedBfsEntry = (NodeId, u32, Vec<(NodeId, EdgeType)>);
type GatedGraphNeighbor = (NodeId, EdgeType, HashMap<String, Value>, NodeId, NodeId);
const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 1024;
const MAX_STATEMENT_CACHE_ENTRIES: usize = 1024;
const SAME_PROCESS_REOPEN_RETRY: Duration = Duration::from_millis(500);
const CROSS_PROCESS_LOCK_SETTLE_RETRY: Duration = Duration::from_millis(250);
const DEFAULT_TRIGGER_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(60);
// redb may need a small metadata page on the next write, especially for a new
// file with the format metadata table. Keep the disk-limit error deterministic
// instead of starting a write that cannot commit cleanly.
const MIN_DISK_WRITE_HEADROOM_BYTES: u64 = 1024;
// Checkpoint export streams per-category batches so peak extra memory stays
// proportional to a batch, never a second whole-table copy.
const EXPORT_BATCH_SIZE: usize = 1024;
const EXPORT_NODE_BATCH_SIZE: usize = 256;

mod cron;
pub(crate) mod event_bus;
pub(crate) mod gate;
pub(crate) mod trigger;
use cron::CronState;
use event_bus::EventBusState;
use trigger::{TriggerCallbackThreadGuard, TriggerContention, TriggerState};

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
    pub query_vector_source: Option<contextdb_core::types::VectorIndexRef>,
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

/// Report returned by [`Database::export_snapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExportReport {
    pub snapshot_lsn: Lsn,
    pub rows: u64,
    pub edges: u64,
    pub vectors: u64,
    pub bytes_written: u64,
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
    /// Count of queue entries the sink dispatcher has pulled to the front and
    /// evaluated the per-registration access gate for — whether the entry was
    /// then delivered, permanently failed, or gate-denied and rotated back.
    ///
    /// This is ALWAYS-ON (not a test-only build): for admitted batches the
    /// dispatcher folds the increment into the SAME single post-batch metrics
    /// write that already records `delivered` / `permanent_failures` (zero
    /// additional lock acquisitions on the delivery path); only a denied-entry
    /// rotation pays its own acquisition (a scoped consumer that cannot see
    /// some queued events, throttled by the dispatcher's own 10 ms rotation
    /// wait). The cost is negligible
    /// and the field lets a caller prove the dispatcher has completed a full
    /// sweep of the queue — i.e. every gate-denied entry was examined and
    /// rejected, not merely not-yet-processed. Tests read it to make
    /// "denied events are never delivered" a state assertion instead of a sleep.
    pub examined: u64,
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
    static CRON_CALLBACK_VECTOR_SCHEMA_GATE: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static CRON_CALLBACK_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static TRIGGER_CALLBACK_TX: std::cell::Cell<Option<TxId>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_CALLBACK_DB: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_CALLBACK_VECTOR_SCHEMA_GATE: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_CALLBACK_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static TRIGGER_CALLBACK_NAME: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
    static TRIGGER_CALLBACK_WALLCLOCK: std::cell::Cell<Option<Wallclock>> =
        const { std::cell::Cell::new(None) };
    static TRIGGER_INSERT_STATE_MACHINE_CACHE: std::cell::RefCell<HashMap<usize, HashMap<String, bool>>> =
        std::cell::RefCell::new(HashMap::new());
    static USER_COMMIT_ACTIVE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static USER_COMMIT_TRIGGER_REENTRY: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH: std::cell::Cell<u32> =
        const { std::cell::Cell::new(0) };
    static SQL_WRITE_CONTROL_BYPASS_STACK: std::cell::RefCell<Vec<(usize, TxId)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static DB_OPERATION_STACK: std::cell::RefCell<Vec<usize>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static VECTOR_SCHEMA_READ_STACK: std::cell::RefCell<Vec<(usize, VectorIndexRef)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

struct SyncApplyTriggerGateGuard;

struct SqlWriteControlBypassGuard {
    db_id: usize,
    tx: TxId,
}

#[derive(Default)]
struct VectorSchemaGates {
    gates: Mutex<HashMap<VectorIndexRef, Arc<RwLock<()>>>>,
    epochs: Mutex<HashMap<VectorIndexRef, u64>>,
}

impl VectorSchemaGates {
    fn sorted_refs(refs: impl IntoIterator<Item = VectorIndexRef>) -> Vec<VectorIndexRef> {
        let mut refs = refs.into_iter().collect::<Vec<_>>();
        refs.sort_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
        refs.dedup();
        refs
    }

    fn gate_for(&self, index: &VectorIndexRef) -> Arc<RwLock<()>> {
        self.gates
            .lock()
            .entry(index.clone())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    fn epoch_for(&self, index: &VectorIndexRef) -> u64 {
        self.epochs.lock().get(index).copied().unwrap_or(0)
    }

    fn bump_epochs(&self, refs: &[VectorIndexRef]) {
        let mut epochs = self.epochs.lock();
        for index in refs {
            let epoch = epochs.entry(index.clone()).or_insert(0);
            *epoch = epoch.saturating_add(1);
        }
    }
}

pub(crate) struct VectorSchemaReadGuard {
    db_id: usize,
    refs: Vec<VectorIndexRef>,
    _guards: Vec<ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
}

impl VectorSchemaReadGuard {
    fn new(
        db_id: usize,
        refs: Vec<VectorIndexRef>,
        guards: Vec<ArcRwLockReadGuard<parking_lot::RawRwLock, ()>>,
    ) -> Self {
        VECTOR_SCHEMA_READ_STACK.with(|stack| {
            stack
                .borrow_mut()
                .extend(refs.iter().cloned().map(|index| (db_id, index)));
        });
        Self {
            db_id,
            refs,
            _guards: guards,
        }
    }
}

impl Drop for VectorSchemaReadGuard {
    fn drop(&mut self) {
        VECTOR_SCHEMA_READ_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            for expected in self.refs.iter().rev() {
                let popped = stack.pop();
                debug_assert_eq!(popped, Some((self.db_id, expected.clone())));
            }
        });
    }
}

pub(crate) struct VectorSchemaWriteGuard {
    _guards: Vec<ArcRwLockWriteGuard<parking_lot::RawRwLock, ()>>,
}

impl Drop for SyncApplyTriggerGateGuard {
    fn drop(&mut self) {
        SYNC_APPLY_TRIGGER_GATE_BYPASS_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

impl Drop for SqlWriteControlBypassGuard {
    fn drop(&mut self) {
        SQL_WRITE_CONTROL_BYPASS_STACK.with(|stack| {
            let popped = stack.borrow_mut().pop();
            debug_assert_eq!(
                popped,
                Some((self.db_id, self.tx)),
                "SQL write-control bypass stack mismatch"
            );
        });
    }
}

struct UserCommitCallbackGuard {
    prior_active: bool,
    prior_reentry: bool,
}

impl Drop for UserCommitCallbackGuard {
    fn drop(&mut self) {
        USER_COMMIT_TRIGGER_REENTRY.with(|slot| slot.set(self.prior_reentry));
        USER_COMMIT_ACTIVE.with(|slot| slot.set(self.prior_active));
    }
}

fn global_callback_active_count() -> &'static AtomicUsize {
    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    &ACTIVE
}

/// Embedded contextdb database handle.
///
/// # Store ownership and concurrency
///
/// A database file is opened by exactly one owner at a time. A second open of
/// the same path — whether from another thread in this process or from a
/// separate process — returns [`Error::DatabaseLocked`]; there is no read-only,
/// shared, or replica open. Single-writer ownership is a deliberate guarantee of
/// the substrate, not a missing feature, and it is enforced at two layers (an
/// in-process open registry and an on-disk PID/OS file lock).
///
/// Embed it the way you embed any single-writer store (SQLite, LMDB, redb): one
/// process opens the handle and keeps it for its lifetime, and every read and
/// write — including answering queries on behalf of other parts of your
/// application — goes through that one owner. For a long-running service this
/// means the process that holds the handle serves its own reads: a second
/// command reads by asking the running owner, not by re-opening the file, and
/// you never keep a parallel copy of the data outside the owner to work around
/// the lock. See the "Store Ownership & Concurrency" section of
/// `docs/architecture.md`.
///
/// Trigger concurrency follows the canonical callback contract documented on
/// [`Error::CallbackActiveCrossThread`]: same-DB cross-thread trigger
/// contention waits and proceeds inside the engine, unrelated databases proceed
/// independently, same-thread callback reentry returns
/// [`Error::CallbackReentry`], callback tx-bound handles are isolated to the
/// runner thread, and cron same-DB contention still returns the typed cron
/// callback-active error immediately.
///
/// # Drop semantics
///
/// Trigger waiters do not park while holding the public-operation read guard,
/// so `close()` can win the closed-handle transition after an active callback
/// exits. Production callers should call [`Database::close`] explicitly before
/// dropping the last handle when deterministic shutdown matters; a waiter that
/// wakes after close observes the normal closed-handle error.
pub struct Database {
    tx_mgr: Arc<TxManager<DynStore>>,
    relational_store: Arc<RelationalStore>,
    graph_store: Arc<GraphStore>,
    vector_store: Arc<VectorStore>,
    vector_schema_gates: Arc<VectorSchemaGates>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    persistence: Option<Arc<RedbPersistence>>,
    open_registry_path: Mutex<Option<PathBuf>>,
    operation_gate: Arc<RwLock<()>>,
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
    /// The ONE hub this database's retained tables are delivered to, loaded
    /// from database metadata at open so a restart cannot let a second hub
    /// claim first place.
    retention_sync_peer: Mutex<Option<String>>,
    cron: Arc<CronState>,
    event_bus: Arc<EventBusState>,
    trigger: Arc<TriggerState>,
    sync_relay_mode: Arc<AtomicBool>,
    in_memory_applied_push_watermarks: Arc<Mutex<HashMap<String, Lsn>>>,
    pending_event_bus_ddl: Mutex<HashMap<TxId, Vec<DdlChange>>>,
    pending_commit_metadata: Mutex<HashMap<TxId, PendingCommitMetadata>>,
    disk_limit: AtomicU64,
    disk_limit_startup_ceiling: AtomicU64,
    sync_watermark: Arc<AtomicLsn>,
    closed: AtomicBool,
    resource_closed: Arc<AtomicBool>,
    rows_examined: AtomicU64,
    last_vector_search_used_hnsw: AtomicBool,
    last_vector_search_trace: RwLock<Option<VectorSearchDebugTrace>>,
    statement_cache: RwLock<HashMap<String, Arc<CachedStatement>>>,
    rank_formula_cache: RwLock<HashMap<(String, String), Arc<RankFormula>>>,
    acl_grant_cache: RwLock<HashMap<AclGrantCacheKey, Arc<HashSet<uuid::Uuid>>>>,
    rank_policy_eval_count: AtomicU64,
    rank_policy_formula_parse_count: AtomicU64,
    fk_indexed_tuple_probes: AtomicU64,
    fk_full_scan_fallbacks: AtomicU64,
    commit_rows_validated: AtomicU64,
    commit_indexed_probes: AtomicU64,
    commit_staged_vs_staged_comparisons: AtomicU64,
    commit_scan_rows_touched: AtomicU64,
    commit_index_maintenance_visits: AtomicU64,
    #[cfg(feature = "test-seams")]
    commit_stage_wall_nanos: [AtomicU64; 7],
    corrupt_joined_values: RwLock<HashSet<(String, RowId, String)>>,
    resource_owner: bool,
}

/// Steady-state snapshot of trigger-progress telemetry counters. The
/// accessor is `#[doc(hidden)] pub` so integration tests (which compile
/// against the released crate signature, not `cfg(test)`) can read it,
/// while it remains absent from rustdoc and unsupported as a public API.
/// cg observes throughput via `Store::record_observation`, not via these
/// counters.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TriggerProgressTelemetrySnapshot {
    pub wait_observed: u64,
    /// Engine-internal "retry-eligible refusal" count for same-DB B2. Under
    /// the wait-and-proceed contract this stays 0 under healthy contention;
    /// it is positive only when the deadlock-guard timeout regime surfaces
    /// the same-DB typed Err.
    pub typed_err_observed_same_db: u64,
    pub deadlock_guard_timeout_observed: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AccessConstraints {
    contexts: Option<BTreeSet<ContextId>>,
    scope_labels: Option<BTreeSet<ScopeLabel>>,
    principal: Option<Principal>,
}

fn narrowed_constraint_set<T: Ord + Clone>(
    parent: &Option<BTreeSet<T>>,
    child: Option<BTreeSet<T>>,
) -> Option<BTreeSet<T>> {
    match (parent, child) {
        (Some(parent), Some(child)) => Some(parent.intersection(&child).cloned().collect()),
        (Some(parent), None) => Some(parent.clone()),
        (None, child) => child,
    }
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

pub(crate) struct UpdateReplacementContext<'a> {
    pub(crate) meta: &'a TableMeta,
    pub(crate) committed_row_exists: bool,
    pub(crate) created_at: Wallclock,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct DeleteReleaseBytes {
    relational: Vec<usize>,
    edges: Vec<usize>,
    vectors: Vec<usize>,
}

#[derive(Debug, Default, Clone)]
struct PendingCommitMetadata {
    conditional_update_guards: Vec<PendingConditionalUpdateGuard>,
    upsert_intents: Vec<PendingUpsertIntent>,
    vector_schema_epochs: HashMap<VectorIndexRef, u64>,
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
    fail_on_conflict: bool,
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

struct CommitTimeUpsertLookup<'a> {
    intents: &'a [PendingUpsertIntent],
    by_row: HashMap<(TableName, RowId), Vec<usize>>,
}

#[derive(Debug, Default)]
struct CommitValidationOutcome {
    conditional_noop_count: u64,
    conditional_conflict_count: u64,
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

struct TriggerCloseWaiterGuard<'a> {
    trigger: &'a TriggerState,
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

impl<'a> TriggerCloseWaiterGuard<'a> {
    fn new(trigger: &'a TriggerState) -> Self {
        trigger.close_waiter_count.fetch_add(1, Ordering::SeqCst);
        Self { trigger }
    }
}

impl Drop for TriggerCloseWaiterGuard<'_> {
    fn drop(&mut self) {
        self.trigger
            .close_waiter_count
            .fetch_sub(1, Ordering::SeqCst);
        self.trigger.waiters.notify_all();
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
    let cross_process_retry_deadline = Instant::now() + CROSS_PROCESS_LOCK_SETTLE_RETRY;

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
            Err(Error::DatabaseLocked { holder_pid, path })
                if holder_pid != std::process::id()
                    && path == canonical_path
                    && Instant::now() < cross_process_retry_deadline =>
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
    // Wake-cycles STARTED by the current maintenance thread (incremented on
    // each wake, before the pending gate). Test-build only — production
    // carries neither the field nor the increment; the unit tests poll it as
    // the liveness half of a state-based wait.
    #[cfg(test)]
    wakes: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PruningReport {
    pub pruned_rows: u64,
    pub blocked_count: u64,
    pub blocked: Vec<String>,
    /// An ESTIMATE of the live bytes the pruned row versions — and the vectors
    /// and edges that went with them — accounted for, on the same estimator
    /// [`TableSizeEstimate`] uses. It tracks the SIZE of what left, so two
    /// populations with the same row count and different payloads report
    /// materially different reclaim, but it is an accounting figure and NOT
    /// the number of bytes handed back to the filesystem: the estimator
    /// deliberately over-counts large values, and freed pages are reused in
    /// place unless a compaction runs. `file_bytes_before` / `file_bytes_after`
    /// carry the physical truth.
    pub reclaimed_bytes: u64,
    /// The database file's size before and after this cycle; `None` for an
    /// in-memory database, which has no file.
    pub file_bytes_before: Option<u64>,
    pub file_bytes_after: Option<u64>,
    /// Whether the file itself actually got smaller. Freed pages are reused in
    /// place, so a prune that does not compact frees space WITHOUT shrinking
    /// the file — this field says which happened.
    pub file_shrank: bool,
    /// Whether this cycle ran a full storage compaction.
    pub compacted: bool,
    /// The storage layer's observed dead-space fraction as it stood BEFORE
    /// this cycle pruned anything — accumulated waste that survived page reuse
    /// from earlier cycles, not the space this prune just released. It is the
    /// value the compaction decision was taken on, so a cycle that pruned rows
    /// compacted exactly when this reached
    /// [`REDB_COMPACT_FRAGMENTATION_THRESHOLD`]. A cycle that pruned NOTHING
    /// never compacts, whatever this reads.
    pub fragmentation_before: f64,
    /// Rows carrying a timestamp further into the future than
    /// [`RETENTION_CLOCK_SKEW_TOLERANCE`]. They are never pruned early — and
    /// never silently pinned either: every cycle they stay future-dated counts
    /// them here and names their table below.
    pub future_dated_rows: u64,
    pub future_dated_tables: Vec<String>,
    /// Commit-index entries this cycle removed. The index holds one entry per
    /// commit and nothing used to remove them, so it grew with every write
    /// forever; retention now trims it to what a consumer can still name.
    pub pruned_commit_index_entries: u64,
}

/// What the engine-owned maintenance loop is doing for this database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MaintenanceStatus {
    /// Exactly `active_maintenance_loops > 0`.
    pub running: bool,
    /// This database holds at least one table that declares RETAIN.
    pub retention_enabled: bool,
    /// This database holds at least one currency table (see
    /// [`VERSION_COMPACTION_TABLES`]).
    pub currency_compaction_enabled: bool,
    /// Loops spawned for this database. Both jobs share ONE loop, so this is
    /// 1 whenever anything is maintained and 0 when nothing is.
    pub active_maintenance_loops: usize,
}

/// Everything one maintenance cycle did: generic retention, currency version
/// compaction, and the engine's own durable trigger-audit retention.
#[derive(Debug, Clone, Default)]
pub struct MaintenanceReport {
    pub pruning: PruningReport,
    pub currency: CurrencyCompactionReport,
    pub pruned_trigger_audit_rows: u64,
}

/// An honest per-table size answer: an ESTIMATE of the table's live bytes plus
/// its exact row count, with the whole file's size reported ALONGSIDE as
/// itself. The whole-file number is never divided between tables — several
/// tables share one file, and attributing physical bytes to one of them would
/// be a claim the engine cannot make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSizeEstimate {
    pub table: String,
    pub estimated_live_bytes: u64,
    pub row_count: u64,
    pub whole_file_bytes: Option<u64>,
}

/// Whether a maintenance cycle's currency pass runs unconditionally (an
/// explicit call) or behind the cheap superseded-version gate (a scheduled
/// tick, so a quiet node never pays the commit lock).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurrencyGate {
    Always,
    Scheduled,
}

/// The engine-owned pieces one maintenance cycle needs, cloned out of the
/// `Database` handle so the SAME cycle code runs whether a caller drives it
/// synchronously (`Database::run_maintenance_cycle`) or the engine's own loop
/// dispatches it on a tick.
pub(crate) struct MaintenanceContext {
    relational: Arc<RelationalStore>,
    graph: Arc<GraphStore>,
    vector: Arc<VectorStore>,
    accountant: Arc<MemoryAccountant>,
    persistence: Option<Arc<RedbPersistence>>,
    change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    trigger: Arc<trigger::TriggerState>,
    tx_mgr: Arc<TxManager<DynStore>>,
    sync_watermark: Arc<AtomicLsn>,
    trigger_audit_retention: Duration,
}

impl MaintenanceContext {
    fn file_bytes(&self) -> Option<u64> {
        self.persistence
            .as_ref()
            .and_then(|persistence| std::fs::metadata(persistence.path()).ok())
            .map(|meta| meta.len())
    }

    /// Generic retention, then — once the rows are gone — the compaction
    /// decision on the SAME conservative threshold the currency path uses.
    /// True when any table declares RETAIN. The prune deep-clones every table's
    /// rows inside the commit lock, so on a database that retains NOTHING —
    /// which is most of them — that whole pass is pure cost with no possible
    /// effect. The cheap gate the currency pass has always had never covered
    /// this path.
    fn has_retention_work(&self) -> bool {
        self.relational
            .table_meta
            .read()
            .values()
            .any(|meta| meta.default_ttl_seconds.is_some())
    }

    fn run_pruning(&self) -> Result<PruningReport> {
        // Nothing declares RETAIN, so there is nothing this pass could prune,
        // no fragmentation decision it could take, and no report field it could
        // fill. Bail BEFORE the commit lock and before the table clone.
        if !self.has_retention_work() {
            return Ok(PruningReport::default());
        }
        let file_bytes_before = self.file_bytes();
        // Sampled BEFORE the prune, deliberately. redb counts just-freed pages
        // as fragmented, so a reading taken AFTER a prune is dominated by the
        // pages that prune released — it measures the prune, not the file, and
        // reads high whatever fraction was removed. The pre-prune sample
        // measures the thing the shared threshold is written about: waste that
        // SURVIVED page reuse across earlier cycles. One consequence is
        // deliberate: a large prune of a previously tight file may not compact
        // until the next cycle, which is the rare-not-per-cycle posture the
        // constant's own doc describes.
        let fragmentation_before = self
            .persistence
            .as_ref()
            .map(|persistence| persistence.fragmentation_ratio().unwrap_or(0.0))
            .unwrap_or(0.0);
        let mut report = self.tx_mgr.with_commit_lock(|| {
            checked_prune_expired_rows(self, self.sync_watermark.load(Ordering::SeqCst))
        })?;
        report.file_bytes_before = file_bytes_before;
        report.fragmentation_before = fragmentation_before;
        // Compaction itself still runs AFTER the prune (there is no point
        // rewriting the file before the rows leave it) and OUTSIDE the commit
        // lock: redb compaction takes the redb mutex with `&mut` and must not
        // run while a txn is outstanding on this thread. Only the DECISION
        // rides the pre-prune observation above, and the reported
        // `fragmentation_before` is that same observed value — the decision
        // input and the receipt are one number, never a derived stand-in.
        if report.pruned_rows > 0
            && let Some(persistence) = self.persistence.as_ref()
            && fragmentation_before >= REDB_COMPACT_FRAGMENTATION_THRESHOLD
        {
            persistence.compact()?;
            report.compacted = true;
        }
        report.file_bytes_after = self.file_bytes();
        report.file_shrank = matches!(
            (report.file_bytes_before, report.file_bytes_after),
            (Some(before), Some(after)) if after < before
        );
        Ok(report)
    }

    fn run_currency(&self, gate: CurrencyGate) -> Result<CurrencyCompactionReport> {
        if gate == CurrencyGate::Scheduled
            && !currency_compaction_pending(
                &self.relational,
                VERSION_COMPACTION_TABLES,
                CURRENCY_COMPACTION_SUPERSEDED_THRESHOLD,
            )
        {
            return Ok(CurrencyCompactionReport::default());
        }
        let mut report = self.tx_mgr.with_commit_lock(|| {
            compact_currency_versions_inner(
                &self.relational,
                &self.graph,
                &self.vector,
                self.accountant.as_ref(),
                self.persistence.as_ref(),
                &self.change_log,
                VERSION_COMPACTION_TABLES,
            )
        })?;
        if report.pruned_versions > 0
            && let Some(persistence) = self.persistence.as_ref()
            && persistence.fragmentation_ratio().unwrap_or(0.0)
                >= REDB_COMPACT_FRAGMENTATION_THRESHOLD
        {
            persistence.compact()?;
            report.redb_compacted = true;
        }
        Ok(report)
    }

    /// The engine's own durable trigger-audit history is the first internal
    /// consumer of retention: entries older than the shipped default window
    /// age out, including entries a DROP TABLE orphaned. The in-memory ring
    /// keeps its own bounded semantics and is never touched here.
    fn run_trigger_audit_retention(&self) -> Result<u64> {
        let now = Wallclock::now();
        let cutoff = now
            .0
            .saturating_sub(self.trigger_audit_retention.as_millis() as u64);
        match self.persistence.as_ref() {
            Some(persistence) => persistence.prune_trigger_audit_history(cutoff),
            None => {
                let mut history = self.trigger.volatile_audit_history.lock();
                let before = history.len();
                history.retain(|(stamped_at, _)| stamped_at.0 >= cutoff);
                Ok((before - history.len()) as u64)
            }
        }
    }

    fn run_cycle(&self, gate: CurrencyGate) -> Result<MaintenanceReport> {
        let pruning = self.run_pruning()?;
        let pruned_trigger_audit_rows = self.run_trigger_audit_retention()?;
        let currency = self.run_currency(gate)?;
        Ok(MaintenanceReport {
            pruning,
            currency,
            pruned_trigger_audit_rows,
        })
    }
}

/// Observability: an operator must be able to see maintenance happen. Silent
/// when a tick reclaimed nothing.
fn log_maintenance_cycle(report: &MaintenanceReport) {
    if report.pruning.pruned_rows == 0
        && report.currency.pruned_versions == 0
        && report.pruned_trigger_audit_rows == 0
        && report.pruning.future_dated_rows == 0
    {
        return;
    }
    println!(
        "maintenance_cycle pruned_rows={} reclaimed_bytes={} compacted={} file_shrank={} \
         future_dated_rows={} future_dated_tables={} trigger_audit_rows={} \
         currency_versions={} currency_redb_compacted={}",
        report.pruning.pruned_rows,
        report.pruning.reclaimed_bytes,
        report.pruning.compacted,
        report.pruning.file_shrank,
        report.pruning.future_dated_rows,
        report.pruning.future_dated_tables.join(","),
        report.pruned_trigger_audit_rows,
        report.currency.pruned_versions,
        report.currency.redb_compacted,
    );
}

/// Database-metadata key holding the ONE hub a retained table is delivered to.
pub(crate) const RETENTION_SYNC_PEER_CONFIG_KEY: &str = "retention_sync_peer";

/// High-churn "currency" tables whose rows are rewritten every poll cadence via
/// `INSERT … ON CONFLICT DO UPDATE`. Each update mints a new MVCC version and
/// tombstones the prior one, and nothing ever reclaims the superseded versions,
/// so a long-running fleet node accumulates hundreds of thousands of physical
/// versions for a handful of live rows (observed: 248,996 `work_capabilities`
/// versions in a 365MB debris ledger for ~10 live rows). Version compaction
/// (`Database::compact_currency_versions`) collapses each logical row back to
/// its single current version for exactly these tables. They are all
/// `LatestWins` (or immutable-key) under sync, so only the latest version has
/// any consumer value — see the sync-safety argument on `compact_currency_versions`.
pub(crate) const VERSION_COMPACTION_TABLES: &[&str] =
    &["work_capabilities", "work_node_contacts", "peer_directory"];

/// Reclaim freed pages with a full redb `compact()` after a version-compaction
/// pass once at least this fraction of the file is dead space. A first pass over
/// an accumulated debris ledger frees most of the file and trips this; steady
/// state stays well below it (freed pages are reused in place), so compaction is
/// rare, not per-cycle.
pub const REDB_COMPACT_FRAGMENTATION_THRESHOLD: f64 = 0.5;

/// A worker whose clock runs ahead of the holder's stamps its rows in the
/// future. Drift within this much is ordinary fleet skew: the row ages
/// normally and is never reported. Beyond it the stamp is an operator event —
/// the row is never deleted early, and every maintenance cycle counts it and
/// names its table until the clocks agree again.
pub const RETENTION_CLOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(5 * 60);

/// The shipped default retention for the engine's OWN durable trigger-audit
/// history, applied with zero consumer configuration. Generous enough that a
/// real operator still has a week of firing history to read, bounded enough
/// that a trigger-heavy node cannot grow the table without limit. Audit rows
/// orphaned by a DROP TABLE age out on the same window.
pub(crate) const TRIGGER_AUDIT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The engine-owned maintenance loop ticks at this cadence. One loop serves
/// both jobs: its currency pass is gated by
/// [`CURRENCY_COMPACTION_SUPERSEDED_THRESHOLD`], so a quiet node does near-zero
/// work, and only a tick that actually did something prints a receipt line.
pub(crate) const MAINTENANCE_TICK: Duration = Duration::from_secs(60);

/// A maintenance tick compacts only once superseded versions across the eligible
/// tables reach this count, so steady-state churn below it never pays the
/// commit-lock/rewrite cost.
pub(crate) const CURRENCY_COMPACTION_SUPERSEDED_THRESHOLD: usize = 64;

/// Outcome of one `compact_currency_versions` pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CurrencyCompactionReport {
    /// Superseded relational row versions physically removed.
    pub pruned_versions: u64,
    /// Change-log entries dropped in lockstep with those versions.
    pub pruned_change_log_entries: u64,
    /// Estimated in-memory row bytes released by the pruned versions.
    pub reclaimed_bytes: u64,
    /// The eligible tables that actually had superseded versions pruned.
    pub compacted_tables: Vec<String>,
    /// Whether a redb file compaction ran to reclaim the freed pages.
    pub redb_compacted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FkProbeStats {
    pub indexed_tuple_probes: u64,
    pub full_scan_fallbacks: u64,
}

/// Per-stage commit work counters. `#[doc(hidden)]` test-introspection surface
/// in the `FkProbeStats` precedent pattern. Stage order matches `commit_validate`:
/// conditional-update revalidation, UNIQUE / commit-time upsert rewrite validation,
/// FK, composite-FK, vector-schema revalidation, store apply / index maintenance,
/// and trigger-audit / event projection staging.
///
/// Tier 1 - always-on integer work counters (the acceptance arms assert on these).
/// Tier 2 - per-stage cumulative wall time, only populated under the `test-seams`
/// feature; `None` on default builds so non-feature builds compile and the field
/// is never load-bearing for correctness.
#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitStageStats {
    /// Rows whose constraints were validated, summed across the validated stages.
    pub rows_validated: u64,
    /// Indexed probes issued by the validated stages (UNIQUE / FK / composite-FK).
    pub indexed_probes: u64,
    /// Staged-row-vs-staged-row comparisons performed across all validated stages.
    pub staged_vs_staged_comparisons: u64,
    /// Rows touched by full-table-scan primitives during commit validation.
    pub scan_rows_touched: u64,
    /// Index slots the apply-time maintenance loop iterated over for this write set.
    pub index_maintenance_visits: u64,
    /// Per-stage cumulative wall time in nanoseconds, ordered as in the struct doc.
    /// `None` unless built with `test-seams`. Never read by correctness assertions.
    pub stage_wall_nanos: Option<[u64; 7]>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StagedTupleKey {
    table: String,
    columns: Vec<String>,
    values: Vec<DirectedValue>,
}

impl StagedTupleKey {
    fn new(table: &str, columns: &[String], values: &[Value]) -> Self {
        Self {
            table: table.to_string(),
            columns: columns.to_vec(),
            values: values
                .iter()
                .cloned()
                .map(|value| DirectedValue::Asc(TotalOrdAsc(value)))
                .collect(),
        }
    }
}

#[derive(Debug, Default)]
struct StagedTupleLookup {
    entries: BTreeMap<StagedTupleKey, BTreeSet<RowId>>,
}

impl StagedTupleLookup {
    fn rebuild(db: &Database, ws: &WriteSet) -> Result<Self> {
        let columns_by_table = db.staged_tuple_key_columns_by_table();
        Ok(Self::from_write_set(&columns_by_table, ws))
    }

    fn from_write_set(columns_by_table: &HashMap<String, Vec<Vec<String>>>, ws: &WriteSet) -> Self {
        let mut lookup = Self::default();
        for (table, row) in &ws.relational_inserts {
            lookup.add_row_with_columns(columns_by_table, table, row);
        }
        lookup
    }

    fn add_row_with_columns(
        &mut self,
        columns_by_table: &HashMap<String, Vec<Vec<String>>>,
        table: &str,
        row: &VersionedRow,
    ) {
        let Some(table_columns) = columns_by_table.get(table) else {
            return;
        };
        for columns in table_columns {
            let Some(values) = tuple_values_for_row(row, columns) else {
                continue;
            };
            if !Database::values_can_be_tuple_key(&values) {
                continue;
            }
            self.entries
                .entry(StagedTupleKey::new(table, columns, &values))
                .or_default()
                .insert(row.row_id);
        }
    }

    fn remove_row_with_columns(
        &mut self,
        columns_by_table: &HashMap<String, Vec<Vec<String>>>,
        table: &str,
        row: &VersionedRow,
    ) {
        let Some(table_columns) = columns_by_table.get(table) else {
            return;
        };
        for columns in table_columns {
            let Some(values) = tuple_values_for_row(row, columns) else {
                continue;
            };
            if !Database::values_can_be_tuple_key(&values) {
                continue;
            }
            let key = StagedTupleKey::new(table, columns, &values);
            let remove_entry = self.entries.get_mut(&key).is_some_and(|row_ids| {
                row_ids.remove(&row.row_id);
                row_ids.is_empty()
            });
            if remove_entry {
                self.entries.remove(&key);
            }
        }
    }

    fn apply_delta(
        &mut self,
        columns_by_table: &HashMap<String, Vec<Vec<String>>>,
        delta: &StagedTupleLookupDelta,
    ) {
        for event in &delta.events {
            match event {
                StagedTupleLookupEvent::Add { table, row } => {
                    self.add_row_with_columns(columns_by_table, table, row);
                }
                StagedTupleLookupEvent::Remove { table, row } => {
                    self.remove_row_with_columns(columns_by_table, table, row);
                }
            }
        }
    }

    fn staged_unique_conflict(
        &self,
        table: &str,
        row_id: RowId,
        columns: &[String],
        values: &[Value],
        stats: &mut CommitStageStats,
    ) -> Option<RowId> {
        stats.staged_vs_staged_comparisons = stats.staged_vs_staged_comparisons.saturating_add(1);
        self.entries
            .get(&StagedTupleKey::new(table, columns, values))
            .and_then(|row_ids| row_ids.iter().copied().find(|other| *other != row_id))
    }

    fn contains_tuple(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        stats: Option<&mut CommitStageStats>,
    ) -> bool {
        if let Some(stats) = stats {
            stats.staged_vs_staged_comparisons =
                stats.staged_vs_staged_comparisons.saturating_add(1);
        }
        self.entries
            .get(&StagedTupleKey::new(table, columns, values))
            .is_some_and(|row_ids| !row_ids.is_empty())
    }
}

#[derive(Debug)]
enum StagedTupleLookupEvent {
    Add { table: TableName, row: VersionedRow },
    Remove { table: TableName, row: VersionedRow },
}

#[derive(Debug, Default)]
struct StagedTupleLookupDelta {
    events: Vec<StagedTupleLookupEvent>,
}

impl StagedTupleLookupDelta {
    fn add(&mut self, table: &str, row: &VersionedRow) {
        self.events.push(StagedTupleLookupEvent::Add {
            table: table.to_string(),
            row: row.clone(),
        });
    }

    fn remove(&mut self, table: &str, row: &VersionedRow) {
        self.events.push(StagedTupleLookupEvent::Remove {
            table: table.to_string(),
            row: row.clone(),
        });
    }
}

#[derive(Debug)]
struct CommitTimeUpsertRewrite {
    table: TableName,
    row_id: RowId,
    lookup_delta: StagedTupleLookupDelta,
}

struct PreparedPropagationOptions<'a> {
    tx: TxId,
    snapshot: SnapshotId,
    lookup_delta: Option<&'a mut StagedTupleLookupDelta>,
}

struct CommitStageStatsGuard<'a> {
    db: &'a Database,
    stats: CommitStageStats,
    flushed: bool,
}

#[cfg(feature = "test-seams")]
struct CommitStageTimer {
    last: Instant,
}

#[cfg(feature = "test-seams")]
impl CommitStageTimer {
    fn new() -> Self {
        Self {
            last: Instant::now(),
        }
    }

    fn record(&mut self, stats: &mut CommitStageStats, stage: usize) {
        let now = Instant::now();
        let delta = now
            .duration_since(self.last)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        self.last = now;
        let stages = stats.stage_wall_nanos.get_or_insert([0; 7]);
        if let Some(slot) = stages.get_mut(stage) {
            *slot = slot.saturating_add(delta);
        }
    }
}

impl<'a> CommitStageStatsGuard<'a> {
    fn new(db: &'a Database) -> Self {
        Self {
            db,
            stats: CommitStageStats::default(),
            flushed: false,
        }
    }

    fn stats_mut(&mut self) -> &mut CommitStageStats {
        &mut self.stats
    }

    fn flush(&mut self) {
        if !self.flushed {
            self.db.add_commit_stage_stats(&self.stats);
            self.flushed = true;
        }
    }
}

impl Drop for CommitStageStatsGuard<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}

impl PruningRuntime {
    fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
            #[cfg(test)]
            wakes: Arc::new(AtomicU64::new(0)),
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
            vector_schema_gates: Arc::new(VectorSchemaGates::default()),
            change_log,
            ddl_log,
            persistence,
            open_registry_path: Mutex::new(open_registry_path),
            operation_gate: Arc::new(RwLock::new(())),
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
            retention_sync_peer: Mutex::new(None),
            cron: Arc::new(CronState::new()),
            event_bus,
            trigger,
            sync_relay_mode: Arc::new(AtomicBool::new(false)),
            in_memory_applied_push_watermarks: Arc::new(Mutex::new(HashMap::new())),
            pending_event_bus_ddl: Mutex::new(HashMap::new()),
            pending_commit_metadata: Mutex::new(HashMap::new()),
            disk_limit: AtomicU64::new(disk_limit.unwrap_or(0)),
            disk_limit_startup_ceiling: AtomicU64::new(disk_limit_startup_ceiling.unwrap_or(0)),
            sync_watermark: Arc::new(AtomicLsn::new(Lsn(0))),
            closed: AtomicBool::new(false),
            resource_closed: Arc::new(AtomicBool::new(false)),
            rows_examined: AtomicU64::new(0),
            last_vector_search_used_hnsw: AtomicBool::new(false),
            last_vector_search_trace: RwLock::new(None),
            statement_cache: RwLock::new(HashMap::new()),
            rank_formula_cache: RwLock::new(HashMap::new()),
            acl_grant_cache: RwLock::new(HashMap::new()),
            rank_policy_eval_count: AtomicU64::new(0),
            rank_policy_formula_parse_count: AtomicU64::new(0),
            fk_indexed_tuple_probes: AtomicU64::new(0),
            fk_full_scan_fallbacks: AtomicU64::new(0),
            commit_rows_validated: AtomicU64::new(0),
            commit_indexed_probes: AtomicU64::new(0),
            commit_staged_vs_staged_comparisons: AtomicU64::new(0),
            commit_scan_rows_touched: AtomicU64::new(0),
            commit_index_maintenance_visits: AtomicU64::new(0),
            #[cfg(feature = "test-seams")]
            commit_stage_wall_nanos: std::array::from_fn(|_| AtomicU64::new(0)),
            corrupt_joined_values: RwLock::new(HashSet::new()),
            resource_owner: true,
        }
    }

    /// Opens the database file at `path`, becoming its sole owner for the
    /// lifetime of the returned handle. A second open of the same path — this
    /// process or another — returns [`Error::DatabaseLocked`]; see the "Store
    /// ownership and concurrency" section on [`Database`]. Use
    /// [`Database::open_memory`] for an ephemeral instance.
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
        db.load_retention_sync_peer();
        db.start_maintenance_if_eligible();
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

    pub fn scoped_with_contexts(
        &self,
        contexts: std::collections::BTreeSet<contextdb_core::types::ContextId>,
    ) -> Self {
        self.scoped_with_constraints(Some(contexts), None, None)
    }

    pub fn scoped_with_constraints(
        &self,
        contexts: Option<std::collections::BTreeSet<contextdb_core::types::ContextId>>,
        scope_labels: Option<std::collections::BTreeSet<contextdb_core::types::ScopeLabel>>,
        principal: Option<contextdb_core::types::Principal>,
    ) -> Self {
        let contexts = narrowed_constraint_set(&self.access.contexts, contexts);
        let scope_labels = narrowed_constraint_set(&self.access.scope_labels, scope_labels);
        let principal = self.access.principal.clone().or(principal);
        Self {
            tx_mgr: self.tx_mgr.clone(),
            relational_store: self.relational_store.clone(),
            graph_store: self.graph_store.clone(),
            vector_store: self.vector_store.clone(),
            vector_schema_gates: self.vector_schema_gates.clone(),
            change_log: self.change_log.clone(),
            ddl_log: self.ddl_log.clone(),
            persistence: self.persistence.clone(),
            open_registry_path: Mutex::new(None),
            operation_gate: self.operation_gate.clone(),
            apply_phase_pause: self.apply_phase_pause.clone(),
            relational: MemRelationalExecutor::new(
                self.relational_store.clone(),
                self.tx_mgr.clone(),
            ),
            graph: MemGraphExecutor::new(self.graph_store.clone(), self.tx_mgr.clone()),
            vector: MemVectorExecutor::new_with_accountant(
                self.vector_store.clone(),
                self.tx_mgr.clone(),
                Arc::new(OnceLock::new()),
                self.accountant.clone(),
            ),
            session_tx: Mutex::new(None),
            instance_id: uuid::Uuid::new_v4(),
            owner_thread: thread::current().id(),
            plugin: self.plugin.clone(),
            access: AccessConstraints {
                contexts,
                scope_labels,
                principal,
            },
            accountant: self.accountant.clone(),
            conflict_policies: RwLock::new(self.conflict_policies.read().clone()),
            subscriptions: self.subscriptions.clone(),
            pruning_runtime: Mutex::new(PruningRuntime::new()),
            pruning_guard: self.pruning_guard.clone(),
            retention_sync_peer: Mutex::new(self.retention_sync_peer.lock().clone()),
            cron: self.cron.clone(),
            event_bus: self.event_bus.clone(),
            trigger: self.trigger.clone(),
            sync_relay_mode: self.sync_relay_mode.clone(),
            in_memory_applied_push_watermarks: self.in_memory_applied_push_watermarks.clone(),
            pending_event_bus_ddl: Mutex::new(HashMap::new()),
            pending_commit_metadata: Mutex::new(HashMap::new()),
            disk_limit: AtomicU64::new(self.disk_limit.load(Ordering::SeqCst)),
            disk_limit_startup_ceiling: AtomicU64::new(
                self.disk_limit_startup_ceiling.load(Ordering::SeqCst),
            ),
            sync_watermark: self.sync_watermark.clone(),
            closed: AtomicBool::new(false),
            resource_closed: self.resource_closed.clone(),
            rows_examined: AtomicU64::new(0),
            last_vector_search_used_hnsw: AtomicBool::new(false),
            last_vector_search_trace: RwLock::new(None),
            statement_cache: RwLock::new(HashMap::new()),
            rank_formula_cache: RwLock::new(HashMap::new()),
            acl_grant_cache: RwLock::new(HashMap::new()),
            rank_policy_eval_count: AtomicU64::new(0),
            rank_policy_formula_parse_count: AtomicU64::new(0),
            fk_indexed_tuple_probes: AtomicU64::new(0),
            fk_full_scan_fallbacks: AtomicU64::new(0),
            commit_rows_validated: AtomicU64::new(0),
            commit_indexed_probes: AtomicU64::new(0),
            commit_staged_vs_staged_comparisons: AtomicU64::new(0),
            commit_scan_rows_touched: AtomicU64::new(0),
            commit_index_maintenance_visits: AtomicU64::new(0),
            #[cfg(feature = "test-seams")]
            commit_stage_wall_nanos: std::array::from_fn(|_| AtomicU64::new(0)),
            corrupt_joined_values: RwLock::new(HashSet::new()),
            resource_owner: false,
        }
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
        let mut sanitized_row_tables = HashSet::new();
        for (name, meta) in &all_meta {
            let mut runtime_meta = meta.clone();
            let user_indexes = runtime_meta
                .indexes
                .iter()
                .filter(|index| index.kind == IndexKind::UserDeclared)
                .cloned()
                .collect::<Vec<_>>();
            runtime_meta.indexes = crate::executor::auto_indexes_for_table_meta(&runtime_meta);
            runtime_meta.indexes.extend(user_indexes);
            relational.create_table(name, runtime_meta.clone());
            // Register EVERY index declared in TableMeta.indexes — this
            // includes auto-indexes (kind=Auto) synthesized at CREATE TABLE
            // time AND user-declared indexes (kind=UserDeclared).
            for decl in &runtime_meta.indexes {
                if decl.kind == IndexKind::Auto {
                    relational.create_exact_index_storage(name, &decl.name, decl.columns.clone());
                } else {
                    relational.create_index_storage(name, &decl.name, decl.columns.clone());
                }
            }
            for mut row in persistence.load_relational_table(name)? {
                if sanitize_loaded_row_for_meta(&mut row, &runtime_meta) {
                    sanitized_row_tables.insert(name.clone());
                }
                relational.insert_loaded_row(name, row);
            }
        }
        let loaded_row_ids = relational
            .tables
            .read()
            .iter()
            .map(|(table, rows)| {
                (
                    table.clone(),
                    rows.iter()
                        .filter(|row| row.deleted_tx.is_none())
                        .map(|row| row.row_id)
                        .collect::<HashSet<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let sync_source_lsns = persistence
            .load_sync_source_lsns()?
            .into_iter()
            .filter(|((table, row_id), _)| {
                loaded_row_ids
                    .get(table)
                    .is_some_and(|row_ids| row_ids.contains(row_id))
            })
            .collect();
        relational.replace_sync_source_lsns(sync_source_lsns);

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
        let loaded_ddl_log = persistence.load_ddl_log()?;
        let (mut loaded_vectors, repaired_vector_refs) = reconcile_loaded_vectors_for_meta(
            persistence.load_vectors()?,
            &all_meta,
            &loaded_ddl_log,
        );
        let supplemented_vectors =
            supplement_loaded_vectors_from_rows(&relational, &all_meta, &mut loaded_vectors);
        for entry in &loaded_vectors {
            vector.insert_loaded_vector(entry.clone());
        }

        let loaded_change_log = persistence.load_change_log()?;
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
        let hydrated_row_tables = hydrate_relational_vector_values(&relational, &loaded_vectors);
        if repaired_vector_refs || supplemented_vectors {
            persistence.rewrite_vectors(&loaded_vectors)?;
        }
        for table in sanitized_row_tables
            .into_iter()
            .chain(hydrated_row_tables.into_iter())
            .collect::<HashSet<_>>()
        {
            if let Some(rows) = relational.tables.read().get(&table) {
                persistence.rewrite_table_rows(&table, rows)?;
            }
        }

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

    pub fn begin(&self) -> Result<TxId> {
        let _operation = self.open_operation_after_public_tx_control_wait("begin")?;
        Ok(self.tx_mgr.begin())
    }

    fn callback_active_process_wide() -> bool {
        global_callback_active_count().load(Ordering::SeqCst) > 0
    }

    fn trigger_deadlock_timeout() -> Duration {
        std::env::var("CONTEXTDB_TRIGGER_DEADLOCK_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_TRIGGER_DEADLOCK_TIMEOUT)
    }

    fn open_operation_after_public_tx_control_wait(
        &self,
        surface: &'static str,
    ) -> Result<DatabaseOperationGuard<'_>> {
        self.open_operation_after_public_tx_control_wait_for_tx(surface, None)
    }

    fn open_operation_after_public_tx_control_wait_for_tx(
        &self,
        surface: &'static str,
        tx: Option<TxId>,
    ) -> Result<DatabaseOperationGuard<'_>> {
        let operation = self.open_operation()?;
        if !Self::callback_active_process_wide() {
            return Ok(operation);
        }
        drop(operation);
        self.assert_callback_reentry_allowed()?;
        if let Some(tx) = tx {
            self.assert_trigger_callback_tx_not_captured_cross_thread(tx)?;
        }
        self.assert_public_tx_control_cross_thread_allowed_for_surface(surface)?;
        self.open_operation()
    }

    fn assert_callback_reentry_allowed(&self) -> Result<()> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Cron,
            });
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) {
            self.record_user_commit_trigger_reentry();
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            });
        }
        Ok(())
    }

    fn assert_public_tx_control_cross_thread_allowed_for_surface(
        &self,
        surface: &'static str,
    ) -> Result<()> {
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        match self
            .trigger
            .callback_contention_for_tx_control(self.owner_thread, self.cron.has_schedules())
        {
            TriggerContention::None => Ok(()),
            TriggerContention::SameDb => self.wait_for_same_db_trigger_callback_idle(surface),
        }
    }

    fn wait_for_same_db_trigger_callback_idle(&self, surface: &'static str) -> Result<()> {
        let timeout = Self::trigger_deadlock_timeout();
        let started = Instant::now();
        let mut observed_wait = false;
        let mut guard = self.trigger.wait_lock.lock();
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(closed_database_error());
            }
            self.assert_callback_reentry_allowed()?;
            match self
                .trigger
                .callback_contention_for_tx_control(self.owner_thread, self.cron.has_schedules())
            {
                TriggerContention::None => {
                    if surface != "close"
                        && self.trigger.close_waiter_count.load(Ordering::SeqCst) > 0
                    {
                        let _ = self
                            .trigger
                            .waiters
                            .wait_for(&mut guard, Duration::from_millis(1));
                        continue;
                    }
                    return Ok(());
                }
                TriggerContention::SameDb => {
                    if !observed_wait {
                        self.trigger
                            .wait_observed_count
                            .fetch_add(1, Ordering::SeqCst);
                        observed_wait = true;
                    }
                }
            }

            let elapsed = started.elapsed();
            if elapsed >= timeout {
                return Err(self.trigger_same_db_deadlock_timeout_error(surface, elapsed));
            }
            let remaining = timeout.saturating_sub(elapsed);
            self.trigger.waiters.wait_for(&mut guard, remaining);
        }
    }

    fn trigger_same_db_deadlock_timeout_error(
        &self,
        surface: &'static str,
        waited: Duration,
    ) -> Error {
        self.trigger
            .deadlock_guard_timeout_observed_count
            .fetch_add(1, Ordering::SeqCst);
        self.trigger
            .typed_err_observed_same_db_count
            .fetch_add(1, Ordering::SeqCst);
        let trigger_name = self.trigger.active_trigger_name_for_wait();
        let waited_ms = waited.as_millis() as u64;
        tracing::warn!(
            trigger = %trigger_name,
            trigger_name = %trigger_name,
            waited_ms = waited_ms,
            surface = surface,
            "trigger callback wait exceeded deadlock guard"
        );
        Error::CallbackActiveCrossThread {
            kind: CallbackKind::Trigger,
        }
    }

    // Internal engine-managed writes still need callback-active gates, and
    // trigger contention stays scoped to this database so independent embedded
    // handles do not poison each other through process-wide state.
    fn begin_for_internal_write(&self) -> Result<TxId> {
        let _operation = self.open_operation()?;
        self.assert_internal_write_callbacks_allowed()?;
        Ok(self.tx_mgr.begin())
    }

    fn begin_for_public_autocommit_write(&self) -> Result<TxId> {
        let _operation = self.open_operation_after_internal_write_wait("execute")?;
        Ok(self.tx_mgr.begin())
    }

    fn rollback_preopened_autocommit_tx(&self, tx: Option<TxId>) {
        if let Some(tx) = tx {
            let _ = self.tx_mgr.rollback_empty_without_commit_lock(tx);
        }
    }

    fn enter_sql_write_control_bypass(&self, tx: TxId) -> SqlWriteControlBypassGuard {
        let db_id = self as *const Self as usize;
        SQL_WRITE_CONTROL_BYPASS_STACK.with(|stack| {
            stack.borrow_mut().push((db_id, tx));
        });
        SqlWriteControlBypassGuard { db_id, tx }
    }

    fn sql_write_control_bypass_active(&self, tx: TxId) -> bool {
        let db_id = self as *const Self as usize;
        SQL_WRITE_CONTROL_BYPASS_STACK.with(|stack| stack.borrow().contains(&(db_id, tx)))
    }

    fn assert_internal_write_callbacks_allowed(&self) -> Result<()> {
        if !Self::callback_active_process_wide() {
            return Ok(());
        }
        self.assert_callback_reentry_allowed()?;
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            });
        }
        Ok(())
    }

    fn open_operation_after_internal_write_wait(
        &self,
        surface: &'static str,
    ) -> Result<DatabaseOperationGuard<'_>> {
        let operation = self.open_operation()?;
        if !Self::callback_active_process_wide() {
            return Ok(operation);
        }
        drop(operation);
        self.assert_callback_reentry_allowed()?;
        self.assert_internal_write_cross_thread_allowed(surface)?;
        self.open_operation()
    }

    fn cron_callback_tx_bound_matches(&self, tx: TxId) -> bool {
        let cron_tx = CRON_CALLBACK_TX.with(|slot| slot.get());
        let cron_db = CRON_CALLBACK_DB.with(|slot| slot.get());
        cron_tx == Some(tx) && cron_db == Some(self as *const Self as usize)
    }

    pub(crate) fn trigger_callback_tx_bound_matches(&self, tx: TxId) -> bool {
        let trigger_tx = TRIGGER_CALLBACK_TX.with(|slot| slot.get());
        let trigger_db = TRIGGER_CALLBACK_DB.with(|slot| slot.get());
        trigger_tx == Some(tx) && trigger_db == Some(self as *const Self as usize)
    }

    pub(crate) fn trigger_callback_wallclock(&self) -> Wallclock {
        TRIGGER_CALLBACK_WALLCLOCK
            .with(|slot| slot.get())
            .unwrap_or_else(current_wallclock)
    }

    fn callback_tx_bound_matches(&self, tx: TxId) -> bool {
        self.cron_callback_tx_bound_matches(tx) || self.trigger_callback_tx_bound_matches(tx)
    }

    pub(super) fn vector_schema_gate_id(&self) -> usize {
        Arc::as_ptr(&self.vector_schema_gates) as usize
    }

    fn callback_active_on_current_thread_for_this_db(&self) -> bool {
        let gate_id = self.vector_schema_gate_id();
        (CRON_CALLBACK_ACTIVE.with(|active| active.get())
            && CRON_CALLBACK_VECTOR_SCHEMA_GATE.with(|slot| slot.get()) == Some(gate_id))
            || (TRIGGER_CALLBACK_ACTIVE.with(|active| active.get())
                && TRIGGER_CALLBACK_VECTOR_SCHEMA_GATE.with(|slot| slot.get()) == Some(gate_id))
    }

    fn assert_callback_tx_bound_write_allowed(&self, tx: TxId) -> Result<()> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get())
            && !self.cron_callback_tx_bound_matches(tx)
        {
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Cron,
            });
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get())
            && !self.trigger_callback_tx_bound_matches(tx)
        {
            self.record_user_commit_trigger_reentry();
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            });
        }
        Ok(())
    }

    fn assert_trigger_callback_tx_not_captured_cross_thread(&self, tx: TxId) -> Result<()> {
        if self.trigger.callback_tx_active(tx) && !self.trigger_callback_tx_bound_matches(tx) {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Trigger,
            });
        }
        Ok(())
    }

    fn open_operation_after_write_control_wait(
        &self,
        tx: TxId,
        surface: &'static str,
    ) -> Result<DatabaseOperationGuard<'_>> {
        let operation = self.open_operation()?;
        if self.sql_write_control_bypass_active(tx)
            || !Self::callback_active_process_wide()
            || self.callback_tx_bound_matches(tx)
        {
            return Ok(operation);
        }
        drop(operation);
        self.assert_callback_tx_bound_write_allowed(tx)?;
        self.assert_trigger_callback_tx_not_captured_cross_thread(tx)?;
        self.assert_write_control_cross_thread_allowed_for_surface(surface)?;
        self.open_operation()
    }

    fn open_operation_after_statement_callback_wait(
        &self,
        stmt: &Statement,
        surface: &'static str,
    ) -> Result<DatabaseOperationGuard<'_>> {
        let operation = self.open_operation()?;
        if !Self::callback_active_process_wide() {
            return Ok(operation);
        }
        drop(operation);
        self.assert_statement_allowed_for_callbacks_for_surface(stmt, surface)?;
        self.open_operation()
    }

    fn assert_write_control_cross_thread_allowed_for_surface(
        &self,
        surface: &'static str,
    ) -> Result<()> {
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        match self
            .trigger
            .callback_contention_for_tx_control(self.owner_thread, self.cron.has_schedules())
        {
            TriggerContention::None => Ok(()),
            TriggerContention::SameDb => self.wait_for_same_db_trigger_callback_idle(surface),
        }
    }

    fn assert_internal_write_cross_thread_allowed(&self, surface: &'static str) -> Result<()> {
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        match self
            .trigger
            .callback_contention_for_tx_control(self.owner_thread, self.cron.has_schedules())
        {
            TriggerContention::None => Ok(()),
            TriggerContention::SameDb => self.wait_for_same_db_trigger_callback_idle(surface),
        }
    }

    /// Test/example helper. Production code MUST use `?` propagation against
    /// the typed callback-active errors. Unwraps the engine's
    /// `CallbackActiveCrossThread` / `CallbackReentry` variants as panics; not
    /// appropriate for code that may run alongside trigger or cron callbacks.
    pub fn begin_or_panic(&self) -> TxId {
        self.begin().unwrap_or_else(|err| panic!("{err}"))
    }

    pub fn commit(&self, tx: TxId) -> Result<()> {
        let _operation =
            self.open_operation_after_public_tx_control_wait_for_tx("commit", Some(tx))?;
        let _user_commit = self.enter_user_commit_callback_scope();
        self.commit_with_source(tx, CommitSource::User)
    }

    pub fn rollback(&self, tx: TxId) -> Result<()> {
        let _operation =
            self.open_operation_after_public_tx_control_wait_for_tx("rollback", Some(tx))?;
        self.rollback_without_callback_tx_control(tx)
    }

    fn rollback_without_callback_tx_control(&self, tx: TxId) -> Result<()> {
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

    fn enter_user_commit_callback_scope(&self) -> UserCommitCallbackGuard {
        let prior_active = USER_COMMIT_ACTIVE.with(|slot| slot.replace(true));
        let prior_reentry = USER_COMMIT_TRIGGER_REENTRY.with(|slot| slot.replace(false));
        UserCommitCallbackGuard {
            prior_active,
            prior_reentry,
        }
    }

    fn record_user_commit_trigger_reentry(&self) {
        if self.access_is_admin() && USER_COMMIT_ACTIVE.with(|slot| slot.get()) {
            USER_COMMIT_TRIGGER_REENTRY.with(|slot| slot.set(true));
        }
    }

    fn take_user_commit_trigger_reentry(&self) -> bool {
        USER_COMMIT_TRIGGER_REENTRY.with(|slot| slot.replace(false))
    }

    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let cached = self.cached_statement(sql);
        let parsed_stmt;
        let (stmt, cached_plan) = if let Some(cached) = cached.as_ref() {
            (&cached.stmt, Some(&cached.plan))
        } else {
            parsed_stmt = contextdb_parser::parse(sql)?;
            (&parsed_stmt, None)
        };
        if Self::callback_active_process_wide() {
            self.assert_statement_allowed_for_callbacks_for_surface(stmt, "execute")?;
        }

        match stmt {
            Statement::Begin => {
                if self.session_tx.lock().is_none() {
                    if Self::callback_active_process_wide() {
                        self.assert_callback_reentry_allowed()?;
                        self.assert_public_tx_control_cross_thread_allowed_for_surface("begin")?;
                    }
                    let _operation = self.open_operation()?;
                    let mut session = self.session_tx.lock();
                    if session.is_none() {
                        *session = Some(self.tx_mgr.begin());
                    }
                }
                return Ok(QueryResult::empty());
            }
            Statement::Commit => {
                let tx = *self.session_tx.lock();
                if let Some(tx) = tx {
                    match self.commit(tx) {
                        Ok(()) => {
                            let mut session = self.session_tx.lock();
                            if *session == Some(tx) {
                                *session = None;
                            }
                        }
                        Err(err) => {
                            if self.tx_mgr.cloned_write_set(tx).is_err() {
                                let mut session = self.session_tx.lock();
                                if *session == Some(tx) {
                                    *session = None;
                                }
                            }
                            return Err(err);
                        }
                    }
                }
                return Ok(QueryResult::empty());
            }
            Statement::Rollback => {
                let tx = *self.session_tx.lock();
                if let Some(tx) = tx {
                    self.rollback(tx)?;
                    let mut session = self.session_tx.lock();
                    if *session == Some(tx) {
                        *session = None;
                    }
                }
                return Ok(QueryResult::empty());
            }
            _ => {}
        }

        let active_tx = self.active_session_tx();
        let preopened_autocommit_tx =
            if active_tx.is_none() && Self::statement_uses_public_autocommit_write(stmt) {
                Some(self.begin_for_public_autocommit_write()?)
            } else {
                None
            };

        let operation_result = if preopened_autocommit_tx.is_some() {
            self.open_operation()
        } else {
            self.open_operation_after_statement_callback_wait(stmt, "execute")
        };
        let _operation = match operation_result {
            Ok(operation) => operation,
            Err(error) => {
                self.rollback_preopened_autocommit_tx(preopened_autocommit_tx);
                return Err(error);
            }
        };

        match stmt {
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
                    .ddl_change_for_statement(stmt, self.active_session_tx())
                    .expect("trigger statement has DDL change");
                self.apply_trigger_ddl_from_user(ddl)?;
                return Ok(QueryResult::empty());
            }
            _ => {}
        }

        self.execute_statement_with_plan(
            stmt,
            sql,
            params,
            active_tx,
            cached_plan,
            preopened_autocommit_tx,
        )
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

    fn assert_statement_allowed_inside_cron_callback(
        &self,
        stmt: &Statement,
        forbidden_in_callback: bool,
        requires_callback_tx: bool,
    ) -> Result<()> {
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) && forbidden_in_callback {
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Cron,
            });
        }
        if CRON_CALLBACK_ACTIVE.with(|active| active.get()) && requires_callback_tx {
            let active_tx = self.active_session_tx();
            let cron_tx = CRON_CALLBACK_TX.with(|slot| slot.get());
            let cron_db = CRON_CALLBACK_DB.with(|slot| slot.get());
            let this_db = self as *const Self as usize;
            if active_tx.is_none() || active_tx != cron_tx || cron_db != Some(this_db) {
                return Err(Error::CallbackReentry {
                    kind: CallbackKind::Cron,
                });
            }
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) && forbidden_in_callback {
            // Self-drop of the currently firing trigger has a long-standing
            // admin-error contract: block the DDL locally, preserve the active
            // trigger, and let the firing cascade continue if the callback
            // handles the error.
            if let Statement::DropTrigger { name } = stmt {
                let drops_active_trigger = TRIGGER_CALLBACK_NAME
                    .with(|active_name| active_name.borrow().as_deref() == Some(name));
                let drops_from_active_trigger_handle = TRIGGER_CALLBACK_DB
                    .with(|active_db| active_db.get() == Some(self as *const Self as usize));
                if drops_active_trigger && drops_from_active_trigger_handle {
                    return Ok(());
                }
            }
            self.record_user_commit_trigger_reentry();
            return Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            });
        }
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) && requires_callback_tx {
            let active_tx = self.active_session_tx();
            let trigger_tx = TRIGGER_CALLBACK_TX.with(|slot| slot.get());
            let trigger_db = TRIGGER_CALLBACK_DB.with(|slot| slot.get());
            let this_db = self as *const Self as usize;
            if active_tx.is_none() || active_tx != trigger_tx || trigger_db != Some(this_db) {
                self.record_user_commit_trigger_reentry();
                return Err(Error::CallbackReentry {
                    kind: CallbackKind::Trigger,
                });
            }
        }
        Ok(())
    }

    fn assert_statement_allowed_during_cross_thread_callback(
        &self,
        forbidden_in_callback: bool,
        requires_callback_tx: bool,
        surface: &'static str,
    ) -> Result<()> {
        if !forbidden_in_callback && !requires_callback_tx {
            return Ok(());
        }
        if self.cron.callback_active_on_other_thread() {
            return Err(Error::CallbackActiveCrossThread {
                kind: CallbackKind::Cron,
            });
        }
        if self
            .trigger
            .callback_active_on_other_thread(self.owner_thread)
        {
            return self.wait_for_same_db_trigger_callback_idle(surface);
        }
        if requires_callback_tx {
            match self
                .trigger
                .callback_contention_for_tx_control(self.owner_thread, false)
            {
                TriggerContention::None => {}
                TriggerContention::SameDb => {
                    return self.wait_for_same_db_trigger_callback_idle(surface);
                }
            }
        }
        Ok(())
    }

    fn assert_statement_allowed_for_callbacks_for_surface(
        &self,
        stmt: &Statement,
        surface: &'static str,
    ) -> Result<()> {
        let forbidden_in_callback = Self::statement_forbidden_inside_cron_callback(stmt);
        let requires_callback_tx = Self::statement_requires_cron_bound_handle(stmt);
        if !forbidden_in_callback && !requires_callback_tx {
            return Ok(());
        }
        if !Self::callback_active_process_wide() {
            return Ok(());
        }
        self.assert_statement_allowed_inside_cron_callback(
            stmt,
            forbidden_in_callback,
            requires_callback_tx,
        )?;
        self.assert_statement_allowed_during_cross_thread_callback(
            forbidden_in_callback,
            requires_callback_tx,
            surface,
        )
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
        match (stmt, plan) {
            (Statement::Insert(ins), PhysicalPlan::Insert(_)) => {
                !ins.table.eq_ignore_ascii_case("GRAPH")
                    && !ins.table.eq_ignore_ascii_case("__edges")
            }
            (Statement::Update(_), PhysicalPlan::Update(_))
            | (Statement::Delete(_), PhysicalPlan::Delete(_)) => true,
            _ => false,
        }
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

    fn statement_uses_public_autocommit_write(stmt: &Statement) -> bool {
        matches!(
            stmt,
            Statement::Insert(_) | Statement::Delete(_) | Statement::Update(_)
        )
    }

    fn plan_vector_schema_refs(&self, plan: &PhysicalPlan) -> Vec<VectorIndexRef> {
        match plan {
            PhysicalPlan::Insert(plan) => self.vector_schema_refs_for_table(&plan.table),
            PhysicalPlan::Delete(plan) => self.vector_schema_refs_for_table(&plan.table),
            PhysicalPlan::Update(plan) => self.vector_schema_refs_for_table(&plan.table),
            _ => Vec::new(),
        }
    }

    pub(crate) fn write_set_vector_schema_refs(ws: &WriteSet) -> Vec<VectorIndexRef> {
        VectorSchemaGates::sorted_refs(
            ws.vector_inserts
                .iter()
                .map(|entry| entry.index.clone())
                .chain(ws.vector_deletes.iter().map(|(index, _, _)| index.clone()))
                .chain(ws.vector_moves.iter().map(|(index, _, _, _)| index.clone())),
        )
    }

    pub(crate) fn write_set_touches_vector_schema(ws: &WriteSet) -> bool {
        !Self::write_set_vector_schema_refs(ws).is_empty()
    }

    fn record_vector_schema_epoch(&self, tx: TxId, index: &VectorIndexRef) -> Result<()> {
        self.record_vector_schema_epochs(tx, [index.clone()])
    }

    fn record_vector_schema_epochs(
        &self,
        tx: TxId,
        refs: impl IntoIterator<Item = VectorIndexRef>,
    ) -> Result<()> {
        let refs = VectorSchemaGates::sorted_refs(refs);
        if refs.is_empty() {
            return Ok(());
        }
        self.tx_mgr.with_write_set(tx, |_| ())?;
        let observed = refs
            .into_iter()
            .map(|index| {
                let epoch = self.vector_schema_gates.epoch_for(&index);
                (index, epoch)
            })
            .collect::<Vec<_>>();
        let mut metadata = self.pending_commit_metadata.lock();
        let metadata = metadata.entry(tx).or_default();
        for (index, epoch) in observed {
            metadata.vector_schema_epochs.entry(index).or_insert(epoch);
        }
        Ok(())
    }

    pub(crate) fn clear_statement_cache(&self) {
        self.statement_cache.write().clear();
    }

    fn clear_trigger_insert_state_machine_cache(db_key: usize) {
        TRIGGER_INSERT_STATE_MACHINE_CACHE.with(|cache| {
            cache.borrow_mut().remove(&db_key);
        });
    }

    fn trigger_insert_table_has_state_machine(&self, table: &str) -> Result<bool> {
        let db_key = self as *const Self as usize;
        if let Some(cached) = TRIGGER_INSERT_STATE_MACHINE_CACHE.with(|cache| {
            cache
                .borrow()
                .get(&db_key)
                .and_then(|by_table| by_table.get(table).copied())
        }) {
            return Ok(cached);
        }

        let has_state_machine = self
            .relational_store
            .table_has_state_machine(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        TRIGGER_INSERT_STATE_MACHINE_CACHE.with(|cache| {
            cache
                .borrow_mut()
                .entry(db_key)
                .or_default()
                .insert(table.to_string(), has_state_machine);
        });
        Ok(has_state_machine)
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
        preopened_autocommit_tx: Option<TxId>,
    ) -> Result<QueryResult> {
        // Reset per-query rows_examined once at the entry point so every
        // sub-plan (union, CTE, subquery IndexScan) accumulates into the
        // shared counter rather than overwriting prior counts.
        self.__reset_rows_examined();
        match plan {
            PhysicalPlan::Insert(_) | PhysicalPlan::Delete(_) | PhysicalPlan::Update(_) => {
                let tx = match preopened_autocommit_tx {
                    Some(tx) => tx,
                    None => self.begin_for_public_autocommit_write()?,
                };
                let vector_schema_refs = self.plan_vector_schema_refs(plan);
                let _vector_schema = (!vector_schema_refs.is_empty())
                    .then(|| self.vector_schema_read_many(vector_schema_refs));
                let result = {
                    let _write_gate = self.enter_sql_write_control_bypass(tx);
                    let snapshot = self.snapshot_for_read();
                    self.with_snapshot_override(snapshot, || {
                        execute_plan(self, plan, params, Some(tx))
                    })
                };
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
                        let _ = self.rollback_without_callback_tx_control(tx);
                        Err(e)
                    }
                }
            }
            _ => {
                if preopened_autocommit_tx.is_some() {
                    self.rollback_preopened_autocommit_tx(preopened_autocommit_tx);
                    return Err(Error::Other(
                        "internal autocommit transaction reserved for non-DML statement"
                            .to_string(),
                    ));
                }
                execute_plan(self, plan, params, None)
            }
        }
    }

    pub fn explain(&self, sql: &str) -> Result<String> {
        let _operation = self.open_operation()?;
        let stmt = contextdb_parser::parse(sql)?;
        let plan = contextdb_planner::plan(&stmt)?;
        let vector_shape = vector_search_shape_from_plan(&plan);
        let _vector_schema = vector_shape
            .as_ref()
            .map(|shape| self.vector_schema_read(&shape.index));
        let snapshot = self.snapshot();
        let mut output = plan.explain();
        let uses_hnsw = vector_shape
            .as_ref()
            .is_some_and(|shape| self.vector_hnsw_strategy_for_explain(shape, snapshot));
        if uses_hnsw {
            output = output.replace("VectorSearch(", "HNSWSearch(");
            output = output.replace("VectorSearch {", "HNSWSearch {");
        } else {
            output = annotate_vector_search_strategy(output, "BruteForce");
            output = output.replace("VectorSearch {", "VectorSearch { strategy: BruteForce,");
        }
        Ok(output)
    }

    fn vector_hnsw_strategy_for_explain(
        &self,
        shape: &VectorExplainShape,
        snapshot: SnapshotId,
    ) -> bool {
        if !self
            .vector
            .hnsw_eligible_without_build(&shape.index, snapshot)
        {
            return false;
        }
        !shape.restricted_candidates
            || self
                .vector
                .hnsw_search_covers_all_without_build(&shape.index, shape.k)
    }

    pub fn execute_in_tx(
        &self,
        tx: TxId,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> Result<QueryResult> {
        let cached = self.cached_statement(sql);
        let parsed_stmt;
        let (stmt, cached_plan) = if let Some(cached) = cached.as_ref() {
            (&cached.stmt, Some(&cached.plan))
        } else {
            parsed_stmt = contextdb_parser::parse(sql)?;
            (&parsed_stmt, None)
        };
        let trigger_callback_bound = self.trigger_callback_tx_bound_matches(tx);
        if trigger_callback_bound && matches!(stmt, Statement::Update(_) | Statement::Delete(_)) {
            if let Some(plan) = cached_plan {
                let snapshot = self.snapshot_for_read();
                return self
                    .with_snapshot_override(snapshot, || execute_plan(self, plan, params, Some(tx)))
                    .map(strip_internal_row_id);
            }

            let stmt = self.pre_resolve_cte_subqueries(stmt, params, Some(tx))?;
            let plan = contextdb_planner::plan(&stmt)?;
            self.cache_statement_if_eligible(sql, &stmt, &plan);
            let snapshot = self.snapshot_for_read();
            return self
                .with_snapshot_override(snapshot, || execute_plan(self, &plan, params, Some(tx)))
                .map(strip_internal_row_id);
        }
        let requires_callback_tx = Self::statement_requires_cron_bound_handle(stmt);
        if trigger_callback_bound {
            self.assert_statement_allowed_inside_cron_callback(
                stmt,
                Self::statement_forbidden_inside_cron_callback(stmt),
                requires_callback_tx,
            )?;
        } else if Self::callback_active_process_wide() {
            self.assert_callback_tx_bound_write_allowed(tx)?;
            self.assert_trigger_callback_tx_not_captured_cross_thread(tx)?;
            self.assert_statement_allowed_for_callbacks_for_surface(stmt, "execute_in_tx")?;
        }
        let _operation = if trigger_callback_bound {
            None
        } else if requires_callback_tx {
            Some(self.open_operation_after_write_control_wait(tx, "execute_in_tx")?)
        } else {
            Some(self.open_operation_after_statement_callback_wait(stmt, "execute_in_tx")?)
        };
        self.execute_statement_with_plan(stmt, sql, params, Some(tx), cached_plan, None)
    }

    /// Register a commit-time guard that requires the visible row selected by
    /// `key_column = key_value` to keep the supplied column values until this
    /// transaction commits. Returns `Ok(false)` when the row is not visible or
    /// does not currently match the predicates.
    pub fn guard_row_conditions_in_tx(
        &self,
        tx: TxId,
        table: &str,
        key_column: &str,
        key_value: &Value,
        predicates: &[(String, Value)],
    ) -> Result<bool> {
        let _operation = self.open_operation_after_public_tx_control_wait_for_tx(
            "guard_row_conditions_in_tx",
            Some(tx),
        )?;
        self.tx_mgr.with_write_set(tx, |_| ())?;
        self.assert_table_read_allowed(table)?;
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        if !meta.columns.iter().any(|column| column.name == key_column) {
            return Err(Error::ColumnNotFound {
                table: table.to_string(),
                column: key_column.to_string(),
            });
        }
        for (column, _) in predicates {
            if !meta
                .columns
                .iter()
                .any(|definition| definition.name == *column)
            {
                return Err(Error::ColumnNotFound {
                    table: table.to_string(),
                    column: column.clone(),
                });
            }
        }

        let snapshot = self.snapshot();
        let Some(row) = self.point_lookup_in_tx(tx, table, key_column, key_value, snapshot)? else {
            return Ok(false);
        };
        if !self.read_allowed_for_row(table, &meta, &row, snapshot)? {
            return Ok(false);
        }
        if !predicates
            .iter()
            .all(|(column, value)| row.values.get(column) == Some(value))
        {
            return Ok(false);
        }
        let counts = self.write_set_counts(tx)?;
        self.record_conditional_update_guard(
            tx,
            table.to_string(),
            row.row_id,
            predicates.to_vec(),
            counts,
            counts,
            true,
        )?;
        Ok(true)
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
        let pending_trigger_active_guards =
            std::cell::RefCell::new(Vec::<TriggerCallbackThreadGuard>::new());
        let mut pending_sink_events = Vec::new();
        let mut committed_trigger_audit_entries = Vec::new();
        let validation_noop_count = std::cell::Cell::new(0_u64);
        let pre_apply_index_maintenance_visits = std::cell::Cell::new(0_u64);
        #[cfg(feature = "test-seams")]
        let apply_started = std::cell::Cell::new(None::<Instant>);
        let delete_release_bytes = std::cell::RefCell::new(DeleteReleaseBytes::default());
        let (lsn, ws) = {
            match self.tx_mgr.commit_with_lsn_active_prepare_and_applied_mut(
                tx,
                |_| {
                    if source != CommitSource::SyncPull {
                        let outcome = match self.prepare_active_trigger_write_set_for_dispatch(tx) {
                            Ok(outcome) => outcome,
                            Err(error) => {
                                let reason = error.to_string();
                                let pending = pending_trigger_audits.borrow();
                                if !pending.is_empty() {
                                    self.append_rolled_back_trigger_audits(&pending, tx, &reason)?;
                                }
                                return Err(error);
                            }
                        };
                        Self::reject_user_conditional_update_conflicts(source, &outcome)?;
                        validation_noop_count.set(
                            validation_noop_count
                                .get()
                                .saturating_add(outcome.conditional_noop_count),
                        );
                        match self.dispatch_triggers_for_tx(tx) {
                            Ok(outcome) => {
                                *pending_trigger_audits.borrow_mut() = outcome.pending;
                                pending_trigger_active_guards
                                    .borrow_mut()
                                    .extend(outcome.active_guards);
                            }
                            Err(failure) => {
                                let failure = *failure;
                                pending_trigger_active_guards
                                    .borrow_mut()
                                    .extend(failure.active_guards);
                                return Err(failure.error);
                            }
                        }
                        let outcome = self.prepare_active_trigger_write_set_for_dispatch(tx)?;
                        Self::reject_user_conditional_update_conflicts(source, &outcome)?;
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
                        Self::reject_user_conditional_update_conflicts(source, &final_validation)?;
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
                        #[cfg(feature = "test-seams")]
                        let projection_started = Instant::now();
                        let prepared_sink_events = self
                            .prepare_sink_events_for_write_set_with_event_bus_ddl(
                                ws,
                                event_bus_ddl,
                            )?;
                        if self.persistence.is_some()
                            && let Some(lsn) = ws.commit_lsn
                        {
                            self.event_bus
                                .stage_sink_events_for_persistence(lsn, prepared_sink_events);
                        } else {
                            pending_sink_events = prepared_sink_events;
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
                        #[cfg(feature = "test-seams")]
                        {
                            let delta = Instant::now()
                                .duration_since(projection_started)
                                .as_nanos()
                                .min(u128::from(u64::MAX))
                                as u64;
                            let mut stage_wall_nanos = [0; 7];
                            stage_wall_nanos[6] = delta;
                            self.add_commit_stage_stats(&CommitStageStats {
                                stage_wall_nanos: Some(stage_wall_nanos),
                                ..Default::default()
                            });
                        }
                        *delete_release_bytes.borrow_mut() =
                            self.delete_release_bytes_for_write_set(ws);
                        pre_apply_index_maintenance_visits
                            .set(self.relational_store.index_maintenance_visits());
                        #[cfg(feature = "test-seams")]
                        apply_started.set(Some(Instant::now()));
                    }
                    Ok(())
                },
                |lsn, ws| {
                    if !ws.is_empty() {
                        let index_maintenance_visits = self
                            .relational_store
                            .index_maintenance_visits()
                            .saturating_sub(pre_apply_index_maintenance_visits.get());
                        #[cfg(feature = "test-seams")]
                        let mut stats = CommitStageStats {
                            index_maintenance_visits,
                            ..Default::default()
                        };
                        #[cfg(not(feature = "test-seams"))]
                        let stats = CommitStageStats {
                            index_maintenance_visits,
                            ..Default::default()
                        };
                        #[cfg(feature = "test-seams")]
                        if let Some(started) = apply_started.get() {
                            let delta = Instant::now()
                                .duration_since(started)
                                .as_nanos()
                                .min(u128::from(u64::MAX))
                                as u64;
                            let mut stage_wall_nanos = [0; 7];
                            stage_wall_nanos[5] = delta;
                            stats.stage_wall_nanos = Some(stage_wall_nanos);
                        }
                        self.add_commit_stage_stats(&stats);
                        self.publish_staged_event_bus_ddl_commit(lsn);
                        self.publish_staged_trigger_ddl_commit(lsn);
                        // DDL metadata mutation also needs the commit mutex.
                        // Release delete-side accounting here so DROP/RENAME
                        // cannot remove vector state before cleanup observes it.
                        self.release_delete_allocations_from_bytes(&delete_release_bytes.borrow());
                    }
                },
            ) {
                Ok(committed) => committed,
                Err(failure) => {
                    if let Some(ws) = failure.write_set.as_deref() {
                        self.plugin.commit_failed(ws, source, &failure.error);
                    }
                    if let Some(lsn) = failure.write_set.as_ref().and_then(|ws| ws.commit_lsn) {
                        let _ = self.event_bus.take_staged_sink_events_for_persistence(lsn);
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
            }
        };
        self.pending_commit_metadata.lock().remove(&tx);

        if !ws.is_empty() {
            self.plugin.post_commit(&ws, source);
            let sink_events_to_publish = if self.persistence.is_some() {
                ws.commit_lsn
                    .and_then(|lsn| self.event_bus.take_staged_sink_events_for_persistence(lsn))
                    .map(EventBusState::materialize_staged_sink_events)
                    .unwrap_or_default()
            } else {
                pending_sink_events
            };
            self.publish_prepared_sink_events_to_memory(sink_events_to_publish);
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
            conditional_conflict_count: 0,
        })
    }

    fn reject_user_conditional_update_conflicts(
        source: CommitSource,
        outcome: &CommitValidationOutcome,
    ) -> Result<()> {
        if source == CommitSource::User && outcome.conditional_conflict_count > 0 {
            return Err(Error::ConditionalUpdateConflict {
                count: outcome.conditional_conflict_count,
            });
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

    fn execute_statement_with_plan(
        &self,
        stmt: &Statement,
        sql: &str,
        params: &HashMap<String, Value>,
        tx: Option<TxId>,
        cached_plan: Option<&PhysicalPlan>,
        mut preopened_autocommit_tx: Option<TxId>,
    ) -> Result<QueryResult> {
        if let Err(error) = self.plugin.on_query(sql) {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx.take());
            return Err(error);
        }

        if let Some(change) = self.ddl_change_for_statement(stmt, tx).as_ref()
            && let Err(error) = self.plugin.on_ddl(change)
        {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx.take());
            return Err(error);
        }

        let started = Instant::now();
        if let Some(result) = self.execute_event_bus_statement(stmt, tx) {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx.take());
            let outcome = query_outcome_from_result(&result);
            self.plugin.post_query(sql, started.elapsed(), &outcome);
            return result;
        }

        // Handle INSERT INTO GRAPH / __edges as a virtual table routing to the graph store.
        if let Statement::Insert(ins) = stmt
            && (ins.table.eq_ignore_ascii_case("GRAPH")
                || ins.table.eq_ignore_ascii_case("__edges"))
        {
            return self.execute_graph_insert(ins, params, tx, preopened_autocommit_tx.take());
        }

        let result = (|| {
            if let Some(plan) = cached_plan {
                let skip_static_dml_validation =
                    matches!(plan, PhysicalPlan::Update(_) | PhysicalPlan::Delete(_));
                return self.run_planned_statement(
                    stmt,
                    plan,
                    params,
                    tx,
                    preopened_autocommit_tx.take(),
                    skip_static_dml_validation,
                );
            }

            let (stmt, plan) = {
                // Pre-resolve InSubquery expressions with CTE context before planning.
                let stmt = self.pre_resolve_cte_subqueries(stmt, params, tx)?;
                let plan = contextdb_planner::plan(&stmt)?;
                self.cache_statement_if_eligible(sql, &stmt, &plan);
                (stmt, plan)
            };
            self.run_planned_statement(
                &stmt,
                &plan,
                params,
                tx,
                preopened_autocommit_tx.take(),
                false,
            )
        })();
        if result.is_err() {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx.take());
        }
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
        preopened_autocommit_tx: Option<TxId>,
        skip_static_dml_validation: bool,
    ) -> Result<QueryResult> {
        if !skip_static_dml_validation && let Err(error) = validate_dml(plan, self, params) {
            self.rollback_preopened_autocommit_tx(preopened_autocommit_tx);
            return Err(error);
        }
        let result = match tx {
            Some(tx) => {
                // Reset rows_examined at the top of an in-tx statement so
                // sub-plans accumulate rather than overwrite.
                self.__reset_rows_examined();
                let _write_gate = if self.trigger_callback_tx_bound_matches(tx) {
                    None
                } else {
                    Some(self.enter_sql_write_control_bypass(tx))
                };
                let snapshot = self.snapshot_for_read();
                self.with_snapshot_override(snapshot, || execute_plan(self, plan, params, Some(tx)))
            }
            None => self.execute_autocommit(plan, params, preopened_autocommit_tx),
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
        preopened_autocommit_tx: Option<TxId>,
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
        let tx = match tx {
            Some(tx) => tx,
            None => match preopened_autocommit_tx {
                Some(tx) => tx,
                None => self.begin_for_public_autocommit_write()?,
            },
        };
        let result = {
            let _write_gate = self.enter_sql_write_control_bypass(tx);
            (|| {
                let mut count = 0u64;
                for row_exprs in &ins.values {
                    let source = resolve_expr(&row_exprs[source_idx], params)?;
                    let target = resolve_expr(&row_exprs[target_idx], params)?;
                    let edge_type = resolve_expr(&row_exprs[edge_type_idx], params)?;

                    let source_uuid = match &source {
                        Value::Uuid(u) => *u,
                        Value::Text(t) => uuid::Uuid::parse_str(t).map_err(|e| {
                            Error::PlanError(format!("invalid source_id uuid: {e}"))
                        })?,
                        _ => return Err(Error::PlanError("source_id must be UUID".into())),
                    };
                    let target_uuid = match &target {
                        Value::Uuid(u) => *u,
                        Value::Text(t) => uuid::Uuid::parse_str(t).map_err(|e| {
                            Error::PlanError(format!("invalid target_id uuid: {e}"))
                        })?,
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
                Ok(count)
            })()
        };

        let count = match result {
            Ok(count) => count,
            Err(error) => {
                if auto_commit {
                    let _ = self.rollback_without_callback_tx_control(tx);
                }
                return Err(error);
            }
        };

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
                        declared_sync_direction,
                        declared_unit,
                    } => {
                        // The execution paths refuse this statement outright, so
                        // the projection must not hand a plugin a PushOnly table
                        // the engine is about to reject.
                        if *declared_sync_direction
                            == Some(contextdb_parser::ast::RetainedSyncDirection::TwoWay)
                        {
                            return None;
                        }
                        meta.default_ttl_seconds = Some(*duration_seconds);
                        meta.sync_safe = *sync_safe;
                        meta.retained_sync_policy = Some(RetainedSyncPolicy::PushOnly);
                        meta.retain_declared_unit = Some(*declared_unit);
                    }
                    AlterAction::DropRetain => {
                        meta.default_ttl_seconds = None;
                        meta.sync_safe = false;
                        meta.retained_sync_policy = None;
                        meta.retain_declared_unit = None;
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
        let trigger_callback_bound = self.trigger_callback_tx_bound_matches(tx);
        let _operation = if trigger_callback_bound {
            None
        } else {
            Some(self.open_operation_after_write_control_wait(tx, "insert_row")?)
        };
        if !trigger_callback_bound || !self.trigger.ready.load(Ordering::SeqCst) {
            self.ensure_trigger_table_ready(table, "insert_row")?;
        }
        // Statement-scoped bound: `Value::TxId(n)` must satisfy
        // `n <= max(committed_watermark, tx)` so writes inside an active
        // transaction can reference their own allocated TxId. The error,
        // when fired, still reports `committed_watermark` per plan B7.
        let mut values =
            self.coerce_row_for_insert(table, values, Some(self.committed_watermark()), Some(tx))?;
        self.complete_insert_access_values(table, &mut values)?;
        if !trigger_callback_bound {
            self.validate_row_constraints(tx, table, &values, None)?;
        }
        if trigger_callback_bound && !self.has_access_constraints_for_query() {
            let has_state_machine = self.trigger_insert_table_has_state_machine(table)?;
            let row_id = self.relational_store.new_row_id();
            self.assert_row_write_allowed(table, row_id, &values, self.snapshot_for_read())?;
            if !has_state_machine {
                return self
                    .relational
                    .insert_with_row_id_assume_no_state_machine_at(
                        tx,
                        table,
                        row_id,
                        values,
                        self.trigger_callback_wallclock(),
                    );
            }
            return self.relational.insert_with_row_id(
                tx,
                table,
                row_id,
                values,
                self.snapshot_for_read(),
            );
        }
        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(table, row_id, &values, self.snapshot_for_read())?;
        self.relational
            .insert_with_row_id(tx, table, row_id, values, self.snapshot_for_read())
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
        if !self.trigger_callback_tx_bound_matches(tx) {
            self.validate_row_constraints(tx, table, &values, Some(old_row_id))?;
        }
        self.assert_row_write_allowed(table, old_row_id, &values, self.snapshot_for_read())?;
        self.relational
            .insert_with_row_id(tx, table, old_row_id, values, self.snapshot_for_read())
    }

    pub(crate) fn replace_row_after_update_validation(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<RowId> {
        self.relational.delete(tx, table, row_id)?;
        self.relational
            .insert_with_row_id(tx, table, row_id, values, snapshot)
    }

    pub(crate) fn replace_row_after_update_validation_counted(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        context: UpdateReplacementContext<'_>,
    ) -> Result<(RowId, WriteSetCounts, WriteSetCounts)> {
        if context.meta.immutable {
            return Err(Error::ImmutableTable(table.to_string()));
        }
        let table_name = table.to_string();
        let row = VersionedRow {
            row_id,
            values,
            created_tx: tx,
            deleted_tx: None,
            lsn: Lsn(0),
            created_at: Some(context.created_at),
        };
        self.tx_mgr.with_write_set(tx, |ws| {
            let before = current_write_set_counts(ws);
            ws.relational_inserts.retain(|(staged_table, staged_row)| {
                !(staged_table == &table_name && staged_row.row_id == row_id)
            });
            if context.committed_row_exists
                && !ws
                    .relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == &table_name && *deleted_row_id == row_id
                    })
            {
                ws.relational_deletes.push((table_name.clone(), row_id, tx));
            }
            ws.relational_inserts.push((table_name, row));
            let after = current_write_set_counts(ws);
            (row_id, before, after)
        })
    }

    /// Internal variant used by sync-apply: skips the TXID bound check because
    /// peer TxIds may legitimately exceed the local watermark. Still enforces
    /// wrong-variant + reverse-direction TXID column rules.
    /// Insert a row that arrived over sync. `created_at` is the birth time the
    /// row carried from the node that wrote it: retention judges a row by that
    /// stamp against the holder's own clock, so a replicated row keeps its age
    /// instead of being handed a fresh window on arrival. `None` (an origin
    /// that recorded no stamp) falls back to stamping the row here.
    pub(crate) fn insert_row_for_sync(
        &self,
        tx: TxId,
        table: &str,
        values: HashMap<ColName, Value>,
        created_at: Option<Wallclock>,
    ) -> Result<RowId> {
        let values = self.coerce_row_for_insert(table, values, None, None)?;
        self.validate_row_constraints(tx, table, &values, None)?;
        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(table, row_id, &values, self.snapshot())?;
        match created_at {
            Some(created_at) => self.relational.insert_with_row_id_at(
                tx,
                table,
                row_id,
                values,
                self.snapshot(),
                created_at,
            ),
            None => self
                .relational
                .insert_with_row_id(tx, table, row_id, values, self.snapshot()),
        }
    }

    /// Upsert a row that arrived over sync. See [`Self::insert_row_for_sync`]
    /// for what `created_at` carries and why.
    pub(crate) fn upsert_row_for_sync(
        &self,
        tx: TxId,
        table: &str,
        conflict_col: &str,
        values: HashMap<ColName, Value>,
        created_at: Option<Wallclock>,
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
            self.insert_synced_row_at(tx, table, existing.row_id, values, snapshot, created_at)?;
            if let (Some(uuid), Some(state), Some(_meta)) =
                (row_uuid, new_state.as_deref(), meta.as_ref())
            {
                self.propagate_state_change_if_needed(tx, table, Some(uuid), Some(state))?;
            }
            return Ok(UpsertResult::Updated);
        }

        let row_id = self.relational_store.new_row_id();
        self.assert_row_write_allowed(table, row_id, &values, snapshot)?;
        self.insert_synced_row_at(tx, table, row_id, values, snapshot, created_at)?;
        if let (Some(uuid), Some(state), Some(_meta)) =
            (row_uuid, new_state.as_deref(), meta.as_ref())
        {
            self.propagate_state_change_if_needed(tx, table, Some(uuid), Some(state))?;
        }
        Ok(UpsertResult::Inserted)
    }

    /// Stage a synced row, honouring the birth time it carried when it has
    /// one and stamping locally when it does not.
    fn insert_synced_row_at(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
        created_at: Option<Wallclock>,
    ) -> Result<RowId> {
        match created_at {
            Some(created_at) => self
                .relational
                .insert_with_row_id_at(tx, table, row_id, values, snapshot, created_at),
            None => self
                .relational
                .insert_with_row_id(tx, table, row_id, values, snapshot),
        }
    }

    /// Whether this table's row for `natural_key` currently holds a version
    /// that ARRIVED over sync rather than one written here. The marker is set
    /// when a sync apply stages the row and cleared by any local write to it
    /// (see `sync_source_lsn_updates`), so it describes the CURRENT version,
    /// never the row's history: a pulled row that was then edited locally
    /// reads `false`.
    pub fn row_version_arrived_by_sync(
        &self,
        table: &str,
        natural_key_column: &str,
        natural_key_value: &Value,
    ) -> bool {
        let _operation = self.assert_open_operation();
        let Ok(Some(row)) = self.point_lookup(
            table,
            natural_key_column,
            natural_key_value,
            self.snapshot_for_read(),
        ) else {
            return false;
        };
        self.relational_store
            .sync_source_lsn(table, row.row_id)
            .is_some()
    }

    /// Test-introspection: how many commit-index entries are retained, and the
    /// lowest LSN among them (`Lsn(0)` when the index is empty). Production-dead
    /// — nothing but tests reads it.
    #[doc(hidden)]
    pub fn commit_index_census_for_test(&self) -> (usize, Lsn) {
        let _operation = self.assert_open_operation();
        self.tx_mgr.commit_index_census()
    }

    /// Test-introspection: how many commit-index entries sit strictly below
    /// `lsn`. The anchor rule wants exactly one after a prune — fewer drops the
    /// oldest surviving delete, more is unreclaimed growth. Production-dead.
    #[doc(hidden)]
    pub fn commit_index_entries_below_for_test(&self, lsn: Lsn) -> usize {
        let _operation = self.assert_open_operation();
        self.tx_mgr.commit_index_snapshot().range(..lsn).count()
    }

    fn set_sync_row_source_lsn(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        source_lsn: Lsn,
    ) -> Result<()> {
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.set_relational_insert_source_lsn(table.to_string(), row_id, source_lsn);
        })
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
                self.assert_row_write_allowed(table, row_id, &values, self.snapshot_for_read())?;
                self.relational
                    .insert_with_row_id(tx, table, row_id, values, self.snapshot_for_read())
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
        let _operation = self.open_operation_after_write_control_wait(tx, "upsert_row")?;
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
        indexes.get(table).is_some_and(|table_indexes| {
            table_indexes
                .values()
                .any(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
        })
    }

    /// Returns true if `table` has an index whose first-column prefix contains
    /// exactly the columns in `cols` (same order).
    fn index_covers_composite(&self, table: &str, cols: &[String]) -> bool {
        let indexes = self.relational_store.indexes.read();
        indexes.get(table).is_some_and(|table_indexes| {
            table_indexes.values().any(|idx| {
                idx.columns.len() >= cols.len()
                    && idx
                        .columns
                        .iter()
                        .zip(cols.iter())
                        .all(|((c, _), want)| c == want)
            })
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
        let (tx_staged_deletes, tx_delete_predicates, staged_overlap) =
            self.tx_mgr.with_write_set(tx, |ws| {
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
                let delete_predicates = ws
                    .relational_delete_predicates
                    .iter()
                    .filter(|predicate| predicate.table == table)
                    .cloned()
                    .collect::<Vec<_>>();
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
                (deletes, delete_predicates, overlap)
            })?;
        let entries = {
            let indexes = self.relational_store.indexes.read();
            // Prefer conventional auto-index names when they still match the
            // requested column, then fall back to any exact single-column index.
            let pk_key = format!("__pk_{column}");
            let unique_key = format!("__unique_{column}");
            let storage = indexes.get(table).and_then(|table_indexes| {
                table_indexes
                    .get(&pk_key)
                    .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                    .or_else(|| {
                        table_indexes
                            .get(&unique_key)
                            .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                    })
                    .or_else(|| {
                        table_indexes
                            .values()
                            .find(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                    })
            });
            let Some(storage) = storage else {
                return Ok(ConstraintProbe::NoIndex);
            };
            let key = vec![DirectedValue::Asc(TotalOrdAsc(value.clone()))];
            storage
                .exact_postings(&key)
                .map(|entries| entries.to_vec())
                .unwrap_or_default()
        };
        for entry in entries {
            if let Some(sid) = skip_row_id
                && entry.row_id == sid
            {
                continue;
            }
            if tx_staged_deletes.contains(&entry.row_id) {
                continue;
            }
            if !tx_delete_predicates.is_empty()
                && let Some(row) = self
                    .relational_store
                    .row_by_id(table, entry.row_id, snapshot)
                && row_matches_delete_predicates(&tx_delete_predicates, table, &row)
            {
                continue;
            }
            if entry.visible_at(snapshot) {
                return Ok(ConstraintProbe::Match(entry.row_id));
            }
        }
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
        if cols.is_empty() || values.is_empty() || cols.len() != values.len() {
            return Ok(None);
        }
        // Rows this tx has already staged for delete are not obstructions.
        let (tx_staged_deletes, tx_delete_predicates): (
            std::collections::HashSet<RowId>,
            Vec<RelationalDeletePredicate>,
        ) = self.tx_mgr.with_write_set(tx, |ws| {
            let deletes = ws
                .relational_deletes
                .iter()
                .filter(|(t, _, _)| t == table)
                .map(|(_, row_id, _)| *row_id)
                .collect();
            let predicates = ws
                .relational_delete_predicates
                .iter()
                .filter(|predicate| predicate.table == table)
                .cloned()
                .collect();
            (deletes, predicates)
        })?;
        let index_name = self.relational_store.table_meta(table).and_then(|meta| {
            meta.indexes
                .iter()
                .find(|decl| {
                    decl.columns.len() >= cols.len()
                        && (decl.kind != IndexKind::Auto || decl.columns.len() == cols.len())
                        && decl
                            .columns
                            .iter()
                            .zip(cols.iter())
                            .all(|((c, _), w)| c == w)
                })
                .map(|decl| decl.name.clone())
        });
        let Some(index_name) = index_name else {
            return Ok(None);
        };
        let entries = {
            let indexes = self.relational_store.indexes.read();
            let Some(storage) = indexes
                .get(table)
                .and_then(|table_indexes| table_indexes.get(&index_name))
            else {
                return Ok(None);
            };
            let prefix = index_key_from_values(&storage.columns[..cols.len()], values);
            if storage.columns.len() == cols.len() {
                storage
                    .exact_postings(&prefix)
                    .map(|entries| entries.to_vec())
                    .unwrap_or_default()
            } else {
                storage
                    .tree
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| {
                        key.len() >= prefix.len() && key[..prefix.len()] == prefix[..]
                    })
                    .flat_map(|(_, entries)| entries.iter().cloned())
                    .collect::<Vec<_>>()
            }
        };
        for entry in entries {
            if let Some(sid) = skip_row_id
                && entry.row_id == sid
            {
                continue;
            }
            if tx_staged_deletes.contains(&entry.row_id) {
                continue;
            }
            if !tx_delete_predicates.is_empty()
                && let Some(row) = self
                    .relational_store
                    .row_by_id(table, entry.row_id, snapshot)
                && row_matches_delete_predicates(&tx_delete_predicates, table, &row)
            {
                continue;
            }
            if entry.visible_at(snapshot) {
                return Ok(Some(entry.row_id));
            }
        }
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
            .filter(|column| matches!(column.column_type, ColumnType::TxId))
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
        let mut stats_guard = CommitStageStatsGuard::new(self);
        #[cfg(feature = "test-seams")]
        let mut stage_timer = CommitStageTimer::new();
        let validation = self.revalidate_conditional_updates(
            ws,
            snapshot,
            &metadata.conditional_update_guards,
            Some(stats_guard.stats_mut()),
        )?;
        #[cfg(feature = "test-seams")]
        stage_timer.record(stats_guard.stats_mut(), 0);
        self.validate_vector_schema_epochs(ws, &metadata.vector_schema_epochs)?;
        self.validate_vector_write_set_schema(ws)?;
        let mut deleted_rows_by_table = self.deleted_row_snapshots_by_table(ws);
        self.validate_unique_constraints_in_write_set(
            ws,
            snapshot,
            &metadata.upsert_intents,
            &mut deleted_rows_by_table,
            stats_guard.stats_mut(),
        )?;
        #[cfg(feature = "test-seams")]
        stage_timer.record(stats_guard.stats_mut(), 1);
        self.validate_foreign_keys_in_write_set(
            ws,
            snapshot,
            &deleted_rows_by_table,
            stats_guard.stats_mut(),
        )?;
        #[cfg(feature = "test-seams")]
        stage_timer.record(stats_guard.stats_mut(), 2);
        self.validate_composite_foreign_keys_in_write_set(
            ws,
            snapshot,
            &deleted_rows_by_table,
            stats_guard.stats_mut(),
        )?;
        #[cfg(feature = "test-seams")]
        stage_timer.record(stats_guard.stats_mut(), 3);
        self.validate_vector_schema_epochs(ws, &metadata.vector_schema_epochs)?;
        self.validate_vector_write_set_schema(ws)?;
        #[cfg(feature = "test-seams")]
        stage_timer.record(stats_guard.stats_mut(), 4);
        stats_guard.flush();
        Ok(validation)
    }

    fn validate_vector_schema_epochs(
        &self,
        ws: &WriteSet,
        recorded_epochs: &HashMap<VectorIndexRef, u64>,
    ) -> Result<()> {
        for index in Self::write_set_vector_schema_refs(ws) {
            let Some(recorded_epoch) = recorded_epochs.get(&index) else {
                return Err(Self::stale_vector_schema_error(&index));
            };
            let current_epoch = self.vector_schema_gates.epoch_for(&index);
            if current_epoch != *recorded_epoch {
                return Err(Self::stale_vector_schema_error(&index));
            }
        }
        Ok(())
    }

    fn stale_vector_schema_error(index: &VectorIndexRef) -> Error {
        Error::SchemaInvalid {
            reason: format!(
                "vector index {}.{} changed while transaction was open; retry transaction",
                index.table, index.column
            ),
        }
    }

    fn validate_vector_write_set_schema(&self, ws: &WriteSet) -> Result<()> {
        let mut refs = ws
            .vector_inserts
            .iter()
            .map(|entry| entry.index.clone())
            .chain(ws.vector_deletes.iter().map(|(index, _, _)| index.clone()))
            .chain(ws.vector_moves.iter().map(|(index, _, _, _)| index.clone()))
            .collect::<Vec<_>>();
        refs.sort_by(|a, b| a.table.cmp(&b.table).then(a.column.cmp(&b.column)));
        refs.dedup();

        for index in refs {
            let Some(meta) = self.table_meta(&index.table) else {
                return Err(Error::UnknownVectorIndex { index });
            };
            let Some(column) = meta
                .columns
                .iter()
                .find(|column| column.name == index.column)
            else {
                return Err(Error::UnknownVectorIndex { index });
            };
            let ColumnType::Vector(expected) = column.column_type else {
                return Err(Error::UnknownVectorIndex { index });
            };
            let state = self.vector_store.state(&index)?;
            if state.dimension() != expected {
                return Err(Error::VectorIndexDimensionMismatch {
                    index,
                    expected,
                    actual: state.dimension(),
                });
            }
        }

        for entry in &ws.vector_inserts {
            let state = self.vector_store.state(&entry.index)?;
            let expected = state.dimension();
            if entry.vector.len() != expected {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: entry.index.clone(),
                    expected,
                    actual: entry.vector.len(),
                });
            }
        }
        for (table, row) in &ws.relational_inserts {
            let Some(meta) = self.table_meta(table) else {
                continue;
            };
            for (column_name, value) in &row.values {
                let Value::Vector(vector) = value else {
                    continue;
                };
                let index = VectorIndexRef::new(table, column_name);
                let Some(column) = meta
                    .columns
                    .iter()
                    .find(|column| column.name == *column_name)
                else {
                    return Err(Error::UnknownVectorIndex { index });
                };
                let ColumnType::Vector(expected) = column.column_type else {
                    return Err(Error::UnknownVectorIndex { index });
                };
                if vector.len() != expected {
                    return Err(Error::VectorIndexDimensionMismatch {
                        index,
                        expected,
                        actual: vector.len(),
                    });
                }
            }
        }

        Ok(())
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

    fn upsert_intent_indexes_by_row(
        upsert_intents: &[PendingUpsertIntent],
    ) -> HashMap<(TableName, RowId), Vec<usize>> {
        let mut indexes = HashMap::<(TableName, RowId), Vec<usize>>::new();
        for (index, intent) in upsert_intents.iter().enumerate() {
            indexes
                .entry((intent.table.clone(), intent.row_id))
                .or_default()
                .push(index);
        }
        indexes
    }

    fn upsert_intent_index_by_conflict(
        upsert_intents: &[PendingUpsertIntent],
    ) -> HashMap<(TableName, RowId, Vec<ColName>), usize> {
        let mut indexes = HashMap::<(TableName, RowId, Vec<ColName>), usize>::new();
        for (index, intent) in upsert_intents.iter().enumerate() {
            indexes
                .entry((
                    intent.table.clone(),
                    intent.row_id,
                    intent.conflict_columns.clone(),
                ))
                .or_insert(index);
        }
        indexes
    }

    fn rewrite_commit_time_upserts_for_write_set(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        upsert_intents: &[PendingUpsertIntent],
    ) -> Result<()> {
        if upsert_intents.is_empty() {
            return Ok(());
        }
        let upsert_lookup = CommitTimeUpsertLookup {
            intents: upsert_intents,
            by_row: Self::upsert_intent_indexes_by_row(upsert_intents),
        };
        let mut deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        let mut index = 0;
        while index < ws.relational_inserts.len() {
            let table = ws.relational_inserts[index].0.clone();
            let skip_deleted = deleted_by_table.get(&table).unwrap_or(&empty_deleted);
            let prefix = Self::relational_insert_prefix_identity(ws, index);
            if let Some(rewrite) = self.apply_original_commit_time_upsert_if_needed(
                ws,
                index,
                skip_deleted,
                &upsert_lookup,
                snapshot,
                None,
            )? {
                deleted_by_table = Self::deleted_row_ids_by_table(ws);
                index =
                    Self::relational_insert_index_by_row(ws, &rewrite.table, rewrite.row_id, index)
                        .unwrap_or_else(|| {
                            if Self::relational_insert_prefix_unchanged(ws, &prefix) {
                                index.saturating_add(1)
                            } else {
                                index.min(ws.relational_inserts.len())
                            }
                        });
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

    fn deleted_row_ids_by_table(ws: &WriteSet) -> HashMap<String, HashSet<RowId>> {
        let mut deleted = HashMap::<String, HashSet<RowId>>::new();
        for (table, row_id, _) in &ws.relational_deletes {
            deleted.entry(table.clone()).or_default().insert(*row_id);
        }
        deleted
    }

    fn deleted_row_snapshots_by_table(
        &self,
        ws: &WriteSet,
    ) -> HashMap<String, HashMap<RowId, VersionedRow>> {
        if ws.relational_deletes.is_empty() {
            return HashMap::new();
        }
        let mut seen = HashSet::new();
        let mut keys = Vec::with_capacity(ws.relational_deletes.len());
        for (table, row_id, _) in &ws.relational_deletes {
            if seen.insert((table.clone(), *row_id)) {
                keys.push((table.clone(), *row_id));
            }
        }
        self.relational_store.live_rows_by_id(&keys)
    }

    fn relational_insert_prefix_identity(ws: &WriteSet, end: usize) -> Vec<(String, RowId)> {
        ws.relational_inserts
            .iter()
            .take(end)
            .map(|(table, row)| (table.clone(), row.row_id))
            .collect()
    }

    fn relational_insert_prefix_unchanged(ws: &WriteSet, expected: &[(String, RowId)]) -> bool {
        ws.relational_inserts.len() >= expected.len()
            && ws.relational_inserts.iter().zip(expected.iter()).all(
                |((table, row), (expected_table, expected_row_id))| {
                    table == expected_table && row.row_id == *expected_row_id
                },
            )
    }

    fn value_can_be_tuple_key(value: &Value) -> bool {
        !matches!(value, Value::Float64(_) | Value::Json(_) | Value::Vector(_))
    }

    fn values_can_be_tuple_key(values: &[Value]) -> bool {
        values.iter().all(Self::value_can_be_tuple_key)
    }

    fn add_staged_tuple_columns(
        columns_by_table: &mut HashMap<String, Vec<Vec<String>>>,
        table: &str,
        columns: Vec<String>,
    ) {
        if columns.is_empty() {
            return;
        }
        let table_columns = columns_by_table.entry(table.to_string()).or_default();
        if !table_columns.iter().any(|existing| existing == &columns) {
            table_columns.push(columns);
        }
    }

    fn staged_tuple_key_columns_by_table(&self) -> HashMap<String, Vec<Vec<String>>> {
        let metas = self.relational_store.table_meta.read();
        let mut columns_by_table = HashMap::<String, Vec<Vec<String>>>::new();

        for (table, meta) in metas.iter() {
            for column in &meta.columns {
                if column.primary_key || column.unique {
                    Self::add_staged_tuple_columns(
                        &mut columns_by_table,
                        table,
                        vec![column.name.clone()],
                    );
                }
                if let Some(reference) = &column.references {
                    Self::add_staged_tuple_columns(
                        &mut columns_by_table,
                        table,
                        vec![column.name.clone()],
                    );
                    Self::add_staged_tuple_columns(
                        &mut columns_by_table,
                        &reference.table,
                        vec![reference.column.clone()],
                    );
                }
            }

            for unique_constraint in &meta.unique_constraints {
                Self::add_staged_tuple_columns(
                    &mut columns_by_table,
                    table,
                    unique_constraint.clone(),
                );
            }

            for fk in &meta.composite_foreign_keys {
                Self::add_staged_tuple_columns(
                    &mut columns_by_table,
                    table,
                    fk.child_columns.clone(),
                );
                Self::add_staged_tuple_columns(
                    &mut columns_by_table,
                    &fk.parent_table,
                    fk.parent_columns.clone(),
                );
            }
        }

        columns_by_table
    }

    fn add_commit_stat(counter: &AtomicU64, delta: u64) {
        if delta > 0 {
            counter.fetch_add(delta, Ordering::SeqCst);
        }
    }

    fn add_commit_stage_stats(&self, stats: &CommitStageStats) {
        Self::add_commit_stat(&self.commit_rows_validated, stats.rows_validated);
        Self::add_commit_stat(&self.commit_indexed_probes, stats.indexed_probes);
        Self::add_commit_stat(
            &self.commit_staged_vs_staged_comparisons,
            stats.staged_vs_staged_comparisons,
        );
        Self::add_commit_stat(&self.commit_scan_rows_touched, stats.scan_rows_touched);
        Self::add_commit_stat(
            &self.commit_index_maintenance_visits,
            stats.index_maintenance_visits,
        );
        #[cfg(feature = "test-seams")]
        if let Some(stage_wall_nanos) = stats.stage_wall_nanos {
            for (counter, delta) in self
                .commit_stage_wall_nanos
                .iter()
                .zip(stage_wall_nanos.into_iter())
            {
                Self::add_commit_stat(counter, delta);
            }
        }
    }

    fn committed_unique_conflict_row_id(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,
    ) -> Result<Option<RowId>> {
        let mut stats = stats;
        if columns.is_empty() || columns.len() != values.len() {
            return Ok(None);
        }

        if let Some(row_id) = self.indexed_visible_row_id_by_columns(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            stats.as_deref_mut(),
        ) {
            return Ok(row_id);
        }

        let before_scan_rows = stats
            .as_ref()
            .map(|_| self.relational_store.scan_rows_touched());
        let rows = self
            .relational
            .scan_filter_with_tx(None, table, snapshot, &|row| {
                !skip_deleted.contains(&row.row_id)
                    && columns
                        .iter()
                        .zip(values.iter())
                        .all(|(column, value)| row.values.get(column) == Some(value))
            })?;
        if let (Some(stats), Some(before_scan_rows)) = (stats, before_scan_rows) {
            stats.scan_rows_touched = stats.scan_rows_touched.saturating_add(
                self.relational_store
                    .scan_rows_touched()
                    .saturating_sub(before_scan_rows),
            );
        }
        Ok(rows.into_iter().next().map(|row| row.row_id))
    }

    fn indexed_visible_row_id_by_columns(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,
    ) -> Option<Option<RowId>> {
        if columns.is_empty() || columns.len() != values.len() {
            return Some(None);
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
                let pk_key = format!("__pk_{column}");
                let unique_key = format!("__unique_{column}");
                let storage = indexes.get(table).and_then(|table_indexes| {
                    table_indexes
                        .get(&pk_key)
                        .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == *column)
                        .or_else(|| {
                            table_indexes
                                .get(&unique_key)
                                .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == *column)
                        })
                        .or_else(|| {
                            table_indexes
                                .values()
                                .find(|idx| idx.columns.len() == 1 && idx.columns[0].0 == *column)
                        })
                });
                match storage {
                    Some(storage) => {
                        let key = index_key_from_values(&storage.columns[..1], values);
                        (
                            true,
                            storage.exact_postings(&key).and_then(|entries| {
                                visible_posting(entries, snapshot, skip_deleted)
                            }),
                        )
                    }
                    None => (false, None),
                }
            } else {
                let storage = indexes.get(table).and_then(|table_indexes| {
                    table_indexes.values().find(|idx| {
                        idx.columns.len() >= columns.len()
                            && (!idx.exact_only() || idx.columns.len() == columns.len())
                            && idx
                                .columns
                                .iter()
                                .zip(columns.iter())
                                .all(|((have, _), want)| have == want)
                    })
                });
                match storage {
                    Some(storage) => {
                        let prefix =
                            index_key_from_values(&storage.columns[..columns.len()], values);
                        let row_id = if storage.columns.len() == columns.len() {
                            storage.exact_postings(&prefix).and_then(|entries| {
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

        if !index_checked {
            return None;
        }

        if let Some(stats) = stats {
            stats.indexed_probes = stats.indexed_probes.saturating_add(1);
        }
        Some(indexed_row_id)
    }

    fn required_indexed_visible_row_by_columns(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
    ) -> Result<Option<VersionedRow>> {
        let Some(row_id) = self.indexed_visible_row_id_by_columns(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            None,
        ) else {
            return Err(Error::Other(format!(
                "exact sync key probe on {table}({}) has no covering index",
                columns.join(", ")
            )));
        };
        Ok(row_id.and_then(|row_id| self.relational_store.row_by_id(table, row_id, snapshot)))
    }

    fn required_indexed_visible_row_by_column(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
    ) -> Result<Option<VersionedRow>> {
        let columns = vec![column.to_string()];
        let values = vec![value.clone()];
        self.required_indexed_visible_row_by_columns(
            table,
            &columns,
            &values,
            snapshot,
            skip_deleted,
        )
    }

    fn committed_unique_conflict(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,
    ) -> Result<Option<VersionedRow>> {
        let Some(row_id) = self.committed_unique_conflict_row_id(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            stats,
        )?
        else {
            return Ok(None);
        };
        Ok(self.relational_store.row_by_id(table, row_id, snapshot))
    }

    fn committed_unique_probe_required_for_replacement(
        &self,
        row_id: RowId,
        columns: &[String],
        values: &[Value],
        skip_deleted: &HashSet<RowId>,
        deleted_rows: Option<&HashMap<RowId, VersionedRow>>,
    ) -> bool {
        if !skip_deleted.contains(&row_id) {
            return true;
        }
        !deleted_rows
            .and_then(|rows| rows.get(&row_id))
            .is_some_and(|committed| {
                columns
                    .iter()
                    .zip(values.iter())
                    .all(|(column, value)| committed.values.get(column) == Some(value))
            })
    }

    fn staged_unique_conflict_in_write_set(
        ws: &WriteSet,
        insert_index: usize,
        table: &str,
        columns: &[String],
        values: &[Value],
        stats: Option<&mut CommitStageStats>,
    ) -> Option<RowId> {
        let mut stats = stats;
        for (other_index, (other_table, other_row)) in ws.relational_inserts.iter().enumerate() {
            if other_index == insert_index || other_table != table {
                continue;
            }
            if let Some(stats) = stats.as_deref_mut() {
                stats.staged_vs_staged_comparisons =
                    stats.staged_vs_staged_comparisons.saturating_add(1);
            }
            if ws.relational_inserts[insert_index].1.row_id == other_row.row_id {
                continue;
            }
            if columns
                .iter()
                .zip(values.iter())
                .all(|(column, value)| other_row.values.get(column) == Some(value))
            {
                return Some(other_row.row_id);
            }
        }
        None
    }

    fn indexed_visible_row_exists(
        &self,
        table: &str,
        column: &str,
        value: &Value,
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,
    ) -> Option<bool> {
        use contextdb_core::{DirectedValue, TotalOrdAsc, TotalOrdDesc};

        let indexes = self.relational_store.indexes.read();
        let pk_key = format!("__pk_{column}");
        let unique_key = format!("__unique_{column}");
        let storage = indexes.get(table).and_then(|table_indexes| {
            table_indexes
                .get(&pk_key)
                .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                .or_else(|| {
                    table_indexes
                        .get(&unique_key)
                        .filter(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                })
                .or_else(|| {
                    table_indexes
                        .values()
                        .find(|idx| idx.columns.len() == 1 && idx.columns[0].0 == column)
                })
        })?;
        let key = match storage.columns.first().map(|(_, direction)| direction) {
            Some(SortDirection::Desc) => vec![DirectedValue::Desc(TotalOrdDesc(value.clone()))],
            _ => vec![DirectedValue::Asc(TotalOrdAsc(value.clone()))],
        };
        if let Some(stats) = stats {
            stats.indexed_probes = stats.indexed_probes.saturating_add(1);
        }
        Some(storage.exact_postings(&key).is_some_and(|entries| {
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
        stats: Option<&mut CommitStageStats>,
    ) -> Result<bool> {
        let mut stats = stats;
        if let Some(exists) = self.indexed_visible_row_exists(
            table,
            column,
            value,
            snapshot,
            skip_deleted,
            stats.as_deref_mut(),
        ) {
            return Ok(exists);
        }

        let before_scan_rows = stats
            .as_ref()
            .map(|_| self.relational_store.scan_rows_touched());
        let rows = self
            .relational
            .scan_filter_with_tx(None, table, snapshot, &|row| {
                !skip_deleted.contains(&row.row_id) && row.values.get(column) == Some(value)
            })?;
        if let (Some(stats), Some(before_scan_rows)) = (stats, before_scan_rows) {
            stats.scan_rows_touched = stats.scan_rows_touched.saturating_add(
                self.relational_store
                    .scan_rows_touched()
                    .saturating_sub(before_scan_rows),
            );
        }
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
        self.required_indexed_visible_row_by_column(table, column, value, snapshot, skip_deleted)
    }

    fn indexed_visible_row_exists_by_columns(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,
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
            let Some(storage) = indexes.get(table).and_then(|table_indexes| {
                table_indexes.values().find(|storage| {
                    storage.columns.len() >= columns.len()
                        && (!storage.exact_only() || storage.columns.len() == columns.len())
                        && storage
                            .columns
                            .iter()
                            .zip(columns.iter())
                            .all(|((indexed_column, _), wanted)| indexed_column == wanted)
                })
            }) else {
                self.fk_full_scan_fallbacks.fetch_add(1, Ordering::SeqCst);
                return Err(Error::Other(format!(
                    "composite foreign key probe on {table}({}) has no covering index",
                    columns.join(", ")
                )));
            };

            self.fk_indexed_tuple_probes.fetch_add(1, Ordering::SeqCst);
            if let Some(stats) = stats {
                stats.indexed_probes = stats.indexed_probes.saturating_add(1);
            }
            let prefix = index_key_from_values(&storage.columns[..columns.len()], values);
            if storage.columns.len() == columns.len() {
                storage
                    .exact_postings(&prefix)
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
        Ok(row_id.is_some())
    }

    fn apply_commit_time_upsert(
        &self,
        ws: &mut WriteSet,
        insert_index: usize,
        conflict_row: &VersionedRow,
        intent: &PendingUpsertIntent,
        snapshot: SnapshotId,
    ) -> Result<Option<CommitTimeUpsertRewrite>> {
        if intent.table != ws.relational_inserts[insert_index].0
            || intent.row_id != ws.relational_inserts[insert_index].1.row_id
        {
            return Ok(None);
        }

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
        let mut lookup_delta = StagedTupleLookupDelta::default();
        lookup_delta.remove(&table, &ws.relational_inserts[insert_index].1);
        lookup_delta.add(&table, &replacement);
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
            let mut propagation_options = PreparedPropagationOptions {
                tx: replacement_tx,
                snapshot,
                lookup_delta: Some(&mut lookup_delta),
            };
            self.propagate_state_change_in_prepared_write_set(
                ws,
                &table,
                Some(uuid),
                Some(state),
                &mut propagation_options,
            )?;
        }
        Ok(Some(CommitTimeUpsertRewrite {
            table,
            row_id: conflict_row.row_id,
            lookup_delta,
        }))
    }

    fn relational_insert_index_by_row(
        ws: &WriteSet,
        table: &str,
        row_id: RowId,
        preferred_index: usize,
    ) -> Option<usize> {
        if ws
            .relational_inserts
            .get(preferred_index)
            .is_some_and(|(candidate_table, row)| candidate_table == table && row.row_id == row_id)
        {
            return Some(preferred_index);
        }
        if let Some(index) = preferred_index.checked_sub(1)
            && ws
                .relational_inserts
                .get(index)
                .is_some_and(|(candidate_table, row)| {
                    candidate_table == table && row.row_id == row_id
                })
        {
            return Some(index);
        }
        ws.relational_inserts
            .iter()
            .position(|(candidate_table, row)| candidate_table == table && row.row_id == row_id)
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
        upsert_lookup: &CommitTimeUpsertLookup<'_>,
        snapshot: SnapshotId,
        stats: Option<&mut CommitStageStats>,
    ) -> Result<Option<CommitTimeUpsertRewrite>> {
        let mut stats = stats;
        let (table, row) = ws.relational_inserts[insert_index].clone();
        let Some(intent_indexes) = upsert_lookup.by_row.get(&(table.clone(), row.row_id)) else {
            return Ok(None);
        };
        for intent_index in intent_indexes {
            let intent = &upsert_lookup.intents[*intent_index];
            if intent.conflict_columns.is_empty() {
                continue;
            }
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
                stats.as_deref_mut(),
            )?
            else {
                continue;
            };
            if let Some(replacement) =
                self.apply_commit_time_upsert(ws, insert_index, &conflict, intent, snapshot)?
            {
                return Ok(Some(replacement));
            }
        }

        Ok(None)
    }

    fn validate_unique_constraints_in_write_set(
        &self,
        ws: &mut WriteSet,
        snapshot: SnapshotId,
        upsert_intents: &[PendingUpsertIntent],
        deleted_rows_by_table: &mut HashMap<String, HashMap<RowId, VersionedRow>>,
        stats: &mut CommitStageStats,
    ) -> Result<()> {
        let upsert_lookup = CommitTimeUpsertLookup {
            intents: upsert_intents,
            by_row: Self::upsert_intent_indexes_by_row(upsert_intents),
        };
        let upsert_intent_by_conflict = Self::upsert_intent_index_by_conflict(upsert_intents);
        let staged_tuple_columns = self.staged_tuple_key_columns_by_table();
        let mut staged_lookup = StagedTupleLookup::from_write_set(&staged_tuple_columns, ws);
        let mut deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        let mut index = 0;
        'validate: while index < ws.relational_inserts.len() {
            let (table, row) = ws.relational_inserts[index].clone();
            let meta = self
                .table_meta(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            let skip_deleted = deleted_by_table.get(&table).unwrap_or(&empty_deleted);
            stats.rows_validated = stats.rows_validated.saturating_add(1);

            if let Some(rewrite) = self.apply_original_commit_time_upsert_if_needed(
                ws,
                index,
                skip_deleted,
                &upsert_lookup,
                snapshot,
                Some(stats),
            )? {
                staged_lookup.apply_delta(&staged_tuple_columns, &rewrite.lookup_delta);
                deleted_by_table = Self::deleted_row_ids_by_table(ws);
                *deleted_rows_by_table = self.deleted_row_snapshots_by_table(ws);
                index =
                    Self::relational_insert_index_by_row(ws, &rewrite.table, rewrite.row_id, index)
                        .unwrap_or_else(|| index.min(ws.relational_inserts.len()));
                continue 'validate;
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
                let staged_conflict = if Self::values_can_be_tuple_key(&values) {
                    staged_lookup
                        .staged_unique_conflict(&table, row.row_id, &columns, &values, stats)
                } else {
                    Self::staged_unique_conflict_in_write_set(
                        ws,
                        index,
                        &table,
                        &columns,
                        &values,
                        Some(stats),
                    )
                };
                if staged_conflict.is_some() {
                    return Err(Error::UniqueViolation {
                        table,
                        column: column.name.clone(),
                    });
                }
                let upsert_intent_index = upsert_intent_by_conflict
                    .get(&(table.clone(), row.row_id, columns.clone()))
                    .copied();
                let deleted_rows = deleted_rows_by_table.get(&table);
                let conflict = if self.committed_unique_probe_required_for_replacement(
                    row.row_id,
                    &columns,
                    &values,
                    skip_deleted,
                    deleted_rows,
                ) {
                    if upsert_intent_index.is_some() {
                        self.committed_unique_conflict(
                            &table,
                            &columns,
                            &values,
                            snapshot,
                            skip_deleted,
                            Some(stats),
                        )?
                    } else if self
                        .committed_unique_conflict_row_id(
                            &table,
                            &columns,
                            &values,
                            snapshot,
                            skip_deleted,
                            Some(stats),
                        )?
                        .is_some()
                    {
                        Some(VersionedRow {
                            row_id: RowId(0),
                            values: HashMap::new(),
                            created_tx: TxId(0),
                            deleted_tx: None,
                            lsn: Lsn(0),
                            created_at: None,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(conflict) = conflict {
                    if let Some(intent_index) = upsert_intent_index
                        && let Some(rewrite) = self.apply_commit_time_upsert(
                            ws,
                            index,
                            &conflict,
                            &upsert_intents[intent_index],
                            snapshot,
                        )?
                    {
                        staged_lookup.apply_delta(&staged_tuple_columns, &rewrite.lookup_delta);
                        deleted_by_table = Self::deleted_row_ids_by_table(ws);
                        *deleted_rows_by_table = self.deleted_row_snapshots_by_table(ws);
                        index = Self::relational_insert_index_by_row(
                            ws,
                            &rewrite.table,
                            rewrite.row_id,
                            index,
                        )
                        .unwrap_or_else(|| index.min(ws.relational_inserts.len()));
                        continue 'validate;
                    }
                    return Err(Error::UniqueViolation {
                        table,
                        column: column.name.clone(),
                    });
                }
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
                let staged_conflict = if Self::values_can_be_tuple_key(&values) {
                    staged_lookup.staged_unique_conflict(
                        &table,
                        row.row_id,
                        unique_constraint,
                        &values,
                        stats,
                    )
                } else {
                    Self::staged_unique_conflict_in_write_set(
                        ws,
                        index,
                        &table,
                        unique_constraint,
                        &values,
                        Some(stats),
                    )
                };
                if staged_conflict.is_some() {
                    return Err(Error::UniqueViolation {
                        table,
                        column: unique_constraint.first().cloned().unwrap_or_default(),
                    });
                }
                let upsert_intent_index = upsert_intent_by_conflict
                    .get(&(table.clone(), row.row_id, unique_constraint.clone()))
                    .copied();
                let deleted_rows = deleted_rows_by_table.get(&table);
                let conflict = if self.committed_unique_probe_required_for_replacement(
                    row.row_id,
                    unique_constraint,
                    &values,
                    skip_deleted,
                    deleted_rows,
                ) {
                    if upsert_intent_index.is_some() {
                        self.committed_unique_conflict(
                            &table,
                            unique_constraint,
                            &values,
                            snapshot,
                            skip_deleted,
                            Some(stats),
                        )?
                    } else if self
                        .committed_unique_conflict_row_id(
                            &table,
                            unique_constraint,
                            &values,
                            snapshot,
                            skip_deleted,
                            Some(stats),
                        )?
                        .is_some()
                    {
                        Some(VersionedRow {
                            row_id: RowId(0),
                            values: HashMap::new(),
                            created_tx: TxId(0),
                            deleted_tx: None,
                            lsn: Lsn(0),
                            created_at: None,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(conflict) = conflict {
                    if let Some(intent_index) = upsert_intent_index
                        && let Some(rewrite) = self.apply_commit_time_upsert(
                            ws,
                            index,
                            &conflict,
                            &upsert_intents[intent_index],
                            snapshot,
                        )?
                    {
                        staged_lookup.apply_delta(&staged_tuple_columns, &rewrite.lookup_delta);
                        deleted_by_table = Self::deleted_row_ids_by_table(ws);
                        *deleted_rows_by_table = self.deleted_row_snapshots_by_table(ws);
                        index = Self::relational_insert_index_by_row(
                            ws,
                            &rewrite.table,
                            rewrite.row_id,
                            index,
                        )
                        .unwrap_or_else(|| index.min(ws.relational_inserts.len()));
                        continue 'validate;
                    }
                    return Err(Error::UniqueViolation {
                        table,
                        column: unique_constraint.first().cloned().unwrap_or_default(),
                    });
                }
            }

            index += 1;
        }
        Ok(())
    }

    fn validate_foreign_keys_in_write_set(
        &self,
        ws: &WriteSet,
        snapshot: SnapshotId,
        deleted_rows_by_table: &HashMap<String, HashMap<RowId, VersionedRow>>,
        stats: &mut CommitStageStats,
    ) -> Result<()> {
        let staged_lookup = StagedTupleLookup::rebuild(self, ws)?;
        let deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        for (table, row) in &ws.relational_inserts {
            stats.rows_validated = stats.rows_validated.saturating_add(1);
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
                let staged_deletes = deleted_by_table
                    .get(&reference.table)
                    .unwrap_or(&empty_deleted);
                let parent_columns = vec![reference.column.clone()];
                let parent_values = std::slice::from_ref(value);
                let staged_match = if Self::values_can_be_tuple_key(parent_values) {
                    staged_lookup.contains_tuple(
                        &reference.table,
                        &parent_columns,
                        parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, &reference.table, &parent_columns, parent_values)
                };
                let committed_match = if staged_match {
                    true
                } else {
                    self.visible_row_exists_by_column(
                        &reference.table,
                        &reference.column,
                        value,
                        snapshot,
                        staged_deletes,
                        Some(stats),
                    )?
                };
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
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let Some(parent_row) = deleted_rows_by_table
                .get(parent_table)
                .and_then(|rows| rows.get(parent_row_id))
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
                if *parent_value == Value::Null {
                    continue;
                }
                let parent_columns = vec![ref_column.clone()];
                let parent_values = std::slice::from_ref(parent_value);
                let parent_replaced_with_same_key = if Self::values_can_be_tuple_key(parent_values)
                {
                    staged_lookup.contains_tuple(
                        parent_table,
                        &parent_columns,
                        parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, parent_table, &parent_columns, parent_values)
                };
                if parent_replaced_with_same_key {
                    continue;
                }
                let child_deletes = deleted_by_table.get(child_table).unwrap_or(&empty_deleted);
                if self.visible_row_exists_by_column(
                    child_table,
                    child_column,
                    parent_value,
                    snapshot,
                    child_deletes,
                    Some(stats),
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
        deleted_rows_by_table: &HashMap<String, HashMap<RowId, VersionedRow>>,
        stats: &mut CommitStageStats,
    ) -> Result<()> {
        let staged_lookup = StagedTupleLookup::rebuild(self, ws)?;
        let deleted_by_table = Self::deleted_row_ids_by_table(ws);
        let empty_deleted = HashSet::new();
        for (table, row) in &ws.relational_inserts {
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let meta = self
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            for fk in &meta.composite_foreign_keys {
                let Some(values) = tuple_values_for_row(row, &fk.child_columns) else {
                    continue;
                };
                let parent_deletes = deleted_by_table
                    .get(&fk.parent_table)
                    .unwrap_or(&empty_deleted);
                let staged_match = if Self::values_can_be_tuple_key(&values) {
                    staged_lookup.contains_tuple(
                        &fk.parent_table,
                        &fk.parent_columns,
                        &values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, &fk.parent_table, &fk.parent_columns, &values)
                };
                let committed_match = if staged_match {
                    true
                } else {
                    self.indexed_visible_row_exists_by_columns(
                        &fk.parent_table,
                        &fk.parent_columns,
                        &values,
                        snapshot,
                        parent_deletes,
                        Some(stats),
                    )?
                };
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
            stats.rows_validated = stats.rows_validated.saturating_add(1);
            let Some(parent_row) = deleted_rows_by_table
                .get(parent_table)
                .and_then(|rows| rows.get(parent_row_id))
            else {
                continue;
            };
            for (ref_table, ref_columns, child_table, child_columns) in &reverse_refs {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_values) = tuple_values_for_row(parent_row, ref_columns) else {
                    continue;
                };
                let parent_replaced_with_same_key = if Self::values_can_be_tuple_key(&parent_values)
                {
                    staged_lookup.contains_tuple(
                        parent_table,
                        ref_columns,
                        &parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, parent_table, ref_columns, &parent_values)
                };
                if parent_replaced_with_same_key {
                    continue;
                }

                let staged_child_match = if Self::values_can_be_tuple_key(&parent_values) {
                    staged_lookup.contains_tuple(
                        child_table,
                        child_columns,
                        &parent_values,
                        Some(stats),
                    )
                } else {
                    staged_tuple_exists(ws, child_table, child_columns, &parent_values)
                };
                if staged_child_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: child_columns.clone(),
                        parent_table: parent_table.clone(),
                        parent_columns: ref_columns.clone(),
                    });
                }

                let child_deletes = deleted_by_table.get(child_table).unwrap_or(&empty_deleted);
                if self.indexed_visible_row_exists_by_columns(
                    child_table,
                    child_columns,
                    &parent_values,
                    snapshot,
                    child_deletes,
                    Some(stats),
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
        mut stats: Option<&mut CommitStageStats>,
    ) -> Result<CommitValidationOutcome> {
        let mut outcome = CommitValidationOutcome::default();
        for guard in guards.iter().rev() {
            if let Some(stats) = stats.as_deref_mut() {
                stats.rows_validated = stats.rows_validated.saturating_add(1);
            }
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
            if guard.fail_on_conflict {
                outcome.conditional_conflict_count =
                    outcome.conditional_conflict_count.saturating_add(1);
            } else {
                outcome.conditional_noop_count = outcome.conditional_noop_count.saturating_add(1);
            }
        }
        Ok(outcome)
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
            if !self.propagation_rules_can_react(table, state) {
                return Ok(());
            }
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

    pub(crate) fn propagation_rules_can_react(&self, table: &str, state: &str) -> bool {
        let metas = self.relational_store().table_meta.read();
        if metas.get(table).is_some_and(|meta| {
            meta.propagation_rules.iter().any(|rule| match rule {
                PropagationRule::Edge { trigger_state, .. }
                | PropagationRule::VectorExclusion { trigger_state } => trigger_state == state,
                PropagationRule::ForeignKey { .. } => false,
            })
        }) {
            return true;
        }
        metas.values().any(|meta| {
            meta.propagation_rules.iter().any(|rule| {
                matches!(
                    rule,
                    PropagationRule::ForeignKey {
                        referenced_table,
                        trigger_state,
                        ..
                    } if referenced_table == table && trigger_state == state
                )
            })
        })
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
        table: &str,
        row_uuid: Option<uuid::Uuid>,
        new_state: Option<&str>,
        options: &mut PreparedPropagationOptions<'_>,
    ) -> Result<()> {
        let (Some(uuid), Some(state)) = (row_uuid, new_state) else {
            return Ok(());
        };
        if !self.propagation_rules_can_react(table, state) {
            return Ok(());
        }
        if ws.propagation_in_progress {
            return Ok(());
        }

        ws.propagation_in_progress = true;
        let result = self.propagate_in_prepared_write_set(ws, table, uuid, state, options);
        ws.propagation_in_progress = false;
        result
    }

    fn propagate_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        table: &str,
        row_uuid: uuid::Uuid,
        new_state: &str,
        options: &mut PreparedPropagationOptions<'_>,
    ) -> Result<()> {
        let metas = self.relational_store().table_meta.read().clone();
        let mut queue: VecDeque<PropagationQueueEntry> = VecDeque::new();
        let mut visited: HashSet<(String, uuid::Uuid)> = HashSet::new();
        let mut abort_violation: Option<Error> = None;
        let ctx = PropagationContext {
            tx: options.tx,
            snapshot: options.snapshot,
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
                options.snapshot,
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
                options.snapshot,
            )?;
            self.assert_row_write_allowed(
                &entry.table,
                existing.row_id,
                &next_values,
                options.snapshot,
            )?;

            let reached_state = match self.upsert_row_in_prepared_write_set(
                ws,
                &entry.table,
                &state_column,
                &existing,
                next_values,
                options,
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
        result.retain(|row| {
            !deleted_row_ids.contains(&row.row_id)
                && !row_matches_delete_predicates(&ws.relational_delete_predicates, table, row)
        });

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
        table: &str,
        state_column: &str,
        existing: &VersionedRow,
        next_values: HashMap<ColName, Value>,
        options: &mut PreparedPropagationOptions<'_>,
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

        if let Err(err) = self.delete_row_in_prepared_write_set(
            ws,
            options.tx,
            table,
            existing.row_id,
            options.lookup_delta.as_deref_mut(),
        ) {
            self.accountant.release(row_bytes);
            return Err(err);
        }

        let inserted = VersionedRow {
            row_id: existing.row_id,
            values: next_values,
            created_tx: options.tx,
            deleted_tx: None,
            lsn: ws.commit_lsn.unwrap_or(Lsn(0)),
            created_at: Some(current_wallclock()),
        };
        if let Some(delta) = options.lookup_delta.as_deref_mut() {
            delta.add(table, &inserted);
        }
        ws.relational_inserts.push((table.to_string(), inserted));
        Ok(true)
    }

    fn delete_row_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        tx: TxId,
        table: &str,
        row_id: RowId,
        mut lookup_delta: Option<&mut StagedTupleLookupDelta>,
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
            if let Some(delta) = lookup_delta.as_deref_mut() {
                delta.remove(&removed_table, &row);
            }
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
        let trigger_callback_bound = self.trigger_callback_tx_bound_matches(tx);
        let _operation = if trigger_callback_bound || self.sql_write_control_bypass_active(tx) {
            None
        } else {
            Some(self.open_operation_after_write_control_wait(tx, "delete_row")?)
        };
        self.ensure_trigger_table_ready(table, "delete_row")?;
        self.assert_row_id_write_allowed(Some(tx), table, row_id, self.snapshot_for_read())?;
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

    pub(crate) fn record_relational_delete_predicate(
        &self,
        tx: TxId,
        table: String,
        predicates: Vec<(ColName, Value)>,
    ) -> Result<()> {
        if predicates.is_empty() {
            return Ok(());
        }
        self.tx_mgr.with_write_set(tx, |ws| {
            let predicate = RelationalDeletePredicate { table, predicates };
            if !ws.relational_delete_predicates.contains(&predicate) {
                ws.relational_delete_predicates.push(predicate);
            }
        })
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
        let deleted_row_ids = self
            .tx_mgr
            .with_write_set(tx, |ws| Self::deleted_row_ids_for_table(ws, table))?;
        if let Some(committed) = self.committed_unique_conflict(
            table,
            columns,
            values,
            snapshot,
            &deleted_row_ids,
            None,
        )? {
            return Ok(Some(committed));
        }

        self.tx_mgr.with_write_set(tx, |ws| {
            let mut seen_inserts = HashSet::new();
            ws.relational_inserts
                .iter()
                .rev()
                .filter(|(insert_table, row)| {
                    insert_table == table
                        && seen_inserts.insert(row.row_id)
                        && (!deleted_row_ids.contains(&row.row_id)
                            || self
                                .relational_store
                                .row_by_id(table, row.row_id, snapshot)
                                .is_some())
                })
                .find(|(_, row)| {
                    columns
                        .iter()
                        .zip(values.iter())
                        .all(|(column, value)| row.values.get(column) == Some(value))
                })
                .map(|(_, row)| row.clone())
        })
    }

    pub(crate) fn unique_row_lookup_in_tx(
        &self,
        tx: TxId,
        table: &str,
        columns: &[ColName],
        values: &[Value],
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        if columns.is_empty() || columns.len() != values.len() {
            return Ok(None);
        }

        let skip_deleted = HashSet::new();
        if let Some(committed) =
            self.committed_unique_conflict(table, columns, values, snapshot, &skip_deleted, None)?
        {
            let deleted_in_tx = self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_deletes
                    .iter()
                    .any(|(delete_table, row_id, _)| {
                        delete_table == table && *row_id == committed.row_id
                    })
            })?;
            if !deleted_in_tx {
                return Ok(Some(committed));
            }
        }

        if let Some(staged) = self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_inserts
                .iter()
                .rev()
                .find_map(|(insert_table, row)| {
                    if insert_table != table
                        || tuple_values_for_row(row, columns).as_deref() != Some(values)
                    {
                        return None;
                    }
                    let deleted_in_tx =
                        ws.relational_deletes
                            .iter()
                            .any(|(delete_table, row_id, _)| {
                                delete_table == table && *row_id == row.row_id
                            });
                    let replaces_committed_row = deleted_in_tx
                        && self
                            .relational_store
                            .row_by_id(table, row.row_id, snapshot)
                            .is_some();
                    (!deleted_in_tx || replaces_committed_row).then(|| row.clone())
                })
        })? {
            return Ok(Some(staged));
        }

        Ok(None)
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
        let _operation = self.open_operation_after_write_control_wait(tx, "insert_edge")?;
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
        let _operation = self.open_operation_after_write_control_wait(tx, "delete_edge")?;
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
        let _operation = self.open_operation_after_write_control_wait(tx, "insert_vector")?;
        let _vector_schema = self.vector_schema_read(&index);
        self.ensure_trigger_table_ready(&index.table, "insert_vector")?;
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
        let _vector_schema = self.vector_schema_read(&index);
        self.record_vector_schema_epoch(tx, &index)?;
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
        let _operation = self.open_operation_after_write_control_wait(tx, "delete_vector")?;
        let _vector_schema = self.vector_schema_read(&index);
        self.record_vector_schema_epoch(tx, &index)?;
        self.ensure_trigger_table_ready(&index.table, "delete_vector")?;
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
        let _vector_schema = self.vector_schema_read(&index);
        self.record_vector_schema_epoch(tx, &index)?;
        self.ensure_trigger_table_ready(&index.table, "move_vector")?;
        if self.vector_store.try_state(&index).is_none() {
            return Err(Error::UnknownVectorIndex { index });
        }
        if old_row_id == new_row_id {
            return Ok(());
        }
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
        let _vector_schema = self.vector_schema_read(&index);
        if self.vector_store.try_state(&index).is_none() {
            return Err(Error::UnknownVectorIndex { index });
        }
        let effective_candidates =
            self.effective_read_candidates(&index.table, snapshot, candidates)?;
        let (rows, trace) = self.vector.search_with_strategy_for_test(
            index,
            query,
            k,
            effective_candidates.as_ref(),
            snapshot,
        )?;
        self.last_vector_search_used_hnsw
            .store(trace.used_hnsw, Ordering::SeqCst);
        *self.last_vector_search_trace.write() = Some(trace);
        Ok(rows)
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
        let _vector_schema = self.vector_schema_read(&index);
        self.semantic_search_with_candidates_under_schema_read(query, candidates)
    }

    pub(crate) fn semantic_search_with_candidates_under_schema_read(
        &self,
        query: SemanticQuery,
        candidates: Option<RoaringTreemap>,
    ) -> Result<Vec<SearchResult>> {
        self.semantic_search_with_candidates_under_schema_read_with_strategy(query, candidates)
            .map(|(results, _)| results)
    }

    pub(crate) fn semantic_search_with_candidates_under_schema_read_with_strategy(
        &self,
        query: SemanticQuery,
        candidates: Option<RoaringTreemap>,
    ) -> Result<(Vec<SearchResult>, bool)> {
        self.semantic_search_with_candidates_under_schema_read_in_tx_with_strategy(
            None, query, candidates,
        )
    }

    pub(crate) fn semantic_search_with_candidates_under_schema_read_in_tx_with_strategy(
        &self,
        tx: Option<TxId>,
        query: SemanticQuery,
        candidates: Option<RoaringTreemap>,
    ) -> Result<(Vec<SearchResult>, bool)> {
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
                self.vector_entry_count_in_tx(tx, &index)?.max(query.limit)
            } else {
                query.limit
            };
            let (mut rows, used_hnsw) = self.query_vector_strict_in_tx_with_strategy(
                tx,
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
            let results = rows
                .into_iter()
                .map(|(row_id, vector_score)| {
                    let anchor = self.find_row_by_id_in_tx(tx, &query.table, row_id, snapshot)?;
                    let values = self.search_result_values_in_tx(
                        tx,
                        &index,
                        row_id,
                        snapshot,
                        anchor.values,
                    )?;
                    Ok(SearchResult {
                        row_id,
                        values,
                        vector_score,
                        rank: vector_score,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok((results, used_hnsw));
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
        let entry_count = self.vector_entry_count_in_tx(tx, &index)?;
        let internal_k = self.rank_policy_candidate_k(entry_count, query.limit);
        let (mut raw, used_hnsw) = self.query_vector_strict_in_tx_with_strategy(
            tx,
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
            let anchor = self.find_row_by_id_in_tx(tx, &query.table, row_id, snapshot)?;
            let joined = self.joined_row_for_rank_policy(tx, policy, &anchor, snapshot)?;
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
            let values = self.search_result_values_in_tx(
                tx,
                &index,
                row_id,
                snapshot,
                merged_rank_values(&anchor, joined.as_ref()),
            )?;
            ranked.push(SearchResult {
                row_id,
                values,
                vector_score,
                rank,
            });
        }
        ranked.sort_by(compare_ranked_results);
        ranked.truncate(query.limit);
        Ok((ranked, used_hnsw))
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

    /// Acceptance-test accessor: threads currently in this database's trigger
    /// callback owner map. Zero at rest — nonzero after every callback
    /// completed means `TriggerCallbackThreadGuard::drop` leaked an entry.
    #[doc(hidden)]
    pub fn trigger_callback_owner_thread_count_for_test(&self) -> usize {
        self.trigger.owner_thread_count()
    }

    /// Acceptance-test accessor. Reads at steady state (after stop signal
    /// + writer-threads joined). Snapshots are not atomic across counters.
    #[doc(hidden)]
    pub fn trigger_progress_telemetry_snapshot_for_test(&self) -> TriggerProgressTelemetrySnapshot {
        TriggerProgressTelemetrySnapshot {
            wait_observed: self.trigger.wait_observed_count.load(Ordering::SeqCst),
            typed_err_observed_same_db: self
                .trigger
                .typed_err_observed_same_db_count
                .load(Ordering::SeqCst),
            deadlock_guard_timeout_observed: self
                .trigger
                .deadlock_guard_timeout_observed_count
                .load(Ordering::SeqCst),
        }
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

    pub(crate) fn query_vector_strict_in_tx_with_strategy(
        &self,
        tx: Option<TxId>,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<(Vec<(RowId, f32)>, bool)> {
        self.vector_store.validate_vector(&index, query.len())?;
        if let Some(tx) = tx {
            let overlay_result = self.tx_mgr.with_write_set(tx, |ws| {
                if write_set_touches_vector_search(ws, &index) {
                    Some(self.query_vector_strict_with_write_set_exact(
                        ws,
                        index.clone(),
                        query,
                        k,
                        candidates,
                        snapshot,
                    ))
                } else {
                    None
                }
            })?;
            if let Some(result) = overlay_result {
                return result;
            }
        }
        let effective_candidates =
            self.effective_read_candidates(&index.table, snapshot, candidates)?;
        let (rows, trace) = self.vector.search_with_strategy_for_test(
            index,
            query,
            k,
            effective_candidates.as_ref(),
            snapshot,
        )?;
        self.last_vector_search_used_hnsw
            .store(trace.used_hnsw, Ordering::SeqCst);
        let used_hnsw = trace.used_hnsw;
        *self.last_vector_search_trace.write() = Some(trace);
        Ok((rows, used_hnsw))
    }

    fn query_vector_strict_with_write_set_exact(
        &self,
        ws: &WriteSet,
        index: VectorIndexRef,
        query: &[f32],
        k: usize,
        candidates: Option<&RoaringTreemap>,
        snapshot: SnapshotId,
    ) -> Result<(Vec<(RowId, f32)>, bool)> {
        if k == 0 {
            return Ok((Vec::new(), false));
        }
        let state = self.vector_store.state(&index)?;
        let staged_entries = ws
            .vector_inserts
            .iter()
            .filter(|entry| entry.index == index && entry.deleted_tx.is_none())
            .count();
        let overlay_bytes = estimate_active_tx_vector_overlay_bytes(
            state.entry_count().saturating_add(staged_entries),
            state.dimension(),
        );
        self.accountant.try_allocate_for(
            overlay_bytes,
            "query",
            &format!("active_tx_vector_overlay@{}.{}", index.table, index.column),
            "Reduce active transaction vector search scope or raise MEMORY_LIMIT.",
        )?;

        let result = (|| -> Result<(Vec<(RowId, f32)>, bool)> {
            let mut entries: HashMap<RowId, Vec<f32>> = HashMap::new();
            for entry in self.vector_store.entries_for_index(&index)? {
                if entry.visible_at(snapshot) {
                    entries.insert(entry.row_id, entry.vector);
                }
            }

            for (delete_table, row_id, _) in &ws.relational_deletes {
                let has_same_row_replacement =
                    ws.relational_inserts.iter().any(|(insert_table, row)| {
                        insert_table == &index.table && row.row_id == *row_id
                    });
                if delete_table == &index.table && !has_same_row_replacement {
                    entries.remove(row_id);
                }
            }
            for (delete_index, row_id, _) in &ws.vector_deletes {
                if delete_index == &index {
                    entries.remove(row_id);
                }
            }
            for (move_index, from_row_id, to_row_id, _) in &ws.vector_moves {
                if move_index == &index
                    && let Some(vector) = entries.remove(from_row_id)
                {
                    entries.insert(*to_row_id, vector);
                }
            }
            for entry in &ws.vector_inserts {
                if entry.index == index && entry.deleted_tx.is_none() {
                    entries.insert(entry.row_id, entry.vector.clone());
                }
            }

            let mut scored = Vec::with_capacity(entries.len().min(k));
            for (row_id, vector) in entries {
                if let Some(candidates) = candidates
                    && !candidates.contains(row_id.0)
                {
                    continue;
                }
                if !self.row_id_read_allowed_in_write_set_for_query(
                    ws,
                    &index.table,
                    row_id,
                    snapshot,
                )? {
                    continue;
                }
                scored.push((row_id, cosine_similarity(&vector, query)));
            }
            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            scored.truncate(k);
            Ok((scored, false))
        })();
        self.accountant.release(overlay_bytes);
        result
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

    fn vector_entry_count_in_tx(&self, tx: Option<TxId>, index: &VectorIndexRef) -> Result<usize> {
        let base = self.vector_entry_count(index);
        let Some(tx) = tx else {
            return Ok(base);
        };
        self.tx_mgr.with_write_set(tx, |ws| {
            base.saturating_add(
                ws.vector_inserts
                    .iter()
                    .filter(|entry| entry.index == *index && entry.deleted_tx.is_none())
                    .count(),
            )
        })
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
        tx: Option<TxId>,
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
            .get(&policy.joined_table)
            .and_then(|table_indexes| table_indexes.get(&policy.protected_index))
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
        if let Some(tx) = tx {
            drop(indexes);
            return self.joined_row_for_rank_policy_in_tx(tx, policy, &join_value, snapshot);
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
            if let Some(entries) = storage.exact_postings(&vec![key_component.clone()]) {
                consider(entries);
            }
        } else if !storage.exact_only() {
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

    fn joined_row_for_rank_policy_in_tx(
        &self,
        tx: TxId,
        policy: &RankPolicy,
        join_value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        let rows = self.scan_in_tx_raw(tx, &policy.joined_table, snapshot)?;
        let mut best: Option<VersionedRow> = None;
        for row in rows {
            let Some(value) = row.values.get(&policy.joined_column) else {
                continue;
            };
            if !values_equal_for_rank_join(value, join_value) {
                continue;
            }
            if !self.row_read_allowed_for_change(&policy.joined_table, &row, snapshot) {
                continue;
            }
            if best
                .as_ref()
                .is_none_or(|current| current.row_id < row.row_id)
            {
                best = Some(row);
            }
        }
        Ok(best)
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

    fn search_result_values_in_tx(
        &self,
        tx: Option<TxId>,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
        mut values: HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>> {
        if let Some(entry) = self.vector_entry_for_row_in_tx(tx, index, row_id, snapshot)? {
            values.insert(index.column.clone(), Value::Vector(entry.vector));
        }
        Ok(values)
    }

    fn pending_vector_dimension(&self, tx: TxId, index: &VectorIndexRef) -> Result<Option<usize>> {
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.vector_inserts
                .iter()
                .rev()
                .find(|entry| entry.index == *index && entry.deleted_tx.is_none())
                .map(|entry| entry.vector.len())
        })
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
    pub fn __debug_last_query_vector_used_hnsw_for_test(&self) -> bool {
        let _operation = self.assert_open_operation();
        self.last_vector_search_used_hnsw.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    pub fn __debug_last_query_vector_trace_for_test(&self) -> Option<VectorSearchDebugTrace> {
        let _operation = self.assert_open_operation();
        self.last_vector_search_trace.read().clone()
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_len(&self, index: VectorIndexRef) -> Option<usize> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        self.vector_hnsw_len_under_schema_read(&index)
    }

    pub(crate) fn vector_hnsw_len_under_schema_read(
        &self,
        index: &VectorIndexRef,
    ) -> Option<usize> {
        self.vector_store
            .try_state(index)
            .and_then(|state| state.hnsw_len())
    }

    pub(crate) fn assert_vector_index_exists_under_schema_read(
        &self,
        index: &VectorIndexRef,
    ) -> Result<()> {
        if self.vector_store.try_state(index).is_some() {
            Ok(())
        } else {
            Err(Error::UnknownVectorIndex {
                index: index.clone(),
            })
        }
    }

    pub(crate) fn validate_vector_under_schema_read(
        &self,
        index: &VectorIndexRef,
        actual: usize,
    ) -> Result<()> {
        self.vector_store.validate_vector(index, actual)
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_stats(&self, index: VectorIndexRef) -> Option<HnswGraphStats> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
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
        let _vector_schema = self.vector_schema_read(&index);
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
        let _vector_schema = self.vector_schema_read(&index);
        self.vector_store
            .raw_hnsw_entry_count_for_row(&index, row_id)
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_topology_digest_for_test(
        &self,
        index: VectorIndexRef,
    ) -> Option<u64> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        self.vector_store.raw_hnsw_topology_digest_for_test(&index)
    }

    #[doc(hidden)]
    pub fn __debug_vector_hnsw_build_serial_for_test(&self, index: VectorIndexRef) -> Option<u64> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read(&index);
        self.vector_store.raw_hnsw_build_serial_for_test(&index)
    }

    #[doc(hidden)]
    pub fn __debug_vector_storage_bytes_per_entry(
        &self,
        index: VectorIndexRef,
    ) -> Result<Vec<usize>> {
        let _operation = self.open_operation()?;
        let _vector_schema = self.vector_schema_read(&index);
        self.vector_store.storage_bytes_per_entry(&index)
    }

    pub fn has_live_vector(&self, row_id: RowId, snapshot: SnapshotId) -> bool {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read_many(self.vector_store_schema_refs());
        self.vector_store
            .live_entries_for_row(row_id, snapshot)
            .into_iter()
            .any(|entry| self.row_id_read_allowed_for_change(&entry.index.table, row_id, snapshot))
    }

    pub fn live_vector_entry(&self, row_id: RowId, snapshot: SnapshotId) -> Option<VectorEntry> {
        let _operation = self.assert_open_operation();
        let _vector_schema = self.vector_schema_read_many(self.vector_store_schema_refs());
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

    pub(crate) fn natural_key_column_for_table(&self, table: &str) -> Result<String> {
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
        natural_key_column_for_meta(&meta).ok_or_else(|| {
            Error::PlanError(format!(
                "ROW_VECTOR source table `{table}` has no natural key"
            ))
        })
    }

    pub(crate) fn row_id_for_natural_key_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        key_col: &str,
        key_value: &Value,
        snapshot: SnapshotId,
    ) -> Result<Option<RowId>> {
        if let Some(tx) = tx {
            let staged = self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_inserts
                    .iter()
                    .rev()
                    .find(|(insert_table, row)| {
                        insert_table == table && row.values.get(key_col) == Some(key_value)
                    })
                    .map(|(_, row)| row.row_id)
            })?;
            if staged.is_some() {
                return Ok(staged);
            }
        }

        let Some(row_id) = self.row_id_for_natural_key(table, key_col, key_value, snapshot) else {
            return Ok(None);
        };

        if let Some(tx) = tx {
            let deleted = self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == table && *deleted_row_id == row_id
                    })
            })?;
            if deleted {
                return Ok(None);
            }
        }

        Ok(Some(row_id))
    }

    pub(crate) fn vector_entry_for_row_in_tx(
        &self,
        tx: Option<TxId>,
        index: &VectorIndexRef,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<Option<VectorEntry>> {
        if let Some(tx) = tx {
            enum VectorEntryOverlay {
                Found(VectorEntry),
                Deleted,
                Moved { from: RowId, to: RowId },
                Unchanged,
            }

            let overlay =
                self.tx_mgr.with_write_set(tx, |ws| {
                    if let Some(entry) = ws
                        .vector_inserts
                        .iter()
                        .rev()
                        .find(|entry| {
                            entry.index == *index
                                && entry.row_id == row_id
                                && entry.deleted_tx.is_none()
                        })
                        .cloned()
                    {
                        return VectorEntryOverlay::Found(entry);
                    }
                    if ws
                        .vector_deletes
                        .iter()
                        .rev()
                        .any(|(deleted_index, deleted_row_id, _)| {
                            deleted_index == index && *deleted_row_id == row_id
                        })
                    {
                        return VectorEntryOverlay::Deleted;
                    }
                    if let Some((_, from_row_id, to_row_id, _)) = ws.vector_moves.iter().rev().find(
                        |(moved_index, from_row_id, to_row_id, _)| {
                            moved_index == index && (*from_row_id == row_id || *to_row_id == row_id)
                        },
                    ) {
                        return VectorEntryOverlay::Moved {
                            from: *from_row_id,
                            to: *to_row_id,
                        };
                    }
                    VectorEntryOverlay::Unchanged
                })?;
            match overlay {
                VectorEntryOverlay::Found(entry) => return Ok(Some(entry)),
                VectorEntryOverlay::Deleted => return Ok(None),
                VectorEntryOverlay::Moved { from, to } => {
                    if from == row_id {
                        return Ok(None);
                    }
                    let mut entry = self.vector_store_live_entry_for_row(index, from, snapshot);
                    if let Some(entry) = entry.as_mut() {
                        entry.row_id = to;
                    }
                    return Ok(entry);
                }
                VectorEntryOverlay::Unchanged => {}
            }
        }

        Ok(self.vector_store_live_entry_for_row(index, row_id, snapshot))
    }

    pub(crate) fn find_row_by_id_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<VersionedRow> {
        if let Some(tx) = tx {
            let staged = self.tx_mgr.with_write_set(tx, |ws| {
                let staged_insert = ws
                    .relational_inserts
                    .iter()
                    .rev()
                    .find(|(insert_table, row)| insert_table == table && row.row_id == row_id)
                    .map(|(_, row)| row.clone());
                if staged_insert.is_some() {
                    return staged_insert;
                }
                if ws
                    .relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == table && *deleted_row_id == row_id
                    })
                {
                    return None;
                }
                None
            })?;
            if let Some(row) = staged {
                return Ok(row);
            }
        }
        self.find_row_by_id_at(table, row_id, snapshot)
    }

    fn row_id_read_allowed_in_write_set_for_query(
        &self,
        ws: &WriteSet,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        if let Some(row) = ws
            .relational_inserts
            .iter()
            .rev()
            .find(|(insert_table, row)| insert_table == table && row.row_id == row_id)
            .map(|(_, row)| row.clone())
        {
            return Ok(!self
                .filter_rows_for_read(table, vec![row], snapshot)?
                .is_empty());
        }
        if ws
            .relational_deletes
            .iter()
            .any(|(delete_table, deleted_row_id, _)| {
                delete_table == table && *deleted_row_id == row_id
            })
        {
            return Ok(false);
        }
        let row = self.row_visible_at_snapshot(table, row_id, snapshot);
        let Some(row) = row else {
            return Ok(false);
        };
        Ok(!self
            .filter_rows_for_read(table, vec![row], snapshot)?
            .is_empty())
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
        let Some(meta) = self.table_meta(table) else {
            return self
                .graph_store
                .forward_adj
                .read()
                .values()
                .flat_map(|entries| entries.iter().cloned())
                .collect();
        };
        if !has_graph_edge_table_shape(&meta) {
            return self
                .graph_store
                .forward_adj
                .read()
                .values()
                .flat_map(|entries| entries.iter().cloned())
                .collect();
        }
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

    /// Execute a query at a specific snapshot.
    ///
    /// Relational reads and `GRAPH_TABLE` traversal inside the query see state
    /// at or before `snapshot`. Under constrained handles, explicit anchor
    /// reads at the pinned snapshot return typed visibility errors for rows
    /// hidden by context, scope-label, or ACL gates at that snapshot.
    pub fn execute_at_snapshot(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        snapshot: SnapshotId,
    ) -> Result<QueryResult> {
        self.with_snapshot_override(snapshot, || self.execute(sql, params))
    }

    pub(crate) fn with_snapshot_override<T>(
        &self,
        snapshot: SnapshotId,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        SNAPSHOT_OVERRIDE.with(|cell| {
            struct SnapshotOverrideGuard<'a> {
                cell: &'a std::cell::RefCell<Option<SnapshotId>>,
                prior: Option<SnapshotId>,
            }

            impl Drop for SnapshotOverrideGuard<'_> {
                fn drop(&mut self) {
                    self.cell.replace(self.prior);
                }
            }

            let prior = cell.replace(Some(snapshot));
            let _guard = SnapshotOverrideGuard { cell, prior };
            f()
        })
    }

    pub(crate) fn snapshot_for_read(&self) -> SnapshotId {
        SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().unwrap_or_else(|| self.snapshot()))
    }

    pub(crate) fn vector_schema_read(&self, index: &VectorIndexRef) -> VectorSchemaReadGuard {
        self.vector_schema_read_many([index.clone()])
    }

    pub(crate) fn vector_schema_read_table(&self, table: &str) -> Option<VectorSchemaReadGuard> {
        let refs = self.vector_schema_refs_for_table(table);
        (!refs.is_empty()).then(|| self.vector_schema_read_many(refs))
    }

    pub(crate) fn vector_schema_read_many(
        &self,
        refs: impl IntoIterator<Item = VectorIndexRef>,
    ) -> VectorSchemaReadGuard {
        let db_id = self.vector_schema_gate_id();
        // Same-thread trigger/cron callbacks already run inside the commit
        // mutex. Vector DDL metadata mutation also takes that mutex, so taking
        // the schema gate here would create commit-mutex -> schema-gate order.
        let callback_thread = self.callback_active_on_current_thread_for_this_db();
        let refs = VectorSchemaGates::sorted_refs(refs);
        let guards = if callback_thread {
            Vec::new()
        } else {
            let held = VECTOR_SCHEMA_READ_STACK.with(|stack| stack.borrow().clone());
            refs.iter()
                .filter(|index| !held.contains(&(db_id, (*index).clone())))
                .map(|index| self.vector_schema_gates.gate_for(index).read_arc())
                .collect()
        };
        VectorSchemaReadGuard::new(db_id, refs, guards)
    }

    pub(crate) fn vector_schema_write(&self, index: &VectorIndexRef) -> VectorSchemaWriteGuard {
        self.vector_schema_write_many([index.clone()])
    }

    pub(crate) fn vector_schema_write_table(&self, table: &str) -> Option<VectorSchemaWriteGuard> {
        let refs = self.vector_schema_refs_for_table(table);
        (!refs.is_empty()).then(|| self.vector_schema_write_many(refs))
    }

    pub(crate) fn vector_schema_write_many(
        &self,
        refs: impl IntoIterator<Item = VectorIndexRef>,
    ) -> VectorSchemaWriteGuard {
        let refs = VectorSchemaGates::sorted_refs(refs);
        let guards = refs
            .iter()
            .map(|index| self.vector_schema_gates.gate_for(index).write_arc())
            .collect();
        self.vector_schema_gates.bump_epochs(&refs);
        VectorSchemaWriteGuard { _guards: guards }
    }

    pub(crate) fn vector_schema_refs_for_table(&self, table: &str) -> Vec<VectorIndexRef> {
        self.table_meta(table)
            .map(|meta| {
                meta.columns
                    .into_iter()
                    .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    .map(|column| VectorIndexRef::new(table, column.name))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn vector_schema_refs_for_state_propagation_from_table(
        &self,
        table: &str,
    ) -> Vec<VectorIndexRef> {
        let metas = self.relational_store().table_meta.read().clone();
        let mut roots = HashSet::new();
        if let Some(meta) = metas.get(table) {
            for rule in &meta.propagation_rules {
                match rule {
                    PropagationRule::Edge { trigger_state, .. }
                    | PropagationRule::VectorExclusion { trigger_state } => {
                        roots.insert(trigger_state.clone());
                    }
                    PropagationRule::ForeignKey { .. } => {}
                }
            }
        }
        for meta in metas.values() {
            for rule in &meta.propagation_rules {
                if let PropagationRule::ForeignKey {
                    referenced_table,
                    trigger_state,
                    ..
                } = rule
                    && referenced_table == table
                {
                    roots.insert(trigger_state.clone());
                }
            }
        }

        let mut refs = Vec::new();
        let mut queue = roots
            .into_iter()
            .map(|state| (table.to_string(), state))
            .collect::<VecDeque<_>>();
        let mut visited = HashSet::new();
        while let Some((source_table, source_state)) = queue.pop_front() {
            if !visited.insert((source_table.clone(), source_state.clone())) {
                continue;
            }
            let Some(meta) = metas.get(&source_table) else {
                continue;
            };
            for rule in &meta.propagation_rules {
                match rule {
                    PropagationRule::VectorExclusion { trigger_state }
                        if trigger_state == &source_state =>
                    {
                        refs.extend(
                            meta.columns
                                .iter()
                                .filter(|column| {
                                    matches!(column.column_type, ColumnType::Vector(_))
                                })
                                .map(|column| {
                                    VectorIndexRef::new(&source_table, column.name.clone())
                                }),
                        );
                    }
                    PropagationRule::Edge {
                        trigger_state,
                        target_state,
                        ..
                    } if trigger_state == &source_state => {
                        queue.push_back((source_table.clone(), target_state.clone()));
                    }
                    _ => {}
                }
            }

            for (owner_table, owner_meta) in &metas {
                for rule in &owner_meta.propagation_rules {
                    if let PropagationRule::ForeignKey {
                        referenced_table,
                        trigger_state,
                        target_state,
                        ..
                    } = rule
                        && referenced_table == &source_table
                        && trigger_state == &source_state
                    {
                        queue.push_back((owner_table.clone(), target_state.clone()));
                    }
                }
            }
        }
        VectorSchemaGates::sorted_refs(refs)
    }

    fn vector_store_schema_refs(&self) -> Vec<VectorIndexRef> {
        self.vector_store
            .index_infos()
            .into_iter()
            .map(|info| info.index)
            .collect()
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
                        created_at: row.created_at,
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
                        created_at: None,
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

    /// Index slots the commit-time insert/delete maintenance loop iterated
    /// over for the most recent committed write set(s), summed across rows.
    #[doc(hidden)]
    pub fn __index_maintenance_visits(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.index_maintenance_visits()
    }

    #[doc(hidden)]
    pub fn __reset_index_maintenance_visits(&self) {
        let _operation = self.assert_open_operation();
        self.relational_store.reset_index_maintenance_visits();
    }

    /// Rows touched by relational full-scan primitives since the last reset.
    #[doc(hidden)]
    pub fn __relational_scan_rows_touched(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.scan_rows_touched()
    }

    #[doc(hidden)]
    pub fn __reset_relational_scan_rows_touched(&self) {
        let _operation = self.assert_open_operation();
        self.relational_store.reset_scan_rows_touched();
    }

    /// Index slots iterated during file-load/reopen replay. Read after
    /// `Database::open(path)` returns.
    #[doc(hidden)]
    pub fn __open_index_maintenance_visits(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.relational_store.open_index_maintenance_visits()
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
                .get(table)
                .is_some_and(|table_indexes| {
                    table_indexes
                        .values()
                        .any(|idx| idx.columns.first().is_some_and(|(c, _)| c == column))
                });
        let trace = if covered {
            QueryTrace {
                physical_plan: "IndexScan",
                index_used: None,
                predicates_pushed: Default::default(),
                indexes_considered: Default::default(),
                sort_elided: false,
                query_vector_source: None,
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

    /// Run one pruning cycle. Called by the maintenance loop or manually.
    pub fn run_pruning_cycle(&self) -> u64 {
        let _operation = self.assert_open_operation();
        let _guard = self.pruning_guard.lock();
        match self.maintenance_context().run_pruning() {
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
        self.maintenance_context().run_pruning()
    }

    /// Run ONE maintenance cycle synchronously: generic retention, the
    /// engine's own durable trigger-audit retention, and currency version
    /// compaction — the exact work the engine-owned loop dispatches on a tick,
    /// through the same code path.
    pub fn run_maintenance_cycle(&self) -> Result<MaintenanceReport> {
        let _operation = self.assert_open_operation();
        let _guard = self.pruning_guard.lock();
        self.maintenance_context().run_cycle(CurrencyGate::Always)
    }

    /// What the engine-owned maintenance loop is doing for this database.
    pub fn maintenance_status(&self) -> MaintenanceStatus {
        let _operation = self.assert_open_operation();
        let active_maintenance_loops = usize::from(self.pruning_runtime.lock().handle.is_some());
        MaintenanceStatus {
            running: active_maintenance_loops > 0,
            retention_enabled: self.has_retained_tables(),
            currency_compaction_enabled: self.has_currency_compaction_tables(),
            active_maintenance_loops,
        }
    }

    /// The retention window applied to the engine's OWN durable trigger-audit
    /// history. A shipped default: a consumer that configures nothing still
    /// gets a bounded table.
    pub fn trigger_audit_retention(&self) -> Option<Duration> {
        let _operation = self.assert_open_operation();
        Some(TRIGGER_AUDIT_RETENTION)
    }

    /// An honest size answer for one table: estimated live bytes, exact live
    /// row count, and the whole file's size alongside as itself. `None` when
    /// the table does not exist.
    pub fn table_size_estimate(&self, table: &str) -> Option<TableSizeEstimate> {
        let _operation = self.assert_open_operation();
        let meta = self.table_meta(table)?;
        let whole_file_bytes = self.disk_file_size();
        Some(self.table_size_estimate_for_meta(table, &meta, whole_file_bytes))
    }

    /// One answer per table in this database, each describing its own table.
    pub fn table_size_estimates(&self) -> Result<Vec<TableSizeEstimate>> {
        let _operation = self.open_operation()?;
        let whole_file_bytes = self.disk_file_size();
        let metas = self.relational_store.table_meta.read().clone();
        let mut estimates = metas
            .iter()
            .map(|(table, meta)| self.table_size_estimate_for_meta(table, meta, whole_file_bytes))
            .collect::<Vec<_>>();
        estimates.sort_by(|left, right| left.table.cmp(&right.table));
        Ok(estimates)
    }

    fn table_size_estimate_for_meta(
        &self,
        table: &str,
        meta: &TableMeta,
        whole_file_bytes: Option<u64>,
    ) -> TableSizeEstimate {
        let tables = self.relational_store.tables.read();
        let rows = tables.get(table);
        let mut row_count = 0u64;
        let mut estimated_live_bytes = 0u64;
        for row in rows.into_iter().flatten() {
            if row.deleted_tx.is_some() {
                continue;
            }
            row_count = row_count.saturating_add(1);
            estimated_live_bytes = estimated_live_bytes
                .saturating_add(estimate_row_bytes_for_meta(&row.values, meta, true) as u64);
        }
        TableSizeEstimate {
            table: table.to_string(),
            estimated_live_bytes,
            row_count,
            whole_file_bytes,
        }
    }

    /// Record the ONE hub a retained table is delivered to. Persisted in
    /// database metadata, so a reboot cannot let a different hub claim first
    /// place. Re-registering the SAME peer is a reconnect and succeeds; a
    /// second, DIFFERENT peer is refused loudly, naming both.
    pub fn register_retention_sync_peer(&self, node_id: &str) -> Result<()> {
        let _operation = self.open_operation()?;
        let mut established = self.retention_sync_peer.lock();
        if let Some(existing) = established.as_deref() {
            if existing == node_id {
                return Ok(());
            }
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "a retained table is delivered to exactly one hub: this database is already \
                     established with hub {existing}, so hub {node_id} is refused"
                ),
            });
        }
        if let Some(persistence) = &self.persistence {
            persistence.flush_config_value(RETENTION_SYNC_PEER_CONFIG_KEY, &node_id.to_string())?;
        }
        *established = Some(node_id.to_string());
        Ok(())
    }

    /// The established retention hub, read back from database metadata after a
    /// restart.
    pub fn retention_sync_peer(&self) -> Option<String> {
        let _operation = self.assert_open_operation();
        self.retention_sync_peer.lock().clone()
    }

    /// Collapse the high-churn currency tables (see [`VERSION_COMPACTION_TABLES`])
    /// back to one live version per logical row, dropping superseded MVCC
    /// versions and their change-log entries in lockstep, then — if dead pages
    /// now dominate the file — reclaim them with a redb compaction. Provably
    /// sync-safe (see `compact_currency_versions_inner`): every `changes_since`
    /// consumer still converges to current truth across a prune.
    pub fn compact_currency_versions(&self) -> Result<CurrencyCompactionReport> {
        let _operation = self.assert_open_operation();
        let _guard = self.pruning_guard.lock();
        self.maintenance_context()
            .run_currency(CurrencyGate::Always)
    }

    /// True when this database holds any compaction-eligible currency table.
    /// Non-fabric consumers (cg and every other) never install these, so they
    /// never get a maintenance thread.
    fn has_currency_compaction_tables(&self) -> bool {
        let tables = self.relational_store.tables.read();
        VERSION_COMPACTION_TABLES
            .iter()
            .any(|name| tables.contains_key(*name))
    }

    /// True when any table declares RETAIN, whether it was created here or
    /// arrived from disk on reopen or over synced DDL.
    fn has_retained_tables(&self) -> bool {
        self.relational_store
            .table_meta
            .read()
            .values()
            .any(|meta| meta.default_ttl_seconds.is_some())
    }

    /// True when this database keeps a durable trigger-audit history that the
    /// shipped default retention has to bound.
    fn has_durable_trigger_audit(&self) -> bool {
        !self.trigger.declarations.lock().is_empty()
    }

    fn maintenance_context(&self) -> MaintenanceContext {
        MaintenanceContext {
            relational: self.relational_store.clone(),
            graph: self.graph_store.clone(),
            vector: self.vector_store.clone(),
            accountant: self.accountant.clone(),
            persistence: self.persistence.clone(),
            change_log: self.change_log.clone(),
            trigger: self.trigger.clone(),
            tx_mgr: self.tx_mgr.clone(),
            sync_watermark: self.sync_watermark.clone(),
            trigger_audit_retention: TRIGGER_AUDIT_RETENTION,
        }
    }

    /// Start the ONE engine-owned maintenance loop iff this database has
    /// anything to maintain — a retained table, a currency table, or a durable
    /// trigger audit. Called at open (next to the cron tickler) and again
    /// whenever DDL lands, so a fresh install, a REOPEN, and a table that
    /// ARRIVED over synced DDL all self-maintain with no consumer call. A
    /// database with nothing to maintain spawns no thread at all.
    ///
    /// Registry-table invariant maintenance is engine-owned as of this commit:
    /// the earlier "host-driven maintenance" model was unrealized — `run_pruning_cycle`
    /// had zero production callers, so even `work_inputs` TTL never fired on a real
    /// fabric node, and the currency tables grew without bound (a 365MB debris
    /// ledger held 248,996 `work_capabilities` versions for ~10 live rows). An
    /// engine whose bounded-registry invariant depends on every host remembering a
    /// maintenance call is the silent-degradation class the substrate exists to
    /// kill, and the hub accumulates versions via sync-apply that no write-helper
    /// hook would catch — so the substrate self-maintains.
    /// Read the established retention hub back out of database metadata at
    /// open, so the multi-hub refusal survives a reboot.
    pub(crate) fn load_retention_sync_peer(&self) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        match persistence.load_config_value::<String>(RETENTION_SYNC_PEER_CONFIG_KEY) {
            Ok(peer) => *self.retention_sync_peer.lock() = peer,
            Err(err) => {
                tracing::warn!(error = %err, "failed to load the established retention sync peer")
            }
        }
    }

    pub(crate) fn start_maintenance_if_eligible(&self) {
        let eligible = self.has_currency_compaction_tables()
            || self.has_retained_tables()
            || self.has_durable_trigger_audit();
        if eligible && !self.__maintenance_thread_running() {
            self.spawn_maintenance(MAINTENANCE_TICK);
        }
    }

    /// Test-only: restart the currency-maintenance thread at a short interval so
    /// a test need not wait the production cadence. No shipped surface sets this.
    #[doc(hidden)]
    pub fn __set_currency_maintenance_interval(&self, interval: Duration) {
        let _operation = self.assert_open_operation();
        self.spawn_maintenance(interval);
    }

    /// Test-only: whether a maintenance/pruning thread is currently running.
    #[doc(hidden)]
    pub fn __maintenance_thread_running(&self) -> bool {
        self.pruning_runtime.lock().handle.is_some()
    }

    /// Test-only: wake-cycles STARTED by the current currency-maintenance
    /// thread since spawn (counted on wake, before the pending gate).
    /// Test-build only — production carries neither the counter nor this
    /// reader; tests poll it as the liveness half of a state-based wait.
    #[cfg(test)]
    pub(crate) fn __maintenance_wakes(&self) -> u64 {
        self.pruning_runtime.lock().wakes.load(Ordering::SeqCst)
    }

    fn spawn_maintenance(&self, interval: Duration) {
        self.stop_pruning_thread();

        let shutdown = Arc::new(AtomicBool::new(false));
        #[cfg(test)]
        let wakes = Arc::new(AtomicU64::new(0));
        #[cfg(test)]
        let thread_wakes = wakes.clone();
        let context = self.maintenance_context();
        let pruning_guard = self.pruning_guard.clone();
        let thread_shutdown = shutdown.clone();

        let handle = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::SeqCst) {
                sleep_with_shutdown(&thread_shutdown, interval);
                if thread_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                #[cfg(test)]
                thread_wakes.fetch_add(1, Ordering::SeqCst);
                let _guard = pruning_guard.lock();
                // ONE loop, every job. Each pass carries its OWN cheap gate so a
                // quiet node does near-zero work per tick: the currency pass is
                // gated on superseded-version count, the retention pass bails
                // before the commit lock when no table declares RETAIN, and the
                // audit pass takes no write transaction unless something aged
                // out. No pass relies on another's gate.
                match context.run_cycle(CurrencyGate::Scheduled) {
                    Ok(report) => log_maintenance_cycle(&report),
                    Err(err) => log_pruning_error(&err),
                }
            }
        });

        let mut runtime = self.pruning_runtime.lock();
        runtime.shutdown = shutdown;
        runtime.handle = Some(handle);
        #[cfg(test)]
        {
            runtime.wakes = wakes;
        }
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

    /// Per-stage commit work counters accumulated since the last reset.
    /// `#[doc(hidden)]` test-introspection surface.
    #[doc(hidden)]
    pub fn __commit_stage_stats(&self) -> CommitStageStats {
        let _operation = self.assert_open_operation();
        #[cfg(feature = "test-seams")]
        let stage_wall_nanos = {
            let timings = self
                .commit_stage_wall_nanos
                .each_ref()
                .map(|counter| counter.load(Ordering::SeqCst));
            timings.iter().any(|nanos| *nanos != 0).then_some(timings)
        };
        #[cfg(not(feature = "test-seams"))]
        let stage_wall_nanos = None;
        CommitStageStats {
            rows_validated: self.commit_rows_validated.load(Ordering::SeqCst),
            indexed_probes: self.commit_indexed_probes.load(Ordering::SeqCst),
            staged_vs_staged_comparisons: self
                .commit_staged_vs_staged_comparisons
                .load(Ordering::SeqCst),
            scan_rows_touched: self.commit_scan_rows_touched.load(Ordering::SeqCst),
            index_maintenance_visits: self.commit_index_maintenance_visits.load(Ordering::SeqCst),
            stage_wall_nanos,
        }
    }

    #[doc(hidden)]
    pub fn __reset_commit_stage_stats(&self) {
        let _operation = self.assert_open_operation();
        self.commit_rows_validated.store(0, Ordering::SeqCst);
        self.commit_indexed_probes.store(0, Ordering::SeqCst);
        self.commit_staged_vs_staged_comparisons
            .store(0, Ordering::SeqCst);
        self.commit_scan_rows_touched.store(0, Ordering::SeqCst);
        self.commit_index_maintenance_visits
            .store(0, Ordering::SeqCst);
        #[cfg(feature = "test-seams")]
        for counter in &self.commit_stage_wall_nanos {
            counter.store(0, Ordering::SeqCst);
        }
    }

    /// Set the maintenance loop interval so a test need not wait the
    /// production cadence. Restarts the same single loop; no shipped surface
    /// sets this.
    pub fn set_pruning_interval(&self, interval: Duration) {
        let _operation = self.assert_open_operation();
        self.spawn_maintenance(interval);
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

    /// Exports a transactionally consistent snapshot of this database into a
    /// brand-new artifact file at `dest`. The source stays fully live: the
    /// commit mutex is held only for a bounded snapshot capture (never across
    /// the copy), and pruning is deferred for the export's duration so rows
    /// alive-but-expired at the snapshot land in the artifact.
    pub fn export_snapshot(&self, dest: impl AsRef<Path>) -> Result<ExportReport> {
        let _operation = self.open_operation()?;
        let dest = dest.as_ref();
        // Fast typed error before anything is created; the no-replace
        // hard-link publish below is the race-proof backstop.
        if dest.symlink_metadata().is_ok() {
            return Err(Error::ExportDestinationExists {
                path: dest.to_path_buf(),
            });
        }
        let _pruning_deferral = self.pruning_guard.lock();

        // The unique segment sits before the temp extension so each attempt's
        // PID-lock path (`{dest}.{unique}.lock`) is unique too: concurrent
        // exports never contend on a lock path.
        let unique = uuid::Uuid::new_v4().simple().to_string();
        let mut temp_name = dest.as_os_str().to_os_string();
        temp_name.push(format!(".{unique}.tmpexport"));
        let temp_path = PathBuf::from(temp_name);
        let temp_lock_path = temp_path.with_extension("lock");

        match self.write_export_artifact(dest, &temp_path) {
            Ok(report) => {
                // No-replace publish: hard_link fails with AlreadyExists if a
                // dest appeared since the up-front check, so an existing
                // destination always survives and a same-dest race has
                // exactly one winner.
                let publish = std::fs::hard_link(&temp_path, dest);
                let _ = std::fs::remove_file(&temp_path);
                let _ = std::fs::remove_file(&temp_lock_path);
                match publish {
                    Ok(()) => Ok(report),
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                        Err(Error::ExportDestinationExists {
                            path: dest.to_path_buf(),
                        })
                    }
                    Err(err) => Err(export_io_error(dest, &err)),
                }
            }
            Err(err) => {
                // Remove every file this attempt created — the temp artifact
                // and its lock — and nothing else.
                let _ = std::fs::remove_file(&temp_path);
                let _ = std::fs::remove_file(&temp_lock_path);
                Err(err)
            }
        }
    }

    fn write_export_artifact(&self, dest: &Path, temp_path: &Path) -> Result<ExportReport> {
        // A missing destination directory fails here, before any file exists;
        // the directory is never created on the caller's behalf.
        let artifact =
            RedbPersistence::create(temp_path).map_err(|err| export_io_error(dest, &err))?;
        let copied = self
            .copy_snapshot_into_artifact(&artifact)
            .and_then(|report| {
                // Compact so `bytes_written` (and the artifact itself) reflect
                // content size, not allocator slack from the batched copy.
                artifact.compact()?;
                Ok(report)
            });
        artifact.close();
        let mut report = copied?;
        let file = std::fs::File::open(temp_path).map_err(|err| export_io_error(dest, &err))?;
        file.sync_all().map_err(|err| export_io_error(dest, &err))?;
        report.bytes_written = file
            .metadata()
            .map_err(|err| export_io_error(dest, &err))?
            .len();
        Ok(report)
    }

    fn copy_snapshot_into_artifact(&self, artifact: &RedbPersistence) -> Result<ExportReport> {
        // Single commit point: the committed watermark TxId, committed LSN,
        // and commit-index prefix are captured together under the commit
        // mutex. The mutex is released before any copying starts; commits
        // landing mid-copy carry visibility TxIds above the watermark and LSN
        // stamps above the snapshot LSN, so the filters below exclude them
        // entirely — a commit is in the artifact entirely or not at all.
        let (watermark, snapshot_lsn, commit_index) = self.with_commit_lock(|| {
            let watermark = self.tx_mgr.current_tx_max();
            let snapshot_lsn = self.tx_mgr.current_lsn();
            let commit_index = self.tx_mgr.commit_index_prefix(snapshot_lsn);
            (watermark, snapshot_lsn, commit_index)
        });

        let table_meta = self.relational_store.table_meta.read().clone();
        for (name, meta) in &table_meta {
            artifact.flush_table_meta(name, meta)?;
        }

        // Rows: streaming storage-layer copy, batched by position. Positions
        // are stable for the copy's duration: commits only append versions or
        // set tombstones in place, and pruning (the only remover) is deferred.
        let mut row_count = 0u64;
        let mut table_names: Vec<&String> = table_meta.keys().collect();
        table_names.sort();
        for name in table_names {
            let name = name.as_str();
            let meta = table_meta.get(name);
            let mut offset = 0usize;
            loop {
                let (batch, source_lsns) = {
                    let tables = self.relational_store.tables.read();
                    let Some(stored) = tables.get(name) else {
                        break;
                    };
                    if offset >= stored.len() {
                        break;
                    }
                    let end = stored.len().min(offset + EXPORT_BATCH_SIZE);
                    let mut batch = Vec::with_capacity(end - offset);
                    let mut source_lsns = Vec::new();
                    for row in &stored[offset..end] {
                        let Some(row) = export_row_at_snapshot(row, watermark) else {
                            continue;
                        };
                        if row.deleted_tx.is_none() {
                            row_count += 1;
                            if let Some(lsn) =
                                self.relational_store.sync_source_lsn(name, row.row_id)
                            {
                                source_lsns.push((name.to_string(), row.row_id, lsn));
                            }
                        }
                        batch.push(row);
                    }
                    offset = end;
                    (batch, source_lsns)
                };
                artifact.append_table_rows_batch(name, meta, &batch)?;
                artifact.append_sync_source_lsns_batch(&source_lsns)?;
            }
        }

        // Edges: the forward adjacency is the canonical copy; the writer
        // derives the reverse table from the same entries.
        let mut edge_count = 0u64;
        let nodes: Vec<NodeId> = self
            .graph_store
            .forward_adj
            .read()
            .keys()
            .copied()
            .collect();
        for chunk in nodes.chunks(EXPORT_NODE_BATCH_SIZE) {
            let batch = {
                let forward = self.graph_store.forward_adj.read();
                let mut batch = Vec::new();
                for node in chunk {
                    let Some(entries) = forward.get(node) else {
                        continue;
                    };
                    for entry in entries {
                        let Some(edge) = export_edge_at_snapshot(entry, watermark) else {
                            continue;
                        };
                        if edge.deleted_tx.is_none() {
                            edge_count += 1;
                        }
                        batch.push(edge);
                    }
                }
                batch
            };
            artifact.append_graph_edges_batch(&batch)?;
        }

        // Vectors: complete per-column copy with the column's declared
        // quantization, so relational vector values re-hydrate on open and
        // HNSW rebuilds deterministically from the same stored data.
        let quantization = RedbPersistence::vector_quantization_map(&table_meta);
        let mut vector_count = 0u64;
        for info in self.vector_store.index_infos() {
            let entries = self.vector_store.entries_for_index(&info.index)?;
            let mut batch = Vec::new();
            for entry in &entries {
                let Some(entry) = export_vector_at_snapshot(entry, watermark) else {
                    continue;
                };
                if entry.deleted_tx.is_none() {
                    vector_count += 1;
                }
                batch.push(entry);
                if batch.len() >= EXPORT_BATCH_SIZE {
                    artifact.append_vector_entries_batch(&batch, &quantization)?;
                    batch.clear();
                }
            }
            artifact.append_vector_entries_batch(&batch, &quantization)?;
        }

        // Change log: entries at or below the snapshot LSN, with per-LSN key
        // indices tracked across batches so multi-entry commits never collide.
        let mut offset = 0usize;
        let mut lsn_run: Option<(Lsn, usize)> = None;
        loop {
            let batch: Vec<(usize, ChangeLogEntry)> = {
                let log = self.change_log.read();
                if offset >= log.len() {
                    break;
                }
                let end = log.len().min(offset + EXPORT_BATCH_SIZE);
                let mut batch = Vec::new();
                for entry in &log[offset..end] {
                    let lsn = entry.lsn();
                    if lsn > snapshot_lsn {
                        continue;
                    }
                    let index = match lsn_run {
                        Some((run_lsn, next)) if run_lsn == lsn => next,
                        _ => 0,
                    };
                    lsn_run = Some((lsn, index + 1));
                    batch.push((index, entry.clone()));
                }
                offset = end;
                batch
            };
            artifact.append_change_log_entries_batch(&batch)?;
        }

        let ddl_entries: Vec<(Lsn, DdlChange)> = self
            .ddl_log
            .read()
            .iter()
            .filter(|(lsn, _)| *lsn <= snapshot_lsn)
            .cloned()
            .collect();
        artifact.append_ddl_log_entries(&ddl_entries)?;

        artifact.flush_commit_index_entries(&commit_index)?;

        match &self.persistence {
            Some(persistence) => {
                // File-backed source: raw copy of the whole config table
                // (tenant watermark keys are dynamic), the durable sink
                // queues, and the audit tables.
                let config = persistence.dump_config_raw()?;
                artifact.flush_encoded_config_values(
                    config
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.clone()))
                        .collect(),
                )?;
                for sink in persistence.sink_queue_names()? {
                    let entries = persistence.dump_sink_queue_raw(&sink)?;
                    for chunk in entries.chunks(EXPORT_BATCH_SIZE) {
                        artifact.append_sink_queue_raw(&sink, chunk)?;
                    }
                }
                let trigger_audits = persistence.dump_trigger_audit_raw()?;
                for chunk in trigger_audits.chunks(EXPORT_BATCH_SIZE) {
                    artifact.append_trigger_audit_raw(chunk)?;
                }
                let sink_audits = persistence.dump_sink_audit_raw()?;
                for chunk in sink_audits.chunks(EXPORT_BATCH_SIZE) {
                    artifact.append_sink_audit_raw(chunk)?;
                }
            }
            None => {
                // In-memory source: persisted-only operational config does
                // not exist here and is not exported, but DDL-declared state
                // is database state — cron schedules, event-bus definitions,
                // and trigger declarations live only in memory and export
                // exactly as file-backed DDL persistence would have written
                // them.
                let mut config_values = self.export_cron_config_values()?;
                config_values.extend(self.export_event_bus_config_values()?);
                config_values.extend(self.export_trigger_config_values()?);
                artifact.flush_encoded_config_values(config_values)?;
            }
        }

        Ok(ExportReport {
            snapshot_lsn,
            rows: row_count,
            edges: edge_count,
            vectors: vector_count,
            bytes_written: 0,
        })
    }

    pub fn close(&self) -> Result<()> {
        let db_id = self as *const Self as usize;
        let _close_waiter = if Self::callback_active_process_wide() {
            Some(TriggerCloseWaiterGuard::new(&self.trigger))
        } else {
            None
        };
        if Self::callback_active_process_wide() {
            self.assert_callback_reentry_allowed()?;
        }
        if DB_OPERATION_STACK.with(|stack| stack.borrow().contains(&db_id)) {
            return Err(Error::Other(
                "cannot close database from inside an active operation".to_string(),
            ));
        }
        self.assert_public_tx_control_cross_thread_allowed_for_surface("close")?;
        {
            let _operation_barrier = self.operation_gate.write();
            if self.closed.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            if self.resource_owner {
                self.resource_closed.store(true, Ordering::SeqCst);
            }
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
        let event_bus_shutdown = self.stop_event_bus_threads();
        self.stop_pruning_thread();
        if self.resource_owner {
            self.subscriptions.lock().subscribers.clear();
            if !event_bus_shutdown.deferred_resource_cleanup() {
                if let Some(persistence) = &self.persistence {
                    persistence.close();
                }
                self.release_open_registry();
            }
        }
        if self.resource_owner {
            self.plugin.on_close()
        } else {
            Ok(())
        }
    }

    fn open_operation(&self) -> Result<DatabaseOperationGuard<'_>> {
        let db_id = self as *const Self as usize;
        let nested = DB_OPERATION_STACK.with(|stack| stack.borrow().contains(&db_id));
        if nested {
            DB_OPERATION_STACK.with(|stack| stack.borrow_mut().push(db_id));
            return Ok(DatabaseOperationGuard { db_id, _lock: None });
        }

        let lock = self.operation_gate.read();
        if self.closed.load(Ordering::SeqCst) || self.resource_closed.load(Ordering::SeqCst) {
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
        db.load_retention_sync_peer();
        db.start_maintenance_if_eligible();
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
        db.load_retention_sync_peer();
        db.start_maintenance_if_eligible();
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

    pub(crate) fn drain_vector_index_maintenance_for_ddl(&self, index: &VectorIndexRef) {
        let Some(state) = self.vector_store.try_state(index) else {
            return;
        };
        #[cfg(feature = "test-seams")]
        // Marks entry to the DDL drain; tests then assert schema mutation waits
        // for the subsequent same-ref maintenance drain to complete.
        self.pause_vector_ddl_for_test(index);
        // Reconfiguration with the current shape is a no-op, but it acquires
        // the affected ref's maintenance lock before SQL metadata changes.
        self.vector_store.register_or_reconfigure_empty_index(
            index.clone(),
            state.dimension(),
            state.quantization(),
        );
    }

    pub(crate) fn drain_vector_table_maintenance_for_ddl(&self, table: &str) {
        #[cfg(feature = "test-seams")]
        // Same entry marker as the per-ref drain, keyed by table.* for DROP TABLE.
        self.pause_vector_ddl_for_test(&VectorIndexRef::new(table, "*"));
        let indexes = self
            .vector_store
            .index_infos()
            .into_iter()
            .filter(|info| info.index.table == table)
            .map(|info| info.index)
            .collect::<Vec<_>>();
        for index in indexes {
            self.drain_vector_index_maintenance_for_ddl(&index);
        }
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn pause_vector_ddl_for_test(&self, index: &VectorIndexRef) {
        self.vector_store.maybe_pause_ddl_for_test(index);
    }

    pub(crate) fn vector_index_infos(&self) -> Vec<contextdb_vector::store::VectorIndexInfo> {
        let _vector_schema = self.vector_schema_read_many(self.vector_store_schema_refs());
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

    pub(crate) fn delete_release_bytes_for_write_set(
        &self,
        ws: &contextdb_tx::WriteSet,
    ) -> DeleteReleaseBytes {
        let mut bytes = DeleteReleaseBytes::default();
        for (table, row_id, _) in &ws.relational_deletes {
            if let Some(row) =
                self.relational_store
                    .row_by_id(table, *row_id, SnapshotId::from_raw_wire(u64::MAX))
            {
                bytes.relational.push(
                    self.table_meta(table)
                        .map(|meta| estimate_row_bytes_for_meta(&row.values, &meta, false))
                        .unwrap_or_else(|| row.estimated_bytes()),
                );
            }
        }

        for (source, edge_type, target, _) in &ws.adj_deletes {
            if let Some(edge) = self.find_edge(source, target, edge_type) {
                bytes.edges.push(edge.estimated_bytes());
            }
        }

        for (index, row_id, _) in &ws.vector_deletes {
            if let Some(vector) = self.find_vector_by_index_and_row(index, *row_id) {
                bytes
                    .vectors
                    .push(self.vector_insert_accounted_bytes(index, vector.vector.len()));
            }
        }
        bytes
    }

    pub(crate) fn release_delete_allocations_from_bytes(&self, bytes: &DeleteReleaseBytes) {
        for bytes in &bytes.relational {
            self.accountant.release(*bytes);
        }
        for bytes in &bytes.edges {
            self.accountant.release(*bytes);
        }
        for bytes in &bytes.vectors {
            self.accountant.release(*bytes);
        }
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

    // 8th parameter `fail_on_conflict` is load-bearing: it differentiates
    // stale-conditional-UPDATE silent no-ops from explicit row-guard atomic
    // rollbacks. Internal pub(crate) helper; struct refactor is overkill.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_conditional_update_guard(
        &self,
        tx: TxId,
        table: TableName,
        row_id: RowId,
        predicates: Vec<(ColName, Value)>,
        before: WriteSetCounts,
        after: WriteSetCounts,
        fail_on_conflict: bool,
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
                fail_on_conflict,
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
        let mut vector_schema_refs = self.vector_schema_refs_for_table(&table);
        if self.table_meta(&table).is_some_and(|meta| {
            meta.state_machine.is_some_and(|state_machine| {
                details
                    .update_columns
                    .iter()
                    .any(|(column_name, _)| *column_name == state_machine.column)
            })
        }) {
            vector_schema_refs
                .extend(self.vector_schema_refs_for_state_propagation_from_table(&table));
        }
        self.record_vector_schema_epochs(tx, vector_schema_refs)?;
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
        let change = alter_table_ddl_change(name, meta, Vec::new());
        self.append_ddl_change(lsn, change)
    }

    pub(crate) fn persist_table_meta_rows_vectors_and_log_alter_table_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        lsn: Lsn,
    ) -> Result<()> {
        self.persist_table_meta_rows_vectors_and_append_alter_table_ddl(name, meta, lsn, Vec::new())
    }

    pub(crate) fn persist_table_meta_rows_vectors_and_log_alter_table_ddl_with_vector_rename(
        &self,
        name: &str,
        meta: &TableMeta,
        from: &str,
        to: &str,
        lsn: Lsn,
    ) -> Result<()> {
        self.persist_table_meta_rows_vectors_and_append_alter_table_ddl(
            name,
            meta,
            lsn,
            vec![sync_vector_rename_constraint(from, to)],
        )
    }

    fn persist_table_meta_rows_vectors_and_append_alter_table_ddl(
        &self,
        name: &str,
        meta: &TableMeta,
        lsn: Lsn,
        extra_constraints: Vec<String>,
    ) -> Result<()> {
        let change = alter_table_ddl_change(name, meta, extra_constraints);
        if let Some(persistence) = &self.persistence {
            let rows = self
                .relational_store
                .tables
                .read()
                .get(name)
                .cloned()
                .unwrap_or_default();
            let vectors = self.vector_store.all_entries();
            persistence.rewrite_table_meta_rows_vectors_and_append_ddl_log(
                name, meta, &rows, &vectors, lsn, &change,
            )?;
        }
        self.ddl_log.write().push((lsn, change));
        Ok(())
    }

    fn append_ddl_change(&self, lsn: Lsn, change: DdlChange) -> Result<()> {
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

    pub fn persisted_sync_watermarks(&self, tenant_id: &TenantId) -> Result<(Lsn, Lsn)> {
        let _operation = self.open_operation()?;
        let Some(persistence) = &self.persistence else {
            return Ok((Lsn(0), Lsn(0)));
        };
        let push = persistence
            .load_config_value::<u64>(&tenant_id.config_key("sync_push_watermark"))?
            .map(Lsn)
            .unwrap_or(Lsn(0));
        let pull = persistence
            .load_config_value::<u64>(&tenant_id.config_key("sync_pull_watermark"))?
            .map(Lsn)
            .unwrap_or(Lsn(0));
        Ok((push, pull))
    }

    pub fn persist_sync_push_watermark(&self, tenant_id: &TenantId, watermark: Lsn) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            persistence
                .flush_config_value(&tenant_id.config_key("sync_push_watermark"), &watermark.0)?;
        }
        Ok(())
    }

    pub fn persist_sync_pull_watermark(&self, tenant_id: &TenantId, watermark: Lsn) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            persistence
                .flush_config_value(&tenant_id.config_key("sync_pull_watermark"), &watermark.0)?;
        }
        Ok(())
    }

    /// Per-tenant record of the highest edge-LSN this database has applied
    /// from sync pushes. Stored as a config value so `export_snapshot` carries
    /// it with the artifact at the snapshot point, exactly like the tenant sync
    /// watermark keys. In-memory databases retain this value for the life of
    /// the handle; `None` means no push has been applied for the tenant.
    pub fn persisted_sync_applied_push_watermark(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<Lsn>> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            return Ok(persistence
                .load_config_value::<u64>(&tenant_id.config_key("sync_applied_push_watermark"))?
                .map(Lsn));
        }
        Ok(self
            .in_memory_applied_push_watermarks
            .lock()
            .get(tenant_id.as_str())
            .copied())
    }

    /// Persist the per-tenant applied-push watermark. Callers enforce
    /// monotonic advancement; this only writes the config value or the
    /// in-memory equivalent for ephemeral databases.
    pub fn persist_sync_applied_push_watermark(
        &self,
        tenant_id: &TenantId,
        watermark: Lsn,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            persistence.flush_config_value(
                &tenant_id.config_key("sync_applied_push_watermark"),
                &watermark.0,
            )?;
        } else {
            self.in_memory_applied_push_watermarks
                .lock()
                .insert(tenant_id.as_str().to_string(), watermark);
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
                    created_at: row.created_at,
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
                    created_at: row.created_at,
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
        self.restore_vector_owner_rows(&mut rows, &vectors);

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

        let empty_skip_deleted = HashSet::new();
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
                self.required_indexed_visible_row_by_column(
                    &row.table,
                    &row.natural_key.column,
                    &row.natural_key.value,
                    self.snapshot(),
                    &empty_skip_deleted,
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
                    Self::apply_sync_alter_to_projected_table_meta(
                        name,
                        meta,
                        incoming,
                        constraints,
                    );
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
                if let Some(meta) = projected.get_mut(table)
                    && crate::executor::reserved_index_prefix(name).is_none()
                {
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

    fn sync_meta_column_names(meta: &TableMeta) -> HashSet<String> {
        meta.columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    fn sync_vector_columns(meta: &TableMeta) -> BTreeMap<String, (usize, VectorQuantization)> {
        meta.columns
            .iter()
            .filter_map(|column| match column.column_type {
                ColumnType::Vector(dimension) => {
                    Some((column.name.clone(), (dimension, column.quantization)))
                }
                _ => None,
            })
            .collect()
    }

    fn sync_alter_is_full_shape(
        existing: &TableMeta,
        incoming: &TableMeta,
        constraints: &[String],
    ) -> bool {
        let incoming_column_names = Self::sync_meta_column_names(incoming);
        if existing
            .columns
            .iter()
            .filter(|column| !matches!(column.column_type, ColumnType::Vector(_)))
            .any(|column| !incoming_column_names.contains(&column.name))
        {
            return false;
        }

        let existing_vector_columns = Self::sync_vector_columns(existing);
        let incoming_vector_columns = Self::sync_vector_columns(incoming);
        let existing_column_names = Self::sync_meta_column_names(existing);
        let added_non_vector = incoming.columns.iter().any(|column| {
            !matches!(column.column_type, ColumnType::Vector(_))
                && !existing_column_names.contains(&column.name)
        });
        if existing_vector_columns.iter().any(|(column, shape)| {
            incoming_vector_columns
                .get(column)
                .is_some_and(|incoming_shape| incoming_shape != shape)
        }) {
            return true;
        }
        let marked_rename =
            Self::sync_marked_vector_rename(existing, incoming, constraints).is_some();

        let removed = existing_vector_columns
            .keys()
            .filter(|column| !incoming_vector_columns.contains_key(*column))
            .count();
        let added = incoming_vector_columns
            .keys()
            .filter(|column| !existing_vector_columns.contains_key(*column))
            .count();
        if removed == 0 {
            return added > 0;
        }
        if added == 0 {
            return !added_non_vector;
        }
        marked_rename
    }

    fn sync_marked_vector_rename(
        existing: &TableMeta,
        incoming: &TableMeta,
        constraints: &[String],
    ) -> Option<(String, String)> {
        let marked = sync_vector_rename_from_constraints(constraints)?;
        let existing_vector_columns = Self::sync_vector_columns(existing);
        let incoming_vector_columns = Self::sync_vector_columns(incoming);
        let (from, to) = marked;
        if incoming_vector_columns.contains_key(&from) || existing_vector_columns.contains_key(&to)
        {
            return None;
        }
        (existing_vector_columns.get(&from) == incoming_vector_columns.get(&to))
            .then_some((from, to))
    }

    fn merged_sync_full_shape_vector_alter_meta(
        table_name: &str,
        existing: &TableMeta,
        incoming: TableMeta,
        constraints: &[String],
    ) -> TableMeta {
        let existing_columns = existing
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.clone()))
            .collect::<HashMap<_, _>>();
        let renamed_vector_column =
            Self::sync_marked_vector_rename(existing, &incoming, constraints);
        let mut merged = existing.clone();
        merged.columns = incoming
            .columns
            .into_iter()
            .map(|incoming_column| {
                let existing_column = existing_columns
                    .get(&incoming_column.name)
                    .or_else(|| {
                        renamed_vector_column
                            .as_ref()
                            .and_then(|(from, to)| (to == &incoming_column.name).then_some(from))
                            .and_then(|from| existing_columns.get(from))
                    })
                    .cloned();
                if let Some(mut column) = existing_column {
                    let incoming_references = incoming_column.references.clone();
                    let incoming_name = incoming_column.name.clone();
                    let incoming_column_type = incoming_column.column_type.clone();
                    let incoming_quantization = incoming_column.quantization;
                    column.name = incoming_name;
                    column.column_type = incoming_column_type;
                    column.quantization = incoming_quantization;
                    merge_sync_alter_existing_column(&mut column, incoming_column);
                    column.references = incoming_references;
                    column
                } else {
                    incoming_column
                }
            })
            .collect();
        if !incoming.unique_constraints.is_empty() {
            merged.unique_constraints = incoming.unique_constraints;
        }
        if !incoming.composite_foreign_keys.is_empty() {
            merged.composite_foreign_keys = incoming.composite_foreign_keys;
        }
        let final_column_names = Self::sync_meta_column_names(&merged);
        let user_indexes = existing
            .indexes
            .iter()
            .filter(|index| {
                index.kind == IndexKind::UserDeclared
                    && index
                        .columns
                        .iter()
                        .all(|(column, _)| final_column_names.contains(column))
            })
            .cloned()
            .collect::<Vec<_>>();
        merged.indexes = crate::executor::auto_indexes_for_table_meta(&merged);
        merged.indexes.extend(user_indexes);
        resolve_sync_rank_policies(table_name, &mut merged);
        merged
    }

    fn sync_alter_vector_shape_changed(
        existing: &TableMeta,
        incoming: &TableMeta,
        constraints: &[String],
    ) -> bool {
        Self::sync_alter_is_full_shape(existing, incoming, constraints)
            && Self::sync_vector_columns(existing) != Self::sync_vector_columns(incoming)
    }

    fn apply_sync_alter_to_projected_table_meta(
        table_name: &str,
        meta: &mut TableMeta,
        incoming: TableMeta,
        constraints: &[String],
    ) {
        if Self::sync_alter_vector_shape_changed(meta, &incoming, constraints) {
            *meta = Self::merged_sync_full_shape_vector_alter_meta(
                table_name,
                meta,
                incoming,
                constraints,
            );
            return;
        }

        let mut existing = Self::sync_meta_column_names(meta);
        for column in incoming.columns {
            if existing.insert(column.name.clone()) {
                meta.columns.push(column);
            } else if let Some(current) = meta
                .columns
                .iter_mut()
                .find(|current| current.name == column.name)
            {
                merge_sync_alter_existing_column(current, column);
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
                        Self::apply_sync_alter_to_projected_table_meta(
                            name,
                            meta,
                            incoming,
                            constraints,
                        );
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
                    if let Some(meta) = projected.get_mut(table)
                        && crate::executor::reserved_index_prefix(name).is_none()
                    {
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
                Self::validate_projected_foreign_key_schema(&projected_table_meta)?;
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
                Self::validate_projected_foreign_key_schema(&projected_table_meta)?;
                self.apply_sync_trigger_ddl_to_projection(
                    &mut projected_trigger_declarations,
                    ddl,
                    &projected_table_meta,
                )?;
            }
        }
        Ok(projected_trigger_declarations)
    }

    pub(crate) fn validate_projected_foreign_key_schema(
        projected: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        for (table, meta) in projected {
            crate::executor::validate_exact_constraint_keys_for_meta(table, meta)?;
            crate::executor::validate_single_column_foreign_keys_for_meta(table, meta, |parent| {
                projected.get(parent).cloned()
            })?;
            crate::executor::validate_composite_foreign_keys_for_meta(table, meta, |parent| {
                projected.get(parent).cloned()
            })?;
        }
        Ok(())
    }

    pub(crate) fn replace_table_meta_and_refresh_auto_indexes(
        &self,
        table: &str,
        old_meta: &TableMeta,
        mut new_meta: TableMeta,
    ) -> Result<TableMeta> {
        let old_auto_names = old_meta
            .indexes
            .iter()
            .filter(|index| index.kind == IndexKind::Auto)
            .map(|index| index.name.clone())
            .collect::<HashSet<_>>();
        let old_auto_columns = old_meta
            .indexes
            .iter()
            .filter(|index| index.kind == IndexKind::Auto)
            .map(|index| (index.name.clone(), index.columns.clone()))
            .collect::<HashMap<_, _>>();
        let user_indexes = new_meta
            .indexes
            .iter()
            .filter(|index| index.kind == IndexKind::UserDeclared)
            .cloned()
            .collect::<Vec<_>>();
        let auto_indexes = crate::executor::auto_indexes_for_table_meta(&new_meta);
        let new_auto_names = auto_indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<HashSet<_>>();
        let changed_auto_names = auto_indexes
            .iter()
            .filter(|index| {
                old_auto_columns
                    .get(&index.name)
                    .is_some_and(|old_columns| old_columns != &index.columns)
            })
            .map(|index| index.name.clone())
            .collect::<HashSet<_>>();

        new_meta.indexes = auto_indexes.clone();
        new_meta.indexes.extend(user_indexes);
        {
            let store = self.relational_store();
            let mut metas = store.table_meta.write();
            if !metas.contains_key(table) {
                return Err(Error::TableNotFound(table.to_string()));
            }
            metas.insert(table.to_string(), new_meta.clone());
        }

        let store = self.relational_store();
        for old_name in old_auto_names.difference(&new_auto_names) {
            store.drop_index_storage(table, old_name);
        }
        for changed_name in &changed_auto_names {
            store.drop_index_storage(table, changed_name);
        }
        for index in auto_indexes {
            store.create_exact_index_storage(table, &index.name, index.columns.clone());
            store.rebuild_index(table, &index.name);
        }

        Ok(new_meta)
    }

    fn validate_sync_table_ddl_against_projected_meta(
        projected: &HashMap<String, TableMeta>,
        change: &DdlChange,
    ) -> Result<()> {
        // A two-way retained table is illegal EVERYWHERE, not only where a
        // local operator types it: a peer that spells `TWO WAY` in arriving
        // DDL is refused here, before any of the statement is applied, exactly
        // as the local CREATE and ALTER paths refuse it. Nothing silently
        // converts it — silent conversion would let a foreign edge define a
        // posture this engine has refused at every other door.
        refuse_two_way_retained_sync_ddl(change)?;
        refuse_keyless_sync_safe_sync_ddl(projected, change)?;
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
            } => Self::validate_sync_alter_table_shape_ddl(
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
                if let Some(prefix) = crate::executor::reserved_index_prefix(name) {
                    return Err(Error::ReservedIndexName {
                        table: table.clone(),
                        name: name.clone(),
                        prefix: prefix.to_string(),
                    });
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
                    if !crate::executor::btree_indexable(&column_meta.column_type) {
                        return Err(Error::ColumnNotIndexable {
                            table: table.clone(),
                            column: column.clone(),
                            column_type: column_meta.column_type.clone(),
                        });
                    }
                }
                Ok(())
            }
            DdlChange::DropIndex { table, name } => {
                if projected
                    .get(table)
                    .is_some_and(|meta| meta.indexes.iter().any(|index| index.name == *name))
                    && let Some(prefix) = crate::executor::reserved_index_prefix(name)
                {
                    return Err(Error::ReservedIndexName {
                        table: table.clone(),
                        name: name.clone(),
                        prefix: prefix.to_string(),
                    });
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn validate_sync_alter_table_shape_ddl(
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

        let incoming = rough_sync_table_meta(
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        );
        let mut candidate = projected.get(name).cloned().unwrap_or_default();
        Self::apply_sync_alter_to_projected_table_meta(name, &mut candidate, incoming, constraints);
        let candidate_lookup = candidate.clone();
        crate::executor::validate_exact_constraint_keys_for_meta(name, &candidate)?;
        crate::executor::validate_single_column_foreign_keys_for_meta(
            name,
            &candidate,
            |parent| {
                if parent == name {
                    Some(candidate_lookup.clone())
                } else {
                    projected.get(parent).cloned()
                }
            },
        )?;
        crate::executor::validate_composite_foreign_keys_for_meta(name, &candidate, |parent| {
            if parent == name {
                Some(candidate_lookup.clone())
            } else {
                projected.get(parent).cloned()
            }
        })?;
        Ok(())
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
        crate::executor::validate_exact_constraint_keys_for_meta(name, &candidate)?;
        crate::executor::validate_single_column_foreign_keys_for_meta(
            name,
            &candidate,
            |parent| {
                if parent == name {
                    Some(candidate_lookup.clone())
                } else {
                    projected.get(parent).cloned()
                }
            },
        )?;
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
            if !row.deleted
                && !self.sync_incoming_values_allowed_for_access(
                    &row.table,
                    table_meta,
                    &row.values,
                    row.lsn,
                )?
            {
                continue;
            }
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
            if let Some(table_meta) = projected_table_meta.get(&row.table)
                && !self.sync_incoming_values_allowed_for_access(
                    &row.table,
                    table_meta,
                    &row.values,
                    row.lsn,
                )?
            {
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
                    && !self.sync_row_applies_over_committed(row, &committed, policy)
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
                    self.required_indexed_visible_row_by_column(
                        &reference.table,
                        &reference.column,
                        value,
                        snapshot,
                        deleted_parent_row_ids,
                    )?
                    .is_some()
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
        let mut reverse_refs = Vec::new();
        for (child_table, meta) in projected_table_meta {
            let current_child_meta = self.table_meta(child_table);
            for column in &meta.columns {
                let Some(reference) = &column.references else {
                    continue;
                };
                let current_child_column = current_child_meta.as_ref().and_then(|current| {
                    current
                        .columns
                        .iter()
                        .find(|current_column| current_column.name == column.name)
                });
                let current_relationship_exists = current_child_column
                    .and_then(|current_column| current_column.references.as_ref())
                    == Some(reference);
                reverse_refs.push((
                    reference.table.clone(),
                    reference.column.clone(),
                    child_table.clone(),
                    column.name.clone(),
                    current_child_column.is_some(),
                    current_relationship_exists,
                ));
            }
        }

        for (parent_table, parent_values) in &deleted_projected_rows {
            for (
                ref_table,
                ref_column,
                child_table,
                child_column,
                current_child_column_exists,
                current_relationship_exists,
            ) in &reverse_refs
            {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_value) = parent_values.get(ref_column) else {
                    continue;
                };
                if *parent_value == Value::Null {
                    continue;
                }
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
                if !current_relationship_exists && !current_child_column_exists {
                    continue;
                }
                if self
                    .required_indexed_visible_row_by_column(
                        child_table,
                        child_column,
                        parent_value,
                        snapshot,
                        deleted_child_row_ids,
                    )?
                    .is_some()
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
                None,
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

    fn sync_incoming_values_allowed_for_access(
        &self,
        table: &str,
        meta: &TableMeta,
        values: &HashMap<String, Value>,
        lsn: Lsn,
    ) -> Result<bool> {
        if self.access_is_admin() {
            return Ok(true);
        }
        let row = VersionedRow {
            row_id: RowId(0),
            values: values.clone(),
            created_tx: TxId(0),
            deleted_tx: None,
            lsn,
            created_at: None,
        };
        self.read_allowed_for_row(table, meta, &row, self.snapshot())
    }

    fn sync_row_applies_over_committed(
        &self,
        row: &RowChange,
        committed: &VersionedRow,
        policy: ConflictPolicy,
    ) -> bool {
        match policy {
            ConflictPolicy::InsertIfNotExists | ConflictPolicy::ServerWins => false,
            ConflictPolicy::EdgeWins => true,
            ConflictPolicy::LatestWins => {
                let committed_source = self
                    .relational_store
                    .sync_source_lsn(&row.table, committed.row_id);
                self.sync_latest_wins_incoming(row, committed, committed_source)
            }
        }
    }

    /// LatestWins per-row comparison. `committed_source` is the row's stored
    /// sync provenance LSN (the sidecar), resolved once by the caller so the
    /// hot apply path performs a single sidecar probe per row. The row's
    /// provenance is anchored at its first sync-apply and is intentionally NOT
    /// the receiver's drifted commit LSN, so a genuinely-newer same-provenance
    /// update wins regardless of how far the receiver's commit clock has run
    /// ahead — and a downstream reader's echo (carrying the reader's inflated
    /// clock) cannot raise it.
    fn sync_latest_wins_incoming(
        &self,
        row: &RowChange,
        committed: &VersionedRow,
        committed_source: Option<Lsn>,
    ) -> bool {
        let committed_lsn = committed_source.unwrap_or(committed.lsn);
        if row.lsn != committed_lsn {
            return row.lsn > committed_lsn;
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
        row.values == committed.values
    }

    fn projected_sync_incoming_values_for_applied_rows(
        &self,
        rows: &[RowChange],
        policies: &ConflictPolicies,
    ) -> Result<ProjectedSyncApply> {
        let snapshot = self.snapshot();
        let mut projected_rows = Vec::<ProjectedSyncRow>::new();
        let mut projected_rows_cache = HashMap::new();
        let mut deleted_committed_row_ids = HashMap::<String, HashSet<RowId>>::new();
        let mut synthetic_row_ids = HashSet::<RowId>::new();
        let mut next_synthetic_row_id = u64::MAX;

        for row in rows {
            if row.values.is_empty() {
                continue;
            }
            if !row.deleted
                && let Some(table_meta) = self.table_meta(&row.table)
                && !self.sync_incoming_values_allowed_for_access(
                    &row.table,
                    &table_meta,
                    &row.values,
                    row.lsn,
                )?
            {
                continue;
            }

            let empty_deleted = HashSet::new();
            let skip_deleted = deleted_committed_row_ids
                .get(&row.table)
                .unwrap_or(&empty_deleted);
            let existing = sync_visible_point_lookup(
                self,
                &projected_rows_cache,
                &row.table,
                &row.natural_key.column,
                &row.natural_key.value,
                snapshot,
                skip_deleted,
            )?;

            if row.deleted {
                if let Some(local) = existing {
                    remove_cached_row(&mut projected_rows_cache, &row.table, local.row_id);
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
                        &projected_rows_cache,
                        deleted_committed_row_ids
                            .get(&row.table)
                            .unwrap_or(&empty_deleted),
                    )?
                    .is_none(),
                Some(local) => {
                    self.sync_row_applies_over_committed(row, local, policy)
                        && self
                            .sync_upsert_constraint_error_for_values(
                                &row.table,
                                &values,
                                local,
                                &projected_rows_cache,
                                deleted_committed_row_ids
                                    .get(&row.table)
                                    .unwrap_or(&empty_deleted),
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
                &mut projected_rows_cache,
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
        projected_rows_cache: &HashMap<String, Vec<VersionedRow>>,
        deleted_committed_row_ids: &HashSet<RowId>,
    ) -> Result<Option<String>> {
        self.sync_projected_row_constraint_error_for_values(
            table,
            values,
            projected_rows_cache,
            None,
            deleted_committed_row_ids,
        )
    }

    fn sync_projected_row_constraint_error_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        projected_rows_cache: &HashMap<String, Vec<VersionedRow>>,
        skip_row_id: Option<RowId>,
        deleted_committed_row_ids: &HashSet<RowId>,
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

        for col_def in &meta.columns {
            if (col_def.primary_key || col_def.unique)
                && let Some(new_value) = values.get(&col_def.name)
                && *new_value != Value::Null
                && projected_rows_cache.get(table).is_some_and(|rows| {
                    rows.iter().any(|row| {
                        skip_row_id != Some(row.row_id)
                            && row.values.get(&col_def.name) == Some(new_value)
                    })
                })
            {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}.{}",
                    table, col_def.name
                )));
            }
            if (col_def.primary_key || col_def.unique)
                && let Some(new_value) = values.get(&col_def.name)
                && *new_value != Value::Null
                && let Some(conflict) = self.required_indexed_visible_row_by_column(
                    table,
                    &col_def.name,
                    new_value,
                    self.snapshot(),
                    deleted_committed_row_ids,
                )?
                && skip_row_id != Some(conflict.row_id)
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
            if projected_rows_cache.get(table).is_some_and(|rows| {
                rows.iter().any(|row| {
                    skip_row_id != Some(row.row_id)
                        && tuple_values_for_row(row, unique_columns).as_ref() == Some(&tuple)
                })
            }) {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}({})",
                    table,
                    unique_columns.join(", ")
                )));
            }
            if let Some(conflict) = self.required_indexed_visible_row_by_columns(
                table,
                unique_columns,
                &tuple,
                self.snapshot(),
                deleted_committed_row_ids,
            )? && skip_row_id != Some(conflict.row_id)
            {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}({})",
                    table,
                    unique_columns.join(", ")
                )));
            }
        }

        Ok(None)
    }

    fn sync_committed_unique_conflict_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        deleted_committed_row_ids: &HashSet<RowId>,
    ) -> Result<Option<VersionedRow>> {
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

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
            if let Some(conflict) = self.required_indexed_visible_row_by_column(
                table,
                &column.name,
                value,
                self.snapshot(),
                deleted_committed_row_ids,
            )? {
                return Ok(Some(conflict));
            }
        }

        for columns in &meta.unique_constraints {
            let Some(tuple) = tuple_values_from_map(values, columns) else {
                continue;
            };
            if let Some(conflict) = self.required_indexed_visible_row_by_columns(
                table,
                columns,
                &tuple,
                self.snapshot(),
                deleted_committed_row_ids,
            )? {
                return Ok(Some(conflict));
            }
        }

        Ok(None)
    }

    fn sync_upsert_constraint_error_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        existing: &VersionedRow,
        projected_rows_cache: &HashMap<String, Vec<VersionedRow>>,
        deleted_committed_row_ids: &HashSet<RowId>,
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
            projected_rows_cache,
            Some(existing.row_id),
            deleted_committed_row_ids,
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
                None,
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
        let _vector_schema = self.vector_schema_read_many(self.vector_store_schema_refs());
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
                            created_at: row.created_at,
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
                        created_at: None,
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
        self.restore_vector_owner_rows(&mut rows, &vectors);

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

    #[cfg(feature = "test-seams")]
    pub fn vector_store_for_test(&self) -> Arc<VectorStore> {
        let _operation = self.assert_open_operation();
        self.vector_store.clone()
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

    /// Marks trigger registration complete after all declared callbacks have
    /// been registered.
    ///
    /// Trigger callbacks use the canonical callback contract documented on
    /// [`Error::CallbackActiveCrossThread`]. Same-DB cross-thread writers wait
    /// and proceed; unrelated cross-DB writers proceed independently;
    /// same-thread callback reentry returns [`Error::CallbackReentry`].
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

    /// Marks this handle as a sync relay hub.
    ///
    /// Relay handles file incoming sync batches from other machines without
    /// requiring local trigger callbacks for trigger-attached tables. The flag
    /// affects only sync-apply trigger callback readiness; trigger execution
    /// remains disabled on apply and all normal data/schema validation still
    /// runs.
    pub fn enable_sync_relay_mode(&self) {
        self.sync_relay_mode.store(true, Ordering::SeqCst);
    }

    pub(crate) fn sync_relay_mode_enabled(&self) -> bool {
        self.sync_relay_mode.load(Ordering::SeqCst)
    }

    /// Registers a host callback for a previously declared trigger. Every
    /// write to the trigger's table fires the callback synchronously inside
    /// the firing transaction's commit window. The callback's tx-bound
    /// `&Database` argument supports relational, graph, and vector cascade
    /// writes that commit atomically with the firing tx.
    ///
    /// # Concurrency contract
    ///
    /// While the callback is active on Thread A, other threads writing to
    /// the same `Database` (or an internal handle/`Arc` clone that shares
    /// the same trigger state) wait until the callback's transaction
    /// completes, then proceed as `Ok`. Unrelated cross-DB writers proceed
    /// independently; same-thread reentry returns [`Error::CallbackReentry`];
    /// callback tx-bound handles remain isolated to the runner thread; cron
    /// same-DB contention remains immediate; and a bounded deadlock-guard
    /// timeout returns the typed callback-active error plus one
    /// `tracing::warn!`.
    ///
    /// # Example
    ///
    /// Two writer threads + one parked callback. Both commits land.
    ///
    /// ```rust
    /// use contextdb_core::Value;
    /// use contextdb_engine::Database;
    /// use std::collections::HashMap;
    /// use std::sync::{Arc, Barrier, mpsc};
    /// use std::thread;
    /// use std::time::Duration;
    /// use uuid::Uuid;
    ///
    /// let db = Arc::new(Database::open_memory());
    /// db.execute("CREATE TABLE t (id UUID PRIMARY KEY)", &HashMap::new()).unwrap();
    /// db.execute("CREATE TRIGGER tr ON t WHEN INSERT", &HashMap::new()).unwrap();
    ///
    /// let entered = Arc::new(Barrier::new(2));
    /// let done = Arc::new(Barrier::new(2));
    /// let entered_cb = entered.clone();
    /// let done_cb = done.clone();
    /// db.register_trigger_callback("tr", move |_db, _ctx| {
    ///     entered_cb.wait();
    ///     done_cb.wait();
    ///     Ok(())
    /// }).unwrap();
    /// db.complete_initialization().unwrap();
    ///
    /// // Thread A fires the trigger (its callback parks at entered.wait()).
    /// let db_a = db.clone();
    /// let fire_a = thread::spawn(move || {
    ///     let tx = db_a.begin().expect("a begin");
    ///     db_a.execute_in_tx(
    ///         tx,
    ///         "INSERT INTO t (id) VALUES ($id)",
    ///         &HashMap::from([("id".to_string(), Value::Uuid(Uuid::from_u128(1)))]),
    ///     ).expect("a insert");
    ///     db_a.commit(tx).expect("a commit")
    /// });
    /// entered.wait();  // callback is now parked
    ///
    /// // Thread B writes against the same DB while A's callback is parked.
    /// // Same-DB contention waits and proceeds.
    /// let db_b = db.clone();
    /// let (started_tx, started_rx) = mpsc::channel();
    /// let writer_b = thread::spawn(move || {
    ///     started_tx.send(()).unwrap();
    ///     let tx = db_b.begin().expect("b begin must wait then succeed");
    ///     db_b.commit(tx).expect("b commit")
    /// });
    /// started_rx.recv().unwrap();
    /// thread::sleep(Duration::from_millis(50));
    ///
    /// // Release A's callback. Both commits land.
    /// done.wait();
    /// fire_a.join().unwrap();
    /// writer_b.join().unwrap();
    /// ```
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

    /// Lists declared triggers.
    ///
    /// See [`Error::CallbackActiveCrossThread`] for the trigger concurrency
    /// contract that applies once callbacks are registered and initialization
    /// is complete.
    pub fn list_triggers(&self) -> Vec<TriggerDeclaration> {
        let _operation = self.assert_open_operation();
        self.persisted_trigger_declarations()
    }

    /// Lists registered host callback names.
    ///
    /// See [`Error::CallbackActiveCrossThread`] for the same-DB wait,
    /// unrelated-DB independence, Class A reentry, cron B1, and deadlock-guard
    /// behavior that applies to active callbacks.
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

    /// Returns the in-memory trigger audit ring.
    ///
    /// Deadlock-guard timeouts are operator tracing events, not durable trigger
    /// audit rows; see [`Error::CallbackActiveCrossThread`] for the timeout
    /// contract.
    pub fn trigger_audit_log(&self) -> Vec<TriggerAuditEntry> {
        let _operation = self.assert_open_operation();
        self.trigger.audit_ring.lock().iter().cloned().collect()
    }

    /// Returns persisted trigger audit history filtered by trigger name/status.
    ///
    /// Deadlock-guard timeout diagnostics are emitted via `tracing::warn!`
    /// rather than this audit history. See [`Error::CallbackActiveCrossThread`].
    pub fn trigger_audit_history(
        &self,
        filter: TriggerAuditFilter,
    ) -> Result<Vec<TriggerAuditEntry>> {
        let _operation = self.open_operation()?;
        let history = if let Some(persistence) = &self.persistence {
            persistence.load_trigger_audit_history()?
        } else {
            self.trigger
                .volatile_audit_history
                .lock()
                .iter()
                .map(|(_, entry)| entry.clone())
                .collect()
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
        let _operation = self.open_operation_after_public_tx_control_wait("apply_changes")?;
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

        if self.sync_relay_mode_enabled() {
            if changes.ddl.is_empty() {
                let projected_table_meta = self.relational_store.table_meta.read().clone();
                self.preflight_sync_rows_against_projected_schema(
                    &changes.rows,
                    &projected_table_meta,
                )?;
            }
            return Ok(());
        }

        self.ensure_sync_apply_ready()
    }

    fn apply_changes_single_lsn_group(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<ApplyResult> {
        let mut tx = self.begin()?;
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
        let mut applied_rows_cache: HashMap<String, Vec<VersionedRow>> = HashMap::new();
        let mut applied_deleted_committed_row_ids: HashMap<String, HashSet<RowId>> = HashMap::new();
        let mut applied_new_row_ids: HashSet<RowId> = HashSet::new();
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
                        applied_rows_cache.remove(&name);
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
                            self.drain_vector_table_maintenance_for_ddl(&name);
                            let _vector_schema = self.vector_schema_write_table(&name);
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
                            self.drain_vector_table_maintenance_for_ddl(&name);
                            let _vector_schema = self.vector_schema_write_table(&name);
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
                        applied_rows_cache.remove(&name);
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
                        let incoming = rough_sync_table_meta(
                            &columns,
                            &constraints,
                            &foreign_keys,
                            &composite_foreign_keys,
                            &composite_unique,
                        );
                        let existing_vector_columns = Self::sync_vector_columns(&existing);
                        let incoming_vector_columns = Self::sync_vector_columns(&incoming);
                        let existing_column_names = Self::sync_meta_column_names(&existing);
                        let vector_shape_changed = Self::sync_alter_vector_shape_changed(
                            &existing,
                            &incoming,
                            &constraints,
                        );
                        if vector_shape_changed {
                            let removed_vector_columns = existing_vector_columns
                                .keys()
                                .filter(|column| !incoming_vector_columns.contains_key(*column))
                                .cloned()
                                .collect::<Vec<_>>();
                            let renamed_vector_column =
                                Self::sync_marked_vector_rename(&existing, &incoming, &constraints);
                            let changed_existing_columns = existing_vector_columns
                                .iter()
                                .filter(|(column, shape)| {
                                    incoming_vector_columns
                                        .get(*column)
                                        .is_some_and(|incoming_shape| incoming_shape != *shape)
                                })
                                .map(|(column, _)| column.clone())
                                .collect::<Vec<_>>();
                            for column in removed_vector_columns
                                .iter()
                                .chain(changed_existing_columns.iter())
                            {
                                self.drain_vector_index_maintenance_for_ddl(&VectorIndexRef::new(
                                    &name,
                                    column.clone(),
                                ));
                            }
                            let vector_schema_refs = existing_vector_columns
                                .keys()
                                .chain(incoming_vector_columns.keys())
                                .map(|column| VectorIndexRef::new(&name, column.clone()));
                            let _vector_schema = self.vector_schema_write_many(vector_schema_refs);
                            self.allocate_ddl_lsn(|lsn| {
                                let store = self.relational_store();
                                let meta = Self::merged_sync_full_shape_vector_alter_meta(
                                    &name,
                                    &existing,
                                    incoming.clone(),
                                    &constraints,
                                );
                                if let Some((from, to)) = &renamed_vector_column {
                                    store
                                        .alter_table_rename_column(&name, from, to)
                                        .map_err(Error::Other)?;
                                }
                                for column in &removed_vector_columns {
                                    if renamed_vector_column
                                        .as_ref()
                                        .is_some_and(|(from, _)| from == column)
                                    {
                                        continue;
                                    }
                                    store
                                        .alter_table_drop_column(&name, column)
                                        .map_err(Error::Other)?;
                                }
                                for column in &changed_existing_columns {
                                    store
                                        .alter_table_drop_column(&name, column)
                                        .map_err(Error::Other)?;
                                }

                                let meta = self.replace_table_meta_and_refresh_auto_indexes(
                                    &name, &existing, meta,
                                )?;

                                if let Some((from, to)) = &renamed_vector_column
                                    && self
                                        .vector_store
                                        .try_state(&VectorIndexRef::new(&name, from))
                                        .is_some()
                                {
                                    self.rename_vector_index(&name, from, to)?;
                                }
                                for column in &removed_vector_columns {
                                    if renamed_vector_column
                                        .as_ref()
                                        .is_some_and(|(from, _)| from == column)
                                    {
                                        continue;
                                    }
                                    self.deregister_vector_index(&name, column);
                                }
                                for column in &changed_existing_columns {
                                    self.deregister_vector_index(&name, column);
                                }
                                for column in &meta.columns {
                                    if matches!(column.column_type, ColumnType::Vector(_))
                                        && self
                                            .vector_store
                                            .try_state(&VectorIndexRef::new(&name, &column.name))
                                            .is_none()
                                    {
                                        self.register_vector_index_for_column(&name, column);
                                    }
                                }
                                if let Some((from, to)) = &renamed_vector_column {
                                    self.persist_table_meta_rows_vectors_and_log_alter_table_ddl_with_vector_rename(
                                        &name, &meta, from, to, lsn,
                                    )
                                } else {
                                    self.persist_table_meta_rows_vectors_and_log_alter_table_ddl(
                                        &name, &meta, lsn,
                                    )
                                }
                            })?;
                        } else {
                            for (col, ty) in &columns {
                                if existing_column_names.contains(col.as_str()) {
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
                        }
                        if !vector_shape_changed && let Some(mut meta) = self.table_meta(&name) {
                            let old_meta = meta.clone();
                            for incoming_column in incoming.columns {
                                if let Some(existing_column) = meta
                                    .columns
                                    .iter_mut()
                                    .find(|column| column.name == incoming_column.name)
                                {
                                    merge_sync_alter_existing_column(
                                        existing_column,
                                        incoming_column,
                                    );
                                }
                            }
                            if !incoming.unique_constraints.is_empty() {
                                meta.unique_constraints = incoming.unique_constraints;
                            }
                            if !incoming.composite_foreign_keys.is_empty() {
                                meta.composite_foreign_keys = incoming.composite_foreign_keys;
                            }
                            let meta = self.replace_table_meta_and_refresh_auto_indexes(
                                &name, &old_meta, meta,
                            )?;
                            self.persist_table_meta(&name, &meta)?;
                        }
                        self.clear_statement_cache();
                        table_meta_cache.remove(&name);
                        applied_rows_cache.remove(&name);
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
                                if let Some(prefix) = crate::executor::reserved_index_prefix(&name)
                                {
                                    return Err(Error::ReservedIndexName {
                                        table: table.clone(),
                                        name: name.clone(),
                                        prefix: prefix.to_string(),
                                    });
                                }
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
                            applied_rows_cache.remove(&table);
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
        let vector_tables = changes
            .vectors
            .iter()
            .map(|vector| vector.index.table.clone())
            .collect::<HashSet<_>>();
        let mut rows = changes.rows;
        rows.sort_by(|left, right| {
            if vector_tables.contains(&left.table) || vector_tables.contains(&right.table) {
                std::cmp::Ordering::Equal
            } else {
                right.deleted.cmp(&left.deleted)
            }
        });
        let mut non_vector_delete_phase_open = false;
        for row in rows {
            if row.values.is_empty() {
                result.skipped_rows += 1;
                if commit_each_row {
                    self.commit_with_source(tx, CommitSource::SyncPull)?;
                    tx = self.begin()?;
                }
                continue;
            }

            let policy = policies
                .per_table
                .get(&row.table)
                .copied()
                .unwrap_or(policies.default);

            let row_has_vector = cached_table_meta(self, &mut table_meta_cache, &row.table)
                .is_some_and(|meta| {
                    meta.columns
                        .iter()
                        .any(|col| matches!(col.column_type, ColumnType::Vector(_)))
                });
            if !row.deleted && non_vector_delete_phase_open {
                self.commit_with_source(tx, CommitSource::SyncPull)?;
                tx = self.begin()?;
                non_vector_delete_phase_open = false;
            }

            if !row.deleted
                && let Some(meta) = cached_table_meta(self, &mut table_meta_cache, &row.table)
                && !self.sync_incoming_values_allowed_for_access(
                    &row.table,
                    &meta,
                    &row.values,
                    row.lsn,
                )?
            {
                result.skipped_rows += 1;
                let row_has_vector = meta
                    .columns
                    .iter()
                    .any(|col| matches!(col.column_type, ColumnType::Vector(_)));
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
                    reason: Some("incoming row hidden by access scope".to_string()),
                });
                if commit_each_row {
                    self.commit_with_source(tx, CommitSource::SyncPull)?;
                    tx = self.begin()?;
                }
                continue;
            }

            let skip_deleted = applied_deleted_committed_row_ids
                .get(&row.table)
                .cloned()
                .unwrap_or_default();
            let existing = match sync_visible_point_lookup(
                self,
                &applied_rows_cache,
                &row.table,
                &row.natural_key.column,
                &row.natural_key.value,
                self.snapshot(),
                &skip_deleted,
            ) {
                Ok(existing) => existing,
                Err(err) => {
                    let _ = self.rollback(tx);
                    return Err(err);
                }
            };
            let is_delete = row.deleted;

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
                        remove_cached_row(&mut applied_rows_cache, &row.table, local.row_id);
                        if !applied_new_row_ids.remove(&local.row_id) {
                            applied_deleted_committed_row_ids
                                .entry(row.table.clone())
                                .or_default()
                                .insert(local.row_id);
                        }
                        result.applied_rows += 1;
                        if !row_has_vector {
                            non_vector_delete_phase_open = true;
                        }
                    }
                } else {
                    result.skipped_rows += 1;
                }
                if commit_each_row {
                    self.commit_with_source(tx, CommitSource::SyncPull)?;
                    tx = self.begin()?;
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

                        if constraint_error.is_none()
                            && let Some(err) = self.sync_insert_constraint_error_for_values(
                                &row.table,
                                &values,
                                &applied_rows_cache,
                                &skip_deleted,
                            )?
                        {
                            constraint_error = Some(err);
                        }

                        if constraint_error.is_some()
                            && matches!(policy, ConflictPolicy::EdgeWins)
                            && !row_has_vector
                            && let Some(conflict) = self.sync_committed_unique_conflict_for_values(
                                &row.table,
                                &values,
                                &skip_deleted,
                            )?
                        {
                            let mut skip_with_conflict = skip_deleted.clone();
                            skip_with_conflict.insert(conflict.row_id);
                            if self
                                .sync_insert_constraint_error_for_values(
                                    &row.table,
                                    &values,
                                    &applied_rows_cache,
                                    &skip_with_conflict,
                                )?
                                .is_none()
                            {
                                if let Err(err) = self.delete_row(tx, &row.table, conflict.row_id) {
                                    constraint_error = Some(format!(
                                        "edge_wins unique replacement delete failed: {err}"
                                    ));
                                } else {
                                    remove_cached_row(
                                        &mut applied_rows_cache,
                                        &row.table,
                                        conflict.row_id,
                                    );
                                    if !applied_new_row_ids.remove(&conflict.row_id) {
                                        applied_deleted_committed_row_ids
                                            .entry(row.table.clone())
                                            .or_default()
                                            .insert(conflict.row_id);
                                    }
                                    constraint_error = None;
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
                                tx = self.begin()?;
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
                                tx = self.begin()?;
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

                    match self.insert_row_for_sync(tx, &row.table, values.clone(), row.created_at) {
                        Ok(new_row_id) => {
                            if let Err(err) =
                                self.set_sync_row_source_lsn(tx, &row.table, new_row_id, row.lsn)
                            {
                                let _ = self.rollback(tx);
                                return Err(err);
                            }
                            applied_new_row_ids.insert(new_row_id);
                            record_cached_insert(
                                &mut applied_rows_cache,
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
                    // Resolve the row's stored provenance once: it feeds the
                    // win/lose comparison and, on apply, is re-stamped unchanged
                    // so the upsert's internal delete (which clears the sidecar)
                    // does not drop the anchor. A single sidecar probe per row
                    // keeps the hot apply path allocation- and double-probe-free.
                    let committed_source = self
                        .relational_store
                        .sync_source_lsn(&row.table, local.row_id);
                    let incoming_wins =
                        self.sync_latest_wins_incoming(&row, &local, committed_source);

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
                            reason: Some("latest_wins_local_lsn_newer_or_equal".to_string()),
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
                                        tx = self.begin()?;
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
                            row.created_at,
                        ) {
                            Ok(upsert_result) => {
                                // A byte-identical re-delivery upserts to NoOp:
                                // storage is untouched, provenance stays put, and
                                // it must not count as applied work.
                                if matches!(upsert_result, UpsertResult::NoOp) {
                                    if row_has_vector
                                        && vector_row_ids.get(vector_row_idx).is_some()
                                    {
                                        consume_vector_row_group(
                                            &vector_row_ids,
                                            &mut vector_row_idx,
                                            local.row_id,
                                            &mut vector_row_map,
                                        );
                                    }
                                    if commit_each_row {
                                        self.commit_with_source(tx, CommitSource::SyncPull)?;
                                        tx = self.begin()?;
                                    }
                                    continue;
                                }
                                // Re-stamp the EXISTING provenance (anchored at
                                // first sync-apply), not this update's emission
                                // LSN. The upsert internally deletes+reinserts the
                                // row, which clears the sidecar; writing the frozen
                                // value back preserves the anchor. Moving it to the
                                // emission LSN is what let a downstream reader's
                                // echo (carrying the reader's inflated clock) poison
                                // the anchor and strand the writer.
                                if let Some(source_lsn) = committed_source
                                    && let Err(err) = self.set_sync_row_source_lsn(
                                        tx,
                                        &row.table,
                                        local.row_id,
                                        source_lsn,
                                    )
                                {
                                    let _ = self.rollback(tx);
                                    return Err(err);
                                }
                                applied_deleted_committed_row_ids
                                    .entry(row.table.clone())
                                    .or_default()
                                    .insert(local.row_id);
                                upsert_cached_projection(
                                    &mut applied_rows_cache,
                                    &row.table,
                                    local.row_id,
                                    values.clone(),
                                    row.lsn,
                                );
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
                (Some(local), ConflictPolicy::EdgeWins) => {
                    result.conflicts.push(Conflict {
                        natural_key: row.natural_key.clone(),
                        resolution: ConflictPolicy::EdgeWins,
                        reason: Some("edge_wins".to_string()),
                    });
                    let committed_source = self
                        .relational_store
                        .sync_source_lsn(&row.table, local.row_id);
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
                        row.created_at,
                    ) {
                        Ok(_upsert_result) => {
                            // Re-stamp the frozen provenance the upsert just
                            // cleared; an EdgeWins update does not move the anchor
                            // (see the LatestWins arm).
                            if let Some(source_lsn) = committed_source
                                && let Err(err) = self.set_sync_row_source_lsn(
                                    tx,
                                    &row.table,
                                    local.row_id,
                                    source_lsn,
                                )
                            {
                                let _ = self.rollback(tx);
                                return Err(err);
                            }
                            applied_deleted_committed_row_ids
                                .entry(row.table.clone())
                                .or_default()
                                .insert(local.row_id);
                            upsert_cached_projection(
                                &mut applied_rows_cache,
                                &row.table,
                                local.row_id,
                                values.clone(),
                                row.lsn,
                            );
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
                tx = self.begin()?;
            }
        }

        if batch_row_commits {
            self.commit_with_source(tx, CommitSource::SyncPull)?;
            tx = self.begin()?;
        }

        for edge in changes.edges {
            let is_delete = matches!(edge.properties.get("__deleted"), Some(Value::Bool(true)));
            if is_delete {
                if let Err(err) = self.delete_edge(tx, edge.source, edge.target, &edge.edge_type) {
                    if is_sync_access_scope_error(&err) {
                        result.skipped_rows += 1;
                        continue;
                    }
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
                    if is_sync_access_scope_error(&err) {
                        result.skipped_rows += 1;
                        continue;
                    }
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

    fn restore_vector_owner_rows(&self, rows: &mut Vec<RowChange>, vectors: &[VectorChange]) {
        if vectors.is_empty() {
            return;
        }
        let mut represented = rows
            .iter()
            .filter(|row| !row.deleted)
            .map(|row| {
                (
                    row.table.clone(),
                    row.natural_key.column.clone(),
                    format!("{:?}", row.natural_key.value),
                    row.lsn,
                )
            })
            .collect::<HashSet<_>>();
        let snapshot = self.snapshot();
        for vector in vectors {
            let Some(row) =
                self.row_visible_at_snapshot(&vector.index.table, vector.row_id, snapshot)
            else {
                continue;
            };
            if !self.row_read_allowed_for_change(&vector.index.table, &row, snapshot) {
                continue;
            }
            let Some((natural_key, values)) =
                self.row_change_values_from_row(&vector.index.table, &row)
            else {
                continue;
            };
            let key = (
                vector.index.table.clone(),
                natural_key.column.clone(),
                format!("{:?}", natural_key.value),
                vector.lsn,
            );
            if !represented.insert(key) {
                continue;
            }
            rows.push(RowChange {
                table: vector.index.table.clone(),
                natural_key,
                values,
                deleted: false,
                lsn: vector.lsn,
                created_at: None,
            });
        }
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

    pub(crate) fn assert_row_id_read_allowed_for_change(
        &self,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<()> {
        if let Some(tx) = tx {
            let staged = self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_inserts
                    .iter()
                    .rev()
                    .find(|(insert_table, row)| insert_table == table && row.row_id == row_id)
                    .map(|(_, row)| row.clone())
            })?;
            if let Some(row) = staged {
                let rows = self.filter_rows_for_anchor_read(table, vec![row], snapshot)?;
                if rows.is_empty() {
                    return Err(Error::NotFound(format!("row {row_id} in table {table}")));
                }
                return Ok(());
            }
        }
        if self.row_id_read_allowed_for_change(table, row_id, snapshot) {
            return Ok(());
        }
        let Some(row) = self.row_visible_at_snapshot(table, row_id, snapshot) else {
            return Err(Error::NotFound(format!("row {row_id} in table {table}")));
        };
        let rows = self.filter_rows_for_anchor_read(table, vec![row], snapshot)?;
        if rows.is_empty() {
            return Err(Error::NotFound(format!("row {row_id} in table {table}")));
        }
        Ok(())
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

fn write_set_touches_vector_search(ws: &WriteSet, index: &VectorIndexRef) -> bool {
    ws.vector_inserts.iter().any(|entry| entry.index == *index)
        || ws
            .vector_deletes
            .iter()
            .any(|(delete_index, _, _)| delete_index == index)
        || ws
            .vector_moves
            .iter()
            .any(|(move_index, _, _, _)| move_index == index)
        || ws
            .relational_deletes
            .iter()
            .any(|(delete_table, _, _)| delete_table == &index.table)
}

fn estimate_active_tx_vector_overlay_bytes(entry_count: usize, dimension: usize) -> usize {
    let decoded_vector_bytes = dimension.saturating_mul(std::mem::size_of::<f32>());
    let per_entry = std::mem::size_of::<VectorEntry>()
        .saturating_add(std::mem::size_of::<(RowId, Vec<f32>)>())
        .saturating_add(decoded_vector_bytes)
        .saturating_add(64);
    entry_count.saturating_mul(per_entry)
}

// Row-stamp clock. Delegates to the canonical `Wallclock::now()` so the core
// test-clock seam covers every `created_at` stamp. Semantic delta vs the old
// inline read, reachable only with a pre-Unix-epoch system clock: this panics
// (`unwrap`) where the old read stamped 0 (`unwrap_or_default`) — deliberate,
// because a 0 stamp makes every TTL row instantly ancient and silently pruned.
fn current_wallclock() -> Wallclock {
    Wallclock::now()
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

fn projected_point_lookup(
    cache: &HashMap<String, Vec<VersionedRow>>,
    table: &str,
    col: &str,
    value: &Value,
) -> Option<VersionedRow> {
    cache
        .get(table)
        .and_then(|rows| rows.iter().find(|r| r.values.get(col) == Some(value)))
        .cloned()
}

fn sync_visible_point_lookup(
    db: &Database,
    cache: &HashMap<String, Vec<VersionedRow>>,
    table: &str,
    col: &str,
    value: &Value,
    snapshot: SnapshotId,
    skip_deleted: &HashSet<RowId>,
) -> Result<Option<VersionedRow>> {
    if let Some(projected) = projected_point_lookup(cache, table, col, value) {
        return Ok(Some(projected));
    }
    if db.table_meta(table).is_none() {
        return Err(Error::TableNotFound(table.to_string()));
    }
    db.required_indexed_visible_row_by_column(table, col, value, snapshot, skip_deleted)
}

fn record_cached_insert(
    cache: &mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
    row: VersionedRow,
) {
    cache.entry(table.to_string()).or_default().push(row);
}

fn upsert_cached_projection(
    cache: &mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
    row_id: RowId,
    values: HashMap<String, Value>,
    lsn: Lsn,
) {
    let rows = cache.entry(table.to_string()).or_default();
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

struct VectorExplainShape {
    index: VectorIndexRef,
    k: usize,
    restricted_candidates: bool,
}

fn vector_search_shape_from_plan(plan: &PhysicalPlan) -> Option<VectorExplainShape> {
    match plan {
        PhysicalPlan::VectorSearch {
            table,
            column,
            k,
            candidates,
            ..
        }
        | PhysicalPlan::HnswSearch {
            table,
            column,
            k,
            candidates,
            ..
        } => Some(VectorExplainShape {
            index: VectorIndexRef::new(table.clone(), column.clone()),
            k: usize::try_from(*k).unwrap_or(usize::MAX),
            restricted_candidates: candidates
                .as_deref()
                .is_some_and(|candidate| !is_unrestricted_scan_for_table(candidate, table)),
        }),
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. } => vector_search_shape_from_plan(input),
        PhysicalPlan::Join { left, right, .. } => {
            vector_search_shape_from_plan(left).or_else(|| vector_search_shape_from_plan(right))
        }
        PhysicalPlan::Pipeline(plans) => plans.iter().find_map(vector_search_shape_from_plan),
        _ => None,
    }
}

fn is_unrestricted_scan_for_table(plan: &PhysicalPlan, table: &str) -> bool {
    matches!(
        plan,
        PhysicalPlan::Scan {
            table: scan_table,
            filter: None,
            ..
        } if scan_table == table
    )
}

fn annotate_vector_search_strategy(mut output: String, strategy: &str) -> String {
    let needle = "VectorSearch(";
    let mut search_from = 0;
    while let Some(relative_pos) = output[search_from..].find(needle) {
        let operator_start = search_from + relative_pos;
        let open_paren = operator_start + needle.len() - 1;
        let mut depth = 0_u32;
        let mut insert_at = None;
        for (relative_idx, ch) in output[open_paren..].char_indices() {
            match ch {
                '(' => depth = depth.saturating_add(1),
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        insert_at = Some(open_paren + relative_idx);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(insert_at) = insert_at else {
            break;
        };
        let annotation = format!(", strategy={strategy}");
        output.insert_str(insert_at, &annotation);
        search_from = insert_at + annotation.len();
    }
    output
}

fn sanitize_loaded_row_for_meta(row: &mut VersionedRow, meta: &TableMeta) -> bool {
    let columns = meta
        .columns
        .iter()
        .map(|column| (column.name.as_str(), &column.column_type))
        .collect::<HashMap<_, _>>();
    let before = row.values.len();
    row.values
        .retain(|column, value| match columns.get(column.as_str()) {
            Some(ColumnType::Vector(dimension)) => {
                matches!(value, Value::Vector(vector) if vector.len() == *dimension)
            }
            Some(_) => true,
            None => false,
        });
    row.values.len() != before
}

fn vector_specs_from_meta(
    table_meta: &HashMap<String, TableMeta>,
) -> HashMap<VectorIndexRef, usize> {
    let mut specs = HashMap::new();
    for (table, meta) in table_meta {
        for column in &meta.columns {
            if let ColumnType::Vector(dimension) = column.column_type {
                specs.insert(
                    VectorIndexRef::new(table.clone(), column.name.clone()),
                    dimension,
                );
            }
        }
    }
    specs
}

#[derive(Clone)]
struct VectorRenameDdl {
    lsn: Lsn,
    from: VectorIndexRef,
    to: VectorIndexRef,
}

fn vector_renames_from_ddl_log(ddl_log: &[(Lsn, DdlChange)]) -> Vec<VectorRenameDdl> {
    let mut renames = Vec::new();
    for (lsn, change) in ddl_log {
        let DdlChange::AlterTable {
            name, constraints, ..
        } = change
        else {
            continue;
        };
        if let Some((from, to)) = sync_vector_rename_from_constraints(constraints) {
            renames.push(VectorRenameDdl {
                lsn: *lsn,
                from: VectorIndexRef::new(name, from),
                to: VectorIndexRef::new(name, to),
            });
        }
    }
    renames.sort_by(|a, b| {
        a.lsn
            .cmp(&b.lsn)
            .then(a.from.table.cmp(&b.from.table))
            .then(a.from.column.cmp(&b.from.column))
            .then(a.to.table.cmp(&b.to.table))
            .then(a.to.column.cmp(&b.to.column))
    });
    renames
}

fn resolve_loaded_vector_index(
    index: &VectorIndexRef,
    entry_lsn: Lsn,
    vector_specs: &HashMap<VectorIndexRef, usize>,
    renames: &[VectorRenameDdl],
) -> Option<VectorIndexRef> {
    let mut current = index.clone();
    let mut seen = HashSet::new();
    for _ in 0..=renames.len() {
        if !seen.insert(current.clone()) {
            return None;
        }
        if let Some(rename) = renames
            .iter()
            .filter(|rename| rename.from == current && entry_lsn <= rename.lsn)
            .min_by_key(|rename| rename.lsn)
        {
            current = rename.to.clone();
            continue;
        }
        return vector_specs.contains_key(&current).then_some(current);
    }
    None
}

fn reconcile_loaded_vectors_for_meta(
    vectors: Vec<VectorEntry>,
    table_meta: &HashMap<String, TableMeta>,
    ddl_log: &[(Lsn, DdlChange)],
) -> (Vec<VectorEntry>, bool) {
    let vector_specs = vector_specs_from_meta(table_meta);
    let renames = vector_renames_from_ddl_log(ddl_log);
    let mut repaired = false;
    let mut reconciled = Vec::with_capacity(vectors.len());
    for mut entry in vectors {
        let Some(index) =
            resolve_loaded_vector_index(&entry.index, entry.lsn, &vector_specs, &renames)
        else {
            repaired = true;
            continue;
        };
        if entry.index != index {
            entry.index = index;
            repaired = true;
        }
        if vector_specs.get(&entry.index).copied() == Some(entry.vector.len()) {
            reconciled.push(entry);
        } else {
            repaired = true;
        }
    }
    (reconciled, repaired)
}

fn supplement_loaded_vectors_from_rows(
    relational: &RelationalStore,
    table_meta: &HashMap<String, TableMeta>,
    vectors: &mut Vec<VectorEntry>,
) -> bool {
    let mut seen = vectors
        .iter()
        .map(|entry| {
            (
                entry.index.clone(),
                entry.row_id,
                entry.created_tx,
                entry.lsn,
            )
        })
        .collect::<HashSet<_>>();
    let mut supplemented = false;
    let tables = relational.tables.read();
    for (table, meta) in table_meta {
        let Some(rows) = tables.get(table) else {
            continue;
        };
        for column in &meta.columns {
            let ColumnType::Vector(dimension) = column.column_type else {
                continue;
            };
            let index = VectorIndexRef::new(table.clone(), column.name.clone());
            for row in rows {
                let Some(Value::Vector(vector)) = row.values.get(&column.name) else {
                    continue;
                };
                if vector.len() != dimension {
                    continue;
                }
                let key = (index.clone(), row.row_id, row.created_tx, row.lsn);
                if seen.insert(key) {
                    vectors.push(VectorEntry {
                        index: index.clone(),
                        row_id: row.row_id,
                        vector: vector.clone(),
                        created_tx: row.created_tx,
                        deleted_tx: row.deleted_tx,
                        lsn: row.lsn,
                    });
                    supplemented = true;
                }
            }
        }
    }
    supplemented
}

fn hydrate_relational_vector_values(
    relational: &RelationalStore,
    vectors: &[VectorEntry],
) -> HashSet<String> {
    let mut changed = HashSet::new();
    if vectors.is_empty() {
        return changed;
    }
    let mut tables = relational.tables.write();
    for entry in vectors {
        let Some(rows) = tables.get_mut(&entry.index.table) else {
            continue;
        };
        if let Some(row) = rows.iter_mut().find(|row| {
            row.row_id == entry.row_id && row.created_tx == entry.created_tx && row.lsn == entry.lsn
        }) {
            let value = Value::Vector(entry.vector.clone());
            if row.values.get(&entry.index.column) != Some(&value) {
                row.values.insert(entry.index.column.clone(), value);
                changed.insert(entry.index.table.clone());
            }
        }
    }
    changed
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
        {
            let _operation_barrier = self.operation_gate.write();
            if self.closed.swap(true, Ordering::SeqCst) {
                return;
            }
            if self.resource_owner {
                self.resource_closed.store(true, Ordering::SeqCst);
            }
        }
        self.stop_cron_tickler();
        let event_bus_shutdown = self.stop_event_bus_threads();
        let runtime = self.pruning_runtime.get_mut();
        runtime.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = runtime.handle.take() {
            let _ = handle.join();
        }
        if self.resource_owner {
            self.subscriptions.lock().subscribers.clear();
            if !event_bus_shutdown.deferred_resource_cleanup() {
                if let Some(persistence) = &self.persistence {
                    persistence.close();
                }
                self.release_open_registry();
            }
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
    created_tx: TxId,
}

impl RetentionRowKey {
    fn new(table: &str, row_id: RowId, created_tx: TxId) -> Self {
        Self {
            table: table.to_string(),
            row_id,
            created_tx,
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

fn prune_expired_rows(ctx: &MaintenanceContext, sync_watermark: Lsn) -> Result<PruningReport> {
    let relational_store = &ctx.relational;
    let graph_store = &ctx.graph;
    let vector_store = &ctx.vector;
    let accountant = ctx.accountant.as_ref();
    let persistence = ctx.persistence.as_ref();
    let change_log = &ctx.change_log;
    let tx_mgr = ctx.tx_mgr.as_ref();
    let now = Wallclock::now();
    let metas = relational_store.table_meta.read().clone();
    let mut pruned_versions_by_table: HashMap<String, Vec<(RowId, TxId)>> = HashMap::new();
    let mut pruned_live_row_ids = HashSet::new();
    let mut pruned_node_ids = HashSet::new();
    let mut pruned_change_keys: HashSet<(String, RowId, Lsn)> = HashSet::new();
    let mut released_row_bytes = 0usize;
    let mut blocked = Vec::new();

    let table_snapshot = relational_store.tables.read().clone();
    // A stamp further ahead than the tolerance can never age out on this
    // holder's clock, so it would pin its row silently. Count it per table,
    // every cycle, whether or not anything prunes.
    let skew_horizon = now
        .0
        .saturating_add(RETENTION_CLOCK_SKEW_TOLERANCE.as_millis() as u64);
    let mut future_dated_rows = 0u64;
    let mut future_dated_tables: Vec<String> = Vec::new();
    for (table_name, rows) in &table_snapshot {
        let Some(meta) = metas.get(table_name) else {
            continue;
        };
        if meta.default_ttl_seconds.is_none() {
            continue;
        }
        let mut table_has_future_dated = false;
        for row in rows {
            if row.deleted_tx.is_none()
                && row.created_at.is_some_and(|stamp| stamp.0 > skew_horizon)
            {
                future_dated_rows = future_dated_rows.saturating_add(1);
                table_has_future_dated = true;
            }
        }
        if table_has_future_dated {
            future_dated_tables.push(table_name.clone());
        }
    }
    future_dated_tables.sort();
    let prune_candidates = retention_prune_candidates(
        &metas,
        &table_snapshot,
        now,
        sync_watermark,
        relational_store,
    );
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
            let row_key = RetentionRowKey::new(table_name, row.row_id, row.created_tx);
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

            pruned_versions_by_table
                .entry(table_name.clone())
                .or_default()
                .push((row.row_id, row.created_tx));
            // The change-log entries this row produced go with it. Collected
            // from the rows actually being pruned, so no watermark logic is
            // needed here: `row_is_prunable` already refuses to prune an
            // unconfirmed SYNC SAFE row, which means a pruned row is a
            // confirmed row and an unconfirmed backlog's entries are never in
            // this set. One rule, enforced at one place.
            pruned_change_keys.insert((table_name.clone(), row.row_id, row.lsn));
            released_row_bytes = released_row_bytes.saturating_add(estimate_row_bytes_for_meta(
                &row.values,
                meta,
                false,
            ));
            if row.deleted_tx.is_none() {
                pruned_live_row_ids.insert(row.row_id);
                if let Some(Value::Uuid(id)) = row.values.get("id") {
                    pruned_node_ids.insert(*id);
                }
            }
        }
    }

    let pruned_version_count = pruned_versions_by_table
        .values()
        .map(|versions| versions.len() as u64)
        .sum::<u64>();
    if pruned_version_count == 0 {
        return Ok(PruningReport {
            pruned_rows: 0,
            blocked_count: blocked.len() as u64,
            blocked,
            future_dated_rows,
            future_dated_tables,
            ..PruningReport::default()
        });
    }

    let pruned_versions_by_table_sets = pruned_versions_by_table
        .iter()
        .map(|(table, versions)| {
            (
                table.clone(),
                versions.iter().copied().collect::<HashSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let post_prune_table_rows = pruned_versions_by_table_sets
        .iter()
        .map(|(table_name, versions)| {
            let rows = table_snapshot
                .get(table_name)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|row| !versions.contains(&(row.row_id, row.created_tx)))
                .collect::<Vec<_>>();
            (table_name.clone(), rows)
        })
        .collect::<HashMap<_, _>>();
    let post_prune_vectors = vector_store
        .all_entries()
        .into_iter()
        .filter(|entry| !pruned_live_row_ids.contains(&entry.row_id))
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

    // The rows are leaving, so the change-log entries that referenced them
    // leave in the SAME commit. Without this the log kept an entry for every
    // write ever made — the rows aged out while their history did not, and the
    // file grew linearly under steady churn however little data was live.
    // Prune the in-memory log first and keep the survivors to rewrite
    // persistence with, so memory and disk agree; CHANGE-LOG-FIRST for the same
    // crash-safety reason the currency path documents.
    let surviving_change_log = {
        let mut log = change_log.write();
        log.retain(|entry| !change_entry_references_pruned_version(entry, &pruned_change_keys));
        log.clone()
    };

    if let Some(persistence) = persistence {
        persistence.rewrite_change_log(&surviving_change_log)?;
        persistence.rewrite_pruned_state(
            &post_prune_table_rows,
            &post_prune_vectors,
            &post_prune_edges,
        )?;
    }

    // The commit index holds one entry per commit and nothing removed them, so
    // it grew with every write forever — after the change log was bounded it
    // was the only remaining per-write term in the file. Trim it LAST, and
    // deliberately: over-retention is the safe failure direction (spare entries
    // are inert), whereas an index trimmed before the rows left could make a
    // still-present row invisible until the next reopen rebuilt it.
    let retained_commit_index = retained_commit_index_after_prune(tx_mgr, &surviving_change_log);
    let pruned_commit_index_entries = tx_mgr.replace_commit_index(retained_commit_index.clone());
    if pruned_commit_index_entries > 0
        && let Some(persistence) = persistence
    {
        persistence.rewrite_commit_index(&retained_commit_index)?;
    }

    for (table_name, versions) in &pruned_versions_by_table_sets {
        relational_store.remove_row_versions(table_name, versions);
    }

    for table_name in pruned_versions_by_table.keys() {
        if let Some(meta) = metas.get(table_name) {
            for index in &meta.indexes {
                relational_store.rebuild_index(table_name, &index.name);
            }
        }
    }

    let released_vector_bytes = vector_store.prune_row_ids(&pruned_live_row_ids, accountant);

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
        pruned_rows: pruned_version_count,
        blocked_count: blocked.len() as u64,
        blocked,
        reclaimed_bytes: released_row_bytes
            .saturating_add(released_vector_bytes)
            .saturating_add(released_edge_bytes) as u64,
        future_dated_rows,
        future_dated_tables,
        pruned_commit_index_entries,
        ..PruningReport::default()
    })
}

fn checked_prune_expired_rows(
    ctx: &MaintenanceContext,
    sync_watermark: Lsn,
) -> Result<PruningReport> {
    prune_expired_rows(ctx, sync_watermark)
}

/// The commit-index entries a prune must KEEP.
///
/// `snapshot_at` is a FLOOR lookup (`range(..=lsn).next_back()`), and the delete
/// arms of `changes_since` resolve `snapshot_at(lsn - 1)` — they ask about the
/// LSN BELOW their own entry. So retaining only the surviving change log's own
/// LSNs would leave the OLDEST surviving delete resolving against nothing,
/// degrading to an all-invisible snapshot that silently drops it from the
/// changeset. The kept set is therefore every surviving entry PLUS the ANCHOR:
/// the greatest entry strictly below the lowest LSN any consumer can name.
///
/// When the prune expired everything the surviving log is empty and there is no
/// floor to derive; the index is never trimmed to empty, because an empty index
/// answers `TxId(0)` for every lookup and would hide the whole database. The
/// most recent entry is kept as the anchor instead.
fn retained_commit_index_after_prune(
    tx_mgr: &TxManager<DynStore>,
    surviving_change_log: &[ChangeLogEntry],
) -> BTreeMap<Lsn, TxId> {
    let index = tx_mgr.commit_index_snapshot();
    let surviving_lsns = surviving_change_log
        .iter()
        .map(|entry| entry.lsn())
        .collect::<BTreeSet<_>>();

    let Some(floor) = surviving_lsns.iter().next().copied() else {
        return index
            .iter()
            .next_back()
            .map(|(lsn, tx)| (*lsn, *tx))
            .into_iter()
            .collect();
    };

    let mut retained = index
        .iter()
        .filter(|(lsn, _)| surviving_lsns.contains(lsn))
        .map(|(lsn, tx)| (*lsn, *tx))
        .collect::<BTreeMap<_, _>>();
    if let Some((anchor_lsn, anchor_tx)) = index.range(..floor).next_back() {
        retained.insert(*anchor_lsn, *anchor_tx);
    }
    retained
}

/// Whether a change-log entry names one of the pruned `(table, row_id, lsn)`
/// versions, so it must be dropped in lockstep. Only row entries can reference a
/// relational version; edge/vector entries never do.
fn change_entry_references_pruned_version(
    entry: &ChangeLogEntry,
    pruned: &HashSet<(String, RowId, Lsn)>,
) -> bool {
    match entry {
        ChangeLogEntry::RowInsert {
            table, row_id, lsn, ..
        }
        | ChangeLogEntry::RowDelete {
            table, row_id, lsn, ..
        } => pruned.contains(&(table.clone(), *row_id, *lsn)),
        _ => false,
    }
}

/// Collapse each logical row of the compaction-eligible `tables` down to its
/// single current (max-LSN) version, dropping every superseded version and — in
/// lockstep — the change-log entries that referenced those versions.
///
/// Sync safety (the hard constraint): `changes_since`'s change-log replay reads
/// the row version AT each logged LSN (`row_for_change`), and on a missing
/// version silently substitutes the latest. Removing a version WITHOUT removing
/// its change-log entry would therefore corrupt replay. This function removes
/// them together, CHANGE-LOG-FIRST, so a crash between the two persisted writes
/// leaves at worst extra un-referenced versions (harmless — never replayed),
/// never an orphaned entry. Because the current version and its entry are always
/// kept, every `changes_since` replay — full edge pull (since=0), incremental
/// pull, or watermark-regressed re-push — still reconstructs the current row and
/// converges to current truth. These tables are all `LatestWins`, so the dropped
/// intermediate versions had no consumer value: a peer applies only the latest.
fn compact_currency_versions_inner(
    relational_store: &Arc<RelationalStore>,
    graph_store: &Arc<GraphStore>,
    vector_store: &Arc<VectorStore>,
    accountant: &MemoryAccountant,
    persistence: Option<&Arc<RedbPersistence>>,
    change_log: &Arc<RwLock<Vec<ChangeLogEntry>>>,
    tables: &[&str],
) -> Result<CurrencyCompactionReport> {
    let metas = relational_store.table_meta.read().clone();
    let table_snapshot = relational_store.tables.read().clone();

    let mut pruned_versions_by_table: HashMap<String, HashSet<(RowId, TxId)>> = HashMap::new();
    let mut pruned_change_keys: HashSet<(String, RowId, Lsn)> = HashSet::new();
    let mut released_row_bytes = 0usize;

    for table in tables {
        let table = *table;
        let Some(rows) = table_snapshot.get(table) else {
            continue;
        };
        // The keeper for each row_id is its greatest-LSN version — the current
        // one for a currency table (updates always advance the LSN).
        let mut keeper_lsn: HashMap<RowId, Lsn> = HashMap::new();
        for row in rows {
            keeper_lsn
                .entry(row.row_id)
                .and_modify(|current| {
                    if row.lsn > *current {
                        *current = row.lsn;
                    }
                })
                .or_insert(row.lsn);
        }
        for row in rows {
            if keeper_lsn.get(&row.row_id).copied() == Some(row.lsn) {
                continue;
            }
            pruned_versions_by_table
                .entry(table.to_string())
                .or_default()
                .insert((row.row_id, row.created_tx));
            pruned_change_keys.insert((table.to_string(), row.row_id, row.lsn));
            if let Some(meta) = metas.get(table) {
                released_row_bytes = released_row_bytes
                    .saturating_add(estimate_row_bytes_for_meta(&row.values, meta, false));
            }
        }
    }

    let pruned_version_count: u64 = pruned_versions_by_table
        .values()
        .map(|versions| versions.len() as u64)
        .sum();
    if pruned_version_count == 0 {
        return Ok(CurrencyCompactionReport::default());
    }

    let post_prune_table_rows: HashMap<String, Vec<VersionedRow>> = pruned_versions_by_table
        .iter()
        .map(|(table, versions)| {
            let rows = table_snapshot
                .get(table)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|row| !versions.contains(&(row.row_id, row.created_tx)))
                .collect::<Vec<_>>();
            (table.clone(), rows)
        })
        .collect();

    // Prune the in-memory change log first; keep the survivors to rewrite
    // persistence with, so memory and disk agree.
    let (surviving_change_log, pruned_change_log_entries) = {
        let mut log = change_log.write();
        let before = log.len();
        log.retain(|entry| !change_entry_references_pruned_version(entry, &pruned_change_keys));
        let removed = (before - log.len()) as u64;
        (log.clone(), removed)
    };

    if let Some(persistence) = persistence {
        // CHANGE-LOG-FIRST for crash-safety (see the doc comment above).
        persistence.rewrite_change_log(&surviving_change_log)?;
        // Only the compacted relational tables are rewritten; vectors and edges
        // are unchanged, but `rewrite_pruned_state` rewrites those wholesale, so
        // pass the full current sets through untouched.
        let all_vectors = vector_store.all_entries();
        let all_edges = graph_store
            .forward_adj
            .read()
            .values()
            .flat_map(|entries| entries.iter().cloned())
            .collect::<Vec<_>>();
        persistence.rewrite_pruned_state(&post_prune_table_rows, &all_vectors, &all_edges)?;
    }

    for (table, versions) in &pruned_versions_by_table {
        relational_store.remove_row_versions(table, versions);
        if let Some(meta) = metas.get(table) {
            for index in &meta.indexes {
                relational_store.rebuild_index(table, &index.name);
            }
        }
    }
    accountant.release(released_row_bytes);

    let mut compacted_tables: Vec<String> = pruned_versions_by_table.keys().cloned().collect();
    compacted_tables.sort();
    Ok(CurrencyCompactionReport {
        pruned_versions: pruned_version_count,
        pruned_change_log_entries,
        reclaimed_bytes: released_row_bytes as u64,
        compacted_tables,
        redb_compacted: false,
    })
}

/// Cheap pre-tick gate for the maintenance thread: are there at least
/// `threshold` superseded versions across the eligible `tables`? Reads only the
/// relational tables lock (never the commit lock) and early-returns, so a quiet
/// node does near-zero work per tick.
fn currency_compaction_pending(
    relational_store: &Arc<RelationalStore>,
    tables: &[&str],
    threshold: usize,
) -> bool {
    let snapshot = relational_store.tables.read();
    let mut superseded = 0usize;
    for table in tables {
        let Some(rows) = snapshot.get(*table) else {
            continue;
        };
        let distinct: HashSet<RowId> = rows.iter().map(|row| row.row_id).collect();
        superseded += rows.len().saturating_sub(distinct.len());
        if superseded >= threshold {
            return true;
        }
    }
    false
}

fn export_io_error(dest: &Path, err: &dyn std::fmt::Display) -> Error {
    Error::ExportIo {
        path: dest.to_path_buf(),
        reason: err.to_string(),
    }
}

/// Storage-layer snapshot filter: versions created after the watermark are
/// skipped entirely; a version live at the watermark with a post-snapshot
/// tombstone exports as live, so post-snapshot markers never leak.
fn export_row_at_snapshot(row: &VersionedRow, watermark: TxId) -> Option<VersionedRow> {
    if row.created_tx.0 > watermark.0 {
        return None;
    }
    let mut row = row.clone();
    if row.deleted_tx.is_some_and(|tx| tx.0 > watermark.0) {
        row.deleted_tx = None;
    }
    Some(row)
}

fn export_edge_at_snapshot(entry: &AdjEntry, watermark: TxId) -> Option<AdjEntry> {
    if entry.created_tx.0 > watermark.0 {
        return None;
    }
    let mut entry = entry.clone();
    if entry.deleted_tx.is_some_and(|tx| tx.0 > watermark.0) {
        entry.deleted_tx = None;
    }
    Some(entry)
}

fn export_vector_at_snapshot(entry: &VectorEntry, watermark: TxId) -> Option<VectorEntry> {
    if entry.created_tx.0 > watermark.0 {
        return None;
    }
    let mut entry = entry.clone();
    if entry.deleted_tx.is_some_and(|tx| tx.0 > watermark.0) {
        entry.deleted_tx = None;
    }
    Some(entry)
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
    relational_store: &RelationalStore,
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
            let arrived_by_sync = relational_store
                .sync_source_lsn(table_name, row.row_id)
                .is_some();
            if row_is_prunable(row, meta, now, sync_watermark, arrived_by_sync) {
                candidates.insert(RetentionRowKey::new(table_name, row.row_id, row.created_tx));
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
                let parent_key =
                    RetentionRowKey::new(parent_table, parent_row.row_id, parent_row.created_tx);
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
                    let child_key = RetentionRowKey::new(child_table, row.row_id, row.created_tx);
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
    arrived_by_sync: bool,
) -> bool {
    // `SYNC SAFE` is a delete-AFTER-DELIVERY promise, so it can only pin a row
    // that still owes a delivery — one this node WROTE and has not yet had
    // confirmed. A row that ARRIVED by sync has already been delivered, to
    // here; it owes nothing onward (a retained table is one-way, and this is
    // the receiving end), so gating it on a watermark that nothing will ever
    // advance would pin the hub's copy forever instead of retaining it.
    if meta.sync_safe && !arrived_by_sync && row.lsn >= sync_watermark {
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

/// Given a delete's threshold `lsn`, return the `deleted_tx` of the tombstone
/// with the greatest `lsn` strictly below the threshold, or `None` if there is
/// none. `versions` must be sorted ascending by `lsn` (stably, so equal-`lsn`
/// entries preserve their original store order). This reproduces the original
/// `filter(lsn < threshold).max_by_key(lsn).deleted_tx` semantics exactly,
/// including its tie-break: `max_by_key` returns the LAST of several equal
/// maxima, which is the highest-index (last-stored) equal-`lsn` entry here.
fn resolve_tombstone(versions: &[(Lsn, TxId)], threshold: Lsn) -> Option<TxId> {
    let boundary = versions.partition_point(|(lsn, _)| *lsn < threshold);
    if boundary == 0 {
        None
    } else {
        Some(versions[boundary - 1].1)
    }
}

/// Per-(table, row_id) tombstone lookup borrowed from the relational store.
type RowTombstoneIndex<'a> = HashMap<&'a str, HashMap<RowId, Vec<(Lsn, TxId)>>>;
/// Per-(source, edge_type, target) tombstone lookup borrowed from the graph store.
type EdgeTombstoneIndex<'a> = HashMap<(NodeId, &'a str, NodeId), Vec<(Lsn, TxId)>>;
/// Per-(index, row_id) tombstone lookup borrowed from the materialized vectors.
type VectorTombstoneIndex<'a> = HashMap<(&'a VectorIndexRef, RowId), Vec<(Lsn, TxId)>>;

/// Reconstructs the `Lsn -> TxId` commit index from the live stores plus the
/// change log. Runs on EVERY `Database::open`, so it must be near-linear: it
/// builds per-(table, row_id) / per-edge / per-(index, row_id) tombstone
/// lookups ONCE by borrowing (never cloning whole tables or adjacency lists),
/// then resolves each delete with a binary search — O(R + D log k) total, where
/// the original was O(deletes × rows). The produced index is byte-identical to
/// the original algorithm for any input (proven by
/// `commit_index_reconstruction_tests::commit_index_matches_naive_across_shapes`).
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

    // Relational: one borrowed pass records every created_tx and collects
    // tombstones keyed by (table, row_id). The read guard is held for the whole
    // function so the &str table keys stay valid through delete resolution.
    let relational_tables = relational.tables.read();
    let mut row_tombstones: RowTombstoneIndex = HashMap::new();
    for (table, rows) in relational_tables.iter() {
        for row in rows {
            add_entry(row.lsn, row.created_tx);
            if let Some(deleted_tx) = row.deleted_tx {
                row_tombstones
                    .entry(table.as_str())
                    .or_default()
                    .entry(row.row_id)
                    .or_default()
                    .push((row.lsn, deleted_tx));
            }
        }
    }
    for by_row in row_tombstones.values_mut() {
        for versions in by_row.values_mut() {
            versions.sort_by_key(|(lsn, _)| *lsn);
        }
    }

    // Graph: same borrowed pass, tombstones keyed by (source, edge_type, target).
    let forward_adj = graph.forward_adj.read();
    let mut edge_tombstones: EdgeTombstoneIndex = HashMap::new();
    for entries in forward_adj.values() {
        for edge in entries {
            add_entry(edge.lsn, edge.created_tx);
            if let Some(deleted_tx) = edge.deleted_tx {
                edge_tombstones
                    .entry((edge.source, edge.edge_type.as_str(), edge.target))
                    .or_default()
                    .push((edge.lsn, deleted_tx));
            }
        }
    }
    for versions in edge_tombstones.values_mut() {
        versions.sort_by_key(|(lsn, _)| *lsn);
    }

    // Vector: `all_entries` already materializes an owned Vec, so borrow it for
    // the created_tx pass and the (index, row_id) tombstone lookup.
    let vector_entries = vector.all_entries();
    let mut vector_tombstones: VectorTombstoneIndex = HashMap::new();
    for entry in &vector_entries {
        add_entry(entry.lsn, entry.created_tx);
        if let Some(deleted_tx) = entry.deleted_tx {
            vector_tombstones
                .entry((&entry.index, entry.row_id))
                .or_default()
                .push((entry.lsn, deleted_tx));
        }
    }
    for versions in vector_tombstones.values_mut() {
        versions.sort_by_key(|(lsn, _)| *lsn);
    }

    for entry in change_log {
        match entry {
            ChangeLogEntry::RowInsert { .. }
            | ChangeLogEntry::EdgeInsert { .. }
            | ChangeLogEntry::VectorInsert { .. } => {}
            ChangeLogEntry::RowDelete {
                table, row_id, lsn, ..
            } => {
                if let Some(deleted_tx) = row_tombstones
                    .get(table.as_str())
                    .and_then(|by_row| by_row.get(row_id))
                    .and_then(|versions| resolve_tombstone(versions, *lsn))
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
                if let Some(deleted_tx) = edge_tombstones
                    .get(&(*source, edge_type.as_str(), *target))
                    .and_then(|versions| resolve_tombstone(versions, *lsn))
                {
                    add_entry(*lsn, deleted_tx);
                }
            }
            ChangeLogEntry::VectorDelete {
                index: vector_index,
                row_id,
                lsn,
            } => {
                if let Some(deleted_tx) = vector_tombstones
                    .get(&(vector_index, *row_id))
                    .and_then(|versions| resolve_tombstone(versions, *lsn))
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
    relational.rebuild_row_position_maps();
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

fn is_sync_access_scope_error(err: &Error) -> bool {
    matches!(
        err,
        Error::ContextScopeViolation { .. }
            | Error::ScopeLabelViolation { .. }
            | Error::AclDenied { .. }
            | Error::PrincipalRequired { .. }
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

pub(crate) fn sql_type_for_meta_column(
    col: &contextdb_core::ColumnDef,
    rules: &[PropagationRule],
) -> String {
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
        if sync_constraint_is_vector_rename(constraint) {
            continue;
        }
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

fn alter_table_ddl_change(
    name: &str,
    meta: &TableMeta,
    extra_constraints: Vec<String>,
) -> DdlChange {
    let mut constraints = create_table_constraints_from_meta(meta);
    constraints.extend(extra_constraints);
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
        constraints,
        foreign_keys: single_column_foreign_keys_from_meta(meta, &HashSet::new()),
        composite_foreign_keys: meta.composite_foreign_keys.clone(),
        composite_unique: meta.unique_constraints.clone(),
    }
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

fn sync_constraint_is_vector_rename(constraint: &str) -> bool {
    constraint
        .trim()
        .to_ascii_uppercase()
        .starts_with("VECTOR_RENAME(")
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

fn sync_vector_rename_constraint(from: &str, to: &str) -> String {
    format!("VECTOR_RENAME({from},{to})")
}

fn sync_vector_rename_from_constraints(constraints: &[String]) -> Option<(String, String)> {
    const PREFIX: &str = "VECTOR_RENAME(";
    for constraint in constraints {
        let trimmed = constraint.trim();
        let upper = trimmed.to_ascii_uppercase();
        if !upper.starts_with(PREFIX) || !trimmed.ends_with(')') {
            continue;
        }
        let body = &trimmed[PREFIX.len()..trimmed.len().saturating_sub(1)];
        let (from, to) = body.split_once(',')?;
        let from = from
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_string();
        let to = to
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_string();
        if !from.is_empty() && !to.is_empty() {
            return Some((from, to));
        }
    }
    None
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

fn merge_sync_alter_existing_column(current: &mut ColumnDef, incoming: ColumnDef) {
    if incoming.primary_key {
        current.primary_key = true;
        current.nullable = false;
    }
    if incoming.unique {
        current.unique = true;
    }
    if !incoming.nullable {
        current.nullable = false;
    }
    if incoming.expires {
        current.expires = true;
    }
    if incoming.immutable {
        current.immutable = true;
    }
    if incoming.context_id {
        current.context_id = true;
    }
    if incoming.references.is_some() {
        current.references = incoming.references;
    }
    if incoming.rank_policy.is_some() {
        current.rank_policy = incoming.rank_policy;
    }
}

/// Whether a synced DDL constraint spells the refused two-way posture on a
/// retained table. Matched on the rendered constraint text because that is the
/// shape table DDL travels in; the local paths refuse the same spelling at the
/// parsed-statement level.
fn constraint_declares_two_way_retention(constraint: &str) -> bool {
    let upper = constraint.to_ascii_uppercase();
    let normalized = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    normalized.starts_with("RETAIN ") && normalized.contains("TWO WAY")
}

/// Whether a synced DDL constraint declares the `SYNC SAFE` delivery promise.
fn constraint_declares_sync_safe(constraint: &str) -> bool {
    let upper = constraint.to_ascii_uppercase();
    let normalized = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    normalized.starts_with("RETAIN ") && normalized.contains("SYNC SAFE")
}

/// Refuse arriving DDL that declares `SYNC SAFE` on a table with no key to
/// deliver rows by, on BOTH the CreateTable and AlterTable paths. Without this
/// a foreign edge could install locally the exact declaration every local door
/// rejects, and this engine would then delete undelivered rows on its behalf.
fn refuse_keyless_sync_safe_sync_ddl(
    projected: &HashMap<String, TableMeta>,
    change: &DdlChange,
) -> Result<()> {
    let (name, meta) = match change {
        DdlChange::CreateTable {
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        } => {
            if !constraints.iter().any(|c| constraint_declares_sync_safe(c)) {
                return Ok(());
            }
            (
                name,
                rough_sync_table_meta(
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                ),
            )
        }
        DdlChange::AlterTable {
            name, constraints, ..
        } => {
            if !constraints.iter().any(|c| constraint_declares_sync_safe(c)) {
                return Ok(());
            }
            match projected.get(name) {
                Some(meta) => (name, meta.clone()),
                None => return Ok(()),
            }
        }
        _ => return Ok(()),
    };
    // The arriving declaration promises delivery, so apply the same key
    // requirement the local CREATE and ALTER paths apply.
    let mut meta = meta;
    meta.sync_safe = true;
    crate::executor::refuse_sync_safe_without_key_for(name, &meta)
}

/// Refuse arriving table DDL that declares a two-way retained table, on BOTH
/// the CreateTable and AlterTable arrival paths. Returns before any projection
/// or apply work, so a refused statement leaves existing metadata untouched.
fn refuse_two_way_retained_sync_ddl(change: &DdlChange) -> Result<()> {
    let (name, constraints) = match change {
        DdlChange::CreateTable {
            name, constraints, ..
        }
        | DdlChange::AlterTable {
            name, constraints, ..
        } => (name, constraints),
        _ => return Ok(()),
    };
    if constraints
        .iter()
        .any(|constraint| constraint_declares_two_way_retention(constraint))
    {
        return Err(Error::SchemaInvalid {
            reason: format!(
                "table '{name}' arrived declaring a retained table as TWO WAY; a RETAIN table is \
                 delivered one way (edge -> hub), so the declaration is refused rather than \
                 silently converted"
            ),
        });
    }
    Ok(())
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
        retained_sync_policy: None,
        retain_declared_unit: None,
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

    let rank_policy = sync_rank_policy_from_column_type(name, ty);

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
        rank_policy,
        context_id: upper.contains("CONTEXT_ID"),
        scope_label: None,
        acl_ref: None,
    }
}

fn sync_rank_policy_from_column_type(name: &str, ty: &str) -> Option<RankPolicy> {
    if !normalize_schema_type(ty)
        .to_ascii_uppercase()
        .contains("RANK_POLICY")
    {
        return None;
    }
    let sql = format!("CREATE TABLE __sync_rank_policy ({name} {ty})");
    let Ok(Statement::CreateTable(table)) = contextdb_parser::parse(&sql) else {
        return None;
    };
    table.columns.into_iter().next().and_then(|column| {
        column
            .rank_policy
            .map(|policy| crate::executor::map_rank_policy(&policy))
    })
}

fn resolve_sync_rank_policies(table_name: &str, meta: &mut TableMeta) {
    let joined_meta = meta.clone();
    let anchor_columns = joined_meta.columns.clone();
    for column in &mut meta.columns {
        let Some(policy) = column.rank_policy.as_mut() else {
            continue;
        };
        if policy.joined_table != table_name {
            continue;
        }
        if let Some(protected_index) =
            sync_rank_policy_protected_index(&joined_meta, &policy.joined_column)
        {
            policy.protected_index = protected_index;
        }
        if let Some(anchor_column) =
            resolve_sync_rank_policy_anchor_column(policy, &anchor_columns, &joined_meta)
        {
            policy.anchor_column = anchor_column;
        }
    }
}

fn sync_rank_policy_protected_index(meta: &TableMeta, joined_column: &str) -> Option<String> {
    meta.indexes
        .iter()
        .filter(|index| index.kind == IndexKind::UserDeclared)
        .chain(meta.indexes.iter())
        .find(|index| {
            index
                .columns
                .first()
                .is_some_and(|(column, _)| column == joined_column)
        })
        .map(|index| index.name.clone())
}

fn resolve_sync_rank_policy_anchor_column(
    policy: &RankPolicy,
    anchor_columns: &[ColumnDef],
    joined_meta: &TableMeta,
) -> Option<String> {
    let joined_column = joined_meta
        .columns
        .iter()
        .find(|column| column.name == policy.joined_column)?;
    let anchor_by_name = |name: &str| anchor_columns.iter().find(|column| column.name == name);
    let mut candidates = Vec::new();
    if joined_column.primary_key {
        let singular = sync_singular_table_name(&policy.joined_table);
        for name in [
            format!("{singular}_id"),
            format!("{}_id", policy.joined_table),
        ] {
            if anchor_by_name(&name).is_some() && !candidates.contains(&name) {
                candidates.push(name);
            }
        }
    }
    if candidates.is_empty() && anchor_by_name(&policy.joined_column).is_some() {
        candidates.push(policy.joined_column.clone());
    }
    if candidates.is_empty()
        && let Some(primary_key) = anchor_columns.iter().find(|column| column.primary_key)
    {
        candidates.push(primary_key.name.clone());
    }
    if candidates.is_empty() && anchor_by_name("id").is_some() {
        candidates.push("id".to_string());
    }
    let [anchor_column] = candidates.as_slice() else {
        return None;
    };
    let anchor_def = anchor_by_name(anchor_column)?;
    (anchor_def.column_type == joined_column.column_type).then(|| anchor_column.clone())
}

fn sync_singular_table_name(table: &str) -> String {
    if let Some(stem) = table.strip_suffix("ies") {
        format!("{stem}y")
    } else if let Some(stem) = table.strip_suffix('s') {
        stem.to_string()
    } else {
        table.to_string()
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
        constraints.push(retain_clause_from_meta(meta, ttl_seconds, false));
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

/// Render the table-level `RETAIN` and edge / vector-exclusion `PROPAGATE`
/// clauses for a `TableMeta`, deterministically ordered for stable output.
/// Foreign-key propagation is rendered on its owning column (see
/// `sql_type_for_meta_column`), so `PropagationRule::ForeignKey` is skipped
/// here — matching the sync DDL emitter's split.
/// The declared retention clause, policy included. The one-way direction is
/// rendered explicitly so an operator reading `.schema` — or an edge receiving
/// this DDL over sync — sees the contract the table actually carries.
fn retain_clause_from_meta(meta: &TableMeta, ttl_seconds: u64, as_declared: bool) -> String {
    let window = match (as_declared, meta.retain_declared_unit) {
        (true, Some(unit)) if unit.seconds_multiplier() != 0 => {
            format!("{} {}", ttl_seconds / unit.seconds_multiplier(), unit.sql())
        }
        _ => ttl_seconds_to_sql(ttl_seconds),
    };
    let mut clause = format!("RETAIN {window}");
    if meta.sync_safe {
        clause.push_str(" SYNC SAFE");
        if meta.retained_sync_policy == Some(RetainedSyncPolicy::PushOnly) {
            clause.push_str(" PUSH ONLY");
        }
    }
    clause
}

pub(crate) fn retain_and_propagate_clauses_from_meta(meta: &TableMeta) -> Vec<String> {
    let mut clauses = Vec::new();

    if let Some(ttl_seconds) = meta.default_ttl_seconds {
        clauses.push(retain_clause_from_meta(meta, ttl_seconds, true));
    }

    let mut propagate = Vec::new();
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
                    "PROPAGATE ON EDGE {edge_type} {dir} STATE {trigger_state} SET {target_state}"
                );
                if *max_depth != 10 {
                    clause.push_str(&format!(" MAX DEPTH {max_depth}"));
                }
                if *abort_on_failure {
                    clause.push_str(" ABORT ON FAILURE");
                }
                propagate.push(clause);
            }
            PropagationRule::VectorExclusion { trigger_state } => {
                propagate.push(format!("PROPAGATE ON STATE {trigger_state} EXCLUDE VECTOR"));
            }
            PropagationRule::ForeignKey { .. } => {}
        }
    }
    propagate.sort();
    clauses.extend(propagate);

    clauses
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

fn has_graph_edge_table_shape(meta: &TableMeta) -> bool {
    [
        ("source_id", ColumnType::Uuid),
        ("target_id", ColumnType::Uuid),
        ("edge_type", ColumnType::Text),
    ]
    .into_iter()
    .all(|(name, column_type)| has_exact_column_type(meta, name, &column_type))
}

fn has_exact_column_type(meta: &TableMeta, name: &str, column_type: &ColumnType) -> bool {
    let mut columns = meta.columns.iter().filter(|column| column.name == name);
    matches!(columns.next(), Some(column) if &column.column_type == column_type)
        && columns.next().is_none()
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

    /// Returns the DB plus the mock-clock guard, which the caller must hold:
    /// the expiry read (`Wallclock::now()` inside `run_pruning_cycle*`) runs
    /// on the test thread later in the test body, and dropping the guard
    /// early would hand it the real clock again.
    fn db_with_expired_row() -> (Database, contextdb_core::WallclockTestClockGuard) {
        let mock_now = Arc::new(AtomicU64::new(1_000_000));
        let clock = {
            let mock_now = Arc::clone(&mock_now);
            Wallclock::test_clock_guard(move || mock_now.load(Ordering::SeqCst))
        };
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
        // Advance 2 s past the row's stamped time — beyond the 1 s TTL. Both
        // the insert stamp and the expiry read happen on this thread, so the
        // mock governs both; no real sleep needed.
        mock_now.fetch_add(2_000, Ordering::SeqCst);
        (db, clock)
    }

    #[test]
    fn checked_prune_reports_persistence_failure_without_mutating_memory() {
        let (db, _clock) = db_with_expired_row();

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
        let (db, _clock) = db_with_expired_row();

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

#[cfg(test)]
mod commit_index_reconstruction_tests {
    //! Guards for `commit_index_across_all`, the change-log → `BTreeMap<Lsn, TxId>`
    //! reconstruction that `Database::open` runs unconditionally on every open.
    //!
    //! Two contracts are covered:
    //!   * `commit_index_matches_naive_across_shapes` — the OPTIMIZED implementation
    //!     must produce a byte-identical index to the original O(deletes × rows)
    //!     algorithm (frozen here as `naive_commit_index`) for every representative
    //!     change-log shape, including delete-of-nonexistent-row and
    //!     delete-after-multiple-inserts-of-the-same-id.
    //!   * `commit_index_reconstruction_is_not_quadratic` — the reconstruction must
    //!     complete in near-linear time. This is the regression that took down the
    //!     owner's node: the original code linearly rescanned every row in a table
    //!     for EACH `RowDelete` entry, so the work ledger's per-input retention
    //!     deletes made `open` O(deletes × rows) and pinned the boot thread at 100%
    //!     CPU for minutes until the Supervisor watchdog killed the add-on.
    use super::*;

    fn row(row_id: RowId, created_tx: u64, deleted_tx: Option<u64>, lsn: Lsn) -> VersionedRow {
        VersionedRow {
            row_id,
            values: HashMap::new(),
            created_tx: TxId(created_tx),
            deleted_tx: deleted_tx.map(TxId),
            lsn,
            created_at: None,
        }
    }

    fn edge(
        source: NodeId,
        target: NodeId,
        created_tx: u64,
        deleted_tx: Option<u64>,
        lsn: Lsn,
    ) -> AdjEntry {
        AdjEntry {
            source,
            target,
            edge_type: "REL".to_string(),
            properties: HashMap::new(),
            created_tx: TxId(created_tx),
            deleted_tx: deleted_tx.map(TxId),
            lsn,
        }
    }

    fn relational_with(tables: Vec<(&str, Vec<VersionedRow>)>) -> RelationalStore {
        let store = RelationalStore::new();
        {
            let mut guard = store.tables.write();
            for (name, rows) in tables {
                guard.insert(name.to_string(), rows);
            }
        }
        store
    }

    fn graph_with(edges: Vec<AdjEntry>) -> GraphStore {
        let store = GraphStore::new();
        {
            let mut fwd = store.forward_adj.write();
            for e in edges {
                fwd.entry(e.source).or_default().push(e);
            }
        }
        store
    }

    fn empty_vector() -> VectorStore {
        VectorStore::default()
    }

    fn row_delete(table: &str, row_id: RowId, lsn: Lsn) -> ChangeLogEntry {
        ChangeLogEntry::RowDelete {
            table: table.to_string(),
            row_id,
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Null,
            },
            lsn,
        }
    }

    /// The ORIGINAL O(deletes × rows) reconstruction, frozen verbatim so the
    /// optimized production function can be proven output-identical. Any drift
    /// here would defeat the equivalence guard, so it must not be "improved."
    fn naive_commit_index(
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
                                    row.row_id == *row_id
                                        && row.lsn < *lsn
                                        && row.deleted_tx.is_some()
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

    #[test]
    fn commit_index_matches_naive_across_shapes() {
        let n1 = uuid::Uuid::from_u128(1);
        let n2 = uuid::Uuid::from_u128(2);
        let n3 = uuid::Uuid::from_u128(3);

        // Each case: (relational tables, edges, change_log). Every shape the
        // reconstruction must handle identically to the frozen naive algorithm.
        type Case<'a> = (
            Vec<(&'a str, Vec<VersionedRow>)>,
            Vec<AdjEntry>,
            Vec<ChangeLogEntry>,
        );
        let cases: Vec<Case> = vec![
            // Empty store, no change log.
            (vec![], vec![], vec![]),
            // Inserts only — created_tx entries, no deletes.
            (
                vec![(
                    "t",
                    vec![
                        row(RowId(1), 5, None, Lsn(10)),
                        row(RowId(2), 6, None, Lsn(12)),
                    ],
                )],
                vec![],
                vec![],
            ),
            // Delete of a nonexistent row (no matching tombstone) — must add nothing.
            (
                vec![("t", vec![row(RowId(1), 5, None, Lsn(10))])],
                vec![],
                vec![row_delete("t", RowId(999), Lsn(50))],
            ),
            // Delete against a nonexistent table — must add nothing.
            (
                vec![("t", vec![row(RowId(1), 5, None, Lsn(10))])],
                vec![],
                vec![row_delete("other", RowId(1), Lsn(50))],
            ),
            // Single tombstone resolved by its delete entry.
            (
                vec![("t", vec![row(RowId(1), 5, Some(9), Lsn(10))])],
                vec![],
                vec![row_delete("t", RowId(1), Lsn(20))],
            ),
            // Delete-after-multiple-inserts-of-the-same-id: several tombstoned
            // versions of row_id 1; the delete at lsn 40 must pick the max-lsn
            // tombstone with lsn < 40 (the one at lsn 30 → deleted_tx 31), not the
            // later lsn-45 tombstone, and not the earlier lsn-15 one.
            (
                vec![(
                    "t",
                    vec![
                        row(RowId(1), 3, Some(4), Lsn(15)),
                        row(RowId(1), 20, Some(21), Lsn(30)),
                        row(RowId(1), 44, Some(46), Lsn(45)),
                    ],
                )],
                vec![],
                vec![row_delete("t", RowId(1), Lsn(40))],
            ),
            // Two delete entries against the same id at different thresholds pick
            // different tombstones (proves the resolution is per-entry, not global).
            (
                vec![(
                    "t",
                    vec![
                        row(RowId(7), 3, Some(4), Lsn(15)),
                        row(RowId(7), 20, Some(21), Lsn(30)),
                    ],
                )],
                vec![],
                vec![
                    row_delete("t", RowId(7), Lsn(20)),
                    row_delete("t", RowId(7), Lsn(40)),
                ],
            ),
            // Edges: inserts + a resolvable edge delete + a nonexistent edge delete.
            (
                vec![],
                vec![
                    edge(n1, n2, 5, Some(8), Lsn(10)),
                    edge(n1, n3, 6, None, Lsn(12)),
                ],
                vec![
                    ChangeLogEntry::EdgeDelete {
                        source: n1,
                        target: n2,
                        edge_type: "REL".to_string(),
                        lsn: Lsn(30),
                    },
                    ChangeLogEntry::EdgeDelete {
                        source: n2,
                        target: n3,
                        edge_type: "REL".to_string(),
                        lsn: Lsn(31),
                    },
                ],
            ),
            // Mixed: rows across two tables + edges + interleaved deletes, with an
            // lsn collision (two entries at the same lsn must resolve to max TxId).
            (
                vec![
                    (
                        "a",
                        vec![
                            row(RowId(1), 5, Some(50), Lsn(10)),
                            row(RowId(2), 7, None, Lsn(14)),
                        ],
                    ),
                    ("b", vec![row(RowId(1), 6, Some(9), Lsn(11))]),
                ],
                vec![edge(n1, n2, 8, Some(60), Lsn(13))],
                vec![
                    row_delete("a", RowId(1), Lsn(100)),
                    row_delete("b", RowId(1), Lsn(100)),
                    ChangeLogEntry::EdgeDelete {
                        source: n1,
                        target: n2,
                        edge_type: "REL".to_string(),
                        lsn: Lsn(100),
                    },
                ],
            ),
            // Lsn(0) tombstone: a version at lsn 0 (skipped by add_entry) that a
            // later RowDelete still resolves against. Exercises the tombstone
            // map's Lsn(0) boundary in partition_point / the naive max_by_key.
            (
                vec![(
                    "t",
                    vec![
                        row(RowId(1), 5, Some(9), Lsn(0)),
                        row(RowId(1), 10, None, Lsn(12)),
                    ],
                )],
                vec![],
                vec![row_delete("t", RowId(1), Lsn(8))],
            ),
        ];

        for (idx, (tables, edges, change_log)) in cases.into_iter().enumerate() {
            let relational = relational_with(tables);
            let graph = graph_with(edges);
            let vector = empty_vector();
            let expected = naive_commit_index(&relational, &graph, &vector, &change_log);
            let actual = commit_index_across_all(&relational, &graph, &vector, &change_log);
            assert_eq!(
                actual, expected,
                "case {idx}: optimized commit index must match the frozen naive reconstruction"
            );
        }
    }

    #[test]
    fn commit_index_matches_naive_for_vector_deletes() {
        use contextdb_core::VectorQuantization;
        // Build a vector store with two tombstoned versions of one (index,row_id)
        // plus a live one, so a VectorDelete must resolve to the max-lsn tombstone
        // below its threshold — exactly as the naive scan does.
        let index = VectorIndexRef::new("t", "vec");
        let vector = VectorStore::default();
        vector.register_index(index.clone(), 2, VectorQuantization::F32);
        let entry = |created_tx: u64, deleted_tx: Option<u64>, lsn: Lsn| VectorEntry {
            index: index.clone(),
            row_id: RowId(1),
            vector: vec![0.0, 0.0],
            created_tx: TxId(created_tx),
            deleted_tx: deleted_tx.map(TxId),
            lsn,
        };
        vector.insert_loaded_vector(entry(3, Some(4), Lsn(15)));
        vector.insert_loaded_vector(entry(20, Some(21), Lsn(30)));
        vector.insert_loaded_vector(entry(44, None, Lsn(45)));

        let relational = relational_with(vec![]);
        let graph = graph_with(vec![]);
        let change_log = vec![
            // Resolves against the lsn-30 tombstone (max lsn < 40).
            ChangeLogEntry::VectorDelete {
                index: index.clone(),
                row_id: RowId(1),
                lsn: Lsn(40),
            },
            // Delete of a nonexistent (index,row_id) — must add nothing.
            ChangeLogEntry::VectorDelete {
                index: VectorIndexRef::new("t", "other"),
                row_id: RowId(1),
                lsn: Lsn(40),
            },
        ];

        let expected = naive_commit_index(&relational, &graph, &vector, &change_log);
        let actual = commit_index_across_all(&relational, &graph, &vector, &change_log);
        assert_eq!(
            actual, expected,
            "optimized vector-delete resolution must match the frozen naive reconstruction"
        );
    }

    #[test]
    fn commit_index_reconstruction_is_not_quadratic() {
        // Reproduces the live incident shape: one table with N tombstoned rows and
        // N RowDelete change-log entries (the work ledger's per-input retention
        // deletes). The original algorithm rescans all N rows for EACH delete →
        // ~N² row visits. Here N = 60_000, so the original does ~3.6e9 row
        // comparisons; an O(R + D) reconstruction does ~60_000 map builds plus
        // 60_000 lookups. The gap is > 1000×, so a generous 10s wall-clock bound
        // cleanly separates them — this is NOT a 2× timing race. The original code
        // takes tens of seconds here (and pinned a real node's CPU for minutes on a
        // 349MB ledger); any near-linear implementation finishes in well under a
        // second.
        const N: u64 = 60_000;

        let mut rows = Vec::with_capacity(N as usize);
        let mut change_log = Vec::with_capacity(N as usize);
        for i in 0..N {
            // Row i: tombstoned at lsn 2i+2, deleted by tx N+i+1.
            rows.push(row(RowId(i), i + 1, Some(N + i + 1), Lsn(2 * i + 2)));
            // Delete entry i: threshold lsn 2i+3 (> the row's lsn), so each entry
            // resolves against its row and exercises the full scan+max path.
            change_log.push(row_delete("t", RowId(i), Lsn(2 * i + 3)));
        }

        let relational = relational_with(vec![("t", rows)]);
        let graph = graph_with(vec![]);
        let vector = empty_vector();

        let start = std::time::Instant::now();
        let index = commit_index_across_all(&relational, &graph, &vector, &change_log);
        let elapsed = start.elapsed();

        // Sanity: every delete resolved (created_tx entries + delete entries).
        assert_eq!(
            index.len(),
            2 * N as usize,
            "each row contributes a created_tx entry and each delete a distinct-lsn entry"
        );
        assert!(
            elapsed < Duration::from_secs(10),
            "commit index reconstruction of {N} rows + {N} deletes took {elapsed:?}; \
             the O(deletes × rows) original cannot meet this bound while any O(R+D) \
             implementation finishes in milliseconds"
        );
    }
}

#[cfg(test)]
mod currency_version_compaction_tests {
    //! Guards for currency-table version compaction (`compact_currency_versions`),
    //! the fix for unbounded MVCC version accumulation in the fabric currency
    //! tables (a 365MB debris ledger held 248,996 `work_capabilities` versions
    //! for ~10 live rows).
    //!
    //!  * `compaction_bounds_versions_and_change_log` — after churning one logical
    //!    capability row N times, compaction collapses it to a single physical
    //!    version AND drops the N-1 change-log entries in lockstep. Without the
    //!    fix, both stay at N.
    //!  * `changes_since_converges_to_current_truth_after_compaction` — the hard
    //!    sync constraint: a full edge pull (`changes_since(0)`), a
    //!    watermark-regressed re-push (`changes_since(mid)`), and a prune during
    //!    ongoing churn each still yield exactly the current row with the current
    //!    value — no orphaned change-log entry replays a stale/substituted
    //!    version, and nothing is lost.
    use super::*;
    use tempfile::TempDir;

    fn tags() -> Vec<String> {
        vec!["class:example.detector".to_string()]
    }

    fn physical_versions(db: &Database, table: &str) -> usize {
        db.relational_store
            .tables
            .read()
            .get(table)
            .map(|rows| rows.len())
            .unwrap_or(0)
    }

    fn change_log_row_inserts(db: &Database, table: &str) -> usize {
        db.change_log
            .read()
            .iter()
            .filter(
                |entry| matches!(entry, ChangeLogEntry::RowInsert { table: t, .. } if t == table),
            )
            .count()
    }

    fn current_advertised_at(db: &Database) -> i64 {
        let result = db
            .execute(
                "SELECT advertised_at FROM work_capabilities",
                &HashMap::new(),
            )
            .expect("select advertised_at");
        assert_eq!(result.rows.len(), 1, "exactly one live capability row");
        match &result.rows[0][0] {
            Value::Timestamp(ms) => *ms,
            other => panic!("advertised_at should be a Timestamp, got {other:?}"),
        }
    }

    /// changes_since row values for `work_capabilities` only.
    fn capability_changes(db: &Database, since: Lsn) -> Vec<i64> {
        db.changes_since(since)
            .rows
            .into_iter()
            .filter(|change| change.table == "work_capabilities")
            .map(|change| match change.values.get("advertised_at") {
                Some(Value::Timestamp(ms)) => *ms,
                other => panic!("advertised_at should be a Timestamp, got {other:?}"),
            })
            .collect()
    }

    fn open_ledger() -> (TempDir, Database) {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("fabric-ledger.db");
        let db = Database::open(&path).expect("open ledger");
        crate::work_ledger::install_work_ledger_schema(&db).expect("install work ledger schema");
        (tmp, db)
    }

    #[test]
    fn compaction_bounds_versions_and_change_log() {
        let (_tmp, db) = open_ledger();
        // Hold the auto-maintenance tick (60s in production) out of this test: under machine
        // load the 200-upsert setup can outlast one tick, and a mid-setup auto-compaction
        // destroys the accumulated-versions precondition this test exists to compact manually.
        // The auto-heal path is proven by installing_the_ledger_starts_maintenance_and_it_auto_heals.
        db.__set_currency_maintenance_interval(Duration::from_secs(3600));
        const N: i64 = 200;
        for advertised_at in 1..=N {
            crate::work_ledger::advertise_capability(
                &db,
                "node-a",
                "example-detector-cpu",
                &tags(),
                advertised_at,
            )
            .expect("advertise");
        }

        // The defect: one logical row, N physical versions, N change-log entries.
        assert_eq!(physical_versions(&db, "work_capabilities"), N as usize);
        assert_eq!(change_log_row_inserts(&db, "work_capabilities"), N as usize);

        let report = db.compact_currency_versions().expect("compact");
        assert_eq!(report.pruned_versions, (N - 1) as u64);
        // Each ON-CONFLICT update writes both a RowInsert (new version) and a
        // RowDelete (old version), so at least the N-1 superseded inserts plus
        // their deletes are dropped in lockstep.
        assert!(
            report.pruned_change_log_entries >= (N - 1) as u64,
            "at least the superseded versions' change-log entries must be dropped in \
             lockstep; got {}",
            report.pruned_change_log_entries
        );

        // Bounded: one live version, one surviving RowInsert, tiny total log.
        assert_eq!(
            physical_versions(&db, "work_capabilities"),
            1,
            "compaction must collapse a logical row to its single current version"
        );
        assert_eq!(
            change_log_row_inserts(&db, "work_capabilities"),
            1,
            "the superseded versions' change-log entries must be dropped in lockstep"
        );
        assert!(
            db.change_log.read().len() <= 2,
            "only the keeper commit's entries survive; got {}",
            db.change_log.read().len()
        );
        // Correctness preserved: the surviving row is the CURRENT one.
        assert_eq!(current_advertised_at(&db), N);

        // Idempotent: nothing left to compact.
        let again = db.compact_currency_versions().expect("compact again");
        assert_eq!(again.pruned_versions, 0);
    }

    #[test]
    fn changes_since_survives_reopen_after_compaction() {
        // The rewritten change log must round-trip through persistence: after a
        // compaction, a reopened ledger must reconstruct the same current truth
        // (proves rewrite_change_log wrote valid keys/ordering and commit-index
        // reconstruction still agrees).
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("fabric-ledger.db");
        {
            let db = Database::open(&path).expect("open");
            crate::work_ledger::install_work_ledger_schema(&db).expect("install");
            // Pin the background tick out of the way: the version buildup below
            // is the precondition, and an auto-compaction racing it would turn
            // this test's verdict into a scheduling artifact.
            db.__set_currency_maintenance_interval(Duration::from_secs(3600));
            for advertised_at in 1..=80 {
                crate::work_ledger::advertise_capability(
                    &db,
                    "node-a",
                    "example-detector-cpu",
                    &tags(),
                    advertised_at,
                )
                .expect("advertise");
            }
            db.compact_currency_versions().expect("compact");
        }
        // Reopen from disk.
        let db = Database::open(&path).expect("reopen");
        assert_eq!(physical_versions(&db, "work_capabilities"), 1);
        assert_eq!(current_advertised_at(&db), 80);
        assert_eq!(
            capability_changes(&db, Lsn(0)),
            vec![80],
            "a full pull after reopen must still converge to current truth"
        );
    }

    #[test]
    fn changes_since_converges_to_current_truth_after_compaction() {
        let (_tmp, db) = open_ledger();
        // Pin the background tick out of the way. Without this, an
        // auto-compaction landing between the buildup and the
        // `changes_since(...).rows[N/2]` read collapses the row vector and
        // index-panics — the canonical scheduling-dependent verdict flip.
        db.__set_currency_maintenance_interval(Duration::from_secs(3600));
        const N: i64 = 120;
        for advertised_at in 1..=N {
            crate::work_ledger::advertise_capability(
                &db,
                "node-a",
                "example-detector-cpu",
                &tags(),
                advertised_at,
            )
            .expect("advertise");
        }
        // Record a mid-range LSN for the watermark-regressed re-push case.
        let mid_lsn = db.changes_since(Lsn(0)).rows[N as usize / 2].lsn;

        db.compact_currency_versions().expect("compact");

        // Full edge pull (since = 0): exactly the current row, current value.
        let full = capability_changes(&db, Lsn(0));
        assert_eq!(
            full,
            vec![N],
            "a wiped-edge full pull after a prune must transfer exactly the current \
             capability version and converge to current truth — no orphaned change-log \
             entry replaying a stale/substituted version"
        );

        // Watermark-regressed re-push (since = mid): still converges, still bounded.
        let regressed = capability_changes(&db, mid_lsn);
        assert!(
            regressed.iter().all(|value| *value == N),
            "a re-push replayed from a regressed watermark must only surface the current \
             value, never a substituted one; got {regressed:?}"
        );
        assert!(
            regressed.len() <= 1,
            "re-push transfer must be bounded to the live row count, got {}",
            regressed.len()
        );

        // Prune during ongoing traffic loses nothing: churn more, compact, pull.
        for advertised_at in (N + 1)..=(N + 40) {
            crate::work_ledger::advertise_capability(
                &db,
                "node-a",
                "example-detector-cpu",
                &tags(),
                advertised_at,
            )
            .expect("advertise");
        }
        db.compact_currency_versions().expect("compact again");
        assert_eq!(
            capability_changes(&db, Lsn(0)),
            vec![N + 40],
            "after a second prune over new traffic the pull still converges to the latest"
        );
    }

    /// Acceptance against a real large debris ledger. Ignored by default
    /// (needs the corpus file); run with the path in `DEBRIS_LEDGER_CORPUS`:
    ///   DEBRIS_LEDGER_CORPUS=/path/to/debris-ledger.db \
    ///     cargo test -p contextdb-engine --lib debris_corpus_acceptance -- --ignored --nocapture
    /// Copies the corpus to a temp file first (never mutates the original).
    #[test]
    #[ignore = "requires the DEBRIS_LEDGER_CORPUS debris file"]
    fn debris_corpus_acceptance_shrinks_debris_ledger() {
        let Ok(src) = std::env::var("DEBRIS_LEDGER_CORPUS") else {
            panic!("set DEBRIS_LEDGER_CORPUS to a copy-able debris ledger path");
        };
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("fabric-ledger.db");
        std::fs::copy(&src, &path).expect("copy corpus ledger");

        let bytes_before = std::fs::metadata(&path).unwrap().len();
        let db = Database::open(&path).expect("open corpus ledger");
        let versions_before = physical_versions(&db, "work_capabilities");
        println!(
            "debris_corpus before: file={} MB work_capabilities_versions={}",
            bytes_before / 1_048_576,
            versions_before
        );

        let report = db.compact_currency_versions().expect("compact corpus");
        let versions_after = physical_versions(&db, "work_capabilities");
        drop(db);
        let bytes_after = std::fs::metadata(&path).unwrap().len();
        println!(
            "debris_corpus after: file={} MB work_capabilities_versions={} pruned_versions={} \
             pruned_change_log_entries={} redb_compacted={}",
            bytes_after / 1_048_576,
            versions_after,
            report.pruned_versions,
            report.pruned_change_log_entries,
            report.redb_compacted
        );

        assert!(
            versions_after <= 64,
            "version count must be bounded to the live-row order of magnitude; got {versions_after}"
        );
        assert!(
            report.redb_compacted,
            "a debris ledger this fragmented must trigger a redb compaction"
        );
        assert!(
            bytes_after < 16 * 1_048_576,
            "the compacted debris ledger must drop to a few MB; got {} MB",
            bytes_after / 1_048_576
        );
    }

    #[test]
    fn installing_the_ledger_starts_maintenance_and_it_auto_heals() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("fabric-ledger.db");
        let db = Database::open(&path).expect("open");
        // Fresh DB with no eligible tables yet: no thread.
        assert!(
            !db.__maintenance_thread_running(),
            "a database with no currency tables must not spawn a maintenance thread"
        );

        crate::work_ledger::install_work_ledger_schema(&db).expect("install");
        // Installing the capability registry starts the engine-owned thread.
        assert!(
            db.__maintenance_thread_running(),
            "installing the currency schema must start the maintenance thread in-session"
        );

        for advertised_at in 1..=200 {
            crate::work_ledger::advertise_capability(
                &db,
                "node-a",
                "example-detector-cpu",
                &tags(),
                advertised_at,
            )
            .expect("advertise");
        }
        assert_eq!(physical_versions(&db, "work_capabilities"), 200);

        // Speed the cadence up and let the thread heal the accumulated versions.
        db.__set_currency_maintenance_interval(Duration::from_millis(40));
        // State-polled wait with sleep pacing: poll the thread's wake counter
        // and the healed version count under a generous failure ceiling (this
        // test's promise IS that a real background thread does real work, so
        // a ceiling remains by necessity — but success is decided by observed
        // STATE, never by how long we happened to sleep).
        let baseline_wakes = db.__maintenance_wakes();
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(10) {
            if db.__maintenance_wakes() > baseline_wakes
                && physical_versions(&db, "work_capabilities") == 1
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            physical_versions(&db, "work_capabilities"),
            1,
            "the background maintenance thread must auto-compact accumulated versions"
        );
        assert_eq!(current_advertised_at(&db), 200);
    }

    #[test]
    fn non_eligible_database_never_spawns_a_maintenance_thread() {
        // A database without currency tables must get ZERO new threads,
        // on fresh open, after unrelated schema, and on reopen.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("plain.db");
        {
            let db = Database::open(&path).expect("open");
            assert!(
                !db.__maintenance_thread_running(),
                "fresh non-eligible open"
            );
            db.execute(
                "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
                &HashMap::new(),
            )
            .expect("create");
            db.execute(
                "INSERT INTO notes (id, body) VALUES (1, 'hi')",
                &HashMap::new(),
            )
            .expect("insert");
            assert!(
                !db.__maintenance_thread_running(),
                "an unrelated schema must not start a maintenance thread"
            );
        }
        // Reopen: still no eligible tables, still no thread.
        let db = Database::open(&path).expect("reopen");
        assert!(
            !db.__maintenance_thread_running(),
            "reopening a non-eligible database must not start a maintenance thread"
        );
    }

    #[test]
    fn close_joins_the_maintenance_thread() {
        // Shutdown-aware: an eligible database's maintenance thread must join
        // on close (no zombie thread). Event/state recast: `close()` joins
        // synchronously, so its RETURN is the completion event and the
        // thread-gone state is the assertion; a genuinely hung join hangs the
        // test and is caught by the harness/mutants timeout (the failure
        // ceiling). The old `elapsed < 2s` wall-clock bound asserted only
        // promptness and flipped verdicts under load — the join cadence is
        // structurally bounded by sleep_with_shutdown's 50ms poll regardless.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("fabric-ledger.db");
        let db = Database::open(&path).expect("open");
        crate::work_ledger::install_work_ledger_schema(&db).expect("install");
        assert!(db.__maintenance_thread_running());
        db.close().expect("close");
        assert!(
            !db.__maintenance_thread_running(),
            "close must join and clear the maintenance thread"
        );
    }
}
