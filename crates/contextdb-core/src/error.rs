use crate::types::{ContextId, Principal, RowId, ScopeLabel, TxId, Value, VectorIndexRef};
use std::borrow::Cow;
use std::collections::BTreeSet;

/// Distinguishes which callback context produced a callback-active error.
///
/// Contract: final `Display` for callback-active errors must be alloc-free
/// (inline `&'static str` templates, never `kind.to_string()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CallbackKind {
    Trigger,
    Cron,
}

impl std::fmt::Display for CallbackKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CallbackKind::Trigger => "trigger",
            CallbackKind::Cron => "cron",
        })
    }
}

struct ContextSetDisplay<'a>(&'a BTreeSet<ContextId>);
impl std::fmt::Display for ContextSetDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("{")?;
        for (index, context) in self.0.iter().enumerate() {
            if index != 0 { f.write_str(", ")?; }
            write!(f, "{}", context.0)?;
        }
        f.write_str("}")
    }
}

struct ScopeLabelSetDisplay<'a>(&'a BTreeSet<ScopeLabel>);
impl std::fmt::Display for ScopeLabelSetDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("{")?;
        for (index, label) in self.0.iter().enumerate() {
            if index != 0 { f.write_str(", ")?; }
            f.write_str(label.0.as_str())?;
        }
        f.write_str("}")
    }
}

struct PrincipalDisplay<'a>(&'a Principal);
impl std::fmt::Display for PrincipalDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Principal::System => f.write_str("system"),
            Principal::Agent(id) => write!(f, "agent:{id}"),
            Principal::Human(id) => write!(f, "human:{id}"),
        }
    }
}

struct ContextScopeViolationDisplay<'a> {
    requested: &'a ContextId,
    allowed: &'a BTreeSet<ContextId>,
}
impl std::fmt::Display for ContextScopeViolationDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("row hidden by context scope: requested ")?;
        if self.requested.0 == uuid::Uuid::from_u128(u128::MAX) {
            f.write_str("row has no context")?;
        } else {
            write!(f, "context {}", self.requested.0)?;
        }
        write!(f, ", allowed {}", ContextSetDisplay(self.allowed))
    }
}
fn context_scope_violation_display<'a>(
    requested: &'a ContextId, allowed: &'a BTreeSet<ContextId>,
) -> ContextScopeViolationDisplay<'a> {
    ContextScopeViolationDisplay { requested, allowed }
}

struct ScopeLabelViolationDisplay<'a> {
    requested: &'a ScopeLabel,
    allowed: &'a BTreeSet<ScopeLabel>,
}
impl std::fmt::Display for ScopeLabelViolationDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "row hidden by scope label: requested {}, allowed {}",
            self.requested.0, ScopeLabelSetDisplay(self.allowed))
    }
}
fn scope_label_violation_display<'a>(
    requested: &'a ScopeLabel, allowed: &'a BTreeSet<ScopeLabel>,
) -> ScopeLabelViolationDisplay<'a> {
    ScopeLabelViolationDisplay { requested, allowed }
}

struct AclDeniedDisplay<'a> {
    table: &'a str,
    row_id: &'a RowId,
    principal: &'a Principal,
}
impl std::fmt::Display for AclDeniedDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "row hidden by ACL on table `{}` row {} for principal {}",
            self.table, self.row_id, PrincipalDisplay(self.principal))
    }
}
fn acl_denied_display<'a>(
    table: &'a str, row_id: &'a RowId, principal: &'a Principal,
) -> AclDeniedDisplay<'a> {
    AclDeniedDisplay { table, row_id, principal }
}

/// Safe structural reasons a vector partition declaration can be refused.
///
/// These cases deliberately carry no column value or partition-key value.
/// The enclosing error identifies only the vector index; callers can render
/// the fixed reason without accidentally echoing data from a bound parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum VectorPartitionDeclarationIssue {
    #[error("vector partition clauses require a VECTOR column")]
    RequiresVectorColumn,
    #[error("PARTITION_KEY must contain at least one column")]
    EmptyPartitionKey,
    #[error("PARTITION_KEY references a column that does not exist in the same table")]
    UnknownPartitionKeyColumn,
    #[error("every PARTITION_KEY column must be NOT NULL")]
    NullablePartitionKeyColumn,
    #[error("PARTITION_KEY columns must use UUID, TEXT, INTEGER, BOOLEAN, TIMESTAMP, or TXID")]
    UnsupportedPartitionKeyColumnType,
    #[error("a PARTITION_KEY column may appear only once")]
    DuplicatePartitionKeyColumn,
    #[error("PARTITION_KEY may not contain the vector column itself")]
    VectorColumnInPartitionKey,
    #[error("MAX_PARTITIONS requires PARTITION_KEY")]
    MaxPartitionsWithoutPartitionKey,
    #[error("MAX_PARTITIONS must be a positive integer")]
    MaxPartitionsNotPositive,
    #[error("MAX_PARTITIONS is outside the persisted numeric range")]
    MaxPartitionsOutOfRange,
}

/// Safe structural reasons a per-column vector workload policy can be
/// refused. The enclosing error identifies the column; values are omitted so
/// bound or synced input is never echoed through the diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum VectorPolicyDeclarationIssue {
    #[error("vector policy clauses require a VECTOR column")]
    RequiresVectorColumn,
    #[error("AUTO_INDEX_AT must be a positive integer")]
    AutoIndexAtNotPositive,
    #[error("AUTO_INDEX_AT is outside the persisted numeric range")]
    AutoIndexAtOutOfRange,
    #[error("FLAT_BELOW must be a non-negative integer")]
    FlatBelowNegative,
    #[error("FLAT_BELOW is outside the persisted numeric range")]
    FlatBelowOutOfRange,
    #[error("AUTO_EXPANSION_CEILING must be an integer percent from 1 through 100")]
    AutoExpansionCeilingOutOfRange,
    #[error(
        "HNSW SEARCH_RADIUS must be a decimal from 0 through 2 with at most six decimal places"
    )]
    SearchRadiusOutOfRange,
    #[error("HNSW policy values must be positive integers")]
    HnswValueNotPositive,
    #[error("an HNSW policy value is outside the persisted numeric range")]
    HnswValueOutOfRange,
    #[error("HNSW EF_CONSTRUCTION must be at least M")]
    EfConstructionBelowM,
    #[error("vector consolidation percentages must be integers from 1 through 100")]
    ConsolidationPercentOutOfRange,
}

/// Why a `SET STORAGE_COMPACTION` declaration was refused. The SQL statement
/// and the Rust setter refuse the same declarations with the same issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StorageCompactionDeclarationIssue {
    #[error("START_RATIO must be greater than 0 and at most 1")]
    StartRatioOutOfRange,
    #[error("STOP_RATIO must be greater than 0 and at most 1")]
    StopRatioOutOfRange,
    #[error("MAX_SWEEPS must be at least 1")]
    MaxSweepsNotPositive,
    #[error("MAX_SWEEPS is outside the persisted numeric range")]
    MaxSweepsOutOfRange,
    #[error("MIN_INTERVAL must be positive")]
    MinIntervalNotPositive,
    #[error("MIN_INTERVAL is longer than 9223372036854775807 milliseconds")]
    MinIntervalOutOfRange,
    #[error("a clause may appear only once in one declaration")]
    DuplicateClause,
}

/// The engine's own answers.
///
/// This vocabulary is the engine's, and it is free to grow and be reordered:
/// no wire carries it. A caller reading over the local owner channel is still
/// entitled to the same answer an in-process call would have produced, but
/// that entitlement is met by the read channel's own error document, which
/// names each class a read can return under a tag written down in its own
/// source. Adding, reordering, or reshaping a variant here cannot move a byte
/// a reader parses.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Native cancellation identities are never reused. This is a local,
    /// typed construction refusal rather than a channel or wire condition.
    #[doc(hidden)]
    #[error("native cancellation identity space exhausted")]
    NativeCancellationIdentityExhausted,
    #[error("bounded read session is not implemented")]
    ReadSessionNotImplemented,
    #[error("owner reads did not drain before shutdown deadline")]
    OwnerReadDrainTimeout,
    #[error(
        "event-bus queue write refused after owner quiescence; queued event remains available after reopen"
    )]
    EventBusQueueQuiescent,
    #[error("read operation was cancelled")]
    ReadCancelled,
    /// A writable open asked to bound its wait for starting readers by a
    /// cancellation or a deadline, and this process cannot give it a wait it
    /// can end early. An open that asks for neither still waits.
    #[error("a cancellable or bounded wait for starting readers is not available in this process")]
    ReaderWaitUnsupported,
    /// The helper a writable open's cancellable wait started ended without
    /// handing over the store; the open is refused and holds nothing.
    #[error("the wait for starting readers failed: {0}")]
    ReaderWaitHelperFailed(String),
    /// A writable open's acquisition helper could not be confirmed dead after
    /// its wait was ended. This process keeps the store held, so nothing the
    /// helper might still be granted can reach anyone else; a later open or
    /// close of the store re-checks, and process exit releases it.
    #[error(
        "the store at {path} is held by this process until an ended reader wait is confirmed; restart the process if it persists"
    )]
    ReaderWaitQuarantined { path: String },
    #[error("{0}")]
    ReadFailure(crate::read_contract::ReadFailure),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("{0} are immutable")]
    ImmutableTable(String),
    #[error("column `{column}` on table `{table}` is immutable")]
    ImmutableColumn { table: String, column: String },
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(String),
    #[error("conditional update conflict: {count} staged update(s) no longer matched at commit")]
    ConditionalUpdateConflict { count: u64 },
    #[error("propagation aborted: {table}.{column} transition {from} -> {to} is invalid")]
    PropagationAborted {
        table: String,
        column: String,
        from: String,
        to: String,
    },
    #[error("BFS depth exceeds maximum allowed ({0})")]
    BfsDepthExceeded(u32),
    #[error("BFS visited set exceeded limit ({0})")]
    BfsVisitedExceeded(usize),
    #[error("vector index dimension mismatch on {index}: expected {expected}, got {actual}")]
    VectorIndexDimensionMismatch {
        index: VectorIndexRef,
        expected: usize,
        actual: usize,
    },
    #[error(
        "vector source identity mismatch on {index} row {row_id}: expected created {expected_created_tx}/LSN {expected_lsn}, got created {actual_created_tx:?}/LSN {actual_lsn:?}"
    )]
    VectorSourceIdentityMismatch {
        index: VectorIndexRef,
        row_id: RowId,
        expected_created_tx: TxId,
        expected_lsn: crate::types::Lsn,
        actual_created_tx: Option<TxId>,
        actual_lsn: Option<crate::types::Lsn>,
    },
    #[error("unknown vector index {index}")]
    UnknownVectorIndex { index: VectorIndexRef },
    #[error("invalid vector partition declaration on {index}: {issue}")]
    InvalidVectorPartitionDeclaration {
        index: VectorIndexRef,
        issue: VectorPartitionDeclarationIssue,
    },
    #[error("invalid vector policy declaration on {index}: {issue}")]
    InvalidVectorPolicyDeclaration {
        index: VectorIndexRef,
        issue: VectorPolicyDeclarationIssue,
    },
    #[error("invalid storage compaction declaration: {issue}")]
    InvalidStorageCompactionDeclaration {
        issue: StorageCompactionDeclarationIssue,
    },
    /// A write's PARTITION_KEY value is not of the type its column declares.
    ///
    /// The declaration is sound; the VALUE the caller wrote is not, so this
    /// is deliberately not an `InvalidVectorPartitionDeclaration`, which
    /// would send a developer to inspect a `CREATE TABLE` that has nothing
    /// wrong with it. Only the key column is named: the offending value is
    /// bound or synced input and is never echoed through the diagnostic.
    #[error(
        "vector partition key value refused on {index}: the value written to PARTITION_KEY column `{column}` is not of the column's declared type"
    )]
    VectorPartitionKeyValueTypeMismatch {
        index: VectorIndexRef,
        column: String,
    },
    #[error(
        "vector partition limit reached on {index}: MAX_PARTITIONS is {max_partitions}; raise MAX_PARTITIONS and retry"
    )]
    VectorPartitionLimitExceeded {
        index: VectorIndexRef,
        max_partitions: u32,
    },
    #[error(
        "EXACT vector search on {index} requires {required_bytes} bytes but the active budget has {available_bytes} bytes available; narrow the authorized scope or raise its active limit"
    )]
    VectorExactSearchBudgetExceeded {
        index: VectorIndexRef,
        required_bytes: u64,
        available_bytes: u64,
    },
    #[error(
        "INDEXED vector search on {index} has no complete maintained route; wait for or repair index maintenance"
    )]
    VectorIndexedRouteUnavailable { index: VectorIndexRef },
    #[error(
        "INDEXED vector search on {index} refused: partition {partition} (in scope order) holds live rows its maintained graph does not connect (graph_frontier_disconnected); repair index maintenance, or use AUTO or EXACT"
    )]
    VectorIndexedPartitionDisconnected {
        index: VectorIndexRef,
        partition: u64,
    },
    #[error(
        "filtered vector search on {index} has no bounded relational candidate route for columns {predicate_columns:?}; add a supporting relational index or use EXACT"
    )]
    VectorFilteredRouteUnavailable {
        index: VectorIndexRef,
        predicate_columns: Vec<String>,
    },
    #[error("whole-vector-index inspection requires an unrestricted database handle")]
    VectorWholeIndexInspectionDenied,
    #[error("persisted row vector source row missing on {index}: key {key}")]
    PersistedRowVectorRowMissing { index: VectorIndexRef, key: String },
    #[error("persisted row vector source cell is NULL on {index}: key {key}")]
    PersistedRowVectorCellNull { index: VectorIndexRef, key: String },
    #[error("rank policy on index `{index}` references unknown column `{column}`")]
    RankPolicyColumnUnknown { index: String, column: String },
    #[error(
        "rank policy on index `{index}` references ambiguous column `{column}` (present on both anchor and joined table); rename one of the columns -- the rank-formula grammar does not support table-qualified column references"
    )]
    RankPolicyColumnAmbiguous { index: String, column: String },
    #[error(
        "rank policy on index `{index}` references column `{column}` of type {actual}; expected {expected}"
    )]
    RankPolicyColumnType {
        index: String,
        column: String,
        expected: String,
        actual: String,
    },
    #[error("rank policy on index `{index}` joins unknown table `{table}`")]
    RankPolicyJoinTableUnknown { index: String, table: String },
    #[error(
        "rank policy on index `{index}` joins column `{column}` not present on table `{table}`"
    )]
    RankPolicyJoinColumnUnknown {
        index: String,
        table: String,
        column: String,
    },
    #[error(
        "rank policy on index `{index}` joins column `{joined_table}.{column}` which has no index; add: CREATE INDEX {joined_table}_{column}_idx ON {joined_table}({column});"
    )]
    RankPolicyJoinColumnUnindexed {
        index: String,
        joined_table: String,
        column: String,
    },
    #[error("rank policy sort key `{sort_key}` not found on index `{index}`")]
    RankPolicyNotFound { index: String, sort_key: String },
    #[error(
        "rank policy formula on index `{index}` failed to parse at position {position}: {reason}"
    )]
    RankPolicyFormulaParse {
        index: String,
        position: usize,
        reason: String,
    },
    #[error("USE RANK requires ORDER BY embedding <=> $param in the same query")]
    UseRankRequiresVectorOrder,
    #[error("USE VECTOR requires ORDER BY embedding <=> $param in the same query")]
    UseVectorRequiresVectorOrder,
    #[error("USE RANK requires LIMIT in the same query")]
    UseRankRequiresLimit,
    #[error(
        "{}",
        drop_blocked_rank_policy_display(
            table,
            column,
            dropped_index,
            policy_table,
            policy_column,
            sort_key
        )
    )]
    DropBlockedByRankPolicy {
        table: Box<str>,
        column: Option<Box<str>>,
        dropped_index: Option<Box<str>>,
        policy_table: Box<str>,
        policy_column: Box<str>,
        sort_key: Box<str>,
    },
    #[error(
        "This store was written by an earlier build and is not read. Move it aside and create the store again."
    )]
    LegacyVectorStoreDetected {
        found_format_marker: String,
        expected_release: String,
    },
    #[error("corrupt store at {path}: {reason}")]
    StoreCorrupted { path: String, reason: String },
    #[error("vector metadata I/O failed during {operation}: {reason}")]
    VectorMetadataIo { operation: String, reason: String },
    #[error(
        "no store at {path}: this open was asked to change a store that already exists, so it created nothing — point it at an existing store, or open it with the creating disposition if a new one is wanted"
    )]
    StoreMissing { path: String },
    #[error(
        "the store's identity could not be proven, so its fingerprint identifies nothing: more than one name resolves to the file at {path}"
    )]
    StoreIdentityUnprovable { path: String },
    #[error("{operation} cannot safely coordinate readers through {path}: {reason}")]
    ReaderCoordinationUnavailable {
        operation: String,
        path: String,
        reason: String,
    },
    #[error(
        "{operation} cannot take a holder-presence lock through {path}: {reason}; this filesystem cannot prove whether a background-work holder is alive"
    )]
    HolderPresenceUnavailable {
        operation: String,
        path: String,
        reason: String,
    },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("transaction not found: {0}")]
    TxNotFound(TxId),
    #[error("unique constraint violation: {table}.{column}")]
    UniqueViolation { table: String, column: String },
    #[error(
        "{}",
        foreign_key_violation_display(child_table, child_columns, parent_table, parent_columns)
    )]
    ForeignKeyViolation {
        child_table: String,
        child_columns: Vec<String>,
        parent_table: String,
        parent_columns: Vec<String>,
    },
    #[error("recursive CTEs are not supported")]
    RecursiveCteNotSupported,
    #[error("window form is not supported: {form}")]
    WindowFunctionNotSupported { form: String },
    #[error("stored procedures/functions are not supported")]
    StoredProcNotSupported,
    #[error("graph traversal requires explicit depth bound")]
    UnboundedTraversal,
    #[error("vector search requires LIMIT clause")]
    UnboundedVectorSearch,
    #[error("subqueries not supported; use CTE chaining")]
    SubqueryNotSupported,
    #[error("full-text search (WHERE column MATCH) is not supported")]
    FullTextSearchNotSupported,
    #[error(
        "ORDER BY requires a column reference, a SELECT-list alias naming one, COALESCE(...), or arithmetic (+, -, *, /); this sort key expression is not supported"
    )]
    OrderByExpressionNotSupported,
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("plan error: {0}")]
    PlanError(String),
    #[error("sync error: {0}")]
    SyncError(Cow<'static, str>),
    #[error("tenant table policy is not declared for {table}")]
    TenantPolicyNotDeclared { table: String },
    #[error("tenant table policy for {table} differs at {clause}")]
    TenantPolicyMismatch { table: String, clause: String },
    #[error("tenant table policy for {table} is already bound")]
    TenantPolicyBound { table: String },
    #[error("table {table} differs from its authenticated binding at {clause}")]
    TableBindingMismatch { table: String, clause: String },
    /// An authenticated authored transaction restates a table this node
    /// declared for itself, with a policy that differs from the declaration.
    /// The declaration cannot change from here and the transaction cannot be
    /// installed in part, so the whole transaction is refused; only its
    /// author can correct it.
    #[error(
        "the explicit local policy for '{table}' conflicts with the arriving declaration at \
         {clause}: local policy {local_policy}; arriving policy {arriving_policy}; the local \
         declaration is immutable once bound, so the whole authored transaction is refused and \
         must be corrected by its author, not retried"
    )]
    DeclaredPolicyPreserved {
        table: String,
        clause: String,
        local_policy: String,
        arriving_policy: String,
    },
    /// A later unit touches a table that an earlier authored transaction in
    /// the same stream was refused for, so it waits behind that refusal
    /// instead of installing out of the author's order or failing the rest
    /// of the stream.
    #[error("table '{table}' is held back behind a refused authored transaction: {refusal}")]
    TableHeldBehindRefusedUnit { table: String, refusal: String },
    #[error("delivery manifest required for root table {table}")]
    ManifestRequired { table: String },
    #[error("delivery manifest is incomplete for root table {table}")]
    ManifestIncomplete { table: String },
    #[error("delivery manifest member from {table} was not written in this transaction")]
    ManifestMemberOutsideTransaction { table: String },
    #[error("tenant table policy declarations must originate at authoritative hub {hub_node_id}")]
    DeclareRequiresAuthoritativeHub { hub_node_id: String },
    #[error("DISCARD is not eligible for {table} with sync direction {direction}")]
    DiscardNotEligible { table: String, direction: String },
    #[error("DISCARD is not permitted on a hub; use PURGE")]
    DiscardNotOnHub,
    #[error(
        "edge discard denied by hub {hub_node_id} for {table} in mode {mode:?}: {pending_count} pending unit(s)"
    )]
    EdgeDiscardDenied {
        hub_node_id: String,
        table: String,
        mode: crate::EdgeDiscardMode,
        pending_count: u64,
    },
    #[error("PURGE must originate at authoritative hub {hub_node_id}; run PURGE there")]
    PurgeRequiresAuthoritativeHub { hub_node_id: String },
    /// A push re-offered a row the hub already knows was deleted -- the SAME
    /// terminated lineage, not a fresh write. The store already agrees (both
    /// sides converged on the delete); refusing the replay is correct, but
    /// it is NOT a definitive push failure the way a constraint violation or
    /// an unreachable endpoint is. A caller driving an unconditional
    /// exit-time push (the CLI's final-flush-on-quit) should treat this as
    /// benign convergence -- exit clean, optional notice -- never as a hard
    /// failure. See [`Error::SyncPushUnconfirmed`] for the sibling
    /// "indeterminate, not failed" contract this mirrors.
    #[error(
        "push replays a lineage the hub already terminated by an accepted delete: {table} key {key:?} (store already agrees; no action needed)"
    )]
    SyncReplayOfAcceptedDelete {
        table: String,
        key: Vec<(String, Value)>,
    },
    /// A peer was asked to serve schema it received from another author, but
    /// it no longer holds that author's exact signed declarations for `table`.
    /// Received schema only ever leaves a peer as its author wrote it -- the
    /// author's statements, order and signatures -- so the peer refuses
    /// rather than restating the migration under its own name.
    #[error("received schema for table {table} cannot be served as its author wrote it: {reason}")]
    ReceivedSchemaAuthorshipUnavailable { table: String, reason: String },
    #[error("PURGE must run as a standalone authoritative statement")]
    PurgeRequiresStandaloneExecution,
    #[error(
        "purge causality fence rejects {table} key {key:?}: lineage root {lineage_root} was permanently purged at frontier {frontier}"
    )]
    PurgeCausalityFence {
        table: String,
        key: Vec<(String, Value)>,
        lineage_root: String,
        frontier: crate::types::Lsn,
    },
    #[error(
        "export snapshot fence rejects {table} key {key:?}: lineage root {lineage_root} was permanently purged at frontier {frontier} after snapshot {snapshot_lsn}"
    )]
    PurgeExportSnapshotFence {
        table: String,
        key: Vec<(String, Value)>,
        lineage_root: String,
        frontier: crate::types::Lsn,
        snapshot_lsn: crate::types::Lsn,
    },
    #[error("table {0} is not sync-eligible (no natural key)")]
    NotSyncEligible(String),
    #[error(
        "cycle detected: inserting {edge_type} edge from {source_node} to {target_node} would create a cycle"
    )]
    CycleDetected {
        edge_type: String,
        source_node: uuid::Uuid,
        target_node: uuid::Uuid,
    },
    #[error("plugin rejected at {hook}: {reason}")]
    PluginRejected { hook: String, reason: String },
    #[error(
        "memory budget exceeded: {subsystem}/{operation} requested {requested_bytes} bytes, {available_bytes} available of {budget_limit_bytes} budget. Hint: {hint}"
    )]
    MemoryBudgetExceeded {
        subsystem: Cow<'static, str>,
        operation: Cow<'static, str>,
        requested_bytes: usize,
        available_bytes: usize,
        budget_limit_bytes: usize,
        hint: Cow<'static, str>,
    },
    #[error(
        "disk budget exceeded: {operation} - file is {current_bytes} bytes, limit is {budget_limit_bytes} bytes. Hint: {hint}"
    )]
    DiskBudgetExceeded {
        operation: String,
        current_bytes: u64,
        budget_limit_bytes: u64,
        hint: String,
    },
    /// Scratch working storage could not complete `create`, `read`, or `write`.
    /// This is an I/O failure, not a refusal of the store's configured disk
    /// budget. Retain the kind without exposing a scratch path or error source.
    #[error("scratch working storage unavailable during {operation}: {io_kind}")]
    ScratchWorkingStorageUnavailable {
        operation: String,
        io_kind: std::io::ErrorKind,
    },
    #[error("column type mismatch: {table}.{column} expected {expected}, got {actual}")]
    ColumnTypeMismatch {
        table: String,
        column: String,
        expected: String,
        actual: String,
    },
    #[error("tx id out of range: {table}.{column} value {value} exceeds max {max}")]
    TxIdOutOfRange {
        table: String,
        column: String,
        value: u64,
        max: u64,
    },
    #[error("tx id overflow: {table} incoming {incoming}")]
    TxIdOverflow { table: String, incoming: u64 },
    #[error("column {table}.{column} is NOT NULL")]
    ColumnNotNullable { table: String, column: String },
    #[error("index not found: {table}.{index}")]
    IndexNotFound { table: String, index: String },
    #[error("duplicate index: {table}.{index}")]
    DuplicateIndex { table: String, index: String },
    #[error("reserved index name: {table}.{name} uses reserved prefix `{prefix}`")]
    ReservedIndexName {
        table: String,
        name: String,
        prefix: String,
    },
    #[error("column not indexable: {table}.{column} has type {column_type:?}")]
    ColumnNotIndexable {
        table: String,
        column: String,
        column_type: crate::table_meta::ColumnType,
    },
    #[error("column in index: {table}.{column} referenced by index {index}")]
    ColumnInIndex {
        table: String,
        column: String,
        index: String,
    },

    #[error(
        "cannot alter {table}.{column}: it is a partition key for vector index {index}; create a replacement table with the desired vector partition key and copy the wanted rows"
    )]
    VectorPartitionKeyInUse {
        table: String,
        column: String,
        index: String,
    },
    #[error("column not found: {table}.{column}")]
    ColumnNotFound { table: String, column: String },
    #[error("database is locked: another process (pid {holder_pid}) holds {path:?}")]
    DatabaseLocked {
        holder_pid: u32,
        path: std::path::PathBuf,
    },
    #[error("{}", context_scope_violation_display(requested, allowed))]
    ContextScopeViolation {
        requested: crate::types::ContextId,
        allowed: std::collections::BTreeSet<crate::types::ContextId>,
    },
    #[error("{}", scope_label_violation_display(requested, allowed))]
    ScopeLabelViolation {
        requested: crate::types::ScopeLabel,
        allowed: std::collections::BTreeSet<crate::types::ScopeLabel>,
    },
    #[error("principal required for read on table `{table}`")]
    PrincipalRequired { table: String },
    #[error("{}", acl_denied_display(table, row_id, principal))]
    AclDenied {
        table: String,
        row_id: RowId,
        principal: Principal,
    },
    #[error("missed cron ticks exceeded policy: {ticks} ticks missed under '{policy}'")]
    MissedTicksExceeded { ticks: u32, policy: String },
    #[error("database is not initialized: trigger callbacks must be registered before {operation}")]
    EngineNotInitialized { operation: String },
    #[error("trigger callback missing: {trigger_name}")]
    TriggerCallbackMissing { trigger_name: String },
    #[error("trigger not declared: {trigger_name}")]
    TriggerNotDeclared { trigger_name: String },
    #[error("trigger callback already registered: {trigger_name}")]
    TriggerAlreadyRegistered { trigger_name: String },
    #[error("trigger cascade depth exceeded for {trigger_name}: depth {depth}")]
    TriggerCascadeDepthExceeded { trigger_name: String, depth: u32 },
    #[error("trigger event is not supported: {event}")]
    TriggerEventUnsupported { event: String },
    #[error("trigger callback failed for {trigger_name}: {reason}")]
    TriggerCallbackFailed {
        trigger_name: String,
        reason: String,
    },
    #[error(
        "sync trigger `{trigger_name}` attempted forbidden effect `{effect}`; INCLUDING SYNC callbacks may only insert relational rows"
    )]
    SyncTriggerEffectNotAllowed {
        trigger_name: String,
        effect: String,
    },
    #[error("trigger operation requires an admin database handle: {operation}")]
    TriggerRequiresAdmin { operation: String },
    #[error("schema invalid: {reason}")]
    SchemaInvalid { reason: String },
    /// Cross-thread retry-safe contention. Post-TriggerActiveSameDBProgress,
    /// ordinary same-DB cross-thread trigger contention waits inside the engine
    /// and proceeds as `Ok`; unrelated cross-DB writers proceed independently.
    /// This typed error remains for cron same-DB contention, callback tx-bound
    /// handles used from the wrong thread, and bounded trigger deadlock-guard
    /// timeouts.
    ///
    /// Canonical callback contract: same-DB trigger Class B waits and
    /// proceeds; unrelated DBs are independent; Class A callback-thread reentry
    /// returns [`Error::CallbackReentry`]; cron same-DB Class B returns this
    /// typed error immediately; if a same-DB trigger wait exceeds the engine's
    /// deadlock-guard deadline of 60 seconds -- a fixed deadline no
    /// environment name can move -- the engine returns this typed error and
    /// emits exactly one `tracing::warn!` with `trigger_name`, `waited_ms`,
    /// and `surface`.
    ///
    /// Greppable cross-process substring (sync-wire, log scrapers): the Display
    /// string contains `"callback active on another thread"` for both kinds.
    // MAINTAINER: there are no em-dashes in the CallbackActiveCrossThread template,
    // but see the CallbackReentry template below -- the em-dash character `—` is
    // U+2014 (NOT ASCII hyphen `-`). The public-API string contract pins exact
    // bytes (cross-process consumers may grep for these strings); preserve every
    // em-dash on any future edit and never substitute `--` or `-`.
    #[error("{kind} callback active on another thread; retry once the callback completes")]
    CallbackActiveCrossThread { kind: CallbackKind },

    /// API misuse. The caller is inside its own callback body. Tx-control from
    /// any `Database` handle is forbidden, and write-control is allowed only
    /// through the supplied tx-bound callback handle. Retrying cannot recover.
    /// See [`Error::CallbackActiveCrossThread`] for the complementary Class B
    /// contract: same-DB trigger contention waits, unrelated DBs proceed
    /// independently, cron B1 remains immediate, and timeouts emit an operator
    /// `tracing::warn!`.
    ///
    /// Greppable substring: the Display string contains `"operation not allowed inside"`
    /// for both kinds.
    // MAINTAINER: the em-dash character `—` below is U+2014 (NOT ASCII hyphen `-`).
    // Public-API string contract pins exact bytes -- preserve the em-dash on any
    // future edit and never substitute `--` or `-`.
    #[error("operation not allowed inside a {kind} callback (API misuse — fix the call site)")]
    CallbackReentry { kind: CallbackKind },
    #[error("export destination already exists: {path:?}")]
    ExportDestinationExists { path: std::path::PathBuf },
    #[error("export I/O failure at {path:?}: {reason}")]
    ExportIo {
        path: std::path::PathBuf,
        reason: String,
    },
    #[error("work ledger: input of job {job_id} is a blob_ref; route it to the blob resolver")]
    InputRequiresBlobResolver { job_id: String },
    /// A job's ledger-carried input copies are gone: they aged out under
    /// `work_inputs`' declared retention window before the job was executed.
    /// Distinct from a job with no ledger inputs at all -- that job's input
    /// reference names a different kind. A worker reaching this must see a
    /// typed refusal and record a failure; it must never execute on
    /// silently-empty input.
    #[error("work ledger: input of job {job_id} expired under work_inputs' retention window")]
    WorkInputExpired { job_id: String },
    /// A push was interrupted after its batch left the edge, and the hub could
    /// not be reached to confirm whether the batch landed. The outcome is
    /// INDETERMINATE — the data may or may not have committed on the hub — so
    /// this is distinct from a definitive [`Error::SyncError`] failure. The
    /// push watermark is left unadvanced; a later push reconciles idempotently.
    #[error("sync push unconfirmed: {detail}")]
    SyncPushUnconfirmed { detail: String },
    /// A compaction's internal handle recycle (close then reopen the same
    /// on-disk file, to release in-process allocator state a file-level
    /// compact does not) closed the old handle successfully but could not
    /// reopen it. The on-disk file itself is untouched by the recycle step
    /// (compaction already finished before this ran), so it stays intact and
    /// openable; this store instance is left closed and must be reopened
    /// fresh by the caller.
    #[error(
        "store handle recycle failed for {path}: {reason} (on-disk file is intact; reopen fresh)"
    )]
    StoreHandleRecycleFailed { path: String, reason: String },
    /// A commit reached durable storage, but moving its prepared image into
    /// memory was interrupted by an unwind. The in-memory state of this
    /// handle no longer matches what is durable, and there is no correct
    /// partial publication to patch it back to, so the handle refuses every
    /// later read and write. Reopening the database recovers the complete
    /// committed state from the durable image.
    #[error(
        "a commit reached durable storage but its in-memory publication was interrupted; \
         this database refuses service until it is reopened, which recovers the committed state"
    )]
    PublicationInterrupted,
    #[error("{0}")]
    Other(Cow<'static, str>),
}

impl Error {
    pub const fn owner_read_drain_timeout() -> Self {
        Self::OwnerReadDrainTimeout
    }
}

impl From<crate::read_contract::ReadFailure> for Error {
    fn from(failure: crate::read_contract::ReadFailure) -> Self {
        Self::ReadFailure(failure)
    }
}

struct DropBlockedRankPolicyDisplay<'a> {
    table: &'a str,
    column: &'a Option<Box<str>>,
    dropped_index: &'a Option<Box<str>>,
    policy_table: &'a str,
    policy_column: &'a str,
    sort_key: &'a str,
}
impl std::fmt::Display for DropBlockedRankPolicyDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { table, column, dropped_index, policy_table, policy_column, sort_key } = self;
        if let Some(column) = column {
            return write!(f,
                "cannot drop or rename column `{table}.{column}`: rank policy `{sort_key}` on `{policy_table}.{policy_column}` depends on it"
            );
        }
        if let Some(index) = dropped_index {
            return write!(f,
                "cannot drop index `{index}` on `{table}`: rank policy `{sort_key}` on `{policy_table}.{policy_column}` depends on it"
            );
        }
        write!(f,
            "cannot drop table `{table}`: rank policy `{sort_key}` on `{policy_table}.{policy_column}` depends on it"
        )
    }
}
fn drop_blocked_rank_policy_display<'a>(
    table: &'a str,
    column: &'a Option<Box<str>>,
    dropped_index: &'a Option<Box<str>>,
    policy_table: &'a str,
    policy_column: &'a str,
    sort_key: &'a str,
) -> DropBlockedRankPolicyDisplay<'a> {
    DropBlockedRankPolicyDisplay { table, column, dropped_index, policy_table, policy_column, sort_key }
}

struct ForeignKeyViolationDisplay<'a> {
    child_table: &'a str,
    child_columns: &'a [String],
    parent_table: &'a str,
    parent_columns: &'a [String],
}
impl std::fmt::Display for ForeignKeyViolationDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "foreign key violation: {}(", self.child_table)?;
        for (index, column) in self.child_columns.iter().enumerate() {
            if index != 0 { f.write_str(", ")?; }
            f.write_str(column)?;
        }
        write!(f, ") references {}(", self.parent_table)?;
        for (index, column) in self.parent_columns.iter().enumerate() {
            if index != 0 { f.write_str(", ")?; }
            f.write_str(column)?;
        }
        f.write_str(")")
    }
}
fn foreign_key_violation_display<'a>(
    child_table: &'a str,
    child_columns: &'a [String],
    parent_table: &'a str,
    parent_columns: &'a [String],
) -> ForeignKeyViolationDisplay<'a> {
    ForeignKeyViolationDisplay { child_table, child_columns, parent_table, parent_columns }
}

pub type Result<T> = std::result::Result<T, Error>;
