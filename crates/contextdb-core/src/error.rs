use crate::types::{TxId, VectorIndexRef};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("{0} are immutable")]
    ImmutableTable(String),
    #[error("column `{column}` on table `{table}` is immutable")]
    ImmutableColumn { table: String, column: String },
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(String),
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
    #[error("vector index dimension mismatch on {index:?}: expected {expected}, got {actual}")]
    VectorIndexDimensionMismatch {
        index: VectorIndexRef,
        expected: usize,
        actual: usize,
    },
    #[error("unknown vector index {index:?}")]
    UnknownVectorIndex { index: VectorIndexRef },
    #[error(
        "legacy vector store detected (format marker {found_format_marker:?}); rebuild required for release {expected_release} - sync from a 1.0+ peer or recreate the schema and reimport"
    )]
    LegacyVectorStoreDetected {
        found_format_marker: String,
        expected_release: String,
    },
    #[error("corrupt vector store at {path}: {reason}")]
    StoreCorrupted { path: String, reason: String },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("transaction not found: {0}")]
    TxNotFound(TxId),
    #[error("unique constraint violation: {table}.{column}")]
    UniqueViolation { table: String, column: String },
    #[error("foreign key violation: {table}.{column} references {ref_table}")]
    ForeignKeyViolation {
        table: String,
        column: String,
        ref_table: String,
    },
    #[error("recursive CTEs are not supported")]
    RecursiveCteNotSupported,
    #[error("window functions are not supported")]
    WindowFunctionNotSupported,
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
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("plan error: {0}")]
    PlanError(String),
    #[error("sync error: {0}")]
    SyncError(String),
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
        subsystem: String,
        operation: String,
        requested_bytes: usize,
        available_bytes: usize,
        budget_limit_bytes: usize,
        hint: String,
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
    #[error("column type mismatch: {table}.{column} expected {expected}, got {actual}")]
    ColumnTypeMismatch {
        table: String,
        column: String,
        expected: &'static str,
        actual: &'static str,
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
    #[error("column not found: {table}.{column}")]
    ColumnNotFound { table: String, column: String },
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
