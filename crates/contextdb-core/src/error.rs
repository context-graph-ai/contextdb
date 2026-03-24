use crate::types::TxId;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("{0} are immutable")]
    ImmutableTable(String),
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
    #[error("dimension mismatch: expected {expected}, got {got}")]
    VectorDimensionMismatch { expected: usize, got: usize },
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
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
