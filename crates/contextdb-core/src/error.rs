use crate::types::TxId;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("{0} are immutable")]
    ImmutableTable(String),
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(String),
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
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("plan error: {0}")]
    PlanError(String),
    #[error("sync error: {0}")]
    SyncError(String),
    #[error("table {0} is not sync-eligible (no natural key)")]
    NotSyncEligible(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
