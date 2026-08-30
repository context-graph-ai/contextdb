//! The read channel's own error vocabulary.
//!
//! A caller reading over the local channel is entitled to the same answer an
//! in-process call would have produced: the same class, carrying the same
//! fields, classified the same way. That entitlement used to be met by
//! carrying [`contextdb_core::Error`] itself inside the frame, which made the
//! GLOBAL error taxonomy part of the local wire: a sync, purge, trigger, or
//! plugin error appended anywhere but the end of that enum moved the bytes a
//! reader parses, and error fields had to be shaped for a wire they had
//! nothing to do with.
//!
//! So the channel owns its own document instead. Every class a READ can
//! return is named here with an EXPLICIT tag written in this source file, and
//! everything else arrives as [`ReadChannelError::Unknown`], which keeps the
//! class name and the words. Engine error maintenance -- adding, reordering,
//! or reshaping variants of [`contextdb_core::Error`] -- cannot move a single
//! byte of this document, because nothing here is derived from that enum's
//! declaration order.
//!
//! Rules for changing this file:
//!
//! - A tag is a number written down here. It is never computed, never derived
//!   from a variant's position, and never reused for a different class.
//! - A new class APPENDS a new tag. An existing tag's body shape is fixed.
//! - Every field is a primitive, a string, or a read-contract type that is
//!   already pinned as read-protocol wire (that is only
//!   [`ReadFailure`]). Identity types from the engine's own vocabulary are
//!   flattened into strings and integers here so that their layout, too, stays
//!   out of the local wire.
//!
//! The wire shape is a two-element tuple: the tag, then the class body. A
//! class with no fields has an empty body. Tags stay below 251 so that a tag
//! occupies one byte under bincode's standard integer encoding, which is what
//! makes the checked-in golden bytes readable by hand.

use contextdb_core::read_contract::ReadFailure;
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeSet;
use std::fmt;

/// The bounded read session has no implementation on this build.
pub const TAG_READ_SESSION_NOT_IMPLEMENTED: u16 = 1;
/// Owner reads did not drain before the shutdown deadline.
pub const TAG_OWNER_READ_DRAIN_TIMEOUT: u16 = 2;
/// The read was cancelled.
pub const TAG_READ_CANCELLED: u16 = 3;
/// A typed read refusal, carried in the read contract's own pinned shape.
pub const TAG_READ_FAILURE: u16 = 4;
/// The statement did not parse.
pub const TAG_PARSE_ERROR: u16 = 5;
/// The statement parsed but could not be planned.
pub const TAG_PLAN_ERROR: u16 = 6;
/// The schema the read resolved against is not valid.
pub const TAG_SCHEMA_INVALID: u16 = 7;
/// The named table does not exist.
pub const TAG_TABLE_NOT_FOUND: u16 = 8;
/// The named column does not exist on that table.
pub const TAG_COLUMN_NOT_FOUND: u16 = 9;
/// A value did not have the column's declared type.
pub const TAG_COLUMN_TYPE_MISMATCH: u16 = 10;
/// The named index does not exist on that table.
pub const TAG_INDEX_NOT_FOUND: u16 = 11;
/// Something the read named does not exist.
pub const TAG_NOT_FOUND: u16 = 12;
/// Recursive common table expressions are not supported.
pub const TAG_RECURSIVE_CTE_NOT_SUPPORTED: u16 = 13;
/// Window functions are not supported.
pub const TAG_WINDOW_FUNCTION_NOT_SUPPORTED: u16 = 14;
/// Stored procedures and functions are not supported.
pub const TAG_STORED_PROC_NOT_SUPPORTED: u16 = 15;
/// Subqueries are not supported.
pub const TAG_SUBQUERY_NOT_SUPPORTED: u16 = 16;
/// Full-text search is not supported.
pub const TAG_FULL_TEXT_SEARCH_NOT_SUPPORTED: u16 = 17;
/// That sort key expression is not supported.
pub const TAG_ORDER_BY_EXPRESSION_NOT_SUPPORTED: u16 = 18;
/// A graph traversal arrived without a depth bound.
pub const TAG_UNBOUNDED_TRAVERSAL: u16 = 19;
/// A vector search arrived without a limit.
pub const TAG_UNBOUNDED_VECTOR_SEARCH: u16 = 20;
/// The traversal went past its depth bound.
pub const TAG_BFS_DEPTH_EXCEEDED: u16 = 21;
/// The traversal's visited set went past its bound.
pub const TAG_BFS_VISITED_EXCEEDED: u16 = 22;
/// The read named a vector index that does not exist.
pub const TAG_UNKNOWN_VECTOR_INDEX: u16 = 23;
/// The query vector's dimension does not match the index.
pub const TAG_VECTOR_INDEX_DIMENSION_MISMATCH: u16 = 24;
/// A persisted row vector names a source row that is gone.
pub const TAG_PERSISTED_ROW_VECTOR_ROW_MISSING: u16 = 25;
/// A persisted row vector's source cell is NULL.
pub const TAG_PERSISTED_ROW_VECTOR_CELL_NULL: u16 = 26;
/// `USE RANK` requires a vector order in the same query.
pub const TAG_USE_RANK_REQUIRES_VECTOR_ORDER: u16 = 27;
/// `USE RANK` requires a limit in the same query.
pub const TAG_USE_RANK_REQUIRES_LIMIT: u16 = 28;
/// The named rank policy does not exist on that index.
pub const TAG_RANK_POLICY_NOT_FOUND: u16 = 29;
/// A rank policy references a column that does not exist.
pub const TAG_RANK_POLICY_COLUMN_UNKNOWN: u16 = 30;
/// A rank policy references a column present on two tables.
pub const TAG_RANK_POLICY_COLUMN_AMBIGUOUS: u16 = 31;
/// A rank policy references a column of the wrong type.
pub const TAG_RANK_POLICY_COLUMN_TYPE: u16 = 32;
/// A rank policy joins a table that does not exist.
pub const TAG_RANK_POLICY_JOIN_TABLE_UNKNOWN: u16 = 33;
/// A rank policy joins a column that is not on that table.
pub const TAG_RANK_POLICY_JOIN_COLUMN_UNKNOWN: u16 = 34;
/// A rank policy joins a column that carries no index.
pub const TAG_RANK_POLICY_JOIN_COLUMN_UNINDEXED: u16 = 35;
/// A rank policy's formula did not parse.
pub const TAG_RANK_POLICY_FORMULA_PARSE: u16 = 36;
/// The table requires a principal and the read presented none.
pub const TAG_PRINCIPAL_REQUIRED: u16 = 37;
/// A row is hidden from this principal by an access control list.
pub const TAG_ACL_DENIED: u16 = 38;
/// A row is hidden from this reader by context scope.
pub const TAG_CONTEXT_SCOPE_VIOLATION: u16 = 39;
/// A row is hidden from this reader by scope label.
pub const TAG_SCOPE_LABEL_VIOLATION: u16 = 40;
/// The store is corrupt.
pub const TAG_STORE_CORRUPTED: u16 = 41;
/// The store's identity could not be proven.
pub const TAG_STORE_IDENTITY_UNPROVABLE: u16 = 42;
/// The store predates the current on-disk format.
pub const TAG_LEGACY_VECTOR_STORE_DETECTED: u16 = 43;
/// Another process holds the store.
pub const TAG_DATABASE_LOCKED: u16 = 44;
/// The read went past the memory budget.
pub const TAG_MEMORY_BUDGET_EXCEEDED: u16 = 45;
/// The read went past the disk budget.
pub const TAG_DISK_BUDGET_EXCEEDED: u16 = 46;
/// An engine answer that only carries prose.
pub const TAG_OTHER: u16 = 47;
/// An engine answer this channel has no tag for. The class name and the words
/// still travel, so a caller learns what happened even though the class is not
/// one this channel names.
pub const TAG_UNKNOWN: u16 = 48;

/// The principal kinds, tagged explicitly rather than by declaration order.
const PRINCIPAL_KIND_SYSTEM: u32 = 0;
const PRINCIPAL_KIND_AGENT: u32 = 1;
const PRINCIPAL_KIND_HUMAN: u32 = 2;

/// One field of a class body, as the wire carries it. The decoder's
/// allocation-free preflight walks a body through this description, so a body
/// can never be admitted under a grammar different from the one that wrote it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyField {
    /// A length-prefixed string.
    Word,
    /// An unsigned 32-bit integer.
    Number32,
    /// An unsigned 64-bit integer.
    Number64,
    /// A length-prefixed sequence of strings.
    WordSequence,
    /// A read refusal in the read contract's own pinned shape.
    Failure,
}

/// The body shape one tag declares. A tag with no entry here is not a tag this
/// channel writes.
pub const fn body_grammar(tag: u16) -> Option<&'static [BodyField]> {
    use BodyField::{Failure, Number32, Number64, Word, WordSequence};
    Some(match tag {
        TAG_READ_SESSION_NOT_IMPLEMENTED
        | TAG_OWNER_READ_DRAIN_TIMEOUT
        | TAG_READ_CANCELLED
        | TAG_RECURSIVE_CTE_NOT_SUPPORTED
        | TAG_WINDOW_FUNCTION_NOT_SUPPORTED
        | TAG_STORED_PROC_NOT_SUPPORTED
        | TAG_SUBQUERY_NOT_SUPPORTED
        | TAG_FULL_TEXT_SEARCH_NOT_SUPPORTED
        | TAG_ORDER_BY_EXPRESSION_NOT_SUPPORTED
        | TAG_UNBOUNDED_TRAVERSAL
        | TAG_UNBOUNDED_VECTOR_SEARCH
        | TAG_USE_RANK_REQUIRES_VECTOR_ORDER
        | TAG_USE_RANK_REQUIRES_LIMIT => &[],
        TAG_READ_FAILURE => &[Failure],
        TAG_PARSE_ERROR
        | TAG_PLAN_ERROR
        | TAG_SCHEMA_INVALID
        | TAG_TABLE_NOT_FOUND
        | TAG_NOT_FOUND
        | TAG_PRINCIPAL_REQUIRED
        | TAG_STORE_IDENTITY_UNPROVABLE
        | TAG_OTHER => &[Word],
        TAG_COLUMN_NOT_FOUND
        | TAG_INDEX_NOT_FOUND
        | TAG_UNKNOWN_VECTOR_INDEX
        | TAG_RANK_POLICY_NOT_FOUND
        | TAG_RANK_POLICY_COLUMN_UNKNOWN
        | TAG_RANK_POLICY_COLUMN_AMBIGUOUS
        | TAG_RANK_POLICY_JOIN_TABLE_UNKNOWN
        | TAG_STORE_CORRUPTED
        | TAG_LEGACY_VECTOR_STORE_DETECTED
        | TAG_UNKNOWN => &[Word, Word],
        TAG_COLUMN_TYPE_MISMATCH | TAG_RANK_POLICY_COLUMN_TYPE => &[Word, Word, Word, Word],
        TAG_PERSISTED_ROW_VECTOR_ROW_MISSING
        | TAG_PERSISTED_ROW_VECTOR_CELL_NULL
        | TAG_RANK_POLICY_JOIN_COLUMN_UNKNOWN
        | TAG_RANK_POLICY_JOIN_COLUMN_UNINDEXED => &[Word, Word, Word],
        TAG_BFS_DEPTH_EXCEEDED => &[Number32],
        TAG_BFS_VISITED_EXCEEDED => &[Number64],
        TAG_VECTOR_INDEX_DIMENSION_MISMATCH => &[Word, Word, Number64, Number64],
        TAG_RANK_POLICY_FORMULA_PARSE => &[Word, Number64, Word],
        TAG_ACL_DENIED => &[Word, Number64, Number32, Word],
        TAG_CONTEXT_SCOPE_VIOLATION | TAG_SCOPE_LABEL_VIOLATION => &[Word, WordSequence],
        TAG_DATABASE_LOCKED => &[Number32, Word],
        TAG_MEMORY_BUDGET_EXCEEDED => &[Word, Word, Number64, Number64, Number64, Word],
        TAG_DISK_BUDGET_EXCEEDED => &[Word, Number64, Number64, Word],
        _ => return None,
    })
}

/// Every tag this channel writes, in ascending order. Tests walk this to prove
/// each class encodes under the grammar its tag declares.
pub const ALL_TAGS: &[u16] = &[
    TAG_READ_SESSION_NOT_IMPLEMENTED,
    TAG_OWNER_READ_DRAIN_TIMEOUT,
    TAG_READ_CANCELLED,
    TAG_READ_FAILURE,
    TAG_PARSE_ERROR,
    TAG_PLAN_ERROR,
    TAG_SCHEMA_INVALID,
    TAG_TABLE_NOT_FOUND,
    TAG_COLUMN_NOT_FOUND,
    TAG_COLUMN_TYPE_MISMATCH,
    TAG_INDEX_NOT_FOUND,
    TAG_NOT_FOUND,
    TAG_RECURSIVE_CTE_NOT_SUPPORTED,
    TAG_WINDOW_FUNCTION_NOT_SUPPORTED,
    TAG_STORED_PROC_NOT_SUPPORTED,
    TAG_SUBQUERY_NOT_SUPPORTED,
    TAG_FULL_TEXT_SEARCH_NOT_SUPPORTED,
    TAG_ORDER_BY_EXPRESSION_NOT_SUPPORTED,
    TAG_UNBOUNDED_TRAVERSAL,
    TAG_UNBOUNDED_VECTOR_SEARCH,
    TAG_BFS_DEPTH_EXCEEDED,
    TAG_BFS_VISITED_EXCEEDED,
    TAG_UNKNOWN_VECTOR_INDEX,
    TAG_VECTOR_INDEX_DIMENSION_MISMATCH,
    TAG_PERSISTED_ROW_VECTOR_ROW_MISSING,
    TAG_PERSISTED_ROW_VECTOR_CELL_NULL,
    TAG_USE_RANK_REQUIRES_VECTOR_ORDER,
    TAG_USE_RANK_REQUIRES_LIMIT,
    TAG_RANK_POLICY_NOT_FOUND,
    TAG_RANK_POLICY_COLUMN_UNKNOWN,
    TAG_RANK_POLICY_COLUMN_AMBIGUOUS,
    TAG_RANK_POLICY_COLUMN_TYPE,
    TAG_RANK_POLICY_JOIN_TABLE_UNKNOWN,
    TAG_RANK_POLICY_JOIN_COLUMN_UNKNOWN,
    TAG_RANK_POLICY_JOIN_COLUMN_UNINDEXED,
    TAG_RANK_POLICY_FORMULA_PARSE,
    TAG_PRINCIPAL_REQUIRED,
    TAG_ACL_DENIED,
    TAG_CONTEXT_SCOPE_VIOLATION,
    TAG_SCOPE_LABEL_VIOLATION,
    TAG_STORE_CORRUPTED,
    TAG_STORE_IDENTITY_UNPROVABLE,
    TAG_LEGACY_VECTOR_STORE_DETECTED,
    TAG_DATABASE_LOCKED,
    TAG_MEMORY_BUDGET_EXCEEDED,
    TAG_DISK_BUDGET_EXCEEDED,
    TAG_OTHER,
    TAG_UNKNOWN,
];

/// One engine answer, in the vocabulary this channel owns.
#[derive(Debug, Clone, PartialEq)]
pub enum ReadChannelError {
    ReadSessionNotImplemented,
    OwnerReadDrainTimeout,
    ReadCancelled,
    ReadFailure(ReadFailure),
    ParseError {
        message: String,
    },
    PlanError {
        message: String,
    },
    SchemaInvalid {
        reason: String,
    },
    TableNotFound {
        table: String,
    },
    ColumnNotFound {
        table: String,
        column: String,
    },
    ColumnTypeMismatch {
        table: String,
        column: String,
        expected: String,
        actual: String,
    },
    IndexNotFound {
        table: String,
        index: String,
    },
    NotFound {
        message: String,
    },
    RecursiveCteNotSupported,
    WindowFunctionNotSupported,
    StoredProcNotSupported,
    SubqueryNotSupported,
    FullTextSearchNotSupported,
    OrderByExpressionNotSupported,
    UnboundedTraversal,
    UnboundedVectorSearch,
    BfsDepthExceeded {
        maximum: u32,
    },
    BfsVisitedExceeded {
        limit: u64,
    },
    UnknownVectorIndex {
        table: String,
        column: String,
    },
    VectorIndexDimensionMismatch {
        table: String,
        column: String,
        expected: u64,
        actual: u64,
    },
    PersistedRowVectorRowMissing {
        table: String,
        column: String,
        key: String,
    },
    PersistedRowVectorCellNull {
        table: String,
        column: String,
        key: String,
    },
    UseRankRequiresVectorOrder,
    UseRankRequiresLimit,
    RankPolicyNotFound {
        index: String,
        sort_key: String,
    },
    RankPolicyColumnUnknown {
        index: String,
        column: String,
    },
    RankPolicyColumnAmbiguous {
        index: String,
        column: String,
    },
    RankPolicyColumnType {
        index: String,
        column: String,
        expected: String,
        actual: String,
    },
    RankPolicyJoinTableUnknown {
        index: String,
        table: String,
    },
    RankPolicyJoinColumnUnknown {
        index: String,
        table: String,
        column: String,
    },
    RankPolicyJoinColumnUnindexed {
        index: String,
        joined_table: String,
        column: String,
    },
    RankPolicyFormulaParse {
        index: String,
        position: u64,
        reason: String,
    },
    PrincipalRequired {
        table: String,
    },
    AclDenied {
        table: String,
        row_id: u64,
        principal_kind: u32,
        principal_id: String,
    },
    ContextScopeViolation {
        requested: String,
        allowed: Vec<String>,
    },
    ScopeLabelViolation {
        requested: String,
        allowed: Vec<String>,
    },
    StoreCorrupted {
        path: String,
        reason: String,
    },
    StoreIdentityUnprovable {
        path: String,
    },
    LegacyVectorStoreDetected {
        found_format_marker: String,
        expected_release: String,
    },
    DatabaseLocked {
        holder_pid: u32,
        path: String,
    },
    MemoryBudgetExceeded {
        subsystem: String,
        operation: String,
        requested_bytes: u64,
        available_bytes: u64,
        budget_limit_bytes: u64,
        hint: String,
    },
    DiskBudgetExceeded {
        operation: String,
        current_bytes: u64,
        budget_limit_bytes: u64,
        hint: String,
    },
    Other {
        message: String,
    },
    /// The class this channel has no tag for. The words still travel.
    Unknown {
        class_name: String,
        message: String,
    },
}

impl ReadChannelError {
    /// The tag this class travels under. Written down, never computed.
    pub const fn tag(&self) -> u16 {
        match self {
            Self::ReadSessionNotImplemented => TAG_READ_SESSION_NOT_IMPLEMENTED,
            Self::OwnerReadDrainTimeout => TAG_OWNER_READ_DRAIN_TIMEOUT,
            Self::ReadCancelled => TAG_READ_CANCELLED,
            Self::ReadFailure(_) => TAG_READ_FAILURE,
            Self::ParseError { .. } => TAG_PARSE_ERROR,
            Self::PlanError { .. } => TAG_PLAN_ERROR,
            Self::SchemaInvalid { .. } => TAG_SCHEMA_INVALID,
            Self::TableNotFound { .. } => TAG_TABLE_NOT_FOUND,
            Self::ColumnNotFound { .. } => TAG_COLUMN_NOT_FOUND,
            Self::ColumnTypeMismatch { .. } => TAG_COLUMN_TYPE_MISMATCH,
            Self::IndexNotFound { .. } => TAG_INDEX_NOT_FOUND,
            Self::NotFound { .. } => TAG_NOT_FOUND,
            Self::RecursiveCteNotSupported => TAG_RECURSIVE_CTE_NOT_SUPPORTED,
            Self::WindowFunctionNotSupported => TAG_WINDOW_FUNCTION_NOT_SUPPORTED,
            Self::StoredProcNotSupported => TAG_STORED_PROC_NOT_SUPPORTED,
            Self::SubqueryNotSupported => TAG_SUBQUERY_NOT_SUPPORTED,
            Self::FullTextSearchNotSupported => TAG_FULL_TEXT_SEARCH_NOT_SUPPORTED,
            Self::OrderByExpressionNotSupported => TAG_ORDER_BY_EXPRESSION_NOT_SUPPORTED,
            Self::UnboundedTraversal => TAG_UNBOUNDED_TRAVERSAL,
            Self::UnboundedVectorSearch => TAG_UNBOUNDED_VECTOR_SEARCH,
            Self::BfsDepthExceeded { .. } => TAG_BFS_DEPTH_EXCEEDED,
            Self::BfsVisitedExceeded { .. } => TAG_BFS_VISITED_EXCEEDED,
            Self::UnknownVectorIndex { .. } => TAG_UNKNOWN_VECTOR_INDEX,
            Self::VectorIndexDimensionMismatch { .. } => TAG_VECTOR_INDEX_DIMENSION_MISMATCH,
            Self::PersistedRowVectorRowMissing { .. } => TAG_PERSISTED_ROW_VECTOR_ROW_MISSING,
            Self::PersistedRowVectorCellNull { .. } => TAG_PERSISTED_ROW_VECTOR_CELL_NULL,
            Self::UseRankRequiresVectorOrder => TAG_USE_RANK_REQUIRES_VECTOR_ORDER,
            Self::UseRankRequiresLimit => TAG_USE_RANK_REQUIRES_LIMIT,
            Self::RankPolicyNotFound { .. } => TAG_RANK_POLICY_NOT_FOUND,
            Self::RankPolicyColumnUnknown { .. } => TAG_RANK_POLICY_COLUMN_UNKNOWN,
            Self::RankPolicyColumnAmbiguous { .. } => TAG_RANK_POLICY_COLUMN_AMBIGUOUS,
            Self::RankPolicyColumnType { .. } => TAG_RANK_POLICY_COLUMN_TYPE,
            Self::RankPolicyJoinTableUnknown { .. } => TAG_RANK_POLICY_JOIN_TABLE_UNKNOWN,
            Self::RankPolicyJoinColumnUnknown { .. } => TAG_RANK_POLICY_JOIN_COLUMN_UNKNOWN,
            Self::RankPolicyJoinColumnUnindexed { .. } => TAG_RANK_POLICY_JOIN_COLUMN_UNINDEXED,
            Self::RankPolicyFormulaParse { .. } => TAG_RANK_POLICY_FORMULA_PARSE,
            Self::PrincipalRequired { .. } => TAG_PRINCIPAL_REQUIRED,
            Self::AclDenied { .. } => TAG_ACL_DENIED,
            Self::ContextScopeViolation { .. } => TAG_CONTEXT_SCOPE_VIOLATION,
            Self::ScopeLabelViolation { .. } => TAG_SCOPE_LABEL_VIOLATION,
            Self::StoreCorrupted { .. } => TAG_STORE_CORRUPTED,
            Self::StoreIdentityUnprovable { .. } => TAG_STORE_IDENTITY_UNPROVABLE,
            Self::LegacyVectorStoreDetected { .. } => TAG_LEGACY_VECTOR_STORE_DETECTED,
            Self::DatabaseLocked { .. } => TAG_DATABASE_LOCKED,
            Self::MemoryBudgetExceeded { .. } => TAG_MEMORY_BUDGET_EXCEEDED,
            Self::DiskBudgetExceeded { .. } => TAG_DISK_BUDGET_EXCEEDED,
            Self::Other { .. } => TAG_OTHER,
            Self::Unknown { .. } => TAG_UNKNOWN,
        }
    }
}

/// The class name a fallback answer carries. A derived `Debug` names the
/// variant first, so the leading identifier is the class -- and this runs only
/// on the fallback path, where the alternative is losing the class entirely.
fn class_name_of(error: &contextdb_core::Error) -> String {
    let rendered = format!("{error:?}");
    let name: String = rendered
        .chars()
        .take_while(|character| character.is_alphanumeric() || *character == '_')
        .collect();
    if name.is_empty() {
        "UnnamedEngineAnswer".to_owned()
    } else {
        name
    }
}

fn context_word(context: &contextdb_core::types::ContextId) -> String {
    context.0.to_string()
}

fn context_words(contexts: &BTreeSet<contextdb_core::types::ContextId>) -> Vec<String> {
    contexts.iter().map(context_word).collect()
}

fn label_words(labels: &BTreeSet<contextdb_core::types::ScopeLabel>) -> Vec<String> {
    labels.iter().map(|label| label.0.clone()).collect()
}

fn context_from_word(word: &str) -> Option<contextdb_core::types::ContextId> {
    uuid::Uuid::parse_str(word)
        .ok()
        .map(contextdb_core::types::ContextId)
}

impl From<&contextdb_core::Error> for ReadChannelError {
    fn from(error: &contextdb_core::Error) -> Self {
        use contextdb_core::Error as Engine;
        match error {
            Engine::ReadSessionNotImplemented => Self::ReadSessionNotImplemented,
            Engine::OwnerReadDrainTimeout => Self::OwnerReadDrainTimeout,
            Engine::ReadCancelled => Self::ReadCancelled,
            Engine::ReadFailure(failure) => Self::ReadFailure(failure.clone()),
            Engine::ParseError(message) => Self::ParseError {
                message: message.clone(),
            },
            Engine::PlanError(message) => Self::PlanError {
                message: message.clone(),
            },
            Engine::SchemaInvalid { reason } => Self::SchemaInvalid {
                reason: reason.clone(),
            },
            Engine::TableNotFound(table) => Self::TableNotFound {
                table: table.clone(),
            },
            Engine::ColumnNotFound { table, column } => Self::ColumnNotFound {
                table: table.clone(),
                column: column.clone(),
            },
            Engine::ColumnTypeMismatch {
                table,
                column,
                expected,
                actual,
            } => Self::ColumnTypeMismatch {
                table: table.clone(),
                column: column.clone(),
                expected: expected.to_string(),
                actual: actual.to_string(),
            },
            Engine::IndexNotFound { table, index } => Self::IndexNotFound {
                table: table.clone(),
                index: index.clone(),
            },
            Engine::NotFound(message) => Self::NotFound {
                message: message.clone(),
            },
            Engine::RecursiveCteNotSupported => Self::RecursiveCteNotSupported,
            Engine::WindowFunctionNotSupported => Self::WindowFunctionNotSupported,
            Engine::StoredProcNotSupported => Self::StoredProcNotSupported,
            Engine::SubqueryNotSupported => Self::SubqueryNotSupported,
            Engine::FullTextSearchNotSupported => Self::FullTextSearchNotSupported,
            Engine::OrderByExpressionNotSupported => Self::OrderByExpressionNotSupported,
            Engine::UnboundedTraversal => Self::UnboundedTraversal,
            Engine::UnboundedVectorSearch => Self::UnboundedVectorSearch,
            Engine::BfsDepthExceeded(maximum) => Self::BfsDepthExceeded { maximum: *maximum },
            Engine::BfsVisitedExceeded(limit) => Self::BfsVisitedExceeded {
                limit: *limit as u64,
            },
            Engine::UnknownVectorIndex { index } => Self::UnknownVectorIndex {
                table: index.table.clone(),
                column: index.column.clone(),
            },
            Engine::VectorIndexDimensionMismatch {
                index,
                expected,
                actual,
            } => Self::VectorIndexDimensionMismatch {
                table: index.table.clone(),
                column: index.column.clone(),
                expected: *expected as u64,
                actual: *actual as u64,
            },
            Engine::PersistedRowVectorRowMissing { index, key } => {
                Self::PersistedRowVectorRowMissing {
                    table: index.table.clone(),
                    column: index.column.clone(),
                    key: key.clone(),
                }
            }
            Engine::PersistedRowVectorCellNull { index, key } => Self::PersistedRowVectorCellNull {
                table: index.table.clone(),
                column: index.column.clone(),
                key: key.clone(),
            },
            Engine::UseRankRequiresVectorOrder => Self::UseRankRequiresVectorOrder,
            Engine::UseRankRequiresLimit => Self::UseRankRequiresLimit,
            Engine::RankPolicyNotFound { index, sort_key } => Self::RankPolicyNotFound {
                index: index.clone(),
                sort_key: sort_key.clone(),
            },
            Engine::RankPolicyColumnUnknown { index, column } => Self::RankPolicyColumnUnknown {
                index: index.clone(),
                column: column.clone(),
            },
            Engine::RankPolicyColumnAmbiguous { index, column } => {
                Self::RankPolicyColumnAmbiguous {
                    index: index.clone(),
                    column: column.clone(),
                }
            }
            Engine::RankPolicyColumnType {
                index,
                column,
                expected,
                actual,
            } => Self::RankPolicyColumnType {
                index: index.clone(),
                column: column.clone(),
                expected: expected.clone(),
                actual: actual.clone(),
            },
            Engine::RankPolicyJoinTableUnknown { index, table } => {
                Self::RankPolicyJoinTableUnknown {
                    index: index.clone(),
                    table: table.clone(),
                }
            }
            Engine::RankPolicyJoinColumnUnknown {
                index,
                table,
                column,
            } => Self::RankPolicyJoinColumnUnknown {
                index: index.clone(),
                table: table.clone(),
                column: column.clone(),
            },
            Engine::RankPolicyJoinColumnUnindexed {
                index,
                joined_table,
                column,
            } => Self::RankPolicyJoinColumnUnindexed {
                index: index.clone(),
                joined_table: joined_table.clone(),
                column: column.clone(),
            },
            Engine::RankPolicyFormulaParse {
                index,
                position,
                reason,
            } => Self::RankPolicyFormulaParse {
                index: index.clone(),
                position: *position as u64,
                reason: reason.clone(),
            },
            Engine::PrincipalRequired { table } => Self::PrincipalRequired {
                table: table.clone(),
            },
            Engine::AclDenied {
                table,
                row_id,
                principal,
            } => {
                let (principal_kind, principal_id) = match principal {
                    contextdb_core::types::Principal::System => {
                        (PRINCIPAL_KIND_SYSTEM, String::new())
                    }
                    contextdb_core::types::Principal::Agent(id) => {
                        (PRINCIPAL_KIND_AGENT, id.clone())
                    }
                    contextdb_core::types::Principal::Human(id) => {
                        (PRINCIPAL_KIND_HUMAN, id.clone())
                    }
                };
                Self::AclDenied {
                    table: table.clone(),
                    row_id: row_id.0,
                    principal_kind,
                    principal_id,
                }
            }
            Engine::ContextScopeViolation { requested, allowed } => Self::ContextScopeViolation {
                requested: context_word(requested),
                allowed: context_words(allowed),
            },
            Engine::ScopeLabelViolation { requested, allowed } => Self::ScopeLabelViolation {
                requested: requested.0.clone(),
                allowed: label_words(allowed),
            },
            Engine::StoreCorrupted { path, reason } => Self::StoreCorrupted {
                path: path.clone(),
                reason: reason.clone(),
            },
            Engine::StoreIdentityUnprovable { path } => {
                Self::StoreIdentityUnprovable { path: path.clone() }
            }
            Engine::LegacyVectorStoreDetected {
                found_format_marker,
                expected_release,
            } => Self::LegacyVectorStoreDetected {
                found_format_marker: found_format_marker.clone(),
                expected_release: expected_release.clone(),
            },
            Engine::DatabaseLocked { holder_pid, path } => Self::DatabaseLocked {
                holder_pid: *holder_pid,
                path: path.to_string_lossy().into_owned(),
            },
            Engine::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => Self::MemoryBudgetExceeded {
                subsystem: subsystem.clone(),
                operation: operation.clone(),
                requested_bytes: *requested_bytes as u64,
                available_bytes: *available_bytes as u64,
                budget_limit_bytes: *budget_limit_bytes as u64,
                hint: hint.clone(),
            },
            Engine::DiskBudgetExceeded {
                operation,
                current_bytes,
                budget_limit_bytes,
                hint,
            } => Self::DiskBudgetExceeded {
                operation: operation.clone(),
                current_bytes: *current_bytes,
                budget_limit_bytes: *budget_limit_bytes,
                hint: hint.clone(),
            },
            Engine::Other(message) => Self::Other {
                message: message.clone(),
            },
            other => Self::Unknown {
                class_name: class_name_of(other),
                message: other.to_string(),
            },
        }
    }
}

/// A count that travelled as a 64-bit number, back in the width this platform
/// counts with. A machine narrower than the one that wrote it keeps the
/// largest count it can hold rather than silently wrapping to a small one.
fn count(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

impl From<ReadChannelError> for contextdb_core::Error {
    fn from(document: ReadChannelError) -> Self {
        use contextdb_core::types::{ContextId, Principal, RowId, ScopeLabel, VectorIndexRef};
        match document {
            ReadChannelError::ReadSessionNotImplemented => Self::ReadSessionNotImplemented,
            ReadChannelError::OwnerReadDrainTimeout => Self::OwnerReadDrainTimeout,
            ReadChannelError::ReadCancelled => Self::ReadCancelled,
            ReadChannelError::ReadFailure(failure) => Self::ReadFailure(failure),
            ReadChannelError::ParseError { message } => Self::ParseError(message),
            ReadChannelError::PlanError { message } => Self::PlanError(message),
            ReadChannelError::SchemaInvalid { reason } => Self::SchemaInvalid { reason },
            ReadChannelError::TableNotFound { table } => Self::TableNotFound(table),
            ReadChannelError::ColumnNotFound { table, column } => {
                Self::ColumnNotFound { table, column }
            }
            ReadChannelError::ColumnTypeMismatch {
                table,
                column,
                expected,
                actual,
            } => Self::ColumnTypeMismatch {
                table,
                column,
                expected,
                actual,
            },
            ReadChannelError::IndexNotFound { table, index } => {
                Self::IndexNotFound { table, index }
            }
            ReadChannelError::NotFound { message } => Self::NotFound(message),
            ReadChannelError::RecursiveCteNotSupported => Self::RecursiveCteNotSupported,
            ReadChannelError::WindowFunctionNotSupported => Self::WindowFunctionNotSupported,
            ReadChannelError::StoredProcNotSupported => Self::StoredProcNotSupported,
            ReadChannelError::SubqueryNotSupported => Self::SubqueryNotSupported,
            ReadChannelError::FullTextSearchNotSupported => Self::FullTextSearchNotSupported,
            ReadChannelError::OrderByExpressionNotSupported => Self::OrderByExpressionNotSupported,
            ReadChannelError::UnboundedTraversal => Self::UnboundedTraversal,
            ReadChannelError::UnboundedVectorSearch => Self::UnboundedVectorSearch,
            ReadChannelError::BfsDepthExceeded { maximum } => Self::BfsDepthExceeded(maximum),
            ReadChannelError::BfsVisitedExceeded { limit } => {
                Self::BfsVisitedExceeded(count(limit))
            }
            ReadChannelError::UnknownVectorIndex { table, column } => Self::UnknownVectorIndex {
                index: VectorIndexRef { table, column },
            },
            ReadChannelError::VectorIndexDimensionMismatch {
                table,
                column,
                expected,
                actual,
            } => Self::VectorIndexDimensionMismatch {
                index: VectorIndexRef { table, column },
                expected: count(expected),
                actual: count(actual),
            },
            ReadChannelError::PersistedRowVectorRowMissing { table, column, key } => {
                Self::PersistedRowVectorRowMissing {
                    index: VectorIndexRef { table, column },
                    key,
                }
            }
            ReadChannelError::PersistedRowVectorCellNull { table, column, key } => {
                Self::PersistedRowVectorCellNull {
                    index: VectorIndexRef { table, column },
                    key,
                }
            }
            ReadChannelError::UseRankRequiresVectorOrder => Self::UseRankRequiresVectorOrder,
            ReadChannelError::UseRankRequiresLimit => Self::UseRankRequiresLimit,
            ReadChannelError::RankPolicyNotFound { index, sort_key } => {
                Self::RankPolicyNotFound { index, sort_key }
            }
            ReadChannelError::RankPolicyColumnUnknown { index, column } => {
                Self::RankPolicyColumnUnknown { index, column }
            }
            ReadChannelError::RankPolicyColumnAmbiguous { index, column } => {
                Self::RankPolicyColumnAmbiguous { index, column }
            }
            ReadChannelError::RankPolicyColumnType {
                index,
                column,
                expected,
                actual,
            } => Self::RankPolicyColumnType {
                index,
                column,
                expected,
                actual,
            },
            ReadChannelError::RankPolicyJoinTableUnknown { index, table } => {
                Self::RankPolicyJoinTableUnknown { index, table }
            }
            ReadChannelError::RankPolicyJoinColumnUnknown {
                index,
                table,
                column,
            } => Self::RankPolicyJoinColumnUnknown {
                index,
                table,
                column,
            },
            ReadChannelError::RankPolicyJoinColumnUnindexed {
                index,
                joined_table,
                column,
            } => Self::RankPolicyJoinColumnUnindexed {
                index,
                joined_table,
                column,
            },
            ReadChannelError::RankPolicyFormulaParse {
                index,
                position,
                reason,
            } => Self::RankPolicyFormulaParse {
                index,
                position: count(position),
                reason,
            },
            ReadChannelError::PrincipalRequired { table } => Self::PrincipalRequired { table },
            ReadChannelError::AclDenied {
                table,
                row_id,
                principal_kind,
                principal_id,
            } => Self::AclDenied {
                table,
                row_id: RowId(row_id),
                principal: match principal_kind {
                    PRINCIPAL_KIND_AGENT => Principal::Agent(principal_id),
                    PRINCIPAL_KIND_HUMAN => Principal::Human(principal_id),
                    _ => Principal::System,
                },
            },
            ReadChannelError::ContextScopeViolation { requested, allowed } => {
                let Some(requested) = context_from_word(&requested) else {
                    return Self::Other(format!(
                        "ContextScopeViolation: requested context {requested} did not arrive as a readable identifier"
                    ));
                };
                Self::ContextScopeViolation {
                    requested,
                    allowed: allowed
                        .iter()
                        .filter_map(|word| context_from_word(word))
                        .collect::<BTreeSet<ContextId>>(),
                }
            }
            ReadChannelError::ScopeLabelViolation { requested, allowed } => {
                Self::ScopeLabelViolation {
                    requested: ScopeLabel(requested),
                    allowed: allowed
                        .into_iter()
                        .map(ScopeLabel)
                        .collect::<BTreeSet<ScopeLabel>>(),
                }
            }
            ReadChannelError::StoreCorrupted { path, reason } => {
                Self::StoreCorrupted { path, reason }
            }
            ReadChannelError::StoreIdentityUnprovable { path } => {
                Self::StoreIdentityUnprovable { path }
            }
            ReadChannelError::LegacyVectorStoreDetected {
                found_format_marker,
                expected_release,
            } => Self::LegacyVectorStoreDetected {
                found_format_marker,
                expected_release,
            },
            ReadChannelError::DatabaseLocked { holder_pid, path } => Self::DatabaseLocked {
                holder_pid,
                path: std::path::PathBuf::from(path),
            },
            ReadChannelError::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => Self::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: count(requested_bytes),
                available_bytes: count(available_bytes),
                budget_limit_bytes: count(budget_limit_bytes),
                hint,
            },
            ReadChannelError::DiskBudgetExceeded {
                operation,
                current_bytes,
                budget_limit_bytes,
                hint,
            } => Self::DiskBudgetExceeded {
                operation,
                current_bytes,
                budget_limit_bytes,
                hint,
            },
            ReadChannelError::Other { message } => Self::Other(message),
            ReadChannelError::Unknown {
                class_name,
                message,
            } => Self::Other(format!("{class_name}: {message}")),
        }
    }
}

impl Serialize for ReadChannelError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut document = serializer.serialize_tuple(2)?;
        document.serialize_element(&self.tag())?;
        match self {
            Self::ReadSessionNotImplemented
            | Self::OwnerReadDrainTimeout
            | Self::ReadCancelled
            | Self::RecursiveCteNotSupported
            | Self::WindowFunctionNotSupported
            | Self::StoredProcNotSupported
            | Self::SubqueryNotSupported
            | Self::FullTextSearchNotSupported
            | Self::OrderByExpressionNotSupported
            | Self::UnboundedTraversal
            | Self::UnboundedVectorSearch
            | Self::UseRankRequiresVectorOrder
            | Self::UseRankRequiresLimit => document.serialize_element(&())?,
            Self::ReadFailure(failure) => document.serialize_element(failure)?,
            Self::ParseError { message }
            | Self::PlanError { message }
            | Self::NotFound { message }
            | Self::Other { message } => document.serialize_element(message)?,
            Self::SchemaInvalid { reason } => document.serialize_element(reason)?,
            Self::TableNotFound { table } | Self::PrincipalRequired { table } => {
                document.serialize_element(table)?;
            }
            Self::StoreIdentityUnprovable { path } => document.serialize_element(path)?,
            Self::ColumnNotFound { table, column } | Self::UnknownVectorIndex { table, column } => {
                document.serialize_element(&(table, column))?;
            }
            Self::RankPolicyJoinTableUnknown { index, table } => {
                document.serialize_element(&(index, table))?;
            }
            Self::IndexNotFound { table, index } => document.serialize_element(&(table, index))?,
            Self::RankPolicyNotFound { index, sort_key } => {
                document.serialize_element(&(index, sort_key))?;
            }
            Self::RankPolicyColumnUnknown { index, column }
            | Self::RankPolicyColumnAmbiguous { index, column } => {
                document.serialize_element(&(index, column))?;
            }
            Self::StoreCorrupted { path, reason } => document.serialize_element(&(path, reason))?,
            Self::LegacyVectorStoreDetected {
                found_format_marker,
                expected_release,
            } => document.serialize_element(&(found_format_marker, expected_release))?,
            Self::Unknown {
                class_name,
                message,
            } => document.serialize_element(&(class_name, message))?,
            Self::ColumnTypeMismatch {
                table,
                column,
                expected,
                actual,
            } => document.serialize_element(&(table, column, expected, actual))?,
            Self::RankPolicyColumnType {
                index,
                column,
                expected,
                actual,
            } => document.serialize_element(&(index, column, expected, actual))?,
            Self::PersistedRowVectorRowMissing { table, column, key }
            | Self::PersistedRowVectorCellNull { table, column, key } => {
                document.serialize_element(&(table, column, key))?;
            }
            Self::RankPolicyJoinColumnUnknown {
                index,
                table,
                column,
            } => document.serialize_element(&(index, table, column))?,
            Self::RankPolicyJoinColumnUnindexed {
                index,
                joined_table,
                column,
            } => document.serialize_element(&(index, joined_table, column))?,
            Self::BfsDepthExceeded { maximum } => document.serialize_element(maximum)?,
            Self::BfsVisitedExceeded { limit } => document.serialize_element(limit)?,
            Self::VectorIndexDimensionMismatch {
                table,
                column,
                expected,
                actual,
            } => document.serialize_element(&(table, column, expected, actual))?,
            Self::RankPolicyFormulaParse {
                index,
                position,
                reason,
            } => document.serialize_element(&(index, position, reason))?,
            Self::AclDenied {
                table,
                row_id,
                principal_kind,
                principal_id,
            } => document.serialize_element(&(table, row_id, principal_kind, principal_id))?,
            Self::ContextScopeViolation { requested, allowed }
            | Self::ScopeLabelViolation { requested, allowed } => {
                document.serialize_element(&(requested, allowed))?;
            }
            Self::DatabaseLocked { holder_pid, path } => {
                document.serialize_element(&(holder_pid, path))?;
            }
            Self::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            } => document.serialize_element(&(
                subsystem,
                operation,
                requested_bytes,
                available_bytes,
                budget_limit_bytes,
                hint,
            ))?,
            Self::DiskBudgetExceeded {
                operation,
                current_bytes,
                budget_limit_bytes,
                hint,
            } => {
                document.serialize_element(&(operation, current_bytes, budget_limit_bytes, hint))?
            }
        }
        document.end()
    }
}

struct ReadChannelErrorVisitor;

fn body<'de, A, T>(sequence: &mut A) -> Result<T, A::Error>
where
    A: SeqAccess<'de>,
    T: Deserialize<'de>,
{
    sequence
        .next_element::<T>()?
        .ok_or_else(|| de::Error::custom("read-channel error document has no class body"))
}

impl<'de> Visitor<'de> for ReadChannelErrorVisitor {
    type Value = ReadChannelError;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a read-channel error document: a tag and its class body")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let tag: u16 = sequence
            .next_element()?
            .ok_or_else(|| de::Error::custom("read-channel error document has no tag"))?;
        Ok(match tag {
            TAG_READ_SESSION_NOT_IMPLEMENTED => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::ReadSessionNotImplemented
            }
            TAG_OWNER_READ_DRAIN_TIMEOUT => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::OwnerReadDrainTimeout
            }
            TAG_READ_CANCELLED => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::ReadCancelled
            }
            TAG_RECURSIVE_CTE_NOT_SUPPORTED => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::RecursiveCteNotSupported
            }
            TAG_WINDOW_FUNCTION_NOT_SUPPORTED => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::WindowFunctionNotSupported
            }
            TAG_STORED_PROC_NOT_SUPPORTED => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::StoredProcNotSupported
            }
            TAG_SUBQUERY_NOT_SUPPORTED => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::SubqueryNotSupported
            }
            TAG_FULL_TEXT_SEARCH_NOT_SUPPORTED => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::FullTextSearchNotSupported
            }
            TAG_ORDER_BY_EXPRESSION_NOT_SUPPORTED => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::OrderByExpressionNotSupported
            }
            TAG_UNBOUNDED_TRAVERSAL => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::UnboundedTraversal
            }
            TAG_UNBOUNDED_VECTOR_SEARCH => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::UnboundedVectorSearch
            }
            TAG_USE_RANK_REQUIRES_VECTOR_ORDER => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::UseRankRequiresVectorOrder
            }
            TAG_USE_RANK_REQUIRES_LIMIT => {
                body::<A, ()>(&mut sequence)?;
                ReadChannelError::UseRankRequiresLimit
            }
            TAG_READ_FAILURE => {
                ReadChannelError::ReadFailure(body::<A, ReadFailure>(&mut sequence)?)
            }
            TAG_PARSE_ERROR => ReadChannelError::ParseError {
                message: body(&mut sequence)?,
            },
            TAG_PLAN_ERROR => ReadChannelError::PlanError {
                message: body(&mut sequence)?,
            },
            TAG_NOT_FOUND => ReadChannelError::NotFound {
                message: body(&mut sequence)?,
            },
            TAG_OTHER => ReadChannelError::Other {
                message: body(&mut sequence)?,
            },
            TAG_SCHEMA_INVALID => ReadChannelError::SchemaInvalid {
                reason: body(&mut sequence)?,
            },
            TAG_TABLE_NOT_FOUND => ReadChannelError::TableNotFound {
                table: body(&mut sequence)?,
            },
            TAG_PRINCIPAL_REQUIRED => ReadChannelError::PrincipalRequired {
                table: body(&mut sequence)?,
            },
            TAG_STORE_IDENTITY_UNPROVABLE => ReadChannelError::StoreIdentityUnprovable {
                path: body(&mut sequence)?,
            },
            TAG_COLUMN_NOT_FOUND => {
                let (table, column) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::ColumnNotFound { table, column }
            }
            TAG_UNKNOWN_VECTOR_INDEX => {
                let (table, column) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::UnknownVectorIndex { table, column }
            }
            TAG_INDEX_NOT_FOUND => {
                let (table, index) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::IndexNotFound { table, index }
            }
            TAG_RANK_POLICY_NOT_FOUND => {
                let (index, sort_key) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::RankPolicyNotFound { index, sort_key }
            }
            TAG_RANK_POLICY_COLUMN_UNKNOWN => {
                let (index, column) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::RankPolicyColumnUnknown { index, column }
            }
            TAG_RANK_POLICY_COLUMN_AMBIGUOUS => {
                let (index, column) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::RankPolicyColumnAmbiguous { index, column }
            }
            TAG_RANK_POLICY_JOIN_TABLE_UNKNOWN => {
                let (index, table) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::RankPolicyJoinTableUnknown { index, table }
            }
            TAG_STORE_CORRUPTED => {
                let (path, reason) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::StoreCorrupted { path, reason }
            }
            TAG_LEGACY_VECTOR_STORE_DETECTED => {
                let (found_format_marker, expected_release) =
                    body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::LegacyVectorStoreDetected {
                    found_format_marker,
                    expected_release,
                }
            }
            TAG_UNKNOWN => {
                let (class_name, message) = body::<A, (String, String)>(&mut sequence)?;
                ReadChannelError::Unknown {
                    class_name,
                    message,
                }
            }
            TAG_COLUMN_TYPE_MISMATCH => {
                let (table, column, expected, actual) =
                    body::<A, (String, String, String, String)>(&mut sequence)?;
                ReadChannelError::ColumnTypeMismatch {
                    table,
                    column,
                    expected,
                    actual,
                }
            }
            TAG_RANK_POLICY_COLUMN_TYPE => {
                let (index, column, expected, actual) =
                    body::<A, (String, String, String, String)>(&mut sequence)?;
                ReadChannelError::RankPolicyColumnType {
                    index,
                    column,
                    expected,
                    actual,
                }
            }
            TAG_PERSISTED_ROW_VECTOR_ROW_MISSING => {
                let (table, column, key) = body::<A, (String, String, String)>(&mut sequence)?;
                ReadChannelError::PersistedRowVectorRowMissing { table, column, key }
            }
            TAG_PERSISTED_ROW_VECTOR_CELL_NULL => {
                let (table, column, key) = body::<A, (String, String, String)>(&mut sequence)?;
                ReadChannelError::PersistedRowVectorCellNull { table, column, key }
            }
            TAG_RANK_POLICY_JOIN_COLUMN_UNKNOWN => {
                let (index, table, column) = body::<A, (String, String, String)>(&mut sequence)?;
                ReadChannelError::RankPolicyJoinColumnUnknown {
                    index,
                    table,
                    column,
                }
            }
            TAG_RANK_POLICY_JOIN_COLUMN_UNINDEXED => {
                let (index, joined_table, column) =
                    body::<A, (String, String, String)>(&mut sequence)?;
                ReadChannelError::RankPolicyJoinColumnUnindexed {
                    index,
                    joined_table,
                    column,
                }
            }
            TAG_BFS_DEPTH_EXCEEDED => ReadChannelError::BfsDepthExceeded {
                maximum: body(&mut sequence)?,
            },
            TAG_BFS_VISITED_EXCEEDED => ReadChannelError::BfsVisitedExceeded {
                limit: body(&mut sequence)?,
            },
            TAG_VECTOR_INDEX_DIMENSION_MISMATCH => {
                let (table, column, expected, actual) =
                    body::<A, (String, String, u64, u64)>(&mut sequence)?;
                ReadChannelError::VectorIndexDimensionMismatch {
                    table,
                    column,
                    expected,
                    actual,
                }
            }
            TAG_RANK_POLICY_FORMULA_PARSE => {
                let (index, position, reason) = body::<A, (String, u64, String)>(&mut sequence)?;
                ReadChannelError::RankPolicyFormulaParse {
                    index,
                    position,
                    reason,
                }
            }
            TAG_ACL_DENIED => {
                let (table, row_id, principal_kind, principal_id) =
                    body::<A, (String, u64, u32, String)>(&mut sequence)?;
                ReadChannelError::AclDenied {
                    table,
                    row_id,
                    principal_kind,
                    principal_id,
                }
            }
            TAG_CONTEXT_SCOPE_VIOLATION => {
                let (requested, allowed) = body::<A, (String, Vec<String>)>(&mut sequence)?;
                ReadChannelError::ContextScopeViolation { requested, allowed }
            }
            TAG_SCOPE_LABEL_VIOLATION => {
                let (requested, allowed) = body::<A, (String, Vec<String>)>(&mut sequence)?;
                ReadChannelError::ScopeLabelViolation { requested, allowed }
            }
            TAG_DATABASE_LOCKED => {
                let (holder_pid, path) = body::<A, (u32, String)>(&mut sequence)?;
                ReadChannelError::DatabaseLocked { holder_pid, path }
            }
            TAG_MEMORY_BUDGET_EXCEEDED => {
                let (
                    subsystem,
                    operation,
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    hint,
                ) = body::<A, (String, String, u64, u64, u64, String)>(&mut sequence)?;
                ReadChannelError::MemoryBudgetExceeded {
                    subsystem,
                    operation,
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    hint,
                }
            }
            TAG_DISK_BUDGET_EXCEEDED => {
                let (operation, current_bytes, budget_limit_bytes, hint) =
                    body::<A, (String, u64, u64, String)>(&mut sequence)?;
                ReadChannelError::DiskBudgetExceeded {
                    operation,
                    current_bytes,
                    budget_limit_bytes,
                    hint,
                }
            }
            _ => {
                return Err(de::Error::custom(
                    "read-channel error document names a tag this build does not write",
                ));
            }
        })
    }
}

impl<'de> Deserialize<'de> for ReadChannelError {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_tuple(2, ReadChannelErrorVisitor)
    }
}
