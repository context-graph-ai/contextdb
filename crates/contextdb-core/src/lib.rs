pub mod companion;
pub mod error;
pub mod find_attribution;
mod json_read_builder;
pub mod memory;
#[doc(hidden)]
pub mod read_contract;
#[doc(hidden)]
pub mod read_memory;
pub mod serving_cache_share;
pub mod table_meta;
pub mod traits;
pub mod types;

pub use companion::{store_companion_path, store_holders_path};
pub use error::{
    CallbackKind, Error, Result, VectorPartitionDeclarationIssue, VectorPolicyDeclarationIssue,
};
pub use memory::*;
// Explicit re-exports only — do NOT glob-export `table_meta::*`. `SortDirection`
// collides by name with `contextdb_parser::ast::SortDirection`, so downstream
// crates must use fully-qualified paths at ambiguous sites.
pub use table_meta::{
    AclRef, ColumnDef, ColumnType, CompositeForeignKey, ConflictPolicy, DEFAULT_CONFLICT_POLICY,
    DEFAULT_HISTORY_POLICY, DEFAULT_MAINTAINED_COUNTS, DEFAULT_SYNC_DIRECTION,
    DEFAULT_VECTOR_CONSOLIDATION_CHANGE_PERCENT, DEFAULT_VECTOR_CONSOLIDATION_TOMBSTONE_PERCENT,
    DEFAULT_VECTOR_MAX_PARTITIONS, DEFAULT_VECTOR_POLICY_REVISION, EdgeDiscardMode,
    ForeignKeyReference, HistoryPolicy, IndexDecl, IndexKind, MaintainedCounts, PropagationRule,
    RankPolicy, RetainUnit, ScopeLabelKind, SingleColumnForeignKey, SortDirection,
    StateMachineConstraint, SyncDirection, TableMeta, VectorQuantization,
    render_maintained_count_columns, render_sql_identifier,
};
pub use traits::*;
pub use types::*;
