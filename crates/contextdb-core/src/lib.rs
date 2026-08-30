pub mod companion;
pub mod error;
pub mod memory;
#[doc(hidden)]
pub mod read_contract;
pub mod table_meta;
pub mod traits;
pub mod types;

pub use companion::store_companion_path;
pub use error::{CallbackKind, Error, Result};
pub use memory::*;
// Explicit re-exports only — do NOT glob-export `table_meta::*`. `SortDirection`
// collides by name with `contextdb_parser::ast::SortDirection`, so downstream
// crates must use fully-qualified paths at ambiguous sites.
pub use table_meta::{
    AclRef, ColumnDef, ColumnType, CompositeForeignKey, ConflictPolicy, DEFAULT_CONFLICT_POLICY,
    DEFAULT_HISTORY_POLICY, DEFAULT_SYNC_DIRECTION, ForeignKeyReference, HistoryPolicy, IndexDecl,
    IndexKind, PropagationRule, RankPolicy, RetainUnit, ScopeLabelKind, SingleColumnForeignKey,
    SortDirection, StateMachineConstraint, SyncDirection, TableMeta, VectorQuantization,
};
pub use traits::*;
pub use types::*;
