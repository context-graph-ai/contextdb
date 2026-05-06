pub mod error;
pub mod memory;
pub mod table_meta;
pub mod traits;
pub mod types;

pub use error::{Error, Result};
pub use memory::*;
// Explicit re-exports only — do NOT glob-export `table_meta::*`. `SortDirection`
// collides by name with `contextdb_parser::ast::SortDirection`, so downstream
// crates must use fully-qualified paths at ambiguous sites.
pub use table_meta::{
    AclRef, ColumnDef, ColumnType, CompositeForeignKey, ForeignKeyReference, IndexDecl, IndexKind,
    PropagationRule, RankPolicy, ScopeLabelKind, SingleColumnForeignKey, SortDirection,
    StateMachineConstraint, TableMeta, VectorQuantization,
};
pub use traits::*;
pub use types::*;
