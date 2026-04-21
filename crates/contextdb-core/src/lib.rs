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
    ColumnDef, ColumnType, ForeignKeyReference, IndexDecl, PropagationRule, SortDirection,
    StateMachineConstraint, TableMeta,
};
pub use traits::*;
pub use types::*;
