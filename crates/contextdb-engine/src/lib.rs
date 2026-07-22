//! The embeddable storage engine: an in-process database with a SQL-like
//! executor, versioned rows, triggers, and sync-changeset production. This
//! crate has no network dependency — [`sync`] only tracks and packages
//! changes for a transport to move; `contextdb-server` is what dials out.
//!
//! # Example
//!
//! Open an in-memory database, create a table, insert a row, and read it
//! back. contextdb ships no built-in tables — the schema below is only an
//! example a caller defines for their own data:
//!
//! ```
//! use contextdb_engine::Database;
//! use std::collections::HashMap;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Database::open_memory();
//! let params = HashMap::new();
//!
//! // An example schema — contextdb ships no built-in tables.
//! db.execute(
//!     "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
//!     &params,
//! )?;
//!
//! let mut insert_params = HashMap::new();
//! insert_params.insert(
//!     "id".to_string(),
//!     contextdb_core::Value::Uuid(uuid::Uuid::new_v4()),
//! );
//! insert_params.insert(
//!     "body".to_string(),
//!     contextdb_core::Value::Text("hello contextdb".to_string()),
//! );
//! db.execute(
//!     "INSERT INTO notes (id, body) VALUES ($id, $body)",
//!     &insert_params,
//! )?;
//!
//! let result = db.execute("SELECT body FROM notes", &params)?;
//! assert_eq!(result.rows.len(), 1);
//! # Ok(())
//! # }
//! ```

pub mod cli_render;
pub mod composite_store;
pub mod database;
pub mod executor;
pub mod peer_directory;
pub mod persistence;
pub mod persistent_store;
pub mod plugin;
pub mod rank_formula;
pub mod schema_enforcer;
pub mod sync;
pub mod sync_types;
pub mod work_ledger;

#[doc(hidden)]
pub use database::CommitStageStats;
pub use database::TriggerProgressTelemetrySnapshot;
pub use database::{
    ApplyPhasePauseGuard, CronAuditEntry, CronAuditKind, CronPauseGuard, SinkError, SinkEvent,
    SinkMetrics, TriggerAuditEntry, TriggerAuditFilter, TriggerAuditStatus,
    TriggerAuditStatusFilter, TriggerContext, TriggerDeclaration, TriggerEvent,
};
pub use database::{
    CascadeReport, Database, ExportReport, IndexCandidate, QueryResult, QueryTrace,
};
pub use database::{
    MaintenanceReport, MaintenanceStatus, PruningReport, REDB_COMPACT_FRAGMENTATION_THRESHOLD,
    RETENTION_CLOCK_SKEW_TOLERANCE, TableSizeEstimate,
};
pub use database::{SearchResult, SemanticQuery};
pub use sync::{ChangeApplication, ChangeTracking};
pub use sync_types::*;
