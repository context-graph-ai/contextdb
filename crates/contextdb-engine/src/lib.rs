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

// With this opt-in, the authenticated sync orchestration compiles beside
// `Database`; raw apply and progress writers therefore remain crate-private.
// The server crate exposes the stable safe façade instead of a capability.
extern crate self as contextdb_engine;

mod blob_repository;
#[cfg(feature = "iroh")]
pub mod blob_store;
pub mod cli_render;
pub mod composite_store;
pub mod database;
pub mod executor;
#[cfg(not(feature = "test-seams"))]
mod memory_accounting;
#[cfg(feature = "test-seams")]
pub mod memory_accounting;
pub mod peer_directory;
pub mod persistence;
pub mod persistent_store;
pub mod plugin;
pub mod rank_formula;
pub mod schema_enforcer;
pub mod sync;
pub mod sync_types;
pub mod work_ledger;

#[cfg(feature = "sync-orchestration")]
pub mod error;
#[cfg(not(feature = "sync-orchestration"))]
mod error;
#[cfg(feature = "sync-orchestration")]
pub mod identity;
#[cfg(not(feature = "sync-orchestration"))]
pub(crate) mod identity;
#[cfg(feature = "sync-orchestration")]
pub mod protocol;
#[cfg(not(feature = "sync-orchestration"))]
pub(crate) mod protocol;
#[cfg(feature = "sync-orchestration")]
pub mod subjects;
#[cfg(feature = "sync-orchestration")]
pub mod sync_client;
#[cfg(feature = "sync-orchestration")]
pub mod sync_server;
#[cfg(feature = "sync-orchestration")]
mod sync_system_tables;
#[cfg(feature = "sync-orchestration")]
pub mod transfer_receipts;
#[cfg(feature = "sync-orchestration")]
pub mod transport;

#[cfg(feature = "iroh")]
pub use blob_store::{BlobStore, ResolveError};
#[doc(hidden)]
pub use database::CommitStageStats;
pub use database::TriggerProgressTelemetrySnapshot;
pub use database::{
    ApplyPhasePauseGuard, CronAuditEntry, CronAuditKind, CronPauseGuard, CronScheduleStatus,
    EventBusStatus, EventTypeStatus, RouteStatus, SinkError, SinkEvent, SinkMetrics, SinkStatus,
    TriggerAuditEntry, TriggerAuditFilter, TriggerAuditStatus, TriggerAuditStatusFilter,
    TriggerContext, TriggerDeclaration, TriggerEvent,
};
pub use database::{
    CascadeReport, Database, ExportReport, IndexCandidate, QueryResult, QueryTrace,
};
pub use database::{
    CompactionReport, MaintenancePolicy, MaintenanceReport, MaintenanceStatus, PruningReport,
    REDB_COMPACT_FRAGMENTATION_THRESHOLD, RETENTION_CLOCK_SKEW_TOLERANCE, SnapshotPin,
    TableSizeEstimate,
};
pub use database::{SearchResult, SemanticQuery};
#[cfg(feature = "sync-orchestration")]
pub use identity::FabricIdentity;
pub use sync::ChangeTracking;
#[cfg(feature = "sync-orchestration")]
pub use sync_client::SyncClient;
#[cfg(feature = "sync-orchestration")]
pub use sync_server::SyncServer;
pub use sync_types::{
    ApplyResult, ChangeSet, Conflict, DdlChange, EdgeChange, NaturalKey, RowChange, SyncAdoption,
    SyncDirection, VectorChange, natural_key_columns_for_meta,
};
