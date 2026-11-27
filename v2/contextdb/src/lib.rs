#![doc = include_str!("../README.md")]

mod commit;
mod database;
mod erase;
mod events;
mod maintenance;
mod memory;
mod query;
mod reader;
mod schema;
mod settings;
mod sql;
mod stage;
mod store;
mod system;
mod types;
mod vector;

pub use database::{
    CommitEvent, CommitSource, CorePlugin, Create, Database, DatabaseOpenOptions, DatabasePlugin,
    EventBusStatus, EventTypeStatus, PluginHealth, QueryOutcome, RouteStatus, ScheduleStatus,
    SinkMetrics, SinkStatus, SubscriptionMetrics, Transaction, WorkDone,
};
pub use events::{TriggerAuditEntry, TriggerAuditStatus};
pub use maintenance::{
    ContentSize, ExportReport, Maintenance, MaintenanceReport, VectorMaintenanceReport,
    VerifyReport, WaitingOn,
};
pub use memory::Owned;
pub use query::QueryResult;
pub use reader::{
    CancellationListener, Cursor, OwnerReadCancellation, OwnerRequestHandler, ReadLimits,
    ReadPhase, ReadProgress, ReadProgressObserver, ReadSession, ReadSessionOptions, RowVersions,
    Snapshot, request_owner,
};
pub use settings::parse_size;
pub use sql::ast::{EdgeDiscard, EdgePropagation, Propagation, Reference, ScopeLabels};
pub use sql::read_session_answers;
pub use store::{ChangeSet, StorageBackend, WrapStorage};
pub use system::Clock;
pub use types::{
    ColumnDef, ColumnDefault, ColumnType, ConflictPolicy, ContextId, DeclaredEvents, Error,
    EventTypeDecl, EventTypeTrigger, History, IndexDecl, KeptCounts, Principal, Quantization,
    Result, Retain, RetainUnit, RouteDecl, RouteWhereIn, ScopeLabel, SinkDecl, SinkError,
    SinkEvent, SinkType, SortDirection, StateMachine, SyncDirection, TableMeta, TriggerContext,
    TriggerDecl, TriggerEvent, TxId, Uuid, Value, VectorIndexRef, VectorOptions,
    VectorPartitionDeclarationIssue, VectorPolicyDeclarationIssue, VectorSearchMode,
};

/// The JSON package whose `Value` a [`Value::Json`] holds, so a host needs no copy of its own.
pub use serde_json;

/// Writes the map of named values that [`Database::execute`] binds into a statement.
///
/// `params! {}` is the empty map. Each value is anything a [`Value`] can be made from.
#[macro_export]
macro_rules! params {
    () => {
        ::std::collections::HashMap::<::std::string::String, $crate::Value>::new()
    };
    ($($name:expr => $value:expr),+ $(,)?) => {{
        let mut map = ::std::collections::HashMap::<::std::string::String, $crate::Value>::new();
        $(map.insert(::std::string::String::from($name), $crate::Value::from($value));)+
        map
    }};
}
