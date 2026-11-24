//! The carried host hooks, with the lean transaction's own change set.
use crate::{ChangeSet, Error, Result};
use std::time::Duration;

/// Who submitted a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommitSource {
    User,
    AutoCommit,
    SyncPull,
}
/// The result of executing a submitted statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOutcome {
    Success { row_count: usize },
    Error { error: String },
}
/// Health reported by the installed host plug-in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginHealth {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}
/// Host lifecycle hooks. A veto returns its original error; notifications cannot undo a commit.
/// Sync hooks arrive with their callers when sync is implemented.
pub trait DatabasePlugin: Send + Sync {
    fn pre_commit(&self, _changes: &ChangeSet, _source: CommitSource) -> Result<()> {
        Ok(())
    }
    fn post_commit(&self, _changes: &ChangeSet, _source: CommitSource) {}
    fn commit_failed(&self, _changes: &ChangeSet, _source: CommitSource, _error: &Error) {}
    fn on_open(&self) -> Result<()> {
        Ok(())
    }
    fn on_close(&self) -> Result<()> {
        Ok(())
    }
    fn on_ddl(&self, _sql: &str) -> Result<()> {
        Ok(())
    }
    fn on_query(&self, _sql: &str) -> Result<()> {
        Ok(())
    }
    fn post_query(&self, _sql: &str, _duration: Duration, _outcome: &QueryOutcome) {}
    fn health(&self) -> PluginHealth {
        PluginHealth::Healthy
    }
    fn describe(&self) -> serde_json::Value {
        serde_json::json!({})
    }
}
/// Default host plug-in; all hooks are no-ops.
pub struct CorePlugin;
impl DatabasePlugin for CorePlugin {}

/// Delivery counters for this open, with queue depth read from the committed store.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SinkMetrics {
    pub delivered: u64,
    pub queued: u64,
    pub retried: u64,
    pub uncertain: u64,
    pub permanent_failures: u64,
    pub examined: u64,
    pub window_refusals: u64,
    pub window_refused: bool,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct EventTypeStatus {
    pub name: String,
    pub trigger: String,
    pub table: String,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct SinkStatus {
    pub name: String,
    pub sink_type: String,
    pub callback_registered: bool,
    pub metrics: SinkMetrics,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct RouteStatus {
    pub name: String,
    pub event_type: String,
    pub sink: String,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScheduleStatus {
    pub name: String,
    pub callback_registered: bool,
}
/// One committed snapshot of declared events, sinks and routes.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EventBusStatus {
    /// False for readers: handler registration and live delivery counters are unavailable.
    pub live: bool,
    pub event_types: Vec<EventTypeStatus>,
    pub sinks: Vec<SinkStatus>,
    pub routes: Vec<RouteStatus>,
    pub schedules: Vec<ScheduleStatus>,
}
