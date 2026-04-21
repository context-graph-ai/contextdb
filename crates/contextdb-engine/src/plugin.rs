use crate::sync_types::{ChangeSet, DdlChange};
use contextdb_core::{Lsn, Result};
use contextdb_tx::WriteSet;
use std::time::Duration;

/// Source of a commit operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommitSource {
    User,
    AutoCommit,
    SyncPull,
}

/// Outcome of a query execution.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryOutcome {
    Success { row_count: usize },
    Error { error: String },
}

/// Health status reported by a plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginHealth {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

/// Lifecycle hooks for the database engine.
pub trait DatabasePlugin: Send + Sync {
    fn pre_commit(&self, _ws: &WriteSet, _source: CommitSource) -> Result<()> {
        Ok(())
    }
    fn post_commit(&self, _ws: &WriteSet, _source: CommitSource) {}
    fn on_open(&self) -> Result<()> {
        Ok(())
    }
    fn on_close(&self) -> Result<()> {
        Ok(())
    }
    fn on_ddl(&self, _change: &DdlChange) -> Result<()> {
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
    fn on_sync_push(&self, _changeset: &mut ChangeSet) -> Result<()> {
        Ok(())
    }
    fn on_sync_pull(&self, _changeset: &mut ChangeSet) -> Result<()> {
        Ok(())
    }
}

/// Lightweight summary of each commit, delivered to subscribers.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitEvent {
    pub source: CommitSource,
    pub lsn: Lsn,
    pub tables_changed: Vec<String>,
    pub row_count: usize,
}

/// Health metrics for the subscription system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionMetrics {
    pub active_channels: usize,
    pub events_sent: u64,
    pub events_dropped: u64,
}

/// Default plugin — all no-ops. Every Database gets this automatically.
pub struct CorePlugin;
impl DatabasePlugin for CorePlugin {}
