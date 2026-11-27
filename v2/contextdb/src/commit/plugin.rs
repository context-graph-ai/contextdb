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
