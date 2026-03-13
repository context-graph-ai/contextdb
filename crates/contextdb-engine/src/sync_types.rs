//! Sync contract types for contextDB change-tracking and replication.
//!
//! These types define the public API for sync operations. The actual
//! implementations are pending (all methods currently `unimplemented!()`).

use contextdb_core::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// A set of changes extracted from a database since a given LSN.
#[derive(Debug, Clone, Default)]
pub struct ChangeSet {
    pub rows: Vec<RowChange>,
    pub edges: Vec<EdgeChange>,
    pub vectors: Vec<VectorChange>,
    pub ddl: Vec<DdlChange>,
}

impl ChangeSet {
    /// Filters this changeset to only include tables matching the given directions.
    pub fn filter_by_direction(
        &self,
        _directions: &HashMap<String, SyncDirection>,
        _include: &[SyncDirection],
    ) -> ChangeSet {
        unimplemented!("sync not implemented — direction filtering pending")
    }
}

#[derive(Debug, Clone)]
pub struct RowChange {
    pub table: String,
    pub natural_key: NaturalKey,
    pub values: HashMap<String, Value>,
    pub lsn: u64,
}

#[derive(Debug, Clone)]
pub struct EdgeChange {
    pub source: Uuid,
    pub target: Uuid,
    pub edge_type: String,
    pub properties: HashMap<String, Value>,
    pub lsn: u64,
}

#[derive(Debug, Clone)]
pub struct VectorChange {
    pub row_id: u64,
    pub vector: Vec<f32>,
    pub lsn: u64,
}

#[derive(Debug, Clone)]
pub enum DdlChange {
    CreateTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct NaturalKey {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    InsertIfNotExists,
    ServerWins,
    EdgeWins,
    LatestWins,
}

#[derive(Debug, Clone)]
pub struct ConflictPolicies {
    pub per_table: HashMap<String, ConflictPolicy>,
    pub default: ConflictPolicy,
}

impl ConflictPolicies {
    pub fn uniform(policy: ConflictPolicy) -> Self {
        Self {
            per_table: HashMap::new(),
            default: policy,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub applied_rows: usize,
    pub skipped_rows: usize,
    pub conflicts: Vec<Conflict>,
    pub new_lsn: u64,
}

#[derive(Debug, Clone)]
pub struct Conflict {
    pub natural_key: NaturalKey,
    pub resolution: ConflictPolicy,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    Push,
    Pull,
    Both,
    None,
}
