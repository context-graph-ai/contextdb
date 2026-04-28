//! Sync contract types for contextDB change-tracking and replication.
//!
//! These types define the public API for sync operations. The actual
//! implementations are pending (all methods currently `unimplemented!()`).

use contextdb_core::{Lsn, RowId, Value, VectorIndexRef};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// A set of changes extracted from a database since a given LSN.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
        directions: &HashMap<String, SyncDirection>,
        include: &[SyncDirection],
    ) -> ChangeSet {
        let include_dir = |table: &str| {
            let dir = directions
                .get(table)
                .copied()
                .unwrap_or(SyncDirection::Both);
            include.contains(&dir)
        };

        ChangeSet {
            rows: self
                .rows
                .iter()
                .filter(|r| include_dir(&r.table))
                .cloned()
                .collect(),
            edges: self.edges.clone(),
            vectors: self.vectors.clone(),
            ddl: self
                .ddl
                .iter()
                .filter(|d| match d {
                    DdlChange::CreateTable { name, .. } => include_dir(name),
                    DdlChange::DropTable { name } => include_dir(name),
                    DdlChange::AlterTable { name, .. } => include_dir(name),
                    DdlChange::CreateIndex { table, .. } => include_dir(table),
                    DdlChange::DropIndex { table, .. } => include_dir(table),
                })
                .cloned()
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowChange {
    pub table: String,
    pub natural_key: NaturalKey,
    pub values: HashMap<String, Value>,
    pub deleted: bool,
    pub lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeChange {
    pub source: Uuid,
    pub target: Uuid,
    pub edge_type: String,
    pub properties: HashMap<String, Value>,
    pub lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorChange {
    pub index: VectorIndexRef,
    pub row_id: RowId,
    pub vector: Vec<f32>,
    pub lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DdlChange {
    CreateTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
    },
    DropTable {
        name: String,
    },
    AlterTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
    },
    CreateIndex {
        table: String,
        name: String,
        columns: Vec<(String, contextdb_core::SortDirection)>,
    },
    DropIndex {
        table: String,
        name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NaturalKey {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictPolicy {
    InsertIfNotExists,
    ServerWins,
    EdgeWins,
    LatestWins,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyResult {
    pub applied_rows: usize,
    pub skipped_rows: usize,
    pub conflicts: Vec<Conflict>,
    pub new_lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    pub natural_key: NaturalKey,
    pub resolution: ConflictPolicy,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncDirection {
    Push,
    Pull,
    Both,
    None,
}
