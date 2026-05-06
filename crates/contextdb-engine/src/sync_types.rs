//! Sync contract types for contextDB change-tracking and replication.
//!
//! These types define the public API for sync operations and the durable wire
//! shape used by file-backed history, full snapshots, and server replication.

use contextdb_core::{
    CompositeForeignKey, Lsn, RowId, SingleColumnForeignKey, TableMeta, Value, VectorIndexRef,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// A set of changes extracted from a database since a given LSN.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChangeSet {
    pub rows: Vec<RowChange>,
    pub edges: Vec<EdgeChange>,
    pub vectors: Vec<VectorChange>,
    pub ddl: Vec<DdlChange>,
    #[serde(default)]
    pub ddl_lsn: Vec<Lsn>,
}

impl ChangeSet {
    pub fn validate_ddl_lsn_cardinality(&self) -> std::result::Result<(), String> {
        if self.ddl_lsn.len() == self.ddl.len() {
            return Ok(());
        }
        Err(format!(
            "invalid ChangeSet ddl_lsn length: got {}, expected {} for {} DDL entries",
            self.ddl_lsn.len(),
            self.ddl.len(),
            self.ddl.len()
        ))
    }

    pub fn max_lsn(&self) -> Option<Lsn> {
        self.max_data_lsn()
            .into_iter()
            .chain(self.ddl_lsn.iter().copied())
            .max()
    }

    pub fn max_data_lsn(&self) -> Option<Lsn> {
        self.rows
            .iter()
            .map(|row| row.lsn)
            .chain(self.edges.iter().map(|edge| edge.lsn))
            .chain(self.vectors.iter().map(|vector| vector.lsn))
            .max()
    }

    pub fn data_entry_count(&self) -> usize {
        self.rows.len() + self.edges.len() + self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data_entry_count() == 0 && self.ddl.is_empty()
    }

    pub fn has_create_trigger_ddl(&self) -> bool {
        self.ddl
            .iter()
            .any(|ddl| matches!(ddl, DdlChange::CreateTrigger { .. }))
    }

    pub fn split_at_trigger_bootstrap_barriers(self) -> Vec<ChangeSet> {
        let mut batches = Vec::new();
        let mut current = ChangeSet::default();

        for group in self.split_by_data_lsn() {
            if group.has_create_trigger_ddl() && group.data_entry_count() == 0 {
                if !current.is_empty() {
                    batches.push(std::mem::take(&mut current));
                }
                batches.push(group);
                continue;
            }
            current.rows.extend(group.rows);
            current.edges.extend(group.edges);
            current.vectors.extend(group.vectors);
            current.ddl.extend(group.ddl);
            current.ddl_lsn.extend(group.ddl_lsn);
        }

        if !current.is_empty() || batches.is_empty() {
            batches.push(current);
        }
        batches
    }

    pub fn split_by_data_lsn(self) -> Vec<ChangeSet> {
        let mut groups = BTreeMap::<Lsn, ChangeSet>::new();
        for row in self.rows {
            groups.entry(row.lsn).or_default().rows.push(row);
        }
        for edge in self.edges {
            groups.entry(edge.lsn).or_default().edges.push(edge);
        }
        for vector in self.vectors {
            groups.entry(vector.lsn).or_default().vectors.push(vector);
        }
        let fallback_ddl_lsn = groups.keys().next().copied();
        for (index, ddl) in self.ddl.into_iter().enumerate() {
            let Some(lsn) = self.ddl_lsn.get(index).copied().or(fallback_ddl_lsn) else {
                groups.entry(Lsn(0)).or_default().ddl.push(ddl);
                continue;
            };
            let group = groups.entry(lsn).or_default();
            group.ddl.push(ddl);
            group.ddl_lsn.push(lsn);
        }

        if groups.is_empty() {
            return vec![ChangeSet {
                rows: Vec::new(),
                edges: Vec::new(),
                vectors: Vec::new(),
                ddl: Vec::new(),
                ddl_lsn: Vec::new(),
            }];
        }
        groups.into_values().collect::<Vec<_>>()
    }

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
        let include_event_table =
            |table: &str| directions.is_empty() || (!table.is_empty() && include_dir(table));
        let included_route_sinks = self
            .ddl
            .iter()
            .filter_map(|d| match d {
                DdlChange::CreateRoute { table, sink, .. } if include_event_table(table) => {
                    Some(sink.clone())
                }
                _ => None,
            })
            .collect::<std::collections::HashSet<_>>();
        let include_ddl = |d: &DdlChange| match d {
            DdlChange::CreateTable { name, .. } => include_dir(name),
            DdlChange::DropTable { name } => include_dir(name),
            DdlChange::AlterTable { name, .. } => include_dir(name),
            DdlChange::CreateIndex { table, .. } => include_dir(table),
            DdlChange::DropIndex { table, .. } => include_dir(table),
            DdlChange::CreateTrigger { table, .. } => include_dir(table),
            DdlChange::DropTrigger { .. } => true,
            DdlChange::CreateEventType { table, .. } => include_event_table(table),
            DdlChange::CreateSink { name, .. } => {
                directions.is_empty() || included_route_sinks.contains(name)
            }
            DdlChange::CreateRoute { table, .. } => include_event_table(table),
            DdlChange::DropRoute { table, .. } => include_event_table(table),
        };
        let carry_ddl_lsn = self.ddl_lsn.len() == self.ddl.len();
        let mut ddl = Vec::new();
        let mut ddl_lsn = Vec::new();
        for (index, change) in self.ddl.iter().enumerate() {
            if include_ddl(change) {
                ddl.push(change.clone());
                if carry_ddl_lsn {
                    ddl_lsn.push(self.ddl_lsn[index]);
                }
            }
        }

        ChangeSet {
            rows: self
                .rows
                .iter()
                .filter(|r| include_dir(&r.table))
                .cloned()
                .collect(),
            edges: self.edges.clone(),
            vectors: self
                .vectors
                .iter()
                .filter(|v| include_dir(&v.index.table))
                .cloned()
                .collect(),
            ddl,
            ddl_lsn,
        }
    }
}

pub(crate) fn natural_key_column_for_meta(meta: &TableMeta) -> Option<String> {
    meta.natural_key_column
        .clone()
        .or_else(|| {
            meta.columns
                .iter()
                .find(|column| column.primary_key)
                .map(|column| column.name.clone())
        })
        .or_else(|| {
            meta.columns
                .iter()
                .find(|column| column.name == "id")
                .map(|_| "id".to_string())
        })
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DdlChange {
    CreateTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
        #[serde(default)]
        foreign_keys: Vec<SingleColumnForeignKey>,
        #[serde(default)]
        composite_foreign_keys: Vec<CompositeForeignKey>,
        #[serde(default)]
        composite_unique: Vec<Vec<String>>,
    },
    DropTable {
        name: String,
    },
    AlterTable {
        name: String,
        columns: Vec<(String, String)>,
        constraints: Vec<String>,
        #[serde(default)]
        foreign_keys: Vec<SingleColumnForeignKey>,
        #[serde(default)]
        composite_foreign_keys: Vec<CompositeForeignKey>,
        #[serde(default)]
        composite_unique: Vec<Vec<String>>,
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
    CreateTrigger {
        name: String,
        table: String,
        on_events: Vec<String>,
    },
    DropTrigger {
        name: String,
    },
    CreateEventType {
        name: String,
        trigger: String,
        table: String,
    },
    CreateSink {
        name: String,
        sink_type: String,
        url: Option<String>,
    },
    CreateRoute {
        name: String,
        event_type: String,
        sink: String,
        #[serde(default)]
        table: String,
        where_in: Option<(String, Vec<String>)>,
    },
    DropRoute {
        name: String,
        #[serde(default)]
        table: String,
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
