use crate::sync_types::{DdlChange, NaturalKey};
use contextdb_core::{EdgeType, Lsn, NodeId, Result, RowId, TableName, Value, VectorIndexRef};
use contextdb_graph::GraphStore;
use contextdb_relational::RelationalStore;
use contextdb_tx::{WriteSet, WriteSetApplicator};
use contextdb_vector::VectorStore;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChangeLogEntry {
    RowInsert {
        table: TableName,
        row_id: RowId,
        lsn: Lsn,
    },
    RowDelete {
        table: TableName,
        row_id: RowId,
        natural_key: NaturalKey,
        lsn: Lsn,
    },
    EdgeInsert {
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        lsn: Lsn,
    },
    EdgeDelete {
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        lsn: Lsn,
    },
    VectorInsert {
        index: VectorIndexRef,
        row_id: RowId,
        lsn: Lsn,
    },
    VectorDelete {
        index: VectorIndexRef,
        row_id: RowId,
        lsn: Lsn,
    },
}

impl ChangeLogEntry {
    pub fn lsn(&self) -> Lsn {
        match self {
            ChangeLogEntry::RowInsert { lsn, .. }
            | ChangeLogEntry::RowDelete { lsn, .. }
            | ChangeLogEntry::EdgeInsert { lsn, .. }
            | ChangeLogEntry::EdgeDelete { lsn, .. }
            | ChangeLogEntry::VectorInsert { lsn, .. }
            | ChangeLogEntry::VectorDelete { lsn, .. } => *lsn,
        }
    }
}

pub struct CompositeStore {
    pub relational: Arc<RelationalStore>,
    pub graph: Arc<GraphStore>,
    pub vector: Arc<VectorStore>,
    pub change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    pub ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    accountant: Arc<contextdb_core::MemoryAccountant>,
}

impl CompositeStore {
    pub fn new(
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector: Arc<VectorStore>,
        change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
        ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
        accountant: Arc<contextdb_core::MemoryAccountant>,
    ) -> Self {
        Self {
            relational,
            graph,
            vector,
            change_log,
            ddl_log,
            accountant,
        }
    }

    pub(crate) fn build_change_log_entries(&self, ws: &WriteSet) -> Vec<ChangeLogEntry> {
        let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
        let mut log_entries = Vec::new();

        for (table, row) in &ws.relational_inserts {
            log_entries.push(ChangeLogEntry::RowInsert {
                table: table.clone(),
                row_id: row.row_id,
                lsn,
            });
        }

        for (table, row_id, _) in &ws.relational_deletes {
            let natural_key = self
                .relational
                .tables
                .read()
                .get(table)
                .and_then(|rows| rows.iter().find(|r| r.row_id == *row_id))
                .and_then(|row| {
                    row.values.get("id").cloned().map(|value| NaturalKey {
                        column: "id".to_string(),
                        value,
                    })
                })
                .unwrap_or_else(|| NaturalKey {
                    column: "id".to_string(),
                    value: Value::Int64(row_id.0 as i64),
                });

            log_entries.push(ChangeLogEntry::RowDelete {
                table: table.clone(),
                row_id: *row_id,
                natural_key,
                lsn,
            });
        }

        for entry in &ws.adj_inserts {
            log_entries.push(ChangeLogEntry::EdgeInsert {
                source: entry.source,
                target: entry.target,
                edge_type: entry.edge_type.clone(),
                lsn,
            });
        }

        for (source, edge_type, target, _) in &ws.adj_deletes {
            log_entries.push(ChangeLogEntry::EdgeDelete {
                source: *source,
                target: *target,
                edge_type: edge_type.clone(),
                lsn,
            });
        }

        for (index, row_id, _) in &ws.vector_deletes {
            log_entries.push(ChangeLogEntry::VectorDelete {
                index: index.clone(),
                row_id: *row_id,
                lsn,
            });
        }

        for entry in &ws.vector_inserts {
            log_entries.push(ChangeLogEntry::VectorInsert {
                index: entry.index.clone(),
                row_id: entry.row_id,
                lsn,
            });
        }

        log_entries
    }
}

impl WriteSetApplicator for CompositeStore {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        let log_entries = self.build_change_log_entries(&ws);

        self.relational.apply_inserts(ws.relational_inserts);
        self.relational.apply_deletes(ws.relational_deletes);
        self.graph.apply_inserts(ws.adj_inserts);
        self.graph.apply_deletes(ws.adj_deletes);
        self.vector.apply_changes_with_accountant(
            ws.vector_deletes,
            ws.vector_inserts,
            ws.vector_moves,
            ws.commit_lsn.unwrap_or(Lsn(0)),
            Some(&self.accountant),
        );
        self.change_log.write().extend(log_entries);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        self.relational.new_row_id()
    }
}
