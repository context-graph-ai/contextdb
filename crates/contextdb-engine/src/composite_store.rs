use crate::sync_types::{DdlChange, NaturalKey};
use contextdb_core::{EdgeType, NodeId, Result, RowId, TableMeta, TableName, Value};
use contextdb_graph::GraphStore;
use contextdb_relational::RelationalStore;
use contextdb_tx::{WriteSet, WriteSetApplicator};
use contextdb_vector::VectorStore;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum ChangeLogEntry {
    RowInsert {
        table: TableName,
        row_id: RowId,
        lsn: u64,
    },
    RowDelete {
        table: TableName,
        row_id: RowId,
        natural_key: NaturalKey,
        lsn: u64,
    },
    EdgeInsert {
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        lsn: u64,
    },
    EdgeDelete {
        source: NodeId,
        target: NodeId,
        edge_type: EdgeType,
        lsn: u64,
    },
    VectorInsert {
        row_id: RowId,
        lsn: u64,
    },
    VectorDelete {
        row_id: RowId,
        lsn: u64,
    },
}

impl ChangeLogEntry {
    pub fn lsn(&self) -> u64 {
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
    pub change_log: RwLock<Vec<ChangeLogEntry>>,
    pub ddl_log: RwLock<Vec<(u64, DdlChange)>>,
}

impl CompositeStore {
    pub fn new(
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector: Arc<VectorStore>,
    ) -> Self {
        Self {
            relational,
            graph,
            vector,
            change_log: RwLock::new(Vec::new()),
            ddl_log: RwLock::new(Vec::new()),
        }
    }

    pub fn log_create_table(&self, name: &str, meta: &TableMeta, lsn: u64) {
        let columns = meta
            .columns
            .iter()
            .map(|col| {
                let ty = match col.column_type {
                    contextdb_core::ColumnType::Integer => "INTEGER".to_string(),
                    contextdb_core::ColumnType::Real => "REAL".to_string(),
                    contextdb_core::ColumnType::Text => "TEXT".to_string(),
                    contextdb_core::ColumnType::Boolean => "BOOLEAN".to_string(),
                    contextdb_core::ColumnType::Json => "JSON".to_string(),
                    contextdb_core::ColumnType::Uuid => "UUID".to_string(),
                    contextdb_core::ColumnType::Vector(dim) => format!("VECTOR({dim})"),
                };
                (col.name.clone(), ty)
            })
            .collect();

        let mut constraints = Vec::new();
        if meta.immutable {
            constraints.push("IMMUTABLE".to_string());
        }
        if let Some(sm) = &meta.state_machine {
            let states = sm
                .transitions
                .iter()
                .map(|(from, to)| format!("{from} -> [{}]", to.join(", ")))
                .collect::<Vec<_>>()
                .join(", ");
            constraints.push(format!("STATE MACHINE ({}: {})", sm.column, states));
        }
        if !meta.dag_edge_types.is_empty() {
            let edge_types = meta
                .dag_edge_types
                .iter()
                .map(|edge_type| format!("'{edge_type}'"))
                .collect::<Vec<_>>()
                .join(", ");
            constraints.push(format!("DAG({edge_types})"));
        }

        self.ddl_log.write().push((
            lsn,
            DdlChange::CreateTable {
                name: name.to_string(),
                columns,
                constraints,
            },
        ));
    }

    pub fn log_drop_table(&self, _name: &str, _lsn: u64) {}
}

impl WriteSetApplicator for CompositeStore {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        let lsn = ws.commit_lsn.unwrap_or(0);
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
                    value: Value::Int64(*row_id as i64),
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

        for entry in &ws.vector_inserts {
            log_entries.push(ChangeLogEntry::VectorInsert {
                row_id: entry.row_id,
                lsn,
            });
        }

        for (row_id, _) in &ws.vector_deletes {
            log_entries.push(ChangeLogEntry::VectorDelete {
                row_id: *row_id,
                lsn,
            });
        }

        self.relational.apply_inserts(ws.relational_inserts);
        self.relational.apply_deletes(ws.relational_deletes);
        self.graph.apply_inserts(ws.adj_inserts);
        self.graph.apply_deletes(ws.adj_deletes);
        self.vector.apply_inserts(ws.vector_inserts);
        self.vector.apply_deletes(ws.vector_deletes);
        self.change_log.write().extend(log_entries);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        self.relational.new_row_id()
    }
}
