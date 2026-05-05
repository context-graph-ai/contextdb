use crate::sync_types::{DdlChange, NaturalKey, natural_key_column_for_meta};
use contextdb_core::{EdgeType, Lsn, NodeId, Result, RowId, TableName, Value, VectorIndexRef};
use contextdb_graph::GraphStore;
use contextdb_relational::RelationalStore;
use contextdb_tx::{WriteSet, WriteSetApplicator};
use contextdb_vector::VectorStore;
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    apply_phase_pause: Arc<ApplyPhasePause>,
}

#[derive(Debug)]
pub(crate) struct ApplyPhasePause {
    state: Mutex<ApplyPhasePauseState>,
    waiters: Condvar,
}

#[derive(Debug, Default)]
struct ApplyPhasePauseState {
    generation: u64,
    armed: bool,
    reached: bool,
    released: bool,
}

impl ApplyPhasePause {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(ApplyPhasePauseState::default()),
            waiters: Condvar::new(),
        }
    }

    pub(crate) fn arm(&self) -> u64 {
        let mut state = self.state.lock();
        state.generation = state.generation.saturating_add(1);
        state.armed = true;
        state.reached = false;
        state.released = false;
        self.waiters.notify_all();
        state.generation
    }

    pub(crate) fn wait_until_reached(&self, generation: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock();
        while state.generation == generation && state.armed && !state.reached {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            self.waiters
                .wait_for(&mut state, deadline.saturating_duration_since(now));
        }
        state.generation == generation && state.reached
    }

    pub(crate) fn release(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.generation == generation && state.armed {
            state.released = true;
            self.waiters.notify_all();
        }
    }

    fn maybe_pause(&self) {
        let mut state = self.state.lock();
        if !state.armed || state.released {
            return;
        }
        state.reached = true;
        self.waiters.notify_all();
        while state.armed && !state.released {
            self.waiters.wait(&mut state);
        }
        state.armed = false;
        state.reached = false;
        state.released = false;
        self.waiters.notify_all();
    }
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
        Self::new_with_apply_phase_pause(
            relational,
            graph,
            vector,
            change_log,
            ddl_log,
            accountant,
            Arc::new(ApplyPhasePause::new()),
        )
    }

    pub(crate) fn new_with_apply_phase_pause(
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector: Arc<VectorStore>,
        change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
        ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
        accountant: Arc<contextdb_core::MemoryAccountant>,
        apply_phase_pause: Arc<ApplyPhasePause>,
    ) -> Self {
        Self {
            relational,
            graph,
            vector,
            change_log,
            ddl_log,
            accountant,
            apply_phase_pause,
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
            let natural_key = self.natural_key_for_row_delete(table, *row_id);

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

    fn natural_key_for_row_delete(&self, table: &str, row_id: RowId) -> NaturalKey {
        let meta = self.relational.table_meta.read().get(table).cloned();
        let key_col = meta.as_ref().and_then(natural_key_column_for_meta);
        let row_values = self
            .relational
            .tables
            .read()
            .get(table)
            .and_then(|rows| {
                rows.iter()
                    .rev()
                    .find(|row| row.row_id == row_id && row.deleted_tx.is_none())
                    .cloned()
            })
            .map(|row| row.values);

        if let (Some(column), Some(values)) = (key_col, row_values)
            && let Some(value) = values.get(&column).cloned()
        {
            return NaturalKey { column, value };
        }

        NaturalKey {
            column: "id".to_string(),
            value: Value::Int64(row_id.0 as i64),
        }
    }

    pub(crate) fn apply_exact(&self, ws: WriteSet) -> Result<()> {
        let log_entries = self.build_change_log_entries(&ws);

        self.relational.apply_deletes(ws.relational_deletes);
        self.relational.apply_inserts(ws.relational_inserts);
        self.apply_phase_pause.maybe_pause();
        self.graph.apply_deletes(ws.adj_deletes);
        self.graph.apply_inserts(ws.adj_inserts);
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
}

impl WriteSetApplicator for CompositeStore {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        self.apply_exact(ws)
    }

    fn new_row_id(&self) -> RowId {
        self.relational.new_row_id()
    }
}
