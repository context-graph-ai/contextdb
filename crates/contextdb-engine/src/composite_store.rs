use crate::sync_types::{DdlChange, NaturalKey, natural_key_column_for_meta};
use contextdb_core::{
    EdgeType, Lsn, NodeId, Result, RowId, TableMeta, TableName, Value, VectorIndexRef, VersionedRow,
};
use contextdb_graph::GraphStore;
use contextdb_relational::RelationalStore;
use contextdb_tx::{WriteSet, WriteSetApplicator};
use contextdb_vector::VectorStore;
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) type SyncSourceLsnClear = (TableName, RowId);
pub(crate) type SyncSourceLsnSet = (TableName, RowId, Lsn);

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
        if ws.relational_deletes.is_empty() {
            return self.build_change_log_entries_with_snapshots(ws, None, None);
        }
        let table_meta = self.relational.table_meta.read().clone();
        let deleted_rows = self.deleted_rows_snapshot_for_write_set(ws);
        self.build_change_log_entries_with_snapshots(ws, Some(&table_meta), deleted_rows.as_ref())
    }

    pub(crate) fn build_change_log_entries_with_snapshots(
        &self,
        ws: &WriteSet,
        table_meta_snapshot: Option<&HashMap<String, TableMeta>>,
        deleted_rows_snapshot: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
    ) -> Vec<ChangeLogEntry> {
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
            let natural_key = self.natural_key_for_row_delete_with_snapshots(
                table,
                *row_id,
                table_meta_snapshot,
                deleted_rows_snapshot,
            );

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

    pub(crate) fn deleted_rows_snapshot_for_write_set(
        &self,
        ws: &WriteSet,
    ) -> Option<HashMap<String, HashMap<RowId, VersionedRow>>> {
        if ws.relational_deletes.is_empty() {
            return None;
        }
        let mut seen = HashSet::<(String, RowId)>::new();
        let mut keys = Vec::with_capacity(ws.relational_deletes.len());
        for (table, row_id, _) in &ws.relational_deletes {
            if seen.insert((table.clone(), *row_id)) {
                keys.push((table.clone(), *row_id));
            }
        }
        Some(self.relational.live_rows_by_id(&keys))
    }

    fn natural_key_for_row_delete_with_snapshots(
        &self,
        table: &str,
        row_id: RowId,
        table_meta_snapshot: Option<&HashMap<String, TableMeta>>,
        deleted_rows_snapshot: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
    ) -> NaturalKey {
        if let Some(row) = deleted_rows_snapshot
            .and_then(|by_table| by_table.get(table))
            .and_then(|by_row| by_row.get(&row_id))
        {
            return self.natural_key_for_row_values(
                row_id,
                &row.values,
                table_meta_snapshot.and_then(|by_table| by_table.get(table)),
            );
        }

        self.natural_key_for_row_delete(table, row_id)
    }

    fn natural_key_for_row_values(
        &self,
        row_id: RowId,
        values: &HashMap<String, Value>,
        meta: Option<&TableMeta>,
    ) -> NaturalKey {
        if let Some(column) = meta.and_then(natural_key_column_for_meta)
            && let Some(value) = values.get(&column).cloned()
        {
            return NaturalKey { column, value };
        }

        NaturalKey {
            column: "id".to_string(),
            value: Value::Int64(row_id.0 as i64),
        }
    }

    fn natural_key_for_row_delete(&self, table: &str, row_id: RowId) -> NaturalKey {
        let meta = self.relational.table_meta.read().get(table).cloned();
        let key_col = meta.as_ref().and_then(natural_key_column_for_meta);
        let row_values = self
            .relational
            .live_row_by_id(table, row_id)
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

    pub(crate) fn apply_exact(&self, ws: &WriteSet) -> Result<()> {
        let log_entries = self.build_change_log_entries(ws);
        self.apply_exact_with_log_entries(ws, log_entries)
    }

    pub(crate) fn apply_exact_with_log_entries(
        &self,
        ws: &WriteSet,
        log_entries: Vec<ChangeLogEntry>,
    ) -> Result<()> {
        if ws.relational_deletes.is_empty() || ws.relational_inserts.is_empty() {
            self.relational.apply_deletes_ref(&ws.relational_deletes);
            self.relational.apply_inserts_ref(&ws.relational_inserts);
        } else {
            self.relational
                .apply_replacements_ref(&ws.relational_deletes, &ws.relational_inserts);
        }
        self.apply_phase_pause.maybe_pause();
        self.graph.apply_deletes_ref(&ws.adj_deletes);
        self.graph.apply_inserts_ref(&ws.adj_inserts);
        self.vector.apply_changes_with_accountant_ref(
            &ws.vector_deletes,
            &ws.vector_inserts,
            &ws.vector_moves,
            ws.commit_lsn.unwrap_or(Lsn(0)),
            Some(&self.accountant),
        );
        let (clear_source_lsns, set_source_lsns) = sync_source_lsn_updates(ws);
        if !clear_source_lsns.is_empty() {
            self.relational.clear_sync_source_lsns(clear_source_lsns);
        }
        if !set_source_lsns.is_empty() {
            self.relational.set_sync_source_lsns(set_source_lsns);
        }
        self.change_log.write().extend(log_entries);
        Ok(())
    }
}

pub(crate) fn sync_source_lsn_updates(
    ws: &WriteSet,
) -> (Vec<SyncSourceLsnClear>, Vec<SyncSourceLsnSet>) {
    let mut clear = ws
        .relational_deletes
        .iter()
        .map(|(table, row_id, _)| (table.clone(), *row_id))
        .collect::<Vec<_>>();
    let mut set = Vec::new();
    for (table, row) in &ws.relational_inserts {
        if let Some(source_lsn) = ws
            .relational_insert_source_lsns
            .get(table)
            .and_then(|by_row| by_row.get(&row.row_id))
            .copied()
        {
            set.push((table.clone(), row.row_id, source_lsn));
        } else {
            clear.push((table.clone(), row.row_id));
        }
    }
    (clear, set)
}

impl WriteSetApplicator for CompositeStore {
    fn apply(&self, ws: &WriteSet) -> Result<()> {
        self.apply_exact(ws)
    }

    fn new_row_id(&self) -> RowId {
        self.relational.new_row_id()
    }
}
