use crate::memory_accounting::MemoryAccountant;
use crate::sync_types::{DdlChange, NaturalKey, natural_key_from_row_values};
use contextdb_core::{
    EdgeType, Lsn, NodeId, Result, RowId, TableMeta, TableName, Value, VectorIndexRef, VersionedRow,
};
use contextdb_graph::GraphStore;
use contextdb_relational::RelationalStore;
use contextdb_relational::store::SyncSourceKind;
use contextdb_tx::{WriteSet, WriteSetApplicator};
use contextdb_vector::VectorStore;
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-table shadow of a table's own `RowInsert`/`RowDelete` change-log
/// entries -- `(lsn, row_id)` pairs, in APPEND ORDER, duplicates preserved
/// (a superseding commit emits a `RowDelete` at the SAME `lsn` as its own
/// `RowInsert`, so one pruned `(table, row_id, lsn)` key can legitimately
/// match two entries -- see `change_entry_references_pruned_version`) --
/// kept in lockstep with `change_log` itself at every choke point that
/// touches it (append here, load-from-disk in `Database::open`, and
/// point-removal in the version-cleanup/retention passes) so a scoped pass
/// can find and count exactly ONE table's own entries without visiting
/// every OTHER table's entries sharing the same global change-log `Vec`.
/// See `change_log_table_index_consistency_tests.rs`.
pub(crate) type ChangeLogTableIndex = HashMap<TableName, Vec<(Lsn, RowId)>>;

/// How many change-log entries of ANY kind (row, edge, or vector) currently
/// name a given commit LSN, maintained in lockstep with `change_log` the
/// same way `ChangeLogTableIndex` is. Lets a scoped pass answer "does any
/// change-log entry still cover this LSN" and "what is the lowest LSN still
/// covered" without visiting the whole log (`BTreeMap` so the floor is a
/// cheap first-key lookup, not a scan).
pub(crate) type ChangeLogLsnRefcounts = BTreeMap<Lsn, u64>;

/// Record newly-appended entries into both aux structures, in the SAME
/// order they land in `change_log` -- called once, right where
/// `change_log` itself is extended, so the two never observe a different
/// view of "what has been appended so far."
pub(crate) fn record_change_log_entries(
    table_index: &mut ChangeLogTableIndex,
    lsn_refcounts: &mut ChangeLogLsnRefcounts,
    entries: &[ChangeLogEntry],
) {
    for entry in entries {
        *lsn_refcounts.entry(entry.lsn()).or_insert(0) += 1;
        if let ChangeLogEntry::RowInsert { table, row_id, lsn }
        | ChangeLogEntry::RowDelete {
            table, row_id, lsn, ..
        } = entry
        {
            table_index
                .entry(table.clone())
                .or_default()
                .push((*lsn, *row_id));
        }
    }
}

pub(crate) type SyncSourceLsnClear = (TableName, RowId);
pub(crate) type SyncSourceLsnSet = (TableName, RowId, Lsn, u8);

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

/// Build the exact log payload for a received-schema stage while its detached
/// projection is still available.  Redb and post-commit memory publication
/// both consume this owned vector; neither may rediscover it from live state.
pub(crate) fn build_received_schema_change_log_entries(
    ws: &WriteSet,
    table_meta: &HashMap<String, TableMeta>,
    deleted_rows: &HashMap<String, HashMap<RowId, VersionedRow>>,
    structurally_dropped_tables: &HashSet<String>,
) -> Vec<ChangeLogEntry> {
    let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
    let mut entries = Vec::new();
    for (table, row) in &ws.relational_inserts {
        entries.push(ChangeLogEntry::RowInsert {
            table: table.clone(),
            row_id: row.row_id,
            lsn,
        });
    }
    for (table, row_id, _) in &ws.relational_deletes {
        // A received DROP TABLE removes this row as part of the old table
        // generation's structure, not as a user-requested row deletion. Do
        // not manufacture outbound row history that the new generation
        // cannot authenticate.
        if structurally_dropped_tables.contains(table) {
            continue;
        }
        let natural_key = deleted_rows
            .get(table)
            .and_then(|rows| rows.get(row_id))
            .and_then(|row| {
                table_meta
                    .get(table)
                    .and_then(|meta| natural_key_from_row_values(meta, &row.values))
            })
            .unwrap_or_else(|| NaturalKey::single("id".to_string(), Value::Int64(row_id.0 as i64)));
        entries.push(ChangeLogEntry::RowDelete {
            table: table.clone(),
            row_id: *row_id,
            natural_key,
            lsn,
        });
    }
    for entry in &ws.adj_inserts {
        entries.push(ChangeLogEntry::EdgeInsert {
            source: entry.source,
            target: entry.target,
            edge_type: entry.edge_type.clone(),
            lsn,
        });
    }
    for (source, edge_type, target, _) in &ws.adj_deletes {
        entries.push(ChangeLogEntry::EdgeDelete {
            source: *source,
            target: *target,
            edge_type: edge_type.clone(),
            lsn,
        });
    }
    for (index, row_id, _) in &ws.vector_deletes {
        // Keep vector cleanup paired with its dropped table's row cleanup:
        // neither is an outbound user deletion.
        if structurally_dropped_tables.contains(&index.table) {
            continue;
        }
        entries.push(ChangeLogEntry::VectorDelete {
            index: index.clone(),
            row_id: *row_id,
            lsn,
        });
    }
    for entry in &ws.vector_inserts {
        entries.push(ChangeLogEntry::VectorInsert {
            index: entry.index.clone(),
            row_id: entry.row_id,
            lsn,
        });
    }
    entries
}

/// Infallibly append an already-prepared log payload and its two derived
/// indexes. Received-schema publication calls this after Redb succeeds,
/// without replaying any data WriteSet into the stores a second time.
pub(crate) fn publish_prepared_change_log_entries(
    change_log: &RwLock<Vec<ChangeLogEntry>>,
    table_index: &RwLock<ChangeLogTableIndex>,
    lsn_refcounts: &RwLock<ChangeLogLsnRefcounts>,
    structurally_dropped_tables: &HashSet<String>,
    entries: Vec<ChangeLogEntry>,
) {
    let mut table_index = table_index.write();
    let mut lsn_refcounts = lsn_refcounts.write();
    let mut change_log = change_log.write();
    if !structurally_dropped_tables.is_empty() {
        change_log.retain(|entry| match entry {
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. } => {
                !structurally_dropped_tables.contains(table)
            }
            ChangeLogEntry::VectorInsert { index, .. }
            | ChangeLogEntry::VectorDelete { index, .. } => {
                !structurally_dropped_tables.contains(&index.table)
            }
            ChangeLogEntry::EdgeInsert { .. } | ChangeLogEntry::EdgeDelete { .. } => true,
        });
        table_index.clear();
        lsn_refcounts.clear();
        record_change_log_entries(&mut table_index, &mut lsn_refcounts, &change_log);
    }
    record_change_log_entries(&mut table_index, &mut lsn_refcounts, &entries);
    change_log.extend(entries);
}

pub struct CompositeStore {
    pub relational: Arc<RelationalStore>,
    pub graph: Arc<GraphStore>,
    pub vector: Arc<VectorStore>,
    pub change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    pub change_log_table_index: Arc<RwLock<ChangeLogTableIndex>>,
    pub change_log_lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
    pub ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    accountant: Arc<MemoryAccountant>,
    apply_phase_pause: Arc<ApplyPhasePause>,
}

#[derive(Debug, Default)]
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

    #[cfg(any(test, feature = "test-seams"))]
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

    pub(crate) fn maybe_pause(&self) {
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector: Arc<VectorStore>,
        change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
        change_log_table_index: Arc<RwLock<ChangeLogTableIndex>>,
        change_log_lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
        ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
        accountant: Arc<MemoryAccountant>,
    ) -> Self {
        Self::new_with_apply_phase_pause(
            relational,
            graph,
            vector,
            change_log,
            change_log_table_index,
            change_log_lsn_refcounts,
            ddl_log,
            accountant,
            Arc::new(ApplyPhasePause::new()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_apply_phase_pause(
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector: Arc<VectorStore>,
        change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
        change_log_table_index: Arc<RwLock<ChangeLogTableIndex>>,
        change_log_lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
        ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
        accountant: Arc<MemoryAccountant>,
        apply_phase_pause: Arc<ApplyPhasePause>,
    ) -> Self {
        Self {
            relational,
            graph,
            vector,
            change_log,
            change_log_table_index,
            change_log_lsn_refcounts,
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
        if let Some(meta) = meta
            && let Some(natural_key) = natural_key_from_row_values(meta, values)
        {
            return natural_key;
        }

        NaturalKey::single("id".to_string(), Value::Int64(row_id.0 as i64))
    }

    fn natural_key_for_row_delete(&self, table: &str, row_id: RowId) -> NaturalKey {
        let meta = self.relational.table_meta.read().get(table).cloned();
        let row_values = self
            .relational
            .live_row_by_id(table, row_id)
            .map(|row| row.values);

        if let (Some(meta), Some(values)) = (meta.as_ref(), row_values.as_ref())
            && let Some(natural_key) = natural_key_from_row_values(meta, values)
        {
            return natural_key;
        }

        NaturalKey::single("id".to_string(), Value::Int64(row_id.0 as i64))
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
            Some(&*self.accountant),
        );
        let (clear_source_lsns, set_source_lsns) = sync_source_lsn_updates(ws);
        if !clear_source_lsns.is_empty() {
            self.relational.clear_sync_source_lsns(clear_source_lsns);
        }
        if !set_source_lsns.is_empty() {
            self.relational.set_sync_source_lsns(
                set_source_lsns
                    .iter()
                    .map(|(table, row_id, lsn, _)| (table.clone(), *row_id, *lsn)),
            );
            self.relational
                .set_sync_source_kinds(set_source_lsns.into_iter().map(
                    |(table, row_id, _, kind)| {
                        (
                            table,
                            row_id,
                            match kind {
                                1 => SyncSourceKind::AcceptedLocal,
                                2 => SyncSourceKind::AcceptedLocalPending,
                                _ => SyncSourceKind::Pulled,
                            },
                        )
                    },
                ));
        }
        record_change_log_entries(
            &mut self.change_log_table_index.write(),
            &mut self.change_log_lsn_refcounts.write(),
            &log_entries,
        );
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
            let kind = ws
                .relational_insert_source_kinds
                .get(table)
                .and_then(|rows| rows.get(&row.row_id))
                .copied()
                .unwrap_or(0);
            set.push((table.clone(), row.row_id, source_lsn, kind));
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

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::TxId;

    #[test]
    fn received_schema_drop_omits_structural_row_and_vector_deletes() {
        let write_set = WriteSet {
            commit_lsn: Some(Lsn(9)),
            relational_deletes: vec![
                ("dropped".to_string(), RowId(1), TxId(1)),
                ("kept".to_string(), RowId(2), TxId(1)),
            ],
            vector_deletes: vec![
                (
                    VectorIndexRef::new("dropped", "embedding"),
                    RowId(1),
                    TxId(1),
                ),
                (VectorIndexRef::new("kept", "embedding"), RowId(2), TxId(1)),
            ],
            ..WriteSet::default()
        };
        let structurally_dropped_tables = HashSet::from(["dropped".to_string()]);

        let entries = build_received_schema_change_log_entries(
            &write_set,
            &HashMap::new(),
            &HashMap::new(),
            &structurally_dropped_tables,
        );

        assert!(!entries.iter().any(
            |entry| matches!(entry, ChangeLogEntry::RowDelete { table, .. } if table == "dropped")
        ));
        assert!(!entries.iter().any(
            |entry| matches!(entry, ChangeLogEntry::VectorDelete { index, .. } if index.table == "dropped")
        ));
        assert!(entries.iter().any(
            |entry| matches!(entry, ChangeLogEntry::RowDelete { table, .. } if table == "kept")
        ));
        assert!(entries.iter().any(
            |entry| matches!(entry, ChangeLogEntry::VectorDelete { index, .. } if index.table == "kept")
        ));

        let historic = vec![
            ChangeLogEntry::RowInsert {
                table: "dropped".to_string(),
                row_id: RowId(1),
                lsn: Lsn(4),
            },
            ChangeLogEntry::VectorInsert {
                index: VectorIndexRef::new("dropped", "embedding"),
                row_id: RowId(1),
                lsn: Lsn(4),
            },
            ChangeLogEntry::RowInsert {
                table: "kept".to_string(),
                row_id: RowId(2),
                lsn: Lsn(5),
            },
        ];
        let change_log = RwLock::new(historic.clone());
        let table_index = RwLock::new(ChangeLogTableIndex::new());
        let lsn_refcounts = RwLock::new(ChangeLogLsnRefcounts::new());
        record_change_log_entries(
            &mut table_index.write(),
            &mut lsn_refcounts.write(),
            &historic,
        );

        publish_prepared_change_log_entries(
            &change_log,
            &table_index,
            &lsn_refcounts,
            &structurally_dropped_tables,
            entries,
        );

        assert!(!change_log.read().iter().any(|entry| match entry {
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. } =>
                table == "dropped",
            ChangeLogEntry::VectorInsert { index, .. }
            | ChangeLogEntry::VectorDelete { index, .. } => index.table == "dropped",
            _ => false,
        }));
        assert_eq!(
            table_index.read().get("dropped"),
            None,
            "retired generation coordinates leave the auxiliary index too"
        );
        assert!(
            table_index.read().contains_key("kept"),
            "unaffected table history and newly published deletes remain indexed"
        );
    }
}
