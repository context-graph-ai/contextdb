use crate::memory_accounting::MemoryAccountant;
use crate::sync_types::{DdlChange, NaturalKey, natural_key_from_row_values};
use contextdb_core::{
    ColumnDef, ColumnType, EdgeType, Error, Lsn, NodeId, Result, RowId, SnapshotId, TableMeta,
    TableName, TxId, Value, VectorIndexRef, VectorPartitionDeclarationIssue, VectorPartitionKey,
    VersionedRow,
};
use contextdb_graph::GraphStore;
use contextdb_relational::RelationalStore;
use contextdb_relational::store::SyncSourceKind;
use contextdb_tx::{WriteSet, WriteSetApplicator};
use contextdb_vector::{
    PartitionedVectorDelete, PartitionedVectorEntry, PartitionedVectorMove,
    PreparedPartitionedVectorBatch, VectorStore,
};
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

/// The exact registered-snapshot set captured by Database while its active
/// removal guard is held, staged under the commit LSN that the erased store
/// applicator will receive. Absence means "no retirement authority"; a
/// present empty vector means the guard proved that no active snapshot needs
/// an emptied maintained partition.
pub(crate) type VectorPartitionSnapshotStageRegistry = Arc<Mutex<HashMap<Lsn, Arc<[SnapshotId]>>>>;

/// The exact row/declaration pair from which one local partition identity was
/// derived. This is deliberately an engine sidecar: neither `VectorEntry` nor
/// any synchronization payload acquires a partition key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedVectorPartitionBinding {
    pub(crate) row: VersionedRow,
    /// The table declaration the key columns' types were read from, so the
    /// identity can be re-derived from this binding alone.
    pub(crate) table_meta: TableMeta,
    pub(crate) declaration: ColumnDef,
    pub(crate) partition_key: VectorPartitionKey,
    pub(crate) vector_created_tx: contextdb_core::TxId,
    pub(crate) vector_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq)]
// Inline bindings deliberately retain the exact pre-durability rows and
// declarations needed to apply one validated vector mutation atomically.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PreparedVectorPartitionMutation {
    Delete {
        index: VectorIndexRef,
        before: PreparedVectorPartitionBinding,
        deleted_tx: contextdb_core::TxId,
    },
    Insert {
        index: VectorIndexRef,
        after: PreparedVectorPartitionBinding,
        entry: contextdb_core::VectorEntry,
    },
    Move {
        index: VectorIndexRef,
        before: PreparedVectorPartitionBinding,
        after: PreparedVectorPartitionBinding,
        moved_tx: contextdb_core::TxId,
    },
}

/// One pre-durability projection consumed by both Redb and the in-memory
/// vector registry. The vector-store token contains the cap/membership
/// validation result; `mutations` retains the exact accepted rows and schema
/// declarations needed by persistence.
pub(crate) struct PreparedVectorPartitionMutationBatch {
    pub(crate) commit_lsn: Lsn,
    pub(crate) mutations: Vec<PreparedVectorPartitionMutation>,
    vector_publication: Option<PreparedPartitionedVectorBatch>,
    memberships: contextdb_relational::store::PreparedMembershipBatch,
    retention_expiries: Vec<contextdb_relational::store::PreparedRetentionExpiry>,
}

impl PreparedVectorPartitionMutationBatch {
    pub(crate) fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

/// The single row-to-partition encoder used by ordinary commits and complete
/// received-schema replacement. The core key supplies the durable canonical
/// byte encoding; this function resolves the declared columns in order and
/// stores only components of each key column's declared type, so a stored
/// key and a predicate literal meet under the same `=` the row filter
/// applies. A key value no component of the declared type is `=`-equal to
/// is refused rather than stored as a partition no query can name.
pub(crate) fn vector_partition_key_for_row(
    index: &VectorIndexRef,
    meta: &TableMeta,
    declaration: &ColumnDef,
    row: &VersionedRow,
) -> Result<VectorPartitionKey> {
    if !matches!(&declaration.column_type, ColumnType::Vector(_)) {
        return Err(Error::InvalidVectorPartitionDeclaration {
            index: index.clone(),
            issue: VectorPartitionDeclarationIssue::RequiresVectorColumn,
        });
    }
    let Some(columns) = declaration.partition_key_columns.as_ref() else {
        return Ok(VectorPartitionKey::unpartitioned());
    };
    if columns.is_empty() {
        return Err(Error::InvalidVectorPartitionDeclaration {
            index: index.clone(),
            issue: VectorPartitionDeclarationIssue::EmptyPartitionKey,
        });
    }
    let mut values = Vec::with_capacity(columns.len());
    for column in columns {
        let Some(value) = row.values.get(column) else {
            return Err(Error::InvalidVectorPartitionDeclaration {
                index: index.clone(),
                issue: VectorPartitionDeclarationIssue::UnknownPartitionKeyColumn,
            });
        };
        if matches!(value, Value::Null) {
            return Err(Error::InvalidVectorPartitionDeclaration {
                index: index.clone(),
                issue: VectorPartitionDeclarationIssue::NullablePartitionKeyColumn,
            });
        }
        let Some(key_column) = meta
            .columns
            .iter()
            .find(|candidate| &candidate.name == column)
        else {
            return Err(Error::InvalidVectorPartitionDeclaration {
                index: index.clone(),
                issue: VectorPartitionDeclarationIssue::UnknownPartitionKeyColumn,
            });
        };
        match crate::executor::partition_key_literal(&key_column.column_type, value) {
            crate::executor::PartitionKeyLiteral::Exact(component) => values.push(component),
            // The declaration is fine; the VALUE written to this key column
            // is not of the type it declares. That is a refusal of the write,
            // named by column and never by value, and it is deliberately not
            // reported as a declaration problem the developer would search a
            // sound `CREATE TABLE` for. Ordinary writes and synced rows both
            // arrive here, so both get this one refusal.
            crate::executor::PartitionKeyLiteral::NoPartition
            | crate::executor::PartitionKeyLiteral::Unresolved
            | crate::executor::PartitionKeyLiteral::Unbound => {
                return Err(Error::VectorPartitionKeyValueTypeMismatch {
                    index: index.clone(),
                    column: column.clone(),
                });
            }
        }
    }
    VectorPartitionKey::from_values(&values).ok_or_else(|| {
        Error::InvalidVectorPartitionDeclaration {
            index: index.clone(),
            issue: VectorPartitionDeclarationIssue::UnsupportedPartitionKeyColumnType,
        }
    })
}

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

pub(crate) const RETENTION_EXPIRY_PREFIX: &str = "retention_expiry.v1.";

pub(crate) fn retention_expiry_key(table: &str, row: RowId, created: TxId) -> String {
    format!(
        "{RETENTION_EXPIRY_PREFIX}{}:{table}:{}:{}",
        table.len(),
        row.0,
        created.0
    )
}

fn is_retention_expiry(ws: &WriteSet) -> bool {
    ws.config_writes
        .iter()
        .any(|(key, _)| key.starts_with(RETENTION_EXPIRY_PREFIX))
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
    pub(crate) local_erasure_stages: crate::database::discard::StageRegistry,
    pub relational: Arc<RelationalStore>,
    pub graph: Arc<GraphStore>,
    pub vector: Arc<VectorStore>,
    pub change_log: Arc<RwLock<Vec<ChangeLogEntry>>>,
    pub change_log_table_index: Arc<RwLock<ChangeLogTableIndex>>,
    pub change_log_lsn_refcounts: Arc<RwLock<ChangeLogLsnRefcounts>>,
    pub ddl_log: Arc<RwLock<Vec<(Lsn, DdlChange)>>>,
    vector_partition_snapshot_stages: VectorPartitionSnapshotStageRegistry,
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

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn wait_until_reached_blocking(&self, generation: u64) -> bool {
        let mut state = self.state.lock();
        while state.generation == generation && state.armed && !state.reached {
            self.waiters.wait(&mut state);
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
            Arc::new(Mutex::new(HashMap::new())),
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
        vector_partition_snapshot_stages: VectorPartitionSnapshotStageRegistry,
        accountant: Arc<MemoryAccountant>,
        apply_phase_pause: Arc<ApplyPhasePause>,
    ) -> Self {
        Self {
            local_erasure_stages: Arc::new(Mutex::new(HashMap::new())),
            relational,
            graph,
            vector,
            change_log,
            change_log_table_index,
            change_log_lsn_refcounts,
            ddl_log,
            vector_partition_snapshot_stages,
            accountant,
            apply_phase_pause,
        }
    }

    pub(crate) fn build_change_log_entries_with_snapshots(
        &self,
        ws: &WriteSet,
        table_meta_snapshot: Option<&HashMap<String, TableMeta>>,
        deleted_rows_snapshot: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
    ) -> Vec<ChangeLogEntry> {
        // Local expiry is not an outbound user deletion.
        if is_retention_expiry(ws) {
            return Vec::new();
        }
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

    fn vector_table_meta<'meta>(
        table_meta: &'meta HashMap<String, TableMeta>,
        index: &VectorIndexRef,
    ) -> Result<&'meta TableMeta> {
        table_meta
            .get(&index.table)
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })
    }

    fn vector_declaration(
        table_meta: &HashMap<String, TableMeta>,
        index: &VectorIndexRef,
    ) -> Result<ColumnDef> {
        table_meta
            .get(&index.table)
            .and_then(|meta| {
                meta.columns
                    .iter()
                    .find(|column| column.name == index.column)
            })
            .filter(|column| matches!(&column.column_type, ColumnType::Vector(_)))
            .cloned()
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })
    }

    fn validate_registered_vector_declaration(
        &self,
        index: &VectorIndexRef,
        declaration: &ColumnDef,
    ) -> Result<()> {
        let expected =
            crate::database::vector_index_layout_from_column(declaration).ok_or_else(|| {
                Error::InvalidVectorPartitionDeclaration {
                    index: index.clone(),
                    issue: VectorPartitionDeclarationIssue::RequiresVectorColumn,
                }
            })?;
        if self.vector.index_layout(index)? != expected {
            return Err(Error::Other(format!(
                "registered vector layout does not match the durable declaration for {}.{}",
                index.table, index.column
            )));
        }
        Ok(())
    }

    fn old_accepted_row(
        &self,
        table: &str,
        row_id: RowId,
        deleted_rows: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
    ) -> Option<VersionedRow> {
        deleted_rows
            .and_then(|by_table| by_table.get(table))
            .and_then(|by_row| by_row.get(&row_id))
            .cloned()
            .or_else(|| self.relational.live_row_by_id(table, row_id))
    }

    fn current_vector_for_partition(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
    ) -> Result<Option<contextdb_core::VectorEntry>> {
        let Some(current_key) = self.vector.current_partition_for_row(index, row_id) else {
            return Ok(None);
        };
        if &current_key != partition_key {
            return Err(Error::Other(format!(
                "vector partition membership disagrees with the accepted row for {}.{}",
                index.table, index.column
            )));
        }
        self.vector
            .ensure_raw_partition_loaded(index, partition_key)?;
        Ok(self.vector.live_entry_for_row_in_partition(
            index,
            partition_key,
            row_id,
            SnapshotId::from_raw_wire(u64::MAX),
        ))
    }

    fn prepared_binding(
        index: &VectorIndexRef,
        meta: &TableMeta,
        declaration: ColumnDef,
        row: VersionedRow,
        vector_created_tx: contextdb_core::TxId,
        vector_lsn: Lsn,
    ) -> Result<PreparedVectorPartitionBinding> {
        let partition_key = vector_partition_key_for_row(index, meta, &declaration, &row)?;
        Ok(PreparedVectorPartitionBinding {
            row,
            table_meta: meta.clone(),
            declaration,
            partition_key,
            vector_created_tx,
            vector_lsn,
        })
    }

    /// Resolve every vector write against the finalized rows and declaration,
    /// then ask the vector store to validate the complete keyed batch without
    /// mutating it. A relational key-only replacement synthesizes a local
    /// move for each unchanged live vector column.
    pub(crate) fn prepare_vector_partition_mutations(
        &self,
        ws: &WriteSet,
        table_meta: &HashMap<String, TableMeta>,
        deleted_rows: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
    ) -> Result<PreparedVectorPartitionMutationBatch> {
        self.prepare_vector_partition_mutations_with_reclamation(
            ws,
            table_meta,
            deleted_rows,
            HashSet::new(),
            None,
        )
    }

    /// Database supplies only states it has proved reclaimable while holding
    /// its snapshot-removal guard through durable and memory publication.
    /// This layer never infers that proof from current rows.
    pub(crate) fn prepare_vector_partition_mutations_with_reclaimable_partitions(
        &self,
        ws: &WriteSet,
        table_meta: &HashMap<String, TableMeta>,
        deleted_rows: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
        reclaimable_partitions: HashSet<contextdb_vector::VectorPartitionRef>,
    ) -> Result<PreparedVectorPartitionMutationBatch> {
        self.prepare_vector_partition_mutations_with_reclamation(
            ws,
            table_meta,
            deleted_rows,
            reclaimable_partitions,
            None,
        )
    }

    pub(crate) fn prepare_vector_partition_mutations_with_registered_snapshots(
        &self,
        ws: &WriteSet,
        table_meta: &HashMap<String, TableMeta>,
        deleted_rows: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
        registered_snapshots: &[SnapshotId],
    ) -> Result<PreparedVectorPartitionMutationBatch> {
        self.prepare_vector_partition_mutations_with_reclamation(
            ws,
            table_meta,
            deleted_rows,
            HashSet::new(),
            Some(registered_snapshots),
        )
    }

    pub(crate) fn take_vector_partition_snapshot_stage(
        &self,
        commit_lsn: Option<Lsn>,
    ) -> Option<Arc<[SnapshotId]>> {
        commit_lsn.and_then(|lsn| self.vector_partition_snapshot_stages.lock().remove(&lsn))
    }

    fn prepare_vector_partition_mutations_with_reclamation(
        &self,
        ws: &WriteSet,
        table_meta: &HashMap<String, TableMeta>,
        deleted_rows: Option<&HashMap<String, HashMap<RowId, VersionedRow>>>,
        reclaimable_partitions: HashSet<contextdb_vector::VectorPartitionRef>,
        registered_snapshots: Option<&[SnapshotId]>,
    ) -> Result<PreparedVectorPartitionMutationBatch> {
        let commit_lsn = ws.commit_lsn.unwrap_or(Lsn(0));
        let new_rows = ws
            .relational_inserts
            .iter()
            .map(|(table, row)| ((table.clone(), row.row_id), row.clone()))
            .collect::<HashMap<_, _>>();
        let accepted_new_row = |table: &str, row_id: RowId| {
            new_rows
                .get(&(table.to_owned(), row_id))
                .cloned()
                .or_else(|| self.relational.live_row_by_id(table, row_id))
        };

        let mut mutations = Vec::new();
        let mut keyed_deletes = Vec::with_capacity(ws.vector_deletes.len());
        let mut keyed_inserts = Vec::with_capacity(ws.vector_inserts.len());
        let mut keyed_moves = Vec::with_capacity(ws.vector_moves.len());
        let mut explicit_touches = HashSet::<(VectorIndexRef, RowId)>::new();
        let mut checked_declarations = HashSet::<VectorIndexRef>::new();

        for (index, row_id, deleted_tx) in &ws.vector_deletes {
            let meta = Self::vector_table_meta(table_meta, index)?;
            let declaration = Self::vector_declaration(table_meta, index)?;
            if checked_declarations.insert(index.clone()) {
                self.validate_registered_vector_declaration(index, &declaration)?;
            }
            let row = self
                .old_accepted_row(&index.table, *row_id, deleted_rows)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "accepted source row is missing for vector delete on {}.{}",
                        index.table, index.column
                    ))
                })?;
            let partition_key = vector_partition_key_for_row(index, meta, &declaration, &row)?;
            let vector = self
                .current_vector_for_partition(index, &partition_key, *row_id)?
                .ok_or_else(|| Error::NotFound(format!("vector row {row_id}")))?;
            let before = Self::prepared_binding(
                index,
                meta,
                declaration,
                row,
                vector.created_tx,
                vector.lsn,
            )?;
            keyed_deletes.push(PartitionedVectorDelete::new(
                index.clone(),
                before.partition_key.clone(),
                *row_id,
                *deleted_tx,
            ));
            mutations.push(PreparedVectorPartitionMutation::Delete {
                index: index.clone(),
                before,
                deleted_tx: *deleted_tx,
            });
            explicit_touches.insert((index.clone(), *row_id));
        }

        for entry in &ws.vector_inserts {
            let meta = Self::vector_table_meta(table_meta, &entry.index)?;
            let declaration = Self::vector_declaration(table_meta, &entry.index)?;
            if checked_declarations.insert(entry.index.clone()) {
                self.validate_registered_vector_declaration(&entry.index, &declaration)?;
            }
            let row = accepted_new_row(&entry.index.table, entry.row_id).ok_or_else(|| {
                Error::Other(format!(
                    "accepted destination row is missing for vector insert on {}.{}",
                    entry.index.table, entry.index.column
                ))
            })?;
            let after = Self::prepared_binding(
                &entry.index,
                meta,
                declaration,
                row,
                entry.created_tx,
                entry.lsn,
            )?;
            keyed_inserts.push(PartitionedVectorEntry::new(
                after.partition_key.clone(),
                entry.clone(),
            ));
            mutations.push(PreparedVectorPartitionMutation::Insert {
                index: entry.index.clone(),
                after,
                entry: entry.clone(),
            });
            explicit_touches.insert((entry.index.clone(), entry.row_id));
        }

        for (index, old_row_id, new_row_id, moved_tx) in &ws.vector_moves {
            let meta = Self::vector_table_meta(table_meta, index)?;
            let declaration = Self::vector_declaration(table_meta, index)?;
            if checked_declarations.insert(index.clone()) {
                self.validate_registered_vector_declaration(index, &declaration)?;
            }
            let old_row = self
                .old_accepted_row(&index.table, *old_row_id, deleted_rows)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "accepted source row is missing for vector move on {}.{}",
                        index.table, index.column
                    ))
                })?;
            let new_row = accepted_new_row(&index.table, *new_row_id).ok_or_else(|| {
                Error::Other(format!(
                    "accepted destination row is missing for vector move on {}.{}",
                    index.table, index.column
                ))
            })?;
            let source_key = vector_partition_key_for_row(index, meta, &declaration, &old_row)?;
            let vector = self
                .current_vector_for_partition(index, &source_key, *old_row_id)?
                .ok_or_else(|| Error::NotFound(format!("vector row {old_row_id}")))?;
            let before = Self::prepared_binding(
                index,
                meta,
                declaration.clone(),
                old_row,
                vector.created_tx,
                vector.lsn,
            )?;
            let after =
                Self::prepared_binding(index, meta, declaration, new_row, *moved_tx, commit_lsn)?;
            keyed_moves.push(PartitionedVectorMove::new(
                index.clone(),
                before.partition_key.clone(),
                after.partition_key.clone(),
                *old_row_id,
                *new_row_id,
                *moved_tx,
            ));
            mutations.push(PreparedVectorPartitionMutation::Move {
                index: index.clone(),
                before,
                after,
                moved_tx: *moved_tx,
            });
            explicit_touches.insert((index.clone(), *old_row_id));
            explicit_touches.insert((index.clone(), *new_row_id));
        }

        // A row-only replacement has no new vector payload. Advance its local
        // vector owner binding even when the partition key stays the same:
        // later delete/move preparation names the newly accepted row version.
        for (table, new_row) in &ws.relational_inserts {
            let Some(old_row) = deleted_rows
                .and_then(|by_table| by_table.get(table))
                .and_then(|by_row| by_row.get(&new_row.row_id))
                .cloned()
                .or_else(|| self.relational.live_row_by_id(table, new_row.row_id))
            else {
                continue;
            };
            let Some(meta) = table_meta.get(table) else {
                continue;
            };
            for declaration in meta
                .columns
                .iter()
                .filter(|column| matches!(&column.column_type, ColumnType::Vector(_)))
            {
                let index = VectorIndexRef::new(table.clone(), declaration.name.clone());
                if explicit_touches.contains(&(index.clone(), new_row.row_id)) {
                    continue;
                }
                let old_key = vector_partition_key_for_row(&index, meta, declaration, &old_row)?;
                let new_key = vector_partition_key_for_row(&index, meta, declaration, new_row)?;
                if old_key == new_key
                    && old_row.created_tx == new_row.created_tx
                    && old_row.lsn == new_row.lsn
                {
                    continue;
                }
                if checked_declarations.insert(index.clone()) {
                    self.validate_registered_vector_declaration(&index, declaration)?;
                }
                let Some(vector) =
                    self.current_vector_for_partition(&index, &old_key, new_row.row_id)?
                else {
                    continue;
                };
                let before = Self::prepared_binding(
                    &index,
                    meta,
                    declaration.clone(),
                    old_row.clone(),
                    vector.created_tx,
                    vector.lsn,
                )?;
                // No vector payload arrived, but the local move versions the
                // unchanged bytes at this commit so old and new snapshots keep
                // disjoint source/destination memberships.
                let after = Self::prepared_binding(
                    &index,
                    meta,
                    declaration.clone(),
                    new_row.clone(),
                    new_row.created_tx,
                    commit_lsn,
                )?;
                keyed_moves.push(
                    PartitionedVectorMove::new(
                        index.clone(),
                        before.partition_key.clone(),
                        after.partition_key.clone(),
                        new_row.row_id,
                        new_row.row_id,
                        new_row.created_tx,
                    )
                    .replacing_row_version(),
                );
                mutations.push(PreparedVectorPartitionMutation::Move {
                    index,
                    before,
                    after,
                    moved_tx: new_row.created_tx,
                });
            }
        }

        let memberships = self.relational.prepare_memberships(
            &ws.relational_deletes,
            &ws.relational_inserts,
            self.accountant.clone(),
        )?;
        let vector_publication = if mutations.is_empty() {
            None
        } else {
            let expected_moves = keyed_moves.len();
            let prepared = match registered_snapshots {
                Some(registered_snapshots) => self
                    .vector
                    .prepare_partitioned_batch_with_registered_snapshots(
                        keyed_deletes,
                        keyed_inserts,
                        keyed_moves,
                        registered_snapshots,
                    )?,
                None => self.vector.prepare_partitioned_batch(
                    keyed_deletes,
                    keyed_inserts,
                    keyed_moves,
                    reclaimable_partitions,
                )?,
            };
            if prepared.valid_move_count() != expected_moves {
                return Err(Error::Other(
                    "prepared vector partition batch lost a validated move source".to_string(),
                ));
            }
            Some(prepared)
        };
        let retention_identities = ws
            .config_writes
            .iter()
            .filter(|(key, _)| key.starts_with(RETENTION_EXPIRY_PREFIX))
            .map(|(_, bytes)| crate::persistence::RedbPersistence::decode_config_value(bytes))
            .collect::<Result<Vec<(TableName, RowId, TxId, Lsn)>>>()?;
        let retention_expiries = self
            .relational
            .prepare_retention_expiries(retention_identities, self.accountant.clone())?;
        Ok(PreparedVectorPartitionMutationBatch {
            memberships,
            retention_expiries,
            commit_lsn,
            mutations,
            vector_publication,
        })
    }

    pub(crate) fn apply_exact(&self, ws: &WriteSet) -> Result<()> {
        let registered_snapshots = self.take_vector_partition_snapshot_stage(ws.commit_lsn);
        let table_meta = self.relational.table_meta.read().clone();
        let deleted_rows = self.deleted_rows_snapshot_for_write_set(ws);
        let log_entries = self.build_change_log_entries_with_snapshots(
            ws,
            Some(&table_meta),
            deleted_rows.as_ref(),
        );
        let partition_mutations = match registered_snapshots.as_deref() {
            Some(registered_snapshots) => self
                .prepare_vector_partition_mutations_with_registered_snapshots(
                    ws,
                    &table_meta,
                    deleted_rows.as_ref(),
                    registered_snapshots,
                )?,
            None => {
                self.prepare_vector_partition_mutations(ws, &table_meta, deleted_rows.as_ref())?
            }
        };
        self.apply_exact_with_log_entries(ws, log_entries, partition_mutations)
    }

    pub(crate) fn apply_exact_with_log_entries(
        &self,
        ws: &WriteSet,
        log_entries: Vec<ChangeLogEntry>,
        partition_mutations: PreparedVectorPartitionMutationBatch,
    ) -> Result<()> {
        // Erase the old image after durable success, before ordinary writes publish.
        let erasure = ws
            .commit_lsn
            .and_then(|lsn| self.local_erasure_stages.lock().remove(&lsn));
        if let Some(stage) = erasure {
            (stage.publish)();
        }
        self.relational.apply_prepared_rows(
            &ws.relational_deletes,
            &ws.relational_inserts,
            partition_mutations.memberships,
        );
        self.relational
            .publish_retention_expiries(partition_mutations.retention_expiries);
        self.apply_phase_pause.maybe_pause();
        self.graph.apply_deletes_ref(&ws.adj_deletes);
        self.graph.apply_inserts_ref(&ws.adj_inserts);
        if let Some(publication) = partition_mutations.vector_publication {
            self.vector.publish_prepared_partitioned_batch(
                publication,
                partition_mutations.commit_lsn,
                Some(&*self.accountant),
            );
        }
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
    // Expiry keeps accepted-source provenance until physical reclamation.
    if is_retention_expiry(ws) {
        return (Vec::new(), Vec::new());
    }
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
    // Marks for rows this write set does not rewrite. They follow the staged
    // rows so that a row both rewritten and marked in one transaction ends on
    // the mark, which is the later statement about it.
    set.extend(ws.sync_source_provenance_marks.iter().cloned());
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
