use contextdb_core::{
    DirectedValue, IndexKey, RowId, SortDirection, TableMeta, TableName, TotalOrdAsc, TotalOrdDesc,
    TxId, Value, VersionedRow,
};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub row_id: RowId,
    pub created_tx: TxId,
    pub deleted_tx: Option<TxId>,
}

impl IndexEntry {
    pub fn visible_at(&self, snapshot: contextdb_core::SnapshotId) -> bool {
        self.created_tx.0 <= snapshot.0 && self.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
    }
}

/// Storage for one declared index: BTreeMap keyed by `IndexKey` (direction-
/// wrapped per column) to a Vec of posting entries with MVCC visibility.
#[derive(Debug, Default)]
pub struct IndexStorage {
    pub columns: Vec<(String, SortDirection)>,
    /// Non-unique keys map to Vec<IndexEntry>; tie-break by row_id ascending
    /// is maintained at insert time (I18).
    pub tree: BTreeMap<IndexKey, Vec<IndexEntry>>,
}

impl IndexStorage {
    pub fn new(columns: Vec<(String, SortDirection)>) -> Self {
        Self {
            columns,
            tree: BTreeMap::new(),
        }
    }

    /// Number of live (non-tombstoned) postings, useful for leak detection.
    pub fn total_entries(&self) -> u64 {
        self.tree
            .values()
            .map(|v| v.iter().filter(|e| e.deleted_tx.is_none()).count() as u64)
            .sum()
    }

    /// Total postings including tombstones (for DROP TABLE leak detection).
    pub fn total_entries_including_tombstones(&self) -> u64 {
        self.tree.values().map(|v| v.len() as u64).sum()
    }

    /// Insert a posting at the given key, placing it in row_id-ascending order.
    pub fn insert_posting(&mut self, key: IndexKey, entry: IndexEntry) {
        let vec = self.tree.entry(key).or_default();
        let pos = vec
            .binary_search_by(|e| e.row_id.cmp(&entry.row_id))
            .unwrap_or_else(|i| i);
        vec.insert(pos, entry);
    }

    /// Stamp `deleted_tx` on the posting matching `row_id` at `key`.
    pub fn tombstone_posting(&mut self, key: &IndexKey, row_id: RowId, deleted_tx: TxId) {
        if let Some(vec) = self.tree.get_mut(key) {
            for entry in vec.iter_mut() {
                if entry.row_id == row_id && entry.deleted_tx.is_none() {
                    entry.deleted_tx = Some(deleted_tx);
                    return;
                }
            }
        }
    }
}

pub struct RelationalStore {
    pub tables: RwLock<HashMap<TableName, Vec<VersionedRow>>>,
    row_positions: RwLock<HashMap<(TableName, RowId), usize>>,
    pub table_meta: RwLock<HashMap<TableName, TableMeta>>,
    /// (table, index_name) → IndexStorage. Lives alongside TableMeta.indexes
    /// so readers / writers can look up the physical B-tree without needing a
    /// second-level lock per index.
    pub indexes: RwLock<HashMap<(TableName, String), IndexStorage>>,
    /// Counts how many times `apply_changes`-style batch-level index-lock
    /// acquisitions have happened. The per-row commit path does not bump this
    /// counter; only coarse batch holders (`apply_changes` wrapping N rows) do.
    pub index_write_lock_count: AtomicU64,
    next_row_id: AtomicU64,
}

impl Default for RelationalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RelationalStore {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            row_positions: RwLock::new(HashMap::new()),
            table_meta: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            index_write_lock_count: AtomicU64::new(0),
            next_row_id: AtomicU64::new(1),
        }
    }

    pub fn new_row_id(&self) -> RowId {
        RowId(self.next_row_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Apply row inserts AND maintain every index (user-declared AND auto)
    /// on the affected tables. The index update runs under the same
    /// write-lock scope that the relational insert takes (implicitly held
    /// by the caller's commit_mutex).
    pub fn apply_inserts(&self, inserts: Vec<(TableName, VersionedRow)>) {
        let mut tables = self.tables.write();
        let mut indexes = self.indexes.write();
        let mut row_positions = self.row_positions.write();
        for (table_name, row) in inserts {
            let entry = IndexEntry {
                row_id: row.row_id,
                created_tx: row.created_tx,
                deleted_tx: row.deleted_tx,
            };
            for ((t, _), idx) in indexes.iter_mut() {
                if t != &table_name {
                    continue;
                }
                let key = index_key_for_row(&idx.columns, &row.values);
                idx.insert_posting(key, entry.clone());
            }
            let row_id = row.row_id;
            let rows = tables.entry(table_name.clone()).or_default();
            row_positions.insert((table_name, row_id), rows.len());
            rows.push(row);
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(TableName, RowId, TxId)>) {
        let mut tables = self.tables.write();
        let mut indexes = self.indexes.write();
        for (table_name, row_id, deleted_tx) in deletes {
            let row_values: Option<HashMap<String, Value>> = tables
                .get(&table_name)
                .and_then(|rows| {
                    rows.iter()
                        .rev()
                        .find(|r| r.row_id == row_id && r.deleted_tx.is_none())
                })
                .map(|r| r.values.clone());
            if let Some(values) = row_values {
                for ((t, _), idx) in indexes.iter_mut() {
                    if t != &table_name {
                        continue;
                    }
                    let key = index_key_for_row(&idx.columns, &values);
                    idx.tombstone_posting(&key, row_id, deleted_tx);
                }
            }
            if let Some(rows) = tables.get_mut(&table_name) {
                for row in rows.iter_mut() {
                    if row.row_id == row_id && row.deleted_tx.is_none() {
                        row.deleted_tx = Some(deleted_tx);
                    }
                }
            }
        }
    }

    pub fn create_table(&self, name: &str, meta: TableMeta) {
        self.tables.write().entry(name.to_string()).or_default();
        self.table_meta.write().insert(name.to_string(), meta);
    }

    pub fn insert_loaded_row(&self, name: &str, row: VersionedRow) {
        // Row-load during Database::open: populate every index (user-declared
        // + auto) so the rebuild stays in lockstep. Rows arrive in row_id
        // ascending order, satisfying I18.
        {
            let mut indexes = self.indexes.write();
            let entry = IndexEntry {
                row_id: row.row_id,
                created_tx: row.created_tx,
                deleted_tx: row.deleted_tx,
            };
            for ((t, _), idx) in indexes.iter_mut() {
                if t != name {
                    continue;
                }
                let key = index_key_for_row(&idx.columns, &row.values);
                idx.insert_posting(key, entry.clone());
            }
        }
        let mut tables = self.tables.write();
        let rows = tables.entry(name.to_string()).or_default();
        self.row_positions
            .write()
            .insert((name.to_string(), row.row_id), rows.len());
        rows.push(row);
    }

    pub fn row_by_id(
        &self,
        table: &str,
        row_id: RowId,
        snapshot: contextdb_core::SnapshotId,
    ) -> Option<VersionedRow> {
        let tables = self.tables.read();
        let positions = self.row_positions.read();
        let position = *positions.get(&(table.to_string(), row_id))?;
        drop(positions);
        let rows = tables.get(table)?;
        rows.get(position)
            .filter(|row| row.row_id == row_id && row.visible_at(snapshot))
            .cloned()
            .or_else(|| {
                rows.iter()
                    .rev()
                    .find(|row| row.row_id == row_id && row.visible_at(snapshot))
                    .cloned()
            })
    }

    pub fn max_row_id(&self) -> RowId {
        self.tables
            .read()
            .values()
            .flat_map(|rows| rows.iter().map(|row| row.row_id))
            .max()
            .unwrap_or(RowId(0))
    }

    pub fn set_next_row_id(&self, next_row_id: RowId) {
        self.next_row_id.store(next_row_id.0, Ordering::SeqCst);
    }

    pub fn drop_table(&self, name: &str) {
        self.tables.write().remove(name);
        self.row_positions
            .write()
            .retain(|(table, _), _| table != name);
        self.table_meta.write().remove(name);
        // Drop all indexes whose key-table matches; releases BTreeMap storage.
        let mut indexes = self.indexes.write();
        indexes.retain(|(table, _), _| table != name);
    }

    /// Register a new index storage for (table, name). The IndexDecl must
    /// already be present in the table's TableMeta (callers should
    /// `register_index_meta` before `create_index_storage`).
    pub fn create_index_storage(
        &self,
        table: &str,
        name: &str,
        columns: Vec<(String, SortDirection)>,
    ) {
        self.indexes.write().insert(
            (table.to_string(), name.to_string()),
            IndexStorage::new(columns),
        );
    }

    /// Remove an index storage. Called from DROP INDEX / CASCADE.
    pub fn drop_index_storage(&self, table: &str, name: &str) {
        self.indexes
            .write()
            .remove(&(table.to_string(), name.to_string()));
    }

    /// Build (or rebuild) an index storage by scanning the current table rows.
    /// Used after Database::open completes rebuilding TableMeta.indexes but
    /// before the executor sees queries. Iterates in row_id-ascending order
    /// to preserve I18 tie-break stability.
    pub fn rebuild_index(&self, table: &str, name: &str) {
        let columns = {
            let indexes = self.indexes.read();
            match indexes.get(&(table.to_string(), name.to_string())) {
                Some(idx) => idx.columns.clone(),
                None => return,
            }
        };
        let mut rebuilt = IndexStorage::new(columns.clone());
        let tables = self.tables.read();
        if let Some(rows) = tables.get(table) {
            let mut sorted: Vec<&VersionedRow> = rows.iter().collect();
            sorted.sort_by_key(|r| r.row_id);
            for row in sorted {
                let key = index_key_for_row(&columns, &row.values);
                rebuilt.insert_posting(
                    key,
                    IndexEntry {
                        row_id: row.row_id,
                        created_tx: row.created_tx,
                        deleted_tx: row.deleted_tx,
                    },
                );
            }
        }
        self.indexes
            .write()
            .insert((table.to_string(), name.to_string()), rebuilt);
    }

    /// Introspect total postings across all indexes (including tombstones).
    /// Tests use this to confirm DROP TABLE releases index storage.
    pub fn introspect_indexes_total_entries(&self) -> u64 {
        self.indexes
            .read()
            .values()
            .map(|s| s.total_entries_including_tombstones())
            .sum()
    }

    /// Bump the batch-level index-write lock counter. Called once per
    /// `apply_changes` batch to prove I14.
    pub fn bump_index_write_lock_count(&self) {
        self.index_write_lock_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn index_write_lock_count(&self) -> u64 {
        self.index_write_lock_count.load(Ordering::SeqCst)
    }

    pub fn alter_table_add_column(
        &self,
        table: &str,
        col: contextdb_core::ColumnDef,
    ) -> Result<(), String> {
        let mut meta = self.table_meta.write();
        let m = meta
            .get_mut(table)
            .ok_or_else(|| format!("table '{}' not found", table))?;
        if m.columns.iter().any(|c| c.name == col.name) {
            return Err(format!(
                "column '{}' already exists in table '{}'",
                col.name, table
            ));
        }
        m.columns.push(col);
        Ok(())
    }

    pub fn alter_table_drop_column(&self, table: &str, column: &str) -> Result<(), String> {
        {
            let mut meta = self.table_meta.write();
            let m = meta
                .get_mut(table)
                .ok_or_else(|| format!("table '{}' not found", table))?;
            let pos = m
                .columns
                .iter()
                .position(|c| c.name == column)
                .ok_or_else(|| {
                    format!("column '{}' does not exist in table '{}'", column, table)
                })?;
            if m.columns[pos].primary_key {
                return Err(format!("cannot drop primary key column '{}'", column));
            }
            m.columns.remove(pos);
        }
        {
            let mut tables = self.tables.write();
            if let Some(rows) = tables.get_mut(table) {
                for row in rows.iter_mut() {
                    row.values.remove(column);
                }
            }
        }
        Ok(())
    }

    pub fn alter_table_rename_column(
        &self,
        table: &str,
        from: &str,
        to: &str,
    ) -> Result<(), String> {
        {
            let mut meta = self.table_meta.write();
            let m = meta
                .get_mut(table)
                .ok_or_else(|| format!("table '{}' not found", table))?;
            if m.columns.iter().any(|c| c.name == to) {
                return Err(format!(
                    "column '{}' already exists in table '{}'",
                    to, table
                ));
            }
            let col = m
                .columns
                .iter_mut()
                .find(|c| c.name == from)
                .ok_or_else(|| format!("column '{}' does not exist in table '{}'", from, table))?;
            if col.primary_key {
                return Err(format!("cannot rename primary key column '{}'", from));
            }
            col.name = to.to_string();
        }
        {
            let mut tables = self.tables.write();
            if let Some(rows) = tables.get_mut(table) {
                for row in rows.iter_mut() {
                    if let Some(val) = row.values.remove(from) {
                        row.values.insert(to.to_string(), val);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn is_immutable(&self, table: &str) -> bool {
        self.table_meta
            .read()
            .get(table)
            .is_some_and(|m| m.immutable)
    }

    pub fn validate_state_transition(
        &self,
        table: &str,
        column: &str,
        from: &str,
        to: &str,
    ) -> bool {
        self.table_meta
            .read()
            .get(table)
            .and_then(|m| m.state_machine.as_ref())
            .filter(|sm| sm.column == column)
            .is_none_or(|sm| {
                sm.transitions
                    .get(from)
                    .is_some_and(|targets| targets.iter().any(|t| t == to))
            })
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.read().keys().cloned().collect();
        names.sort();
        names
    }

    pub fn table_meta(&self, name: &str) -> Option<TableMeta> {
        self.table_meta.read().get(name).cloned()
    }
}

/// Build the directed IndexKey for a row's values given the index's column
/// declaration. Missing columns map to `Value::Null` (NULL partition).
pub fn index_key_for_row(
    columns: &[(String, SortDirection)],
    values: &HashMap<String, Value>,
) -> IndexKey {
    columns
        .iter()
        .map(|(col, dir)| {
            let v = values.get(col).cloned().unwrap_or(Value::Null);
            match dir {
                SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v)),
                SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v)),
            }
        })
        .collect()
}

/// Build the directed IndexKey for a Vec of Values (one per indexed column).
pub fn index_key_from_values(columns: &[(String, SortDirection)], values: &[Value]) -> IndexKey {
    columns
        .iter()
        .zip(values.iter())
        .map(|((_, dir), v)| match dir {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v.clone())),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v.clone())),
        })
        .collect()
}
