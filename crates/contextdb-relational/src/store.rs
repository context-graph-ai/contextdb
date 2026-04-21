use contextdb_core::{RowId, TableMeta, TableName, TxId, VersionedRow};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct RelationalStore {
    pub tables: RwLock<HashMap<TableName, Vec<VersionedRow>>>,
    pub table_meta: RwLock<HashMap<TableName, TableMeta>>,
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
            table_meta: RwLock::new(HashMap::new()),
            next_row_id: AtomicU64::new(1),
        }
    }

    pub fn new_row_id(&self) -> RowId {
        RowId(self.next_row_id.fetch_add(1, Ordering::SeqCst))
    }

    pub fn apply_inserts(&self, inserts: Vec<(TableName, VersionedRow)>) {
        let mut tables = self.tables.write();
        for (table_name, row) in inserts {
            tables.entry(table_name).or_default().push(row);
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(TableName, RowId, TxId)>) {
        let mut tables = self.tables.write();
        for (table_name, row_id, deleted_tx) in deletes {
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
        self.tables
            .write()
            .entry(name.to_string())
            .or_default()
            .push(row);
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
        self.table_meta.write().remove(name);
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
