use contextdb_core::{RowId, TableMeta, TableName, VersionedRow};
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
        self.next_row_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn apply_inserts(&self, inserts: Vec<(TableName, VersionedRow)>) {
        let mut tables = self.tables.write();
        for (table_name, row) in inserts {
            tables.entry(table_name).or_default().push(row);
        }
    }

    pub fn apply_deletes(&self, deletes: Vec<(TableName, RowId, u64)>) {
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

    pub fn drop_table(&self, name: &str) {
        self.tables.write().remove(name);
        self.table_meta.write().remove(name);
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
            .map_or(true, |sm| {
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
