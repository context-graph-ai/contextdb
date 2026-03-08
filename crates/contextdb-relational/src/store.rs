use contextdb_core::{schema, RowId, TableName, VersionedRow};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct RelationalStore {
    pub tables: RwLock<HashMap<TableName, Vec<VersionedRow>>>,
    next_row_id: AtomicU64,
}

impl Default for RelationalStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RelationalStore {
    pub fn new() -> Self {
        let mut tables = HashMap::new();
        for name in schema::ONTOLOGY_TABLES {
            tables.insert((*name).to_string(), Vec::new());
        }
        Self {
            tables: RwLock::new(tables),
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

    pub fn create_table(&self, name: &str) {
        self.tables.write().entry(name.to_string()).or_default();
    }

    pub fn drop_table(&self, name: &str) {
        self.tables.write().remove(name);
    }
}
