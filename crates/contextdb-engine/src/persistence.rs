use crate::composite_store::ChangeLogEntry;
use crate::sync_types::DdlChange;
use contextdb_core::{AdjEntry, Error, Result, TableMeta, VectorEntry, VersionedRow};
use contextdb_tx::WriteSet;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashMap;
use std::path::Path;

const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const CONFIG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("config");
const CHANGE_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("change_log");
const DDL_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("ddl_log");
const GRAPH_FWD_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_fwd");
const GRAPH_REV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_rev");
const VECTORS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("vectors");

pub struct RedbPersistence {
    path: std::path::PathBuf,
}

impl RedbPersistence {
    pub fn create(path: &Path) -> Result<Self> {
        let _db = redb::Database::create(path).map_err(Self::storage_error)?;
        Self::acquire_pid_lock(path)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        Self::acquire_pid_lock(path)?;
        let _db = redb::Database::open(path).map_err(Self::storage_error)?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn close(&self) {
        Self::release_pid_lock(&self.path);
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// PID-based advisory lock. Writes current PID to a .lock file.
    /// On open, checks if the .lock file exists and if the PID in it is still alive.
    fn acquire_pid_lock(path: &Path) -> Result<()> {
        let lock_path = path.with_extension("lock");
        if lock_path.exists()
            && let Ok(contents) = std::fs::read_to_string(&lock_path)
            && let Ok(pid) = contents.trim().parse::<u32>()
        {
            let proc_path = format!("/proc/{}", pid);
            if std::path::Path::new(&proc_path).exists() && pid != std::process::id() {
                return Err(Error::Other(
                    "database is locked (another process may have it open)".to_string(),
                ));
            }
            // Either PID not running (stale lock) or same process — overwrite
        }
        std::fs::write(&lock_path, std::process::id().to_string()).map_err(Self::storage_error)?;
        Ok(())
    }

    fn release_pid_lock(path: &Path) {
        let lock_path = path.with_extension("lock");
        let _ = std::fs::remove_file(&lock_path);
    }

    pub fn flush_data(&self, ws: &WriteSet) -> Result<()> {
        self.flush_data_with_logs(ws, &[])
    }

    pub fn flush_data_with_logs(&self, ws: &WriteSet, change_log: &[ChangeLogEntry]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            for (table, row) in &ws.relational_inserts {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let encoded = Self::encode(row)?;
                redb_table
                    .insert(row.row_id, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }

            for (table, row_id, deleted_tx) in &ws.relational_deletes {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let bytes = {
                    let existing = redb_table
                        .get(*row_id)
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| Error::NotFound(format!("row {row_id} in table {table}")))?;
                    let bytes: &[u8] = existing.value();
                    bytes.to_vec()
                };
                let mut row: VersionedRow = Self::decode(&bytes)?;
                row.deleted_tx = Some(*deleted_tx);
                let encoded = Self::encode(&row)?;
                redb_table
                    .insert(*row_id, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }

            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                for entry in &ws.adj_inserts {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }

                for (source, edge_type, target, deleted_tx) in &ws.adj_deletes {
                    let fwd_key = Self::graph_fwd_key_parts(source, target, edge_type);
                    let rev_key = Self::graph_rev_key_parts(source, target, edge_type);

                    let bytes = {
                        let fwd_existing = fwd_table
                            .get(fwd_key.as_slice())
                            .map_err(Self::storage_error)?
                            .ok_or_else(|| {
                                Error::NotFound(format!(
                                    "edge {source} -[{edge_type}]-> {target} in graph_fwd"
                                ))
                            })?;
                        let bytes: &[u8] = fwd_existing.value();
                        bytes.to_vec()
                    };
                    let mut edge: AdjEntry = Self::decode(&bytes)?;
                    edge.deleted_tx = Some(*deleted_tx);
                    let encoded = Self::encode(&edge)?;

                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut vectors_table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;

                for entry in &ws.vector_inserts {
                    let encoded = Self::encode(entry)?;
                    vectors_table
                        .insert(entry.row_id, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }

                for (row_id, deleted_tx) in &ws.vector_deletes {
                    let bytes = {
                        let existing = vectors_table
                            .get(*row_id)
                            .map_err(Self::storage_error)?
                            .ok_or_else(|| Error::NotFound(format!("vector row {row_id}")))?;
                        let bytes: &[u8] = existing.value();
                        bytes.to_vec()
                    };
                    let mut entry: VectorEntry = Self::decode(&bytes)?;
                    entry.deleted_tx = Some(*deleted_tx);
                    let encoded = Self::encode(&entry)?;
                    vectors_table
                        .insert(*row_id, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            if !change_log.is_empty() {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(0);
                for (index, entry) in change_log.iter().enumerate() {
                    let key = Self::change_log_key(lsn, index);
                    let encoded = Self::encode(entry)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn flush_table_meta(&self, name: &str, meta: &TableMeta) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded = Self::encode(meta)?;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_meta(&self, name: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                meta_table
                    .remove(key.as_str())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn flush_config_value<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let encoded = Self::encode(value)?;
                config_table
                    .insert(key, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_config_value(&self, key: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                config_table.remove(key).map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn append_change_log(&self, lsn: u64, entries: &[ChangeLogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, entry) in entries.iter().enumerate() {
                    let key = Self::change_log_key(lsn, index);
                    let encoded = Self::encode(entry)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn append_ddl_log(&self, lsn: u64, change: &DdlChange) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::ddl_log_key(lsn);
                let encoded = Self::encode(change)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_data(&self, name: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let _ = write_txn
                .delete_table(table_def)
                .map_err(Self::storage_error)?;
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_table_rows(&self, name: &str, rows: &[VersionedRow]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(table_def);
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode(row)?;
                    redb_table
                        .insert(row.row_id, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_vectors(&self, vectors: &[VectorEntry]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let encoded = Self::encode(entry)?;
                    table
                        .insert(entry.row_id, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_graph_edges(&self, edges: &[AdjEntry]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(GRAPH_FWD_TABLE);
            let _ = write_txn.delete_table(GRAPH_REV_TABLE);
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                for entry in edges {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn load_all_table_meta(&self) -> Result<HashMap<String, TableMeta>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let meta_table = match read_txn.open_table(META_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut tables = HashMap::new();
            for entry in meta_table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if let Some(name) = key.strip_prefix("table:") {
                    tables.insert(name.to_string(), Self::decode(value.value())?);
                }
            }
            Ok(tables)
        })
    }

    pub fn load_config_value<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let value = match config_table.get(key).map_err(Self::storage_error)? {
                Some(value) => Some(Self::decode(value.value())?),
                None => None,
            };
            Ok(value)
        })
    }

    pub fn load_relational_table(&self, name: &str) -> Result<Vec<VersionedRow>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut rows = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                rows.push(Self::decode(value.value())?);
            }
            Ok(rows)
        })
    }

    pub fn load_all_tables(&self) -> Result<HashMap<String, Vec<VersionedRow>>> {
        let mut all_tables = HashMap::new();
        for name in self.load_all_table_meta()?.into_keys() {
            let rows = self.load_relational_table(&name)?;
            all_tables.insert(name, rows);
        }
        Ok(all_tables)
    }

    pub fn load_forward_edges(&self) -> Result<Vec<AdjEntry>> {
        self.load_graph_table(GRAPH_FWD_TABLE)
    }

    pub fn load_reverse_edges(&self) -> Result<Vec<AdjEntry>> {
        self.load_graph_table(GRAPH_REV_TABLE)
    }

    pub fn load_vectors(&self) -> Result<Vec<VectorEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(VECTORS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut vectors = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                vectors.push(Self::decode(value.value())?);
            }
            Ok(vectors)
        })
    }

    pub fn load_change_log(&self) -> Result<Vec<ChangeLogEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(CHANGE_LOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    pub fn load_ddl_log(&self) -> Result<Vec<(u64, DdlChange)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(DDL_LOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let lsn = key
                    .value()
                    .parse::<u64>()
                    .map_err(|err| Error::Other(format!("invalid ddl log key: {err}")))?;
                entries.push((lsn, Self::decode(value.value())?));
            }
            Ok(entries)
        })
    }

    fn load_graph_table(&self, definition: TableDefinition<&[u8], &[u8]>) -> Result<Vec<AdjEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    fn rel_table_name(name: &str) -> String {
        format!("rel_{name}")
    }

    fn meta_key(name: &str) -> String {
        format!("table:{name}")
    }

    fn change_log_key(lsn: u64, index: usize) -> String {
        format!("{lsn:020}:{index:06}")
    }

    fn ddl_log_key(lsn: u64) -> String {
        format!("{lsn:020}")
    }

    fn graph_fwd_key(entry: &AdjEntry) -> Vec<u8> {
        Self::graph_fwd_key_parts(&entry.source, &entry.target, &entry.edge_type)
    }

    fn graph_rev_key(entry: &AdjEntry) -> Vec<u8> {
        Self::graph_rev_key_parts(&entry.source, &entry.target, &entry.edge_type)
    }

    fn graph_fwd_key_parts(source: &uuid::Uuid, target: &uuid::Uuid, edge_type: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(32 + edge_type.len());
        key.extend_from_slice(source.as_bytes());
        key.extend_from_slice(target.as_bytes());
        key.extend_from_slice(edge_type.as_bytes());
        key
    }

    fn graph_rev_key_parts(source: &uuid::Uuid, target: &uuid::Uuid, edge_type: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(32 + edge_type.len());
        key.extend_from_slice(target.as_bytes());
        key.extend_from_slice(source.as_bytes());
        key.extend_from_slice(edge_type.as_bytes());
        key
    }

    fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|err| Error::Other(format!("bincode encode error: {err}")))
    }

    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        let (value, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|err| Error::Other(format!("bincode decode error: {err}")))?;
        Ok(value)
    }

    fn storage_error(err: impl std::fmt::Display) -> Error {
        let msg = err.to_string();
        if msg.contains("lock") || msg.contains("already open") {
            Error::Other(format!(
                "database is locked (another process may have it open): {msg}"
            ))
        } else {
            Error::Other(format!("redb error: {msg}"))
        }
    }

    fn with_db<T>(&self, f: impl FnOnce(&redb::Database) -> Result<T>) -> Result<T> {
        let db = redb::Database::open(&self.path).map_err(Self::storage_error)?;
        f(&db)
    }
}

impl Drop for RedbPersistence {
    fn drop(&mut self) {
        Self::release_pid_lock(&self.path);
    }
}
