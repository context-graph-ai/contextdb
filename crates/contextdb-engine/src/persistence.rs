use crate::composite_store::ChangeLogEntry;
use crate::sync_types::DdlChange;
use contextdb_core::{
    AdjEntry, ColumnType, Error, Lsn, Result, TableMeta, Value, VectorEntry, VectorIndexRef,
    VectorQuantization, VersionedRow,
};
use contextdb_tx::WriteSet;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;

const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const FORMAT_METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const CONFIG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("config");
const CHANGE_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("change_log");
const DDL_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("ddl_log");
const GRAPH_FWD_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_fwd");
const GRAPH_REV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_rev");
const VECTORS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_entries");
const FORMAT_VERSION_KEY: &str = "format_version";
const CURRENT_FORMAT_VERSION: &str = "1.0.0";

pub struct RedbPersistence {
    path: std::path::PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedVersionedRow {
    row_id: contextdb_core::RowId,
    values: HashMap<String, PersistedValue>,
    created_tx: contextdb_core::TxId,
    deleted_tx: Option<contextdb_core::TxId>,
    lsn: Lsn,
    created_at: Option<contextdb_core::Wallclock>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedValue {
    Plain(Value),
    Vector(PersistedVector),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedVectorEntry {
    index: VectorIndexRef,
    row_id: contextdb_core::RowId,
    vector: PersistedVector,
    created_tx: contextdb_core::TxId,
    deleted_tx: Option<contextdb_core::TxId>,
    lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedVector {
    F32(Vec<f32>),
    SQ8 {
        min: f32,
        max: f32,
        len: u32,
        payload: Vec<u8>,
    },
    SQ4 {
        min: f32,
        max: f32,
        len: u32,
        payload: Vec<u8>,
    },
}

impl PersistedVector {
    fn from_f32(vector: &[f32], quantization: VectorQuantization) -> Self {
        match quantization {
            VectorQuantization::F32 => PersistedVector::F32(vector.to_vec()),
            VectorQuantization::SQ8 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let payload = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 255.0).round().clamp(0.0, 255.0) as u8
                        })
                        .collect()
                };
                PersistedVector::SQ8 {
                    min,
                    max,
                    len: vector.len() as u32,
                    payload,
                }
            }
            VectorQuantization::SQ4 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let mut payload = Vec::with_capacity(vector.len().div_ceil(2));
                let quantized = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 15.0).round().clamp(0.0, 15.0) as u8
                        })
                        .collect::<Vec<_>>()
                };
                for pair in quantized.chunks(2) {
                    let hi = pair[0] & 0x0f;
                    let lo = pair.get(1).copied().unwrap_or(0) & 0x0f;
                    payload.push((hi << 4) | lo);
                }
                PersistedVector::SQ4 {
                    min,
                    max,
                    len: vector.len() as u32,
                    payload,
                }
            }
        }
    }

    fn to_f32(&self) -> Vec<f32> {
        match self {
            PersistedVector::F32(vector) => vector.clone(),
            PersistedVector::SQ8 {
                min,
                max,
                len,
                payload,
            } => {
                let range = *max - *min;
                payload
                    .iter()
                    .take(*len as usize)
                    .map(|byte| {
                        if range <= f32::EPSILON {
                            *min
                        } else {
                            *min + ((*byte as f32) / 255.0) * range
                        }
                    })
                    .collect()
            }
            PersistedVector::SQ4 {
                min,
                max,
                len,
                payload,
            } => {
                let range = *max - *min;
                let mut values = Vec::with_capacity(*len as usize);
                for byte in payload {
                    for q in [byte >> 4, byte & 0x0f] {
                        if values.len() == *len as usize {
                            break;
                        }
                        values.push(if range <= f32::EPSILON {
                            *min
                        } else {
                            *min + ((q as f32) / 15.0) * range
                        });
                    }
                }
                values
            }
        }
    }
}

fn vector_min_max(vector: &[f32]) -> (f32, f32) {
    let Some((first, rest)) = vector.split_first() else {
        return (0.0, 0.0);
    };
    rest.iter()
        .copied()
        .fold((*first, *first), |(min, max), value| {
            (min.min(value), max.max(value))
        })
}

impl RedbPersistence {
    pub fn create(path: &Path) -> Result<Self> {
        Self::acquire_pid_lock(path)?;
        let result = (|| {
            let db = redb::Database::create(path).map_err(Self::storage_error)?;
            Self::write_format_marker(&db)?;
            Ok(Self {
                path: path.to_path_buf(),
            })
        })();
        if result.is_err() {
            Self::release_pid_lock(path);
        }
        result
    }

    pub fn open(path: &Path) -> Result<Self> {
        Self::acquire_pid_lock(path)?;
        let result = (|| {
            let db = Self::open_db_checked(path)?;
            Self::validate_format_marker(&db, path)?;
            Ok(Self {
                path: path.to_path_buf(),
            })
        })();
        if result.is_err() {
            Self::release_pid_lock(path);
        }
        result
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

    fn open_db_checked(path: &Path) -> Result<redb::Database> {
        match catch_unwind(AssertUnwindSafe(|| redb::Database::open(path))) {
            Ok(Ok(db)) => Ok(db),
            Ok(Err(err)) => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!("metadata/format could not be read: {err}"),
            }),
            Err(_) => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: "metadata/format read panicked; store may be truncated or corrupt"
                    .to_string(),
            }),
        }
    }

    fn write_format_marker(db: &redb::Database) -> Result<()> {
        let write_txn = db.begin_write().map_err(Self::storage_error)?;
        {
            let mut table = write_txn
                .open_table(FORMAT_METADATA_TABLE)
                .map_err(Self::storage_error)?;
            let encoded = Self::encode(&CURRENT_FORMAT_VERSION.to_string())?;
            table
                .insert(FORMAT_VERSION_KEY, encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        write_txn.commit().map_err(Self::storage_error)?;
        Ok(())
    }

    fn validate_format_marker(db: &redb::Database, path: &Path) -> Result<()> {
        let read_txn = db.begin_read().map_err(|err| Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: format!("metadata read failed: {err}"),
        })?;
        let table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(Error::LegacyVectorStoreDetected {
                    found_format_marker: String::new(),
                    expected_release: CURRENT_FORMAT_VERSION.to_string(),
                });
            }
            Err(err) => {
                return Err(Error::StoreCorrupted {
                    path: path.display().to_string(),
                    reason: format!("metadata table could not be read: {err}"),
                });
            }
        };
        let value = table
            .get(FORMAT_VERSION_KEY)
            .map_err(|err| Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!("metadata format_version could not be read: {err}"),
            })?
            .ok_or_else(|| Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: "metadata table is missing format_version".to_string(),
            })?;
        let marker: String = Self::decode(value.value()).map_err(|err| Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: format!("metadata format_version is corrupt: {err}"),
        })?;
        if marker == CURRENT_FORMAT_VERSION {
            Ok(())
        } else {
            Err(Error::LegacyVectorStoreDetected {
                found_format_marker: marker,
                expected_release: CURRENT_FORMAT_VERSION.to_string(),
            })
        }
    }

    pub fn flush_data(&self, ws: &WriteSet) -> Result<()> {
        self.flush_data_with_logs(ws, &[])
    }

    pub fn flush_data_with_logs(&self, ws: &WriteSet, change_log: &[ChangeLogEntry]) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            for (table, row) in &ws.relational_inserts {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let encoded = Self::encode_versioned_row(row, table_meta.get(table))?;
                redb_table
                    .insert(row.row_id.0, encoded.as_slice())
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
                        .get(row_id.0)
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| Error::NotFound(format!("row {row_id} in table {table}")))?;
                    let bytes: &[u8] = existing.value();
                    bytes.to_vec()
                };
                let mut row = Self::decode_versioned_row(&bytes, table_meta.get(table))?;
                row.deleted_tx = Some(*deleted_tx);
                let encoded = Self::encode_versioned_row(&row, table_meta.get(table))?;
                redb_table
                    .insert(row_id.0, encoded.as_slice())
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
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    vectors_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }

                for (index, row_id, deleted_tx) in &ws.vector_deletes {
                    let key = Self::vector_key_parts(index, *row_id);
                    let bytes = {
                        let existing = vectors_table
                            .get(key.as_slice())
                            .map_err(Self::storage_error)?
                            .ok_or_else(|| Error::NotFound(format!("vector row {row_id}")))?;
                        let bytes: &[u8] = existing.value();
                        bytes.to_vec()
                    };
                    let mut entry = Self::decode_vector_entry(&bytes)?;
                    entry.deleted_tx = Some(*deleted_tx);
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(&entry, quantization)?;
                    vectors_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }

                for (index, old_row_id, new_row_id, tx) in &ws.vector_moves {
                    let old_key = Self::vector_key_parts(index, *old_row_id);
                    let bytes = {
                        let existing = vectors_table
                            .get(old_key.as_slice())
                            .map_err(Self::storage_error)?
                            .ok_or_else(|| Error::NotFound(format!("vector row {old_row_id}")))?;
                        let bytes: &[u8] = existing.value();
                        bytes.to_vec()
                    };
                    let mut old_entry = Self::decode_vector_entry(&bytes)?;
                    old_entry.deleted_tx = Some(*tx);
                    let quantization = vector_quantization
                        .get(&old_entry.index)
                        .copied()
                        .unwrap_or_default();
                    let old_encoded = Self::encode_vector_entry(&old_entry, quantization)?;
                    vectors_table
                        .insert(old_key.as_slice(), old_encoded.as_slice())
                        .map_err(Self::storage_error)?;

                    let mut new_entry = old_entry;
                    new_entry.row_id = *new_row_id;
                    new_entry.created_tx = *tx;
                    new_entry.deleted_tx = None;
                    new_entry.lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                    let new_key = Self::vector_key(&new_entry);
                    let new_encoded = Self::encode_vector_entry(&new_entry, quantization)?;
                    vectors_table
                        .insert(new_key.as_slice(), new_encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            if !change_log.is_empty() {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
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

    pub fn append_change_log(&self, lsn: Lsn, entries: &[ChangeLogEntry]) -> Result<()> {
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

    pub fn append_ddl_log(&self, lsn: Lsn, change: &DdlChange) -> Result<()> {
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
        let table_meta = self.load_all_table_meta()?;
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
                    let encoded = Self::encode_versioned_row(row, table_meta.get(name))?;
                    redb_table
                        .insert(row.row_id.0, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_vectors(&self, vectors: &[VectorEntry]) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
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
            let table_meta = Self::load_table_meta_in_read_txn(&read_txn, name)?;
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
                rows.push(Self::decode_versioned_row(
                    value.value(),
                    table_meta.as_ref(),
                )?);
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
                vectors.push(Self::decode_vector_entry(value.value())?);
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

    pub fn load_ddl_log(&self) -> Result<Vec<(Lsn, DdlChange)>> {
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
                entries.push((Lsn(lsn), Self::decode(value.value())?));
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

    fn load_table_meta_in_read_txn(
        read_txn: &redb::ReadTransaction,
        name: &str,
    ) -> Result<Option<TableMeta>> {
        let meta_table = match read_txn.open_table(META_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(Self::storage_error(err)),
        };
        let key = Self::meta_key(name);
        meta_table
            .get(key.as_str())
            .map_err(Self::storage_error)?
            .map(|value| Self::decode(value.value()))
            .transpose()
    }

    fn vector_quantization_map(
        table_meta: &HashMap<String, TableMeta>,
    ) -> HashMap<VectorIndexRef, VectorQuantization> {
        let mut indexes = HashMap::new();
        for (table, meta) in table_meta {
            for column in &meta.columns {
                if matches!(column.column_type, ColumnType::Vector(_)) {
                    indexes.insert(
                        VectorIndexRef::new(table.clone(), column.name.clone()),
                        column.quantization,
                    );
                }
            }
        }
        indexes
    }

    fn column_quantization(meta: Option<&TableMeta>, column_name: &str) -> VectorQuantization {
        meta.and_then(|meta| {
            meta.columns
                .iter()
                .find(|column| {
                    column.name == column_name
                        && matches!(column.column_type, ColumnType::Vector(_))
                })
                .map(|column| column.quantization)
        })
        .unwrap_or_default()
    }

    fn encode_versioned_row(row: &VersionedRow, meta: Option<&TableMeta>) -> Result<Vec<u8>> {
        let values = row
            .values
            .iter()
            .map(|(column, value)| {
                let persisted = match value {
                    Value::Vector(vector) => {
                        let quantization = Self::column_quantization(meta, column);
                        if matches!(quantization, VectorQuantization::F32) {
                            PersistedValue::Vector(PersistedVector::from_f32(vector, quantization))
                        } else {
                            PersistedValue::Plain(Value::Null)
                        }
                    }
                    _ => PersistedValue::Plain(value.clone()),
                };
                (column.clone(), persisted)
            })
            .collect::<HashMap<_, _>>();
        Self::encode(&PersistedVersionedRow {
            row_id: row.row_id,
            values,
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        })
    }

    fn decode_versioned_row(bytes: &[u8], _meta: Option<&TableMeta>) -> Result<VersionedRow> {
        let persisted: PersistedVersionedRow = Self::decode(bytes)?;
        let values = persisted
            .values
            .into_iter()
            .map(|(column, value)| {
                let value = match value {
                    PersistedValue::Plain(value) => value,
                    PersistedValue::Vector(vector) => Value::Vector(vector.to_f32()),
                };
                (column, value)
            })
            .collect::<HashMap<_, _>>();
        Ok(VersionedRow {
            row_id: persisted.row_id,
            values,
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
            created_at: persisted.created_at,
        })
    }

    fn encode_vector_entry(
        entry: &VectorEntry,
        quantization: VectorQuantization,
    ) -> Result<Vec<u8>> {
        Self::encode(&PersistedVectorEntry {
            index: entry.index.clone(),
            row_id: entry.row_id,
            vector: PersistedVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        })
    }

    fn decode_vector_entry(bytes: &[u8]) -> Result<VectorEntry> {
        let persisted: PersistedVectorEntry = Self::decode(bytes)?;
        Ok(VectorEntry {
            index: persisted.index,
            row_id: persisted.row_id,
            vector: persisted.vector.to_f32(),
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
        })
    }

    fn rel_table_name(name: &str) -> String {
        format!("rel_{name}")
    }

    fn meta_key(name: &str) -> String {
        format!("table:{name}")
    }

    fn change_log_key(lsn: Lsn, index: usize) -> String {
        format!("{:020}:{index:06}", lsn.0)
    }

    fn ddl_log_key(lsn: Lsn) -> String {
        format!("{:020}", lsn.0)
    }

    fn graph_fwd_key(entry: &AdjEntry) -> Vec<u8> {
        Self::graph_fwd_key_parts(&entry.source, &entry.target, &entry.edge_type)
    }

    fn graph_rev_key(entry: &AdjEntry) -> Vec<u8> {
        Self::graph_rev_key_parts(&entry.source, &entry.target, &entry.edge_type)
    }

    fn vector_key(entry: &VectorEntry) -> Vec<u8> {
        Self::vector_key_parts(&entry.index, entry.row_id)
    }

    fn vector_key_parts(index: &VectorIndexRef, row_id: contextdb_core::RowId) -> Vec<u8> {
        let mut key = Vec::with_capacity(index.table.len() + index.column.len() + 18);
        key.extend_from_slice(index.table.as_bytes());
        key.push(0);
        key.extend_from_slice(index.column.as_bytes());
        key.push(0);
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key
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
