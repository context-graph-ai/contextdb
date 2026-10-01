//! Writable-open bounds: end-key probes and non-derivable identity floors.
use super::*;

const WRITER_RECOVERY_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("writer_recovery");
const BOUNDS_KEY: &str = "bounds";

/// Floors which cannot be reconstructed from relation end keys and the
/// canonical commit directory. Derivable row/commit maxima are not copied.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct WriterRecoveryBounds {
    pub(crate) identity_tx_floor: u64,
    pub(crate) source_lsn_floor: u64,
    pub(crate) external_row_id_floor: u64,
    pub(crate) commit_count: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct WriterOpenBounds {
    pub(crate) max_row_id: RowId,
    pub(crate) max_tx: TxId,
    pub(crate) max_lsn: Lsn,
    pub(crate) commit_first: Option<(Lsn, TxId)>,
    pub(crate) commit_last: Option<(Lsn, TxId)>,
    pub(crate) commit_count: u64,
}

impl RedbPersistence {
    /// Called in the same transaction as every persistence commit, including
    /// store creation and early schema flushes. `len` reads the tree's stored
    /// cardinality, not its entries. History is never enumerated here.
    pub(super) fn write_writer_recovery_bounds_in_write(
        write: &redb::WriteTransaction,
        ws: Option<&WriteSet>,
    ) -> Result<()> {
        let (commit_count, last) = {
            let commits = write
                .open_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            (
                commits.len().map_err(Self::storage_error)?,
                commits
                    .last()
                    .map_err(Self::storage_error)?
                    .map(|(lsn, tx)| (lsn.value(), tx.value())),
            )
        };
        let mut table = write
            .open_table(WRITER_RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let mut bounds: WriterRecoveryBounds = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_exact(value.value()))
            .transpose()?
            .unwrap_or_default();
        bounds.commit_count = commit_count;
        if let Some(ws) = ws {
            // Canonical committed ids derive from the last commit key. Only
            // identities beyond that frontier require an independent floor.
            let tx = Self::write_set_visibility_tx(ws)
                .into_iter()
                .chain(ws.visibility_floor)
                .max()
                .unwrap_or(TxId(0))
                .0;
            if tx > last.map_or(0, |(_, tx)| tx) {
                bounds.identity_tx_floor = bounds.identity_tx_floor.max(tx);
            }
            let source_lsn = ws
                .relational_inserts
                .iter()
                .map(|(_, row)| row.lsn.0)
                .chain(ws.adj_inserts.iter().map(|edge| edge.lsn.0))
                .chain(ws.vector_inserts.iter().map(|entry| entry.lsn.0))
                .chain(ws.commit_lsn.map(|lsn| lsn.0))
                .max()
                .unwrap_or(0);
            if source_lsn > last.map_or(0, |(lsn, _)| lsn) {
                bounds.source_lsn_floor = bounds.source_lsn_floor.max(source_lsn);
            }
            // Vector keys sort by column/partition before row id: their final
            // key is not a global row-id maximum.
            if let Some(row_id) = ws.vector_inserts.iter().map(|entry| entry.row_id.0).max() {
                bounds.external_row_id_floor = bounds.external_row_id_floor.max(row_id);
            }
        }
        let encoded = Self::encode(&bounds)?;
        table
            .insert(BOUNDS_KEY, encoded.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    /// Schema-sized open work: one end-key probe per relation and two commit
    /// end probes. Callers must not reconstruct a directory by walking rows,
    /// graphs, vectors or change history after obtaining these bounds.
    pub(crate) fn writer_open_bounds<'a>(
        &self,
        tables: impl IntoIterator<Item = &'a str>,
    ) -> Result<WriterOpenBounds> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            Self::writer_open_bounds_in_read(&read, tables)
        })
    }

    pub(super) fn writer_open_bounds_in_read<'a>(
        read: &redb::ReadTransaction,
        tables: impl IntoIterator<Item = &'a str>,
    ) -> Result<WriterOpenBounds> {
        let record = read
            .open_table(WRITER_RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let stored: WriterRecoveryBounds = record
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_exact(value.value()))
            .transpose()?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "missing writer recovery bounds".into(),
            })?;
        let commits = read
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(Self::storage_error)?;
        let commit_first = commits
            .first()
            .map_err(Self::storage_error)?
            .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
        let commit_last = commits
            .last()
            .map_err(Self::storage_error)?
            .map(|(lsn, tx)| (Lsn(lsn.value()), TxId(tx.value())));
        let count = commits.len().map_err(Self::storage_error)?;
        if stored.commit_count != count {
            return Err(Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "commit directory cardinality differs from recovery bounds".into(),
            });
        }
        let mut bounds = WriterOpenBounds {
            max_row_id: RowId(stored.external_row_id_floor),
            max_tx: TxId(
                stored
                    .identity_tx_floor
                    .max(commit_last.map_or(0, |(_, tx)| tx.0)),
            ),
            max_lsn: Lsn(stored
                .source_lsn_floor
                .max(commit_last.map_or(0, |(lsn, _)| lsn.0))),
            commit_first,
            commit_last,
            commit_count: count,
        };
        match read.open_table(writer_history::WRITER_CHANGE_LOG_TABLE) {
            Ok(log) => {
                if let Some((key, _)) = log.last().map_err(Self::storage_error)? {
                    let key = key.value();
                    if key.len() != 16 {
                        return Err(Error::StoreCorrupted {
                            path: "writer_change_log".into(),
                            reason: "log end key is not a 16-byte occurrence key".into(),
                        });
                    }
                    bounds.max_lsn = bounds.max_lsn.max(Lsn(u64::from_be_bytes(
                        key[..8].try_into().expect("validated occurrence width"),
                    )));
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
        for table in tables {
            let name = Self::rel_table_name(table);
            let relation = match read.open_table(TableDefinition::<&[u8], &[u8]>::new(&name)) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(e) => return Err(Self::storage_error(e)),
            };
            if let Some((key, _)) = relation.last().map_err(Self::storage_error)? {
                let key = key.value();
                if key.len() != 24 {
                    return Err(Error::StoreCorrupted {
                        path: name,
                        reason: "relation end key is not a 24-byte version key".into(),
                    });
                }
                let row_id = u64::from_be_bytes(key[..8].try_into().expect("validated key width"));
                bounds.max_row_id = bounds.max_row_id.max(RowId(row_id));
            }
        }
        Ok(bounds)
    }
}

impl RedbPersistence {
    /// Snapshot/import batching can install identities without a canonical
    /// commit entry in that batch. Preserve their floors before committing.
    pub(super) fn raise_noncanonical_writer_floors_in_write(
        write: &redb::WriteTransaction,
        tx: u64,
        lsn: u64,
        external_row: u64,
    ) -> Result<()> {
        let mut table = write
            .open_table(WRITER_RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let mut bounds: WriterRecoveryBounds = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_exact(value.value()))
            .transpose()?
            .unwrap_or_default();
        bounds.identity_tx_floor = bounds.identity_tx_floor.max(tx);
        bounds.source_lsn_floor = bounds.source_lsn_floor.max(lsn);
        bounds.external_row_id_floor = bounds.external_row_id_floor.max(external_row);
        let encoded = Self::encode(&bounds)?;
        table
            .insert(BOUNDS_KEY, encoded.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }
}
