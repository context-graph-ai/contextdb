//! Writable-open bounds: end-key probes and non-derivable identity floors.
use super::*;

pub(super) const RECOVERY_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("writer_recovery");
const RETENTION_CLOCK_BOUNDS: TableDefinition<&str, u64> =
    TableDefinition::new("retention_clock_bounds");
const BOUNDS_KEY: &str = "bounds";
const COMMIT_ROOT_KEY: &str = "commit_directory_root";

/// Identity floors survive directory cleanup as well as noncanonical imports.
/// Logical publication frontiers must not rewind when the final entry retires.
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
    pub(crate) fn config_prefix_exists_at_root(
        root: &WriterSnapshot,
        prefix: &str,
    ) -> Result<bool> {
        let read = root.owner_read()?;
        let table = match read.open_table(CONFIG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut entries = table.range(prefix..).map_err(Self::storage_error)?;
        match entries.next() {
            Some(entry) => {
                let (key, _) = entry.map_err(Self::storage_error)?;
                Ok(key.value().starts_with(prefix))
            }
            None => Ok(false),
        }
    }

    pub(crate) fn pruned_change_counts_at_root(
        root: &Arc<WriterSnapshot>,
        pruned: &HashMap<String, HashSet<(RowId, Lsn)>>,
    ) -> Result<(u64, HashMap<Lsn, u64>)> {
        let mut counts = HashMap::new();
        let mut total = 0;
        for (table, identities) in pruned {
            let lsns = identities
                .iter()
                .map(|(_, lsn)| *lsn)
                .collect::<HashSet<_>>();
            for lsn in lsns {
                let mut cursor = root.table_change_cursor(table, lsn, lsn)?;
                while let Some(window) = cursor.next_table_change()? {
                    #[cfg(feature = "test-seams")]
                    crate::database::resident_history_test_seams::note(|work| {
                        work.log_keys_examined += 2
                    });
                    let row = match &window.value {
                        ChangeLogEntry::RowInsert { row_id, .. }
                        | ChangeLogEntry::RowDelete { row_id, .. } => Some(*row_id),
                        _ => None,
                    };
                    if row.is_some_and(|row| identities.contains(&(row, lsn))) {
                        total += 1;
                        *counts.entry(lsn).or_insert(0) += 1;
                    }
                }
            }
        }
        Ok((total, counts))
    }

    pub(crate) fn commit_candidates_at_root(
        root: &WriterSnapshot,
        manager: &contextdb_tx::TransactionManager<Box<dyn contextdb_tx::WriteSetApplicator>>,
        removed: &HashMap<Lsn, u64>,
        affected: &HashSet<Lsn>,
    ) -> Result<Vec<Lsn>> {
        let read = root.owner_read()?;
        let counts = read
            .open_table(TableDefinition::<u64, u64>::new("change_log_lsn_count"))
            .map_err(Self::storage_error)?;
        let mut floor = None;
        for entry in counts.iter().map_err(Self::storage_error)? {
            let (lsn, count) = entry.map_err(Self::storage_error)?;
            #[cfg(feature = "test-seams")]
            crate::database::resident_history_test_seams::note(|work| work.log_keys_examined += 1);
            let lsn = Lsn(lsn.value());
            if count.value() > removed.get(&lsn).copied().unwrap_or(0) {
                floor = Some(lsn);
                break;
            }
        }
        let anchor = manager
            .try_commit_index_floor(
                floor.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            )?
            .map(|(lsn, _)| lsn);
        let mut candidates = Vec::new();
        for lsn in affected {
            #[cfg(feature = "test-seams")]
            crate::database::resident_history_test_seams::note(|work| work.log_keys_examined += 1);
            if Some(*lsn) != anchor
                && root.change_log_occurrences(*lsn)? <= removed.get(lsn).copied().unwrap_or(0)
            {
                candidates.push(*lsn);
            }
        }
        Ok(candidates)
    }

    pub(crate) fn admit_retention_probe_key(
        root: &WriterSnapshot,
        bytes: usize,
    ) -> Result<crate::memory_accounting::OwnedMemoryReservation> {
        root.adapter_control(bytes)
    }

    /// A monotone upper bound can prove that no retained row is future-dated.
    /// Deletes need not lower it: a high stale bound only keeps the exact scan.
    pub(crate) fn retention_clock_upper_in_read(
        root: &WriterSnapshot,
        table: &str,
    ) -> Result<Option<Wallclock>> {
        let read = root.owner_read()?;
        let bounds = match read.open_table(RETENTION_CLOCK_BOUNDS) {
            Ok(bounds) => bounds,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(Self::storage_error(error)),
        };
        Ok(bounds
            .get(table)
            .map_err(Self::storage_error)?
            .map(|value| Wallclock(value.value())))
    }

    pub(super) fn invalidate_retention_clock_in_write(
        write: &redb::WriteTransaction,
        table: &str,
    ) -> Result<()> {
        write
            .open_table(RETENTION_CLOCK_BOUNDS)
            .map_err(Self::storage_error)?
            .remove(table)
            .map_err(Self::storage_error)?;
        Ok(())
    }

    pub(crate) fn writer_recovery_floors_in_read(
        snapshot: &WriterSnapshot,
    ) -> Result<WriterRecoveryBounds> {
        let read = snapshot.owner_read()?;
        let table = read
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let value = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "missing writer recovery bounds".into(),
            })?;
        Self::decode_exact(value.value())
    }

    /// A checkpoint carries non-derivable identity floors even if no retained
    /// row or commit key witnesses them. Destination counts and physical roots
    /// remain those of its own completed copy.
    pub(crate) fn flush_writer_recovery_floors(&self, source: WriterRecoveryBounds) -> Result<()> {
        let _scratch = self
            .proof_accountant
            .get()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant.clone(),
                    512,
                    "storage",
                    "export_writer_floors",
                    "Raise MEMORY_LIMIT to export writer identity floors.",
                )
            })
            .transpose()?;
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write
                    .open_table(RECOVERY_TABLE)
                    .map_err(Self::storage_error)?;
                let mut target: WriterRecoveryBounds = table
                    .get(BOUNDS_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode_exact(value.value()))
                    .transpose()?
                    .ok_or_else(|| Error::StoreCorrupted {
                        path: "writer_recovery".into(),
                        reason: "missing destination writer recovery bounds".into(),
                    })?;
                target.identity_tx_floor = target.identity_tx_floor.max(source.identity_tx_floor);
                target.source_lsn_floor = target.source_lsn_floor.max(source.source_lsn_floor);
                target.external_row_id_floor = target
                    .external_row_id_floor
                    .max(source.external_row_id_floor);
                let encoded = Self::encode(&target)?;
                table
                    .insert(BOUNDS_KEY, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            Self::write_commit_directory_root_in_write(&write)?;
            write.commit().map_err(Self::storage_error)
        })
    }

    /// Called in the same transaction as every persistence commit, including
    /// store creation and early schema flushes. `len` reads the tree's stored
    /// cardinality, not its entries. History is never enumerated here.
    pub(super) fn write_writer_recovery_bounds_in_write(
        write: &redb::WriteTransaction,
        ws: Option<&WriteSet>,
    ) -> Result<()> {
        vector_membership_pages::write_fingerprints(write)?;
        {
            let mut clocks = write
                .open_table(RETENTION_CLOCK_BOUNDS)
                .map_err(Self::storage_error)?;
            if let Some(ws) = ws {
                for (table, row) in &ws.relational_inserts {
                    if let Some(stamp) = row.created_at {
                        let previous = clocks
                            .get(table.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| value.value());
                        // An absent bound is unknown for an existing family.
                        // Only its first ordinary insert can establish complete
                        // coverage without scanning old versions.
                        if previous.is_none() {
                            let counts = write
                                .open_table(RECOVERY_TABLE)
                                .map_err(Self::storage_error)?;
                            let key = format!("row_versions:{table}");
                            let empty_before = counts
                                .get(key.as_str())
                                .map_err(Self::storage_error)?
                                .is_none_or(|value| value.value() == 0u64.to_be_bytes());
                            if !empty_before {
                                continue;
                            }
                        }
                        let maximum = previous.unwrap_or(0);
                        if stamp.0 > maximum {
                            clocks
                                .insert(table.as_str(), stamp.0)
                                .map_err(Self::storage_error)?;
                        }
                    }
                }
            }
        }
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
        // Metadata-only declarations need not have a row commit-index entry.
        // Preserve their frontier from one end-key seek, without decoding history.
        let ddl_lsn = {
            let ddl = write
                .open_table(DDL_LOG_TABLE)
                .map_err(Self::storage_error)?;
            ddl.last()
                .map_err(Self::storage_error)?
                .map(|(key, _)| Self::ddl_lsn_from_key(key.value()))
                .transpose()?
                .unwrap_or(Lsn(0))
        };
        let mut table = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let mut bounds: WriterRecoveryBounds = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_exact(value.value()))
            .transpose()?
            .unwrap_or_default();
        bounds.commit_count = commit_count;
        bounds.source_lsn_floor = bounds.source_lsn_floor.max(ddl_lsn.0);
        if let Some((lsn, tx)) = last {
            bounds.identity_tx_floor = bounds.identity_tx_floor.max(tx);
            bounds.source_lsn_floor = bounds.source_lsn_floor.max(lsn);
        }
        if let Some(ws) = ws {
            for (key, bytes) in &ws.config_writes {
                let (row, lsn) =
                    crate::database::Database::lineage_recovery_floors_for_write(key, bytes)?;
                bounds.external_row_id_floor = bounds.external_row_id_floor.max(row.0);
                bounds.source_lsn_floor = bounds.source_lsn_floor.max(lsn.0);
            }
            // Imports can name identities beyond the local commit frontier.
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
            // A final relational key may be reclaimed, and vector keys sort
            // by column/partition before row id. Preserve every applied row
            // identity in this non-derivable floor before either directory
            // can lose its last witness.
            if let Some(row_id) = ws
                .vector_inserts
                .iter()
                .map(|entry| entry.row_id.0)
                .chain(ws.relational_inserts.iter().map(|(_, row)| row.row_id.0))
                .max()
            {
                bounds.external_row_id_floor = bounds.external_row_id_floor.max(row_id);
            }
        }
        let encoded = Self::encode(&bounds)?;
        table
            .insert(BOUNDS_KEY, encoded.as_slice())
            .map_err(Self::storage_error)?;
        drop(table);
        Self::write_commit_directory_root_in_write(write)?;
        Self::write_row_heads_root(write)?;
        Self::write_writer_relational_counts_in_write(write)
    }

    /// Constant-size root binding, also refreshed after physical relocation.
    /// It authenticates the committed directory without walking history.
    pub(super) fn write_commit_directory_root_in_write(
        write: &redb::WriteTransaction,
    ) -> Result<()> {
        let root = write
            .table_root_fingerprint(COMMIT_INDEX_TABLE)
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]);
        let mut table = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        if table
            .get(COMMIT_ROOT_KEY)
            .map_err(Self::storage_error)?
            .is_some_and(|existing| existing.value() == root)
        {
            return Ok(());
        }
        table
            .insert(COMMIT_ROOT_KEY, root.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }

    // A full row cursor has no selected posting to attest a missing body.
    // Its transaction-coupled cardinality certifies the physical family, even
    // when a corrupt deletion leaves the iterator empty. No identities or
    // row payloads are read to maintain or check this metadata.
    fn write_writer_relational_counts_in_write(write: &redb::WriteTransaction) -> Result<()> {
        let metadata = write.open_table(META_TABLE).map_err(Self::storage_error)?;
        let mut counts = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        for entry in metadata
            .range("table:".."table;")
            .map_err(Self::storage_error)?
        {
            let (name, _) = entry.map_err(Self::storage_error)?;
            let name =
                name.value()
                    .strip_prefix("table:")
                    .ok_or_else(|| Error::StoreCorrupted {
                        path: "meta".into(),
                        reason: "invalid table declaration key".into(),
                    })?;
            let physical = Self::rel_table_name(name);
            let count = match write.open_table(TableDefinition::<&[u8], &[u8]>::new(&physical)) {
                Ok(rows) => rows.len().map_err(Self::storage_error)?,
                Err(redb::TableError::TableDoesNotExist(_)) => 0,
                Err(error) => return Err(Self::storage_error(error)),
            };
            let key = format!("row_versions:{name}");
            let value = count.to_be_bytes();
            if counts
                .get(key.as_str())
                .map_err(Self::storage_error)?
                .is_some_and(|existing| existing.value() == value)
            {
                continue;
            }
            counts
                .insert(key.as_str(), value.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    pub(super) fn verify_writer_relational_count_in_read(
        read: &redb::ReadTransaction,
        table: &str,
        actual: u64,
    ) -> Result<()> {
        let counts = read
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let key = format!("row_versions:{table}");
        let value = counts
            .get(key.as_str())
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::StoreCorrupted {
                path: table.to_owned(),
                reason: "missing saved row cardinality".into(),
            })?;
        let expected = value
            .value()
            .try_into()
            .map(u64::from_be_bytes)
            .map_err(|_| Error::StoreCorrupted {
                path: table.to_owned(),
                reason: "invalid saved row cardinality".into(),
            })?;
        if expected != actual {
            return Err(Error::StoreCorrupted {
                path: table.to_owned(),
                reason: "row family cardinality differs from its committed record".into(),
            });
        }
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
            .open_table(RECOVERY_TABLE)
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
        let expected_root = record
            .get(COMMIT_ROOT_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "missing authenticated commit directory root".into(),
            })?;
        let actual_root = read
            .table_root_fingerprint(COMMIT_INDEX_TABLE)
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]);
        if expected_root.value() != actual_root {
            return Err(Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "commit directory differs from its committed root".into(),
            });
        }
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
        match read.open_table(DDL_LOG_TABLE) {
            Ok(ddl) => {
                if let Some((key, _)) = ddl.last().map_err(Self::storage_error)? {
                    bounds.max_lsn = bounds.max_lsn.max(Self::ddl_lsn_from_key(key.value())?);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(Self::storage_error(error)),
        }
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
    pub(super) fn raise_config_identity_floors_in_write(
        write: &redb::WriteTransaction,
        key: &str,
        bytes: &[u8],
    ) -> Result<()> {
        let (row, lsn) = crate::database::Database::lineage_recovery_floors_for_write(key, bytes)?;
        if row.0 != 0 || lsn.0 != 0 {
            Self::raise_noncanonical_writer_floors_in_write(write, TxId(0), lsn, row)?;
        }
        Ok(())
    }

    /// Snapshot/import batching can install identities without a canonical
    /// commit entry in that batch. Preserve their floors before committing.
    pub(super) fn raise_noncanonical_writer_floors_in_write(
        write: &redb::WriteTransaction,
        tx: TxId,
        lsn: Lsn,
        external_row: RowId,
    ) -> Result<()> {
        let mut table = write
            .open_table(RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let mut bounds: WriterRecoveryBounds = table
            .get(BOUNDS_KEY)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_exact(value.value()))
            .transpose()?
            .unwrap_or_default();
        bounds.identity_tx_floor = bounds.identity_tx_floor.max(tx.0);
        bounds.source_lsn_floor = bounds.source_lsn_floor.max(lsn.0);
        bounds.external_row_id_floor = bounds.external_row_id_floor.max(external_row.0);
        let encoded = Self::encode(&bounds)?;
        table
            .insert(BOUNDS_KEY, encoded.as_slice())
            .map_err(Self::storage_error)?;
        Ok(())
    }
}
