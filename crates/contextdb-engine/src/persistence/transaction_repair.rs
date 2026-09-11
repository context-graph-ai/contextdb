use super::*;

impl RedbPersistence {
    /// Repair damaged transaction ordering in the current storage format.
    /// Every keyed MVCC owner and the index switch in one transaction; a
    /// crash cannot expose renumbered rows with the previous snapshot index.
    pub(crate) fn repair_current_transaction_order(
        &self,
        commit_index: &BTreeMap<Lsn, TxId>,
        tables: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        let mut mapping = HashMap::new();
        for (position, tx) in commit_index.values().enumerate() {
            mapping.entry(*tx).or_insert(TxId(position as u64 + 1));
        }
        if commit_index
            .values()
            .map(|tx| mapping[tx])
            .try_fold(TxId(0), |previous, tx| (tx >= previous).then_some(tx))
            .is_none()
        {
            return Err(Error::StoreCorrupted {
                path: self.path.display().to_string(),
                reason: "commit index assigns one transaction to disjoint commit ranges".into(),
            });
        }
        let remap = |tx: &mut TxId| {
            if let Some(value) = mapping.get(tx) {
                *tx = *value;
            }
        };
        let remap_membership = |entry: &mut VectorPartitionMembershipRecord| {
            remap(&mut entry.row_created_tx);
            remap(&mut entry.vector_created_tx);
            entry.visible_from = entry.row_created_tx.max(entry.vector_created_tx);
            if let Some(deleted) = &mut entry.deleted_tx {
                remap(deleted);
            }
        };
        let remap_tombstone = |entry: &mut VectorPartitionTombstoneRecord| {
            remap_membership(&mut entry.membership);
            remap(&mut entry.deleted_tx);
        };
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            for name in tables.keys() {
                let name = Self::rel_table_name(name);
                self.transform_transaction_records(
                    &write,
                    TableDefinition::new(&name),
                    |_, bytes| {
                        let mut row: PersistedVersionedRow = Self::decode(bytes)?;
                        remap(&mut row.created_tx);
                        if let Some(deleted) = &mut row.deleted_tx {
                            remap(deleted);
                        }
                        Ok((
                            Self::rel_row_key_from_parts(row.row_id, row.created_tx, row.lsn),
                            Self::encode(&row)?,
                        ))
                    },
                )?;
            }
            self.transform_transaction_records(&write, VECTORS_TABLE, |_, bytes| {
                let mut entry: PersistedVectorEntry = Self::decode(bytes)?;
                remap(&mut entry.created_tx);
                if let Some(deleted) = &mut entry.deleted_tx {
                    remap(deleted);
                }
                Ok((
                    Self::vector_identity_key(
                        &entry.index,
                        entry.row_id,
                        entry.created_tx,
                        entry.lsn,
                    ),
                    Self::encode(&entry)?,
                ))
            })?;
            for (definition, reverse) in [(GRAPH_FWD_TABLE, false), (GRAPH_REV_TABLE, true)] {
                self.transform_transaction_records(&write, definition, |_, bytes| {
                    let mut entry: AdjEntry = Self::decode(bytes)?;
                    remap(&mut entry.created_tx);
                    if let Some(deleted) = &mut entry.deleted_tx {
                        remap(deleted);
                    }
                    let key = if reverse {
                        Self::graph_rev_key(&entry)
                    } else {
                        Self::graph_fwd_key(&entry)
                    };
                    Ok((key, Self::encode(&entry)?))
                })?;
            }
            self.transform_transaction_records(
                &write,
                VECTOR_PARTITION_MEMBERSHIP_TABLE,
                |_, bytes| {
                    let mut record: VectorPartitionMembershipRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::Membership,
                            bytes,
                        )?;
                    remap_membership(&mut record);
                    Ok((
                        Self::vector_partition_membership_key(&record),
                        Self::encode_vector_partition_record(
                            VectorPartitionRecordKind::Membership,
                            &record,
                        )?,
                    ))
                },
            )?;
            self.transform_transaction_records(
                &write,
                VECTOR_PARTITION_TOMBSTONE_TABLE,
                |_, bytes| {
                    let mut record: VectorPartitionTombstoneRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::Tombstone,
                            bytes,
                        )?;
                    remap_tombstone(&mut record);
                    Ok((
                        Self::vector_partition_tombstone_key(&record),
                        Self::encode_vector_partition_record(
                            VectorPartitionRecordKind::Tombstone,
                            &record,
                        )?,
                    ))
                },
            )?;
            self.transform_transaction_records(
                &write,
                VECTOR_PARTITION_JOURNAL_TABLE,
                |_, bytes| {
                    let mut record: VectorPartitionJournalRecord = self
                        .decode_vector_partition_record(
                            VectorPartitionRecordKind::Journal,
                            bytes,
                        )?;
                    match &mut record.change {
                        VectorPartitionJournalChange::Upsert(entry) => remap_membership(entry),
                        VectorPartitionJournalChange::Tombstone(entry) => remap_tombstone(entry),
                    }
                    Ok((
                        Self::vector_partition_journal_key(&record),
                        Self::encode_vector_partition_record(
                            VectorPartitionRecordKind::Journal,
                            &record,
                        )?,
                    ))
                },
            )?;
            // These graphs were built against the damaged visibility order.
            // Keep authoritative values and replay records; maintenance owns
            // rebuilding verified generations after this abnormal repair.
            for definition in [
                VECTOR_PARTITION_BASE_GENERATION_TABLE,
                VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
                VECTOR_PARTITION_GENERATION_CATALOG_TABLE,
            ] {
                write
                    .delete_table(definition)
                    .map_err(Self::storage_error)?;
            }
            {
                let mut config = write
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut changes = Vec::new();
                for entry in config
                    .range(crate::composite_store::RETENTION_EXPIRY_PREFIX..)
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if !key
                        .value()
                        .starts_with(crate::composite_store::RETENTION_EXPIRY_PREFIX)
                    {
                        break;
                    }
                    let (table, row, mut created, lsn): (String, RowId, TxId, Lsn) =
                        Self::decode_config_value(value.value())?;
                    remap(&mut created);
                    changes.push((
                        key.value().to_owned(),
                        crate::composite_store::retention_expiry_key(&table, row, created),
                        Self::encode_config_value(&(table, row, created, lsn))?,
                    ));
                }
                for (old, _, _) in &changes {
                    config.remove(old.as_str()).map_err(Self::storage_error)?;
                }
                for (_, key, value) in changes {
                    config
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write
                .delete_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            {
                let mut index = write
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                for (lsn, tx) in commit_index {
                    index
                        .insert(lsn.0, mapping.get(tx).unwrap_or(tx).0)
                        .map_err(Self::storage_error)?;
                }
            }
            write.commit().map_err(Self::storage_error)
        })
    }

    fn transform_transaction_records(
        &self,
        write: &redb::WriteTransaction,
        definition: TableDefinition<&[u8], &[u8]>,
        mut transform: impl FnMut(&[u8], &[u8]) -> Result<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let mut table = write.open_table(definition).map_err(Self::storage_error)?;
        let mut records = Vec::new();
        for entry in table.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            records.push((key.value().to_vec(), transform(key.value(), value.value())?));
        }
        for (key, _) in &records {
            table.remove(key.as_slice()).map_err(Self::storage_error)?;
        }
        for (_, (key, value)) in records {
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }
}
