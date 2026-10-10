use super::*;

impl RedbPersistence {
    /// Subtract exactly one reclaimed row lifetime from each coalesced posting.
    /// The predecessor seek and at most two replacements do not depend on the
    /// number of retained row versions. The caller removes the row manifest in
    /// this same transaction, so no committed root can observe only one half.
    pub(super) fn prune_relational_posting_intervals_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        meta: Option<&TableMeta>,
        body: &[u8],
    ) -> Result<()> {
        let Some(meta) = meta else {
            return Ok(());
        };
        // Row decoding accounts for its own allocations. Pre-admit the key
        // copies, canonical encoding buffers and declaration controls before
        // constructing any of them; all are released by the caller's row scope.
        let mut meta_size = bincode::enc::write::SizeWriter::default();
        bincode::serde::encode_into_writer(meta, &mut meta_size, bincode::config::standard())
            .map_err(|error| Error::Other(format!("bincode size error: {error}")))?;
        crate::read_image_memory::reserve(
            body.len()
                .saturating_mul(16)
                .saturating_add(meta_size.bytes_written.saturating_mul(8))
                .saturating_add(4096),
        )?;
        let row = Self::decode_versioned_row(body, Some(meta))?;
        let Some(end) = row.deleted_tx else {
            // Current-row erasure belongs to the delete/purge path, which
            // maintains current identities and prefix counts itself.
            return Ok(());
        };
        for declaration in Self::relational_index_declarations(meta) {
            let name = Self::relational_index_table_name(table, &declaration.name);
            let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(&name);
            let mut index = match write.open_table(definition) {
                Ok(index) => index,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(error) => return Err(Self::storage_error(error)),
            };
            let corrupt = |reason: &str| Error::StoreCorrupted {
                path: name.clone(),
                reason: reason.to_owned(),
            };
            let normalized =
                contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
            let probe =
                DurableRelationalIndexKey::new(normalized.clone(), row.row_id, row.created_tx)?;
            let posting = {
                let mut range = index.range(..=probe).map_err(Self::storage_error)?;
                decode_relational_index_entry(
                    &self.path,
                    range.next_back().transpose().map_err(Self::storage_error)?,
                )?
            };
            let Some((encoded, position, posting_end)) = posting else {
                return Err(corrupt("reclaimed row has no historical index posting"));
            };
            if position.key != normalized
                || position.row_id != row.row_id
                || position.created_tx > row.created_tx
                || end <= row.created_tx
                || posting_end.is_some_and(|limit| limit < end)
            {
                return Err(corrupt(
                    "reclaimed row lifetime disagrees with historical index posting",
                ));
            }
            let old_key = DurableRelationalIndexKey(encoded);
            let left = position.created_tx < row.created_tx;
            let right = posting_end.is_none_or(|limit| end < limit);
            let right_key = if right {
                Some(DurableRelationalIndexKey::new(
                    normalized.clone(),
                    row.row_id,
                    end,
                )?)
            } else {
                None
            };
            let right_value = if right {
                Some(Self::encode(&posting_end)?)
            } else {
                None
            };
            let left_value = if left {
                Some(Self::encode(&Some(row.created_tx))?)
            } else {
                None
            };
            let current_name = Self::current_relational_index_table_name(table, &declaration.name);
            let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(&current_name);
            let mut current = write
                .open_table(current_definition)
                .map_err(Self::storage_error)?;
            let logical = DurableRelationalIndexKey::new(normalized, row.row_id, TxId(0))?;
            let next_current = if posting_end.is_none() {
                let stamp = current
                    .get(&logical)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode_exact::<TxId>(value.value()))
                    .transpose()?;
                if stamp != Some(position.created_tx) || !right {
                    return Err(corrupt(
                        "open historical posting disagrees with current index",
                    ));
                }
                Some(Self::encode(&end)?)
            } else {
                None
            };
            index.remove(&old_key).map_err(Self::storage_error)?;
            if let Some(value) = left_value {
                index
                    .insert(old_key, value.as_slice())
                    .map_err(Self::storage_error)?;
            }
            if let (Some(key), Some(value)) = (right_key, right_value) {
                index
                    .insert(key, value.as_slice())
                    .map_err(Self::storage_error)?;
            }
            if let Some(value) = next_current {
                current
                    .insert(logical, value.as_slice())
                    .map_err(Self::storage_error)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reclaimed_intervals_split_postings_reanchor_current_and_preserve_old_roots() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("posting-prune.db");
        let db = crate::Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE points (id INTEGER PRIMARY KEY, tag TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute("CREATE INDEX by_tag ON points(tag)", &HashMap::new())
            .unwrap();
        db.execute("INSERT INTO points VALUES (1, 'same')", &HashMap::new())
            .unwrap();
        let meta = db.table_meta("points").unwrap();
        let original = db.scan("points", db.snapshot()).unwrap().remove(0);
        db.close().unwrap();
        let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
        let writer = persistence.proof_accountant().unwrap();
        writer.set_budget(Some(64 * 1024 * 1024)).unwrap();
        let declaration = RedbPersistence::relational_index_declarations(&meta)
            .into_iter()
            .find(|index| index.name == "by_tag")
            .unwrap();
        let normalized =
            contextdb_relational::index_key_for_row(&declaration.columns, &original.values);
        let start = original.created_tx.0;
        let name = RedbPersistence::relational_index_table_name("points", "by_tag");
        let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
            TableDefinition::new(&name);
        let current_name = RedbPersistence::current_relational_index_table_name("points", "by_tag");
        let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
            TableDefinition::new(&current_name);
        persistence
            .with_db(|database| {
                let old = database.begin_read().unwrap();
                let intervals = |read: &redb::ReadTransaction| {
                    let _scope = persistence.writer_decode_scope().unwrap();
                    read.open_table(definition)
                        .unwrap()
                        .iter()
                        .unwrap()
                        .map(|entry| {
                            let (key, value) = entry.unwrap();
                            (
                                key.value().decode_canonical().unwrap().created_tx.0,
                                RedbPersistence::decode_exact::<Option<TxId>>(value.value())
                                    .unwrap()
                                    .map(|tx| tx.0),
                            )
                        })
                        .collect::<Vec<_>>()
                };
                assert_eq!(intervals(&old), vec![(start, None)]);
                let subtract = |lower, upper, budget| -> Result<()> {
                    writer.set_budget(Some(budget))?;
                    let _scope = persistence.writer_decode_scope()?;
                    let mut row = original.clone();
                    row.created_tx = TxId(lower);
                    row.deleted_tx = Some(TxId(upper));
                    let body = RedbPersistence::encode_versioned_row(&row, Some(&meta))?;
                    let write = database
                        .begin_write()
                        .map_err(RedbPersistence::storage_error)?;
                    persistence.prune_relational_posting_intervals_in_write(
                        &write,
                        "points",
                        Some(&meta),
                        &body,
                    )?;
                    write.commit().map_err(RedbPersistence::storage_error)
                };
                subtract(start + 2, start + 3, 64 * 1024 * 1024)?;
                assert_eq!(
                    intervals(&database.begin_read().unwrap()),
                    vec![(start, Some(start + 2)), (start + 3, None)]
                );
                assert_eq!(
                    intervals(&old),
                    vec![(start, None)],
                    "captured root retains the unsplit posting"
                );
                let before = intervals(&database.begin_read().unwrap());
                assert!(matches!(
                    subtract(start, start + 1, 1),
                    Err(Error::MemoryBudgetExceeded { .. })
                ));
                writer.set_budget(Some(64 * 1024 * 1024))?;
                assert_eq!(
                    intervals(&database.begin_read().unwrap()),
                    before,
                    "refusal leaves all postings unchanged"
                );
                subtract(start, start + 1, 64 * 1024 * 1024)?;
                subtract(start + 1, start + 2, 64 * 1024 * 1024)?;
                assert_eq!(
                    intervals(&database.begin_read().unwrap()),
                    vec![(start + 3, None)]
                );
                assert!(
                    matches!(
                        subtract(start + 2, start + 3, 64 * 1024 * 1024),
                        Err(Error::StoreCorrupted { .. })
                    ),
                    "retry cannot manufacture a missing lifetime"
                );
                let read = database.begin_read().unwrap();
                let logical = DurableRelationalIndexKey::new(normalized, original.row_id, TxId(0))?;
                let current = read.open_table(current_definition).unwrap();
                let stamp = current.get(logical).unwrap().unwrap();
                let _scope = persistence.writer_decode_scope()?;
                assert_eq!(
                    RedbPersistence::decode_exact::<TxId>(stamp.value())?,
                    TxId(start + 3)
                );
                Ok(())
            })
            .unwrap();
        drop(persistence);
        assert_eq!(
            writer.usage().used,
            0,
            "all source and scratch owners refund"
        );
    }
}
