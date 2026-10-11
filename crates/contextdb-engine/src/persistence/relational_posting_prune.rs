use super::writer_source::BorrowedRelationalIndexKey;
use super::*;

impl RedbPersistence {
    /// Remove the terminal lifetime using a borrowed posting guard and a single
    /// encoded scalar endpoint. Payload width never multiplies into row copies.
    pub(super) fn prune_terminal_relational_posting_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        meta: Option<&TableMeta>,
        body: &[u8],
    ) -> Result<()> {
        let Some(meta) = meta else {
            return Ok(());
        };
        let mut measured = bincode::enc::write::SizeWriter::default();
        bincode::serde::encode_into_writer(meta, &mut measured, bincode::config::standard())
            .map_err(|e| Error::Other(format!("bincode size error: {e}")))?;
        let initial = measured
            .bytes_written
            .saturating_mul(8)
            .saturating_add(4096);
        let memory = crate::read_image_memory::active_owner()?;
        memory.reserve(initial)?;
        let declarations = Self::relational_index_declarations(meta);
        // auto_indexes_for_table_meta has now destroyed its temporary cloned
        // schema. Keep only the returned declaration capacities funded while
        // the posting key is allocated; refund no surviving declaration bytes.
        let retained = declarations.iter().try_fold(
            declarations
                .capacity()
                .checked_mul(std::mem::size_of::<IndexDecl>())
                .ok_or_else(|| Error::Other("index declarations capacity overflow".into()))?,
            |bytes, declaration| {
                let columns = declaration
                    .columns
                    .capacity()
                    .checked_mul(std::mem::size_of::<(String, contextdb_core::SortDirection)>())
                    .ok_or_else(|| Error::Other("index columns capacity overflow".into()))?;
                declaration.columns.iter().try_fold(
                    bytes
                        .checked_add(declaration.name.capacity())
                        .and_then(|n| n.checked_add(columns))
                        .ok_or_else(|| {
                            Error::Other("index declarations capacity overflow".into())
                        })?,
                    |bytes, (column, _)| {
                        bytes
                            .checked_add(column.capacity())
                            .ok_or_else(|| Error::Other("index column capacity overflow".into()))
                    },
                )
            },
        )?;
        let released = initial
            .checked_sub(retained)
            .ok_or_else(|| Error::Other("index declaration admission bound exceeded".into()))?;
        memory.release(released);
        if content_identity::version_manifest::has_indexed_vector(body, &declarations)? {
            return self.prune_relational_posting_intervals_in_write(
                write,
                table,
                Some(meta),
                body,
            );
        }
        for declaration in declarations {
            let _scope = crate::read_image_memory::active_owner()?
                .row_owner()?
                .enter();
            // Both hashed table names and the temporary formatting input are
            // admitted before construction, independently of payload width.
            crate::read_image_memory::reserve(3 * (12 + 64 + 8))?;
            let mut posting =
                content_identity::version_manifest::scalar_posting_for_body(body, &declaration)?
                    .ok_or_else(|| {
                        Error::Other("scalar posting changed during preparation".into())
                    })?;
            let Some(end) = posting.deleted else {
                continue;
            };
            let name = Self::relational_index_table_name(table, &declaration.name);
            let damage = |reason: &str| Error::StoreCorrupted {
                path: name.clone(),
                reason: reason.into(),
            };
            let definition = TableDefinition::<BorrowedRelationalIndexKey, &[u8]>::new(&name);
            let mut index = match write.open_table(definition) {
                Ok(index) => index,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(e) => return Err(Self::storage_error(e)),
            };
            let start = {
                let mut range = index
                    .range(..=posting.key.as_slice())
                    .map_err(Self::storage_error)?;
                let (key, value) = range
                    .next_back()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| damage("reclaimed row has no terminal posting"))?;
                let key = key.value();
                let (suffix, row_id, created) =
                    content_identity::version_manifest::scalar_posting_identity(key)?;
                let closure: Option<TxId> = Self::decode_exact(value.value())?;
                if row_id != posting.row_id
                    || key[..suffix] != posting.key[..posting.suffix]
                    || created > posting.created
                    || closure != Some(end)
                    || end <= posting.created
                {
                    return Err(damage(
                        "terminal posting disagrees with reclaimed row lifetime",
                    ));
                }
                created
            };
            let current_name = Self::current_relational_index_table_name(table, &declaration.name);
            let current = write
                .open_table(TableDefinition::<BorrowedRelationalIndexKey, &[u8]>::new(
                    &current_name,
                ))
                .map_err(Self::storage_error)?;
            posting.key.truncate(posting.suffix);
            bincode::serde::encode_into_std_write(
                (posting.row_id, TxId(0)),
                &mut posting.key,
                bincode::config::standard(),
            )
            .map_err(|e| Error::Other(format!("posting identity: {e}")))?;
            if current
                .get(posting.key.as_slice())
                .map_err(Self::storage_error)?
                .is_some()
            {
                return Err(damage("terminal posting still has a current identity"));
            }
            drop(current);
            posting.key.truncate(posting.suffix);
            bincode::serde::encode_into_std_write(
                (posting.row_id, start),
                &mut posting.key,
                bincode::config::standard(),
            )
            .map_err(|e| Error::Other(format!("posting identity: {e}")))?;
            if start < posting.created {
                let closed = Self::encode(&Some(posting.created))?;
                index
                    .insert(posting.key.as_slice(), closed.as_slice())
                    .map_err(Self::storage_error)?;
            } else {
                index
                    .remove(posting.key.as_slice())
                    .map_err(Self::storage_error)?;
            }
        }
        Ok(())
    }

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
    fn borrowed_terminal_keys_preserve_the_canonical_scalar_codec() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("terminal-codec.db");
        let persistence = RedbPersistence::create(&path).unwrap();
        let writer = persistence
            .proof_accountant()
            .unwrap_or_else(|| Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into());
        persistence.attach_memory_accountant(writer.clone());
        assert_eq!(persistence.proof_accountant().unwrap().as_ptr(), writer.as_ptr());
        let row = VersionedRow {
            row_id: RowId(u64::MAX - 1),
            values: HashMap::from([
                (
                    "text".into(),
                    Value::Text("utf8\u{03bb}\u{4e2d}".repeat(1024)),
                ),
                ("id".into(), Value::Int64(-123456)),
                ("real".into(), Value::Float64(-0.25)),
                ("flag".into(), Value::Bool(true)),
                ("uuid".into(), Value::Uuid(uuid::Uuid::from_u128(123))),
                ("stamp".into(), Value::Timestamp(456789)),
                ("tx".into(), Value::TxId(TxId(789))),
                ("null".into(), Value::Null),
            ]),
            created_tx: TxId(u64::MAX - 3),
            deleted_tx: Some(TxId(u64::MAX - 2)),
            lsn: Lsn(4321),
            created_at: Some(Wallclock(100)),
        };
        let names = [
            "text", "id", "real", "flag", "uuid", "stamp", "tx", "null", "missing",
        ];
        let declaration = IndexDecl {
            name: "scalar_codec".into(),
            columns: names
                .iter()
                .enumerate()
                .map(|(position, name)| {
                    (
                        (*name).into(),
                        if position % 2 == 0 {
                            contextdb_core::SortDirection::Asc
                        } else {
                            contextdb_core::SortDirection::Desc
                        },
                    )
                })
                .collect(),
            kind: IndexKind::UserDeclared,
        };
        {
            let _scope = persistence.writer_decode_scope().unwrap();
            let body = RedbPersistence::encode_versioned_row(&row, None).unwrap();
            let key =
                content_identity::version_manifest::scalar_posting_for_body(&body, &declaration)
                    .unwrap()
                    .unwrap();
            let expected = DurableRelationalIndexKey::new(
                contextdb_relational::index_key_for_row(&declaration.columns, &row.values),
                row.row_id,
                row.created_tx,
            )
            .unwrap();
            assert_eq!(key.key, expected.0);
            assert_eq!(
                (key.row_id, key.created, key.deleted),
                (row.row_id, row.created_tx, row.deleted_tx)
            );
            assert_eq!(
                content_identity::version_manifest::scalar_posting_identity(&key.key).unwrap(),
                (key.suffix, row.row_id, row.created_tx)
            );
            let mut trailing = key.key.clone();
            trailing.push(0);
            assert!(
                content_identity::version_manifest::scalar_posting_identity(&trailing).is_err()
            );
            let mut trailing = body.clone();
            trailing.push(0);
            assert!(
                content_identity::version_manifest::scalar_posting_for_body(
                    &trailing,
                    &declaration
                )
                .is_err()
            );
        }
        drop(persistence);
        assert_eq!(writer.usage().used, 0);
        assert_eq!(writer.underflow_count_for_test(), 0);
    }

    #[test]
    fn expired_last_indexed_version_stays_absent_from_later_historical_reads() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("expired-posting.db");
        let _clock = Wallclock::test_clock_guard(|| 1_000_000);
        let options = crate::DatabaseOpenOptions {
            maintenance_policy: crate::MaintenancePolicy::CallerDriven,
            ..crate::DatabaseOpenOptions::default()
        };
        let db = crate::Database::open_with_options(&path, options.clone()).unwrap();
        db.execute(
            "CREATE TABLE expired (id INT PRIMARY KEY, tag TEXT, expires TIMESTAMP EXPIRES) \
             RETAIN 1 HOURS SYNC OFF",
            &HashMap::new(),
        )
        .unwrap();
        db.execute("CREATE INDEX by_tag ON expired(tag)", &HashMap::new())
            .unwrap();
        db.execute(
            "INSERT INTO expired VALUES (1, 'same', $expired), (2, 'other', $live)",
            &HashMap::from([
                ("expired".into(), Value::Timestamp(100)),
                ("live".into(), Value::Timestamp(2_000_000)),
            ]),
        )
        .unwrap();
        let snapshot = db.snapshot();
        let read = |db: &crate::Database| {
            db.__with_snapshot_override_for_test(snapshot, || {
                db.execute("SELECT id FROM expired WHERE tag = 'same'", &HashMap::new())
            })
            .unwrap()
            .rows
        };
        assert_eq!(read(&db), vec![vec![Value::Int64(1)]]);
        let pin = db.pin_snapshot(snapshot).unwrap();
        db.run_maintenance_cycle().unwrap();
        assert_eq!(
            db.__physical_version_count_for_test("expired"),
            2,
            "each row still has exactly one physical version"
        );
        assert_eq!(read(&db), vec![vec![Value::Int64(1)]]);
        drop(pin);
        for _ in 0..4 {
            db.run_maintenance_cycle().unwrap();
        }
        assert_eq!(db.__physical_version_count_for_test("expired"), 1);
        assert!(read(&db).is_empty());
        db.close().unwrap();
        drop(db);
        let reopened = crate::Database::open_with_options(&path, options).unwrap();
        assert!(read(&reopened).is_empty());
        reopened.close().unwrap();
    }

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
