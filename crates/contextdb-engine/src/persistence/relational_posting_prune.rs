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
        commitments: &mut historical_index_commitment::CommitmentBatch,
        identity_mutations: &mut content_identity::OriginalIdentityMutations,
    ) -> Result<()> {
        let Some(meta) = meta else {
            return Ok(());
        };
        let mut measured = bincode::enc::write::SizeWriter::default();
        bincode::serde::encode_into_writer(meta, &mut measured, bincode::config::standard())
            .map_err(|e| Error::Other((format!("bincode size error: {e}")).into()))?;
        let initial = measured
            .bytes_written
            .saturating_mul(8)
            .saturating_add(4096);
        let memory = crate::read_image_memory::active_owner()?;
        memory.reserve(initial)?;
        let (planned, _, _) = crate::executor::relational_declaration_plan(meta)?;
        let admitted = initial.max(planned);
        memory.reserve(admitted - initial)?;
        let declarations = {
            #[cfg(test)]
            let _observation =
                content_identity::proof_allocation_oracle_tests::declaration_observation();
            Self::relational_index_declarations(meta)
        };
        // The shared generator has destroyed its temporary declaration backing. Keep only the returned declaration capacities funded while
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
        let released = admitted
            .checked_sub(retained)
            .ok_or_else(|| Error::Other("index declaration admission bound exceeded".into()))?;
        memory.release(released);
        // A live version (retention erases expired rows that were never
        // deleted) leaves the current index and its prefix counts too, which
        // need the row's indexed values.
        if content_identity::version_manifest::has_indexed_vector(body, &declarations)?
            || match declarations.first() {
                Some(declaration) => {
                    content_identity::version_manifest::scalar_posting_for_body(body, declaration)?
                        .is_some_and(|posting| posting.deleted.is_none())
                }
                None => false,
            }
        {
            {
                #[cfg(test)]
                let _observation =
                    content_identity::proof_allocation_oracle_tests::declaration_observation();
                drop(declarations);
            }
            memory.release(retained);
            return self.prune_relational_posting_intervals_in_write(
                write,
                table,
                Some(meta),
                body,
                commitments,
                identity_mutations,
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
            let index = match write.open_table(definition) {
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
            .map_err(|e| Error::Other((format!("posting identity: {e}")).into()))?;
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
            .map_err(|e| Error::Other((format!("posting identity: {e}")).into()))?;
            // Reads above borrow the posting key; the write goes through the
            // table that folds the postings commitment.
            drop(index);
            let mut index =
                historical_index_commitment::HistoricalPostings::open(write, table, &declaration)
                    .map_err(Self::storage_error)?;
            let position = DurableRelationalIndexKey(posting.key);
            if start < posting.created {
                index.put(commitments, write, position, Some(posting.created))?;
            } else {
                index.remove(commitments, write, &position)?;
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
        commitments: &mut historical_index_commitment::CommitmentBatch,
        identity_mutations: &mut content_identity::OriginalIdentityMutations,
    ) -> Result<()> {
        let Some(meta) = meta else {
            return Ok(());
        };
        // Row decoding accounts for its own allocations. Pre-admit the key
        // copies, canonical encoding buffers and declaration controls before
        // constructing any of them; all are released by the caller's row scope.
        let mut meta_size = bincode::enc::write::SizeWriter::default();
        bincode::serde::encode_into_writer(meta, &mut meta_size, bincode::config::standard())
            .map_err(|error| Error::Other((format!("bincode size error: {error}")).into()))?;
        crate::read_image_memory::reserve(
            body.len()
                .saturating_mul(16)
                .saturating_add(meta_size.bytes_written.saturating_mul(8))
                .saturating_add(4096),
        )?;
        let (planned, _, prefix_slots) = crate::executor::relational_declaration_plan(meta)?;
        let metadata_allowance = meta_size
            .bytes_written
            .saturating_mul(8)
            .saturating_add(4096);
        crate::read_image_memory::reserve(planned.saturating_sub(metadata_allowance))?;
        let row = Self::decode_versioned_row(body, Some(meta))?;
        // `None` is a live version that retention erases: its lifetime runs to
        // the open end, and the row leaves the current index with it.
        let end = row.deleted_tx;
        let maintain_counts = end.is_none() && {
            let ready = write
                .open_table(RELATIONAL_INDEX_READY_TABLE)
                .map_err(Self::storage_error)?;
            ready
                .get(table)
                .map_err(Self::storage_error)?
                .is_some_and(|version| version.value() == RELATIONAL_INDEX_READY_VERSION)
        };
        // A maintained count is keyed by table, the prefix's columns with
        // their directions, and the row's values under them -- not by index:
        // two indexes over one prefix share it, and an erased row leaves it
        // once. For one row the values are fixed, so the prefix's columns
        // alone say which count a decrement already reached; the record is
        // index metadata, never a copy of the row's values.
        let declarations = {
            #[cfg(test)]
            let _observation =
                content_identity::proof_allocation_oracle_tests::declaration_observation();
            Self::relational_index_declarations(meta)
        };
        let mut decremented: Vec<(usize, usize)> = Vec::with_capacity(prefix_slots);
        for (declaration_at, declaration) in declarations.iter().enumerate() {
            let name = Self::relational_index_table_name(table, &declaration.name);
            let mut index = match historical_index_commitment::HistoricalPostings::open(
                write,
                table,
                declaration,
            ) {
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
                || end.is_some_and(|end| end <= row.created_tx)
                || posting_end.is_some_and(|limit| end.is_none_or(|end| limit < end))
            {
                return Err(corrupt(
                    "reclaimed row lifetime disagrees with historical index posting",
                ));
            }
            let old_key = DurableRelationalIndexKey(encoded);
            let left = position.created_tx < row.created_tx;
            let right = end.is_some_and(|end| posting_end.is_none_or(|limit| end < limit));
            let right_key = match end {
                Some(end) if right => Some(DurableRelationalIndexKey::new(
                    normalized.clone(),
                    row.row_id,
                    end,
                )?),
                _ => None,
            };
            let right_value = right.then_some(posting_end);
            let left_value = left.then_some(Some(row.created_tx));
            let current_name = Self::current_relational_index_table_name(table, &declaration.name);
            let current_definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                TableDefinition::new(&current_name);
            let mut current = write
                .open_table(current_definition)
                .map_err(Self::storage_error)?;
            let logical = DurableRelationalIndexKey::new(normalized.clone(), row.row_id, TxId(0))?;
            let next_current = if posting_end.is_none() {
                let stamp = current
                    .get(&logical)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode_exact::<TxId>(value.value()))
                    .transpose()?;
                if stamp != Some(position.created_tx) || (end.is_some() && !right) {
                    return Err(corrupt(
                        "open historical posting disagrees with current index",
                    ));
                }
                end.map(|end| Self::encode(&end)).transpose()?
            } else {
                None
            };
            index.remove(commitments, write, &old_key)?;
            if let Some(value) = left_value {
                index.put(commitments, write, old_key, value)?;
            }
            if let (Some(key), Some(value)) = (right_key, right_value) {
                index.put(commitments, write, key, value)?;
            }
            if let Some(value) = next_current {
                current
                    .insert(logical, value.as_slice())
                    .map_err(Self::storage_error)?;
            } else if end.is_none() {
                // The erased live row leaves the current index, its content
                // identity and every maintained prefix count, as a delete does.
                current.remove(&logical).map_err(Self::storage_error)?;
                let mutation = content_identity::remove_posting_from_original(
                    identity_mutations.original(),
                    table,
                    &declaration.name,
                    &logical.0,
                )?;
                identity_mutations.push_original(mutation)?;
                if maintain_counts {
                    let mut counts = write
                        .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                        .map_err(Self::storage_error)?;
                    for prefix_len in 1..=normalized.len() {
                        if !crate::maintained_counts::maintained_count_member(
                            meta,
                            &declaration.columns[..prefix_len],
                        ) {
                            continue;
                        }
                        let prefix = &declaration.columns[..prefix_len];
                        if decremented.iter().any(|(earlier, earlier_len)| {
                            *earlier_len == prefix_len
                                && declarations[*earlier].columns[..prefix_len] == *prefix
                        }) {
                            continue;
                        }
                        {
                            #[cfg(test)]
                            let _observation = content_identity::proof_allocation_oracle_tests::declaration_observation();
                            decremented.push((declaration_at, prefix_len));
                        }
                        let count_key = Self::relational_index_count_key(
                            table,
                            prefix,
                            &normalized[..prefix_len],
                        )?;
                        let next = counts
                            .get(count_key.as_slice())
                            .map_err(Self::storage_error)?
                            .map(|count| count.value())
                            .unwrap_or(0)
                            .checked_sub(1)
                            .ok_or_else(|| Error::StoreCorrupted {
                                path: "relational index count table".to_owned(),
                                reason: "live index-prefix cardinality underflowed".to_owned(),
                            })?;
                        if next == 0 {
                            counts
                                .remove(count_key.as_slice())
                                .map_err(Self::storage_error)?;
                            let mutation = content_identity::count_from_original(
                                identity_mutations.original(),
                                RELATIONAL_INDEX_COUNT_IDENTITY_NAMESPACE,
                                &count_key,
                                None,
                            )?;
                            identity_mutations.push_original(mutation)?;
                        } else {
                            counts
                                .insert(count_key.as_slice(), next)
                                .map_err(Self::storage_error)?;
                            let mutation = content_identity::count_from_original(
                                identity_mutations.original(),
                                RELATIONAL_INDEX_COUNT_IDENTITY_NAMESPACE,
                                &count_key,
                                Some(next),
                            )?;
                            identity_mutations.push_original(mutation)?;
                        }
                    }
                }
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
        let writer = persistence.proof_accountant().unwrap_or_else(|| {
            Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()).into()
        });
        persistence.attach_memory_accountant(writer.clone());
        assert_eq!(
            persistence.proof_accountant().unwrap().as_ptr(),
            writer.as_ptr()
        );
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

    /// A maintained count is keyed by table, columns and values, not by
    /// index. Erasing one expired live row whose two indexes share the
    /// counted prefix takes it out of that count once.
    #[test]
    fn erasing_a_live_row_leaves_a_count_shared_by_two_indexes_once() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shared-count.db");
        let db = crate::Database::open(&path).unwrap();
        for sql in [
            "CREATE TABLE counted (id INTEGER PRIMARY KEY, ctx TEXT, a INTEGER, b INTEGER)",
            "CREATE INDEX counted_by_ctx_a ON counted(ctx, a)",
            "CREATE INDEX counted_by_ctx_b ON counted(ctx, b)",
            "ALTER TABLE counted SET MAINTAIN COUNT (ctx)",
            "INSERT INTO counted VALUES (1, 'x', 1, 1), (2, 'x', 2, 2)",
        ] {
            db.execute(sql, &HashMap::new()).unwrap();
        }
        let meta = db.table_meta("counted").unwrap();
        let live = db
            .scan("counted", db.snapshot())
            .unwrap()
            .into_iter()
            .find(|row| row.values.get("id") == Some(&Value::Int64(1)))
            .unwrap();
        db.close().unwrap();
        let _writer_registration =
            crate::memory_accounting::MemoryAccountant::writer_budget(64 * 1024 * 1024).unwrap();
        let writer_handle = _writer_registration.accountant();
        // open_with_cache -> truncate -> with_db -> reader_decode_scope installs
        // default_decode_accountant into the empty slot unless a writer image for
        // this handle is already active. Hold that image across open, then drop
        // it once attach has kept this same pointer.
        let _open_writer = crate::read_image_memory::ImageMemory::enter_writer_operation(
            writer_handle.clone(),
        )
        .unwrap();
        let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
        persistence.attach_memory_accountant(writer_handle.clone());
        let writer = persistence.proof_accountant().unwrap();
        assert_eq!(writer.as_ptr(), writer_handle.as_ptr());
        drop(_open_writer);
        writer.set_budget(Some(64 * 1024 * 1024)).unwrap();
        let declaration = RedbPersistence::relational_index_declarations(&meta)
            .into_iter()
            .find(|index| index.name == "counted_by_ctx_a")
            .unwrap();
        let normalized =
            contextdb_relational::index_key_for_row(&declaration.columns, &live.values);
        let count_key = RedbPersistence::relational_index_count_key(
            "counted",
            &declaration.columns[..1],
            &normalized[..1],
        )
        .unwrap();
        persistence
            .with_db(|database| {
                let count = || {
                    database
                        .begin_read()
                        .unwrap()
                        .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                        .unwrap()
                        .get(count_key.as_slice())
                        .unwrap()
                        .map(|count| count.value())
                };
                assert_eq!(count(), Some(2), "fixture: two rows under ctx = 'x'");
                let _scope = persistence.writer_decode_scope()?;
                let body = RedbPersistence::encode_versioned_row(&live, Some(&meta))?;
                let write = database
                    .begin_write()
                    .map_err(RedbPersistence::storage_error)?;
                let funded_original = crate::read_image_memory::active_owner()?.preparation_original()?;
                let mut outputs = content_identity::OriginalIdentityMutations::new(&funded_original, 0)?;
                persistence.prune_relational_posting_intervals_in_write(
                    &write,
                    "counted",
                    Some(&meta),
                    &body,
                    &mut historical_index_commitment::CommitmentBatch::for_writer()?,
                    &mut outputs,
                )?;
                write.commit().map_err(RedbPersistence::storage_error)?;
                assert_eq!(
                    count(),
                    Some(1),
                    "the erased row leaves the count its two indexes share exactly once"
                );
                Ok(())
            })
            .unwrap();
    }

    /// Short names make serialized metadata smaller than its native owned
    /// declarations. Prefix directions deliberately prevent count deduplication.
    #[test]
    #[serial_test::serial]
    fn short_index_declarations_are_funded_before_native_allocation() {
        use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("short-declarations.db");
        let db = crate::Database::open(&path).unwrap();
        let columns = ('a'..='z')
            .map(|name| format!("{name} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(
            &format!("CREATE TABLE short ({columns}) RETAIN 1 HOURS SYNC OFF"),
            &HashMap::new(),
        )
        .unwrap();
        for index in 0..1000 {
            let columns = ('a'..='z')
                .enumerate()
                .map(|(position, name)| {
                    let direction = if position < 10 && index & (1 << position) != 0 {
                        "DESC"
                    } else {
                        "ASC"
                    };
                    format!("{name} {direction}")
                })
                .collect::<Vec<_>>()
                .join(", ");
            db.execute(
                &format!("CREATE INDEX i{index:03} ON short ({columns})"),
                &HashMap::new(),
            )
            .unwrap();
        }
        db.execute(
            "ALTER TABLE short SET MAINTAIN COUNT ALL PREFIXES",
            &HashMap::new(),
        )
        .unwrap();
        let values = std::iter::repeat_n("NULL", 26)
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(
            &format!("INSERT INTO short VALUES ({values})"),
            &HashMap::new(),
        )
        .unwrap();
        let row = db.scan("short", db.snapshot()).unwrap().remove(0);
        let meta = db.table_meta("short").unwrap();
        assert_eq!(
            meta.indexes.len(),
            1000,
            "all declarations passed the production DDL path"
        );
        db.close().unwrap();
        let _writer_registration =
            crate::memory_accounting::MemoryAccountant::writer_budget(64 * 1024 * 1024).unwrap();
        let writer_handle = _writer_registration.accountant();
        // open_with_cache -> truncate -> with_db -> reader_decode_scope installs
        // default_decode_accountant into the empty slot unless a writer image for
        // this handle is already active. Hold that image across open, then drop
        // it once attach has kept this same pointer.
        let _open_writer = crate::read_image_memory::ImageMemory::enter_writer_operation(
            writer_handle.clone(),
        )
        .unwrap();
        let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
        persistence.attach_memory_accountant(writer_handle.clone());
        let writer = persistence.proof_accountant().unwrap();
        assert_eq!(writer.as_ptr(), writer_handle.as_ptr());
        drop(_open_writer);
        writer.set_budget(Some(64 * 1024 * 1024)).unwrap();
        persistence.with_db(|database| {
            let body_scope = persistence.writer_decode_scope()?;
            let body = RedbPersistence::encode_versioned_row(&row, Some(&meta))?;
            drop(body_scope);
            let mut size = bincode::enc::write::SizeWriter::default();
            bincode::serde::encode_into_writer(&meta, &mut size, bincode::config::standard()).unwrap();
            let grant = size.bytes_written * 8 + 4096;
            let invoke = |budget| -> Result<(Result<()>, usize)> {
                writer.set_budget(Some(budget))?;
                let _scope = persistence.writer_decode_scope()?;
                let write = database.begin_write().map_err(RedbPersistence::storage_error)?;
                let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
                let funded_original = crate::read_image_memory::active_owner()?.preparation_original()?;
                let mut outputs = content_identity::OriginalIdentityMutations::new(&funded_original, 0)?;
                let observed = oracle::funded_requests(&writer, || {
                    persistence.prune_terminal_relational_posting_in_write(
                        &write, "short", Some(&meta), &body, &mut commitments, &mut outputs)
                });
                drop(write); // Every attempted prune is rolled back.
                Ok(observed)
            };
            let baseline = writer.usage().used;
            let (short, gap) = invoke(baseline + grant - 1)?;
            assert!(matches!(short, Err(Error::MemoryBudgetExceeded { .. })),
                "one byte short refuses before declarations: {short:?}");
            assert_eq!(gap, 0, "refusal allocates no unfunded declaration");
            let (exact, exact_gap) = invoke(baseline + grant)?;
            let (ample, gap) = invoke(64 * 1024 * 1024)?;
            assert_eq!(gap, 0,
                "all declaration containers and overlapping schema clones are admitted before allocation");
            assert!(exact.is_ok() || matches!(exact, Err(Error::MemoryBudgetExceeded { .. })),
                "exact admission succeeds or refuses with the typed budget error: {exact:?}");
            assert_eq!(exact_gap, 0, "exact admission covers actual native capacities");
            assert!(ample.is_ok(), "sufficient headroom cannot fail a valid schema: {ample:?}");
            Ok(())
        }).unwrap();
    }

    /// Erasing a live row holds nothing per maintained prefix: a count key
    /// carries the row's prefix values, so retaining one per prefix would
    /// hold a wide leading value once for every prefix it leads.
    #[test]
    #[serial_test::serial]
    fn erasing_a_wide_live_row_retains_no_count_key_per_prefix() {
        const WIDTH: usize = 256 * 1024;
        const COLUMNS: usize = 64;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wide-count.db");
        let db = crate::Database::open(&path).unwrap();
        let trailing = (1..COLUMNS)
            .map(|column| format!("c{column} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ");
        let indexed = (1..COLUMNS)
            .map(|column| format!("c{column}"))
            .collect::<Vec<_>>()
            .join(", ");
        for sql in [
            format!("CREATE TABLE wide (id INTEGER PRIMARY KEY, lead TEXT, {trailing})"),
            format!("CREATE INDEX wide_by_lead ON wide(lead, {indexed})"),
            "ALTER TABLE wide SET MAINTAIN COUNT ALL PREFIXES".to_owned(),
        ] {
            db.execute(&sql, &HashMap::new()).unwrap();
        }
        let values = (1..COLUMNS)
            .map(|column| column.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        db.execute(
            &format!("INSERT INTO wide VALUES (1, $lead, {values})"),
            &HashMap::from([("lead".to_owned(), Value::Text("w".repeat(WIDTH)))]),
        )
        .unwrap();
        let meta = db.table_meta("wide").unwrap();
        let live = db.scan("wide", db.snapshot()).unwrap().remove(0);
        db.close().unwrap();
        let _writer_registration =
            crate::memory_accounting::MemoryAccountant::writer_budget(256 * 1024 * 1024).unwrap();
        let writer_handle = _writer_registration.accountant();
        let engine_control_baseline = writer_handle.usage().used;
        // open_with_cache -> truncate -> with_db -> reader_decode_scope installs
        // default_decode_accountant into the empty slot unless a writer image for
        // this handle is already active. Hold that image across open, then drop
        // it once attach has kept this same pointer.
        let _open_writer = crate::read_image_memory::ImageMemory::enter_writer_operation(
            writer_handle.clone(),
        )
        .unwrap();
        let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
        persistence.attach_memory_accountant(writer_handle.clone());
        let writer = persistence.proof_accountant().unwrap();
        assert_eq!(writer.as_ptr(), writer_handle.as_ptr());
        drop(_open_writer);
        writer.set_budget(Some(256 * 1024 * 1024)).unwrap();
        persistence
            .with_db(|database| {
                let _scope = persistence.writer_decode_scope()?;
                let body = RedbPersistence::encode_versioned_row(&live, Some(&meta))?;
                let pinned = database.begin_read().map_err(RedbPersistence::storage_error)?;
                let capture = |read: &redb::ReadTransaction| {
                    let counts = read.open_table(RELATIONAL_INDEX_COUNT_TABLE).unwrap()
                        .iter().unwrap().map(|entry| {
                            let (key, value) = entry.unwrap();
                            (key.value().to_vec(), value.value())
                        }).collect::<Vec<_>>();
                    let name = RedbPersistence::rel_table_name("wide");
                    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(&name);
                    let rows = read.open_table(definition).unwrap().iter().unwrap()
                        .map(|entry| { let (key, value) = entry.unwrap();
                            (key.value().to_vec(), value.value().to_vec()) }).collect::<Vec<_>>();
                    let name = RedbPersistence::relational_index_table_name("wide", "wide_by_lead");
                    let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> = TableDefinition::new(&name);
                    let postings = read.open_table(definition).unwrap().iter().unwrap()
                        .map(|entry| { let (key, value) = entry.unwrap();
                            (key.value().0, value.value().to_vec()) }).collect::<Vec<_>>();
                    (counts, rows, postings)
                };
                let before = capture(&pinned);
                let mut size = bincode::enc::write::SizeWriter::default();
                bincode::serde::encode_into_writer(&meta, &mut size, bincode::config::standard()).unwrap();
                let grant = body.len() * 16 + size.bytes_written * 8 + 4096;
                for extra in [grant - 1, grant] {
                    let baseline = writer.usage().used;
                    writer.set_budget(Some(baseline + extra))?;
                    let row_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
                    let attempt = database.begin_write().map_err(RedbPersistence::storage_error)?;
                    let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
                    let funded_original = crate::read_image_memory::active_owner()?.preparation_original()?;
                    let mut outputs = content_identity::OriginalIdentityMutations::new(&funded_original, 0)?;
                    let result = persistence.prune_relational_posting_intervals_in_write(
                        &attempt, "wide", Some(&meta), &body, &mut commitments, &mut outputs);
                    if extra == grant - 1 {
                        assert!(matches!(result, Err(Error::MemoryBudgetExceeded { .. })),
                            "one byte short refuses atomically: {result:?}");
                    } else {
                        assert!(result.is_ok() || matches!(result, Err(Error::MemoryBudgetExceeded { .. })),
                            "exact initial grant succeeds or reports additional physical admission: {result:?}");
                    }
                    drop(outputs);
                    drop(commitments);
                    drop(attempt);
                    drop(row_scope);
                    writer.set_budget(Some(256 * 1024 * 1024))?;
                    assert_eq!(capture(&database.begin_read().unwrap()), before,
                        "refused or rolled-back work preserves counts, postings and row manifest");
                    assert_eq!(capture(&pinned), before, "the pinned committed view stays identical");
                }
                let write = database
                    .begin_write()
                    .map_err(RedbPersistence::storage_error)?;
                let baseline = writer.usage().used;
                let funded_original = crate::read_image_memory::active_owner()?.preparation_original()?;
                let mut outputs = content_identity::OriginalIdentityMutations::new(&funded_original, 0)?;
                let mut commitments = historical_index_commitment::CommitmentBatch::for_writer()?;
                // The erase's outputs (one identity mutation per changed
                // count, each carrying its key) leave with the caller, as a
                // delete's do; what it may not hold is anything beyond them.
                let (erased, peak, held) =
                    crate::persistence::content_identity::proof_allocation_oracle_tests::measured(
                        || {
                            persistence.prune_relational_posting_intervals_in_write(
                                &write,
                                "wide",
                                Some(&meta),
                                &body,
                                &mut commitments,
                                &mut outputs,
                            )
                        },
                    );
                erased?;
                let transient = peak.saturating_sub(held);
                assert!(
                    transient < 24 * WIDTH,
                    "erasing a row whose {WIDTH}-byte value leads {COLUMNS} counted prefixes holds \
                     no key per prefix beyond its outputs: peak {peak}, outputs {held}, \
                     transient {transient} bytes"
                );
                // The caller keeps these outputs after the production row
                // scope has refunded its temporary decoding allowance.
                drop(_scope);
                drop(commitments);
                let output_backing = outputs.mutations.capacity()
                    * std::mem::size_of::<content_identity::ContentIdentityMutation>();
                let payload = output_backing
                    + outputs.mutations.iter().map(|mutation| {
                        mutation.logical_key.capacity()
                            + mutation.logical_value.as_ref().map_or(0, content_identity::LogicalMutationValue::heap_capacity)
                    }).sum::<usize>();
                let payload_capacities = payload - output_backing;
                assert!(payload >= COLUMNS * WIDTH);
                let requests = outputs.mutations.iter().flat_map(|mutation| {
                    std::iter::once((mutation.logical_key.as_ptr() as usize,
                        mutation.logical_key.capacity(), 1))
                        .chain(mutation.logical_value.iter().filter(|value| value.heap_capacity() != 0)
                            .map(|value| (value.as_ptr() as usize, value.heap_capacity(), 1)))
                }).collect::<Vec<_>>();
                use crate::persistence::content_identity::proof_allocation_oracle_tests as oracle;
                let counter = writer.writer_registration_observation_for_test().4;
                oracle::watch_source_free_for_test(&requests, counter);
                let funded_after_scope = writer.usage().used;
                drop(outputs);
                assert!(
                    funded_after_scope.saturating_sub(writer.usage().used)
                        >= payload_capacities + output_backing,
                    "escaped output payload capacities plus output backing refund from the original payer while the writer registration and persistence remain alive: refunded={}, payload={payload}, payload_capacities={payload_capacities}, output_backing={output_backing}, charge_after={}",
                    funded_after_scope.saturating_sub(writer.usage().used),
                    writer.usage().used
                );
                let frees = oracle::source_free_for_test(requests.len());
                oracle::disarm_source_free_watch();
                assert!(
                    funded_after_scope >= payload,
                    "surviving count/current-index output capacities stay charged to the original writer: \
                     payload={payload}, charge={funded_after_scope}, in-scope baseline={baseline}"
                );
                let mut remaining = requests.iter().map(|request| request.1).sum::<usize>();
                for (request, (before, after, count)) in requests.iter().zip(frees) {
                    assert_eq!(count, 1, "each output buffer physically frees exactly once");
                    // Mutations drop in order, key followed by optional value.
                    assert!(before >= remaining && after >= remaining,
                        "output free must precede its invoice refund: remaining={remaining}, before={before}, after={after}");
                    remaining -= request.1;
                }
                Ok(())
            })
            .unwrap();
        drop(persistence);
        assert_eq!(
            writer.usage().used
                .checked_sub(engine_control_baseline)
                .expect("no underfunded controls"),
            0,
            "the original writer refunds after its final outputs free"
        );
        drop(_writer_registration);
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
        let _writer_registration =
            crate::memory_accounting::MemoryAccountant::writer_budget(64 * 1024 * 1024).unwrap();
        let writer_handle = _writer_registration.accountant();
        let engine_control_baseline = writer_handle.usage().used;
        // open_with_cache -> truncate -> with_db -> reader_decode_scope installs
        // default_decode_accountant into the empty slot unless a writer image for
        // this handle is already active. Hold that image across open, then drop
        // it once attach has kept this same pointer.
        let _open_writer = crate::read_image_memory::ImageMemory::enter_writer_operation(
            writer_handle.clone(),
        )
        .unwrap();
        let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
        persistence.attach_memory_accountant(writer_handle.clone());
        let writer = persistence.proof_accountant().unwrap();
        assert_eq!(writer.as_ptr(), writer_handle.as_ptr());
        drop(_open_writer);
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
                    let funded_original = crate::read_image_memory::active_owner()?.preparation_original()?;
                    let mut outputs = content_identity::OriginalIdentityMutations::new(&funded_original, 0)?;
                    persistence.prune_relational_posting_intervals_in_write(
                        &write,
                        "points",
                        Some(&meta),
                        &body,
                        &mut historical_index_commitment::CommitmentBatch::for_writer()?,
                        &mut outputs,
                    )?;
                    drop(outputs);
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
            writer.usage().used
                .checked_sub(engine_control_baseline)
                .expect("no underfunded controls"),
            0,
            "all source and scratch owners refund"
        );
        drop(_writer_registration);
    }
}
