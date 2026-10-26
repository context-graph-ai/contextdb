use super::*;

impl RedbPersistence {
    /// Every raw config value whose key starts with `prefix`, in key order.
    /// Callers that treat the stored bytes as an authority boundary can apply
    /// their own exact decoder without broadening generic config compatibility.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from erasure_frontier_contract_tests.rs"
        )
    )]
    pub(crate) fn load_config_values_raw_with_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut out = Vec::new();
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_enumeration(key);
                out.push((key.to_string(), value.value().to_vec()));
            }
            Ok(out)
        })
    }

    /// The raw config entries under `prefix`, loaded in two passes of one
    /// read transaction: the first sizes them through borrowed guards and
    /// passes the exact load bytes (slots, key and value copies) to `admit`;
    /// only then does the second copy them into an exact-capacity list.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_config_values_raw_admitted(
        &self,
        prefix: &str,
        admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let overflow = || Error::Other("config load size overflow".into());
            let mut count = 0usize;
            let mut bytes = 0usize;
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                count = count.checked_add(1).ok_or_else(overflow)?;
                bytes = bytes
                    .checked_add(key.len())
                    .and_then(|b| b.checked_add(value.value().len()))
                    .ok_or_else(overflow)?;
            }
            if count == 0 {
                return Ok(Vec::new());
            }
            let slots = count
                .checked_mul(std::mem::size_of::<(String, Vec<u8>)>())
                .and_then(|b| b.checked_add(bytes))
                .ok_or_else(overflow)?;
            admit(slots)?;
            let mut out = Vec::with_capacity(count);
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                if out.len() == count {
                    return Err(Error::Other(
                        "config entries changed inside one read".into(),
                    ));
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_enumeration(key);
                out.push((key.to_string(), value.value().to_vec()));
            }
            Ok(out)
        })
    }

    /// The raw config value for exactly `key`, if present.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_config_value_raw(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(err) => return Err(Self::storage_error(err)),
            };
            Ok(config_table
                .get(key)
                .map_err(Self::storage_error)?
                .map(|value| value.value().to_vec()))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn config_value_exists(&self, key: &str) -> Result<bool> {
        self.with_db(|db| {
            let txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match txn.open_table(if policy_hold_index_key(key) {
                POLICY_HOLD_INDEX_TABLE
            } else {
                CONFIG_TABLE
            }) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
                Err(error) => return Err(Self::storage_error(error)),
            };
            Ok(table.get(key).map_err(Self::storage_error)?.is_some())
        })
    }

    /// The borrowed encoded value is admitted before the decoder can allocate.
    /// The caller retains the lease for as long as it keeps the decoded record.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_config_value_admitted<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
        memory: &crate::read_image_memory::ImageMemoryHandle,
    ) -> Result<Option<T>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match txn.open_table(if policy_hold_index_key(key) {
                POLICY_HOLD_INDEX_TABLE
            } else {
                CONFIG_TABLE
            }) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let Some(value) = table.get(key).map_err(Self::storage_error)? else {
                return Ok(None);
            };
            memory.reserve(value.value().len())?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_config_decode(key);
            memory.scope(|| Self::decode(value.value())).map(Some)
        })
    }

    /// Point-read one bounded set of config keys in request order. Each
    /// borrowed value is admitted before either decoding or making an owned
    /// copy; unrelated values in the table are never visited.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_config_values_for_keys_admitted<T: serde::de::DeserializeOwned>(
        &self,
        keys: &[String],
        memory: &crate::read_image_memory::ImageMemoryHandle,
    ) -> Result<Vec<T>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut values = Vec::new();
            for key in keys {
                let Some(value) = table.get(key.as_str()).map_err(Self::storage_error)? else {
                    continue;
                };
                memory.reserve(value.value().len())?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_decode(key);
                values.push(memory.scope(|| Self::decode(value.value()))?);
            }
            Ok(values)
        })
    }

    /// Selection indexes contain physical primary keys. Scan only the requested
    /// scoped runs, then point-read those primary records under one retained lease.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_indexed_config_values<T: serde::de::DeserializeOwned>(
        &self,
        mut keys: BTreeSet<String>,
        prefixes: &[String],
        memory: &crate::read_image_memory::ImageMemoryHandle,
    ) -> Result<Vec<(String, T)>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let indexes = match txn.open_table(POLICY_HOLD_INDEX_TABLE) {
                Ok(table) => Some(table),
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(error) => return Err(Self::storage_error(error)),
            };
            for prefix in prefixes {
                let Some(indexes) = &indexes else {
                    break;
                };
                let upper = writer_source::prefix_end(prefix.as_bytes())
                    .and_then(|end| String::from_utf8(end).ok());
                let upper = upper
                    .as_deref()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
                for entry in indexes
                    .range::<&str>((std::ops::Bound::Included(prefix.as_str()), upper))
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if !key.value().starts_with(prefix) {
                        break;
                    }
                    memory.reserve(value.value().len().saturating_add(128))?;
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::writer_query_test_seams::note_selection_entry();
                    let primary: String = memory.scope(|| Self::decode(value.value()))?;
                    keys.insert(primary);
                }
            }
            let mut values = Vec::new();
            for key in keys {
                let Some(value) = table.get(key.as_str()).map_err(Self::storage_error)? else {
                    continue;
                };
                memory.reserve(value.value().len().saturating_add(key.len()))?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_enumeration(&key);
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_config_decode(&key);
                let decoded = memory.scope(|| Self::decode(value.value()))?;
                values.push((key, decoded));
            }
            Ok(values)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_config_value<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let value = match config_table.get(key).map_err(Self::storage_error)? {
                Some(value) => {
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::writer_query_test_seams::note_config_decode(key);
                    Some(Self::decode(value.value())?)
                }
                None => None,
            };
            Ok(value)
        })
    }

    /// Fixture-only corruption with a matching REAL rooted manifest. The
    /// caller changes only the existing JSON text bytes, so physical identity,
    /// encoded length, row versions and all lifecycle metadata remain intact.
    #[cfg(all(test, feature = "test-seams"))]
    pub(crate) fn malformed_count_json_for_test(
        &self,
        accountant: crate::memory_accounting::AccountantHandle,
        table: &str,
        last: bool,
    ) -> Result<()> {
        let original = crate::memory_accounting::OwnedMemoryReservation::new(accountant);
        let storage =
            native_storage_admission::StorageAdmissionScope::writer_original_native(&original)
                .map_err(Error::NativeEngineOther)?;
        storage
            .run_native(|| {
                self.with_db(|db| {
                    let write = Self::begin_history_write(db)?;
                    let table_name = Self::rel_table_name(table);
                    let definition: TableDefinition<&[u8], &[u8]> =
                        TableDefinition::new(&table_name);
                    {
                        let mut relation =
                            write.open_table(definition).map_err(Self::storage_error)?;
                        let (key, old) = {
                            let mut rows = relation.iter().map_err(Self::storage_error)?;
                            let (key, body) = if last { rows.next_back() } else { rows.next() }
                                .transpose()
                                .map_err(Self::storage_error)?
                                .expect("fixture row");
                            (key.value().to_vec(), body.value().to_vec())
                        };
                        let mut new = old.clone();
                        let offset = new
                            .windows(5)
                            .position(|window| window == [0, 7, 2, b'{', b'}'])
                            .expect("fixture has plain JSON text {}");
                        new[offset + 4] = b'x';
                        self.replace_relational_manifest_in_write_original(
                            &original,
                            &write,
                            table,
                            &key,
                            Some(&old),
                            Some(&new),
                        )?;
                        relation
                            .insert(key.as_slice(), new.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                    write.commit().map_err(Self::storage_error)?;
                    Ok(())
                })
            })
            .map_err(native_storage_admission::original_storage_error)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn decode_relational_source_row(
        table: &str,
        meta: &TableMeta,
        bytes: &[u8],
    ) -> Result<VersionedRow> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        contextdb_core::find_attribution::count("decoded_rows", 1);
        #[cfg(any(test, feature = "test-seams"))]
        {
            crate::read_work_probe::note_decoded_row();
            crate::read_work_probe::note_stored_row_bytes(bytes.len());
        }
        let row = Self::decode_versioned_row(bytes, Some(meta))
            .map_err(|error| Self::relational_decode_error(table, error))?;
        validate_read_row(table, &row, meta).map_err(|error| {
            crate::funded_error::corruption_display(
                crate::read_image_memory::active_owner()
                    .and_then(|owner| owner.diagnostic_budget()),
                table,
                error,
            )
        })?;
        Ok(row)
    }

    /// The stored byte length of each named row version, keyed as it was
    /// asked for. A key the store does not hold is absent from the answer.
    ///
    /// This is what a version physically occupies, which is not what it
    /// occupies in memory: a committed body the in-memory directory has shed
    /// still takes its full room in the file, and a reclaim or size answer
    /// that described only the resident projection would stop scaling with
    /// what was actually stored.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn stored_relational_row_bytes(
        &self,
        keys: &[(String, RowId, TxId, Lsn)],
    ) -> Result<HashMap<(String, RowId, TxId, Lsn), usize>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        #[cfg(test)]
        stored_relational_row_bytes_fault_for_test()?;
        self.with_db(|db| {
            let mut answer = HashMap::new();
            let read = db.begin_read().map_err(Self::storage_error)?;
            let mut opened: HashMap<&str, Option<redb::ReadOnlyTable<&[u8], &[u8]>>> =
                HashMap::new();
            for (table, row_id, created_tx, lsn) in keys {
                let relation = match opened.entry(table.as_str()) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let name = Self::rel_table_name(table);
                        let definition: TableDefinition<&[u8], &[u8]> =
                            TableDefinition::new(name.as_str());
                        let opened = match read.open_table(definition) {
                            Ok(relation) => Some(relation),
                            Err(redb::TableError::TableDoesNotExist(_)) => None,
                            Err(error) => return Err(Self::relational_table_error(table, error)),
                        };
                        entry.insert(opened)
                    }
                };
                let Some(relation) = relation.as_ref() else {
                    continue;
                };
                let key = Self::rel_row_key_from_parts(*row_id, *created_tx, *lsn);
                let Some(stored) = relation
                    .get(key.as_slice())
                    .map_err(|error| Self::relational_storage_error(table, error))?
                else {
                    continue;
                };
                // A borrowed slice into the page redb already holds: reading
                // its length copies nothing.
                let bytes = stored.value().len();
                answer.insert((table.clone(), *row_id, *created_tx, *lsn), bytes);
            }
            Ok(answer)
        })
    }

    /// Load one exact durable row version for a physical MVCC scan.  Point
    /// lookup resolves by snapshot; a physical cursor already names the
    /// version and must not turn two retained versions into two copies of the
    /// newest body.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from empty_declared_index_readiness_tests.rs"
        )
    )]
    pub(crate) fn load_relational_row_version(
        &self,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        created_tx: TxId,
        lsn: Lsn,
    ) -> Result<Option<VersionedRow>> {
        let _decode_scope = self.reader_decode_scope()?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_row_version_point_read();
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let name = Self::rel_table_name(table);
            let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
            let relation = match read.open_table(definition) {
                Ok(relation) => relation,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::relational_table_error(table, error)),
            };
            let key = Self::rel_row_key_from_parts(row_id, created_tx, lsn);
            let Some(value) = relation
                .get(key.as_slice())
                .map_err(|error| Self::relational_storage_error(table, error))?
            else {
                return Ok(None);
            };
            let row = Self::decode_relational_source_row(table, meta, value.value())?;
            if row.row_id != row_id || row.created_tx != created_tx || row.lsn != lsn {
                return Err(crate::funded_error::corruption_display(
                    crate::read_image_memory::active_owner()
                        .and_then(|owner| owner.diagnostic_budget()),
                    self.path().display(),
                    format_args!(
                        "relational table {table} row body disagrees with its saved version key"
                    ),
                ));
            }
            Ok(Some(row))
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn damage_first_saved_row_for_test(
        &self,
        table: &str,
        swap_body: bool,
    ) -> Result<()> {
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
            let name = Self::rel_table_name(table);
            let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
            {
                let mut relation = write.open_table(definition).map_err(Self::storage_error)?;
                let rows = relation
                    .iter()
                    .map_err(Self::storage_error)?
                    .take(2)
                    .map(|entry| {
                        entry
                            .map(|(key, value)| (key.value().to_vec(), value.value().to_vec()))
                            .map_err(Self::storage_error)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if rows.len() != 2 {
                    return Err(Error::Other(
                        ("saved-row damage fixture requires two versions".to_owned()).into(),
                    ));
                }
                if swap_body {
                    relation
                        .insert(rows[0].0.as_slice(), rows[1].1.as_slice())
                        .map_err(Self::storage_error)?;
                } else {
                    relation
                        .remove(rows[0].0.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)
        })
    }

    /// A relational table with an incompatible versioned-key shape is refused
    /// with the typed format error before its rows are read.
    pub(super) fn unrecognized_relational_layout(name: &str) -> Error {
        Error::LegacyVectorStoreDetected {
            found_format_marker: format!("{name} uses an unrecognized relational key layout"),
            expected_release: CURRENT_FORMAT_VERSION.to_owned(),
        }
    }

    /// Charge one persisted row's decoded allocations before each allocation
    /// occurs. The transient charge remains held through the caller's row
    /// insertion and is released before the next stored row is decoded.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_relational_table_with_admission(
        &self,
        name: &str,
        accountant: &crate::memory_accounting::AccountantHandle,
        visit: impl FnMut(VersionedRow) -> Result<()>,
    ) -> Result<()> {
        self.visit_relational_table_impl(name, accountant, visit)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visit_relational_table_impl(
        &self,
        name: &str,
        accountant: &crate::memory_accounting::AccountantHandle,
        mut visit: impl FnMut(VersionedRow) -> Result<()>,
    ) -> Result<()> {
        let page_memory =
            crate::read_image_memory::ImageMemory::operation_writer(accountant.clone())?;
        let _page_scope = page_memory.enter();
        // The existing page callback's native Arc control precedes allocation.
        page_memory.reserve(
            std::mem::size_of::<crate::read_image_memory::ImageMemoryHandle>()
                + 2 * std::mem::size_of::<usize>(),
        )?;
        let admission = {
            let memory = page_memory.clone();
            Arc::new(move |bytes: usize| {
                memory
                    .reserve(bytes)
                    .map_err(|error| redb::StorageError::AdmissionRefused(Box::new(error)))
            })
                as Arc<dyn Fn(usize) -> std::result::Result<(), redb::StorageError> + Send + Sync>
        };
        let read = || {
            self.with_db(|db| {
                let read_txn = db.begin_read().map_err(Self::storage_error)?;
                let table_meta = Self::load_table_meta_in_read_txn(&read_txn, name)?;
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match read_txn.open_table(table_def) {
                    Ok(table) => {
                        for entry in table.iter().map_err(Self::storage_error)? {
                            let (_, value) = entry.map_err(Self::storage_error)?;
                            let decoded = page_memory.staged_row_copy_owner()?;
                            let row = decoded.scope(|| {
                                Self::decode_versioned_row(value.value(), table_meta.as_ref())
                            })?;
                            #[cfg(feature = "in-process-test-seams")]
                            crate::writer_query_test_seams::note_history(
                                crate::writer_query_test_seams::HistoryFamily::Row,
                            );
                            visit(row)?;
                        }
                        Ok(())
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => Ok(()),
                    Err(redb::TableError::TableTypeMismatch { .. }) => {
                        Err(Self::unrecognized_relational_layout(name))
                    }
                    Err(err) => Err(Self::storage_error(err)),
                }
            })
        };
        redb::with_read_memory_admission_storage(admission, read)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_vector_partition_table_in_read<T: serde::de::DeserializeOwned>(
        &self,
        read_txn: &redb::ReadTransaction,
        definition: TableDefinition<&'static [u8], &'static [u8]>,
        kind: VectorPartitionRecordKind,
        expected_key: impl Fn(&T) -> Vec<u8>,
        prefix: Option<&[u8]>,
    ) -> Result<Vec<T>> {
        let table = match read_txn.open_byte_table_bounded(definition) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut records = Vec::new();
        if let Some(prefix) = prefix {
            for entry in table.prefix_bounded(prefix).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if !key.value().starts_with(prefix) {
                    break;
                }
                #[cfg(any(test, feature = "test-seams"))]
                if kind == VectorPartitionRecordKind::Membership {
                    VECTOR_MEMBERSHIP_RECORDS_DECODED
                        .with(|count| count.set(count.get().saturating_add(1)));
                }
                let record = self.decode_vector_partition_record(kind, value.value())?;
                if expected_key(&record).as_slice() != key.value() {
                    return Err(self.vector_partition_corruption(
                        "table key does not match its checksummed payload",
                    ));
                }
                records.push(record);
            }
        } else {
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                #[cfg(feature = "in-process-test-seams")]
                if kind == VectorPartitionRecordKind::Membership {
                    crate::writer_query_test_seams::note_history(
                        crate::writer_query_test_seams::HistoryFamily::VectorMembership,
                    );
                }
                #[cfg(any(test, feature = "test-seams"))]
                if kind == VectorPartitionRecordKind::Membership {
                    VECTOR_MEMBERSHIP_RECORDS_DECODED
                        .with(|count| count.set(count.get().saturating_add(1)));
                }
                let record = self.decode_vector_partition_record(kind, value.value())?;
                if expected_key(&record).as_slice() != key.value() {
                    return Err(self.vector_partition_corruption(
                        "table key does not match its checksummed payload",
                    ));
                }
                records.push(record);
            }
        }
        Ok(records)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn reset_vector_membership_records_decoded_for_test() {
        VECTOR_MEMBERSHIP_RECORDS_DECODED.with(|count| count.set(0));
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn vector_membership_records_decoded_for_test() -> u64 {
        VECTOR_MEMBERSHIP_RECORDS_DECODED.with(std::cell::Cell::get)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn reset_relational_index_rebuild_rows_decoded_for_test() {
        RELATIONAL_INDEX_REBUILD_ROWS_DECODED.with(|count| count.set(0));
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn relational_index_rebuild_rows_decoded_for_test() -> u64 {
        RELATIONAL_INDEX_REBUILD_ROWS_DECODED.with(std::cell::Cell::get)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_vector_partition_table<T: serde::de::DeserializeOwned>(
        &self,
        definition: TableDefinition<&'static [u8], &'static [u8]>,
        kind: VectorPartitionRecordKind,
        expected_key: impl Fn(&T) -> Vec<u8>,
    ) -> Result<Vec<T>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            self.load_vector_partition_table_in_read(
                &read_txn,
                definition,
                kind,
                expected_key,
                None,
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_memberships(
        &self,
    ) -> Result<Vec<VectorPartitionMembershipRecord>> {
        self.load_vector_partition_table(
            VECTOR_PARTITION_MEMBERSHIP_TABLE,
            VectorPartitionRecordKind::Membership,
            Self::vector_partition_membership_key,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn stream_vector_partition_memberships<T>(
        &self,
        partition: &VectorPartitionRef,
        dimension: usize,
        quantization: VectorQuantization,
        prepare: impl FnOnce(usize) -> Result<T>,
        mut visit: impl FnMut(
            &mut T,
            VectorPartitionMembershipRecord,
            &mut dyn FnMut(&VectorPartitionMembershipRecord) -> Result<(VectorEntry, Option<TxId>)>,
        ) -> Result<()>,
    ) -> Result<T> {
        self.with_db(|db| {
            let read = match vector_membership_source::operation_read_for_persistence(self) {
                Some(read) => read,
                None => Arc::new(db.begin_read().map_err(Self::storage_error)?),
            };
            // Memberships and bodies share this pinned root. The callback must
            // never reacquire persistence ownership while close/recycle waits.
            let mut load_body = |membership: &VectorPartitionMembershipRecord| {
                let mut entries = self.load_vector_bodies_for_memberships_in_read(
                    &read,
                    std::slice::from_ref(membership),
                    dimension,
                    Some(quantization),
                )?;
                Ok(entries.pop().expect("one membership loads one body"))
            };
            stream_raw_partition_memberships(
                &read,
                partition,
                prepare,
                |state, membership| visit(state, membership, &mut load_body),
                |reason| self.vector_partition_corruption(reason),
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn load_vector_partition_memberships_for(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<Vec<VectorPartitionMembershipRecord>> {
        let prefix = Self::vector_partition_prefix(index, partition_key);
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            self.load_vector_partition_table_in_read(
                &read_txn,
                VECTOR_PARTITION_MEMBERSHIP_TABLE,
                VectorPartitionRecordKind::Membership,
                Self::vector_partition_membership_key,
                Some(&prefix),
            )
        })
    }

    /// Materialize only the admitted raw prefix, holding one decoded
    /// membership at a time rather than a partition-sized metadata clone.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_entries_at_frontier(
        &self,
        partition: &VectorPartitionRef,
        covered_tx: TxId,
        dimension: usize,
        raw_count: usize,
    ) -> Result<Vec<VectorEntry>> {
        let prefix = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let memberships = match read.open_byte_table_bounded(VECTOR_PARTITION_MEMBERSHIP_TABLE)
            {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut entries = Vec::with_capacity(raw_count);
            for record in memberships
                .prefix_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = record.map_err(Self::storage_error)?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let membership: VectorPartitionMembershipRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::Membership,
                        value.value(),
                        &partition.index,
                        &partition.partition_key,
                    )?;
                if Self::vector_partition_membership_key(&membership).as_slice() != key.value() {
                    return Err(self.vector_partition_corruption(
                        "maintenance membership key disagrees with its payload",
                    ));
                }
                if membership.vector_created_tx > covered_tx {
                    continue;
                }
                let mut body = self.load_vector_entries_for_memberships_in_read(
                    &read,
                    std::slice::from_ref(&membership),
                    dimension,
                    None,
                )?;
                entries.push(body.pop().expect("one membership loads one body"));
            }
            Ok(entries)
        })
    }

    /// Load only membership versions for one row in one exact partition.
    /// The durable key is partition-first and then row-first, so unrelated
    /// rows in even a very large partition are never decoded for a point
    /// vector lookup.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_memberships_for_row(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        row_id: RowId,
    ) -> Result<Vec<VectorPartitionMembershipRecord>> {
        let prefix = Self::vector_partition_membership_row_prefix(index, partition_key, row_id);
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            self.load_vector_partition_table_in_read(
                &read_txn,
                VECTOR_PARTITION_MEMBERSHIP_TABLE,
                VectorPartitionRecordKind::Membership,
                Self::vector_partition_membership_key,
                Some(&prefix),
            )
        })
    }

    /// Point-load exactly the vector bodies named by one checksummed
    /// partition directory. The membership table supplies placement and
    /// effective visibility; the vector table remains the body authority.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_entries_for_memberships(
        &self,
        memberships: &[VectorPartitionMembershipRecord],
        expected_dimension: usize,
        quantization: Option<VectorQuantization>,
    ) -> Result<Vec<VectorEntry>> {
        if memberships.is_empty() {
            return Ok(Vec::new());
        }
        self.with_db(|db| {
            let read_txn = match vector_membership_source::operation_read_for_persistence(self) {
                Some(read) => read,
                None => Arc::new(db.begin_read().map_err(Self::storage_error)?),
            };
            self.load_vector_entries_for_memberships_in_read(
                &read_txn,
                memberships,
                expected_dimension,
                quantization,
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_vector_entries_for_memberships_in_read(
        &self,
        read_txn: &redb::ReadTransaction,
        memberships: &[VectorPartitionMembershipRecord],
        expected_dimension: usize,
        quantization: Option<VectorQuantization>,
    ) -> Result<Vec<VectorEntry>> {
        self.load_vector_bodies_for_memberships_in_read(
            read_txn,
            memberships,
            expected_dimension,
            quantization,
        )
        .map(|entries| entries.into_iter().map(|(entry, _)| entry).collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_vector_bodies_for_memberships_in_read(
        &self,
        read_txn: &redb::ReadTransaction,
        memberships: &[VectorPartitionMembershipRecord],
        expected_dimension: usize,
        quantization: Option<VectorQuantization>,
    ) -> Result<Vec<(VectorEntry, Option<TxId>)>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.prepare_proof_owner()?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_physical_source(
            crate::writer_query_test_seams::PhysicalFamily::Vector,
            read_txn,
        );
        let operation_memory =
            vector_membership_source::historical_memory_for_persistence(self, read_txn)?;
        let load = || {
            let verifier = content_identity::ContentIdentityReadVerifier::open(
                read_txn,
                &self.path,
                if operation_memory.is_some() {
                    None
                } else {
                    self.proof_accountant()
                },
            )?;
            let mut proof = content_identity::ReadAuthenticationNodes::new(
                RAW_VECTOR_BODY_SCAN_PROOF_BYTES,
                if operation_memory.is_some() {
                    None
                } else {
                    self.proof_accountant()
                },
            );
            let table = match read_txn.open_byte_table_bounded(VECTORS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Err(self.vector_partition_corruption(
                        "a non-empty raw-vector directory has no vector body table",
                    ));
                }
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut entries = Vec::with_capacity(memberships.len());
            for membership in memberships {
                vector_membership_source::operation_checkpoint()?;
                let quantization = match quantization {
                    Some(quantization) => quantization,
                    None => {
                        Self::load_table_meta_in_read_txn(read_txn, &membership.index.table)?
                            .and_then(|meta| {
                                meta.columns
                                    .into_iter()
                                    .find(|column| column.name == membership.index.column)
                            })
                            .ok_or_else(|| {
                                self.vector_partition_corruption(
                                    "vector body has no captured quantization declaration",
                                )
                            })?
                            .quantization
                    }
                };
                vector_membership_source::authenticate_row_for_persistence(
                    self,
                    read_txn,
                    &membership.index,
                    &vector_membership_source::body_identity_for_record(membership),
                    expected_dimension,
                    quantization,
                    None,
                )?;
                let key = Self::vector_identity_key(
                    &membership.index,
                    membership.row_id,
                    membership.vector_created_tx,
                    membership.vector_lsn,
                );
                let stored = table
                    .get_bounded(key.as_slice())
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| {
                        self.vector_partition_corruption(
                            "a raw-vector directory identity has no durable body",
                        )
                    })?;
                self.validate_raw_vector_entry_encoded_width(
                    stored.value(),
                    &membership.index,
                    expected_dimension,
                )?;
                let persisted: PersistedVectorEntry =
                    Self::decode(stored.value()).map_err(|error| {
                        if preserves_typed_read_refusal(&error) {
                            return error;
                        }
                        self.vector_partition_corruption(format!(
                            "a raw-vector directory body cannot be decoded: {error}"
                        ))
                    })?;
                if persisted.index != membership.index
                    || persisted.row_id != membership.row_id
                    || persisted.created_tx != membership.vector_created_tx
                    || persisted.lsn != membership.vector_lsn
                {
                    return Err(self.vector_partition_corruption(
                        "a raw-vector body disagrees with its membership identity",
                    ));
                }
                // Validate the exact length that to_f32 will produce BEFORE its
                // clone/dequantization allocation. A corrupt SQ4 coded payload
                // can fit the encoded-width bound yet expand beyond the declared
                // F32 scratch unless its predicted dimension is checked here.
                let decoded_bytes = persisted.vector.decoded_bytes();
                let decoded_dimension = decoded_bytes / std::mem::size_of::<f32>();
                if decoded_dimension != expected_dimension {
                    return Err(self.vector_partition_corruption(format!(
                    "raw-vector body dimension {} does not match declared dimension {expected_dimension}",
                    decoded_dimension
                )));
                }
                crate::read_image_memory::reserve(decoded_bytes).map_err(|error| {
                    if preserves_typed_read_refusal(&error) {
                        return error;
                    }
                    self.vector_partition_corruption(format!(
                        "a raw-vector directory body cannot be decoded: {error}"
                    ))
                })?;
                let mut entry = VectorEntry {
                    index: persisted.index,
                    row_id: persisted.row_id,
                    vector: persisted.vector.to_f32(),
                    created_tx: persisted.created_tx,
                    deleted_tx: persisted.deleted_tx,
                    lsn: persisted.lsn,
                };
                // Membership combines row and vector lifecycle. A row may
                // make the occurrence unavailable before the body receives
                // its own tombstone, so the effective partition visibility is
                // the checksummed membership value.

                if let Some(memory) = &operation_memory {
                    let _proof_controls = contextdb_core::read_memory::ReadCredit::try_new(
                        memory.clone(),
                        std::mem::size_of::<content_identity::ContentIdentityMutation>()
                            .checked_add(entry.vector.len().checked_mul(4).ok_or_else(|| {
                                vector_membership_source::damage("bulk proof body overflow")
                            })?)
                            .and_then(|n| n.checked_add(entry.index.table.len()))
                            .and_then(|n| n.checked_add(entry.index.column.len()))
                            .and_then(|n| n.checked_add(320))
                            .ok_or_else(|| {
                                vector_membership_source::damage("bulk proof controls overflow")
                            })?,
                        "reader",
                        "historical_vector_bulk_mutation",
                        "Raise the declared reader memory limit.",
                    )?;
                    verifier.verify_with_read_memory_before(
                        &content_identity::decoded_vector_version_mutation(&entry, quantization),
                        memory.clone(),
                        &mut || vector_membership_source::operation_checkpoint(),
                    )?;
                    vector_membership_source::authenticate_row_for_persistence(
                        self,
                        read_txn,
                        &membership.index,
                        &vector_membership_source::body_identity_for_record(membership),
                        expected_dimension,
                        quantization,
                        Some(entry.deleted_tx),
                    )?;
                } else {
                    verifier.verify_with_nodes(
                        &mut proof,
                        &content_identity::decoded_vector_version_mutation(&entry, quantization),
                    )?;
                    if entry.deleted_tx.is_none() {
                        verifier.verify_with_nodes(
                            &mut proof,
                            &content_identity::decoded_vector_mutation(&entry, quantization),
                        )?;
                    }
                }
                let canonical_deleted_tx = entry.deleted_tx;
                entry.deleted_tx = membership.deleted_tx;
                entries.push((entry, canonical_deleted_tx));
            }
            Ok(entries)
        };
        match &operation_memory {
            Some(memory) => graph_read::with_pages(memory.clone(), load),
            None => load(),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_raw_vector_entry_encoded_width(
        &self,
        bytes: &[u8],
        index: &VectorIndexRef,
        dimension: usize,
    ) -> Result<()> {
        validate_raw_vector_entry_encoded_width(bytes, index, dimension, |reason| {
            self.vector_partition_corruption(reason)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn open_raw_vector_body_scan(
        self: &Arc<Self>,
        index: &VectorIndexRef,
    ) -> Result<WritableRawVectorBodyScan> {
        if let Some(read) = vector_membership_source::operation_read(
            &vector_membership_source::MembershipOwner::Writable(self.clone()),
        ) {
            let writer = self
                .scoped_vector_root_capture
                .get()
                .map(|capture| capture())
                .transpose()?
                .flatten();
            return self.open_raw_vector_body_scan_at(
                index,
                read,
                writer.map(|root| root.into_capture()),
            );
        }
        self.with_db(|db| {
            let (transaction, root) = match self.paged_root_capture.get() {
                Some(capture) => {
                    let captured = capture()?;
                    captured.authenticate()?;
                    let vector_paged::PagedRootHolder::Writer(writer) = captured.holder else {
                        return Err(self.vector_partition_corruption(
                            "raw body root capture returned another owner",
                        ));
                    };
                    writer.check_table_scope(&index.table)?;
                    let identity = writer.storage_identity();
                    let verified = db
                        .begin_external_read(identity)
                        .map_err(vector_membership_pages::metadata_error)?;
                    if verified.storage_snapshot_identity() != identity {
                        return Err(self.vector_partition_corruption(
                            "raw body read escaped its captured physical root",
                        ));
                    }
                    (Arc::new(verified), Some(writer.into_capture()))
                }
                None => (
                    Arc::new(
                        db.begin_read()
                            .map_err(vector_membership_pages::metadata_error)?,
                    ),
                    None,
                ),
            };
            self.open_raw_vector_body_scan_at(index, transaction, root)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn open_raw_vector_body_scan_at(
        self: &Arc<Self>,
        index: &VectorIndexRef,
        transaction: Arc<redb::ReadTransaction>,
        root: Option<contextdb_tx::native_capture::CaptureHandle>,
    ) -> Result<WritableRawVectorBodyScan> {
        let owner = vector_membership_source::MembershipOwner::Writable(self.clone());
        let memory = vector_membership_source::historical_operation_memory(&owner, &transaction)?;
        let proof_accountant = if memory.is_some() {
            None
        } else {
            self.proof_accountant()
        };
        let open = || {
            vector_membership_source::operation_checkpoint()?;
            if let Some(root) = &root {
                let writer = root.downcast_ref::<WriterSnapshot>().ok_or_else(|| {
                    self.vector_partition_corruption("raw body read has another root owner")
                })?;
                writer.check_table_scope(&index.table)?;
                if transaction.storage_snapshot_identity() != writer.storage_identity() {
                    return Err(self.vector_partition_corruption(
                        "raw body read escaped its captured physical root",
                    ));
                }
            }
            let producer_body_root = if root.is_some() {
                vector_membership_pages::validate_in_read(&transaction)?;
                vector_scan_body_root_matches(&transaction)?
            } else {
                false
            };

            let vectors = {
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_physical_source(
                    crate::writer_query_test_seams::PhysicalFamily::Vector,
                    &transaction,
                );
                #[cfg(any(test, feature = "test-seams"))]
                let _body_reads = crate::writer_query_test_seams::physical_operation(
                    crate::writer_query_test_seams::PhysicalFamily::Vector,
                    &index.table,
                );
                #[cfg(not(any(test, feature = "test-seams")))]
                let _ = index;
                match transaction.open_table(VECTORS_TABLE) {
                    Ok(table) => Some(table),
                    Err(redb::TableError::TableDoesNotExist(_)) => None,
                    Err(error) => return Err(vector_membership_pages::metadata_error(error)),
                }
            };
            Ok(WritableRawVectorBodyScan {
                producer_body_root,
                selected_stream_identity: None,
                source: Arc::clone(self),

                vectors,
                proof: content_identity::ReadAuthenticationNodes::new(
                    RAW_VECTOR_BODY_SCAN_PROOF_BYTES,
                    proof_accountant.clone(),
                ),
                _transaction: transaction,
                _root: root,
            })
        };
        match memory {
            Some(memory) => graph_read::with_pages(memory, open),
            None => open(),
        }
    }

    /// Read one durable vector body through its exact partition, row, vector
    /// transaction, deletion transaction, and LSN identity. The membership
    /// range is row-prefixed, so this never enumerates another row in the
    /// partition, and exactly one body is decoded after placement validation.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_entry_for_partition_identity(
        self: &Arc<Self>,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        self.open_raw_vector_body_scan(index)?.read_partition_body(
            index,
            partition_key,
            identity,
            expected_dimension,
            quantization,
            false,
        )
    }

    /// Enumerate catalog pointers without making one bad record a database
    /// boundary. Exact known physical keys own their fault; otherwise only a
    /// fully checksummed, decoded payload may identify the affected route.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn inspect_vector_partition_generation_catalog(
        &self,
        known_routes: &[VectorPartitionRef],
    ) -> Result<LoadedVectorPartitionGenerationCatalog> {
        let known_by_key = known_routes
            .iter()
            .cloned()
            .map(|route| {
                (
                    Self::vector_partition_catalog_key_from_parts(
                        &route.index,
                        &route.partition_key,
                    ),
                    route,
                )
            })
            .collect::<HashMap<_, _>>();
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table =
                match read.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => {
                        return Ok(LoadedVectorPartitionGenerationCatalog {
                            catalogs: Vec::new(),
                            quarantines: Vec::new(),
                        });
                    }
                    Err(error) => return Err(Self::storage_error(error)),
                };
            let mut catalogs = Vec::new();
            let mut quarantines = HashMap::<VectorPartitionRef, VectorRouteQuarantineReason>::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let physical_key = key.value();
                let physical_route = known_by_key.get(physical_key).cloned();
                match self.decode_vector_partition_record::<VectorPartitionGenerationCatalogRecord>(
                    VectorPartitionRecordKind::GenerationCatalog,
                    value.value(),
                ) {
                    Ok(catalog) => {
                        let payload_route = VectorPartitionRef::new(
                            catalog.index.clone(),
                            catalog.partition_key.clone(),
                        );
                        if Self::vector_partition_catalog_key(&catalog).as_slice() != physical_key {
                            let route = physical_route.unwrap_or(payload_route);
                            quarantines
                                .entry(route)
                                .or_insert(VectorRouteQuarantineReason::CorruptBase);
                        } else {
                            catalogs.push(catalog);
                        }
                    }
                    Err(_) => {
                        if let Some(route) = physical_route {
                            let reason = vector_generation_catalog_fault_reason(value.value());
                            quarantines
                                .entry(route)
                                .and_modify(|current| {
                                    if reason == VectorRouteQuarantineReason::IncompatibleFormat {
                                        *current = reason;
                                    }
                                })
                                .or_insert(reason);
                        }
                    }
                }
            }
            catalogs.retain(|catalog| {
                !quarantines.contains_key(&VectorPartitionRef::new(
                    catalog.index.clone(),
                    catalog.partition_key.clone(),
                ))
            });
            let mut quarantines = quarantines.into_iter().collect::<Vec<_>>();
            quarantines.sort_by(|(left, _), (right, _)| {
                left.index
                    .table
                    .cmp(&right.index.table)
                    .then(left.index.column.cmp(&right.index.column))
                    .then(
                        left.partition_key
                            .canonical_bytes()
                            .cmp(&right.partition_key.canonical_bytes()),
                    )
            });
            Ok(LoadedVectorPartitionGenerationCatalog {
                catalogs,
                quarantines,
            })
        })
    }

    /// Maintenance pins one committed root and separates navigation from
    /// materialization. Only a selected singleton's resource refusal may be
    /// handled locally by the caller; navigation errors remain terminal.
    /// Keep navigation on one authenticated root while callers decode and
    /// release individual catalogs before preparing the next route.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn maintenance_vector_catalog_cursor(
        &self,
    ) -> Result<Option<redb::BoundedByteCursor<'static>>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table =
                match read.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(error) => return Err(Self::storage_error(error)),
                };
            Ok(Some(table.cursor_bounded()))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn scan_vector_catalog_for_maintenance(
        &self,
        mut inspect: impl FnMut(redb::BoundedByteHandle<'_>) -> Result<()>,
    ) -> Result<()> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table =
                match read.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                    Err(error) => return Err(Self::storage_error(error)),
                };
            for handle in table.cursor_bounded() {
                inspect(handle.map_err(Self::storage_error)?)?;
            }
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn decode_maintenance_vector_catalog(
        &self,
        encoded: &[u8],
    ) -> Result<VectorPartitionGenerationCatalogRecord> {
        self.decode_vector_partition_record(VectorPartitionRecordKind::GenerationCatalog, encoded)
    }

    pub(crate) fn maintenance_vector_catalog_fault_reason(
        encoded: &[u8],
    ) -> VectorRouteQuarantineReason {
        vector_generation_catalog_fault_reason(encoded)
    }

    pub(crate) fn maintenance_vector_catalog_decode_bound(encoded_bytes: usize) -> usize {
        Self::vector_partition_record_decode_bound(
            encoded_bytes,
            std::mem::size_of::<VectorPartitionGenerationCatalogRecord>(),
        )
    }

    pub(crate) fn maintenance_vector_catalog_heap_bytes(
        catalog: &VectorPartitionGenerationCatalogRecord,
    ) -> usize {
        Self::vector_partition_catalog_heap_bytes(catalog)
    }

    pub(crate) fn vector_journal_cleanup_key_reservation(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> usize {
        Self::vector_partition_physical_key_bound(index, partition_key, 16).saturating_mul(3)
    }

    /// Stream the existing canonical framing without owning a TEXT payload.
    /// The caller admits the small per-component frame vector first.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn maintenance_catalog_key_matches(
        handle: &redb::BoundedByteHandle<'_>,
        index: &VectorIndexRef,
        key: &VectorPartitionKey,
    ) -> Result<bool> {
        use contextdb_core::VectorPartitionComponent as Component;
        let mut frames = Vec::<([u8; 17], usize)>::with_capacity(key.components().len());
        let mut canonical_len = 9usize;
        for component in key.components() {
            let mut frame = [0u8; 17];
            let len = match component {
                Component::Uuid(value) => {
                    frame[0] = 0;
                    frame[1..17].copy_from_slice(value.as_bytes());
                    17
                }
                Component::Text(value) => {
                    frame[0] = 1;
                    frame[1..9].copy_from_slice(&(value.len() as u64).to_be_bytes());
                    canonical_len = canonical_len.saturating_add(value.len());
                    9
                }
                Component::Integer(value) => {
                    frame[0] = 2;
                    frame[1..9].copy_from_slice(&value.to_be_bytes());
                    9
                }
                Component::Boolean(value) => {
                    frame[0] = 3;
                    frame[1] = u8::from(*value);
                    2
                }
                Component::Timestamp(value) => {
                    frame[0] = 4;
                    frame[1..9].copy_from_slice(&value.to_be_bytes());
                    9
                }
                Component::TxId(value) => {
                    frame[0] = 5;
                    frame[1..9].copy_from_slice(&value.0.to_be_bytes());
                    9
                }
            };
            canonical_len = canonical_len.saturating_add(len);
            frames.push((frame, len));
        }
        let table_len = (index.table.len() as u64).to_be_bytes();
        let column_len = (index.column.len() as u64).to_be_bytes();
        let partition_len = (canonical_len as u64).to_be_bytes();
        let mut canonical_header = [0u8; 9];
        canonical_header[0] = 1;
        canonical_header[1..].copy_from_slice(&(key.components().len() as u64).to_be_bytes());
        let head = [
            table_len.as_slice(),
            index.table.as_bytes(),
            column_len.as_slice(),
            index.column.as_bytes(),
            partition_len.as_slice(),
            canonical_header.as_slice(),
        ];
        let tail = frames
            .iter()
            .zip(key.components())
            .flat_map(|((frame, len), component)| {
                let payload = match component {
                    Component::Text(value) => Some(value.as_bytes()),
                    _ => None,
                };
                std::iter::once(&frame[..*len]).chain(payload)
            });
        handle
            .key_matches_parts(
                index
                    .table
                    .len()
                    .saturating_add(index.column.len())
                    .saturating_add(24)
                    .saturating_add(canonical_len),
                head.into_iter().chain(tail),
            )
            .map_err(Self::storage_error)
    }

    pub(crate) fn maintenance_catalog_key_frame_bytes(key: &VectorPartitionKey) -> usize {
        key.components()
            .len()
            .saturating_mul(std::mem::size_of::<([u8; 17], usize)>())
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from vector_public_cycle_admission_tests.rs"
        )
    )]
    pub(crate) fn encode_maintenance_catalog_for_test(
        catalog: &VectorPartitionGenerationCatalogRecord,
    ) -> Vec<u8> {
        Self::encode_vector_partition_record(VectorPartitionRecordKind::GenerationCatalog, catalog)
            .unwrap()
    }

    /// Borrow one catalog's fixed descriptor suffix for generation pricing.
    /// Route metadata and graph bodies are not decoded into owned values.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_generation_descriptors_for(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<
        Option<(
            VectorPartitionGenerationDescriptor,
            Option<VectorPartitionGenerationDescriptor>,
        )>,
    > {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            Self::vector_partition_generation_descriptors_in_read(&read, index, partition_key)
        })
    }

    /// The fixed catalog suffix at an already-authorized root. No fresh read,
    /// owned route decode, catalog scan or graph body load is performed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_partition_generation_descriptors_in_read(
        read: &redb::ReadTransaction,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<
        Option<(
            VectorPartitionGenerationDescriptor,
            Option<VectorPartitionGenerationDescriptor>,
        )>,
    > {
        let key = Self::vector_partition_catalog_key_from_parts(index, partition_key);
        let table = match read.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let Some(value) = table
            .get_bounded(key.as_slice())
            .map_err(Self::storage_error)?
        else {
            return Ok(None);
        };
        let payload = vector_partition_expected_metadata_payload(
            VectorPartitionRecordKind::GenerationCatalog,
            value.value(),
            index,
            partition_key,
        )
        .map_err(vector_membership_source::damage)?;
        let route_end = vector_partition_expected_route_prefix(payload, index, partition_key)
            .map_err(vector_membership_source::damage)?;
        // The descriptor suffix contains only fixed scalars. Pricing a
        // frontier must not clone its route or allocate a graph body.
        let suffix = &payload[route_end..];
        let ((base, change, _, _), consumed): (
            (
                VectorPartitionGenerationDescriptor,
                Option<VectorPartitionGenerationDescriptor>,
                TxId,
                Lsn,
            ),
            usize,
        ) = bincode::serde::decode_from_slice(suffix, bincode::config::standard()).map_err(
            |_| vector_membership_source::damage("catalog descriptor suffix is malformed"),
        )?;
        if consumed != suffix.len() {
            return Err(vector_membership_source::damage(
                "catalog descriptor suffix has trailing bytes",
            ));
        }
        Ok(Some((base, change)))
    }

    /// Read one lightweight catalog pointer without touching graph bodies or
    /// the partition journal. Maintenance uses this to allocate the next
    /// immutable generation identity; startup uses the all-catalog form above.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_generation_catalog_for(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> Result<Option<VectorPartitionGenerationCatalogRecord>> {
        let key = Self::vector_partition_catalog_key_from_parts(index, partition_key);
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table =
                match read_txn.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(error) => return Err(Self::storage_error(error)),
                };
            let Some(value) = table
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
            else {
                return Ok(None);
            };
            let catalog: VectorPartitionGenerationCatalogRecord = self
                .decode_expected_vector_partition_metadata(
                    VectorPartitionRecordKind::GenerationCatalog,
                    value.value(),
                    index,
                    partition_key,
                )?;
            if Self::vector_partition_catalog_key(&catalog) != key
                || catalog.index != *index
                || catalog.partition_key != *partition_key
            {
                return Err(self.vector_partition_corruption(
                    "generation catalog key does not match its checksummed payload",
                ));
            }
            Ok(Some(catalog))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_generation_catalog_for_admitted(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        workspace: &mut crate::memory_accounting::OwnedMemoryReservation,
    ) -> Result<Option<VectorPartitionGenerationCatalogRecord>> {
        let key = Self::vector_partition_catalog_key_from_parts(index, partition_key);
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table =
                match read_txn.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(error) => return Err(Self::storage_error(error)),
                };
            let Some(value) = table
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
            else {
                return Ok(None);
            };
            workspace.try_grow_for(
                1024usize.saturating_add(value.value().len().saturating_mul(16)),
                "retention",
                "prepare_prune",
                "Raise MEMORY_LIMIT to decode retention generation catalogs.",
            )?;
            let catalog: VectorPartitionGenerationCatalogRecord = self
                .decode_vector_partition_record(
                    VectorPartitionRecordKind::GenerationCatalog,
                    value.value(),
                )?;
            if Self::vector_partition_catalog_key(&catalog) != key
                || catalog.index != *index
                || catalog.partition_key != *partition_key
            {
                return Err(self.vector_partition_corruption(
                    "generation catalog key does not match its checksummed payload",
                ));
            }
            Ok(Some(catalog))
        })
    }

    /// Find the next safe immutable identity when the mutable catalog pointer
    /// itself is quarantined. Inspect one admitted record at a time: graph
    /// bodies remain checksummed evidence without accumulating in an inventory.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn highest_vector_partition_generation_id(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        mut admit_record: impl FnMut(usize) -> Result<()>,
    ) -> Result<u64> {
        // All heap owners depend only on the expected route. The factor covers
        // canonical-key growth, the physical prefix, serialized-prefix growth,
        // and the serde route Vec's geometric capacity; fixed record fields and
        // framing fit in the extra 1 KiB. Graph bytes remain borrowed from redb.
        let route_bytes = partition_key
            .estimated_bytes()
            .saturating_add(index.table.len())
            .saturating_add(index.column.len())
            .saturating_add(128);
        let scratch_bytes = route_bytes.saturating_mul(8).saturating_add(1024);
        admit_record(scratch_bytes)?;
        let prefix = Self::vector_partition_prefix(index, partition_key);
        let route_prefix =
            bincode::serde::encode_to_vec((index, partition_key), bincode::config::standard())
                .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let mut highest = 0;
            for (definition, kind) in [
                (
                    VECTOR_PARTITION_BASE_GENERATION_TABLE,
                    VectorPartitionRecordKind::BaseGeneration,
                ),
                (
                    VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
                    VectorPartitionRecordKind::ChangeGeneration,
                ),
            ] {
                let table = match read.open_byte_table_bounded(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => continue,
                    Err(error) => return Err(Self::storage_error(error)),
                };
                for entry in table
                    .prefix_bounded(prefix.as_slice())
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    admit_record(scratch_bytes)?;
                    let payload = vector_partition_record_payload(kind, value.value())
                        .map_err(|reason| self.vector_partition_corruption(reason))?;
                    // Canonical route bytes precede all other fields. Checking
                    // them before serde owns route metadata prevents hostile
                    // string/Vec lengths from escaping the admitted bound.
                    if !payload.starts_with(&route_prefix) {
                        return Err(self.vector_partition_corruption(
                            "generation route prefix does not match its physical partition",
                        ));
                    }
                    let suffix = &key.value()[prefix.len()..];
                    let (generation_id, valid_key, consumed) = match kind {
                        VectorPartitionRecordKind::BaseGeneration => {
                            let (record, consumed): (
                                BorrowedVectorPartitionBaseGenerationRecord<'_>,
                                usize,
                            ) = bincode::serde::borrow_decode_from_slice(
                                payload,
                                bincode::config::standard(),
                            )
                            .map_err(|error| {
                                self.vector_partition_corruption(format!(
                                    "payload decode failed: {error}"
                                ))
                            })?;
                            let valid = record.index == *index
                                && record.partition_key == *partition_key
                                && suffix == record.generation_id.to_be_bytes();
                            (record.generation_id, valid, consumed)
                        }
                        VectorPartitionRecordKind::ChangeGeneration => {
                            let (record, consumed): (
                                BorrowedVectorPartitionChangeGenerationRecord<'_>,
                                usize,
                            ) = bincode::serde::borrow_decode_from_slice(
                                payload,
                                bincode::config::standard(),
                            )
                            .map_err(|error| {
                                self.vector_partition_corruption(format!(
                                    "payload decode failed: {error}"
                                ))
                            })?;
                            let valid = record.index == *index
                                && record.partition_key == *partition_key
                                && suffix.len() == 16
                                && suffix[..8] == record.base_generation_id.to_be_bytes()
                                && suffix[8..] == record.change_generation_id.to_be_bytes();
                            (record.change_generation_id, valid, consumed)
                        }
                        _ => unreachable!("only immutable graph records are inspected"),
                    };
                    if consumed != payload.len() {
                        return Err(self.vector_partition_corruption(
                            "payload decode failed: bincode exact decode left trailing bytes",
                        ));
                    }
                    if !valid_key {
                        return Err(self.vector_partition_corruption(
                            "table key does not match its checksummed payload",
                        ));
                    }
                    highest = highest.max(generation_id);
                }
            }
            Ok(highest)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn mutate_vector_generation_catalog_record_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
        fault: VectorGenerationCatalogRecordFaultForTest,
    ) -> Result<()> {
        let database = redb_builder().open(path).map_err(Self::storage_error)?;
        let key = Self::vector_partition_catalog_key_from_parts(
            &partition.index,
            &partition.partition_key,
        );
        let write = database.begin_write().map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(Self::storage_error)?;
            let mut bytes = table
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| {
                    Error::Other("vector generation catalog fixture is absent".into())
                })?;
            match fault {
                VectorGenerationCatalogRecordFaultForTest::MalformedEnvelope => {
                    let last = bytes.last_mut().ok_or_else(|| {
                        Error::Other("vector generation catalog fixture is empty".into())
                    })?;
                    *last ^= 0xff;
                    table
                        .insert(key.as_slice(), bytes.as_slice())
                        .map_err(Self::storage_error)?;
                }
                VectorGenerationCatalogRecordFaultForTest::WellFormedNewerEnvelopeVersion {
                    version,
                } => {
                    let offset = VECTOR_PARTITION_RECORD_MAGIC.len();
                    bytes[offset..offset + 2].copy_from_slice(&version.to_be_bytes());
                    let checksum_offset = bytes.len().checked_sub(32).ok_or_else(|| {
                        Error::Other("vector generation catalog fixture is truncated".into())
                    })?;
                    let checksum = blake3::hash(&bytes[..checksum_offset]);
                    bytes[checksum_offset..].copy_from_slice(checksum.as_bytes());
                    table
                        .insert(key.as_slice(), bytes.as_slice())
                        .map_err(Self::storage_error)?;
                }
                VectorGenerationCatalogRecordFaultForTest::RelocateToMalformedPhysicalKey => {
                    let mut malformed_key = key.clone();
                    malformed_key.extend_from_slice(b"\0malformed");
                    table
                        .insert(malformed_key.as_slice(), bytes.as_slice())
                        .map_err(Self::storage_error)?;
                    table.remove(key.as_slice()).map_err(Self::storage_error)?;
                }
            }
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
                graph_observation_checkpoint(11)?;
            }
            graph_commit_result
        }
        .map_err(Self::storage_error)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn corrupt_vector_partition_journal_record_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        Self::corrupt_vector_partition_journal_record_at_for_test(path, partition, false)
    }

    /// Damage the route's LAST journal record, the newest uncovered one.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn corrupt_last_vector_partition_journal_record_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        Self::corrupt_vector_partition_journal_record_at_for_test(path, partition, true)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn corrupt_vector_partition_journal_record_at_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
        last: bool,
    ) -> Result<()> {
        let database = redb_builder().open(path).map_err(Self::storage_error)?;
        let prefix = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        let write = database.begin_write().map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        {
            let mut table = write
                .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                .map_err(Self::storage_error)?;
            let mut selected = None;
            for entry in table
                .prefix_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                selected = Some((key.value().to_vec(), value.value().to_vec()));
                if !last {
                    break;
                }
            }
            let (key, mut bytes) = selected.ok_or_else(|| {
                Error::Other(("vector partition journal fixture is absent".to_owned()).into())
            })?;
            let last = bytes.last_mut().ok_or_else(|| {
                Error::Other(("vector partition journal fixture is empty".to_owned()).into())
            })?;
            *last ^= 0xff;
            table
                .insert(key.as_slice(), bytes.as_slice())
                .map_err(Self::storage_error)?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
                graph_observation_checkpoint(11)?;
            }
            graph_commit_result
        }
        .map_err(Self::storage_error)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn corrupt_vector_partition_base_record_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        Self::flip_vector_partition_base_record_byte_for_test(path, partition, |len| len - 1)
    }

    /// Flip the last byte of a partition's selected change record, leaving its
    /// catalog readable.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn corrupt_vector_partition_change_record_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        Self::flip_vector_partition_generation_record_byte_for_test(path, partition, true, |len| {
            len - 1
        })
    }

    /// Flip one byte in the middle of a partition's selected base record,
    /// which is inside its saved graph: damage no digest was told about.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn corrupt_vector_partition_saved_graph_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        Self::flip_vector_partition_base_record_byte_for_test(path, partition, |len| len / 2)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn flip_vector_partition_base_record_byte_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
        position: impl FnOnce(usize) -> usize,
    ) -> Result<()> {
        Self::flip_vector_partition_generation_record_byte_for_test(
            path, partition, false, position,
        )
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn flip_vector_partition_generation_record_byte_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
        change: bool,
        position: impl FnOnce(usize) -> usize,
    ) -> Result<()> {
        let database = redb_builder().open(path).map_err(Self::storage_error)?;
        let catalog_key = Self::vector_partition_catalog_key_from_parts(
            &partition.index,
            &partition.partition_key,
        );
        let write = database.begin_write().map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        let base_key = {
            let catalogs = write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(Self::storage_error)?;
            let bytes = catalogs
                .get_bounded(catalog_key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| {
                    Error::Other(("vector generation catalog fixture is absent".to_owned()).into())
                })?;
            let catalog: VectorPartitionGenerationCatalogRecord =
                decode_vector_partition_record_payload(
                    VectorPartitionRecordKind::GenerationCatalog,
                    &bytes,
                )
                .map_err(|reason| {
                    Error::Other((format!("invalid catalog fixture: {reason}")).into())
                })?;
            if change {
                let selected = catalog.change.ok_or_else(|| {
                    Error::Other(("selected vector change fixture is absent".to_owned()).into())
                })?;
                Self::vector_partition_change_generation_key_from_parts(
                    &partition.index,
                    &partition.partition_key,
                    catalog.base.generation_id,
                    selected.generation_id,
                )
            } else {
                Self::vector_partition_base_generation_key_from_parts(
                    &partition.index,
                    &partition.partition_key,
                    catalog.base.generation_id,
                )
            }
        };
        {
            let mut bases = write
                .open_table(if change {
                    VECTOR_PARTITION_CHANGE_GENERATION_TABLE
                } else {
                    VECTOR_PARTITION_BASE_GENERATION_TABLE
                })
                .map_err(Self::storage_error)?;
            let mut bytes = bases
                .get_bounded(base_key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| {
                    Error::Other(("selected vector base fixture is absent".to_owned()).into())
                })?;
            if bytes.is_empty() {
                return Err(Error::Other(
                    ("selected vector base fixture is empty".to_owned()).into(),
                ));
            }
            let at = position(bytes.len());
            bytes[at] ^= 0xff;
            bases
                .insert(base_key.as_slice(), bytes.as_slice())
                .map_err(Self::storage_error)?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
                graph_observation_checkpoint(11)?;
            }
            graph_commit_result
        }
        .map_err(Self::storage_error)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn corrupt_vector_partition_graph_page_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        let database = redb_builder().open(path).map_err(Self::storage_error)?;
        let catalog_key = Self::vector_partition_catalog_key_from_parts(
            &partition.index,
            &partition.partition_key,
        );
        let write = database.begin_write().map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        let mut graph_publication_attempt =
            crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
        let base_key = {
            let catalogs = write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(Self::storage_error)?;
            let bytes = catalogs
                .get_bounded(catalog_key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| {
                    Error::Other("vector generation catalog fixture is absent".into())
                })?;
            let catalog: VectorPartitionGenerationCatalogRecord =
                decode_vector_partition_record_payload(
                    VectorPartitionRecordKind::GenerationCatalog,
                    &bytes,
                )
                .map_err(|reason| {
                    Error::Other((format!("invalid catalog fixture: {reason}")).into())
                })?;
            Self::vector_partition_base_generation_key_from_parts(
                &partition.index,
                &partition.partition_key,
                catalog.base.generation_id,
            )
        };
        let base: VectorPartitionBaseGenerationRecord = {
            let bases = write
                .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(Self::storage_error)?;
            let bytes = bases
                .get_bounded(base_key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| Error::Other("selected vector base fixture is absent".into()))?;
            decode_vector_partition_record_payload(
                VectorPartitionRecordKind::BaseGeneration,
                &bytes,
            )
            .map_err(|reason| Error::Other((format!("invalid base fixture: {reason}")).into()))?
        };
        let paged = match vector_paged::paged_storage_of_graph_for_test(
            base.graph_format_version,
            &base.graph.bytes,
        )? {
            Some(storage) => {
                vector_paged::damage_route_pages_in_write_for_test(
                    &write,
                    storage,
                    None,
                    PagedRouteFaultForTest::FlipFirstPointPage,
                )?;
                true
            }
            None => false,
        };
        let root = Self::vector_partition_graph_page_root_key(&base_key, true);
        let page_key = Self::vector_partition_graph_page_key(&root, 0);
        if !paged {
            let mut pages = write
                .open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE)
                .map_err(Self::storage_error)?;
            let mut bytes = pages
                .get_bounded(page_key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| {
                    Error::Other("selected vector graph page fixture is absent".into())
                })?;
            let last = bytes.last_mut().ok_or_else(|| {
                Error::Other("selected vector graph page fixture is empty".into())
            })?;
            *last ^= 0xff;
            pages
                .insert(page_key.as_slice(), bytes.as_slice())
                .map_err(Self::storage_error)?;
        }
        {
            #[cfg(any(test, feature = "test-seams"))]
            graph_observation_checkpoint(10)?;
            let graph_commit_result = write.commit();
            #[cfg(any(test, feature = "test-seams"))]
            if graph_commit_result.is_ok() {
                graph_publication_attempt.committed();
                graph_observation_checkpoint(11)?;
            }
            graph_commit_result
        }
        .map_err(Self::storage_error)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn paged_storage_of_route_in_write_for_test(
        write: &redb::WriteTransaction,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<u64> {
        let catalog_key = Self::vector_partition_catalog_key_from_parts(
            &partition.index,
            &partition.partition_key,
        );
        let catalogs = write
            .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
            .map_err(Self::storage_error)?;
        let bytes = catalogs
            .get_bounded(catalog_key.as_slice())
            .map_err(Self::storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| Error::Other("vector generation catalog fixture is absent".into()))?;
        let catalog: VectorPartitionGenerationCatalogRecord =
            decode_vector_partition_record_payload(
                VectorPartitionRecordKind::GenerationCatalog,
                &bytes,
            )
            .map_err(|reason| {
                Error::Other((format!("invalid catalog fixture: {reason}")).into())
            })?;
        let base_key = Self::vector_partition_base_generation_key_from_parts(
            &partition.index,
            &partition.partition_key,
            catalog.base.generation_id,
        );
        let bases = write
            .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
            .map_err(Self::storage_error)?;
        let bytes = bases
            .get_bounded(base_key.as_slice())
            .map_err(Self::storage_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| Error::Other("selected vector base fixture is absent".into()))?;
        let base: VectorPartitionBaseGenerationRecord = decode_vector_partition_record_payload(
            VectorPartitionRecordKind::BaseGeneration,
            &bytes,
        )
        .map_err(|reason| Error::Other((format!("invalid base fixture: {reason}")).into()))?;
        vector_paged::paged_storage_of_graph_for_test(base.graph_format_version, &base.graph.bytes)?
            .ok_or_else(|| Error::Other("selected vector base is not paged".into()))
    }

    /// Damage one route's committed paged page set; `other` names the route
    /// a cross-route substitution copies from.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn damage_paged_route_pages_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
        other: Option<&contextdb_vector::VectorPartitionRef>,
        fault: PagedRouteFaultForTest,
    ) -> Result<()> {
        let database = redb_builder().open(path).map_err(Self::storage_error)?;
        let write = database.begin_write().map_err(Self::storage_error)?;
        let storage = Self::paged_storage_of_route_in_write_for_test(&write, partition)?;
        let other = other
            .map(|other| Self::paged_storage_of_route_in_write_for_test(&write, other))
            .transpose()?;
        vector_paged::damage_route_pages_in_write_for_test(&write, storage, other, fault)?;
        write.commit().map_err(Self::storage_error)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn debug_vector_partition_journal_for_test(
        &self,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<(Lsn, Vec<Lsn>)> {
        let catalog = self
            .load_vector_partition_generation_catalog_for(
                &partition.index,
                &partition.partition_key,
            )?
            .ok_or_else(|| Error::Other("vector generation catalog fixture is absent".into()))?;
        let truncated_through = catalog
            .change
            .map(|change| change.covered_lsn)
            .unwrap_or(catalog.base.covered_lsn);
        let prefix = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    return Ok((truncated_through, Vec::new()));
                }
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut lsns = Vec::new();
            for entry in table
                .prefix_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let record: VectorPartitionJournalRecord = self.decode_vector_partition_record(
                    VectorPartitionRecordKind::Journal,
                    value.value(),
                )?;
                if Self::vector_partition_journal_key(&record).as_slice() != key.value()
                    || record.index != partition.index
                    || record.partition_key != partition.partition_key
                {
                    return Err(self.vector_partition_corruption(
                        "debug journal route/key verification failed",
                    ));
                }
                lsns.push(record.lsn);
            }
            Ok((truncated_through, lsns))
        })
    }

    /// Count raw membership versions separately from the selected live graph
    /// points. Tombstone records describe absence and are never another point.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_partition_maintenance_entry_counts(
        &self,
        partition: &VectorPartitionRef,
        covered_tx: TxId,
        after_lsn: Option<Lsn>,
    ) -> Result<(usize, usize)> {
        let prefix = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_byte_table_bounded(VECTOR_PARTITION_MEMBERSHIP_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((0, 0)),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut raw = 0usize;
            let mut selected = 0usize;
            for entry in table
                .prefix_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::database::resident_history_test_seams::note(|work| {
                    work.maintenance_source_visits += 1
                });
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let record: VectorPartitionMembershipRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::Membership,
                        value.value(),
                        &partition.index,
                        &partition.partition_key,
                    )?;
                if Self::vector_partition_membership_key(&record).as_slice() != key.value() {
                    return Err(self.vector_partition_corruption(
                        "maintenance membership key disagrees with its payload",
                    ));
                }
                if record.vector_created_tx > covered_tx {
                    continue;
                }
                raw = raw.checked_add(1).ok_or_else(|| {
                    self.vector_partition_corruption("maintenance raw count overflow")
                })?;
                if record.deleted_tx.is_none_or(|tx| tx > covered_tx)
                    && after_lsn.is_none_or(|lsn| record.vector_lsn > lsn)
                {
                    selected = selected.checked_add(1).ok_or_else(|| {
                        self.vector_partition_corruption("maintenance graph count overflow")
                    })?;
                }
            }
            Ok((raw, selected))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_partition_uncovered_tombstone_entry_count(
        &self,
        partition: &VectorPartitionRef,
        after_lsn: Lsn,
        covered_lsn: Lsn,
        mut admit_identities: impl FnMut(usize) -> Result<()>,
    ) -> Result<usize> {
        if after_lsn >= covered_lsn {
            return Ok(0);
        }
        let prefix = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut lower = prefix.clone();
            lower.extend_from_slice(&(after_lsn.0 + 1).to_be_bytes());
            let upper = if let Some(next) = covered_lsn.0.checked_add(1) {
                let mut key = prefix.clone();
                key.extend_from_slice(&next.to_be_bytes());
                key
            } else {
                writer_source::prefix_end(&prefix).ok_or_else(|| {
                    self.vector_partition_corruption("journal prefix has no upper bound")
                })?
            };
            let mut identities = BTreeSet::new();
            for entry in table
                .range(lower.as_slice()..upper.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::database::resident_history_test_seams::note(|work| {
                    work.maintenance_source_visits += 1
                });
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let record: VectorPartitionJournalRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::Journal,
                        value.value(),
                        &partition.index,
                        &partition.partition_key,
                    )?;
                if Self::vector_partition_journal_key(&record).as_slice() != key.value() {
                    return Err(self.vector_partition_corruption(
                        "maintenance journal key disagrees with its payload",
                    ));
                }
                if record.lsn <= after_lsn || record.lsn > covered_lsn {
                    continue;
                }
                if let VectorPartitionJournalChange::Tombstone(tombstone) = record.change {
                    let membership = tombstone.membership;
                    // BTree nodes are admitted before each possible insertion.
                    admit_identities(identities.len().saturating_add(1))?;
                    identities.insert((
                        membership.row_id,
                        membership.vector_created_tx,
                        membership.vector_lsn,
                    ));
                }
            }
            Ok(identities.len())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn reclaim_superseded_vector_partition_generations(
        &self,
        catalog: &VectorPartitionGenerationCatalogRecord,
        retained_chains: &[(u64, Option<u64>)],
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<(usize, u64)> {
        self.with_db(|db| {
            checkpoint()?;
            let write = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
            let result = self.reclaim_vector_generations_in_write_with_checkpoint(
                &write,
                catalog,
                retained_chains,
                true,
                &mut checkpoint,
            )?;
            checkpoint()?;
            Self::write_read_bounds_roots_in_write(&write)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(result)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reclaim_vector_generations_in_write(
        &self,
        write: &redb::WriteTransaction,
        catalog: &VectorPartitionGenerationCatalogRecord,
        retained_chains: &[(u64, Option<u64>)],
    ) -> Result<(usize, u64)> {
        self.prepare_proof_owner()?;
        // These legacy write transactions do not pass a decode workspace.
        // Own their fixed route-inspection scratch locally, never attach it
        // to a caller token that survives this helper's copies.
        let _inspection = self
            .proof_accountant()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    (64usize * 1024).saturating_add(
                        Self::vector_partition_physical_key_bound(
                            &catalog.index,
                            &catalog.partition_key,
                            32,
                        )
                        .saturating_mul(8),
                    ),
                    "vector",
                    "reclaim_vector_generation_metadata",
                    "Free writer memory before reclaiming vector generation metadata.",
                )
            })
            .transpose()?;
        self.reclaim_vector_generations_in_write_with_checkpoint(
            write,
            catalog,
            retained_chains,
            false,
            || Ok(()),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reclaim_vector_generations_in_write_with_checkpoint(
        &self,
        write: &redb::WriteTransaction,
        catalog: &VectorPartitionGenerationCatalogRecord,
        retained_chains: &[(u64, Option<u64>)],
        retire_damage: bool,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<(usize, u64)> {
        // Current and pinned identities are physical protection decisions.
        // Never decode a broken retired graph merely to decide its identity.
        const KEYS: usize = 64;
        let prefix = Self::vector_partition_prefix(&catalog.index, &catalog.partition_key);
        let has_membership = {
            let table = write
                .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                .map_err(Self::storage_error)?;
            table
                .prefix_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
                .next()
                .transpose()
                .map_err(Self::storage_error)?
                .is_some_and(|(key, _)| key.value().starts_with(&prefix))
        };
        let retain_catalog = has_membership || !retained_chains.is_empty();
        let mut remaining = KEYS;
        let mut reclaimed = 0;
        let mut bytes = 0u64;
        for (definition, is_base) in [
            (VECTOR_PARTITION_BASE_GENERATION_TABLE, true),
            (VECTOR_PARTITION_CHANGE_GENERATION_TABLE, false),
        ] {
            let mut table = write.open_table(definition).map_err(Self::storage_error)?;
            while remaining != 0 {
                let mut candidate = None;
                // There are at most two protected keys per held chain. Each
                // completed iteration removes a key or advances its page cleanup.
                for entry in table
                    .prefix_bounded(prefix.as_slice())
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    let suffix = &key.value()[prefix.len()..];
                    let expected = if is_base { 8 } else { 16 };
                    if suffix.len() != expected {
                        return Err(self.vector_partition_corruption(
                            "retired generation physical key is malformed",
                        ));
                    }
                    let base = u64::from_be_bytes(suffix[..8].try_into().unwrap());
                    let change =
                        (!is_base).then(|| u64::from_be_bytes(suffix[8..].try_into().unwrap()));
                    let selected = retain_catalog
                        && if is_base {
                            base == catalog.base.generation_id
                        } else {
                            base == catalog.base.generation_id
                                && change == catalog.change.map(|c| c.generation_id)
                        };
                    let pinned = retained_chains
                        .iter()
                        .any(|(b, c)| *b == base && (is_base || *c == change));
                    if selected || pinned {
                        continue;
                    }
                    // Authenticate both envelopes before a native storage id
                    // becomes deletion authority. Failure only leaves orphan
                    // discovery to the durable fence, never to guessed bytes.
                    checkpoint()?;
                    let native = match Self::authenticated_generation_storage(
                        value.value(),
                        key.value(),
                        is_base,
                    ) {
                        Ok(native) => native,
                        Err(error) => {
                            // Pruning/currency transactions keep their prior
                            // refusal; damaged retirement belongs to the
                            // cancellable repair/catalog maintenance path.
                            if !retire_damage {
                                return Err(error);
                            }
                            // This also covers fitting resident repair, not
                            // only the retirement-to-paged branch.
                            Self::raise_repair_fence_in_write(write)?;
                            None
                        }
                    };
                    candidate = Some((key.value().to_vec(), value.value().len() as u64, native));
                    break;
                }
                let Some((key, value_bytes, native)) = candidate else {
                    break;
                };
                let (complete, count, page_bytes) = Self::remove_retired_graph_pages_step_in_write(
                    write, &key, is_base, remaining,
                )?;
                remaining = remaining.saturating_sub(count);
                reclaimed += count;
                bytes = bytes.saturating_add(page_bytes);
                if !complete || remaining == 0 {
                    return Ok((reclaimed, bytes));
                }
                if let Some(id) = native {
                    Self::queue_authenticated_native_storage_in_write(write, id)?;
                }
                checkpoint()?;
                table.remove(key.as_slice()).map_err(Self::storage_error)?;
                remaining -= 1;
                reclaimed += 1;
                bytes = bytes.saturating_add(value_bytes);
            }
        }
        // A last-membership removal may only drop the catalog after both
        // generation tables are empty; partial page cleanup keeps the parent.
        if !retain_catalog {
            let any = [
                VECTOR_PARTITION_BASE_GENERATION_TABLE,
                VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
            ]
            .into_iter()
            .try_fold(false, |found, definition| -> Result<bool> {
                let table = write.open_table(definition).map_err(Self::storage_error)?;
                Ok(found
                    || table
                        .prefix_bounded(prefix.as_slice())
                        .map_err(Self::storage_error)?
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .is_some_and(|(key, _)| key.value().starts_with(&prefix)))
            })?;
            if !any {
                write
                    .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .map_err(Self::storage_error)?
                    .remove(prefix.as_slice())
                    .map_err(Self::storage_error)?;
                vector_membership_pages::reset_pending_frontier(
                    write,
                    &VectorPartitionRef::new(catalog.index.clone(), catalog.partition_key.clone()),
                )?;
            }
        }
        Ok((reclaimed, bytes))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remove_retired_graph_pages_step_in_write(
        write: &redb::WriteTransaction,
        generation_key: &[u8],
        is_base: bool,
        limit: usize,
    ) -> Result<(bool, usize, u64)> {
        let root = Self::vector_partition_graph_page_root_key(generation_key, is_base);
        let mut pages = write
            .open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE)
            .map_err(Self::storage_error)?;
        let mut count = 0;
        let mut bytes = 0u64;
        while count != limit {
            let key = {
                let next = pages
                    .prefix_bounded(root.as_slice())
                    .map_err(Self::storage_error)?
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?;
                let Some((key, value)) = next else {
                    return Ok((true, count, bytes));
                };
                if !key.value().starts_with(&root) {
                    return Ok((true, count, bytes));
                }
                if key.value().len() != root.len() && key.value().len() != root.len() + 4 {
                    return Err(vector_paged::corruption(
                        "retired graph page physical key is malformed",
                    ));
                }
                let key = key.value().to_vec();
                bytes = bytes.saturating_add(value.value().len() as u64);
                drop(value);
                key
            };
            pages.remove(key.as_slice()).map_err(Self::storage_error)?;
            count += 1;
        }
        Ok((false, count, bytes))
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn debug_superseded_vector_generation_retention_for_test(
        &self,
        catalog: &VectorPartitionGenerationCatalogRecord,
    ) -> Result<(usize, u64)> {
        let prefix = Self::vector_partition_prefix(&catalog.index, &catalog.partition_key);
        let selected_base = Self::vector_partition_base_generation_key_from_parts(
            &catalog.index,
            &catalog.partition_key,
            catalog.base.generation_id,
        );
        let selected_change = catalog.change.map(|change| {
            Self::vector_partition_change_generation_key_from_parts(
                &catalog.index,
                &catalog.partition_key,
                catalog.base.generation_id,
                change.generation_id,
            )
        });
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let mut count = 0usize;
            let mut bytes = 0u64;
            for (definition, selected) in [
                (
                    VECTOR_PARTITION_BASE_GENERATION_TABLE,
                    catalog.change.is_none().then_some(selected_base.as_slice()),
                ),
                (
                    VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
                    selected_change.as_deref(),
                ),
            ] {
                let table = match read.open_byte_table_bounded(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => continue,
                    Err(error) => return Err(Self::storage_error(error)),
                };
                for entry in table
                    .prefix_bounded(prefix.as_slice())
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    if selected.is_none_or(|selected| selected != key.value()) {
                        count = count.saturating_add(1);
                        bytes = bytes.saturating_add(value.value().len() as u64);
                    }
                }
            }
            Ok((count, bytes))
        })
    }

    /// Inspect only the fixed generation prefix. This is format selection,
    /// not graph authentication: the normal loader still verifies the full
    /// envelope and graph digests. No graph body is copied, hashed or decoded.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_partition_base_graph_format(
        &self,
        partition: &VectorPartitionRef,
        generation_id: u64,
    ) -> Result<u16> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = read
                .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(Self::storage_error)?;
            let key = Self::vector_partition_base_generation_key_from_parts(
                &partition.index,
                &partition.partition_key,
                generation_id,
            );
            let value = table
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| {
                    Error::Other("catalog points to a missing base generation".into())
                })?;
            let encoded = value.value();
            let header = VECTOR_PARTITION_RECORD_MAGIC.len() + 2 + 1 + 8;
            if encoded.len() < header + 32 || !encoded.starts_with(VECTOR_PARTITION_RECORD_MAGIC) {
                return Err(Error::Other(
                    "invalid base generation format metadata".into(),
                ));
            }
            let version = VECTOR_PARTITION_RECORD_MAGIC.len();
            if u16::from_be_bytes([encoded[version], encoded[version + 1]])
                != VECTOR_PARTITION_RECORD_VERSION
                || encoded[version + 2] != VectorPartitionRecordKind::BaseGeneration as u8
            {
                return Err(Error::Other(
                    "unsupported base generation format metadata".into(),
                ));
            }
            let length = u64::from_be_bytes(
                encoded[version + 3..header]
                    .try_into()
                    .map_err(|_| Error::Other("invalid base generation metadata length".into()))?,
            );
            if usize::try_from(length).ok() != Some(encoded.len() - header - 32) {
                return Err(Error::Other(
                    "invalid base generation metadata length".into(),
                ));
            }
            let payload = &encoded[header..encoded.len() - 32];
            let route = vector_partition_expected_route_prefix(
                payload,
                &partition.index,
                &partition.partition_key,
            )
            .map_err(Error::Other)?;
            type Prefix = (u64, TxId, Lsn, TxId, Lsn, u32, VectorQuantization, u16, u16);
            let (prefix, _): (Prefix, usize) =
                bincode::serde::decode_from_slice(&payload[route..], bincode::config::standard())
                    .map_err(|error| {
                    Error::Other((format!("invalid base generation prefix: {error}")).into())
                })?;
            if prefix.0 != generation_id
                || prefix.5 == 0
                || prefix.3 < prefix.1
                || prefix.4 < prefix.2
            {
                return Err(Error::Other(
                    "invalid base generation inclusion metadata".into(),
                ));
            }
            Ok(prefix.8)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_generation_record_bytes(
        &self,
        index: &VectorIndexRef,
        key: &VectorPartitionKey,
        base: u64,
        change: Option<u64>,
    ) -> Result<usize> {
        self.with_db(|db| {
            let root = self
                .scoped_vector_root_capture
                .get()
                .map(|capture| capture())
                .transpose()?
                .flatten();
            let read = match &root {
                Some(root) => root.owner_read()?,
                None => Arc::new(db.begin_read().map_err(Self::storage_error)?),
            };
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_physical_source(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                &read,
            );
            #[cfg(any(test, feature = "test-seams"))]
            let _graph_reads = crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                &index.table,
            );
            Self::generation_record_bytes_in_read(&read, index, key, base, change)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn generation_record_bytes_in_read(
        read: &redb::ReadTransaction,
        index: &VectorIndexRef,
        key: &VectorPartitionKey,
        base: u64,
        change: Option<u64>,
    ) -> Result<usize> {
        let base_key = Self::vector_partition_base_generation_key_from_parts(index, key, base);
        let base_table = read
            .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
            .map_err(Self::storage_error)?;
        let base_value = base_table
            .get_bounded(base_key.as_slice())
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::Other("catalog points to a missing base generation".into()))?;
        let mut bytes = base_value.value().len();
        if let Some(change) = change {
            let change_key =
                Self::vector_partition_change_generation_key_from_parts(index, key, base, change);
            let table = read
                .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(Self::storage_error)?;
            let value = table
                .get_bounded(change_key.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| {
                    Error::Other("catalog points to a missing sealed change generation".into())
                })?;
            bytes = bytes.saturating_add(value.value().len());
        }
        Ok(bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn captured_generation_corruption(reason: impl std::fmt::Display) -> Error {
        crate::funded_error::corruption_display(
            crate::read_image_memory::active_owner().and_then(|owner| owner.diagnostic_budget()),
            "vector_partition_generations",
            format_args!("vector partition durability record is corrupt: {reason}"),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn captured_generation_table(
        read: &redb::ReadTransaction,
        table: redb::TableDefinition<'static, &'static [u8], &'static [u8]>,
        missing: &str,
    ) -> Result<redb::ReadOnlyTable<&'static [u8], &'static [u8]>> {
        read.open_byte_table_bounded(table)
            .map_err(|error| match error {
                redb::TableError::Storage(error) => Self::storage_error(error),
                error => Self::captured_generation_corruption(format!("{missing}: {error}")),
            })
    }

    /// Read one immutable base body from an operation's already captured
    /// root. The caller holds that root for the whole decode; this never opens
    /// another read.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_base_generation_in_read(
        read: &redb::ReadTransaction,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
    ) -> Result<VectorPartitionBaseGenerationRecord> {
        let key = Self::vector_partition_base_generation_key_from_parts(
            index,
            partition_key,
            generation_id,
        );
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_physical_source(
            crate::writer_query_test_seams::PhysicalFamily::Graph,
            read,
        );
        #[cfg(any(test, feature = "test-seams"))]
        let _graph_reads = crate::writer_query_test_seams::physical_operation(
            crate::writer_query_test_seams::PhysicalFamily::Graph,
            &index.table,
        );
        let table = Self::captured_generation_table(
            read,
            VECTOR_PARTITION_BASE_GENERATION_TABLE,
            "catalog points to a missing base-generation table",
        )?;
        let value = table
            .get_bounded(key.as_slice())
            .map_err(Self::storage_error)?
            .ok_or_else(|| {
                Self::captured_generation_corruption("catalog points to a missing base generation")
            })?;
        drop(key);
        let record = borrow_vector_partition_base(
            value.value(),
            index,
            partition_key,
            generation_id,
            Self::captured_generation_corruption,
        )?;
        Ok(record.into_owned())
    }

    /// Read one immutable sealed-change body, and authenticate it against its
    /// base, from an operation's already captured root.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_change_generation_in_read(
        read: &redb::ReadTransaction,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        change_generation_id: u64,
    ) -> Result<VectorPartitionChangeGenerationRecord> {
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_physical_source(
            crate::writer_query_test_seams::PhysicalFamily::Graph,
            read,
        );
        #[cfg(any(test, feature = "test-seams"))]
        let _graph_reads = crate::writer_query_test_seams::physical_operation(
            crate::writer_query_test_seams::PhysicalFamily::Graph,
            &index.table,
        );
        let base_key = Self::vector_partition_base_generation_key_from_parts(
            index,
            partition_key,
            base_generation_id,
        );
        let base_table = Self::captured_generation_table(
            read,
            VECTOR_PARTITION_BASE_GENERATION_TABLE,
            "catalog points to a missing base-generation table",
        )?;
        let base_value = base_table
            .get_bounded(base_key.as_slice())
            .map_err(Self::storage_error)?
            .ok_or_else(|| {
                Self::captured_generation_corruption(
                    "change generation points to a missing base generation",
                )
            })?;
        let base = borrow_vector_partition_base(
            base_value.value(),
            index,
            partition_key,
            base_generation_id,
            Self::captured_generation_corruption,
        )?;
        let key = Self::vector_partition_change_generation_key_from_parts(
            index,
            partition_key,
            base_generation_id,
            change_generation_id,
        );
        let table = Self::captured_generation_table(
            read,
            VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
            "catalog points to a missing change-generation table",
        )?;
        let value = table
            .get_bounded(key.as_slice())
            .map_err(Self::storage_error)?
            .ok_or_else(|| {
                Self::captured_generation_corruption(
                    "catalog points to a missing sealed change generation",
                )
            })?;
        let record = borrow_vector_partition_change(
            value.value(),
            index,
            partition_key,
            &base,
            change_generation_id,
            Self::captured_generation_corruption,
        )?;
        Ok(record.into_owned())
    }

    /// Walk one route's journal suffix in an operation's captured root,
    /// streaming one authenticated record at a time. `admit_identities` is
    /// asked before each distinct upsert identity is retained, and
    /// `admit_bodies` once before the immutable vector bodies are read, so
    /// the caller funds growth past its saved minimum before it exists.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_tail_entries_in_read(
        read: &redb::ReadTransaction,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
        through_lsn: Option<Lsn>,
        mut checkpoint: impl FnMut() -> Result<()>,
        admit_bodies: impl FnOnce(usize) -> Result<()>,
    ) -> Result<(Vec<VectorEntry>, Lsn)> {
        let catalog =
            Self::vector_partition_generation_descriptors_in_read(read, index, partition_key)?;
        let floor = catalog.map_or(Lsn(0), |(base, change)| change.unwrap_or(base).covered_lsn);
        if covered_lsn < floor {
            return Err(Self::captured_generation_corruption(
                "captured suffix replay starts below the journal truncation floor",
            ));
        }
        let Some((lower, upper)) = Self::vector_partition_journal_suffix_bounds(
            index,
            partition_key,
            covered_lsn,
            through_lsn,
        )?
        else {
            return Ok((Vec::new(), covered_lsn));
        };
        let journal = match read.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), covered_lsn)),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let mut identities = BTreeSet::new();
        let mut replayed_through_lsn = covered_lsn;
        for entry in journal
            .range(lower.as_slice()..upper.as_slice())
            .map_err(Self::storage_error)?
        {
            checkpoint()?;
            let (key, value) = entry.map_err(Self::storage_error)?;
            #[cfg(test)]
            bounded_journal_suffix_tests::note_examined();
            let record: VectorPartitionJournalRecord = decode_vector_partition_record_payload(
                VectorPartitionRecordKind::Journal,
                value.value(),
            )
            .map_err(Self::captured_generation_corruption)?;
            if record.index != *index
                || record.partition_key != *partition_key
                || Self::vector_partition_journal_key(&record).as_slice() != key.value()
            {
                return Err(Self::captured_generation_corruption(
                    "journal selection encountered a mismatched route or physical key",
                ));
            }
            if record.lsn <= covered_lsn || through_lsn.is_some_and(|through| record.lsn > through)
            {
                continue;
            }
            #[cfg(any(test, feature = "test-seams"))]
            crate::vector_observations::emit(
                index,
                partition_key,
                covered_lsn,
                crate::vector_observations::VectorJournalReplayWork::Record { lsn: record.lsn },
            );
            replayed_through_lsn = replayed_through_lsn.max(record.lsn);
            if let VectorPartitionJournalChange::Upsert(membership) = record.change {
                identities.insert((
                    membership.row_id,
                    membership.vector_created_tx,
                    membership.vector_lsn,
                ));
            }
        }
        drop(journal);
        admit_bodies(identities.len())?;
        if identities.is_empty() {
            return Ok((Vec::new(), replayed_through_lsn));
        }
        let vectors = read
            .open_byte_table_bounded(VECTORS_TABLE)
            .map_err(Self::storage_error)?;
        let mut tail = Vec::with_capacity(identities.len());
        for (row_id, created_tx, lsn) in identities {
            checkpoint()?;
            let key = Self::vector_identity_key(index, row_id, created_tx, lsn);
            let value = vectors
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| {
                    Self::captured_generation_corruption(
                        "journal tail points to a missing immutable vector",
                    )
                })?;
            let entry = Self::decode_vector_entry(value.value())?;
            if entry.index != *index
                || entry.row_id != row_id
                || entry.created_tx != created_tx
                || entry.lsn != lsn
            {
                return Err(Self::captured_generation_corruption(
                    "journal tail vector key disagrees with its payload",
                ));
            }
            #[cfg(any(test, feature = "test-seams"))]
            crate::vector_observations::emit(
                index,
                partition_key,
                covered_lsn,
                crate::vector_observations::VectorJournalReplayWork::Vector {
                    row_id,
                    created_tx,
                    lsn,
                },
            );
            tail.push(entry);
        }
        Ok((tail, replayed_through_lsn))
    }

    /// Saved suffix admission for one serving frontier at an operation's
    /// captured root: the live inserts after `frontier` as the membership
    /// summary committed them with the catalog. Reads one summary key; it
    /// never walks the journal or the membership directory. A frontier the
    /// root's summary does not represent is refused, never estimated.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_suffix_saved_inserts_in_read(
        read: &redb::ReadTransaction,
        partition: &VectorPartitionRef,
        frontier: TxId,
    ) -> Result<usize> {
        let summary = vector_membership_pages::read_partition_summary(read, partition)?;
        if !partition.partition_key.is_unpartitioned()
            && summary == vector_membership_pages::MembershipSummary::default()
        {
            // Missing summary beside a pinned partition catalog is validated
            // empty membership, not a missing suffix. There are no inserts.
            return Ok(0);
        }
        let counts = std::iter::once(&summary.pending)
            .chain(summary.base_pending.iter())
            .chain(summary.candidate_pending.iter())
            .find(|counts| counts.covered_tx == frontier)
            .ok_or_else(|| {
                vector_membership_source::damage(
                    "captured pending summary does not represent the serving frontier",
                )
            })?;
        usize::try_from(counts.pending_inserts)
            .map_err(|_| vector_membership_source::damage("pending insert count overflow"))
    }

    /// The column declaration in force at an operation's captured root.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_layout_in_read(
        read: &redb::ReadTransaction,
        index: &VectorIndexRef,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<contextdb_vector::store::VectorIndexLayout> {
        let meta = Self::load_table_meta_in_read_txn(read, &index.table)?;
        if let Some(column) = meta.as_ref().and_then(|meta| {
            meta.columns
                .iter()
                .find(|column| column.name == index.column)
        }) {
            if let Some(names) = &column.partition_key_columns {
                let bytes = names
                    .iter()
                    .fold(
                        names.len().checked_mul(std::mem::size_of::<String>()),
                        |bytes, name| bytes.and_then(|bytes| bytes.checked_add(name.len())),
                    )
                    .ok_or_else(|| Error::Other("captured layout size overflow".into()))?;
                // Admit before the layout clones its retained strings.
                owner.reserve(bytes)?;
            }
            if let Some(layout) = crate::database::vector_index_layout_from_column(column) {
                return Ok(layout);
            }
        }
        Err(vector_membership_source::damage(
            "captured serving root does not declare this vector column",
        ))
    }

    /// Bind one operation's already captured writable root to the engine's
    /// root-owned serving supplier. The membership bridge authenticated this
    /// exact read and writer; nothing here opens, recaptures or republishes a
    /// root, and nothing reads a graph payload.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn captured_vector_serving_selection_in_read(
        self: &Arc<Self>,
        read: Arc<redb::ReadTransaction>,
        writer: Option<WriterSnapshotHandle>,
        retained: Option<RetainedReadSource>,
        partition: &VectorPartitionRef,
        generations: Option<(
            VectorPartitionGenerationDescriptor,
            Option<VectorPartitionGenerationDescriptor>,
        )>,
        inspection: contextdb_vector::store::RawVectorInspectionSnapshot,
        snapshot: SnapshotId,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<contextdb_vector::store::CapturedVectorServingSelection> {
        let writer = writer.ok_or_else(|| {
            vector_membership_source::damage("writable vector serving has no operation-scoped root")
        })?;
        if retained.is_some() {
            return Err(vector_membership_source::damage(
                "writable vector serving received a committed-image root",
            ));
        }
        crate::database::vector_captured_serving::captured_serving_selection(
            crate::database::vector_captured_serving::CapturedVectorRootLease::writable(
                read, writer,
            )?,
            partition,
            generations,
            inspection,
            snapshot,
            owner,
        )
    }

    /// Read one immutable base body named by an already validated lightweight
    /// catalog descriptor. Lazy restart reserves its cataloged workspace and
    /// resident bytes before calling this method.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_base_generation(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
    ) -> Result<VectorPartitionBaseGenerationRecord> {
        self.load_vector_partition_base_generation_with_size(
            index,
            partition_key,
            generation_id,
            None,
            self.scoped_vector_root_capture
                .get()
                .map(|capture| capture())
                .transpose()?
                .flatten(),
            false,
        )
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from prior_base_decode_tests.rs"
        )
    )]
    pub(crate) fn load_vector_partition_base_generation_with_expected_graph_bytes(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
        expected_graph_bytes: u64,
    ) -> Result<VectorPartitionBaseGenerationRecord> {
        let mut base = self.load_vector_partition_base_generation_with_size(
            index,
            partition_key,
            generation_id,
            Some(expected_graph_bytes),
            None,
            false,
        )?;
        if base.graph.bytes.starts_with(&vector_paged::META_MAGIC) {
            base.graph = VectorPartitionGraphBytes::verified(
                self.materialize_paged_topology_for_test(&base.graph.bytes)?,
            );
        }
        Ok(base)
    }

    /// Maintenance carries the selected native source manifest forward without
    /// loading its graph. Serving opens its pages through the native reader.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_base_manifest_with_expected_graph_bytes(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
        expected_graph_bytes: u64,
    ) -> Result<VectorPartitionBaseGenerationRecord> {
        self.load_vector_partition_base_generation_with_size(
            index,
            partition_key,
            generation_id,
            Some(expected_graph_bytes),
            None,
            true,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_vector_partition_base_generation_with_size(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        generation_id: u64,
        expected_graph_bytes: Option<u64>,
        root: Option<WriterSnapshotHandle>,
        native_only: bool,
    ) -> Result<VectorPartitionBaseGenerationRecord> {
        let key = Self::vector_partition_base_generation_key_from_parts(
            index,
            partition_key,
            generation_id,
        );
        self.with_db(|db| {
            // Keep the operation's captured root and its lease throughout decoding.
            let read_txn = match &root {
                Some(root) => root.owner_read()?,
                None => Arc::new(db.begin_read().map_err(Self::storage_error)?),
            };
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_physical_source(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                &read_txn,
            );
            #[cfg(any(test, feature = "test-seams"))]
            let _graph_reads = crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Graph,
                &index.table,
            );

            let table = read_txn
                .open_byte_table_bounded(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(|error| match error {
                    redb::TableError::Storage(error) => Self::storage_error(error),
                    error => self.vector_partition_corruption(format!(
                        "catalog points to a missing base-generation table: {error}"
                    )),
                })?;
            let value = table
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| {
                    self.vector_partition_corruption("catalog points to a missing base generation")
                })?;
            // Release the lookup key before decoding owned route metadata.
            // Expected-route comparison streams without a prefix allocation.
            drop(key);
            let borrowed = borrow_vector_partition_base(
                value.value(), index, partition_key, generation_id,
                |reason| self.vector_partition_corruption(reason),
            )?;
            if let Some(expected) = expected_graph_bytes
                && u64::try_from(borrowed.graph.bytes.len()).ok() != Some(expected)
            {
                return Err(self.vector_partition_corruption(format!(
                    "base graph byte length {} disagrees with catalog durable byte declaration {expected}",
                    borrowed.graph.bytes.len()
                )));
            }
            if native_only && !borrowed.graph.bytes.starts_with(&vector_paged::META_MAGIC) {
                return Err(self.vector_partition_corruption("maintenance base has no native paged source"));
            }
            let record = borrowed.into_owned();
            Ok(record)
        })
    }

    /// Read one immutable sealed-change body named by an already validated
    /// catalog descriptor.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_change_generation(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        change_generation_id: u64,
    ) -> Result<VectorPartitionChangeGenerationRecord> {
        let key = Self::vector_partition_change_generation_key_from_parts(
            index,
            partition_key,
            base_generation_id,
            change_generation_id,
        );
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let base_key = Self::vector_partition_base_generation_key_from_parts(
                index,
                partition_key,
                base_generation_id,
            );
            let base_table = read_txn
                .open_byte_table_bounded(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(|error| match error {
                    redb::TableError::Storage(error) => Self::storage_error(error),
                    error => self.vector_partition_corruption(format!(
                        "catalog points to a missing base-generation table: {error}"
                    )),
                })?;
            let base_value = base_table
                .get_bounded(base_key.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| {
                    self.vector_partition_corruption(
                        "change generation points to a missing base generation",
                    )
                })?;
            let base = borrow_vector_partition_base(
                base_value.value(),
                index,
                partition_key,
                base_generation_id,
                |reason| self.vector_partition_corruption(reason),
            )?;
            let table = read_txn
                .open_byte_table_bounded(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(|error| match error {
                    redb::TableError::Storage(error) => Self::storage_error(error),
                    error => self.vector_partition_corruption(format!(
                        "catalog points to a missing change-generation table: {error}"
                    )),
                })?;
            let value = table
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| {
                    self.vector_partition_corruption(
                        "catalog points to a missing sealed change generation",
                    )
                })?;
            let record = borrow_vector_partition_change(
                value.value(),
                index,
                partition_key,
                &base,
                change_generation_id,
                |reason| self.vector_partition_corruption(reason),
            )?;
            let record = record.into_owned();
            Ok(record)
        })
    }

    /// Constant-size witness for an optional writer tail: the exact committed
    /// catalog and final journal record, authenticated at one root. The last
    /// journal key is a B-tree seek, never a prefix walk.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn writer_vector_tail_frontier(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<Option<[u8; 32]>> {
        let prefix = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let catalogs =
                match read.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(error) => return Err(Self::storage_error(error)),
                };
            let Some(value) = catalogs
                .get_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
            else {
                return Ok(None);
            };
            let catalog: VectorPartitionGenerationCatalogRecord = self
                .decode_expected_vector_partition_metadata(
                    VectorPartitionRecordKind::GenerationCatalog,
                    value.value(),
                    &partition.index,
                    &partition.partition_key,
                )?;
            if catalog.index != partition.index || catalog.partition_key != partition.partition_key
            {
                return Ok(None);
            }
            let mut digest = blake3::Hasher::new();
            digest.update(value.value());
            match read.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => {
                    let upper = writer_source::prefix_end(&prefix).ok_or_else(|| {
                        self.vector_partition_corruption("journal prefix has no upper bound")
                    })?;
                    if let Some(entry) = table
                        .range(prefix.as_slice()..upper.as_slice())
                        .map_err(Self::storage_error)?
                        .next_back()
                    {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        let record: VectorPartitionJournalRecord = self
                            .decode_vector_partition_record(
                                VectorPartitionRecordKind::Journal,
                                value.value(),
                            )?;
                        if record.index != partition.index
                            || record.partition_key != partition.partition_key
                            || Self::vector_partition_journal_key(&record).as_slice() != key.value()
                        {
                            return Err(self.vector_partition_corruption(
                                "tail frontier journal identity mismatch",
                            ));
                        }
                        digest.update(key.value());
                        digest.update(value.value());
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(Self::storage_error(error)),
            }
            Ok(Some(*digest.finalize().as_bytes()))
        })
    }

    /// Seek strictly after the complete covered LSN cohort, including every
    /// ordinal of that LSN. A finite upper frontier also ends after its whole
    /// cohort. Both keys stay inside this route's canonical prefix.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_partition_journal_suffix_bounds(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
        through_lsn: Option<Lsn>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let Some(first_lsn) = covered_lsn.0.checked_add(1) else {
            return Ok(None);
        };
        if through_lsn.is_some_and(|through| through <= covered_lsn) {
            return Ok(None);
        }
        let prefix = Self::vector_partition_prefix(index, partition_key);
        let mut lower = prefix.clone();
        lower.extend_from_slice(&first_lsn.to_be_bytes());
        let upper = if let Some(next_lsn) = through_lsn.and_then(|through| through.0.checked_add(1))
        {
            let mut upper = prefix;
            upper.extend_from_slice(&next_lsn.to_be_bytes());
            upper
        } else {
            writer_source::prefix_end(&prefix).ok_or_else(|| {
                Self::captured_generation_corruption("journal route has no upper bound")
            })?
        };
        Ok(Some((lower, upper)))
    }

    /// Materialize only the vector insert/move suffix after a sealed coverage
    /// point. The ordered journal chooses exact immutable vector identities;
    /// no partition or whole-vector-table scan occurs.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_partition_tail_entry_count(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
    ) -> Result<usize> {
        self.vector_partition_tail_entry_count_through(index, partition_key, covered_lsn, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_partition_tail_entry_count_through(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
        through_lsn: Option<Lsn>,
    ) -> Result<usize> {
        let Some((lower, upper)) = Self::vector_partition_journal_suffix_bounds(
            index,
            partition_key,
            covered_lsn,
            through_lsn,
        )?
        else {
            return Ok(0);
        };
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut count = 0usize;
            for entry in table
                .range(lower.as_slice()..upper.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                #[cfg(test)]
                bounded_journal_suffix_tests::note_examined();
                let record: VectorPartitionJournalRecord = self.decode_vector_partition_record(
                    VectorPartitionRecordKind::Journal,
                    value.value(),
                )?;
                if Self::vector_partition_journal_key(&record).as_slice() != key.value() {
                    return Err(self.vector_partition_corruption(
                        "journal key does not match its checksummed payload",
                    ));
                }
                if record.lsn.0 > covered_lsn.0
                    && matches!(record.change, VectorPartitionJournalChange::Upsert(_))
                {
                    // This is deliberately an upper bound: a repeated
                    // immutable identity is de-duplicated by the loader, and
                    // its unused reservation is returned after materialize.
                    count = count.checked_add(1).ok_or_else(|| {
                        self.vector_partition_corruption(
                            "journal tail entry count exceeds the process range",
                        )
                    })?;
                }
            }
            Ok(count)
        })
    }

    /// Inspect physical journal identities without decoding or cloning their
    /// payloads. A cleaned catalog need not re-enter its cleanup transaction.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_partition_has_covered_journal_records(
        &self,
        catalog: &VectorPartitionGenerationCatalogRecord,
    ) -> Result<bool> {
        let prefix = Self::vector_partition_prefix(&catalog.index, &catalog.partition_key);
        let covered_lsn = catalog
            .change
            .map(|change| change.covered_lsn)
            .unwrap_or(catalog.base.covered_lsn);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = match read.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let Some(entry) = table
                .prefix_bounded(prefix.as_slice())
                .map_err(Self::storage_error)?
                .next()
            else {
                return Ok(false);
            };
            let (key, _) = entry.map_err(Self::storage_error)?;
            if !key.value().starts_with(&prefix) {
                return Ok(false);
            }
            let lsn = key
                .value()
                .get(prefix.len()..prefix.len().saturating_add(8))
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| {
                    self.vector_partition_corruption("journal identity has a truncated LSN")
                })?;
            // Big-endian LSNs sort first within this route. If its first
            // record is uncovered, every later record is uncovered too.
            Ok(lsn <= covered_lsn.0)
        })
    }

    /// Report whether this route has any durable journal work beyond the
    /// generation catalog's covered frontier. Deletes deliberately count:
    /// they add no mutable graph point, but a later generation must still
    /// represent their authoritative absence before the journal can advance.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn vector_partition_has_uncovered_journal_records(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
    ) -> Result<bool> {
        self.vector_partition_has_uncovered_journal_records_with_admission(
            index,
            partition_key,
            covered_lsn,
            |_| Ok(()),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_partition_has_uncovered_journal_records_with_admission(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
        mut admit_decode: impl FnMut(usize) -> Result<()>,
    ) -> Result<bool> {
        let Some((lower, upper)) =
            Self::vector_partition_journal_suffix_bounds(index, partition_key, covered_lsn, None)?
        else {
            return Ok(false);
        };
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
                Err(error) => return Err(Self::storage_error(error)),
            };
            for entry in table
                .range(lower.as_slice()..upper.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                #[cfg(test)]
                bounded_journal_suffix_tests::note_examined();
                admit_decode(Self::vector_partition_record_decode_bound(
                    value.value().len(),
                    std::mem::size_of::<VectorPartitionJournalRecord>(),
                ))?;
                let record: VectorPartitionJournalRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::Journal,
                        value.value(),
                        index,
                        partition_key,
                    )?;
                if record.index != *index
                    || record.partition_key != *partition_key
                    || Self::vector_partition_journal_key(&record).as_slice() != key.value()
                {
                    return Err(self.vector_partition_corruption(
                        "journal selection encountered a mismatched route or physical key",
                    ));
                }
                if record.lsn.0 > covered_lsn.0 {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    /// Resolve the immutable vector bodies named by uncovered tombstones.
    /// Delete-only generation publication needs those bodies as non-visible
    /// graph carriers even though they are absent from the live entry set.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_uncovered_tombstone_entries(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
        through_lsn: Lsn,
        expected_dimension: usize,
    ) -> Result<Vec<VectorEntry>> {
        let Some((lower, upper)) = Self::vector_partition_journal_suffix_bounds(
            index,
            partition_key,
            covered_lsn,
            Some(through_lsn),
        )?
        else {
            return Ok(Vec::new());
        };
        let memberships = self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let journal = match read_txn.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut memberships = BTreeMap::new();
            for item in journal
                .range(lower.as_slice()..upper.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = item.map_err(Self::storage_error)?;
                #[cfg(test)]
                bounded_journal_suffix_tests::note_examined();
                let record: VectorPartitionJournalRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::Journal,
                        value.value(),
                        index,
                        partition_key,
                    )?;
                if Self::vector_partition_journal_key(&record).as_slice() != key.value() {
                    return Err(self.vector_partition_corruption(
                        "maintenance journal key disagrees with its payload",
                    ));
                }
                if record.lsn <= covered_lsn || record.lsn > through_lsn {
                    continue;
                }
                if let VectorPartitionJournalChange::Tombstone(tombstone) = record.change {
                    let membership = tombstone.membership;
                    memberships.insert(
                        (
                            membership.row_id,
                            membership.vector_created_tx,
                            membership.vector_lsn,
                        ),
                        membership,
                    );
                }
            }
            Ok(memberships.into_values().collect::<Vec<_>>())
        })?;
        self.load_vector_entries_for_memberships(&memberships, expected_dimension, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_tail_entries(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
    ) -> Result<(Vec<VectorEntry>, Lsn)> {
        self.load_vector_partition_tail_entries_through(index, partition_key, covered_lsn, None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_vector_partition_tail_entries_through(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        covered_lsn: Lsn,
        through_lsn: Option<Lsn>,
    ) -> Result<(Vec<VectorEntry>, Lsn)> {
        let _decode_scope = self.reader_decode_scope()?;
        let Some((lower, upper)) = Self::vector_partition_journal_suffix_bounds(
            index,
            partition_key,
            covered_lsn,
            through_lsn,
        )?
        else {
            return Ok((Vec::new(), covered_lsn));
        };
        self.prepare_proof_owner()?;
        self.with_db_handle_loan(|db| {
            let read_txn =
                self.with_storage_admission(|| db.begin_read().map_err(Self::storage_error))?;
            let journal = self.with_storage_admission(|| {
                match read_txn.open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE) {
                    Ok(table) => Ok(Some(table)),
                    Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
                    Err(error) => Err(Self::storage_error(error)),
                }
            })?;
            let Some(journal) = journal else {
                return Ok((Vec::new(), covered_lsn));
            };
            let mut identities = BTreeSet::new();
            let mut replayed_through_lsn = covered_lsn;
            let mut range = self.with_storage_admission(|| {
                journal
                    .range(lower.as_slice()..upper.as_slice())
                    .map_err(Self::storage_error)
            })?;
            while let Some((key, value)) = self
                .with_storage_admission(|| range.next().transpose().map_err(Self::storage_error))?
            {
                // Native page/record receipts retain the real payer themselves.
                // The IO admission control is physically gone before the codec.

                #[cfg(test)]
                bounded_journal_suffix_tests::note_examined();
                let record: VectorPartitionJournalRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::Journal,
                        value.value(),
                        index,
                        partition_key,
                    )?;
                if Self::vector_partition_journal_key(&record).as_slice() != key.value() {
                    return Err(self.vector_partition_corruption(
                        "journal tail key disagrees with its payload",
                    ));
                }
                if record.lsn.0 <= covered_lsn.0 {
                    continue;
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::vector_observations::emit(
                    index,
                    partition_key,
                    covered_lsn,
                    crate::vector_observations::VectorJournalReplayWork::Record { lsn: record.lsn },
                );
                replayed_through_lsn = replayed_through_lsn.max(record.lsn);
                if let VectorPartitionJournalChange::Upsert(membership) = record.change {
                    identities.insert((
                        membership.row_id,
                        membership.vector_created_tx,
                        membership.vector_lsn,
                    ));
                }
            }
            drop(range);
            drop(journal);
            if identities.is_empty() {
                return Ok((Vec::new(), replayed_through_lsn));
            }
            let vectors = self.with_storage_admission(|| {
                read_txn
                    .open_byte_table_bounded(VECTORS_TABLE)
                    .map_err(Self::storage_error)
            })?;
            let mut tail = Vec::with_capacity(identities.len());
            for (row_id, created_tx, lsn) in identities {
                let key_bytes = index
                    .table
                    .len()
                    .checked_add(index.column.len())
                    .and_then(|bytes| bytes.checked_add(34))
                    .ok_or_else(|| Error::Other("vector identity key size overflow".into()))?;
                let key = crate::local_transport::query_funding::Funded::allocate_owned(
                    Some(crate::read_image_memory::active_owner()?.owner()),
                    key_bytes,
                    || Self::vector_identity_key(index, row_id, created_tx, lsn),
                )?;
                let value = self
                    .with_storage_admission(|| {
                        vectors
                            .get_bounded(key.value.as_slice())
                            .map_err(Self::storage_error)
                    })?
                    .ok_or_else(|| {
                        self.vector_partition_corruption(
                            "journal tail points to a missing immutable vector",
                        )
                    })?;
                let entry = Self::decode_vector_entry(value.value())?;
                if entry.index != *index
                    || entry.row_id != row_id
                    || entry.created_tx != created_tx
                    || entry.lsn != lsn
                {
                    return Err(self.vector_partition_corruption(
                        "journal tail vector key disagrees with its payload",
                    ));
                }
                #[cfg(any(test, feature = "test-seams"))]
                crate::vector_observations::emit(
                    index,
                    partition_key,
                    covered_lsn,
                    crate::vector_observations::VectorJournalReplayWork::Vector {
                        row_id,
                        created_tx,
                        lsn,
                    },
                );
                tail.push(entry);
            }
            Ok((tail, replayed_through_lsn))
        })
    }

    /// Redb transaction T1: immutable objects first and the catalog pointer
    /// last, with one durable commit. Encoding and validation belong to the
    /// caller's preparation phase and have already completed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn publish_prepared_vector_partition_generation_candidate(
        &self,
        prepared: &PreencodedVectorPartitionGenerationCandidate,
    ) -> Result<Option<(usize, usize)>> {
        self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            self.insert_preencoded_vector_partition_generation_objects_in_write(
                &write_txn, prepared,
            )?;
            let pending = self.publish_preencoded_vector_partition_generation_catalog_in_write(
                &write_txn, prepared,
            )?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_commit_test_seams::reached(
                crate::writer_commit_test_seams::CommitBoundary::GenerationBeforeRedb,
            );
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(pending)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_bound_vector_partition_graph_pages(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        generation_id: u64,
        is_base: bool,
        generation_key: &[u8],
        graph: &VectorPartitionGraphBytes,
    ) -> Result<PreencodedVectorPartitionGraphPages> {
        let schema_binding = if graph.bytes.starts_with(&vector_paged::META_MAGIC) {
            vector_paged::PagedGenerationMeta::decode(&graph.bytes)?.schema_binding
        } else {
            self.vector_schema_binding(index)?
        };
        Self::prepare_vector_partition_graph_pages_with_binding(
            index,
            partition_key,
            base_generation_id,
            generation_id,
            is_base,
            generation_key,
            graph,
            schema_binding,
        )
    }

    // Preserve the existing framing-only fixture entry. An unbound frame can
    // never pass the transactional publication guard.
    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from generation_admission_tests.rs"
        )
    )]
    pub(super) fn prepare_vector_partition_graph_pages(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        generation_id: u64,
        is_base: bool,
        generation_key: &[u8],
        graph: &VectorPartitionGraphBytes,
    ) -> Result<PreencodedVectorPartitionGraphPages> {
        Self::prepare_vector_partition_graph_pages_with_binding(
            index,
            partition_key,
            base_generation_id,
            generation_id,
            is_base,
            generation_key,
            graph,
            VectorSchemaBinding {
                table_generation: 0,
                column_incarnation: 0,
            },
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_vector_partition_graph_pages_with_binding(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        base_generation_id: u64,
        generation_id: u64,
        is_base: bool,
        generation_key: &[u8],
        graph: &VectorPartitionGraphBytes,
        schema_binding: VectorSchemaBinding,
    ) -> Result<PreencodedVectorPartitionGraphPages> {
        let root_key = Self::vector_partition_graph_page_root_key(generation_key, is_base);
        // A graph page occupies one 64 KiB Redb allocation including its
        // variable key, leaf framing, ordinal, checksum, and body. The prior
        // body-only 64 KiB sizing crossed the allocator boundary and doubled
        // every page's backend/cache footprint.
        let page_bytes = VECTOR_PARTITION_GRAPH_PAGE_RECORD_BYTES
            .checked_sub(
                VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES
                    .saturating_add(root_key.len())
                    .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES)
                    .saturating_add(VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES),
            )
            .filter(|bytes| *bytes != 0)
            .ok_or_else(|| {
                Error::Other(("saved graph page identity exceeds 64 KiB".to_string()).into())
            })?;
        let page_count = graph.bytes.len().div_ceil(page_bytes);
        // Validate the ordinal range before any destructive transaction.
        u32::try_from(page_count).map_err(|_| {
            Error::Other(("saved graph has too many physical pages".to_string()).into())
        })?;
        let manifest = VectorPartitionGraphPageManifestRecord {
            index: index.clone(),
            partition_key: partition_key.clone(),
            base_generation_id,
            generation_id,
            is_base,
            page_bytes: u32::try_from(page_bytes).map_err(|_| {
                Error::Other(("saved graph page size exceeds u32".to_string()).into())
            })?,
            graph_bytes: graph.bytes.len() as u64,
            graph_checksum: graph.checksum,
            page_count: u32::try_from(page_count).map_err(|_| {
                Error::Other(("saved graph has too many physical pages".to_string()).into())
            })?,
            schema_binding,
        };
        let manifest_bytes = Self::encode_vector_partition_record(
            VectorPartitionRecordKind::GraphPageManifest,
            &manifest,
        )?;
        let decoded: VectorPartitionGraphPageManifestRecord =
            decode_vector_partition_record_payload(
                VectorPartitionRecordKind::GraphPageManifest,
                &manifest_bytes,
            )?;
        if decoded != manifest {
            return Err(Error::Other(
                ("saved graph page manifest failed its preparation round trip".to_string()).into(),
            ));
        }
        Ok(PreencodedVectorPartitionGraphPages {
            root_key,
            manifest_bytes,
            graph_bytes: graph.bytes.clone(),
            page_bytes,
        })
    }

    /// The single insertion frame, priced from borrowed immutable candidate
    /// bytes. Base and change pages are streamed sequentially.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn vector_candidate_streamed_frame_bytes(
        candidate: &VectorPartitionGenerationCandidate,
    ) -> Result<usize> {
        let base = Self::vector_streamed_frame_bytes(
            &candidate.base.index,
            &candidate.base.partition_key,
            8,
            candidate.base.graph.bytes.len(),
        )?;
        let change = candidate
            .change
            .as_ref()
            .map(|change| {
                Self::vector_streamed_frame_bytes(
                    &change.index,
                    &change.partition_key,
                    16,
                    change.graph.bytes.len(),
                )
            })
            .transpose()?
            .unwrap_or(0);
        Ok(base.max(change))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn vector_streamed_frame_bytes(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        suffix: usize,
        graph_bytes: usize,
    ) -> Result<usize> {
        use contextdb_core::VectorPartitionComponent as Component;
        if graph_bytes == 0 {
            return Ok(0);
        }
        let overflow =
            || Error::Other(("saved graph page identity size overflow".to_string()).into());
        let mut canonical = 9usize;
        for component in partition_key.components() {
            let bytes = match component {
                Component::Uuid(_) => 17,
                Component::Text(value) => value.len().checked_add(9).ok_or_else(overflow)?,
                Component::Boolean(_) => 2,
                Component::Integer(_) | Component::Timestamp(_) | Component::TxId(_) => 9,
            };
            canonical = canonical.checked_add(bytes).ok_or_else(overflow)?;
        }
        let generation_key = index
            .table
            .len()
            .checked_add(index.column.len())
            .and_then(|bytes| bytes.checked_add(24))
            .and_then(|bytes| bytes.checked_add(canonical))
            .and_then(|bytes| bytes.checked_add(suffix))
            .ok_or_else(overflow)?;
        let ordinary_root = generation_key.checked_add(1).ok_or_else(overflow)?;
        let frame_overhead =
            VECTOR_PARTITION_GRAPH_PAGE_KEY_SUFFIX_BYTES + VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES;
        let root = if ordinary_root
            .checked_add(frame_overhead)
            .and_then(|bytes| bytes.checked_add(VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES))
            .ok_or_else(overflow)?
            < VECTOR_PARTITION_GRAPH_PAGE_RECORD_BYTES
        {
            ordinary_root
        } else {
            36
        };
        let overhead = root.checked_add(frame_overhead).ok_or_else(overflow)?;
        let body_limit = VECTOR_PARTITION_GRAPH_PAGE_RECORD_BYTES
            .checked_sub(overhead + VECTOR_PARTITION_GRAPH_PAGE_LEAF_FRAME_BYTES)
            .filter(|bytes| *bytes != 0)
            .ok_or_else(overflow)?;
        overhead
            .checked_add(graph_bytes.min(body_limit))
            .ok_or_else(overflow)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn prepare_vector_partition_generation_candidate(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
    ) -> Result<PreencodedVectorPartitionGenerationCandidate> {
        self.prepare_vector_partition_generation_candidate_with_admission(candidate, |_| Ok(()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_vector_partition_generation_candidate_with_admission(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
        admit_scan: impl FnMut(usize) -> Result<()>,
    ) -> Result<PreencodedVectorPartitionGenerationCandidate> {
        self.prepare_vector_partition_generation_candidate_inner(candidate, false, admit_scan)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_vector_partition_generation_candidate_inner(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
        preserve_purge_identity: bool,
        mut admit_scan: impl FnMut(usize) -> Result<()>,
    ) -> Result<PreencodedVectorPartitionGenerationCandidate> {
        self.validate_vector_partition_generation_candidate(candidate)?;
        let mut paged_candidate = if preserve_purge_identity {
            self.prepare_saved_topology_candidate_for_purge(candidate, &mut admit_scan)?
        } else {
            self.prepare_saved_topology_candidate(candidate, &mut admit_scan)?
        };
        let candidate = paged_candidate
            .as_ref()
            .map(|saved| &saved.candidate)
            .unwrap_or(candidate);
        self.validate_vector_partition_generation_candidate(candidate)?;
        let base_key = Self::vector_partition_base_generation_key(&candidate.base);
        let base_bytes = Self::encode_vector_partition_record(
            VectorPartitionRecordKind::BaseGeneration,
            &candidate.base,
        )?;
        let _: VectorPartitionBaseGenerationRecord = self.decode_vector_partition_record(
            VectorPartitionRecordKind::BaseGeneration,
            &base_bytes,
        )?;
        let base_pages = self.prepare_bound_vector_partition_graph_pages(
            &candidate.base.index,
            &candidate.base.partition_key,
            candidate.base.generation_id,
            candidate.base.generation_id,
            true,
            &base_key,
            &candidate.base.graph,
        )?;
        let change = candidate
            .change
            .as_ref()
            .map(|change| -> Result<_> {
                let key = Self::vector_partition_change_generation_key(change);
                let bytes = Self::encode_vector_partition_record(
                    VectorPartitionRecordKind::ChangeGeneration,
                    change,
                )?;
                let _: VectorPartitionChangeGenerationRecord = self
                    .decode_vector_partition_record(
                        VectorPartitionRecordKind::ChangeGeneration,
                        &bytes,
                    )?;
                Ok((key, bytes))
            })
            .transpose()?;
        let change_pages = candidate
            .change
            .as_ref()
            .map(|change| {
                let key = Self::vector_partition_change_generation_key(change);
                self.prepare_bound_vector_partition_graph_pages(
                    &change.index,
                    &change.partition_key,
                    change.base_generation_id,
                    change.change_generation_id,
                    false,
                    &key,
                    &change.graph,
                )
            })
            .transpose()?;
        let catalog_key = Self::vector_partition_catalog_key(&candidate.catalog);
        let catalog_bytes = Self::encode_vector_partition_record(
            VectorPartitionRecordKind::GenerationCatalog,
            &candidate.catalog,
        )?;
        let _: VectorPartitionGenerationCatalogRecord = self.decode_vector_partition_record(
            VectorPartitionRecordKind::GenerationCatalog,
            &catalog_bytes,
        )?;
        let attributable_stale_catalog_keys = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table =
                match read.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                    Err(error) => return Err(Self::storage_error(error)),
                };
            let mut keys = Vec::new();
            let mut key_payload_bytes = 0usize;
            let payload = vector_partition_record_payload(
                VectorPartitionRecordKind::GenerationCatalog,
                &catalog_bytes,
            )
            .map_err(|reason| self.vector_partition_corruption(reason))?;
            let route_end = vector_partition_expected_route_prefix(
                payload,
                &candidate.catalog.index,
                &candidate.catalog.partition_key,
            )
            .map_err(|reason| self.vector_partition_corruption(reason))?;
            // Only rejection uses a window. Possible attribution still admits
            // the complete page and authenticates its envelope and suffix.
            let payload_offset = VECTOR_PARTITION_RECORD_MAGIC.len() + 2 + 1 + 8;
            for entry in table
                .iter_bounded_value_prefix(payload_offset, &payload[..route_end])
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if key.value() == catalog_key.as_slice() {
                    continue;
                }
                let existing_keys = key_payload_bytes.saturating_add(
                    keys.capacity()
                        .saturating_mul(std::mem::size_of::<Vec<u8>>()),
                );
                // Authenticate and compare borrowed bytes before inspecting the
                // fixed descriptor suffix; no unrelated route is ever owned.
                // The fixed scratch covers error text from corrupt envelopes
                // or prefixes, including while stale key owners remain live.
                const INSPECTION_ERROR_SCRATCH: usize = 1024;
                admit_scan(existing_keys.saturating_add(INSPECTION_ERROR_SCRATCH))?;
                if vector_partition_catalog_has_expected_route(
                    value.value(),
                    &candidate.catalog.index,
                    &candidate.catalog.partition_key,
                ) {
                    key_payload_bytes = key_payload_bytes.saturating_add(key.value().len());
                    admit_scan(
                        key_payload_bytes
                            .saturating_add(INSPECTION_ERROR_SCRATCH)
                            .saturating_add(
                                // Include the old slot array while a growing
                                // Vec allocates its replacement; even the
                                // first push has four slots of capacity.
                                keys.capacity()
                                    .saturating_add(
                                        keys.capacity()
                                            .saturating_mul(2)
                                            .max(keys.len().saturating_add(1))
                                            .max(4),
                                    )
                                    .saturating_mul(std::mem::size_of::<Vec<u8>>()),
                            ),
                    )?;
                    keys.push(key.value().to_vec());
                }
            }
            Ok(keys)
        })?;
        Ok(PreencodedVectorPartitionGenerationCandidate {
            base_key,
            base_bytes,
            base_pages,
            change,
            change_pages,
            catalog_key,
            catalog_bytes,
            catalog_route: VectorPartitionRef::new(
                candidate.catalog.index.clone(),
                candidate.catalog.partition_key.clone(),
            ),
            attributable_stale_catalog_keys,
            native_storage: [
                candidate
                    .base
                    .graph
                    .bytes
                    .starts_with(&vector_paged::META_MAGIC)
                    .then(|| {
                        vector_paged::authenticated_native_storage_id(&candidate.base.graph.bytes)
                    })
                    .transpose()?,
                candidate
                    .change
                    .as_ref()
                    .filter(|change| change.graph.bytes.starts_with(&vector_paged::META_MAGIC))
                    .map(|change| {
                        vector_paged::authenticated_native_storage_id(&change.graph.bytes)
                    })
                    .transpose()?,
            ],
            _saved_export_ownership: paged_candidate
                .as_mut()
                .and_then(|saved| saved.ownership.take()),
        })
    }

    pub(super) fn vector_partition_key_heap_bytes(key: &VectorPartitionKey) -> usize {
        key.estimated_bytes()
            .saturating_sub(std::mem::size_of::<VectorPartitionKey>())
    }

    pub(super) fn vector_partition_route_heap_bytes(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> usize {
        index
            .table
            .capacity()
            .saturating_add(index.column.capacity())
            .saturating_add(Self::vector_partition_key_heap_bytes(partition_key))
    }

    pub(super) fn vector_partition_catalog_heap_bytes(
        catalog: &VectorPartitionGenerationCatalogRecord,
    ) -> usize {
        Self::vector_partition_route_heap_bytes(&catalog.index, &catalog.partition_key)
    }

    /// Upper-bound heap retained by one decoded vector persistence record.
    /// Route inspection runs before owned decoding and proves the complete
    /// stored component count exists, removing serde's hostile length-hint
    /// reservation. Its initial Vec capacity is at most the actual count;
    /// larger valid sequences can grow geometrically to less than twice that
    /// count. Each component consumes at least two encoded bytes (tag+value),
    /// so one slot per encoded byte covers that growth; the extra byte factor
    /// also covers string payloads. Exact capacities replace this bound after
    /// decoding. Journal inspection covers both independently stored routes.
    pub(super) fn vector_partition_record_decode_bound(
        encoded_bytes: usize,
        inline_bytes: usize,
    ) -> usize {
        encoded_bytes
            .saturating_mul(std::mem::size_of::<VectorPartitionComponent>().saturating_add(1))
            .saturating_add(inline_bytes)
    }

    pub(super) fn vector_partition_physical_key_bound(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        suffix_bytes: usize,
    ) -> usize {
        index
            .table
            .len()
            .saturating_add(index.column.len())
            .saturating_add(partition_key.estimated_bytes())
            .saturating_add(24)
            .saturating_add(suffix_bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn resize_vector_cleanup_workspace(
        workspace: &mut VectorWorkspaceReservation,
        bytes: usize,
        operation: &str,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        crate::vector_observations::observe_workspace_resize_for_test(
            workspace.retained_bytes(),
            bytes,
            if operation == "finalize_repaired_vector_partition" {
                "repair_cleanup"
            } else {
                "normal_cleanup"
            },
        );
        workspace.resize(
            bytes,
            operation,
            "Raise MEMORY_LIMIT with SET MEMORY_LIMIT <SIZE> (or reopen with --memory-limit <SIZE>) before cleaning this maintained route.",
        )
    }

    /// Redb transaction T2: verify the current catalog and every route-owned
    /// journal key/value before deleting exactly the covered prefix. A later
    /// suffix and every other route are outside both the scan and deletion
    /// set. Redb commit makes the deletion all-or-nothing; repeating it after
    /// a crash is a no-op after the same verification.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn truncate_vector_partition_journal_through_catalog(
        &self,
        expected_catalog: &VectorPartitionGenerationCatalogRecord,
        workspace: &mut VectorWorkspaceReservation,
    ) -> Result<bool> {
        self.truncate_vector_partition_journal_through_catalog_with_checkpoint(
            expected_catalog,
            workspace,
            || Ok(()),
        )
    }

    /// Maintenance supplies its stop check to the same transactional cleanup
    /// kernel. The covered prefix is removed in bounded steps of
    /// `VECTOR_JOURNAL_CLEANUP_STEP_RECORDS` records, each its own committed
    /// transaction, repeated until the prefix is gone. The stop check runs
    /// before every record read; a cancelled step publishes none of its own
    /// removals, and the steps already committed stand: covered records are
    /// below the boundary every loader replays from.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(crate) fn truncate_vector_partition_journal_through_catalog_with_checkpoint(
        &self,
        expected_catalog: &VectorPartitionGenerationCatalogRecord,
        workspace: &mut VectorWorkspaceReservation,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<bool> {
        loop {
            let mut removed = 0usize;
            let complete = self.truncate_vector_partition_journal_in_steps(
                expected_catalog,
                workspace,
                &mut checkpoint,
                VECTOR_JOURNAL_CLEANUP_STEP_RECORDS,
                &mut removed,
            )?;
            // A step that removed nothing made no progress to repeat: the
            // prefix is gone, the catalog changed, or another branch owns it.
            if complete || removed == 0 {
                return Ok(complete);
            }
            #[cfg(any(test, feature = "test-seams"))]
            crate::database::maybe_pause_between_vector_journal_cleanup_steps_for_test();
            checkpoint()?;
        }
    }

    /// One bounded step of T2 in its own committed transaction: returns
    /// whether the covered prefix is gone and how many records it removed.
    /// A caller that holds the commit lock takes it for one step at a time.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn truncate_vector_partition_journal_step_with_checkpoint(
        &self,
        expected_catalog: &VectorPartitionGenerationCatalogRecord,
        workspace: &mut VectorWorkspaceReservation,
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<(bool, usize)> {
        let mut removed = 0usize;
        let complete = self.truncate_vector_partition_journal_in_steps(
            expected_catalog,
            workspace,
            checkpoint,
            VECTOR_JOURNAL_CLEANUP_STEP_RECORDS,
            &mut removed,
        )?;
        Ok((complete, removed))
    }

    /// The reopen retry of T2: one bounded step. It reads at most
    /// `VECTOR_JOURNAL_CLEANUP_STEP_RECORDS` journal records, removes the
    /// covered ones among them in one committed transaction and leaves the
    /// rest to maintenance, so opening a store does the same work however long
    /// the route's covered backlog or unindexed suffix is. Returns whether the
    /// covered prefix is now gone.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn truncate_vector_partition_journal_open_step(
        &self,
        expected_catalog: &VectorPartitionGenerationCatalogRecord,
        workspace: &mut VectorWorkspaceReservation,
    ) -> Result<bool> {
        self.truncate_vector_partition_journal_in_steps(
            expected_catalog,
            workspace,
            || Ok(()),
            VECTOR_JOURNAL_CLEANUP_STEP_RECORDS,
            &mut 0,
        )
    }

    /// One bounded step of T2: reads at most `record_limit` journal records
    /// and counts the covered ones it removes in `removed`.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn truncate_vector_partition_journal_in_steps(
        &self,
        expected_catalog: &VectorPartitionGenerationCatalogRecord,
        workspace: &mut VectorWorkspaceReservation,
        mut checkpoint: impl FnMut() -> Result<()>,
        record_limit: usize,
        removed: &mut usize,
    ) -> Result<bool> {
        const OPERATION: &str = "truncate_vector_partition_journal";
        checkpoint()?;
        let catalog_owned = Self::vector_partition_catalog_heap_bytes(expected_catalog);
        let physical_key_bound = Self::vector_partition_physical_key_bound(
            &expected_catalog.index,
            &expected_catalog.partition_key,
            16,
        );
        // Two durable route keys plus the canonical-key buffer used while each
        // is constructed. This replaces the generation-build reservation
        // before any cleanup allocation is made.
        Self::resize_vector_cleanup_workspace(
            workspace,
            catalog_owned.saturating_add(physical_key_bound.saturating_mul(3)),
            OPERATION,
        )?;
        let catalog_key = Self::vector_partition_catalog_key(expected_catalog);
        let prefix =
            Self::vector_partition_prefix(&expected_catalog.index, &expected_catalog.partition_key);
        let fixed_owned = catalog_owned
            .saturating_add(catalog_key.capacity())
            .saturating_add(prefix.capacity());
        debug_assert!(
            fixed_owned <= catalog_owned.saturating_add(physical_key_bound.saturating_mul(3))
        );
        Self::resize_vector_cleanup_workspace(workspace, fixed_owned, OPERATION)?;
        let covered_lsn = expected_catalog
            .change
            .map(|change| change.covered_lsn)
            .unwrap_or(expected_catalog.base.covered_lsn);
        let result = self.with_db(|db| {
            let write_txn = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write_txn);
            {
                let catalogs = write_txn
                    .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .map_err(Self::storage_error)?;
                let Some(value) = catalogs
                    .get_bounded(catalog_key.as_slice())
                    .map_err(Self::storage_error)?
                else {
                    return Ok(false);
                };
                let decode_bound = Self::vector_partition_record_decode_bound(
                    value.value().len(),
                    std::mem::size_of::<VectorPartitionGenerationCatalogRecord>(),
                );
                Self::resize_vector_cleanup_workspace(
                    workspace,
                    fixed_owned.saturating_add(decode_bound),
                    OPERATION,
                )?;
                let actual: VectorPartitionGenerationCatalogRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::GenerationCatalog,
                        value.value(),
                        &expected_catalog.index,
                        &expected_catalog.partition_key,
                    )?;
                let actual_owned = Self::vector_partition_catalog_heap_bytes(&actual);
                debug_assert!(actual_owned <= decode_bound);
                Self::resize_vector_cleanup_workspace(
                    workspace,
                    fixed_owned.saturating_add(actual_owned),
                    OPERATION,
                )?;
                if actual != *expected_catalog {
                    return Ok(false);
                }
                drop(actual);
                Self::resize_vector_cleanup_workspace(workspace, fixed_owned, OPERATION)?;
            }

            if expected_catalog
                .change
                .unwrap_or(expected_catalog.base)
                .generation_id
                & vector_paged::MUTABLE_STORAGE
                != 0
            {
                Self::resize_vector_cleanup_workspace(
                    workspace,
                    fixed_owned.saturating_add(64 * 1024),
                    OPERATION,
                )?;
                if let Some(complete) = self.truncate_native_tail_journal_in_write(
                    &write_txn,
                    expected_catalog,
                    &mut checkpoint,
                )? {
                    Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
                    checkpoint()?;
                    write_txn.commit().map_err(Self::storage_error)?;
                    return Ok(complete);
                }
            }

            if Self::repair_fence_in_write(&write_txn)? {
                Self::resize_vector_cleanup_workspace(
                    workspace,
                    fixed_owned.saturating_add(physical_key_bound.saturating_mul(2)),
                    OPERATION,
                )?;
                let mut journal = write_txn
                    .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                    .map_err(Self::storage_error)?;
                let mut complete = false;
                for _ in 0..64 {
                    checkpoint()?;
                    let key = {
                        let next = journal
                            .prefix_bounded(prefix.as_slice())
                            .map_err(Self::storage_error)?
                            .next()
                            .transpose()
                            .map_err(Self::storage_error)?;
                        let Some((key, value)) = next else {
                            complete = true;
                            break;
                        };
                        if !key.value().starts_with(&prefix) {
                            complete = true;
                            break;
                        }
                        let tail = &key.value()[prefix.len()..];
                        if tail.len() != 16 {
                            return Err(self.vector_partition_corruption(
                                "covered journal physical key is malformed",
                            ));
                        }
                        let lsn = u64::from_be_bytes(tail[..8].try_into().unwrap());
                        if lsn > covered_lsn.0 {
                            complete = true;
                            break;
                        }
                        let key = key.value().to_vec();
                        drop(value);
                        key
                    };
                    journal
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                }
                drop(journal);
                Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
                checkpoint()?;
                {
                    #[cfg(any(test, feature = "test-seams"))]
                    graph_observation_checkpoint(10)?;
                    let graph_commit_result = write_txn.commit();
                    #[cfg(any(test, feature = "test-seams"))]
                    if graph_commit_result.is_ok() {
                        graph_publication_attempt.committed();
                        graph_observation_checkpoint(11)?;
                    }
                    graph_commit_result
                }
                .map_err(Self::storage_error)?;
                return Ok(complete);
            }

            // Opening a table in a Redb write transaction creates an empty
            // table when none exists. Keeping that handle in this scope also
            // makes the transaction lifetime unambiguous through commit.
            let mut journal = write_txn
                .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                .map_err(Self::storage_error)?;
            // Journal keys are `prefix | lsn | ordinal`, so the covered records
            // come first. Each pass re-seeks the route's first record, validates
            // it without decoding it, stops at the first one past the covered
            // boundary, and otherwise removes it. Only one copied physical key
            // is live at a time, reserved before it is copied.
            Self::resize_vector_cleanup_workspace(
                workspace,
                fixed_owned.saturating_add(physical_key_bound.saturating_mul(2)),
                OPERATION,
            )?;
            let width_bound = vector_partition_journal_record_width_bound(
                &expected_catalog.index,
                &expected_catalog.partition_key,
            );
            let mut complete = false;
            let mut read = 0usize;
            while read < record_limit {
                checkpoint()?;
                let key = {
                    let next = journal
                        .prefix_bounded(prefix.as_slice())
                        .map_err(Self::storage_error)?
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?;
                    let Some((key, value)) = next else {
                        complete = true;
                        break;
                    };
                    if !key.value().starts_with(&prefix) {
                        complete = true;
                        break;
                    }
                    read += 1;
                    #[cfg(any(test, feature = "test-seams"))]
                    note_vector_journal_cleanup_record_read();
                    let lsn = validate_vector_partition_journal_record(
                        &prefix,
                        key.value(),
                        value.value(),
                        &expected_catalog.index,
                        &expected_catalog.partition_key,
                        width_bound,
                    )
                    .map_err(|reason| self.vector_partition_corruption(reason))?;
                    if lsn > covered_lsn.0 {
                        complete = true;
                        break;
                    }
                    key.value().to_vec()
                };
                // Covered records lead the route, so the first record read is
                // the first one removed.
                #[cfg(any(test, feature = "test-seams"))]
                if read == 1 {
                    crate::database::maybe_pause_vector_journal_cleanup_for_test();
                }
                journal
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
                *removed += 1;
            }
            Self::resize_vector_cleanup_workspace(workspace, fixed_owned, OPERATION)?;
            drop(journal);
            checkpoint()?;
            Self::write_writer_recovery_bounds_in_write(&write_txn, None)?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write_txn.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            Ok(complete)
        });
        if result.is_ok() {
            drop(prefix);
            drop(catalog_key);
            Self::resize_vector_cleanup_workspace(workspace, catalog_owned, OPERATION)?;
        }
        result
    }

    /// Complete a quarantined route's durable repair after its replacement
    /// catalog is already committed. Physical keys select covered evidence;
    /// bounded authenticated ownership metadata may name native cleanup, and
    /// an unusable ownership reference raises the durable erasure fence.
    /// Writes newer than the replacement's sampled frontier are preserved.
    /// The healthy replacement is already published. Retire only one finite
    /// batch of covered evidence; normal catalog/journal reclamation resumes
    /// the remaining batches, without rebuilding the healthy replacement.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn finalize_repaired_vector_partition(
        &self,
        expected_catalog: &VectorPartitionGenerationCatalogRecord,
        retained_chains: &[(u64, Option<u64>)],
        repair_context_owned: usize,
        workspace: &mut VectorWorkspaceReservation,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<(bool, usize)> {
        const OPERATION: &str = "finalize_repaired_vector_partition";
        let catalog_owned = Self::vector_partition_catalog_heap_bytes(expected_catalog);
        let key_bound = Self::vector_partition_physical_key_bound(
            &expected_catalog.index,
            &expected_catalog.partition_key,
            16,
        );
        let fixed = catalog_owned
            .saturating_add(repair_context_owned)
            .saturating_add(key_bound.saturating_mul(8))
            .saturating_add(64 * 1024);
        Self::resize_vector_cleanup_workspace(workspace, fixed, OPERATION)?;
        let catalog_key = Self::vector_partition_catalog_key(expected_catalog);
        let prefix =
            Self::vector_partition_prefix(&expected_catalog.index, &expected_catalog.partition_key);
        let covered_lsn = expected_catalog
            .change
            .map(|c| c.covered_lsn)
            .unwrap_or(expected_catalog.base.covered_lsn);
        let finalized = self.with_db(|db| {
            checkpoint()?;
            let write = Self::begin_history_write(db)?;
            #[cfg(any(test, feature = "test-seams"))]
            let mut graph_publication_attempt =
                crate::read_work_probe::GraphPublicationAttempt::begin_in_write(&write);
            {
                let catalogs = write
                    .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .map_err(Self::storage_error)?;
                let Some(value) = catalogs
                    .get_bounded(catalog_key.as_slice())
                    .map_err(Self::storage_error)?
                else {
                    return Ok(false);
                };
                let decode = Self::vector_partition_record_decode_bound(
                    value.value().len(),
                    std::mem::size_of::<VectorPartitionGenerationCatalogRecord>(),
                );
                Self::resize_vector_cleanup_workspace(
                    workspace,
                    fixed.saturating_add(decode),
                    OPERATION,
                )?;
                let actual: VectorPartitionGenerationCatalogRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::GenerationCatalog,
                        value.value(),
                        &expected_catalog.index,
                        &expected_catalog.partition_key,
                    )?;
                if actual != *expected_catalog {
                    return Ok(false);
                }
            }
            Self::resize_vector_cleanup_workspace(workspace, fixed, OPERATION)?;
            {
                let mut journal = write
                    .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                    .map_err(Self::storage_error)?;
                for _ in 0..64 {
                    checkpoint()?;
                    let key = {
                        let next = journal
                            .prefix_bounded(prefix.as_slice())
                            .map_err(Self::storage_error)?
                            .next()
                            .transpose()
                            .map_err(Self::storage_error)?;
                        let Some((key, value)) = next else {
                            break;
                        };
                        if !key.value().starts_with(&prefix) {
                            break;
                        }
                        let suffix = &key.value()[prefix.len()..];
                        if suffix.len() != 16 {
                            return Err(vector_paged::corruption(
                                "covered journal key is malformed",
                            ));
                        }
                        let lsn = u64::from_be_bytes(suffix[..8].try_into().unwrap());
                        if lsn > covered_lsn.0 {
                            break;
                        }
                        let key = key.value().to_vec();
                        drop(value);
                        key
                    };
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::database::maybe_pause_vector_journal_cleanup_for_test();
                    checkpoint()?;
                    journal
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            self.reclaim_vector_generations_in_write_with_checkpoint(
                &write,
                expected_catalog,
                retained_chains,
                true,
                &mut checkpoint,
            )?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            checkpoint()?;
            {
                #[cfg(any(test, feature = "test-seams"))]
                graph_observation_checkpoint(10)?;
                let graph_commit_result = write.commit();
                #[cfg(any(test, feature = "test-seams"))]
                if graph_commit_result.is_ok() {
                    graph_publication_attempt.committed();
                    graph_observation_checkpoint(11)?;
                }
                graph_commit_result
            }
            .map_err(Self::storage_error)?;
            // True means the expected repaired catalog remains authoritative,
            // not that all historical cleanup batches have already drained.
            Ok(true)
        })?;
        drop(prefix);
        drop(catalog_key);
        Self::resize_vector_cleanup_workspace(
            workspace,
            catalog_owned.saturating_add(repair_context_owned),
            OPERATION,
        )?;
        Ok((finalized, catalog_owned))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_preencoded_vector_partition_generation_objects_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        prepared: &PreencodedVectorPartitionGenerationCandidate,
    ) -> Result<()> {
        let manifest: VectorPartitionGraphPageManifestRecord = self
            .decode_vector_partition_record(
                VectorPartitionRecordKind::GraphPageManifest,
                &prepared.base_pages.manifest_bytes,
            )?;
        Self::validate_vector_schema_binding_in_write(
            write_txn,
            &prepared.catalog_route.index,
            manifest.schema_binding,
        )?;
        if let Some(change_pages) = &prepared.change_pages {
            let change: VectorPartitionGraphPageManifestRecord = self
                .decode_vector_partition_record(
                    VectorPartitionRecordKind::GraphPageManifest,
                    &change_pages.manifest_bytes,
                )?;
            if change.schema_binding != manifest.schema_binding {
                return Err(self
                    .vector_partition_corruption("base and change column incarnations disagree"));
            }
        }
        // All callers insert immutable objects before publishing the catalog.
        // An emptied partitioned route must insert neither generations nor pages:
        // without a catalog, reclamation cannot discover those objects.
        if vector_membership_pages::is_empty_partitioned_route_in_write(
            write_txn,
            &prepared.catalog_route,
        )? {
            return Ok(());
        }
        let mut bases = write_txn
            .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
            .map_err(Self::storage_error)?;
        Self::insert_preencoded_immutable_vector_partition_record(
            &mut bases,
            &prepared.base_key,
            &prepared.base_bytes,
            self,
        )?;
        drop(bases);
        self.insert_preencoded_vector_partition_graph_pages_in_write(
            write_txn,
            &prepared.base_pages,
        )?;
        if let Some((change_key, change_bytes)) = &prepared.change {
            let mut changes = write_txn
                .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(Self::storage_error)?;
            Self::insert_preencoded_immutable_vector_partition_record(
                &mut changes,
                change_key,
                change_bytes,
                self,
            )?;
        }
        if let Some(change_pages) = &prepared.change_pages {
            self.insert_preencoded_vector_partition_graph_pages_in_write(write_txn, change_pages)?;
        }
        vector_paged::bind_pages_root(write_txn)?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn insert_preencoded_vector_partition_graph_pages_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        prepared: &PreencodedVectorPartitionGraphPages,
    ) -> Result<()> {
        {
            let mut pages = write_txn
                .open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE)
                .map_err(Self::storage_error)?;
            // One page key and one framed body coexist, regardless of the
            // number of pages or the length of the unchanged physical root.
            for (ordinal, body) in prepared.graph_bytes.chunks(prepared.page_bytes).enumerate() {
                let ordinal = ordinal as u32; // range checked during preparation
                let key = Self::vector_partition_graph_page_key(&prepared.root_key, ordinal);
                let mut bytes =
                    Vec::with_capacity(body.len() + VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES);
                bytes.extend_from_slice(&ordinal.to_le_bytes());
                bytes.extend_from_slice(blake3::hash(body).as_bytes());
                bytes.extend_from_slice(body);
                Self::insert_preencoded_immutable_vector_partition_record(
                    &mut pages, &key, &bytes, self,
                )?;
            }
        }
        let mut manifests = write_txn
            .open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE)
            .map_err(Self::storage_error)?;
        Self::insert_preencoded_immutable_vector_partition_record(
            &mut manifests,
            &prepared.root_key,
            &prepared.manifest_bytes,
            self,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn publish_preencoded_vector_partition_generation_catalog_in_write(
        &self,
        write_txn: &redb::WriteTransaction,
        prepared: &PreencodedVectorPartitionGenerationCandidate,
    ) -> Result<Option<(usize, usize)>> {
        if vector_membership_pages::is_empty_partitioned_route_in_write(
            write_txn,
            &prepared.catalog_route,
        )? {
            // A pinned retired chain may still need the current catalog.
            // Only reclaim removes it; a late candidate cannot replace it.
            return Ok(None);
        }
        let mut catalogs = write_txn
            .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
            .map_err(Self::storage_error)?;
        for key in &prepared.attributable_stale_catalog_keys {
            let still_attributable = match catalogs
                .get_bounded(key.as_slice())
                .map_err(Self::storage_error)?
            {
                None => false,
                Some(value) => vector_partition_catalog_has_expected_route(
                    value.value(),
                    &prepared.catalog_route.index,
                    &prepared.catalog_route.partition_key,
                ),
            };
            if still_attributable {
                catalogs
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
            }
        }
        catalogs
            .insert(
                prepared.catalog_key.as_slice(),
                prepared.catalog_bytes.as_slice(),
            )
            .map_err(Self::storage_error)?;
        let catalog: VectorPartitionGenerationCatalogRecord = self.decode_vector_partition_record(
            VectorPartitionRecordKind::GenerationCatalog,
            &prepared.catalog_bytes,
        )?;
        for storage in prepared.native_storage.into_iter().flatten() {
            vector_saved::release_saved_export_in_write(write_txn, storage)?;
        }
        vector_membership_pages::publish_pending_frontier(
            write_txn,
            &prepared.catalog_route,
            catalog.change.unwrap_or(catalog.base).covered_tx,
            catalog.change.map(|_| catalog.base.covered_tx),
        )
    }

    /// Resolve every durable row-version pairing for the selected vector
    /// occurrences before authoritative purge begins its write transaction.
    /// One vector can legitimately have more than one membership after a key
    /// move, so every exact match is returned and later erased.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn resolve_vector_partition_version_identities(
        &self,
        selectors: &[VectorPartitionVersionSelector],
    ) -> Result<Vec<VectorPartitionVersionIdentity>> {
        if selectors.is_empty() {
            return Ok(Vec::new());
        }
        let mut unique_selectors = Vec::new();
        for selector in selectors {
            if !unique_selectors.contains(selector) {
                unique_selectors.push(selector.clone());
            }
        }
        let mut partitions = Vec::new();
        for selector in &unique_selectors {
            let partition = (selector.index.clone(), selector.partition_key.clone());
            if !partitions.contains(&partition) {
                partitions.push(partition);
            }
        }
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let mut resolved = Vec::new();
            for (index, partition_key) in partitions {
                let prefix = Self::vector_partition_prefix(&index, &partition_key);
                let memberships = self.load_vector_partition_table_in_read(
                    &read_txn,
                    VECTOR_PARTITION_MEMBERSHIP_TABLE,
                    VectorPartitionRecordKind::Membership,
                    Self::vector_partition_membership_key,
                    Some(&prefix),
                )?;
                for selector in unique_selectors.iter().filter(|selector| {
                    selector.index == index && selector.partition_key == partition_key
                }) {
                    let mut matched = false;
                    for membership in memberships.iter().filter(|membership| {
                        membership.row_id == selector.row_id
                            && membership.vector_created_tx == selector.vector_created_tx
                            && membership.vector_lsn == selector.vector_lsn
                    }) {
                        matched = true;
                        let identity = VectorPartitionVersionIdentity {
                            index: membership.index.clone(),
                            partition_key: membership.partition_key.clone(),
                            row_id: membership.row_id,
                            row_created_tx: membership.row_created_tx,
                            row_lsn: membership.row_lsn,
                            vector_created_tx: membership.vector_created_tx,
                            vector_lsn: membership.vector_lsn,
                        };
                        if !resolved.contains(&identity) {
                            resolved.push(identity);
                        }
                    }
                    if !matched {
                        return Err(self.vector_partition_corruption(format!(
                            "authoritative purge selected vector {}.{} row {} tx {} lsn {} without a durable membership in its selected partition",
                            selector.index.table,
                            selector.index.column,
                            selector.row_id.0,
                            selector.vector_created_tx.0,
                            selector.vector_lsn.0,
                        )));
                    }
                }
            }
            resolved.sort_by_key(Self::vector_partition_version_identity_key);
            Ok(resolved)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_sync_source_lsns(&self) -> Result<HashMap<(String, RowId), Lsn>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(SYNC_ROW_SOURCE_LSN_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut source_lsns = HashMap::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = Self::decode_sync_row_source_lsn_key(key.value())
                    .ok_or_else(|| Error::Other("sync provenance has an invalid row key".into()))?;
                source_lsns.insert(key, Lsn(value.value()));
            }
            Ok(source_lsns)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_sync_source_kinds(&self) -> Result<HashMap<(String, RowId), u8>> {
        let _decode_scope = self.reader_decode_scope()?;
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(SYNC_ROW_SOURCE_KIND_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut kinds = HashMap::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = Self::decode_sync_row_source_lsn_key(key.value()).ok_or_else(|| {
                    Error::Other("sync provenance kind has an invalid row key".into())
                })?;
                kinds.insert(key, value.value());
            }
            Ok(kinds)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn load_change_log(&self) -> Result<Vec<ChangeLogEntry>> {
        let read_memory =
            crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(16_777_216)?
                .child()?;
        let owner = writer_source::SourceWindowOwner::Read(read_memory.clone());
        // Accountant and image cells now carry their own exact invoices;
        // this existing scope covers only the table/range storage it names.
        let _controls = owner.reserve(
            std::mem::size_of::<redb::ReadOnlyTable<&str, &[u8]>>()
                .saturating_add(std::mem::size_of::<redb::Range<'_, &str, &[u8]>>()),
            "history_load_control",
        )?;
        // This accountant identifies the decode bridge only. All read funding
        // comes from the operation child, including proof pool and output.
        let bridge =
            crate::memory_accounting::AccountantHandle::reader(None, read_memory.clone(), false)?;
        let output = crate::read_image_memory::ImageMemory::admitted_reader_for(
            bridge.clone(),
            read_memory.clone(),
            "history_source",
            "load_change_log",
            "Raise the reader limit to load requested history.",
        )?;
        graph_read::with_pages(read_memory.clone(), || {
            self.with_db(|db| {
                let read_txn = db.begin_read().map_err(Self::storage_error)?;
                let table = match read_txn.open_table(CHANGE_LOG_TABLE) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                    Err(err) => return Err(Self::storage_error(err)),
                };
                let mut entries = Vec::new();
                for entry in table
                    .iter()
                    .map_err(Self::storage_error)?
                    .inspect(|_entry| {
                        #[cfg(any(test, feature = "test-seams"))]
                        if let Ok((key, _)) = _entry {
                            crate::read_work_probe::graph_storage_key(
                                "history",
                                "iteration_step",
                                key.value(),
                            );
                        }
                    })
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    let _window_control = owner.reserve(
                        std::mem::size_of::<crate::read_image_memory::ImageMemory>()
                            .saturating_add(32),
                        "history_proof_window",
                    )?;
                    let scratch = crate::read_image_memory::ImageMemory::charging_reader_for(
                        bridge.clone(),
                        read_memory.clone(),
                        "graph_source",
                        "read_window",
                        "Raise the reader limit to read graph history windows.",
                    );
                    graph_read::with_graph_proof_in_read(
                        &read_txn,
                        None,
                        &owner,
                        scratch,
                        |context, read, memory| {
                            context.verify_stored_graph_history(
                                read,
                                key.value(),
                                value.value(),
                                memory,
                            )
                        },
                    )?;
                    #[cfg(feature = "in-process-test-seams")]
                    crate::writer_query_test_seams::note_history(
                        crate::writer_query_test_seams::HistoryFamily::ChangeLog,
                    );
                    if entries.len() == entries.capacity() {
                        let additional = entries.capacity().max(4);
                        let bytes = additional
                            .checked_mul(std::mem::size_of::<ChangeLogEntry>())
                            .ok_or_else(|| Error::Other("history output size overflow".into()))?;
                        output.reserve(bytes)?;
                        entries.try_reserve_exact(additional).map_err(|error| {
                            Error::Other(
                                (format!("history output allocation failed: {error}")).into(),
                            )
                        })?;
                    }
                    let mut record =
                        output.scope(|| Self::decode_stored_change_log(value.value()))?;
                    // Only public entries are returned. The manual decoder charges
                    // the exact private predecessor copy separately; return that
                    // transient credit as soon as this source window is consumed.
                    if let Some(witness) = record.witness.take() {
                        let bytes = witness.predecessor.len();
                        drop(witness);
                        output.release(bytes);
                    }
                    entries.push(record.entry);
                }
                Ok(entries)
            })
        })
    }
}
