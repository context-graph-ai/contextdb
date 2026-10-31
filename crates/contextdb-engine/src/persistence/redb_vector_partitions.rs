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
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
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
    #[allow(
        dead_code,
        reason = "KEEP-SEAM: test helper behind a test cfg; unit tests or the test-seams build use it"
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
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    pub(crate) fn vector_journal_cleanup_key_reservation(
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
    ) -> usize {
        Self::vector_partition_physical_key_bound(index, partition_key, 16).saturating_mul(3)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn corrupt_vector_partition_journal_record_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        Self::corrupt_vector_partition_journal_record_at_for_test(path, partition, false)
    }

    /// Damage the route's LAST journal record, the newest uncovered one.
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn corrupt_last_vector_partition_journal_record_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        Self::corrupt_vector_partition_journal_record_at_for_test(path, partition, true)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
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
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(crate) fn corrupt_vector_partition_base_record_for_test(
        path: &Path,
        partition: &contextdb_vector::VectorPartitionRef,
    ) -> Result<()> {
        Self::flip_vector_partition_base_record_byte_for_test(path, partition, |len| len - 1)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
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

    // Preserve the existing framing-only fixture entry. An unbound frame can
    // never pass the transactional publication guard.

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
