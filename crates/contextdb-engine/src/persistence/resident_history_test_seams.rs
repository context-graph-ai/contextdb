//! Production-dead fixture controls; every read uses the writer's real redb handle.
use super::*;
use redb::{ReadableDatabase, ReadableTable};
impl RedbPersistence {
    pub(crate) fn durable_page_population_for_test(&self) -> Result<(usize, u64)> {
        self.with_db(|db| {
            let snapshot = db
                .storage_statistics_snapshot()
                .map_err(Self::storage_error)?;
            Ok((
                db.allocator_regions_for_test(),
                snapshot
                    .stats()
                    .map_err(Self::storage_error)?
                    .allocated_pages(),
            ))
        })
    }

    pub(crate) fn create_region_fixture_for_test(path: &Path, region_bytes: u64) -> Result<()> {
        let mut builder = redb::Database::builder();
        builder.set_region_size(region_bytes);
        let db = builder.create(path).map_err(Self::storage_error)?;
        Self::write_format_marker(&db, &mut content_identity::ProofAdmission::unaccounted())
    }
    pub(crate) fn durable_ddl_for_test(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = read
                .open_table(DDL_LOG_TABLE)
                .map_err(Self::storage_error)?;
            table
                .iter()
                .map_err(Self::storage_error)?
                .map(|entry| {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    Ok((key.value().to_owned(), value.value().to_vec()))
                })
                .collect()
        })
    }

    pub(crate) fn native_read_transaction_count_for_test(
        &self,
        transaction_id: u64,
    ) -> Result<u64> {
        self.with_db(|db| Ok(db.native_read_transaction_count_for_test(transaction_id)))
    }
    pub(crate) fn durable_sidecars_for_test(&self) -> Result<(usize, Vec<(String, Vec<u8>)>)> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let table = read.open_table(CONFIG_TABLE).map_err(Self::storage_error)?;
            let mut records = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if crate::writer_query_test_seams::sidecar_family(key.value()).is_some() {
                    records.push((key.value().to_owned(), value.value().to_vec()));
                }
            }
            Ok((db.allocator_regions_for_test(), records))
        })
    }

    pub(crate) fn physical_snapshot_for_test(&self) -> Result<redb::StorageSnapshotIdentity> {
        self.with_db(|db| {
            Ok(db
                .begin_read()
                .map_err(Self::storage_error)?
                .storage_snapshot_identity())
        })
    }

    pub(crate) fn allocator_metadata_bytes_for_test(&self) -> Result<usize> {
        self.with_db(|db| Ok(db.allocator_metadata_bytes_for_test()))
    }

    /// Actual durable relation height, observed only during fixture setup.
    pub(crate) fn durable_relation_tree_height_for_test(&self, name: &str) -> Result<u32> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let name = Self::rel_table_name(name);
            let definition = TableDefinition::<&[u8], &[u8]>::new(&name);
            let table = read.open_table(definition).map_err(Self::storage_error)?;
            Ok(table.stats().map_err(Self::storage_error)?.tree_height())
        })
    }

    pub(crate) fn resident_storage_cursor_for_test(&self) -> String {
        format!("{:?}", self.storage_compaction.lock().unwrap().0)
    }
    pub(crate) fn resident_cache_stats_for_test(&self) -> Result<(u64, u64, u64, usize)> {
        self.with_db(|db| {
            let stats = db.cache_stats();
            Ok((
                stats.read_hits(),
                stats.read_misses(),
                stats.evictions(),
                stats.used_bytes(),
            ))
        })
    }
    pub(crate) fn hold_row_page_for_test(
        &self,
        table: &str,
        ordinal: usize,
    ) -> Result<redb::AccessGuard<'static, &'static [u8]>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let name = Self::rel_table_name(table);
            let definition = TableDefinition::<&[u8], &[u8]>::new(&name);
            let regular = read.open_table(definition).map_err(Self::storage_error)?;
            let mut entries = regular.iter().map_err(Self::storage_error)?;
            let mut position = 0;
            let key = loop {
                let (key, _) = entries
                    .next()
                    .ok_or_else(|| Error::Other("fixture body ordinal absent".into()))?
                    .map_err(Self::storage_error)?;
                if position == ordinal {
                    break key.value().to_vec();
                }
                position += 1;
            };
            drop(entries);
            let bounded = read
                .open_byte_table_bounded(definition)
                .map_err(Self::storage_error)?;
            bounded
                .get_bounded(&key)
                .map_err(Self::storage_error)?
                .ok_or_else(|| Error::Other("fixture body disappeared".into()))
        })
    }
    pub(crate) fn damage_membership_envelope_for_test(
        &self,
        partition: &VectorPartitionRef,
    ) -> Result<()> {
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                    .map_err(Self::storage_error)?;
                let prefix =
                    Self::vector_partition_prefix(&partition.index, &partition.partition_key);
                let (key, value) = table
                    .range(prefix.as_slice()..)
                    .map_err(Self::storage_error)?
                    .next()
                    .ok_or_else(|| Error::Other("fixture membership absent".into()))?
                    .map_err(Self::storage_error)?;
                if !key.value().starts_with(&prefix) {
                    return Err(Error::Other("fixture route absent".into()));
                }
                let owned_key = key.value().to_vec();
                let mut bytes = value.value().to_vec();
                // Preserve magic, version, kind, length and payload shape. Only the
                // checksum differs; bypassing checksum verification makes reload succeed.
                let checksum = bytes.len() - 1;
                bytes[checksum] ^= 1;
                drop(value);
                drop(key);
                let key = owned_key;
                table
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write.commit().map_err(Self::storage_error)
        })
    }
}
