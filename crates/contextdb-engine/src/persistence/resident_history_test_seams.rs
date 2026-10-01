//! Production-dead fixture controls; every read uses the writer's real redb handle.
use super::*;
use redb::{ReadableDatabase, ReadableTable};
impl RedbPersistence {
    pub(crate) fn allocator_metadata_bytes_for_test(&self) -> Result<usize> {
        self.with_db(|db| Ok(db.allocator_metadata_bytes_for_test()))
    }

    /// Count physical durable keys without decoding bodies or building a mirror.
    pub(crate) fn durable_version_count_for_test(&self, name: &str) -> Result<usize> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let name = Self::rel_table_name(name);
            let definition = TableDefinition::<&[u8], &[u8]>::new(&name);
            let table = read.open_table(definition).map_err(Self::storage_error)?;
            Ok(table.len().map_err(Self::storage_error)? as usize)
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
            let key = regular
                .iter()
                .map_err(Self::storage_error)?
                .nth(ordinal)
                .ok_or_else(|| Error::Other("fixture body ordinal absent".into()))?
                .map_err(Self::storage_error)?
                .0
                .value()
                .to_vec();
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
