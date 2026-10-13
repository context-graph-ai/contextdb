//! Durable custody-journal reads: point gets, prefix ranges and derived group
//! membership at the latest committed root. Nothing here stays resident.
use super::*;

impl RedbPersistence {
    /// Measurement only: how many committed CONFIG keys start with `prefix`.
    /// Visits keys; never decodes or copies a value.
    #[cfg(feature = "test-seams")]
    pub(crate) fn count_config_keys_with_prefix_for_test(&self, prefix: &str) -> Result<u64> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut count = 0u64;
            for entry in table.range(prefix..).map_err(Self::storage_error)? {
                let (key, _) = entry.map_err(Self::storage_error)?;
                if !key.value().starts_with(prefix) {
                    break;
                }
                count += 1;
            }
            Ok(count)
        })
    }
}
