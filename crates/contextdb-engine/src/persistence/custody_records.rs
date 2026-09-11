//! Verify only the selected persisted custody keys on read.
use super::*;
impl RedbPersistence {
    pub(crate) fn load_custody_records_raw(
        &self,
        keys: &[String],
    ) -> Result<BTreeMap<String, Vec<u8>>> {
        self.with_db(|db| {
            let tx = db.begin_read().map_err(Self::storage_error)?;
            let table = match tx.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(BTreeMap::new()),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut values = BTreeMap::new();
            for key in keys {
                if let Some(value) = table.get(key.as_str()).map_err(Self::storage_error)? {
                    values.insert(key.clone(), value.value().to_vec());
                }
            }
            Ok(values)
        })
    }
}
