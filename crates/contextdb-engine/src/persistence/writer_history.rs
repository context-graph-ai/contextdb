//! Ordered history selectors, maintained beside their primary log atomically.
use super::*;

pub(super) const WRITER_CHANGE_LOG_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("writer_change_log");
const CHANGE_LOG_BY_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("change_log_by_table");
const CHANGE_LOG_LSN_COUNT: TableDefinition<u64, u64> =
    TableDefinition::new("change_log_lsn_count");

impl RedbPersistence {
    pub(super) fn numeric_change_key(key: &str) -> Result<[u8; 16]> {
        let (lsn, ordinal) = key
            .split_once(':')
            .ok_or_else(|| Error::Other("change-log position has no ordinal".into()))?;
        let lsn: u64 = lsn
            .parse()
            .map_err(|_| Error::Other("invalid change-log LSN".into()))?;
        let ordinal: u64 = ordinal
            .parse()
            .map_err(|_| Error::Other("invalid change-log ordinal".into()))?;
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&lsn.to_be_bytes());
        bytes[8..].copy_from_slice(&ordinal.to_be_bytes());
        Ok(bytes)
    }

    pub(super) fn log_table_selector(
        entry: &ChangeLogEntry,
        position: &[u8; 16],
    ) -> Option<Vec<u8>> {
        let table = match entry {
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. } => {
                table
            }
            _ => return None,
        };
        let mut key = Vec::with_capacity(table.len() + 1 + 16);
        key.extend_from_slice(table.as_bytes());
        key.push(0);
        key.extend_from_slice(position);
        Some(key)
    }

    pub(super) fn write_change_log_selectors_in_write(
        write: &redb::WriteTransaction,
        textual_key: &str,
        encoded: &[u8],
    ) -> Result<()> {
        let position = Self::numeric_change_key(textual_key)?;
        Self::remove_change_log_selectors_in_write(write, textual_key)?;
        let entry: ChangeLogEntry = Self::decode_exact(encoded)?;
        write
            .open_table(WRITER_CHANGE_LOG_TABLE)
            .map_err(Self::storage_error)?
            .insert(position.as_slice(), encoded)
            .map_err(Self::storage_error)?;
        if let Some(key) = Self::log_table_selector(&entry, &position) {
            write
                .open_table(CHANGE_LOG_BY_TABLE)
                .map_err(Self::storage_error)?
                .insert(key.as_slice(), position.as_slice())
                .map_err(Self::storage_error)?;
        }
        let lsn = u64::from_be_bytes(position[..8].try_into().expect("fixed position width"));
        let mut counts = write
            .open_table(CHANGE_LOG_LSN_COUNT)
            .map_err(Self::storage_error)?;
        let current = counts
            .get(lsn)
            .map_err(Self::storage_error)?
            .map_or(0, |value| value.value());
        let next = current
            .checked_add(1)
            .ok_or_else(|| Error::Other("log occurrence count overflow".into()))?;
        counts.insert(lsn, next).map_err(Self::storage_error)?;
        Ok(())
    }

    pub(super) fn remove_change_log_selectors_in_write(
        write: &redb::WriteTransaction,
        textual_key: &str,
    ) -> Result<()> {
        let position = Self::numeric_change_key(textual_key)?;
        let entry = {
            let mut log = write
                .open_table(WRITER_CHANGE_LOG_TABLE)
                .map_err(Self::storage_error)?;
            log.remove(position.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| Self::decode_exact::<ChangeLogEntry>(value.value()))
                .transpose()?
        };
        let Some(entry) = entry else {
            return Ok(());
        };
        if let Some(key) = Self::log_table_selector(&entry, &position) {
            write
                .open_table(CHANGE_LOG_BY_TABLE)
                .map_err(Self::storage_error)?
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        let lsn = u64::from_be_bytes(position[..8].try_into().expect("fixed position width"));
        let mut counts = write
            .open_table(CHANGE_LOG_LSN_COUNT)
            .map_err(Self::storage_error)?;
        let count = counts
            .get(lsn)
            .map_err(Self::storage_error)?
            .map_or(0, |value| value.value());
        let next = count.checked_sub(1).ok_or_else(|| Error::StoreCorrupted {
            path: "change_log_lsn_count".into(),
            reason: "missing log occurrence count".into(),
        })?;
        if next == 0 {
            counts.remove(lsn).map_err(Self::storage_error)?;
        } else {
            counts.insert(lsn, next).map_err(Self::storage_error)?;
        }
        Ok(())
    }

    pub(super) fn clear_change_log_selectors_in_write(
        write: &redb::WriteTransaction,
    ) -> Result<()> {
        write
            .delete_table(WRITER_CHANGE_LOG_TABLE)
            .map_err(Self::storage_error)?;
        write
            .delete_table(CHANGE_LOG_BY_TABLE)
            .map_err(Self::storage_error)?;
        write
            .delete_table(CHANGE_LOG_LSN_COUNT)
            .map_err(Self::storage_error)?;
        Ok(())
    }
}
