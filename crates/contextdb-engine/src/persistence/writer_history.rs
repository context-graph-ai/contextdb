//! Ordered history selectors, maintained beside their primary log atomically.
use super::*;

pub(super) const WRITER_CHANGE_LOG_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("writer_change_log");
const CHANGE_LOG_BY_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("change_log_by_table");
const CHANGE_LOG_LSN_COUNT: TableDefinition<u64, u64> =
    TableDefinition::new("change_log_lsn_count");

impl RedbPersistence {
    pub(super) fn ddl_lsn_from_key(key: &str) -> Result<Lsn> {
        key.split_once(':')
            .map_or(key, |(lsn, _)| lsn)
            .parse::<u64>()
            .map(Lsn)
            .map_err(|_| Error::StoreCorrupted {
                path: "ddl_log".into(),
                reason: "invalid declaration log frontier".into(),
            })
    }

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

impl super::writer_source::WriterSnapshot {
    pub(crate) fn ddl_history_cursor(
        self: &Arc<Self>,
        since: Lsn,
    ) -> Result<super::writer_source::WriterReadCursor> {
        self.ddl_history_cursor_ordered(since, false)
    }

    pub(crate) fn ddl_history_cursor_ordered(
        self: &Arc<Self>,
        since: Lsn,
        reverse: bool,
    ) -> Result<super::writer_source::WriterReadCursor> {
        // Encode bounds on the stack; cursor admission precedes their copies.
        let encode = |mut value: u64| {
            let mut key = [b'0'; 20];
            for digit in key.iter_mut().rev() {
                *digit += (value % 10) as u8;
                value /= 10;
            }
            key
        };
        let lower = encode(since.0.saturating_add(1));
        let mut upper = [b';'; 21];
        upper[..20].copy_from_slice(&encode(self.lsn.0));
        self.cursor(
            "ddl_log",
            super::writer_source::WriterCursorFamily::Text,
            std::ops::Bound::Included(&lower),
            std::ops::Bound::Included(&upper),
            reverse,
        )
    }
}
impl super::writer_source::WriterReadCursor {
    /// Compare only the requested declaration kind. Other history payloads
    /// remain borrowed storage bytes and never become decoded metadata copies.
    pub(crate) fn next_ddl_candidate(
        &mut self,
        encoded_kind: u8,
    ) -> Result<Option<super::writer_source::WriterWindow<(Lsn, Option<DdlChange>)>>> {
        self.next(|key, value| {
            let key = std::str::from_utf8(key)
                .map_err(|_| Error::Other("invalid declaration log key".into()))?;
            let lsn = RedbPersistence::ddl_lsn_from_key(key)?;
            let Some(kind) = value.first() else {
                return Err(Error::StoreCorrupted {
                    path: "ddl_log".into(),
                    reason: "empty declaration history record".into(),
                });
            };
            // All current DDL discriminants use the codec's single-byte form.
            // A matching kind still receives the complete strict decoder, so
            // earlier payload forms retain their established semantic equality.
            let change = if *kind == encoded_kind {
                Some(RedbPersistence::decode_config_value_exact(value)?)
            } else {
                None
            };
            Ok((lsn, change))
        })
    }

    pub(crate) fn next_ddl_position(
        &mut self,
    ) -> Result<Option<super::writer_source::WriterWindow<Lsn>>> {
        self.next(|key, _| {
            let key = std::str::from_utf8(key)
                .map_err(|_| Error::Other("invalid declaration log key".into()))?;
            RedbPersistence::ddl_lsn_from_key(key)
        })
    }

    pub(crate) fn next_ddl_history(
        &mut self,
    ) -> Result<Option<super::writer_source::WriterWindow<(Lsn, DdlChange)>>> {
        self.next(|key, value| {
            let key = std::str::from_utf8(key)
                .map_err(|_| Error::Other("invalid declaration log key".into()))?;
            let lsn = key
                .split_once(':')
                .map_or(key, |(lsn, _)| lsn)
                .parse::<u64>()
                .map_err(|_| Error::Other("invalid declaration log frontier".into()))?;
            let (change, consumed) = crate::read_image_memory::decode(value)?;
            if consumed != value.len() {
                return Err(Error::Other(
                    "declaration history record has trailing bytes".into(),
                ));
            }
            Ok((Lsn(lsn), change))
        })
    }
}
