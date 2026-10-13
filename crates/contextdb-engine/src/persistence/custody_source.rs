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

/// One committed read view over the custody CONFIG rows and their derived
/// group membership. Values are borrowed for the duration of a visit only.
pub(crate) struct CustodyReader {
    config: Option<redb::ReadOnlyTable<&'static str, &'static [u8]>>,
    groups: Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>,
}

impl CustodyReader {
    /// Visit exactly `key`'s stored value, if committed.
    pub(crate) fn get<T>(
        &self,
        key: &str,
        visit: impl FnOnce(Option<&[u8]>) -> Result<T>,
    ) -> Result<T> {
        let Some(config) = &self.config else {
            return visit(None);
        };
        match config.get(key).map_err(RedbPersistence::storage_error)? {
            Some(value) => visit(Some(value.value())),
            None => visit(None),
        }
    }
    /// Visit every committed `(key, value)` whose key starts with `prefix`, in key order.
    pub(crate) fn visit_prefix(
        &self,
        prefix: &str,
        mut visit: impl FnMut(&str, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let Some(config) = &self.config else {
            return Ok(());
        };
        for entry in config
            .range(prefix..)
            .map_err(RedbPersistence::storage_error)?
        {
            let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
            let key = key.value();
            if !key.starts_with(prefix) {
                break;
            }
            visit(key, value.value())?;
        }
        Ok(())
    }
    /// Visit the member keys of exactly `group`, in key order.
    pub(crate) fn visit_group(
        &self,
        group: &str,
        mut visit: impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        let Some(groups) = &self.groups else {
            return Ok(());
        };
        let head = group_head(group)?;
        for entry in groups
            .range::<&[u8]>(head.as_slice()..)
            .map_err(RedbPersistence::storage_error)?
        {
            let (member, _) = entry.map_err(RedbPersistence::storage_error)?;
            let Some(key) = member.value().strip_prefix(head.as_slice()) else {
                break;
            };
            visit(std::str::from_utf8(key).map_err(|_| crate::custody::canonical::invalid())?)?;
        }
        Ok(())
    }
    /// Visit every `(group, member key)` whose group starts with `prefix`, in
    /// group then key order. Groups of one kind share a fixed length, so the
    /// caller names the full group length it expects.
    pub(crate) fn visit_groups_with_prefix(
        &self,
        prefix: &str,
        group_len: usize,
        mut visit: impl FnMut(&str, &str) -> Result<()>,
    ) -> Result<()> {
        let Some(groups) = &self.groups else {
            return Ok(());
        };
        let mut head = u64::try_from(group_len)
            .map_err(|_| crate::custody::canonical::invalid())?
            .to_be_bytes()
            .to_vec();
        head.extend_from_slice(prefix.as_bytes());
        for entry in groups
            .range::<&[u8]>(head.as_slice()..)
            .map_err(RedbPersistence::storage_error)?
        {
            let (member, _) = entry.map_err(RedbPersistence::storage_error)?;
            let member = member.value();
            if !member.starts_with(head.as_slice()) {
                break;
            }
            let body = &member[8..];
            let (group, key) = body
                .split_at_checked(group_len)
                .ok_or_else(crate::custody::canonical::invalid)?;
            visit(
                std::str::from_utf8(group).map_err(|_| crate::custody::canonical::invalid())?,
                std::str::from_utf8(key).map_err(|_| crate::custody::canonical::invalid())?,
            )?;
        }
        Ok(())
    }
}

/// `[u64 BE group length][group]`, the shared head of one group's membership
/// rows in `read_custody_groups` (see `custody_groups::fill_reverse`).
fn group_head(group: &str) -> Result<Vec<u8>> {
    let mut head = u64::try_from(group.len())
        .map_err(|_| crate::custody::canonical::invalid())?
        .to_be_bytes()
        .to_vec();
    head.extend_from_slice(group.as_bytes());
    Ok(head)
}

impl RedbPersistence {
    /// Whether this store's derived custody group index is complete. Only a
    /// store created empty carries the marker; others keep the resident journal.
    pub(crate) fn custody_groups_complete(&self) -> Result<bool> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let groups = match read_txn.open_table(custody_groups::GROUP_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
                Err(err) => return Err(Self::storage_error(err)),
            };
            Ok(groups
                .get(custody_groups::COMPLETE)
                .map_err(Self::storage_error)?
                .is_some_and(|v| v.value() == custody_groups::COMPLETE_VALUE))
        })
    }
    /// Run `read` against one committed custody view. A view whose group index
    /// is incomplete answers `None`: its membership answers could be partial.
    pub(crate) fn with_custody_read<T>(
        &self,
        read: impl FnOnce(&CustodyReader) -> Result<T>,
    ) -> Result<Option<T>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => Some(table),
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(err) => return Err(Self::storage_error(err)),
            };
            let groups = match read_txn.open_table(custody_groups::GROUP_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(err) => return Err(Self::storage_error(err)),
            };
            if groups
                .get(custody_groups::COMPLETE)
                .map_err(Self::storage_error)?
                .is_none_or(|v| v.value() != custody_groups::COMPLETE_VALUE)
            {
                return Ok(None);
            }
            read(&CustodyReader {
                config,
                groups: Some(groups),
            })
            .map(Some)
        })
    }
}
