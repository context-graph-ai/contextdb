//! A row's latest physical identity is maintained beside logical publication.
use super::*;
const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("writer_row_heads");
const ROOT_KEY: &str = "row_heads_root";
fn damage(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "writer_row_heads".into(),
        reason: reason.into(),
    }
}
fn decode(key: &[u8], value: &[u8]) -> Result<[u8; 24]> {
    if value.len() != 56 {
        return Err(damage("invalid row head envelope"));
    }
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(&value[..24]);
    if hash.finalize().as_bytes().as_slice() != &value[24..] {
        return Err(damage("row head checksum or identity mismatch"));
    }
    Ok(value[..24].try_into().unwrap())
}
fn route(key: &[u8]) -> Result<(&str, RowId)> {
    let length = key
        .get(..8)
        .ok_or_else(|| damage("truncated row head route"))?;
    let length = usize::try_from(u64::from_le_bytes(length.try_into().unwrap()))
        .map_err(|_| damage("row head route length overflow"))?;
    let end = 8usize
        .checked_add(length)
        .ok_or_else(|| damage("row head route length overflow"))?;
    if key.len()
        != end
            .checked_add(16)
            .ok_or_else(|| damage("row head route length overflow"))?
        || key[end..end + 8] != 8u64.to_le_bytes()
    {
        return Err(damage("invalid row head route"));
    }
    let table =
        std::str::from_utf8(&key[8..end]).map_err(|_| damage("row head table is not UTF-8"))?;
    let row = RowId(u64::from_le_bytes(key[end + 8..].try_into().unwrap()));
    Ok((table, row))
}
fn logical_key(table: &str, row: RowId) -> Vec<u8> {
    let mut key = Vec::with_capacity(table.len() + 24);
    key.extend_from_slice(&(table.len() as u64).to_le_bytes());
    key.extend_from_slice(table.as_bytes());
    key.extend_from_slice(&8u64.to_le_bytes());
    key.extend_from_slice(&row.0.to_le_bytes());
    key
}
fn rank(key: [u8; 24]) -> (u64, u64) {
    (
        u64::from_be_bytes(key[16..].try_into().unwrap()),
        u64::from_be_bytes(key[8..16].try_into().unwrap()),
    )
}

impl RedbPersistence {
    pub(super) fn apply_writer_row_heads_in_write(
        write: &redb::WriteTransaction,
        mutations: &[content_identity::ContentIdentityMutation],
    ) -> Result<()> {
        let mut heads = write.open_table(TABLE).map_err(Self::storage_error)?;
        for mutation in mutations {
            let Some(projection) = mutation.row_head else {
                continue;
            };
            let (table, row) = route(&mutation.logical_key)?;
            let name = Self::rel_table_name(table);
            let relation: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
            if write
                .table_root_fingerprint(relation)
                .map_err(Self::storage_error)?
                .is_none()
            {
                if projection.is_some() {
                    return Err(damage(
                        "row head publication names an absent physical family",
                    ));
                }
                heads
                    .remove(mutation.logical_key.as_slice())
                    .map_err(Self::storage_error)?;
                continue;
            }
            let relation = write.open_table(relation).map_err(Self::storage_error)?;
            let old = heads
                .get(mutation.logical_key.as_slice())
                .map_err(Self::storage_error)?
                .map(|value| decode(&mutation.logical_key, value.value()))
                .transpose()?;
            let old = match old {
                Some(key)
                    if relation
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .is_some() =>
                {
                    Some(key)
                }
                _ => None,
            };
            let next = match (old, projection) {
                (Some(old), Some(next)) if rank(old) > rank(next) => Some(old),
                (_, Some(next)) => Some(next),
                (old, None) => old,
            };
            if let Some(next) = next {
                if next[..8] != row.0.to_be_bytes()
                    || relation
                        .get(next.as_slice())
                        .map_err(Self::storage_error)?
                        .is_none()
                {
                    return Err(damage(
                        "row head names a missing or foreign physical identity",
                    ));
                }
                let mut encoded = [0; 56];
                encoded[..24].copy_from_slice(&next);
                let mut hash = blake3::Hasher::new();
                hash.update(&mutation.logical_key);
                hash.update(&next);
                encoded[24..].copy_from_slice(hash.finalize().as_bytes());
                heads
                    .insert(mutation.logical_key.as_slice(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            } else {
                heads
                    .remove(mutation.logical_key.as_slice())
                    .map_err(Self::storage_error)?;
            }
        }
        Ok(())
    }

    pub(super) fn write_row_heads_root(write: &redb::WriteTransaction) -> Result<()> {
        {
            write.open_table(TABLE).map_err(Self::storage_error)?;
        }
        let bytes = write
            .table_root_fingerprint(TABLE)
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]);
        let mut metadata = write
            .open_table(writer_recovery::RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        if metadata
            .get(ROOT_KEY)
            .map_err(Self::storage_error)?
            .is_none_or(|old| old.value() != bytes)
        {
            metadata
                .insert(ROOT_KEY, bytes.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    pub(crate) fn writer_row_head(
        root: &WriterSnapshot,
        table: &str,
        row: RowId,
    ) -> Result<[u8; 24]> {
        let read = root.owner_read();
        let heads = read.open_table(TABLE).map_err(Self::storage_error)?;
        let fingerprint = read
            .table_root_fingerprint(TABLE)
            .map_err(Self::storage_error)?
            .unwrap_or([0; 32]);
        let metadata = read
            .open_table(writer_recovery::RECOVERY_TABLE)
            .map_err(Self::storage_error)?;
        let expected = metadata
            .get(ROOT_KEY)
            .map_err(Self::storage_error)?
            .ok_or_else(|| damage("missing row head root binding"))?;
        if expected.value() != fingerprint {
            return Err(damage("row head root binding mismatch"));
        }
        let key = logical_key(table, row);
        let value = heads
            .get(key.as_slice())
            .map_err(Self::storage_error)?
            .ok_or_else(|| damage("missing latest physical row identity"))?;
        let head = decode(&key, value.value())?;
        if head[..8] != row.0.to_be_bytes() {
            return Err(damage("row head belongs to another row"));
        }
        let name = Self::rel_table_name(table);
        let relation: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
        if read
            .open_table(relation)
            .map_err(Self::storage_error)?
            .get(head.as_slice())
            .map_err(Self::storage_error)?
            .is_none()
        {
            return Err(damage("latest row head has no physical body"));
        }
        Ok(head)
    }
}
