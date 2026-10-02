//! Finite current-only cleanup resumes from authenticated physical positions.
use super::*;

const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("currency_cleanup_progress");
const MAGIC: &[u8] = b"CDBCUP01";

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CurrencyCleanupProgress {
    pub(crate) declaration: [u8; 32],
    pub(crate) through: Lsn,
    pub(crate) after_group: Option<RowId>,
    pub(crate) group: Option<CurrencyCleanupGroup>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CurrencyCleanupGroup {
    pub(crate) row: RowId,
    pub(crate) keeper: Option<(Lsn, [u8; 24])>,
    pub(crate) prune_after: Option<[u8; 24]>,
}

fn damage(reason: impl Into<String>) -> Error {
    Error::StoreCorrupted {
        path: "currency_cleanup_progress".into(),
        reason: reason.into(),
    }
}

impl RedbPersistence {
    pub(crate) fn currency_cleanup_declaration(meta: &TableMeta) -> Result<[u8; 32]> {
        let mut hash = blake3::Hasher::new();
        bincode::serde::encode_into_std_write(meta, &mut hash, bincode::config::standard())
            .map_err(|error| Error::Other(format!("cleanup declaration encode failed: {error}")))?;
        Ok(*hash.finalize().as_bytes())
    }

    pub(crate) fn currency_cleanup_progress(
        root: &WriterSnapshot,
        table: &str,
        declaration: [u8; 32],
    ) -> Result<CurrencyCleanupProgress> {
        let fresh = || CurrencyCleanupProgress {
            declaration,
            through: root.lsn,
            after_group: None,
            group: None,
        };
        let read = root.owner_read();
        let progress = match read.open_table(TABLE) {
            Ok(progress) => progress,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(fresh()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let Some(value) = progress.get(table).map_err(Self::storage_error)? else {
            return Ok(fresh());
        };
        let bytes = value.value();
        if bytes.len() < MAGIC.len() + 32 || bytes.len() > 512 || !bytes.starts_with(MAGIC) {
            return Err(damage("invalid cleanup progress envelope"));
        }
        let end = bytes.len() - 32;
        let mut hash = blake3::Hasher::new();
        hash.update(table.as_bytes());
        hash.update(&bytes[..end]);
        if hash.finalize().as_bytes().as_slice() != &bytes[end..] {
            return Err(damage(
                "cleanup progress checksum or table identity mismatch",
            ));
        }
        let state: CurrencyCleanupProgress = Self::decode_exact(&bytes[MAGIC.len()..end])
            .map_err(|error| damage(format!("invalid cleanup progress: {error}")))?;
        if state.through > root.lsn {
            return Err(damage("cleanup frontier is ahead of the captured root"));
        }
        if let Some(group) = &state.group {
            for key in [group.prune_after, group.keeper.map(|(_, key)| key)]
                .into_iter()
                .flatten()
            {
                if key[..8] != group.row.0.to_be_bytes() {
                    return Err(damage("cleanup position belongs to another row"));
                }
            }
            if state.after_group.is_some_and(|row| row >= group.row)
                || group
                    .keeper
                    .is_some_and(|(lsn, key)| lsn > root.lsn || key[16..] != lsn.0.to_be_bytes())
                || (group.keeper.is_none() && group.prune_after.is_some())
            {
                return Err(damage("incoherent cleanup group progress"));
            }
        }
        Ok(if state.declaration == declaration {
            state
        } else {
            fresh()
        })
    }

    pub(crate) fn currency_cleanup_rotation(root: &WriterSnapshot) -> Result<Option<[u8; 32]>> {
        let read = root.owner_read();
        let table = match read.open_table(TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(Self::storage_error(error)),
        };
        let Some(value) = table.get("").map_err(Self::storage_error)? else {
            return Ok(None);
        };
        let bytes = value.value();
        if bytes.len() != MAGIC.len() + 64
            || !bytes.starts_with(MAGIC)
            || blake3::hash(&bytes[..MAGIC.len() + 32])
                .as_bytes()
                .as_slice()
                != &bytes[MAGIC.len() + 32..]
        {
            return Err(damage("invalid cleanup table continuation"));
        }
        Ok(Some(
            bytes[MAGIC.len()..MAGIC.len() + 32].try_into().unwrap(),
        ))
    }

    pub(super) fn write_currency_cleanup_progress(
        write: &redb::WriteTransaction,
        progress: &[(String, CurrencyCleanupProgress)],
        rotation: Option<[u8; 32]>,
    ) -> Result<()> {
        if progress.is_empty() && rotation.is_none() {
            return Ok(());
        }
        let mut table = write.open_table(TABLE).map_err(Self::storage_error)?;
        if let Some(rotation) = rotation {
            let mut bytes = Vec::with_capacity(MAGIC.len() + 64);
            bytes.extend_from_slice(MAGIC);
            bytes.extend_from_slice(&rotation);
            let checksum = blake3::hash(&bytes);
            bytes.extend_from_slice(checksum.as_bytes());
            table
                .insert("", bytes.as_slice())
                .map_err(Self::storage_error)?;
        }
        for (name, state) in progress {
            let payload = Self::encode(state)?;
            let mut bytes = Vec::with_capacity(MAGIC.len() + payload.len() + 32);
            bytes.extend_from_slice(MAGIC);
            bytes.extend_from_slice(&payload);
            let mut hash = blake3::Hasher::new();
            hash.update(name.as_bytes());
            hash.update(&bytes);
            bytes.extend_from_slice(hash.finalize().as_bytes());
            if bytes.len() > 512 {
                return Err(damage("cleanup progress exceeded its fixed envelope"));
            }
            table
                .insert(name.as_str(), bytes.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }
}
