//! Key-only vector identities for admitted schema and integrity walks.
use super::*;
use crate::memory_accounting::{MemoryAccountant, OwnedMemoryReservation};
use std::ops::Bound;

pub(crate) struct VectorBodyIdentity {
    pub(crate) index: VectorIndexRef,
    pub(crate) row_id: RowId,
    pub(crate) created_tx: TxId,
    pub(crate) lsn: Lsn,
}

fn invalid() -> Error {
    Error::StoreCorrupted {
        path: "vector_entries".into(),
        reason: "vector body identity key is malformed".into(),
    }
}

pub(crate) fn decode_key(key: &[u8]) -> Result<VectorBodyIdentity> {
    let table_end = key.iter().position(|byte| *byte == 0).ok_or_else(invalid)?;
    let (table, rest) = key.split_at(table_end);
    let rest = &rest[1..];
    let column_end = rest
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(invalid)?;
    let (column, stamps) = rest.split_at(column_end);
    let stamps = &stamps[1..];
    if table.is_empty() || column.is_empty() || stamps.len() != 24 {
        return Err(invalid());
    }
    Ok(VectorBodyIdentity {
        index: VectorIndexRef::new(
            std::str::from_utf8(table).map_err(|_| invalid())?,
            std::str::from_utf8(column).map_err(|_| invalid())?,
        ),
        row_id: RowId(u64::from_be_bytes(stamps[..8].try_into().unwrap())),
        created_tx: TxId(u64::from_be_bytes(stamps[8..16].try_into().unwrap())),
        lsn: Lsn(u64::from_be_bytes(stamps[16..].try_into().unwrap())),
    })
}

/// The caller supplies its captured read table or its existing write table;
/// this walk never starts another transaction. A table name restricts the
/// key range before any iterator read. Body payloads are neither decoded nor
/// copied; admitted native leaf extents may contain inline body bytes.
/// The visitor owns and admits anything it retains beyond this one window.
pub(crate) fn visit_table<T: redb::ReadableTable<&'static [u8], &'static [u8]>>(
    table: &T,
    table_name: Option<&str>,
    accountant: Arc<MemoryAccountant>,
    before: &mut dyn FnMut() -> Result<()>,
    visit: &mut dyn FnMut(VectorBodyIdentity) -> Result<()>,
) -> Result<()> {
    let mut memory = OwnedMemoryReservation::try_new_for(
        accountant,
        table_name
            .map_or(0, str::len)
            .saturating_mul(32)
            .saturating_add(4096),
        "writer_source",
        "vector_identity_window",
        "Raise MEMORY_LIMIT to read a vector identity window.",
    )?;
    before()?;
    let prefix = table_name.map(|name| {
        let mut prefix = name.as_bytes().to_vec();
        prefix.push(0);
        prefix
    });
    let end = prefix.as_deref().and_then(writer_source::prefix_end);
    let lower = prefix.as_deref().map_or(Bound::Unbounded, Bound::Included);
    let upper = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
    for item in table
        .range::<&[u8]>((lower, upper))
        .map_err(RedbPersistence::storage_error)?
    {
        before()?;
        let (key, _) = item.map_err(RedbPersistence::storage_error)?;
        let needed = key.value().len().saturating_mul(4).saturating_add(4096);
        memory.try_grow_for(
            needed.saturating_sub(memory.bytes()),
            "writer_source",
            "vector_identity_window",
            "Raise MEMORY_LIMIT to decode this vector identity.",
        )?;
        visit(decode_key(key.value())?)?;
    }
    Ok(())
}
