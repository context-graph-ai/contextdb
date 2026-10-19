//! Allocation-free descriptor and owner projections for borrowed physical reads.
use super::*;

#[derive(Deserialize, PartialEq, Eq)]
pub(crate) struct BorrowedProgress<'a> {
    pub(crate) generation_id: u64,
    declaration_digest: u64,
    frontier_tx: u64,
    frontier_lsn: u64,
    graph: PagedGraphState,
    #[serde(borrow)]
    last_source_key: Option<&'a [u8]>,
    total_points: u64,
    phase: u8,
    encoding_cursor: Option<(u8, i32)>,
    encoding_bytes: u64,
    encoded_points: u64,
    encoding_digest: u64,
    #[serde(borrow)]
    cleanup_cursor: Option<&'a [u8]>,
    members: u64,
    member_cursor: Option<(u64, u64, u64)>,
}
#[derive(Deserialize, PartialEq, Eq)]
pub(crate) struct BorrowedMeta<'a> {
    magic: [u8; 8],
    pub(crate) storage_id: u64,
    declaration: PagedDeclaration,
    #[serde(borrow)]
    pub(crate) progress: BorrowedProgress<'a>,
    pub(crate) saved: Option<SavedGenerationBinding>,
    schema_binding: VectorSchemaBinding,
    pub(crate) page_set: PageSetRoot,
}
impl<'a> BorrowedMeta<'a> {
    pub(crate) fn decode(bytes: &'a [u8]) -> Result<Self> {
        let (meta, consumed): (Self, usize) = bincode::serde::borrow_decode_from_slice(
            bytes, bincode::config::standard())
            .map_err(|_| corruption("borrowed paged descriptor cannot be decoded"))?;
        if consumed != bytes.len() || meta.magic != META_MAGIC || meta.storage_id == 0
            || meta.progress.last_source_key.is_some() || meta.progress.cleanup_cursor.is_some()
        { return Err(corruption("borrowed paged descriptor is not a complete published frame")); }
        Ok(meta)
    }
}

/// Every original field, including route, source digest, logical generation,
/// topology and schema lifetime, participates in the normalized comparison.
/// No owned descriptor decode, re-encoding, or Legacy accounting is used.
pub(crate) fn borrowed_change_backing<'a>(storage: u64, frame: &'a [u8])
    -> Result<(BorrowedMeta<'a>, BorrowedMeta<'a>)> {
    if frame.len() > 64 * 1024 { return Err(corruption("native change backing exceeds its codec window")); }
    let payload = open_value(&storage.to_be_bytes(), frame)?;
    let ((original, physical), consumed): ((&[u8], &[u8]), usize) =
        bincode::serde::borrow_decode_from_slice(payload, bincode::config::standard())
            .map_err(|_| corruption("borrowed native backing cannot be decoded"))?;
    if consumed != payload.len() { return Err(corruption("native backing has trailing bytes")); }
    let original = BorrowedMeta::decode(original)?;
    let mut physical = BorrowedMeta::decode(physical)?;
    let physical_storage = physical.storage_id;
    let physical_pages = physical.page_set;
    if original.storage_id != storage || storage & MUTABLE_STORAGE != 0
        || physical_storage & MUTABLE_STORAGE == 0 || physical_storage > i64::MAX as u64
        || original.page_set.pages == MUTABLE_PAGE_SET || physical_pages.pages != MUTABLE_PAGE_SET
    { return Err(corruption("native backing has invalid original/physical coordinates")); }
    physical.storage_id = original.storage_id;
    physical.page_set = original.page_set;
    if physical != original { return Err(corruption("native backing changed an original descriptor field")); }
    physical.storage_id = physical_storage;
    physical.page_set = physical_pages;
    Ok((original, physical))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalOwner {
    pub(crate) storage_id: u64,
    pub(crate) point: PointId,
    pub(crate) data_id: usize,
    pub(crate) graph_origin: usize,
    pub(crate) row_id: RowId,
    pub(crate) lsn: Lsn,
    pub(crate) created_tx: TxId,
    pub(crate) deleted_tx: Option<TxId>,
}

/// This codec does not decode point values or links. The producer's fixed-int
/// point prefix is origin:u64, layer:u8, rank:i32, all little endian.
pub(crate) fn point_origin(encoded: &[u8], expected: PointId) -> Result<usize> {
    let header = encoded.get(..13).ok_or_else(|| corruption("physical point prefix is truncated"))?;
    if header[8] != expected.0 || i32::from_le_bytes(header[9..13].try_into().unwrap()) != expected.1 {
        return Err(corruption("physical point key and stored point disagree"));
    }
    usize::try_from(u64::from_le_bytes(header[..8].try_into().unwrap()))
        .map_err(|_| corruption("physical point origin exceeds usize"))
}

pub(crate) fn owner_from_identity(key: &[u8], frame: &[u8]) -> Result<PhysicalOwner> {
    let Some(PhysicalPagedIdentity::Identity { storage_id, point, data_id, graph_origin,
        row_id, lsn, created_tx, deleted_tx, .. }) = physical_identity(key, frame)?
    else { return Err(corruption("physical owner join did not reach its identity")); };
    Ok(PhysicalOwner { storage_id, point, data_id, graph_origin, row_id, lsn, created_tx, deleted_tx })
}
