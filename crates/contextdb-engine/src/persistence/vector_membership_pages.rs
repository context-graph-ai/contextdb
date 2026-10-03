//! Transaction-coupled membership summaries and ordered source/reverse indexes.
//! These indexes retain only durable metadata; readers borrow one record window.
use super::*;
use contextdb_vector::hnsw::paged_builder::canonical_source_key;

pub(super) const SOURCE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_membership_source_pages");
pub(super) const REVERSE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_membership_reverse_pages");
pub(super) const SUMMARY: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_membership_summary_pages");
const GLOBAL: &[u8] = b"";
const MAGIC: &[u8] = b"CDBVMS01";

#[derive(Default, Clone, Serialize, Deserialize)]
pub(super) struct MembershipSummary {
    pub(super) entries: u64,
    pub(super) live_entries: u64,
    pub(super) max_tx: TxId,
    digest: [u8; 32],
}

fn corruption(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "vector_membership_summary_pages".into(),
        reason: reason.into(),
    }
}

fn encode_summary(key: &[u8], summary: &MembershipSummary) -> Result<Vec<u8>> {
    let payload = RedbPersistence::encode(summary)?;
    let mut bytes = Vec::with_capacity(MAGIC.len() + payload.len() + 32);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&payload);
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(&bytes);
    bytes.extend_from_slice(hash.finalize().as_bytes());
    Ok(bytes)
}

pub(super) fn decode_summary(key: &[u8], bytes: &[u8]) -> Result<MembershipSummary> {
    if bytes.len() < MAGIC.len() + 32 || !bytes.starts_with(MAGIC) {
        return Err(corruption(
            "membership summary envelope is missing or truncated",
        ));
    }
    let end = bytes.len() - 32;
    let mut hash = blake3::Hasher::new();
    hash.update(key);
    hash.update(&bytes[..end]);
    if hash.finalize().as_bytes().as_slice() != &bytes[end..] {
        return Err(corruption("membership summary checksum or key is invalid"));
    }
    let summary: MembershipSummary = RedbPersistence::decode_exact(&bytes[MAGIC.len()..end])?;
    if summary.live_entries > summary.entries {
        return Err(corruption(
            "membership summary live count exceeds its total",
        ));
    }
    Ok(summary)
}

pub(super) fn source_key(record: &VectorPartitionMembershipRecord) -> Vec<u8> {
    let mut key = RedbPersistence::vector_partition_prefix(&record.index, &record.partition_key);
    key.extend_from_slice(&canonical_source_key(
        record.row_id,
        record.row_lsn,
        record.visible_from,
    ));
    // Order the owning MVCC interval, while preserving every original raw
    // body identity in an exclusive physical suffix for rejection/resumption.
    key.extend_from_slice(&(!record.row_created_tx.0).to_be_bytes());
    key.extend_from_slice(&(!record.vector_created_tx.0).to_be_bytes());
    key.extend_from_slice(&(!record.vector_lsn.0).to_be_bytes());
    key
}

pub(super) fn reverse_row_prefix(index: &VectorIndexRef, row: RowId) -> Vec<u8> {
    let mut key = Vec::with_capacity(index.table.len() + index.column.len() + 24);
    RedbPersistence::append_partition_key_component(&mut key, index.table.as_bytes());
    RedbPersistence::append_partition_key_component(&mut key, index.column.as_bytes());
    key.extend_from_slice(&row.0.to_be_bytes());
    key
}

pub(super) fn reverse_key(record: &VectorPartitionMembershipRecord) -> Vec<u8> {
    let mut key = reverse_row_prefix(&record.index, record.row_id);
    key.extend_from_slice(&record.row_created_tx.0.to_be_bytes());
    key.extend_from_slice(&record.row_lsn.0.to_be_bytes());
    RedbPersistence::append_partition_key_component(
        &mut key,
        &record.partition_key.canonical_bytes(),
    );
    key.extend_from_slice(&record.vector_created_tx.0.to_be_bytes());
    key.extend_from_slice(&record.vector_lsn.0.to_be_bytes());
    key
}

fn component<'a>(key: &mut &'a [u8]) -> Result<&'a [u8]> {
    let (length, rest) = key
        .split_at_checked(8)
        .ok_or_else(|| corruption("truncated membership route key"))?;
    let length = usize::try_from(u64::from_be_bytes(length.try_into().unwrap()))
        .map_err(|_| corruption("membership route length overflow"))?;
    let (component, rest) = rest
        .split_at_checked(length)
        .ok_or_else(|| corruption("truncated membership route component"))?;
    *key = rest;
    Ok(component)
}

fn source_key_from_membership_key(mut key: &[u8]) -> Result<Vec<u8>> {
    let table = std::str::from_utf8(component(&mut key)?)
        .map_err(|_| corruption("membership table name is not UTF-8"))?;
    let column = std::str::from_utf8(component(&mut key)?)
        .map_err(|_| corruption("membership column name is not UTF-8"))?;
    let partition = VectorPartitionKey::from_canonical_bytes(component(&mut key)?)
        .map_err(|_| corruption("membership partition key is invalid"))?;
    if key.len() != 40 {
        return Err(corruption("membership identity key has the wrong length"));
    }
    let row = RowId(u64::from_be_bytes(key[..8].try_into().unwrap()));
    let created = TxId(u64::from_be_bytes(key[8..16].try_into().unwrap()));
    let lsn = Lsn(u64::from_be_bytes(key[16..24].try_into().unwrap()));
    let vector_created = TxId(u64::from_be_bytes(key[24..32].try_into().unwrap()));
    let vector_lsn = Lsn(u64::from_be_bytes(key[32..40].try_into().unwrap()));
    let mut source =
        RedbPersistence::vector_partition_prefix(&VectorIndexRef::new(table, column), &partition);
    source.extend_from_slice(&canonical_source_key(row, lsn, created.max(vector_created)));
    source.extend_from_slice(&(!created.0).to_be_bytes());
    source.extend_from_slice(&(!vector_created.0).to_be_bytes());
    source.extend_from_slice(&(!vector_lsn.0).to_be_bytes());
    Ok(source)
}

fn decode_member(key: &[u8], encoded: &[u8]) -> Result<VectorPartitionMembershipRecord> {
    let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, encoded)
        .map_err(|reason| corruption(&reason))?;
    prevalidate_vector_partition_metadata_routes(VectorPartitionRecordKind::Membership, payload)
        .map_err(|reason| corruption(&reason))?;
    let record: VectorPartitionMembershipRecord =
        decode_vector_partition_record_payload(VectorPartitionRecordKind::Membership, encoded)
            .map_err(|reason| corruption(&reason))?;
    if source_key(&record) != key {
        return Err(corruption(
            "membership source key does not match its authenticated identity",
        ));
    }
    Ok(record)
}

fn adjust(
    summary: &mut MembershipSummary,
    key: &[u8],
    encoded: &[u8],
    record: &VectorPartitionMembershipRecord,
    insert: bool,
) -> Result<()> {
    let digest = {
        let mut hash = blake3::Hasher::new();
        hash.update(key);
        hash.update(encoded);
        *hash.finalize().as_bytes()
    };
    for (old, change) in summary.digest.iter_mut().zip(digest) {
        *old ^= change;
    }
    let live = u64::from(record.deleted_tx.is_none());
    if insert {
        summary.entries = summary
            .entries
            .checked_add(1)
            .ok_or_else(|| corruption("membership count overflow"))?;
        summary.live_entries = summary
            .live_entries
            .checked_add(live)
            .ok_or_else(|| corruption("membership live count overflow"))?;
        summary.max_tx = summary
            .max_tx
            .max(record.visible_from)
            .max(record.deleted_tx.unwrap_or(TxId(0)));
    } else {
        summary.entries = summary
            .entries
            .checked_sub(1)
            .ok_or_else(|| corruption("membership count underflow"))?;
        summary.live_entries = summary
            .live_entries
            .checked_sub(live)
            .ok_or_else(|| corruption("membership live count underflow"))?;
    }
    Ok(())
}

/// Called by the existing membership commitment door, in that same transaction.
/// Replacement removes the old authenticated interval before installing the new
/// one. Deletes still have its durable source companion after the forward key
/// was removed; no body scan or resident reverse map is required.
pub(super) fn publish_membership(
    write: &redb::WriteTransaction,
    membership_key: &[u8],
    encoded: Option<&[u8]>,
    accountant: Option<Arc<crate::memory_accounting::MemoryAccountant>>,
) -> Result<()> {
    let _scratch = accountant
        .map(|accountant| {
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                accountant,
                membership_key.len().saturating_mul(32).saturating_add(4096),
                "vector",
                "publish_membership_pages",
                "Raise MEMORY_LIMIT to publish this vector membership.",
            )
        })
        .transpose()?;
    let key = source_key_from_membership_key(membership_key)?;
    let mut source = write
        .open_table(SOURCE)
        .map_err(RedbPersistence::storage_error)?;
    let previous = source
        .get(key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .map(|value| {
            if value.value().len() > membership_key.len().saturating_mul(8).saturating_add(512) {
                return Err(corruption(
                    "membership companion exceeds its bounded metadata window",
                ));
            }
            Ok(value.value().to_vec())
        })
        .transpose()?;
    let previous_record = previous
        .as_deref()
        .map(|bytes| decode_member(&key, bytes))
        .transpose()?;
    let next_record = encoded
        .map(|bytes| decode_member(&key, bytes))
        .transpose()?;
    for record in [previous_record.as_ref(), next_record.as_ref()]
        .into_iter()
        .flatten()
    {
        if RedbPersistence::vector_partition_membership_key(record) != membership_key {
            return Err(corruption(
                "one owning row version has conflicting vector placements",
            ));
        }
    }
    let route = previous_record.as_ref().or(next_record.as_ref());
    let Some(route) = route else {
        return Ok(());
    };
    let partition_key =
        RedbPersistence::vector_partition_prefix(&route.index, &route.partition_key);
    let mut summaries = write
        .open_table(SUMMARY)
        .map_err(RedbPersistence::storage_error)?;
    let read_summary = |key: &[u8]| -> Result<MembershipSummary> {
        summaries
            .get(key)
            .map_err(RedbPersistence::storage_error)?
            .map(|value| decode_summary(key, value.value()))
            .transpose()
            .map(|v| v.unwrap_or_default())
    };
    let mut global = read_summary(GLOBAL)?;
    let mut partition = read_summary(&partition_key)?;
    let mut reverse = write
        .open_table(REVERSE)
        .map_err(RedbPersistence::storage_error)?;
    if source.len().map_err(RedbPersistence::storage_error)? != global.entries
        || reverse.len().map_err(RedbPersistence::storage_error)? != global.entries
    {
        return Err(corruption(
            "membership companion cardinality disagrees with its maintained summary",
        ));
    }
    if let (Some(record), Some(bytes)) = (previous_record.as_ref(), previous.as_ref()) {
        adjust(&mut global, &key, bytes, record, false)?;
        adjust(&mut partition, &key, bytes, record, false)?;
        reverse
            .remove(reverse_key(record).as_slice())
            .map_err(RedbPersistence::storage_error)?;
        source
            .remove(key.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    if let (Some(record), Some(bytes)) = (next_record.as_ref(), encoded) {
        adjust(&mut global, &key, bytes, record, true)?;
        adjust(&mut partition, &key, bytes, record, true)?;
        reverse
            .insert(reverse_key(record).as_slice(), bytes)
            .map_err(RedbPersistence::storage_error)?;
        source
            .insert(key.as_slice(), bytes)
            .map_err(RedbPersistence::storage_error)?;
    }
    let global_bytes = encode_summary(GLOBAL, &global)?;
    let partition_bytes = encode_summary(&partition_key, &partition)?;
    summaries
        .insert(GLOBAL, global_bytes.as_slice())
        .map_err(RedbPersistence::storage_error)?;
    summaries
        .insert(partition_key.as_slice(), partition_bytes.as_slice())
        .map_err(RedbPersistence::storage_error)?;
    drop((source, reverse, summaries));
    Ok(())
}

/// Explicit bulk schema/repair publication already replaces the full forward
/// image in one transaction. Its derived companions are rebuilt in that same
/// transaction, retaining no stale renamed/dropped route keys.
pub(super) fn reset_in_write(write: &redb::WriteTransaction) -> Result<()> {
    for definition in [SOURCE, REVERSE, SUMMARY] {
        match write.delete_table(definition) {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        }
    }
    Ok(())
}

const SOURCE_ROOT: &str = "vector_membership_source_pages_root_v1";
const REVERSE_ROOT: &str = "vector_membership_reverse_pages_root_v1";
const SUMMARY_ROOT: &str = "vector_membership_summary_pages_root_v1";

pub(super) fn write_fingerprints(write: &redb::WriteTransaction) -> Result<()> {
    for (definition, key) in [
        (SOURCE, SOURCE_ROOT),
        (REVERSE, REVERSE_ROOT),
        (SUMMARY, SUMMARY_ROOT),
    ] {
        let fingerprint = write
            .table_root_fingerprint(definition)
            .map_err(RedbPersistence::storage_error)?
            .unwrap_or([0; 32]);
        write
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(RedbPersistence::storage_error)?
            .insert(key, fingerprint.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}

pub(super) fn validate_in_read(read: &redb::ReadTransaction) -> Result<()> {
    let metadata = read
        .open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    for (definition, key) in [
        (SOURCE, SOURCE_ROOT),
        (REVERSE, REVERSE_ROOT),
        (SUMMARY, SUMMARY_ROOT),
    ] {
        let actual = read
            .table_root_fingerprint(definition)
            .map_err(RedbPersistence::storage_error)?
            .unwrap_or([0; 32]);
        let expected = metadata
            .get(key)
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| corruption("required membership page root is missing"))?;
        if expected.value() != actual {
            return Err(corruption(
                "membership page root differs from its committed fingerprint",
            ));
        }
    }
    Ok(())
}

/// Authenticated decode of one ordered source companion for the paged builder.
pub(super) fn decode_source_member(
    key: &[u8],
    encoded: &[u8],
) -> Result<VectorPartitionMembershipRecord> {
    decode_member(key, encoded)
}

/// The route named by a partition prefix (`vector_partition_prefix`).
pub(super) fn partition_from_prefix(mut key: &[u8]) -> Result<VectorPartitionRef> {
    let table = std::str::from_utf8(component(&mut key)?)
        .map_err(|_| corruption("membership table name is not UTF-8"))?;
    let column = std::str::from_utf8(component(&mut key)?)
        .map_err(|_| corruption("membership column name is not UTF-8"))?;
    let partition = VectorPartitionKey::from_canonical_bytes(component(&mut key)?)
        .map_err(|_| corruption("membership partition key is invalid"))?;
    if !key.is_empty() {
        return Err(corruption("partition route key has trailing bytes"));
    }
    Ok(VectorPartitionRef::new(
        VectorIndexRef::new(table, column),
        partition,
    ))
}

/// Live membership count of one partition from its maintained summary.
pub(super) fn partition_live_entries(
    read: &redb::ReadTransaction,
    partition: &VectorPartitionRef,
) -> Result<usize> {
    let summaries = match read.open_table(SUMMARY) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let key = RedbPersistence::vector_partition_prefix(&partition.index, &partition.partition_key);
    let summary = summaries
        .get(key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .map(|value| decode_summary(&key, value.value()))
        .transpose()?
        .unwrap_or_default();
    usize::try_from(summary.live_entries).map_err(|_| corruption("membership live count overflow"))
}
