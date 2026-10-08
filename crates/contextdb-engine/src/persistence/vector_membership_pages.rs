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

#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct MembershipSummary {
    pub(super) entries: u64,
    pub(super) live_entries: u64,
    pub(super) max_tx: TxId,
    digest: [u8; 32],
    pub(super) pending: PendingCounts,
    pub(super) candidate_pending: Option<PendingCounts>,
    pub(super) base_pending: Option<PendingCounts>,
}

/// One catalog frontier and one in-progress frontier; independent of rows.
#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PendingCounts {
    pub(super) covered_tx: TxId,
    pub(super) pending_inserts: u64,
    pub(super) tombstones: u64,
}
impl PendingCounts {
    fn transition(
        &mut self,
        previous: Option<&VectorPartitionMembershipRecord>,
        next: Option<&VectorPartitionMembershipRecord>,
    ) -> Result<()> {
        let pending = |record: &VectorPartitionMembershipRecord| {
            u64::from(record.deleted_tx.is_none() && record.visible_from > self.covered_tx)
        };
        self.pending_inserts = self
            .pending_inserts
            .checked_sub(previous.map_or(0, pending))
            .and_then(|value| value.checked_add(next.map_or(0, pending)))
            .ok_or_else(|| corruption("pending insert count overflow/underflow"))?;
        let closed = |record: &VectorPartitionMembershipRecord| {
            record.deleted_tx.filter(|tx| *tx > self.covered_tx)
        };
        let before = previous.and_then(closed);
        let after = next.and_then(closed);
        if self.covered_tx == TxId(0) {
            // No graph exists yet; count retained closed memberships.
            self.tombstones = self
                .tombstones
                .checked_sub(u64::from(before.is_some()))
                .and_then(|value| value.checked_add(u64::from(after.is_some())))
                .ok_or_else(|| corruption("pending tombstone count overflow/underflow"))?;
        } else if after.is_some() && before != after {
            // Reclaiming a closed directory record does not retire the
            // saved graph's outstanding tombstone. Publication does that.
            self.tombstones = self
                .tombstones
                .checked_add(1)
                .ok_or_else(|| corruption("pending tombstone count overflow"))?;
        }
        Ok(())
    }
}

impl MembershipSummary {
    fn pending_transition(
        &mut self,
        previous: Option<&VectorPartitionMembershipRecord>,
        next: Option<&VectorPartitionMembershipRecord>,
    ) -> Result<()> {
        self.pending.transition(previous, next)?;
        for counts in self
            .candidate_pending
            .iter_mut()
            .chain(self.base_pending.iter_mut())
        {
            counts.transition(previous, next)?;
        }
        Ok(())
    }
}

fn update_pending_summary(
    write: &redb::WriteTransaction,
    partition: &VectorPartitionRef,
    update: impl FnOnce(&mut MembershipSummary) -> Result<()>,
) -> Result<()> {
    let key = RedbPersistence::vector_partition_prefix(&partition.index, &partition.partition_key);
    let mut table = write.open_table(SUMMARY).map_err(metadata_error)?;
    let mut summary = table
        .get_bounded(key.as_slice())
        .map_err(metadata_error)?
        .map(|value| decode_summary(&key, value.value()))
        .transpose()?;
    let mut summary = match summary.take() {
        Some(summary) => summary,
        None => {
            // An empty route can be published before its first membership.
            // Prove emptiness from the authoritative table in this transaction.
            let memberships = write
                .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                .map_err(metadata_error)?;
            if memberships
                .prefix_bounded(key.as_slice())
                .map_err(metadata_error)?
                .next()
                .transpose()
                .map_err(metadata_error)?
                .is_some()
            {
                return Err(corruption(
                    "membership summary is missing for a nonempty partition",
                ));
            }
            MembershipSummary::default()
        }
    };
    update(&mut summary)?;
    if summary.entries == 0 && !partition.partition_key.is_unpartitioned() {
        table.remove(key.as_slice()).map_err(metadata_error)?;
        drop(table);
        return write_fingerprints(write);
    }
    let encoded = encode_summary(&key, &summary)?;
    let unchanged = table
        .get_bounded(key.as_slice())
        .map_err(metadata_error)?
        .is_some_and(|old| old.value() == encoded.as_slice());
    if !unchanged {
        table
            .insert(key.as_slice(), encoded.as_slice())
            .map_err(metadata_error)?;
    }
    drop(table);
    write_fingerprints(write)
}

/// Check the authoritative route in the same transaction before installing
/// a generation catalog. A late candidate cannot resurrect an empty partition.
pub(super) fn is_empty_partitioned_route_in_write(
    write: &redb::WriteTransaction,
    partition: &VectorPartitionRef,
) -> Result<bool> {
    if partition.partition_key.is_unpartitioned() {
        return Ok(false);
    }
    let key = RedbPersistence::vector_partition_prefix(&partition.index, &partition.partition_key);
    let summaries = write.open_table(SUMMARY).map_err(metadata_error)?;
    if let Some(value) = summaries
        .get_bounded(key.as_slice())
        .map_err(metadata_error)?
    {
        return Ok(decode_summary(&key, value.value())?.entries == 0);
    }
    let source = write.open_table(SOURCE).map_err(metadata_error)?;
    if source
        .prefix_bounded(key.as_slice())
        .map_err(metadata_error)?
        .next()
        .transpose()
        .map_err(metadata_error)?
        .is_some()
    {
        return Err(corruption(
            "membership summary is missing for a nonempty partition",
        ));
    }
    Ok(true)
}

/// A vector declaration owns its empty unpartitioned route from this commit.
pub(super) fn ensure_declared_routes(
    write: &redb::WriteTransaction,
    table: &str,
    meta: &TableMeta,
    accountant: Option<Arc<crate::memory_accounting::MemoryAccountant>>,
) -> Result<()> {
    for column in &meta.columns {
        if matches!(column.column_type, ColumnType::Vector(_))
            && column
                .partition_key_columns
                .as_ref()
                .is_none_or(|keys| keys.is_empty())
        {
            let _memory = accountant
                .as_ref()
                .map(|accountant| {
                    crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        accountant.clone(),
                        table
                            .len()
                            .saturating_add(column.name.len())
                            .saturating_mul(32)
                            .saturating_add(4096),
                        "vector",
                        "declare_membership_summary",
                        "Raise MEMORY_LIMIT to declare this vector membership route.",
                    )
                })
                .transpose()?;
            let partition = VectorPartitionRef::new(
                VectorIndexRef::new(table, &column.name),
                VectorPartitionKey::unpartitioned(),
            );
            update_pending_summary(write, &partition, |_| Ok(()))?;
        }
    }
    Ok(())
}

/// Called under the commit gate at the captured current frontier, before any
/// later membership commit; all after-frontier counters start at zero.
pub(super) fn begin_pending_frontier(
    write: &redb::WriteTransaction,
    partition: &VectorPartitionRef,
    frontier: TxId,
) -> Result<()> {
    update_pending_summary(write, partition, |summary| {
        if summary
            .candidate_pending
            .as_ref()
            .is_some_and(|counts| counts.covered_tx == frontier)
        {
            return Ok(());
        }
        summary.candidate_pending = Some(PendingCounts {
            covered_tx: frontier,
            ..Default::default()
        });
        Ok(())
    })
}

pub(super) fn publish_pending_frontier(
    write: &redb::WriteTransaction,
    partition: &VectorPartitionRef,
    frontier: TxId,
    base_frontier: Option<TxId>,
) -> Result<Option<(usize, usize)>> {
    let is_change = base_frontier.is_some();
    let mut published = None;
    let selected_counts = |summary: &MembershipSummary| {
        let covered = base_frontier.unwrap_or(frontier);
        [
            &summary.pending,
            summary.base_pending.as_ref().unwrap_or(&summary.pending),
        ]
        .into_iter()
        .find(|counts| counts.covered_tx == covered)
        .map(|counts| (counts.pending_inserts as usize, counts.tombstones as usize))
    };
    update_pending_summary(write, partition, |summary| {
        if summary.pending.covered_tx == frontier {
            summary.candidate_pending = None;
            published = selected_counts(summary);
            return Ok(());
        }
        let candidate = match summary
            .candidate_pending
            .take()
            .filter(|counts| counts.covered_tx == frontier)
        {
            Some(candidate) => candidate,
            None if summary.entries == 0 => PendingCounts {
                covered_tx: frontier,
                ..Default::default()
            },
            None => {
                return Err(corruption(
                    "membership summary is missing for a nonempty partition",
                ));
            }
        };
        if !is_change
            || summary
                .base_pending
                .as_ref()
                .map(|counts| counts.covered_tx)
                != base_frontier
        {
            // Keep the predecessor's counters while its serving chain can
            // remain installed after a refused load. The first change over
            // a replacement base instead retains that base's own counters.
            summary.base_pending = Some(summary.pending.clone());
        }
        summary.pending = candidate;
        published = selected_counts(summary);
        Ok(())
    })?;
    Ok(published)
}

pub(super) fn metadata_error(error: impl Into<redb::Error>) -> Error {
    let error = error.into();
    let corrupted = match &error {
        redb::Error::Corrupted(reason) => Some(reason.clone()),
        _ => None,
    };
    // Consume the error so an admission payload survives the projection's
    // metadata adapter instead of being flattened through Display.
    let refusal = RedbPersistence::storage_error(error);
    match refusal {
        Error::Other(reason) => match corrupted {
            Some(reason) => corruption(&reason),
            None => Error::VectorMetadataIo {
                operation: "vector membership metadata".into(),
                reason,
            },
        },
        error => error,
    }
}

pub(super) fn typed_metadata_error(error: Error) -> Error {
    match error {
        Error::Other(reason) => Error::VectorMetadataIo {
            operation: "vector membership metadata".into(),
            reason,
        },
        error => error,
    }
}

/// Removing the last catalog changes the authoritative frontier in this same commit.
pub(super) fn reset_pending_frontier(
    write: &redb::WriteTransaction,
    partition: &VectorPartitionRef,
) -> Result<()> {
    update_pending_summary(write, partition, |summary| {
        summary.pending = PendingCounts {
            covered_tx: TxId(0),
            pending_inserts: summary.live_entries,
            tombstones: summary.entries - summary.live_entries,
        };
        summary.base_pending = None;
        summary.candidate_pending = None;
        Ok(())
    })
}

/// Absence is empty only when the authoritative route has no membership or catalog.
/// Each proof reads at most one prefix entry and one catalog key.
pub(super) fn read_partition_summary(
    read: &redb::ReadTransaction,
    partition: &VectorPartitionRef,
) -> Result<MembershipSummary> {
    validate_in_read(read)?;
    let key = RedbPersistence::vector_partition_prefix(&partition.index, &partition.partition_key);
    match read.open_byte_table_bounded(SUMMARY) {
        Ok(table) => {
            if let Some(value) = table.get_bounded(key.as_slice()).map_err(metadata_error)? {
                return decode_summary(&key, value.value());
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(error) => return Err(metadata_error(error)),
    }
    match read.open_byte_table_bounded(VECTOR_PARTITION_MEMBERSHIP_TABLE) {
        Ok(table) => {
            if table
                .prefix_bounded(key.as_slice())
                .map_err(metadata_error)?
                .next()
                .transpose()
                .map_err(metadata_error)?
                .is_some()
            {
                return Err(corruption(
                    "membership summary is missing for a nonempty partition",
                ));
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(error) => return Err(metadata_error(error)),
    }
    match read.open_byte_table_bounded(VECTOR_PARTITION_GENERATION_CATALOG_TABLE) {
        Ok(table) => {
            if table
                .get_bounded(key.as_slice())
                .map_err(metadata_error)?
                .is_some()
            {
                if partition.partition_key.is_unpartitioned() {
                    return Err(corruption(
                        "membership summary is missing beside a generation catalog",
                    ));
                }
                // Authoritative memberships were proven absent above. Reclaim
                // keeps this catalog while a retired generation chain is pinned;
                // its partitioned summary has already retired atomically.
                return Ok(MembershipSummary::default());
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(error) => return Err(metadata_error(error)),
    }
    Ok(MembershipSummary::default())
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
    let summary: MembershipSummary = RedbPersistence::decode_exact(&bytes[MAGIC.len()..end])
        .map_err(|error| corruption(&format!("membership summary payload is invalid: {error}")))?;
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

fn reverse_key_from_membership_key(mut key: &[u8]) -> Result<Vec<u8>> {
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
    let mut reverse = reverse_row_prefix(&VectorIndexRef::new(table, column), row);
    reverse.extend_from_slice(&created.0.to_be_bytes());
    reverse.extend_from_slice(&lsn.0.to_be_bytes());
    RedbPersistence::append_partition_key_component(&mut reverse, &partition.canonical_bytes());
    reverse.extend_from_slice(&vector_created.0.to_be_bytes());
    reverse.extend_from_slice(&vector_lsn.0.to_be_bytes());
    Ok(reverse)
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

fn decode_primary_member(
    membership_key: &[u8],
    encoded: &[u8],
) -> Result<VectorPartitionMembershipRecord> {
    let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, encoded)
        .map_err(|reason| corruption(&reason))?;
    prevalidate_vector_partition_metadata_routes(VectorPartitionRecordKind::Membership, payload)
        .map_err(|reason| corruption(&reason))?;
    let record: VectorPartitionMembershipRecord =
        decode_vector_partition_record_payload(VectorPartitionRecordKind::Membership, encoded)
            .map_err(|reason| corruption(&reason))?;
    if RedbPersistence::vector_partition_membership_key(&record) != membership_key {
        return Err(corruption(
            "one owning row version has conflicting vector placements",
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

fn decode_referenced_body_count(bytes: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| corruption("distinct vector body count has invalid size"))?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn referenced_body_count_in_read(read: &redb::ReadTransaction) -> Result<u64> {
    let metadata = read
        .open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .map_err(metadata_error)?;
    metadata
        .get(VECTOR_REFERENCED_BODY_COUNT_KEY)
        .map_err(metadata_error)?
        .map(|value| decode_referenced_body_count(value.value()))
        .transpose()
        .map(|count| count.unwrap_or(0))
}

pub(super) fn referenced_body_count_in_write(write: &redb::WriteTransaction) -> Result<u64> {
    let metadata = write
        .open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .map_err(metadata_error)?;
    metadata
        .get(VECTOR_REFERENCED_BODY_COUNT_KEY)
        .map_err(metadata_error)?
        .map(|value| decode_referenced_body_count(value.value()))
        .transpose()
        .map(|count| count.unwrap_or(0))
}

fn same_body(a: &VectorPartitionMembershipRecord, b: &VectorPartitionMembershipRecord) -> bool {
    a.index == b.index
        && a.row_id == b.row_id
        && a.vector_created_tx == b.vector_created_tx
        && a.vector_lsn == b.vector_lsn
}

fn lifetimes_overlap(
    a: &VectorPartitionMembershipRecord,
    b: &VectorPartitionMembershipRecord,
) -> bool {
    let nonempty =
        |r: &VectorPartitionMembershipRecord| r.deleted_tx.is_none_or(|end| r.visible_from < end);
    nonempty(a)
        && nonempty(b)
        && a.deleted_tx.is_none_or(|end| b.visible_from < end)
        && b.deleted_tx.is_none_or(|end| a.visible_from < end)
}

/// Existing reverse keys order owning row stamps rather than interval starts.
/// Check only this row's history, keeping one charged decoded neighbour at a
/// time. A row's history can require quadratic work over successive producers;
/// unrelated rows are never scanned and no population identity set is built.
#[cfg(test)]
thread_local! {
    static BODY_PLACEMENT_VALIDATION_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn body_placement_validation_calls() -> u64 {
    BODY_PLACEMENT_VALIDATION_CALLS.with(std::cell::Cell::get)
}

fn validate_body_placement_transition<const CHECK_OVERLAP: bool>(
    write: &redb::WriteTransaction,
    reverse: &redb::Table<'_, &[u8], &[u8]>,
    previous: Option<&VectorPartitionMembershipRecord>,
    next: Option<&VectorPartitionMembershipRecord>,
    accountant: Option<&Arc<crate::memory_accounting::MemoryAccountant>>,
) -> Result<u64> {
    #[cfg(test)]
    BODY_PLACEMENT_VALIDATION_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    let record = previous
        .or(next)
        .expect("a membership transition has a record");
    let prefix = reverse_row_prefix(&record.index, record.row_id);
    let own_key = reverse_key(record);
    let mut another_reference = false;
    for item in reverse
        .prefix_bounded(prefix.as_slice())
        .map_err(metadata_error)?
    {
        let (key, value) = item.map_err(metadata_error)?;
        if key.value() == own_key.as_slice() {
            continue;
        }
        let _window = accountant
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    Arc::clone(accountant),
                    value.value().len().saturating_mul(16).saturating_add(4096),
                    "vector",
                    "validate_body_placement",
                    "Raise MEMORY_LIMIT to validate this vector placement history.",
                )
            })
            .transpose()?;
        let other: VectorPartitionMembershipRecord = decode_vector_partition_record_payload(
            VectorPartitionRecordKind::Membership,
            value.value(),
        )
        .map_err(|reason| corruption(&reason))?;
        if reverse_key(&other).as_slice() != key.value() {
            return Err(corruption(
                "reverse membership key disagrees with its body identity",
            ));
        }
        if same_body(record, &other) {
            another_reference = true;
            if CHECK_OVERLAP && next.is_some_and(|next| lifetimes_overlap(next, &other)) {
                return Err(corruption(
                    "one vector body has overlapping placement lifetimes",
                ));
            }
        }
    }
    let count = referenced_body_count_in_write(write)?;
    match (previous.is_some(), next.is_some(), another_reference) {
        (false, true, false) => count.checked_add(1),
        (true, false, false) => count.checked_sub(1),
        _ => Some(count),
    }
    .ok_or_else(|| corruption("distinct vector body count overflow/underflow"))
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
    pending_transition: bool,
) -> Result<()> {
    publish_membership_impl::<true>(
        write,
        membership_key,
        encoded,
        accountant,
        pending_transition,
    )
}

/// Adversarial fixture publication: authenticate records and companions while
/// deliberately allowing overlapping placement lifetimes.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn publish_corrupt_store_membership_for_test(
    write: &redb::WriteTransaction,
    membership_key: &[u8],
    encoded: Option<&[u8]>,
    accountant: Option<Arc<crate::memory_accounting::MemoryAccountant>>,
    pending_transition: bool,
) -> Result<()> {
    publish_membership_impl::<false>(
        write,
        membership_key,
        encoded,
        accountant,
        pending_transition,
    )
}

fn publish_membership_impl<const CHECK_OVERLAP: bool>(
    write: &redb::WriteTransaction,
    membership_key: &[u8],
    encoded: Option<&[u8]>,
    accountant: Option<Arc<crate::memory_accounting::MemoryAccountant>>,
    pending_transition: bool,
) -> Result<()> {
    let _scratch = accountant
        .as_ref()
        .map(|accountant| {
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                Arc::clone(accountant),
                membership_key.len().saturating_mul(32).saturating_add(4096),
                "vector",
                "publish_membership_pages",
                "Raise MEMORY_LIMIT to publish this vector membership.",
            )
        })
        .transpose()?;
    // The primary identity does not contain the independent placement start.
    // Recover the previous interval from its exact authenticated reverse record;
    // derive the next interval only from the supplied authenticated record.
    let reverse_identity_key = reverse_key_from_membership_key(membership_key)?;
    let mut reverse = write.open_table(REVERSE).map_err(metadata_error)?;
    let previous = reverse
        .get_bounded(reverse_identity_key.as_slice())
        .map_err(metadata_error)?
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
        .map(|bytes| decode_primary_member(membership_key, bytes))
        .transpose()?;
    if previous_record
        .as_ref()
        .is_some_and(|record| reverse_key(record) != reverse_identity_key)
    {
        return Err(corruption(
            "reverse membership key disagrees with its body identity",
        ));
    }
    let next_record = encoded
        .map(|bytes| decode_primary_member(membership_key, bytes))
        .transpose()?;
    let previous_key = previous_record.as_ref().map(source_key);
    let next_key = next_record.as_ref().map(source_key);
    let mut source = write.open_table(SOURCE).map_err(metadata_error)?;
    if let (Some(key), Some(bytes)) = (previous_key.as_ref(), previous.as_deref()) {
        let stored = source
            .get_bounded(key.as_slice())
            .map_err(metadata_error)?
            .ok_or_else(|| corruption("reverse membership has no exact source companion"))?;
        if stored.value() != bytes {
            return Err(corruption(
                "membership source and reverse companions disagree",
            ));
        }
        decode_member(key, stored.value())?;
    }
    if let Some(key) = next_key.as_ref() {
        if previous_key.as_ref() != Some(key)
            && source
                .get_bounded(key.as_slice())
                .map_err(metadata_error)?
                .is_some()
        {
            return Err(corruption("membership source identity is already occupied"));
        }
        decode_member(key, encoded.expect("next source key has an encoded record"))?;
    }
    let route = previous_record.as_ref().or(next_record.as_ref());
    let Some(route) = route else {
        return Ok(());
    };
    let partition_key =
        RedbPersistence::vector_partition_prefix(&route.index, &route.partition_key);
    let mut summaries = write.open_table(SUMMARY).map_err(metadata_error)?;
    let read_summary = |key: &[u8]| -> Result<MembershipSummary> {
        if let Some(value) = summaries.get_bounded(key).map_err(metadata_error)? {
            return decode_summary(key, value.value());
        }
        if source
            .prefix_bounded(key)
            .map_err(metadata_error)?
            .next()
            .transpose()
            .map_err(metadata_error)?
            .is_some()
        {
            return Err(corruption(
                "membership summary is missing for a nonempty partition",
            ));
        }
        Ok(MembershipSummary::default())
    };
    let mut global = read_summary(GLOBAL)?;
    let existing_partition = summaries
        .get_bounded(partition_key.as_slice())
        .map_err(metadata_error)?
        .is_some();
    let mut partition = read_summary(&partition_key)?;
    if !existing_partition {
        let catalogs = write
            .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
            .map_err(metadata_error)?;
        if let Some(value) = catalogs
            .get_bounded(partition_key.as_slice())
            .map_err(metadata_error)?
        {
            let catalog: VectorPartitionGenerationCatalogRecord =
                decode_vector_partition_record_payload(
                    VectorPartitionRecordKind::GenerationCatalog,
                    value.value(),
                )
                .map_err(|reason| corruption(&reason))?;
            partition.pending.covered_tx = catalog.change.unwrap_or(catalog.base).covered_tx;
            partition.base_pending = catalog.change.map(|_| PendingCounts {
                covered_tx: catalog.base.covered_tx,
                ..Default::default()
            });
        }
    }
    if source.len().map_err(metadata_error)? != global.entries
        || reverse.len().map_err(metadata_error)? != global.entries
    {
        return Err(corruption(
            "membership companion cardinality disagrees with its maintained summary",
        ));
    }
    let distinct_bodies = validate_body_placement_transition::<CHECK_OVERLAP>(
        write,
        &reverse,
        previous_record.as_ref(),
        next_record.as_ref(),
        accountant.as_ref(),
    )?;
    content_size::membership_transition(write, previous_record.as_ref(), next_record.as_ref())?;
    if let (Some(record), Some(bytes), Some(key)) = (
        previous_record.as_ref(),
        previous.as_ref(),
        previous_key.as_ref(),
    ) {
        adjust(&mut global, key, bytes, record, false)?;
        adjust(&mut partition, key, bytes, record, false)?;
        reverse
            .remove(reverse_key(record).as_slice())
            .map_err(metadata_error)?;
        source.remove(key.as_slice()).map_err(metadata_error)?;
    }
    if let (Some(record), Some(bytes), Some(key)) =
        (next_record.as_ref(), encoded, next_key.as_ref())
    {
        adjust(&mut global, key, bytes, record, true)?;
        adjust(&mut partition, key, bytes, record, true)?;
        reverse
            .insert(reverse_key(record).as_slice(), bytes)
            .map_err(metadata_error)?;
        source
            .insert(key.as_slice(), bytes)
            .map_err(metadata_error)?;
    }
    write
        .open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .map_err(metadata_error)?
        .insert(
            VECTOR_REFERENCED_BODY_COUNT_KEY,
            distinct_bodies.to_be_bytes().as_slice(),
        )
        .map_err(metadata_error)?;
    if pending_transition {
        global.pending_transition(previous_record.as_ref(), next_record.as_ref())?;
        partition.pending_transition(previous_record.as_ref(), next_record.as_ref())?;
    }
    let global_bytes = encode_summary(GLOBAL, &global)?;
    summaries
        .insert(GLOBAL, global_bytes.as_slice())
        .map_err(metadata_error)?;
    if pending_transition && partition.entries == 0 && !route.partition_key.is_unpartitioned() {
        // Retire only the authoritative summary. Reclaim owns catalogs and
        // graph pages until every pinned chain is released. Rename suppresses
        // this retirement so its pending frontier can move with the route.
        summaries
            .remove(partition_key.as_slice())
            .map_err(metadata_error)?;
    } else {
        let partition_bytes = encode_summary(&partition_key, &partition)?;
        summaries
            .insert(partition_key.as_slice(), partition_bytes.as_slice())
            .map_err(metadata_error)?;
    }
    drop((source, reverse, summaries));
    Ok(())
}

/// Explicit bulk schema/repair publication already replaces the full forward
/// image in one transaction. Its derived companions are rebuilt in that same
/// transaction, retaining no stale renamed/dropped route keys.
pub(super) fn reset_in_write(write: &redb::WriteTransaction) -> Result<()> {
    content_size::reset_memberships(write)?;
    write
        .open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .map_err(metadata_error)?
        .remove(VECTOR_REFERENCED_BODY_COUNT_KEY)
        .map_err(metadata_error)?;
    for definition in [SOURCE, REVERSE, SUMMARY] {
        match write.delete_table(definition) {
            Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(metadata_error(error)),
        }
    }
    Ok(())
}

/// Schema rename preserves the existing serving frontiers. Membership moves
/// update commitments while leaving these counters untouched, then transfer
/// them here before this transaction publishes either renamed image.
pub(super) fn rename_pending_routes(
    write: &redb::WriteTransaction,
    from: &VectorIndexRef,
    to: &VectorIndexRef,
    accountant: Option<Arc<crate::memory_accounting::MemoryAccountant>>,
) -> Result<()> {
    let _prefix_memory = accountant
        .as_ref()
        .map(|accountant| {
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                accountant.clone(),
                from.table
                    .len()
                    .saturating_add(from.column.len())
                    .saturating_mul(4)
                    .saturating_add(128),
                "vector",
                "rename_membership_summary",
                "Raise MEMORY_LIMIT to rename this vector membership route.",
            )
        })
        .transpose()?;
    let mut prefix = Vec::new();
    RedbPersistence::append_partition_key_component(&mut prefix, from.table.as_bytes());
    RedbPersistence::append_partition_key_component(&mut prefix, from.column.as_bytes());
    let mut summaries = write.open_table(SUMMARY).map_err(metadata_error)?;
    loop {
        let next = {
            let mut entries = summaries
                .prefix_bounded(prefix.as_slice())
                .map_err(metadata_error)?;
            entries
                .next()
                .transpose()
                .map_err(metadata_error)?
                .map(|(key, value)| -> Result<_> {
                    let memory = accountant
                        .as_ref()
                        .map(|accountant| {
                            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                                accountant.clone(),
                                key.value()
                                    .len()
                                    .saturating_add(to.table.len())
                                    .saturating_add(to.column.len())
                                    .saturating_mul(32)
                                    .saturating_add(4096),
                                "vector",
                                "rename_membership_summary",
                                "Raise MEMORY_LIMIT to rename this vector membership route.",
                            )
                        })
                        .transpose()?;
                    let old = decode_summary(key.value(), value.value())?;
                    let route = partition_from_prefix(key.value())?;
                    let target = RedbPersistence::vector_partition_prefix(to, &route.partition_key);
                    Ok((
                        key.value().to_vec(),
                        target,
                        old,
                        memory,
                        route.partition_key.is_unpartitioned(),
                    ))
                })
                .transpose()?
        };
        let Some((key, target, old, _memory, unpartitioned)) = next else {
            break;
        };
        let mut renamed = summaries
            .get_bounded(target.as_slice())
            .map_err(metadata_error)?
            .map(|value| decode_summary(&target, value.value()))
            .transpose()?
            .unwrap_or_default();
        renamed.pending = old.pending;
        renamed.base_pending = old.base_pending;
        renamed.candidate_pending = old.candidate_pending;
        if renamed.entries == 0 && !unpartitioned {
            summaries
                .remove(target.as_slice())
                .map_err(metadata_error)?;
        } else {
            summaries
                .insert(
                    target.as_slice(),
                    encode_summary(&target, &renamed)?.as_slice(),
                )
                .map_err(metadata_error)?;
        }
        summaries.remove(key.as_slice()).map_err(metadata_error)?;
    }
    drop(summaries);
    write_fingerprints(write)
}

/// Removed declarations retain no route summary, including declared routes
/// that never acquired a membership. Walk one admitted key at a time.
pub(super) fn remove_routes_matching(
    write: &redb::WriteTransaction,
    table: &str,
    column: Option<&str>,
    accountant: Option<Arc<crate::memory_accounting::MemoryAccountant>>,
) -> Result<()> {
    let _prefix_memory = accountant
        .as_ref()
        .map(|accountant| {
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                accountant.clone(),
                table
                    .len()
                    .saturating_add(column.map_or(0, str::len))
                    .saturating_mul(4)
                    .saturating_add(128),
                "vector",
                "remove_membership_summary",
                "Raise MEMORY_LIMIT to remove this vector membership route.",
            )
        })
        .transpose()?;
    let mut prefix = Vec::new();
    RedbPersistence::append_partition_key_component(&mut prefix, table.as_bytes());
    if let Some(column) = column {
        RedbPersistence::append_partition_key_component(&mut prefix, column.as_bytes());
    }
    let mut summaries = write.open_table(SUMMARY).map_err(metadata_error)?;
    loop {
        let next = {
            let mut entries = summaries
                .prefix_bounded(prefix.as_slice())
                .map_err(metadata_error)?;
            entries
                .next()
                .transpose()
                .map_err(metadata_error)?
                .map(|(key, value)| -> Result<_> {
                    let memory = accountant
                        .as_ref()
                        .map(|accountant| {
                            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                                accountant.clone(),
                                key.value().len().saturating_mul(32).saturating_add(4096),
                                "vector",
                                "remove_membership_summary",
                                "Raise MEMORY_LIMIT to remove this vector membership route.",
                            )
                        })
                        .transpose()?;
                    decode_summary(key.value(), value.value())?;
                    Ok((key.value().to_vec(), memory))
                })
                .transpose()?
        };
        let Some((key, _memory)) = next else {
            break;
        };
        summaries.remove(key.as_slice()).map_err(metadata_error)?;
    }
    drop(summaries);
    write_fingerprints(write)
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
            .map_err(metadata_error)?
            .unwrap_or([0; 32]);
        let mut metadata = write
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(metadata_error)?;
        let unchanged = metadata
            .get(key)
            .map_err(metadata_error)?
            .is_some_and(|old| old.value() == fingerprint);
        if !unchanged {
            metadata
                .insert(key, fingerprint.as_slice())
                .map_err(metadata_error)?;
        }
    }
    Ok(())
}

pub(super) fn validate_in_read(read: &redb::ReadTransaction) -> Result<()> {
    let metadata = match read.open_table(CONTENT_IDENTITY_METADATA_TABLE) {
        Ok(table) => Some(table),
        Err(redb::TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(metadata_error(error)),
    };
    for (definition, key) in [
        (SOURCE, SOURCE_ROOT),
        (REVERSE, REVERSE_ROOT),
        (SUMMARY, SUMMARY_ROOT),
    ] {
        let actual = read
            .table_root_fingerprint(definition)
            .map_err(metadata_error)?;
        let expected = metadata
            .as_ref()
            .map(|table| table.get(key).map_err(metadata_error))
            .transpose()?
            .flatten();
        match expected {
            None if actual.is_none() => {}
            None => {
                return Err(corruption(
                    "membership page root has no committed fingerprint",
                ));
            }
            Some(expected) if expected.value() == actual.unwrap_or([0; 32]) => {}
            Some(_) => {
                return Err(corruption(
                    "membership page root differs from its committed fingerprint",
                ));
            }
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

/// Authenticated decode of one row-ordered reverse membership companion.
pub(super) fn decode_reverse_member(
    key: &[u8],
    encoded: &[u8],
) -> Result<VectorPartitionMembershipRecord> {
    let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, encoded)
        .map_err(|reason| corruption(&reason))?;
    prevalidate_vector_partition_metadata_routes(VectorPartitionRecordKind::Membership, payload)
        .map_err(|reason| corruption(&reason))?;
    let record: VectorPartitionMembershipRecord =
        decode_vector_partition_record_payload(VectorPartitionRecordKind::Membership, encoded)
            .map_err(|reason| corruption(&reason))?;
    if reverse_key(&record) != key {
        return Err(corruption(
            "reverse membership key disagrees with its authenticated identity",
        ));
    }
    Ok(record)
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

/// Decode one retained route only after its concrete component capacity and
/// text payload upper bound have been admitted by the executing owner.
pub(super) fn partition_from_prefix_admitted(
    encoded: &[u8],
    owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<VectorPartitionRef> {
    let mut remaining = encoded;
    let table = component(&mut remaining)?;
    let column = component(&mut remaining)?;
    let partition = component(&mut remaining)?;
    if !remaining.is_empty() {
        return Err(corruption("partition route key has trailing bytes"));
    }
    let count_bytes: [u8; 8] = partition
        .get(1..9)
        .ok_or_else(|| corruption("partition route component count is truncated"))?
        .try_into()
        .map_err(|_| corruption("partition route component count is invalid"))?;
    let count = usize::try_from(u64::from_be_bytes(count_bytes))
        .map_err(|_| corruption("partition route component count is too large"))?;
    // Every canonical component requires at least a tag and one value byte.
    // Refuse impossible count headers before the existing decoder reserves Vec.
    if count > partition.len().saturating_sub(9) / 2 {
        return Err(corruption(
            "partition route component count exceeds its encoding",
        ));
    }
    let component_bytes = count
        .checked_mul(std::mem::size_of::<contextdb_core::VectorPartitionComponent>())
        .ok_or_else(|| corruption("partition route component capacity overflow"))?;
    let bytes = std::mem::size_of::<VectorPartitionRef>()
        .checked_add(table.len())
        .and_then(|bytes| bytes.checked_add(column.len()))
        .and_then(|bytes| bytes.checked_add(component_bytes))
        .and_then(|bytes| bytes.checked_add(partition.len()))
        .ok_or_else(|| corruption("partition route copy capacity overflow"))?;
    owner.reserve(bytes)?;
    partition_from_prefix(encoded)
}

/// Live membership count of one partition from its maintained summary.
pub(super) fn partition_live_entries(
    read: &redb::ReadTransaction,
    partition: &VectorPartitionRef,
) -> Result<usize> {
    let summary = read_partition_summary(read, partition)?;
    usize::try_from(summary.live_entries).map_err(|_| corruption("membership live count overflow"))
}
