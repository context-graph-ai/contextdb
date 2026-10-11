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
/// Per-route occupancy intervals: the maximal, disjoint, non-adjacent
/// intervals of the union of the route's retained membership lifetimes
/// `[visible_from, deleted_tx or OPEN)`. Key: route prefix ++ start tx (BE);
/// value: end tx (BE, `OPEN_END` for open) ++ 8-byte keyed checksum.
pub(super) const OCCUPANCY: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_membership_occupancy_pages");
/// Routes whose occupancy rows await a rebuild after a membership removal.
/// While a route is listed here its hold answer streams the directory.
pub(super) const OCCUPANCY_DIRTY: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_membership_occupancy_dirty");
const OPEN_END: u64 = u64::MAX;
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
    if partition.partition_key.is_unpartitioned()
        || RedbPersistence::vector_schema_binding_in_write(write, &partition.index)?
            == VectorSchemaBinding::UNBOUND
    {
        // Undeclared raw generation carriers have no authoritative membership
        // directory. Only a declared partition can be retired by that directory.
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
    accountant: Option<crate::memory_accounting::AccountantHandle>,
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

/// Publish complete import coverage without disturbing a replacement's counters.
/// The raw frontier and native graph are committed in the same transaction.
pub(super) fn publish_native_tail_frontier(
    write: &redb::WriteTransaction,
    partition: &VectorPartitionRef,
    frontier: TxId,
    base_frontier: TxId,
) -> Result<()> {
    update_pending_summary(write, partition, |summary| {
        if summary.max_tx > frontier {
            return Err(corruption(
                "native tail frontier is behind committed membership",
            ));
        }
        if summary
            .base_pending
            .as_ref()
            .map(|counts| counts.covered_tx)
            != Some(base_frontier)
        {
            let base = [
                &summary.pending,
                summary
                    .candidate_pending
                    .as_ref()
                    .unwrap_or(&summary.pending),
            ]
            .into_iter()
            .find(|counts| counts.covered_tx == base_frontier)
            .cloned()
            .ok_or_else(|| corruption("native tail base frontier has no maintained counters"))?;
            summary.base_pending = Some(base);
        }
        summary.pending = PendingCounts {
            covered_tx: frontier,
            pending_inserts: 0,
            tombstones: 0,
        };
        Ok(())
    })
}

/// The complete replacement has absorbed the later upserts. Its retained
/// tombstones still count toward the ordinary consolidation policy.
pub(super) fn advance_native_candidate_frontier(
    write: &redb::WriteTransaction,
    partition: &VectorPartitionRef,
    previous: TxId,
    next: TxId,
) -> Result<()> {
    update_pending_summary(write, partition, |summary| {
        let candidate = summary
            .candidate_pending
            .as_mut()
            .filter(|counts| counts.covered_tx == previous)
            .ok_or_else(|| corruption("native candidate lost its maintained frontier"))?;
        if summary.max_tx > next || next < previous {
            return Err(corruption("native candidate has stale membership coverage"));
        }
        candidate.covered_tx = next;
        candidate.pending_inserts = 0;
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
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
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
        decode_vector_partition_record_payload_checked(VectorPartitionRecordKind::Membership, encoded)
            .map_err(|failure| failure.into_error(|reason| corruption(&reason)))?;
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
        decode_vector_partition_record_payload_checked(VectorPartitionRecordKind::Membership, encoded)
            .map_err(|failure| failure.into_error(|reason| corruption(&reason)))?;
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

// Existing reverse keys order owning row stamps rather than interval starts.
// Check only this row's history, keeping one charged decoded neighbour at a
// time. A row's history can require quadratic work over successive producers;
// unrelated rows are never scanned and no population identity set is built.
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
    accountant: Option<&crate::memory_accounting::AccountantHandle>,
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
                    accountant.clone(),
                    value.value().len().saturating_mul(16).saturating_add(4096),
                    "vector",
                    "validate_body_placement",
                    "Raise MEMORY_LIMIT to validate this vector placement history.",
                )
            })
            .transpose()?;
        let other: VectorPartitionMembershipRecord = decode_vector_partition_record_payload_checked(
            VectorPartitionRecordKind::Membership,
            value.value(),
        )
        .map_err(|failure| failure.into_error(|reason| corruption(&reason)))?;
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
    accountant: Option<crate::memory_accounting::AccountantHandle>,
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
    accountant: Option<crate::memory_accounting::AccountantHandle>,
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
    accountant: Option<crate::memory_accounting::AccountantHandle>,
    pending_transition: bool,
) -> Result<()> {
    let _call_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    // Authenticated previous/next records are temporary to this transition;
    // their payloads drop before this scope returns their reader credits.
    let _decode_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
    let _scratch = accountant
        .as_ref()
        .map(|accountant| {
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                accountant.clone(),
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
                decode_vector_partition_record_payload_checked(
                    VectorPartitionRecordKind::GenerationCatalog,
                    value.value(),
                )
                .map_err(|failure| failure.into_error(|reason| corruption(&reason)))?;
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
    super::vector_current_identity::transition::<CHECK_OVERLAP>(
        write, previous_record.as_ref(), next_record.as_ref(), accountant.as_ref(),
    )?;
    content_size::membership_transition(write, previous_record.as_ref(), next_record.as_ref())?;
    let boundary_before = partition.max_tx;
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
    drop((source, reverse));
    occupancy_transition(
        write,
        &partition_key,
        previous_record.as_ref(),
        next_record.as_ref(),
        boundary_before,
        &partition,
        accountant.as_ref(),
    )?;
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
    drop(summaries);
    Ok(())
}

fn membership_interval(record: &VectorPartitionMembershipRecord) -> Option<(u64, u64)> {
    let start = record.visible_from.0;
    let end = record.deleted_tx.map_or(OPEN_END, |tx| tx.0);
    (end > start).then_some((start, end))
}

fn occupancy_key(route: &[u8], start: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(route.len() + 8);
    key.extend_from_slice(route);
    key.extend_from_slice(&start.to_be_bytes());
    key
}

fn occupancy_checksum(key: &[u8], end: u64) -> [u8; 8] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"CDBVOI01");
    hash.update(key);
    hash.update(&end.to_be_bytes());
    hash.finalize().as_bytes()[..8].try_into().unwrap()
}

fn encode_occupancy(key: &[u8], end: u64) -> [u8; 16] {
    let mut value = [0u8; 16];
    value[..8].copy_from_slice(&end.to_be_bytes());
    value[8..].copy_from_slice(&occupancy_checksum(key, end));
    value
}

fn decode_occupancy(route: &[u8], key: &[u8], value: &[u8]) -> Result<(u64, u64)> {
    let start = key
        .strip_prefix(route)
        .filter(|start| start.len() == 8)
        .ok_or_else(|| corruption("occupancy interval key does not belong to its route"))?;
    let start = u64::from_be_bytes(start.try_into().unwrap());
    let value: &[u8; 16] = value
        .try_into()
        .map_err(|_| corruption("occupancy interval value has the wrong length"))?;
    let end = u64::from_be_bytes(value[..8].try_into().unwrap());
    if value[8..] != occupancy_checksum(key, end) || end <= start {
        return Err(corruption("occupancy interval checksum or bounds are invalid"));
    }
    Ok((start, end))
}

type OccupancyTable<'txn> = redb::Table<'txn, &'static [u8], &'static [u8]>;

/// The route's row with the greatest start at or below `at`.
fn occupancy_predecessor(
    table: &OccupancyTable<'_>,
    route: &[u8],
    at: u64,
) -> Result<Option<(u64, u64)>> {
    let lower = occupancy_key(route, 0);
    let upper = occupancy_key(route, at);
    let row = table
        .range::<&[u8]>(lower.as_slice()..=upper.as_slice())
        .map_err(metadata_error)?
        .next_back()
        .transpose()
        .map_err(metadata_error)?
        .map(|(key, value)| decode_occupancy(route, key.value(), value.value()))
        .transpose()?;
    Ok(row)
}

/// The route's first row with a start strictly above `after`.
fn occupancy_successor(
    table: &OccupancyTable<'_>,
    route: &[u8],
    after: u64,
) -> Result<Option<(u64, u64)>> {
    if after == OPEN_END {
        return Ok(None);
    }
    let lower = occupancy_key(route, after + 1);
    let upper = occupancy_key(route, OPEN_END);
    let row = table
        .range::<&[u8]>(lower.as_slice()..=upper.as_slice())
        .map_err(metadata_error)?
        .next()
        .transpose()
        .map_err(metadata_error)?
        .map(|(key, value)| decode_occupancy(route, key.value(), value.value()))
        .transpose()?;
    Ok(row)
}

fn put_occupancy(table: &mut OccupancyTable<'_>, route: &[u8], start: u64, end: u64) -> Result<()> {
    let key = occupancy_key(route, start);
    table
        .insert(key.as_slice(), encode_occupancy(&key, end).as_slice())
        .map_err(metadata_error)?;
    Ok(())
}

fn remove_occupancy(table: &mut OccupancyTable<'_>, route: &[u8], start: u64) -> Result<()> {
    table
        .remove(occupancy_key(route, start).as_slice())
        .map_err(metadata_error)?;
    Ok(())
}

/// Union-insert one lifetime: absorb the predecessor only if it overlaps or
/// touches the new start, then absorb following rows while they overlap or
/// touch the expanding merged end. Work is one descent plus absorbed rows.
fn occupancy_union_insert(
    table: &mut OccupancyTable<'_>,
    route: &[u8],
    start: u64,
    end: u64,
) -> Result<()> {
    let (mut merged_start, mut merged_end) = (start, end);
    if let Some((previous_start, previous_end)) = occupancy_predecessor(table, route, start)?
        && previous_end >= start
    {
        if previous_end >= end {
            return Ok(());
        }
        merged_start = previous_start;
        merged_end = merged_end.max(previous_end);
        remove_occupancy(table, route, previous_start)?;
    }
    while let Some((next_start, next_end)) = occupancy_successor(table, route, start)? {
        if next_start > merged_end {
            break;
        }
        merged_end = merged_end.max(next_end);
        remove_occupancy(table, route, next_start)?;
    }
    put_occupancy(table, route, merged_start, merged_end)
}

fn clear_route_occupancy(table: &mut OccupancyTable<'_>, route: &[u8]) -> Result<()> {
    loop {
        let Some((start, _)) = occupancy_successor_inclusive_first(table, route)? else {
            return Ok(());
        };
        remove_occupancy(table, route, start)?;
    }
}

fn occupancy_successor_inclusive_first(
    table: &OccupancyTable<'_>,
    route: &[u8],
) -> Result<Option<(u64, u64)>> {
    let lower = occupancy_key(route, 0);
    let upper = occupancy_key(route, OPEN_END);
    let row = table
        .range::<&[u8]>(lower.as_slice()..=upper.as_slice())
        .map_err(metadata_error)?
        .next()
        .transpose()
        .map_err(metadata_error)?
        .map(|(key, value)| decode_occupancy(route, key.value(), value.value()))
        .transpose()?;
    Ok(row)
}

/// Rebuild a route in arbitrary source order with one decoded interval at a
/// time. Durable union rows and the source cursor stay in this transaction;
/// writer scratch depends on the route key, never its membership count.
fn rebuild_route_occupancy(
    write: &redb::WriteTransaction,
    route: &[u8],
    _entries: u64,
    accountant: Option<&crate::memory_accounting::AccountantHandle>,
) -> Result<()> {
    let _call_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    let _buffer = accountant
        .map(|accountant| {
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                accountant.clone(),
                route.len().saturating_mul(4).saturating_add(4096),
                "vector",
                "rebuild_membership_occupancy",
                "Raise MEMORY_LIMIT to maintain this vector membership route.",
            )
        })
        .transpose()?;
    let source = write.open_table(SOURCE).map_err(metadata_error)?;
    let mut table = write.open_table(OCCUPANCY).map_err(metadata_error)?;
    clear_route_occupancy(&mut table, route)?;
    for entry in source.prefix_bounded(route).map_err(metadata_error)? {
        let (key, value) = entry.map_err(metadata_error)?;
        let interval = {
            let _row_scope = crate::read_image_memory::active_owner()?.row_owner()?.enter();
            let record = decode_member(key.value(), value.value())?;
            membership_interval(&record)
        };
        if let Some((start, end)) = interval {
            occupancy_union_insert(&mut table, route, start, end)?;
        }
    }
    drop((source, table));
    write
        .open_table(OCCUPANCY_DIRTY)
        .map_err(metadata_error)?
        .remove(route)
        .map_err(metadata_error)?;
    Ok(())
}

/// Keep the route's occupancy rows exactly equal to the union of its
/// retained membership lifetimes across one membership transition.
///
/// * Ordinary close at the publishing boundary (the closing tx is at or above
///   every stored boundary of the route): when no open membership remains,
///   the open last row ends at that tx; otherwise the union is unchanged.
/// * Any insertion, open or closed, received, loaded or replayed, and any
///   change that only widens a lifetime: union-insert the new lifetime.
/// * Any other change that can shrink the union (a historical close, a
///   lowered endpoint): rebuild this route from its own directory now.
/// * Removal: an emptied route drops its rows; otherwise the route is listed
///   dirty and the removing pass rebuilds it once (`settle_dirty_occupancy`).
fn occupancy_transition(
    write: &redb::WriteTransaction,
    route: &[u8],
    previous: Option<&VectorPartitionMembershipRecord>,
    next: Option<&VectorPartitionMembershipRecord>,
    boundary_before: TxId,
    partition: &MembershipSummary,
    accountant: Option<&crate::memory_accounting::AccountantHandle>,
) -> Result<()> {
    let previous_interval = previous.and_then(membership_interval);
    let next_interval = next.and_then(membership_interval);
    match (previous, next) {
        (None, None) => Ok(()),
        (None, Some(_)) => {
            let Some((start, end)) = next_interval else {
                return Ok(());
            };
            let mut table = write.open_table(OCCUPANCY).map_err(metadata_error)?;
            occupancy_union_insert(&mut table, route, start, end)
        }
        (Some(_), None) => {
            if partition.entries == 0 {
                let mut table = write.open_table(OCCUPANCY).map_err(metadata_error)?;
                clear_route_occupancy(&mut table, route)?;
                drop(table);
                write
                    .open_table(OCCUPANCY_DIRTY)
                    .map_err(metadata_error)?
                    .remove(route)
                    .map_err(metadata_error)?;
                return Ok(());
            }
            if previous_interval.is_none() {
                return Ok(());
            }
            write
                .open_table(OCCUPANCY_DIRTY)
                .map_err(metadata_error)?
                .insert(route, &[][..])
                .map_err(metadata_error)?;
            Ok(())
        }
        (Some(old), Some(new)) => {
            if previous_interval == next_interval {
                return Ok(());
            }
            let widened = match (previous_interval, next_interval) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some((old_start, old_end)), Some((new_start, new_end))) => {
                    new_start <= old_start && new_end >= old_end
                }
            };
            if widened {
                let Some((start, end)) = next_interval else {
                    return Ok(());
                };
                let mut table = write.open_table(OCCUPANCY).map_err(metadata_error)?;
                return occupancy_union_insert(&mut table, route, start, end);
            }
            let ordinary_close = old.deleted_tx.is_none()
                && old.visible_from == new.visible_from
                && new
                    .deleted_tx
                    .is_some_and(|closed| closed >= boundary_before);
            if ordinary_close {
                if partition.live_entries != 0 {
                    return Ok(());
                }
                let closed = new.deleted_tx.expect("ordinary close has an end").0;
                let mut table = write.open_table(OCCUPANCY).map_err(metadata_error)?;
                let Some((last_start, last_end)) = occupancy_predecessor(&table, route, OPEN_END)?
                else {
                    return Err(corruption("an open membership has no occupancy interval"));
                };
                if last_end != OPEN_END {
                    return Err(corruption("an open membership's occupancy interval is closed"));
                }
                if closed > last_start {
                    put_occupancy(&mut table, route, last_start, closed)?;
                } else {
                    remove_occupancy(&mut table, route, last_start)?;
                }
                return Ok(());
            }
            rebuild_route_occupancy(write, route, partition.entries, accountant)
        }
    }
}

/// Rebuild every route a removing pass listed dirty, once each.
pub(super) fn settle_dirty_occupancy(
    write: &redb::WriteTransaction,
    accountant: Option<crate::memory_accounting::AccountantHandle>,
) -> Result<()> {
    loop {
        let dirty = write.open_table(OCCUPANCY_DIRTY).map_err(metadata_error)?;
        let route = dirty
            .first()
            .map_err(metadata_error)?
            .map(|(key, _)| key.value().to_vec());
        drop(dirty);
        let Some(route) = route else {
            return Ok(());
        };
        let entries = {
            let summaries = write.open_table(SUMMARY).map_err(metadata_error)?;
            summaries
                .get_bounded(route.as_slice())
                .map_err(metadata_error)?
                .map(|value| decode_summary(&route, value.value()))
                .transpose()?
                .map_or(0, |summary| summary.entries)
        };
        rebuild_route_occupancy(write, &route, entries, accountant.as_ref())?;
    }
}

/// Whether any retained membership of `route` is visible to one of
/// `snapshots`: one predecessor lookup per snapshot, no membership read.
/// `None` while the route awaits a rebuild after a removal.
pub(super) fn occupancy_visible_at_any_in_read(
    read: &redb::ReadTransaction,
    route: &[u8],
    snapshots: &[SnapshotId],
) -> Result<Option<bool>> {
    match read.open_table(OCCUPANCY_DIRTY) {
        Ok(dirty) => {
            if dirty.get(route).map_err(metadata_error)?.is_some() {
                return Ok(None);
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(error) => return Err(metadata_error(error)),
    }
    let table = match read.open_table(OCCUPANCY) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Some(false)),
        Err(error) => return Err(metadata_error(error)),
    };
    for snapshot in snapshots {
        let lower = occupancy_key(route, 0);
        let upper = occupancy_key(route, snapshot.0);
        let row = table
            .range::<&[u8]>(lower.as_slice()..=upper.as_slice())
            .map_err(metadata_error)?
            .next_back()
            .transpose()
            .map_err(metadata_error)?;
        if let Some((key, value)) = row {
            let (_, end) = decode_occupancy(route, key.value(), value.value())?;
            if end > snapshot.0 {
                return Ok(Some(true));
            }
        }
    }
    Ok(Some(false))
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
    for definition in [
        SOURCE,
        REVERSE,
        SUMMARY,
        OCCUPANCY,
        OCCUPANCY_DIRTY,
        super::vector_current_identity::CURRENT,
    ] {
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
    accountant: Option<crate::memory_accounting::AccountantHandle>,
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
    accountant: Option<crate::memory_accounting::AccountantHandle>,
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
// The authoritative membership table and its compact open directory are bound
// to each committed root the same way, so a read-only open verifies them in
// O(1) instead of re-proving every membership.
const MEMBERSHIP_ROOT: &str = "vector_membership_authority_pages_root_v1";
const OPEN_DIRECTORY_ROOT: &str = "vector_open_directory_pages_root_v1";

pub(super) fn write_fingerprints(write: &redb::WriteTransaction) -> Result<()> {
    // Greenfield required authority: publish even the empty CURRENT table.
    drop(
        write
            .open_table(super::vector_current_identity::CURRENT)
            .map_err(metadata_error)?,
    );
    for (definition, key) in [
        (
            super::vector_current_identity::CURRENT,
            super::vector_current_identity::ROOT,
        ),
        (SOURCE, SOURCE_ROOT),
        (REVERSE, REVERSE_ROOT),
        (SUMMARY, SUMMARY_ROOT),
        (VECTOR_PARTITION_MEMBERSHIP_TABLE, MEMBERSHIP_ROOT),
        (VECTOR_OPEN_DIRECTORY_TABLE, OPEN_DIRECTORY_ROOT),
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
        (
            super::vector_current_identity::CURRENT,
            super::vector_current_identity::ROOT,
        ),
        (SOURCE, SOURCE_ROOT),
        (REVERSE, REVERSE_ROOT),
        (SUMMARY, SUMMARY_ROOT),
        (VECTOR_PARTITION_MEMBERSHIP_TABLE, MEMBERSHIP_ROOT),
        (VECTOR_OPEN_DIRECTORY_TABLE, OPEN_DIRECTORY_ROOT),
    ] {
        let actual = read
            .table_root_fingerprint(definition)
            .map_err(metadata_error)?;
        let expected = metadata
            .as_ref()
            .map(|table| table.get(key).map_err(metadata_error))
            .transpose()?
            .flatten();
        if key == super::vector_current_identity::ROOT
            && (expected.is_some()
                || read
                    .table_root_fingerprint(SOURCE)
                    .map_err(metadata_error)?
                    .is_some())
            && matches!(
                read.open_table(definition),
                Err(redb::TableError::TableDoesNotExist(_))
            )
        {
            return Err(corruption("required CURRENT identity table is missing"));
        }
        match expected {
            None if actual.is_none() => {}
            None => {
                return Err(corruption(
                    "membership page root has no committed fingerprint",
                ));
            }
            Some(expected) if expected.value() == actual.unwrap_or([0; 32]) => {}
            // A store without its membership directory takes the full-image
            // reconstruction route, which trusts nothing from the absent
            // tables; only a PRESENT table must match its fingerprint.
            Some(_) if actual.is_none() && (key == MEMBERSHIP_ROOT || key == OPEN_DIRECTORY_ROOT) => {}
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
        decode_vector_partition_record_payload_checked(VectorPartitionRecordKind::Membership, encoded)
            .map_err(|failure| failure.into_error(|reason| corruption(&reason)))?;
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

/// Check one maintained summary against its complete captured source prefix.
/// The high-water max_tx may exceed the remaining members after retirement.
pub(super) fn verify_summary_projection(
    read: &redb::ReadTransaction,
    prefix: &[u8],
    encoded: &[u8],
    owner: &mut contextdb_vector::VectorCopyAdmission,
    before: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    before()?;
    let expected = decode_summary(prefix, encoded)?;
    let mut actual = MembershipSummary::default();
    let source = open_optional_read_table(read, SOURCE)?;
    let mut cursor = source.as_ref().map(|table| table.prefix_bounded(prefix).map_err(metadata_error)).transpose()?;
    loop {
        before()?;
        let Some(item) = cursor.as_mut().and_then(Iterator::next) else { break; };
        let (key, value) = item.map_err(metadata_error)?;
        let mut window = owner.scratch();
        window.reserve(key.value().len().checked_add(value.value().len())
            .and_then(|n| n.checked_mul(6)).and_then(|n| n.checked_add(std::mem::size_of::<VectorPartitionMembershipRecord>()))
            .ok_or_else(|| corruption("summary projection window overflow"))?)?;
        let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, value.value())
            .map_err(|reason| corruption(&reason))?;
        window.reserve(vector_partition_route_native_capacity(payload).map_err(|reason| corruption(&reason))?
            .checked_mul(3).ok_or_else(|| corruption("summary route native capacity overflow"))?)?;
        let record = decode_source_member(key.value(), value.value())?;
        adjust(&mut actual, key.value(), value.value(), &record, true)?;
    }
    if expected.entries != actual.entries || expected.live_entries != actual.live_entries
        || expected.digest != actual.digest || expected.max_tx < actual.max_tx {
        return Err(corruption("summary disagrees with its complete captured source projection"));
    }
    before()?;
    Ok(())
}
