//! Canonical vector bodies and placement intervals on the received unit's
//! single unpublished write. Every native window retains the actual original.
use super::*;
use contextdb_core::ColumnDef;
use super::received_working::ReceivedWorkingWrite;
use std::ops::Bound;

pub(super) const WORKING_VECTOR_BODIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_bodies");
pub(super) const WORKING_VECTOR_MEMBERSHIPS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_memberships");
pub(super) const WORKING_VECTOR_REVERSE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_reverse");

// Same-write directory metadata. No partition identity survives in writer heap
// merely because a retained placement exists.
pub(super) const WORKING_VECTOR_SUMMARIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_summaries");
pub(super) const WORKING_VECTOR_COUNTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_counts");
pub(super) const WORKING_VECTOR_EMPTY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_empty");
pub(super) const WORKING_VECTOR_LINEAGE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_lineage");
pub(super) const WORKING_VECTOR_SUMMARY_TX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_summary_tx");

fn summary_corruption() -> Error {
    Error::StoreCorrupted { path: "received working vector summaries".into(),
        reason: "invalid maintained partition summary".into() }
}
fn summary_u64(bytes: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| summary_corruption())?))
}
pub(super) fn summary_decode(bytes: &[u8]) -> Result<RawVectorMembershipSummary> {
    if bytes.len() != 32 || bytes[24] > 1 || bytes[25..].iter().any(|byte| *byte != 0) { return Err(summary_corruption()); }
    let entries = usize::try_from(summary_u64(&bytes[..8])?).map_err(|_| summary_corruption())?;
    let live_entries = usize::try_from(summary_u64(&bytes[8..16])?).map_err(|_| summary_corruption())?;
    if entries == 0 || live_entries > entries || (bytes[24] != 0 && live_entries != 0) { return Err(summary_corruption()); }
    Ok(RawVectorMembershipSummary { entries, live_entries, max_tx: TxId(summary_u64(&bytes[16..24])?) })
}
fn summary_encode(summary: RawVectorMembershipSummary, historical_only: bool) -> Result<[u8; 32]> {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&u64::try_from(summary.entries).map_err(|_| summary_corruption())?.to_be_bytes());
    bytes[8..16].copy_from_slice(&u64::try_from(summary.live_entries).map_err(|_| summary_corruption())?.to_be_bytes());
    bytes[16..24].copy_from_slice(&summary.max_tx.0.to_be_bytes());
    bytes[24] = u8::from(historical_only);
    Ok(bytes)
}

/// All key allocations precede the native guards. The histogram makes max_tx
/// exact even when replacing a previously closed interval with an earlier end.
fn update_partition_summary(write: &redb::WriteTransaction, key: &[u8],
    tx_lower: &mut [u8], tx_upper: &[u8],
    before: Option<(bool, TxId)>, after: &VectorPartitionMembershipRecord, historical_hint: bool) -> Result<()> {
    let partition = key.get(..key.len().checked_sub(40).ok_or_else(summary_corruption)?)
        .ok_or_else(summary_corruption)?;
    let mut names = key;
    component(&mut names).ok_or_else(summary_corruption)?;
    component(&mut names).ok_or_else(summary_corruption)?;
    let index = &key[..key.len() - names.len()];
    let mut summaries = write.open_table(WORKING_VECTOR_SUMMARIES).map_err(RedbPersistence::storage_error)?;
    let previous = summaries.get(partition).map_err(RedbPersistence::storage_error)?
        .map(|value| Ok::<_, Error>((summary_decode(value.value())?, value.value()[24] != 0))).transpose()?;
    if before.is_some() && previous.is_none() { return Err(summary_corruption()); }
    let mut summary = previous.map(|value| value.0).unwrap_or_default();
    let mut histogram = write.open_table(WORKING_VECTOR_SUMMARY_TX).map_err(RedbPersistence::storage_error)?;
    if let Some((live, tx)) = before {
        summary.entries = summary.entries.checked_sub(1).ok_or_else(summary_corruption)?;
        if live { summary.live_entries = summary.live_entries.checked_sub(1).ok_or_else(summary_corruption)?; }
        tx_lower[partition.len()..].copy_from_slice(&tx.0.to_be_bytes());
        let count = histogram.get(&*tx_lower).map_err(RedbPersistence::storage_error)?
            .map(|value| summary_u64(value.value())).transpose()?.ok_or_else(summary_corruption)?;
        if count == 1 { histogram.remove(&*tx_lower).map_err(RedbPersistence::storage_error)?; }
        else { let count = count.checked_sub(1).ok_or_else(summary_corruption)?.to_be_bytes();
            histogram.insert(&*tx_lower, count.as_slice()).map_err(RedbPersistence::storage_error)?; }
    }
    summary.entries = summary.entries.checked_add(1).ok_or_else(summary_corruption)?;
    if after.deleted_tx.is_none() { summary.live_entries = summary.live_entries.checked_add(1).ok_or_else(summary_corruption)?; }
    let tx = after.visible_from.max(after.deleted_tx.unwrap_or(TxId(0)));
    tx_lower[partition.len()..].copy_from_slice(&tx.0.to_be_bytes());
    let count = histogram.get(&*tx_lower).map_err(RedbPersistence::storage_error)?
        .map(|value| summary_u64(value.value())).transpose()?.unwrap_or(0)
        .checked_add(1).ok_or_else(summary_corruption)?.to_be_bytes();
    histogram.insert(&*tx_lower, count.as_slice()).map_err(RedbPersistence::storage_error)?;
    tx_lower[partition.len()..].fill(0);
    {
        let mut range = histogram.range::<&[u8]>((Bound::Included(&*tx_lower), Bound::Included(tx_upper)))
            .map_err(RedbPersistence::storage_error)?;
        let (last, _) = range.next_back().transpose().map_err(RedbPersistence::storage_error)?
            .ok_or_else(summary_corruption)?;
        if last.value().len() != partition.len() + 8 || !last.value().starts_with(partition) {
            return Err(summary_corruption());
        }
        summary.max_tx = TxId(summary_u64(&last.value()[partition.len()..])?);
    }
    let historical_only = previous.map_or(historical_hint, |previous| previous.1 && historical_hint)
        && summary.live_entries == 0;
    summaries.insert(partition, summary_encode(summary, historical_only)?.as_slice()).map_err(RedbPersistence::storage_error)?;
    update_partition_counts(write, index, previous, (summary, historical_only))?;
    update_empty_partition(write, partition, summary, historical_only)?;
    Ok(())
}

fn update_empty_partition(write: &redb::WriteTransaction, key: &[u8],
    summary: RawVectorMembershipSummary, historical_only: bool) -> Result<()> {
    let mut empty = write.open_table(WORKING_VECTOR_EMPTY).map_err(RedbPersistence::storage_error)?;
    if !historical_only && summary.live_entries == 0 {
        empty.insert(key, summary_encode(summary, false)?.as_slice()).map_err(RedbPersistence::storage_error)?;
    } else { empty.remove(key).map_err(RedbPersistence::storage_error)?; }
    Ok(())
}

fn update_partition_counts(write: &redb::WriteTransaction, index: &[u8],
    previous: Option<(RawVectorMembershipSummary, bool)>, next: (RawVectorMembershipSummary, bool)) -> Result<()> {
    let mut counts = write.open_table(WORKING_VECTOR_COUNTS).map_err(RedbPersistence::storage_error)?;
    let mut totals = match counts.get(index).map_err(RedbPersistence::storage_error)? {
        None => [0u64; 6],
        Some(value) => {
            let bytes = value.value();
            if bytes.len() != 48 { return Err(summary_corruption()); }
            [summary_u64(&bytes[..8])?, summary_u64(&bytes[8..16])?, summary_u64(&bytes[16..24])?, summary_u64(&bytes[24..32])?, summary_u64(&bytes[32..40])?, summary_u64(&bytes[40..])?]
        },
    };
    let flags = |(summary, historical): (RawVectorMembershipSummary, bool)|
        [true, !historical && summary.live_entries != 0,
            !historical && summary.entries > summary.live_entries, !historical];
    let before = previous.map_or([false; 4], flags);
    let after = flags(next);
    for ((count, before), after) in totals.iter_mut().zip(before).zip(after) {
        if before && !after { *count = count.checked_sub(1).ok_or_else(summary_corruption)?; }
        if !before && after { *count = count.checked_add(1).ok_or_else(summary_corruption)?; }
    }
    let previous_summary = previous.map(|value| value.0).unwrap_or_default();
    totals[4] = totals[4].checked_sub(u64::try_from(previous_summary.live_entries).map_err(|_| summary_corruption())?)
        .and_then(|n| n.checked_add(u64::try_from(next.0.live_entries).ok()?)).ok_or_else(summary_corruption)?;
    totals[5] = totals[5].checked_sub(u64::try_from(previous_summary.entries).map_err(|_| summary_corruption())?)
        .and_then(|n| n.checked_add(u64::try_from(next.0.entries).ok()?)).ok_or_else(summary_corruption)?;
    if totals[1] > totals[3] || totals[2] > totals[3] || totals[3] > totals[0] || totals[4] > totals[5] { return Err(summary_corruption()); }
    let mut encoded = [0u8; 48];
    for (bytes, value) in encoded.chunks_exact_mut(8).zip(totals) { bytes.copy_from_slice(&value.to_be_bytes()); }
    counts.insert(index, encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
    Ok(())
}

pub(super) fn decode_column_counts(bytes: &[u8]) -> Result<[usize; 6]> {
    if bytes.len() != 48 { return Err(summary_corruption()); }
    let mut counts = [0; 6];
    for (count, bytes) in counts.iter_mut().zip(bytes.chunks_exact(8)) {
        *count = usize::try_from(summary_u64(bytes)?).map_err(|_| summary_corruption())?;
    }
    if counts[1] > counts[3] || counts[2] > counts[3] || counts[3] > counts[0] || counts[4] > counts[5] { return Err(summary_corruption()); }
    Ok(counts)
}
fn index_prefix_original(frame: &OriginalPersistenceFrame<'_>, index: &VectorIndexRef) -> Result<Vec<u8>> {
    let length = index.table.len().checked_add(index.column.len()).and_then(|n| n.checked_add(16)).ok_or_else(summary_corruption)?;
    frame.reserve(length)?;
    let mut key = Vec::with_capacity(length);
    for name in [&index.table, &index.column] {
        key.extend_from_slice(&(name.len() as u64).to_be_bytes()); key.extend_from_slice(name.as_bytes());
    }
    Ok(key)
}
impl ReceivedWorkingWrite {
    fn vector_column_counts_original(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        index: &VectorIndexRef) -> Result<[usize; 6]> {
        self.require_original(original)?;
        let frame = OriginalPersistenceFrame::new(original)?;
        let key = index_prefix_original(&frame, index)?;
        self.with_write(|write| {
            let counts = write.open_table(WORKING_VECTOR_COUNTS).map_err(RedbPersistence::storage_error)?;
            counts.get(key.as_slice()).map_err(RedbPersistence::storage_error)?
                .map(|value| decode_column_counts(value.value())).transpose().map(|value| value.unwrap_or([0; 6]))
        })
    }
}

/// Decode only an admitted typed summary identity, with exact framing checks.
pub(super) fn decode_summary_partition(mut key: &[u8]) -> Result<VectorPartitionKey> {
    use contextdb_core::VectorPartitionComponent;
    component(&mut key).ok_or_else(summary_corruption)?;
    component(&mut key).ok_or_else(summary_corruption)?;
    let mut bytes = component(&mut key).ok_or_else(summary_corruption)?;
    if !key.is_empty() || !consume(&mut bytes, &[1]) { return Err(summary_corruption()); }
    let count = usize::try_from(summary_u64(bytes.get(..8).ok_or_else(summary_corruption)?)?)
        .map_err(|_| summary_corruption())?;
    bytes = &bytes[8..];
    if count > bytes.len() / 2 { return Err(summary_corruption()); }
    let controls = std::alloc::Layout::array::<VectorPartitionComponent>(count)
        .map_err(|_| summary_corruption())?.size();
    crate::read_image_memory::reserve(controls.checked_add(bytes.len()).ok_or_else(summary_corruption)?)?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let tag = *bytes.first().ok_or_else(summary_corruption)?;
        bytes = &bytes[1..];
        let value = match tag {
            0 => {
                let value = uuid::Uuid::from_slice(bytes.get(..16).ok_or_else(summary_corruption)?).map_err(|_| summary_corruption())?;
                bytes = &bytes[16..]; VectorPartitionComponent::Uuid(value)
            },
            1 => VectorPartitionComponent::Text(std::str::from_utf8(component(&mut bytes).ok_or_else(summary_corruption)?)
                .map_err(|_| summary_corruption())?.to_owned()),
            3 => {
                let value = *bytes.first().ok_or_else(summary_corruption)?;
                if value > 1 { return Err(summary_corruption()); }
                bytes = &bytes[1..]; VectorPartitionComponent::Boolean(value != 0)
            },
            2 | 4 | 5 => {
                let value = summary_u64(bytes.get(..8).ok_or_else(summary_corruption)?)?;
                bytes = &bytes[8..];
                match tag {
                    2 => VectorPartitionComponent::Integer(value as i64),
                    4 => VectorPartitionComponent::Timestamp(value as i64),
                    _ => VectorPartitionComponent::TxId(TxId(value)),
                }
            },
            _ => return Err(summary_corruption()),
        };
        values.push(value);
    }
    if !bytes.is_empty() { return Err(summary_corruption()); }
    Ok(VectorPartitionKey::from_components(values))
}

fn summary_key_original(frame: &OriginalPersistenceFrame<'_>, partition: &VectorPartitionRef) -> Result<Vec<u8>> {
    let partition_length = partition_length(&partition.partition_key)?;
    let length = partition.index.table.len().checked_add(partition.index.column.len())
        .and_then(|n| n.checked_add(24)).and_then(|n| n.checked_add(partition_length)).ok_or_else(summary_corruption)?;
    frame.reserve(length)?;
    let mut key = Vec::with_capacity(length);
    for name in [&partition.index.table, &partition.index.column] {
        key.extend_from_slice(&(name.len() as u64).to_be_bytes()); key.extend_from_slice(name.as_bytes());
    }
    key.extend_from_slice(&(partition_length as u64).to_be_bytes()); append_partition(&mut key, &partition.partition_key);
    Ok(key)
}

fn summary_bounds(frame: &OriginalPersistenceFrame<'_>, key: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let prefix = key.get(..key.len().checked_sub(32).ok_or_else(summary_corruption)?)
        .ok_or_else(summary_corruption)?;
    let mut lower = frame.copy_bytes(prefix)?;
    let mut upper = frame.copy_bytes(prefix)?;
    lower[prefix.len()-8..].fill(0);
    upper[prefix.len()-8..].fill(255);
    Ok((lower, upper))
}

pub(crate) struct WorkingVectorMembership {
    record: VectorPartitionMembershipRecord,
    key: Vec<u8>,
    encoded: Vec<u8>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl WorkingVectorMembership {
    pub(crate) fn value(&self) -> &VectorPartitionMembershipRecord { &self.record }
    pub(crate) fn position(&self) -> &[u8] { &self.key }
    pub(crate) fn encoded(&self) -> &[u8] { &self.encoded }
}
impl Drop for WorkingVectorMembership { fn drop(&mut self) {} }

pub(crate) struct WorkingVectorBody {
    entry: VectorEntry,
    key: Vec<u8>,
    encoded: Vec<u8>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl WorkingVectorBody {
    pub(crate) fn value(&self) -> &VectorEntry { &self.entry }
    pub(crate) fn position(&self) -> &[u8] { &self.key }
    pub(crate) fn encoded(&self) -> &[u8] { &self.encoded }
}
impl Drop for WorkingVectorBody { fn drop(&mut self) {} }

struct VectorPosition {
    key: Vec<u8>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}

impl ReceivedWorkingWrite {
    pub(crate) fn vectors_prepared(&self) -> Result<bool> {
        if !self.vector_is_ready() { return Ok(false); }
        self.with_write(|write| Ok(write.table_exists(WORKING_VECTOR_BODIES).map_err(RedbPersistence::storage_error)?
            && write.table_exists(WORKING_VECTOR_MEMBERSHIPS).map_err(RedbPersistence::storage_error)?
            && write.table_exists(WORKING_VECTOR_REVERSE).map_err(RedbPersistence::storage_error)?
            && write.table_exists(WORKING_VECTOR_SUMMARIES).map_err(RedbPersistence::storage_error)?
            && write.table_exists(WORKING_VECTOR_COUNTS).map_err(RedbPersistence::storage_error)?
            && write.table_exists(WORKING_VECTOR_SUMMARY_TX).map_err(RedbPersistence::storage_error)?
            && write.table_exists(WORKING_VECTOR_EMPTY).map_err(RedbPersistence::storage_error)?))
    }

    pub(crate) fn prepare_vectors(
        &self, store: &RedbPersistence,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        root: WriterSnapshotHandle,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.require_original(original)?;
        self.require_store(store)?;
        root.require_working_store(store)?;
        if self.vectors_prepared()? { return Ok(()); }
        for destination in [WORKING_VECTOR_BODIES, WORKING_VECTOR_MEMBERSHIPS, WORKING_VECTOR_REVERSE,
            WORKING_VECTOR_SUMMARIES, WORKING_VECTOR_COUNTS, WORKING_VECTOR_SUMMARY_TX, WORKING_VECTOR_EMPTY,
            WORKING_VECTOR_LINEAGE] {
            let frame = OriginalPersistenceFrame::new(original)?;
            frame.reserve(destination.name().len())?;
            let name = crate::memory_accounting::preparation_copy_string(destination.name())?;
            self.keep_scratch_name(name, frame.into_reservation())?;
            self.with_write(|write| {
                match write.delete_table(destination) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {},
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                }
                write.open_table(destination).map(|_| ()).map_err(RedbPersistence::storage_error)
            })?;
        }
        let mut memberships = root.cursor(writer_source::SourceWindowOwner::Read(read_memory.clone()),
            VECTOR_PARTITION_MEMBERSHIP_TABLE.name(), writer_source::WriterCursorFamily::Bytes,
            Bound::Unbounded, Bound::Unbounded, false)?;
        while let Some(member) = memberships.next_membership_for_working()? {
            if !root.captured_table_metadata().any(|(table, meta)| table == member.value.0.index.table
                && meta.columns.iter().any(|column| column.name == member.value.0.index.column
                    && matches!(column.column_type, ColumnType::Vector(_)))) { continue; }
            let body = root.export_vector_raw_for_working(
                writer_source::SourceWindowOwner::Read(read_memory.clone()), &member.value.0)?;
            let reverse_frame = OriginalPersistenceFrame::new(original)?;
            let reverse_key = reverse_membership_key_original(&reverse_frame, &member.value.0)?;
            let (mut summary_lower, summary_upper) = summary_bounds(&reverse_frame, member.position())?;
            // The two source windows carry the exact root-authenticated key
            // and envelope. No complete body/placement directory is retained.
            self.with_write(|write| {
                update_partition_summary(write, member.position(), &mut summary_lower, &summary_upper, None, &member.value.0, member.value.0.deleted_tx.is_some())?;
                write.open_table(WORKING_VECTOR_BODIES).map_err(RedbPersistence::storage_error)?
                    .insert(body.position(), body.value.as_slice()).map_err(RedbPersistence::storage_error)?;
                write.open_table(WORKING_VECTOR_MEMBERSHIPS).map_err(RedbPersistence::storage_error)?
                    .insert(member.position(), member.value.1.as_slice()).map_err(RedbPersistence::storage_error)?;
                write.open_table(WORKING_VECTOR_REVERSE).map_err(RedbPersistence::storage_error)?
                    .insert(reverse_key.as_slice(), member.value.1.as_slice()).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
        }
        // Current declarations only: retain exact initial index lineage in
        // the same private write, never a resident partition inventory.
        for (table, meta) in root.captured_table_metadata() {
            for column in &meta.columns {
                if !matches!(column.column_type, ColumnType::Vector(_)) { continue; }
                let frame = OriginalPersistenceFrame::new(original)?;
                let index = VectorIndexRef { table: frame.copy_text(table)?, column: frame.copy_text(&column.name)? };
                let key = index_prefix_original(&frame, &index)?;
                let encoded = frame.encode(&index)?;
                self.with_write(|write| {
                    write.open_table(WORKING_VECTOR_LINEAGE).map_err(RedbPersistence::storage_error)?
                        .insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                    Ok(())
                })?;
            }
        }
        self.vector_became_ready();
        Ok(())
    }

    pub(crate) fn next_vector_membership(
        &self, original: &crate::memory_accounting::OwnedMemoryReservation,
        lower: Bound<&[u8]>, upper: Bound<&[u8]>,
    ) -> Result<Option<WorkingVectorMembership>> {
        self.require_original(original)?;
        if !self.vectors_prepared()? { return Err(Error::Other("received vectors have no completed private image".into())); }
        self.with_write(|write| next_membership_in_write(original, write, lower, upper))
    }

    pub(crate) fn vector_body_for_membership(
        &self, original: &crate::memory_accounting::OwnedMemoryReservation,
        member: &VectorPartitionMembershipRecord,
    ) -> Result<WorkingVectorBody> {
        self.require_original(original)?;
        if !self.vectors_prepared()? { return Err(Error::Other("received vectors have no completed private image".into())); }
        let frame = OriginalPersistenceFrame::new(original)?;
        let capacity = member.index.table.len().checked_add(member.index.column.len())
            .and_then(|n| n.checked_add(34)).ok_or_else(|| Error::Other("working vector key overflow".into()))?;
        frame.reserve(capacity)?;
        let key = RedbPersistence::vector_identity_key(&member.index, member.row_id, member.vector_created_tx, member.vector_lsn);
        if key.capacity() > capacity { return Err(Error::Other("working vector key exceeds its original admission".into())); }
        let body = self.with_write(|write| next_body_in_write(original, write,
            Bound::Included(key.as_slice()), Bound::Included(key.as_slice())))?
            .ok_or_else(|| Error::StoreCorrupted { path: "received working vectors".into(),
                reason: "placement lost its canonical body".into() })?;
        if body.entry.index != member.index || body.entry.row_id != member.row_id
            || body.entry.created_tx != member.vector_created_tx || body.entry.lsn != member.vector_lsn {
            return Err(Error::StoreCorrupted { path: "received working vectors".into(),
                reason: "placement names a different canonical body".into() });
        }
        Ok(body)
    }

    pub(crate) fn visit_vector_bodies_original(
        &self, original: &crate::memory_accounting::OwnedMemoryReservation,
        mut visit: impl FnMut(&VectorEntry, &[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.require_original(original)?;
        if !self.vectors_prepared()? { return Err(Error::Other("received vectors have no completed private image".into())); }
        let mut after = None::<VectorPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.key.as_slice()));
            let Some(body) = self.with_write(|write| next_body_in_write(original, write, lower, Bound::Unbounded))? else { break; };
            let frame = OriginalPersistenceFrame::new(original)?;
            let key = frame.copy_bytes(body.position())?;
            after = Some(VectorPosition { key, _owner: frame.into_reservation() });
            visit(body.value(), body.position(), body.encoded())?;
        }
        Ok(())
    }

    pub(crate) fn visit_vector_memberships_original(
        &self, original: &crate::memory_accounting::OwnedMemoryReservation,
        mut visit: impl FnMut(&VectorPartitionMembershipRecord, &[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut after = None::<VectorPosition>;
        loop {
            let lower = after.as_ref().map_or(Bound::Unbounded, |p| Bound::Excluded(p.key.as_slice()));
            let Some(member) = self.next_vector_membership(original, lower, Bound::Unbounded)? else { break; };
            let frame = OriginalPersistenceFrame::new(original)?;
            let key = frame.copy_bytes(member.position())?;
            after = Some(VectorPosition { key, _owner: frame.into_reservation() });
            visit(member.value(), member.position(), member.encoded())?;
        }
        Ok(())
    }
}

fn next_membership_in_write(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    write: &redb::WriteTransaction,
    lower: Bound<&[u8]>, upper: Bound<&[u8]>,
) -> Result<Option<WorkingVectorMembership>> {
    let frame = OriginalPersistenceFrame::new(original)?;
    let (record, key, encoded) = {
        let table = write.open_table(WORKING_VECTOR_MEMBERSHIPS).map_err(RedbPersistence::storage_error)?;
        let mut range = table.range::<&[u8]>((lower, upper)).map_err(RedbPersistence::storage_error)?;
        let Some((key, body)) = range.next().transpose().map_err(RedbPersistence::storage_error)? else { return Ok(None); };
        let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, body.value())
            .map_err(|reason| Error::StoreCorrupted { path: "received working memberships".into(), reason })?;
        let record = frame.decode::<VectorPartitionMembershipRecord>(payload)?;
        if !membership_key_matches(key.value(), &record) {
            return Err(Error::StoreCorrupted { path: "received working memberships".into(), reason: "placement key and body disagree".into() });
        }
        (record, frame.copy_bytes(key.value())?, frame.copy_bytes(body.value())?)
    };
    Ok(Some(WorkingVectorMembership { record, key, encoded, _owner: frame.into_reservation() }))
}

fn next_body_in_write(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    write: &redb::WriteTransaction,
    lower: Bound<&[u8]>, upper: Bound<&[u8]>,
) -> Result<Option<WorkingVectorBody>> {
    let frame = OriginalPersistenceFrame::new(original)?;
    let (entry, key, encoded) = {
        let table = write.open_table(WORKING_VECTOR_BODIES).map_err(RedbPersistence::storage_error)?;
        let mut range = table.range::<&[u8]>((lower, upper)).map_err(RedbPersistence::storage_error)?;
        let Some((key, body)) = range.next().transpose().map_err(RedbPersistence::storage_error)? else { return Ok(None); };
        let entry = RedbPersistence::decode_vector_entry_original(&frame, body.value())?;
        if !body_key_matches(key.value(), &entry) {
            return Err(Error::StoreCorrupted { path: "received working vectors".into(), reason: "canonical key and body disagree".into() });
        }
        (entry, frame.copy_bytes(key.value())?, frame.copy_bytes(body.value())?)
    };
    Ok(Some(WorkingVectorBody { entry, key, encoded, _owner: frame.into_reservation() }))
}

fn consume(bytes: &mut &[u8], expected: &[u8]) -> bool {
    if !bytes.starts_with(expected) { return false; }
    *bytes = &bytes[expected.len()..];
    true
}
fn component<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let size = usize::try_from(u64::from_be_bytes(bytes.get(..8)?.try_into().ok()?)).ok()?;
    *bytes = &bytes[8..];
    let value = bytes.get(..size)?;
    *bytes = &bytes[size..];
    Some(value)
}
fn partition_key_matches(mut bytes: &[u8], key: &VectorPartitionKey) -> bool {
    use contextdb_core::VectorPartitionComponent;
    if !consume(&mut bytes, &[1]) || !consume(&mut bytes, &(key.components().len() as u64).to_be_bytes()) { return false; }
    for value in key.components() {
        let matches = match value {
            VectorPartitionComponent::Uuid(value) => consume(&mut bytes, &[0]) && consume(&mut bytes, value.as_bytes()),
            VectorPartitionComponent::Text(value) => consume(&mut bytes, &[1])
                && consume(&mut bytes, &(value.len() as u64).to_be_bytes()) && consume(&mut bytes, value.as_bytes()),
            VectorPartitionComponent::Integer(value) => consume(&mut bytes, &[2]) && consume(&mut bytes, &value.to_be_bytes()),
            VectorPartitionComponent::Boolean(value) => consume(&mut bytes, &[3, u8::from(*value)]),
            VectorPartitionComponent::Timestamp(value) => consume(&mut bytes, &[4]) && consume(&mut bytes, &value.to_be_bytes()),
            VectorPartitionComponent::TxId(value) => consume(&mut bytes, &[5]) && consume(&mut bytes, &value.0.to_be_bytes()),
        };
        if !matches { return false; }
    }
    bytes.is_empty()
}
pub(super) fn membership_key_matches(mut bytes: &[u8], member: &VectorPartitionMembershipRecord) -> bool {
    if component(&mut bytes) != Some(member.index.table.as_bytes())
        || component(&mut bytes) != Some(member.index.column.as_bytes()) { return false; }
    let Some(partition) = component(&mut bytes) else { return false; };
    partition_key_matches(partition, &member.partition_key)
        && consume(&mut bytes, &member.row_id.0.to_be_bytes())
        && consume(&mut bytes, &member.row_created_tx.0.to_be_bytes())
        && consume(&mut bytes, &member.row_lsn.0.to_be_bytes())
        && consume(&mut bytes, &member.vector_created_tx.0.to_be_bytes())
        && consume(&mut bytes, &member.vector_lsn.0.to_be_bytes()) && bytes.is_empty()
}
pub(super) fn body_key_matches(mut bytes: &[u8], body: &VectorEntry) -> bool {
    consume(&mut bytes, body.index.table.as_bytes()) && consume(&mut bytes, &[0])
        && consume(&mut bytes, body.index.column.as_bytes()) && consume(&mut bytes, &[0])
        && consume(&mut bytes, &body.row_id.0.to_be_bytes())
        && consume(&mut bytes, &body.created_tx.0.to_be_bytes())
        && consume(&mut bytes, &body.lsn.0.to_be_bytes()) && bytes.is_empty()
}

fn partition_length(key: &VectorPartitionKey) -> Result<usize> {
    use contextdb_core::VectorPartitionComponent;
    key.components().iter().try_fold(9usize, |size, value| {
        let extra = match value {
            VectorPartitionComponent::Uuid(_) => 17,
            VectorPartitionComponent::Text(value) => value.len().checked_add(9)
                .ok_or_else(|| Error::Other("working partition key overflow".into()))?,
            VectorPartitionComponent::Boolean(_) => 2,
            _ => 9,
        };
        size.checked_add(extra).ok_or_else(|| Error::Other("working partition key overflow".into()))
    })
}
fn append_partition(bytes: &mut Vec<u8>, key: &VectorPartitionKey) {
    use contextdb_core::VectorPartitionComponent;
    bytes.push(1);
    bytes.extend_from_slice(&(key.components().len() as u64).to_be_bytes());
    for value in key.components() {
        match value {
            VectorPartitionComponent::Uuid(value) => { bytes.push(0); bytes.extend_from_slice(value.as_bytes()); },
            VectorPartitionComponent::Text(value) => { bytes.push(1); bytes.extend_from_slice(&(value.len() as u64).to_be_bytes()); bytes.extend_from_slice(value.as_bytes()); },
            VectorPartitionComponent::Integer(value) => { bytes.push(2); bytes.extend_from_slice(&value.to_be_bytes()); },
            VectorPartitionComponent::Boolean(value) => { bytes.push(3); bytes.push(u8::from(*value)); },
            VectorPartitionComponent::Timestamp(value) => { bytes.push(4); bytes.extend_from_slice(&value.to_be_bytes()); },
            VectorPartitionComponent::TxId(value) => { bytes.push(5); bytes.extend_from_slice(&value.0.to_be_bytes()); },
        }
    }
}
fn membership_key_original(frame: &OriginalPersistenceFrame<'_>, member: &VectorPartitionMembershipRecord) -> Result<Vec<u8>> {
    let partition = partition_length(&member.partition_key)?;
    let length = member.index.table.len().checked_add(member.index.column.len())
        .and_then(|n| n.checked_add(partition)).and_then(|n| n.checked_add(64))
        .ok_or_else(|| Error::Other("working membership identity overflow".into()))?;
    frame.reserve(length)?;
    let mut key = Vec::with_capacity(length);
    for name in [&member.index.table, &member.index.column] {
        key.extend_from_slice(&(name.len() as u64).to_be_bytes()); key.extend_from_slice(name.as_bytes());
    }
    key.extend_from_slice(&(partition as u64).to_be_bytes()); append_partition(&mut key, &member.partition_key);
    for coordinate in [member.row_id.0, member.row_created_tx.0, member.row_lsn.0, member.vector_created_tx.0, member.vector_lsn.0] {
        key.extend_from_slice(&coordinate.to_be_bytes());
    }
    if key.len() != length || key.capacity() != length { return Err(Error::Other("working membership key capacity differs from admission".into())); }
    Ok(key)
}
fn membership_encoded_original(frame: &OriginalPersistenceFrame<'_>, member: &VectorPartitionMembershipRecord) -> Result<Vec<u8>> {
    let payload = frame.encode(member)?;
    let length = payload.len().checked_add(VECTOR_PARTITION_RECORD_MAGIC.len() + 2 + 1 + 8 + 32)
        .ok_or_else(|| Error::Other("working membership envelope overflow".into()))?;
    frame.reserve(length)?;
    let mut encoded = Vec::with_capacity(length);
    encoded.extend_from_slice(VECTOR_PARTITION_RECORD_MAGIC);
    encoded.extend_from_slice(&VECTOR_PARTITION_RECORD_VERSION.to_be_bytes());
    encoded.push(VectorPartitionRecordKind::Membership as u8);
    encoded.extend_from_slice(&(payload.len() as u64).to_be_bytes()); encoded.extend_from_slice(&payload);
    let checksum = blake3::hash(&encoded); encoded.extend_from_slice(checksum.as_bytes());
    if encoded.len() != length || encoded.capacity() != length { return Err(Error::Other("working membership envelope capacity differs from admission".into())); }
    Ok(encoded)
}

impl ReceivedWorkingWrite {
    fn put_vector_membership(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        member: &VectorPartitionMembershipRecord) -> Result<()> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let key = membership_key_original(&frame, member)?;
        let reverse = reverse_membership_key_original(&frame, member)?;
        let encoded = membership_encoded_original(&frame, member)?;
        let before = self.next_vector_membership(original, Bound::Included(key.as_slice()), Bound::Included(key.as_slice()))?
            .map(|window| (window.record.deleted_tx.is_none(), window.record.visible_from.max(window.record.deleted_tx.unwrap_or(TxId(0)))));
        let (mut summary_lower, summary_upper) = summary_bounds(&frame, &key)?;
        self.with_write(|write| {
            update_partition_summary(write, &key, &mut summary_lower, &summary_upper, before, member, true)?;
            write.open_table(WORKING_VECTOR_MEMBERSHIPS).map_err(RedbPersistence::storage_error)?
                .insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
            write.open_table(WORKING_VECTOR_REVERSE).map_err(RedbPersistence::storage_error)?
                .insert(reverse.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
            Ok(())
        })
    }

    /// Exact row-family reverse probe covers every owning-row version and
    /// partition, then compares canonical coordinates. A single reverse point
    /// cannot prove that another placement of this canonical body is absent.
    fn vector_body_has_live_placement(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        index: &VectorIndexRef, row: RowId, created: TxId, lsn: Lsn) -> Result<bool> {
        let bounds = OriginalPersistenceFrame::new(original)?;
        let length = index.table.len().checked_add(index.column.len()).and_then(|n| n.checked_add(24))
            .ok_or_else(|| Error::Other("working vector reverse prefix overflow".into()))?;
        bounds.reserve(length)?;
        let mut prefix = Vec::with_capacity(length);
        for name in [&index.table, &index.column] {
            prefix.extend_from_slice(&(name.len() as u64).to_be_bytes()); prefix.extend_from_slice(name.as_bytes());
        }
        prefix.extend_from_slice(&row.0.to_be_bytes());
        let mut end = bounds.copy_bytes(&prefix)?;
        let end = if let Some(position) = end.iter().rposition(|byte| *byte != 255) {
            end[position] += 1; end.truncate(position + 1); Some(end)
        } else { None };
        let mut after = None::<VectorPosition>;
        loop {
            let frame = OriginalPersistenceFrame::new(original)?;
            let mut paid = 0;
            let copied = loop {
                let copied = self.with_write(|write| {
                    let table = write.open_table(WORKING_VECTOR_REVERSE).map_err(RedbPersistence::storage_error)?;
                    let lower = after.as_ref().map_or(Bound::Included(prefix.as_slice()), |key| Bound::Excluded(key.key.as_slice()));
                    let upper = end.as_ref().map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice()));
                    let mut range = table.range::<&[u8]>((lower, upper)).map_err(RedbPersistence::storage_error)?;
                    let Some((key, value)) = range.next().transpose().map_err(RedbPersistence::storage_error)? else { return Ok(None); };
                    let needed = key.value().len().checked_add(value.value().len())
                        .ok_or_else(|| Error::Other("working reverse window overflow".into()))?;
                    if needed > paid { return Ok(Some(Err(needed))); }
                    Ok(Some(Ok((key.value().to_vec(), value.value().to_vec()))))
                })?;
                match copied {
                    None => return Ok(false),
                    Some(Err(needed)) => { frame.reserve(needed - paid)?; paid = needed; },
                    Some(Ok(value)) => break value,
                }
            };
            // Native guards are gone before decode/admission and the next seek.
            let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, &copied.1)
                .map_err(|reason| Error::StoreCorrupted { path: "received working reverse".into(), reason })?;
            let member: VectorPartitionMembershipRecord = frame.decode(payload)?;
            if !copied.0.starts_with(&prefix) || !reverse_membership_key_matches(&copied.0, &member)
                || member.index != *index || member.row_id != row {
                return Err(summary_corruption());
            }
            if member.vector_created_tx == created && member.vector_lsn == lsn && member.deleted_tx.is_none() { return Ok(true); }
            drop(member); drop(copied.1);
            after = Some(VectorPosition { key: copied.0, _owner: frame.into_reservation() });
        }
    }

    fn close_vector_membership(&self, original: &crate::memory_accounting::OwnedMemoryReservation,
        prepared: &VectorPartitionMembershipRecord, deleted: TxId) -> Result<()> {
        let frame = OriginalPersistenceFrame::new(original)?;
        let key = membership_key_original(&frame, prepared)?;
        let mut current = self.next_vector_membership(original, Bound::Included(key.as_slice()), Bound::Included(key.as_slice()))?
            .ok_or_else(|| Error::StoreCorrupted { path: "received working memberships".into(),
                reason: "delete or move has no source placement".into() })?;
        let previous_deleted = current.record.deleted_tx.take();
        if &current.record != prepared {
            return Err(Error::StoreCorrupted { path: "received working memberships".into(),
                reason: "source placement differs from its prepared row/vector binding".into() });
        }
        if previous_deleted.is_some_and(|seen| seen != deleted) {
            return Err(Error::StoreCorrupted { path: "received working memberships".into(),
                reason: "placement has conflicting deletion transactions".into() });
        }
        current.record.deleted_tx = Some(deleted);
        self.put_vector_membership(original, &current.record)
    }

    /// Mirror the already-validated exact row/partition plan. The real vector
    /// store remains responsible for policy/cap validation and publication.
    pub(crate) fn apply_vector_partition_plan(&self,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        batch: &PreparedVectorPartitionMutationBatch,
        ws: &contextdb_tx::WriteSet,
    ) -> Result<()> {
        self.require_original(original)?;
        if batch.is_empty() { return Ok(()); }
        if !self.vectors_prepared()? { return Err(Error::Other("received vectors have no completed private image".into())); }
        for mutation in &batch.mutations {
            let frame = OriginalPersistenceFrame::new(original)?;
            let make_member = |index: &VectorIndexRef, binding: &PreparedVectorPartitionBinding| {
                frame.reserve(index.table.len().checked_add(index.column.len())
                    .and_then(|n| n.checked_add(binding.partition_key.estimated_bytes().saturating_mul(4)))
                    .ok_or_else(|| Error::Other("working vector binding overflow".into()))?)?;
                RedbPersistence::membership_from_binding(index, binding, None)
            };
            match mutation {
                PreparedVectorPartitionMutation::Delete { index, before, deleted_tx } => {
                    let member = make_member(index, before)?;
                    self.close_vector_membership(original, &member, *deleted_tx)?;
                }
                PreparedVectorPartitionMutation::Insert { index, after, entry } => {
                    let member = make_member(index, after)?;
                    let entry = batch.resolve_insert(ws, *entry, index, after)?;
                    if entry.index != *index || entry.row_id != member.row_id
                        || entry.created_tx != member.vector_created_tx || entry.lsn != member.vector_lsn {
                        return Err(Error::Other("working vector insert escaped its prepared binding".into()));
                    }
                    let encoded = RedbPersistence::encode_vector_entry_original(&frame, entry, after.table_meta.columns[after.declaration].quantization)?;
                    frame.reserve(index.table.len().saturating_add(index.column.len()).saturating_add(34))?;
                    let key = RedbPersistence::vector_key(entry);
                    self.with_write(|write| {
                        write.open_table(WORKING_VECTOR_BODIES).map_err(RedbPersistence::storage_error)?
                            .insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                        Ok(())
                    })?;
                    self.put_vector_membership(original, &member)?;
                }
                PreparedVectorPartitionMutation::Move { index, before, after, moved_tx } => {
                    let source = make_member(index, before)?;
                    let destination = make_member(index, after)?;
                    // Preserve the stored scalar representation while changing
                    // the target row identity, including signed-zero/bounds.
                    let body = self.vector_body_for_membership(original, &source)?;
                    let mut stored: PersistedVectorEntry = frame.decode(body.encoded())?;
                    stored.row_id = destination.row_id;
                    stored.created_tx = destination.vector_created_tx;
                    stored.lsn = destination.vector_lsn;
                    stored.deleted_tx = None;
                    let encoded = frame.encode(&stored)?;
                    frame.reserve(index.table.len().saturating_add(index.column.len()).saturating_add(34))?;
                    let key = RedbPersistence::vector_identity_key(index, destination.row_id,
                        destination.vector_created_tx, destination.vector_lsn);
                    self.close_vector_membership(original, &source, *moved_tx)?;
                    self.with_write(|write| {
                        write.open_table(WORKING_VECTOR_BODIES).map_err(RedbPersistence::storage_error)?
                            .insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                        Ok(())
                    })?;
                    self.put_vector_membership(original, &destination)?;
                }
            }
        }
        // Set the canonical close only after the complete prepared batch:
        // a scalar-only move may reopen the same body in another placement.
        for mutation in &batch.mutations {
            let (index, before, closed) = match mutation {
                PreparedVectorPartitionMutation::Delete { index, before, deleted_tx } => (index, before, *deleted_tx),
                PreparedVectorPartitionMutation::Move { index, before, moved_tx, .. } => (index, before, *moved_tx),
                PreparedVectorPartitionMutation::Insert { .. } => continue,
            };
            let live = self.vector_body_has_live_placement(original, index, before.row.row_id,
                before.vector_created_tx, before.vector_lsn)?;
            if !live {
                let frame = OriginalPersistenceFrame::new(original)?;
                frame.reserve(index.table.len().saturating_add(index.column.len())
                    .saturating_add(before.partition_key.estimated_bytes().saturating_mul(4)))?;
                let member = RedbPersistence::membership_from_binding(index, before, None)?;
                let body = self.vector_body_for_membership(original, &member)?;
                let mut stored: PersistedVectorEntry = frame.decode(body.encoded())?;
                if stored.deleted_tx.is_none() {
                    stored.deleted_tx = Some(closed);
                    let encoded = frame.encode(&stored)?;
                    self.with_write(|write| {
                        write.open_table(WORKING_VECTOR_BODIES).map_err(RedbPersistence::storage_error)?
                            .insert(body.position(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                        Ok(())
                    })?;
                }
            }
        }
        for partition in batch.reclaimable_partitions() {
            let frame = OriginalPersistenceFrame::new(original)?;
            let key = summary_key_original(&frame, partition)?;
            let index_length = partition.index.table.len().checked_add(partition.index.column.len())
                .and_then(|n: usize| n.checked_add(16)).ok_or_else(summary_corruption)?;
            self.with_write(|write| {
                let mut summaries = write.open_table(WORKING_VECTOR_SUMMARIES).map_err(RedbPersistence::storage_error)?;
                let previous = summaries.get(key.as_slice()).map_err(RedbPersistence::storage_error)?
                    .map(|value| Ok::<_, Error>((summary_decode(value.value())?, value.value()[24] != 0))).transpose()?;
                let Some((summary, historical)) = previous else { return Ok(()); };
                if summary.live_entries != 0 { return Err(summary_corruption()); }
                if !historical {
                    summaries.insert(key.as_slice(), summary_encode(summary, true)?.as_slice()).map_err(RedbPersistence::storage_error)?;
                    update_partition_counts(write, &key[..index_length], previous, (summary, true))?;
                    update_empty_partition(write, &key, summary, true)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }
}

use contextdb_vector::store::{RawVectorMembershipSummary, VectorMembershipProvenance};

struct WorkingVectorColumnCountsOwner {
    counts: Arc<parking_lot::RwLock<[usize; 6]>>,
    control: contextdb_tx::SourceCharge,
}
impl Clone for WorkingVectorColumnCountsOwner {
    fn clone(&self) -> Self {
        let control = self.control.clone();
        let counts = self.counts.clone();
        Self { counts, control }
    }
}
impl WorkingVectorColumnCountsOwner {
    fn prepare(funder: &contextdb_tx::SourceFunder, counts: [usize; 6]) -> Result<Self> {
        let control = contextdb_tx::SourceCharge::admit_funded(funder.clone(),
            contextdb_tx::staged_source::source_arc_layout::<parking_lot::RwLock<[usize; 6]>>().size())?;
        let counts = Arc::new(parking_lot::RwLock::new(counts));
        Ok(Self { counts, control })
    }
}
struct WorkingVectorColumnEntry {
    index: VectorIndexRef,
    counts: WorkingVectorColumnCountsOwner,
    seen: bool,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl Drop for WorkingVectorColumnEntry { fn drop(&mut self) {} }
struct WorkingVectorCatalog {
    // Current declarations only. No partition identity or history body lives here.
    columns: parking_lot::Mutex<Vec<WorkingVectorColumnEntry>>,
    host: std::sync::atomic::AtomicUsize,
    funder: contextdb_tx::SourceFunder,
    storage: parking_lot::Mutex<crate::memory_accounting::OwnedMemoryReservation>,
}
/// The catalog's Arc control is external to the allocation it prices. Its
/// declared columns retain only their actual names and shared scalar controls.
pub(crate) struct WorkingVectorCatalogOwner {
    catalog: Arc<WorkingVectorCatalog>,
    control: contextdb_tx::SourceCharge,
}
impl Clone for WorkingVectorCatalogOwner {
    fn clone(&self) -> Self {
        let control = self.control.clone();
        let catalog = self.catalog.clone();
        Self { catalog, control }
    }
}
impl WorkingVectorCatalogOwner {
    pub(crate) fn prepare(original: &crate::memory_accounting::OwnedMemoryReservation,
        funder: &contextdb_tx::SourceFunder) -> Result<Self> {
        crate::memory_accounting::validate_owned_original(original, original.accountant(), funder)?;
        let control = contextdb_tx::SourceCharge::admit_funded(funder.clone(),
            contextdb_tx::staged_source::source_arc_layout::<WorkingVectorCatalog>().size())?;
        let storage = crate::memory_accounting::reserve_preparation_from_original(original, 0,
            "persistence", "working_vector_catalog", "Raise MEMORY_LIMIT to prepare vector partition metadata.")?;
        let catalog = Arc::new(WorkingVectorCatalog { columns: parking_lot::Mutex::new(Vec::new()),
            host: std::sync::atomic::AtomicUsize::new(0), funder: funder.clone(), storage: parking_lot::Mutex::new(storage) });
        Ok(Self { catalog, control })
    }
    pub(crate) fn bind(&self, host: usize) -> Result<()> {
        self.catalog.host.compare_exchange(0, host, std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire).map(|_| ())
            .map_err(|_| Error::Other("working vector catalog is already bound".into()))
    }
    fn ensure_column(&self, index: &VectorIndexRef, initial_counts: [usize; 6]) -> Result<WorkingVectorColumnCountsOwner> {
        if let Some(counts) = self.catalog.columns.lock().iter().find(|entry| entry.index == *index)
            .map(|entry| entry.counts.clone()) { return Ok(counts); }
        let owned = {
            let seed = self.catalog.storage.lock();
            let frame = OriginalPersistenceFrame::new(&seed)?;
            frame.reserve(index.table.len().checked_add(index.column.len()).ok_or_else(summary_corruption)?)?;
            let value = VectorIndexRef { table: crate::memory_accounting::preparation_copy_string(&index.table)?,
                column: crate::memory_accounting::preparation_copy_string(&index.column)? };
            super::OriginalDecoded { value, allocation: frame.into_reservation() }
        };
        let counts = WorkingVectorColumnCountsOwner::prepare(&self.catalog.funder, initial_counts)?;
        let entry = WorkingVectorColumnEntry { index: owned.value, counts, seen: true, _owner: owned.allocation };
        loop {
            let (length, capacity) = {
                let mut columns = self.catalog.columns.lock();
                if let Some(existing) = columns.iter().find(|seen| seen.index == entry.index) { return Ok(existing.counts.clone()); }
                if columns.len() < columns.capacity() {
                    let result = entry.counts.clone(); columns.push(entry); return Ok(result);
                }
                (columns.len(), columns.capacity())
            };
            let target = capacity.checked_mul(2).map(|n| n.max(4)).ok_or_else(summary_corruption)?;
            let bytes = std::alloc::Layout::array::<WorkingVectorColumnEntry>(target).map_err(|_| summary_corruption())?.size();
            let backing = crate::memory_accounting::reserve_preparation_from_original(&self.catalog.storage.lock(), bytes,
                "persistence", "working_vector_declared_columns", "Raise MEMORY_LIMIT for declared vector column metadata.")?;
            // Both originals remain live until the old empty allocation is freed.
            let retired_backing;
            let mut replacement = Vec::with_capacity(target);
            if replacement.capacity() != target { return Err(summary_corruption()); }
            let mut storage = self.catalog.storage.lock();
            let mut columns = self.catalog.columns.lock();
            if columns.len() != length || columns.capacity() != capacity { continue; }
            replacement.append(&mut columns);
            std::mem::swap(&mut *columns, &mut replacement);
            retired_backing = std::mem::replace(&mut *storage, backing);
            drop(columns); drop(storage);
            drop(replacement); drop(retired_backing);
        }
    }
    pub(crate) fn refresh(&self, write: &ReceivedWorkingWrite,
        original: &crate::memory_accounting::OwnedMemoryReservation) -> Result<()> {
        for entry in self.catalog.columns.lock().iter_mut() { entry.seen = false; }
        let mut position = 0usize;
        while let Some(index) = write.vector_declaration_at(original, position)? {
            position = position.checked_add(1).ok_or_else(summary_corruption)?;
            let counts = write.vector_column_counts_original(original, &index.value)?;
            let retained = self.ensure_column(&index.value, counts)?;
            *retained.counts.write() = counts;
            if let Some(entry) = self.catalog.columns.lock().iter_mut().find(|entry| entry.index == index.value) { entry.seen = true; }
        }
        loop {
            let retired = {
                let mut columns = self.catalog.columns.lock();
                let position = columns.iter().position(|entry| !entry.seen);
                position.map(|position| columns.swap_remove(position))
            };
            let Some(retired) = retired else { break; };
            drop(retired); // names/control retire outside the metadata guard
        }
        Ok(())
    }
    fn loan_source(&self) -> Result<super::received_working_source::WorkingReadSource> {
        let host = self.catalog.host.load(std::sync::atomic::Ordering::Acquire);
        super::received_working_source::WorkingReadSource::current_for(host)?
            .ok_or_else(|| Error::Other("working vector source is outside its owning loan".into()))
    }
    fn source(&self) -> Result<super::received_working_source::WorkingReadSource> {
        let source = self.loan_source()?;
        let memory = vector_membership_source::read_memory_for_operation()
            .or_else(crate::read_image_memory::current_reader_budget)
            .ok_or_else(|| Error::Other("working vector read requires its executing reader".into()))?;
        Ok(source.for_read(memory))
    }
    fn copy_partition(&self, index: &VectorIndexRef, partition: &VectorPartitionKey)
        -> Result<super::OriginalDecoded<VectorPartitionRef>> {
        let original = self.catalog.storage.lock();
        Self::copy_partition_original(&original, index, partition)
    }
    fn copy_partition_original(original: &crate::memory_accounting::OwnedMemoryReservation,
        index: &VectorIndexRef, partition: &VectorPartitionKey)
        -> Result<super::OriginalDecoded<VectorPartitionRef>> {
        // This source owns a distinct physical key. Borrowing the catalog's
        // admitted key is not an invoice for its independent copy.
        let encoded = RedbPersistence::encode_owned_event(original, &(index, partition))?;
        let decoded: super::OriginalDecoded<(VectorIndexRef, VectorPartitionKey)> =
            RedbPersistence::decode_owned_event(original, &encoded.bytes)?;
        let (index, partition) = decoded.value;
        Ok(super::OriginalDecoded { value: VectorPartitionRef::new(index, partition), allocation: decoded.allocation })
    }
    fn point_summary(&self, partition: &VectorPartitionRef, owner: &mut VectorCopyAdmission)
        -> Result<RawVectorMembershipSummary> {
        let source = self.loan_source()?;
        let capture = source.capture_vector_catalog()?;
        let length = partition.index.table.len().checked_add(partition.index.column.len())
            .and_then(|n| n.checked_add(24)).and_then(|n| n.checked_add(partition_length(&partition.partition_key).ok()?))
            .ok_or_else(summary_corruption)?;
        let mut scratch = owner.scratch(); scratch.reserve(length)?;
        let mut key = Vec::with_capacity(length);
        for name in [&partition.index.table, &partition.index.column] {
            key.extend_from_slice(&(name.len() as u64).to_be_bytes()); key.extend_from_slice(name.as_bytes());
        }
        let partition_length = partition_length(&partition.partition_key)?;
        key.extend_from_slice(&(partition_length as u64).to_be_bytes()); append_partition(&mut key, &partition.partition_key);
        Ok(source.vector_summary_scalars(&capture, &key)?.map(|value| value.0).unwrap_or_default())
    }
    fn source_control<T>(&self, index: Option<&VectorIndexRef>) -> Result<contextdb_tx::SourceCharge> {
        let names = index.map_or(0, |index| index.table.len().saturating_add(index.column.len()));
        let bytes = contextdb_tx::staged_source::source_arc_layout::<T>().size().checked_add(names)
            .ok_or_else(|| Error::Other("working vector source control overflow".into()))?;
        contextdb_tx::SourceCharge::admit_funded(self.catalog.funder.clone(), bytes)
    }
}

pub(crate) struct WorkingVectorFactoryOwner {
    factory: Arc<WorkingVectorColumnFactory>,
    control: contextdb_tx::SourceCharge,
}
impl WorkingVectorFactoryOwner {
    pub(crate) fn prepare(catalog: WorkingVectorCatalogOwner) -> Result<Self> {
        let control = catalog.source_control::<WorkingVectorColumnFactory>(None)?;
        let factory = Arc::new(WorkingVectorColumnFactory { catalog, control: control.clone() });
        Ok(Self { factory, control })
    }
    pub(crate) fn install(&self, store: &contextdb_vector::VectorStore) {
        store.register_durable_raw_column_factory(self.factory.clone());
    }
}
struct WorkingQueryPartitionOwner {
    source: Arc<WorkingVectorPartitionSource>,
    _control: contextdb_vector::store::QuerySourceControl,
}
struct WorkingPagedVectorCatalog {
    index: VectorIndexRef,
    prefix: Vec<u8>,
    end: Option<Vec<u8>>,
    source: super::received_working_source::WorkingReadSource,
    capture: super::received_working_source::WorkingVectorCapture,
    counts: [usize; 6],
    catalog: WorkingVectorCatalogOwner,
}
impl WorkingVectorCatalogOwner {
    fn query_partition(&self, index: &VectorIndexRef, key: &VectorPartitionKey,
        source: &super::received_working_source::WorkingReadSource,
        capture: super::received_working_source::WorkingVectorCapture,
        summary: RawVectorMembershipSummary, owner: &mut VectorCopyAdmission)
        -> Result<WorkingQueryPartitionOwner> {
        source.check_vector_capture(&capture)?;
        let mut paid = owner.scratch();
        let bytes = index.table.len().checked_add(index.column.len())
            .and_then(|n| n.checked_add(key.estimated_bytes().saturating_sub(std::mem::size_of::<VectorPartitionKey>())))
            .and_then(|n| n.checked_add(contextdb_tx::staged_source::source_arc_layout::<WorkingVectorPartitionSource>().size()))
            .ok_or_else(|| Error::Other("working query source control overflow".into()))?;
        paid.reserve(bytes)?;
        let control = contextdb_vector::store::QuerySourceControl::new(paid)?;
        let partition = VectorPartitionRef::new(index.clone(), key.clone());
        let source = Arc::new(WorkingVectorPartitionSource { partition, catalog: self.clone(),
            owner: WorkingPartitionOwner::Query { control: control.clone(), source: source.clone(), capture, summary } });
        Ok(WorkingQueryPartitionOwner { source, _control: control })
    }
}
impl contextdb_vector::store::PagedVectorCatalog for WorkingPagedVectorCatalog {
    fn validate(&self) -> Result<()> { self.source.check_vector_capture(&self.capture) }
    fn counts(&self) -> [usize; 6] { self.counts }
    fn len(&self) -> usize { self.counts[0] }
    fn probe(&self, key: &VectorPartitionKey, owner: &mut VectorCopyAdmission)
        -> Result<Option<contextdb_vector::store::PagedVectorPartitionSources>> {
        self.source.check_vector_capture(&self.capture)?;
        let partition_length = partition_length(key)?;
        let length = self.prefix.len().checked_add(8).and_then(|n| n.checked_add(partition_length))
            .ok_or_else(summary_corruption)?;
        let mut scratch = owner.scratch();
        scratch.reserve(length)?;
        let mut position = Vec::with_capacity(length);
        position.extend_from_slice(&self.prefix);
        position.extend_from_slice(&(partition_length as u64).to_be_bytes()); append_partition(&mut position, key);
        let Some(window) = self.source.read_vector_summary(&self.capture,
            Bound::Included(position.as_slice()), Bound::Included(position.as_slice()))? else { return Ok(None); };
        if window.value.0 != *key || window.position.as_ref() != position.as_slice() { return Err(summary_corruption()); }
        let source = self.catalog.query_partition(&self.index, key, &self.source, self.capture, window.value.1, owner)?;
        Ok(Some(contextdb_vector::store::PagedVectorPartitionSources {
            memberships: source.source.clone(), loader: source.source.clone(), summary: window.value.1, historical_only: window.value.2,
            control: source._control.clone(),
        }))
    }
    fn visit(&self, owner: &mut VectorCopyAdmission, before_read: &mut dyn FnMut() -> Result<()>,
        visit: &mut dyn FnMut(&VectorPartitionKey, RawVectorMembershipSummary, bool,
            Arc<dyn RawVectorMembershipSource>, Arc<dyn DormantRawVectorLoader>) -> Result<()>) -> Result<()> {
        self.visit_directory(false, owner, before_read, &mut |key, summary, historical, memberships, loader| {
            visit(key, summary, historical, memberships, loader)?; Ok(true)
        })
    }
    fn visit_empty(&self, owner: &mut VectorCopyAdmission,
        visit: &mut dyn FnMut(&VectorPartitionKey, RawVectorMembershipSummary, bool,
            Arc<dyn RawVectorMembershipSource>, Arc<dyn DormantRawVectorLoader>) -> Result<bool>) -> Result<()> {
        self.visit_directory(true, owner, &mut || Ok(()), visit)
    }
}

impl WorkingPagedVectorCatalog {
    fn visit_directory(&self, empty: bool, owner: &mut VectorCopyAdmission, before_read: &mut dyn FnMut() -> Result<()>,
        visit: &mut dyn FnMut(&VectorPartitionKey, RawVectorMembershipSummary, bool,
            Arc<dyn RawVectorMembershipSource>, Arc<dyn DormantRawVectorLoader>) -> Result<bool>) -> Result<()> {
        let expected = if empty { self.counts[3].checked_sub(self.counts[1]).ok_or_else(summary_corruption)? } else { self.counts[0] };
        self.source.check_vector_capture(&self.capture)?;
        let mut selected = None;
        let mut seen = 0usize;
        loop {
            before_read()?;
            let lower = selected.as_ref().map_or(Bound::Included(self.prefix.as_slice()),
                |window: &writer_source::WriterWindow<(VectorPartitionKey, RawVectorMembershipSummary, bool)>| Bound::Excluded(window.position.as_ref()));
            let upper = self.end.as_ref().map_or(Bound::Unbounded, |end| Bound::Excluded(end.as_slice()));
            let window = if empty { self.source.read_vector_empty_summary(&self.capture, lower, upper)? }
                else { self.source.read_vector_summary(&self.capture, lower, upper)? };
            let Some(window) = window else { break; };
            if empty && (window.value.2 || window.value.1.live_entries != 0) { return Err(summary_corruption()); }
            if !window.position.starts_with(&self.prefix) { return Err(summary_corruption()); }
            seen = seen.checked_add(1).ok_or_else(summary_corruption)?;
            if seen > expected { return Err(summary_corruption()); }
            let source = self.catalog.query_partition(&self.index, &window.value.0, &self.source,
                self.capture, window.value.1, owner)?;
            // The reader-owned window is retained through this callback, with
            // no native record/range/registry guard alive.
            if !visit(&window.value.0, window.value.1, window.value.2, source.source.clone(), source.source.clone())? {
                return self.source.check_vector_capture(&self.capture);
            }
            selected = Some(window);
        }
        if seen != expected { return Err(summary_corruption()); }
        self.source.check_vector_capture(&self.capture)
    }
}

struct WorkingVectorColumnFactory { catalog: WorkingVectorCatalogOwner, control: contextdb_tx::SourceCharge }
struct WorkingVectorPartitionFactory {
    index: VectorIndexRef,
    counts: WorkingVectorColumnCountsOwner,
    catalog: WorkingVectorCatalogOwner,
    control: contextdb_tx::SourceCharge,
}
struct WorkingVectorReverseSource { index: VectorIndexRef, catalog: WorkingVectorCatalogOwner, control: contextdb_tx::SourceCharge }
struct WorkingVectorPartitionSource {
    partition: VectorPartitionRef,
    catalog: WorkingVectorCatalogOwner,
    owner: WorkingPartitionOwner,
}
struct WorkingVectorStreamedTail {
    working: WorkingVectorPartitionSource,
    origin: writer_source::WriterWindow<VectorIndexRef>,
    capture: super::received_working_source::WorkingVectorCapture,
    covered_tx: TxId,
    covered_lsn: Lsn,
    // Decoded route metadata stays paid through saved source/loader teardown.
    _metadata_memory: Arc<crate::read_image_memory::ImageMemory>,
}
impl WorkingVectorStreamedTail {
    fn delta(&self, interval: &RawVectorDirectoryEntry, body: &RawVectorDirectoryEntry,
        snapshot: SnapshotId, owner: &mut VectorCopyAdmission,
        before: &mut dyn FnMut() -> bool) -> Result<bool> {
        use contextdb_vector::store::CapturedVectorStreamedTail;
        self.validate()?;
        if !before() { return Err(Error::ReadCancelled); }
        if interval.row_id != body.row_id || interval.membership != body.membership {
            return Err(summary_corruption());
        }
        if interval.created_tx.0 > snapshot.0 || interval.deleted_tx.is_some_and(|tx| tx.0 <= snapshot.0) {
            return Ok(false);
        }
        // Only the actual newest visible owning-row placement is scoreable.
        if self.working.visible_by_row(body.row_id, snapshot, before, owner)?.as_ref() != Some(body) {
            return Ok(false);
        }
        if body.created_tx > self.covered_tx || body.lsn > self.covered_lsn { return Ok(true); }
        let source = self.working.source()?;
        let root = source.root();
        root.check_working_scope(&self.origin.value.table)?;
        let partition = partition_length(&self.working.partition.partition_key)?;
        let length = self.origin.value.table.len().checked_add(self.origin.value.column.len())
            .and_then(|n| n.checked_add(partition)).and_then(|n| n.checked_add(64)).ok_or_else(summary_corruption)?;
        let mut key_memory = owner.scratch();
        key_memory.reserve(length)?;
        let mut key = Vec::with_capacity(length);
        for name in [&self.origin.value.table, &self.origin.value.column] {
            key.extend_from_slice(&(name.len() as u64).to_be_bytes()); key.extend_from_slice(name.as_bytes());
        }
        key.extend_from_slice(&(partition as u64).to_be_bytes());
        append_partition(&mut key, &self.working.partition.partition_key);
        for coordinate in [body.row_id.0, body.membership.owning_row_tx.0, body.membership.row_lsn.0,
            body.created_tx.0, body.lsn.0] { key.extend_from_slice(&coordinate.to_be_bytes()); }
        if key.len() != length || key.capacity() != length { return Err(summary_corruption()); }
        if !before() { return Err(Error::ReadCancelled); }
        let mut cursor = root.cursor(writer_source::SourceWindowOwner::Read(source.reader_budget()),
            VECTOR_PARTITION_MEMBERSHIP_TABLE.name(), writer_source::WriterCursorFamily::Bytes,
            Bound::Included(key.as_slice()), Bound::Included(key.as_slice()), false)?;
        let initial = cursor.next_membership()?;
        let matches = initial.as_ref().is_some_and(|window| window.position() == key.as_slice()
            && window.value.index == self.origin.value
            && window.value.partition_key == self.working.partition.partition_key
            && window.value.visible_from == interval.created_tx
            && raw_body_identity(&window.value) == *body);
        source.check_vector_capture(&self.capture)?;
        Ok(!matches)
    }
}
impl contextdb_vector::store::CapturedVectorStreamedTail for WorkingVectorStreamedTail {
    fn validate(&self) -> Result<()> { self.working.source()?.check_vector_capture(&self.capture) }
    fn includes(&self, interval: &RawVectorDirectoryEntry, body: &RawVectorDirectoryEntry,
        snapshot: SnapshotId, owner: &mut VectorCopyAdmission, before: &mut dyn FnMut() -> bool) -> Result<bool> {
        self.delta(interval, body, snapshot, owner, before)
    }
    fn includes_row(&self, row: RowId, snapshot: SnapshotId, owner: &mut VectorCopyAdmission,
        before: &mut dyn FnMut() -> bool) -> Result<bool> {
        let Some(body) = self.working.visible_by_row(row, snapshot, before, owner)? else { return Ok(false); };
        let mut interval = body.clone();
        interval.created_tx = body.membership.visible_from;
        self.delta(&interval, &body, snapshot, owner, before)
    }
}

enum WorkingPartitionOwner {
    Original { control: contextdb_tx::SourceCharge,
        summary: RawVectorMembershipSummary,
        _partition_memory: crate::memory_accounting::OwnedMemoryReservation },
    Query { control: contextdb_vector::store::QuerySourceControl,
        source: super::received_working_source::WorkingReadSource,
        capture: super::received_working_source::WorkingVectorCapture,
        summary: RawVectorMembershipSummary },
}

use contextdb_vector::{VectorCopyAdmission, MemoryBudget};
use contextdb_vector::store::{DurableRawColumnFactory, DurableRawPartitionFactory, RawVectorReverseMembershipSource,
    RawVectorMembershipSource, RawVectorMembershipCursor, RawVectorDirectoryEntry, DormantRawVectorLoader,
    DormantVectorLoadRequest, RawVectorMembershipSelection};

impl DurableRawColumnFactory for WorkingVectorColumnFactory {
    fn native_source_control(&self) -> Option<contextdb_tx::SourceCharge> { Some(self.control.clone()) }
    fn create(&self, index: &VectorIndexRef, _layout: &contextdb_vector::VectorIndexLayout,
        _workspace: VectorCopyAdmission, _retained: VectorCopyAdmission,
    ) -> Result<(Arc<dyn RawVectorReverseMembershipSource>, Arc<dyn DurableRawPartitionFactory>)> {
        let reverse_control = self.catalog.source_control::<WorkingVectorReverseSource>(Some(index))?;
        let partition_control = self.catalog.source_control::<WorkingVectorPartitionFactory>(Some(index))?;
        let counts = self.catalog.ensure_column(index, [0; 6])?;
        let reverse = Arc::new(WorkingVectorReverseSource {
            index: VectorIndexRef { table: crate::memory_accounting::preparation_copy_string(&index.table)?,
                column: crate::memory_accounting::preparation_copy_string(&index.column)? },
            catalog: self.catalog.clone(), control: reverse_control.clone(),
        });
        let partitions = Arc::new(WorkingVectorPartitionFactory {
            index: VectorIndexRef { table: crate::memory_accounting::preparation_copy_string(&index.table)?,
                column: crate::memory_accounting::preparation_copy_string(&index.column)? },
            counts, catalog: self.catalog.clone(), control: partition_control.clone(),
        });
        Ok((reverse, partitions))
    }
}
impl DurableRawPartitionFactory for WorkingVectorPartitionFactory {
    fn native_source_control(&self) -> Option<contextdb_tx::SourceCharge> { Some(self.control.clone()) }
    fn published_catalog_counts(&self) -> Option<[usize; 6]> { Some(*self.counts.counts.read()) }
    fn catalog_scalar_occupied(&self, index: &VectorIndexRef, key: &VectorPartitionKey) -> Result<Option<bool>> {
        if *index != self.index { return Err(Error::Other("working vector factory index does not match its declaration".into())); }
        let source = self.catalog.source()?;
        let capture = source.capture_vector_catalog()?;
        let partition_length = partition_length(key)?;
        let length = index.table.len().checked_add(index.column.len()).and_then(|n| n.checked_add(24))
            .and_then(|n| n.checked_add(partition_length)).ok_or_else(summary_corruption)?;
        let _credit = contextdb_core::read_memory::ReadCredit::try_new(source.reader_budget(), length,
            "reader", "working_vector_cap_probe", "Raise the declared reader memory limit.")?;
        let mut encoded = Vec::with_capacity(length);
        for name in [&index.table, &index.column] {
            encoded.extend_from_slice(&(name.len() as u64).to_be_bytes()); encoded.extend_from_slice(name.as_bytes());
        }
        encoded.extend_from_slice(&(partition_length as u64).to_be_bytes()); append_partition(&mut encoded, key);
        Ok(Some(source.vector_summary_scalars(&capture, &encoded)?.is_some_and(|(_, historical)| !historical)))
    }
    fn catalog_scalar_counts(&self, index: &VectorIndexRef) -> Result<Option<[usize; 6]>> {
        if *index != self.index { return Err(Error::Other("working vector factory index does not match its declaration".into())); }
        let source = self.catalog.source()?;
        let capture = source.capture_vector_catalog()?;
        let length = index.table.len().checked_add(index.column.len()).and_then(|n| n.checked_add(16))
            .ok_or_else(summary_corruption)?;
        let _credit = contextdb_core::read_memory::ReadCredit::try_new(source.reader_budget(), length,
            "reader", "working_vector_catalog_counts", "Raise the declared reader memory limit.")?;
        let mut prefix = Vec::with_capacity(length);
        for name in [&index.table, &index.column] {
            prefix.extend_from_slice(&(name.len() as u64).to_be_bytes()); prefix.extend_from_slice(name.as_bytes());
        }
        source.vector_partition_counts(&capture, &prefix).map(Some)
    }
    fn capture_catalog(&self, index: &VectorIndexRef, owner: &mut VectorCopyAdmission)
        -> Result<Option<contextdb_vector::store::PagedVectorCatalogSelection>> {
        if *index != self.index { return Err(Error::Other("working vector factory index does not match its declaration".into())); }
        let source = self.catalog.source()?;
        let capture = source.capture_vector_catalog()?;
        let mut paid = owner.scratch();
        let length = index.table.len().checked_add(index.column.len()).and_then(|n| n.checked_add(16))
            .ok_or_else(|| Error::Other("working catalog prefix overflow".into()))?;
        paid.reserve(std::mem::size_of::<WorkingPagedVectorCatalog>()
            .checked_add(length.checked_mul(2).and_then(|n| n.checked_add(index.table.len()))
                .and_then(|n| n.checked_add(index.column.len())).ok_or_else(summary_corruption)?).ok_or_else(summary_corruption)?)?;
        let control = contextdb_vector::store::QuerySourceControl::new(paid)?;
        let mut prefix = Vec::with_capacity(length);
        for name in [&index.table, &index.column] {
            prefix.extend_from_slice(&(name.len() as u64).to_be_bytes()); prefix.extend_from_slice(name.as_bytes());
        }
        let mut end = prefix.clone();
        let end = if let Some(position) = end.iter().rposition(|byte| *byte != 255) {
            end[position] += 1; end.truncate(position + 1); Some(end)
        } else { None };
        let counts = source.vector_partition_counts(&capture, &prefix)?;
        let catalog = Box::new(WorkingPagedVectorCatalog { index: index.clone(), prefix, end, source,
            capture, counts, catalog: self.catalog.clone() });
        Ok(Some(contextdb_vector::store::PagedVectorCatalogSelection { catalog, control }))
    }
    fn create(&self, partition: &VectorPartitionRef, mut workspace: VectorCopyAdmission, _retained: VectorCopyAdmission)
        -> Result<(Arc<dyn RawVectorMembershipSource>, Arc<dyn DormantRawVectorLoader>)> {
        if partition.index != self.index { return Err(Error::Other("working vector partition escaped its declared factory".into())); }
        let control = self.catalog.source_control::<WorkingVectorPartitionSource>(None)?;
        let partition = self.catalog.copy_partition(&partition.index, &partition.partition_key)?;
        let summary = self.catalog.point_summary(&partition.value, &mut workspace)?;
        let source = Arc::new(WorkingVectorPartitionSource {
            partition: partition.value, catalog: self.catalog.clone(), owner: WorkingPartitionOwner::Original { control: control.clone(), summary,
                _partition_memory: partition.allocation },
        });
        Ok((source.clone(), source))
    }
}

struct WorkingVectorCursor {
    capture: super::received_working_source::WorkingVectorCapture,
    source: super::received_working_source::WorkingReadSource,
    selected: Option<writer_source::WriterWindow<VectorPartitionMembershipRecord>>,
    prefix: Vec<u8>,
    end: Option<Vec<u8>>,
    after: Option<(Vec<u8>, contextdb_core::read_memory::ReadCredit)>,
    _bounds: contextdb_core::read_memory::ReadCredit,
}
impl WorkingVectorPartitionSource {
    fn source(&self) -> Result<super::received_working_source::WorkingReadSource> {
        match &self.owner {
            WorkingPartitionOwner::Original { .. } => self.catalog.source(),
            WorkingPartitionOwner::Query { source, capture, .. } => {
                source.check_vector_capture(capture)?; Ok(source.clone())
            },
        }
    }
    fn capture_working_serving(&self, snapshot: SnapshotId, owner: &mut VectorCopyAdmission,
        before: &mut dyn FnMut() -> bool) -> Result<contextdb_vector::store::CapturedVectorServingSelection> {
        use contextdb_vector::store::{CapturedVectorStreamedTail, CapturedVectorStreamedTailHandle,
            QuerySourceControl, RawVectorInspectionSnapshot, VectorGraphGeneration};
        if !before() { return Err(Error::ReadCancelled); }
        let source = self.source()?;
        let capture = source.capture_vector_catalog()?;
        let mut route = owner.scratch();
        let length = self.partition.index.table.len().checked_add(self.partition.index.column.len())
            .and_then(|n| n.checked_add(16)).ok_or_else(summary_corruption)?;
        route.reserve(length)?;
        let mut key = Vec::with_capacity(length);
        for name in [&self.partition.index.table, &self.partition.index.column] {
            key.extend_from_slice(&(name.len() as u64).to_be_bytes()); key.extend_from_slice(name.as_bytes());
        }
        if key.capacity() != length { return Err(summary_corruption()); }
        let origin = source.read_vector_lineage(&capture, &key)?.ok_or_else(||
            Error::VectorIndexedRouteUnavailable { index: self.partition.index.clone() })?;
        if origin.value.table != self.partition.index.table { return Err(summary_corruption()); }
        let root = source.root();
        root.check_working_scope(&origin.value.table)?;
        // The same-write lineage proves key renames; the immutable root proves
        // the actual declaration and source identity those keys came from.
        if !root.captured_table_metadata().any(|(table, meta)| table == origin.value.table
            && meta.columns.iter().any(|column| column.name == origin.value.column
                && matches!(column.column_type, ColumnType::Vector(_)))) { return Err(summary_corruption()); }
        let metadata_memory = source.vector_reader_memory()?;
        let read_scope = metadata_memory.enter();
        route.reserve(self.partition.partition_key.estimated_bytes().saturating_mul(2)
            .saturating_add(origin.value.table.len()).saturating_add(origin.value.column.len()).saturating_add(1024))?;
        let saved_partition = VectorPartitionRef::new(origin.value.clone(), copy_read_partition(&self.partition.partition_key, &mut route)?);
        if !before() { return Err(Error::ReadCancelled); }
        let read = root.owner_read()?;
        let generations = RedbPersistence::vector_partition_generation_descriptors_in_read(
            &read, &saved_partition.index, &saved_partition.partition_key)?;
        let Some((base, change)) = generations else {
            return Err(Error::VectorIndexedRouteUnavailable { index: self.partition.index.clone() });
        };
        let frontier = change.unwrap_or(base);
        if frontier.covered_tx > TxId::from_snapshot(snapshot) {
            return Err(Error::VectorIndexedRouteUnavailable { index: self.partition.index.clone() });
        }
        let summary = self.current_summary(owner)?;
        let mut paid = owner.scratch();
        paid.reserve(contextdb_tx::staged_source::source_arc_layout::<WorkingVectorStreamedTail>().size()
            .saturating_add(self.partition.index.table.len()).saturating_add(self.partition.index.column.len())
            .saturating_add(self.partition.partition_key.estimated_bytes().saturating_sub(std::mem::size_of::<VectorPartitionKey>())))?;
        let control = QuerySourceControl::new(paid)?;
        let tail = WorkingVectorStreamedTail {
            working: WorkingVectorPartitionSource { partition: self.partition.clone(), catalog: self.catalog.clone(),
                owner: WorkingPartitionOwner::Query { control: control.clone(), source: source.clone(), capture, summary } },
            origin, capture, covered_tx: frontier.covered_tx, covered_lsn: frontier.covered_lsn,
            _metadata_memory: metadata_memory.clone(),
        };
        let mut pending = 0usize;
        let mut proof = owner.scratch();
        let mut cursor_memory = owner.scratch();
        let mut failure = None;
        let opened = tail.working.cursor(None, &mut |request| match request {
            DormantVectorLoadRequest::Reserve(bytes) => match cursor_memory.reserve(bytes) {
                Ok(()) => true, Err(error) => { failure = Some(error); false },
            },
            _ => before(),
        });
        if let Some(error) = failure.take() { return Err(error); }
        let mut cursor = opened?;
        loop {
            if !before() { return Err(Error::ReadCancelled); }
            let next = cursor.next_with_body_identity(&mut |request| match request {
                DormantVectorLoadRequest::Reserve(bytes) => match cursor_memory.reserve(bytes) {
                    Ok(()) => true, Err(error) => { failure = Some(error); false },
                },
                _ => before(),
            });
            if let Some(error) = failure.take() { return Err(error); }
            let Some((interval, body)) = next? else { break; };
            if tail.includes(&interval, &body, snapshot, &mut proof, before)? {
                pending = pending.checked_add(1).ok_or_else(summary_corruption)?;
            }
        }
        drop(cursor);
        let identity = |generation: VectorPartitionGenerationDescriptor| VectorGraphGeneration {
            generation_id: generation.generation_id, covered_tx: generation.covered_tx,
            covered_lsn: generation.covered_lsn, durable_bytes: generation.durable_bytes,
            policy_revision: generation.policy_revision, hnsw_m: generation.hnsw_m,
            hnsw_ef_construction: generation.hnsw_ef_construction,
        };
        let inspection = RawVectorInspectionSnapshot::new(true, summary, Some(identity(base)), change.map(identity),
            pending, summary.entries.saturating_sub(summary.live_entries), None);
        let saved = crate::database::vector_captured_serving::captured_serving_selection(
            crate::database::vector_captured_serving::CapturedVectorRootLease::writable(read, root)?,
            &saved_partition, generations, inspection, snapshot, owner)?;
        drop(read_scope);
        if let Some(error) = metadata_memory.take_failure() { drop(saved); return Err(error); }
        source.check_vector_capture(&capture)?;
        saved.with_streamed_tail(CapturedVectorStreamedTailHandle::new(Arc::new(tail), control))
    }

    fn cursor(&self, row: Option<RowId>, request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool) -> Result<WorkingVectorCursor> {
        if !request(DormantVectorLoadRequest::Reserve(std::mem::size_of::<WorkingVectorCursor>())) { return Err(Error::ReadCancelled); }
        let source = self.source()?;
        let capture = source.capture_vector_catalog()?;
        let partition = partition_length(&self.partition.partition_key)?;
        let length = self.partition.index.table.len().checked_add(self.partition.index.column.len())
            .and_then(|n| n.checked_add(partition)).and_then(|n| n.checked_add(24 + if row.is_some() { 8 } else { 0 }))
            .ok_or_else(|| Error::Other("working vector cursor identity overflow".into()))?;
        let bounds = contextdb_core::read_memory::ReadCredit::try_new(source.reader_budget(),
            length.checked_mul(2).ok_or_else(|| Error::Other("working vector cursor identity overflow".into()))?,
            "reader", "working_vector_cursor", "Raise the declared reader memory limit.")?;
        let mut prefix = Vec::with_capacity(length);
        for name in [&self.partition.index.table, &self.partition.index.column] {
            prefix.extend_from_slice(&(name.len() as u64).to_be_bytes()); prefix.extend_from_slice(name.as_bytes());
        }
        prefix.extend_from_slice(&(partition as u64).to_be_bytes()); append_partition(&mut prefix, &self.partition.partition_key);
        if let Some(row) = row { prefix.extend_from_slice(&row.0.to_be_bytes()); }
        if prefix.capacity() != length || prefix.len() != length { return Err(Error::Other("working vector cursor capacity differs from admission".into())); }
        let end = writer_source::prefix_end(&prefix);
        Ok(WorkingVectorCursor { capture, source, selected: None, prefix, end, after: None, _bounds: bounds })
    }
    fn find(&self, row: Option<RowId>, before: &mut dyn FnMut() -> bool,
        owner: &mut VectorCopyAdmission,
        mut accept: impl FnMut(&VectorPartitionMembershipRecord) -> bool,
    ) -> Result<Option<writer_source::WriterWindow<VectorPartitionMembershipRecord>>> {
        let mut scratch = owner.scratch();
        let mut failure = None;
        let opened = self.cursor(row, &mut |request| match request {
            DormantVectorLoadRequest::Reserve(bytes) => match scratch.reserve(bytes) {
                Ok(()) => true,
                Err(error) => { failure = Some(error); false },
            },
            _ => before(),
        });
        if let Some(error) = failure { return Err(error); }
        let mut cursor = opened?;
        while let Some(window) = cursor.next_member(&mut |_| before())? {
            if accept(&window.value) { return Ok(Some(window)); }
        }
        Ok(None)
    }
}
impl WorkingVectorCursor {
    fn next_member(&mut self, request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool)
        -> Result<Option<writer_source::WriterWindow<VectorPartitionMembershipRecord>>> {
        self.selected = None;
        if !request(DormantVectorLoadRequest::Checkpoint) { return Err(Error::ReadCancelled); }
        self.source.check_vector_capture(&self.capture)?;
        let lower = self.after.as_ref().map_or(Bound::Included(self.prefix.as_slice()), |after| Bound::Excluded(after.0.as_slice()));
        let upper = self.end.as_ref().map_or(Bound::Unbounded, |end| Bound::Excluded(end.as_slice()));
        let window = self.source.read_vector_membership(lower, upper)?;
        self.source.check_vector_capture(&self.capture)?;
        let Some(window) = window else { return Ok(None); };
        let credit = contextdb_core::read_memory::ReadCredit::try_new(self.source.reader_budget(), window.position().len(),
            "reader", "working_vector_position", "Raise the declared reader memory limit.")?;
        let key = window.position().to_vec();
        self.after = Some((key, credit));
        Ok(Some(window))
    }
}
fn raw_body_identity(member: &VectorPartitionMembershipRecord) -> RawVectorDirectoryEntry {
    vector_membership_source::body_identity_for_record(member)
}
fn raw_interval(member: &VectorPartitionMembershipRecord) -> RawVectorDirectoryEntry {
    let mut identity = raw_body_identity(member); identity.created_tx = member.visible_from; identity
}
fn visible_member(member: &VectorPartitionMembershipRecord, snapshot: SnapshotId) -> bool {
    member.visible_from.0 <= snapshot.0 && member.deleted_tx.is_none_or(|tx| tx.0 > snapshot.0)
}
impl RawVectorMembershipCursor for WorkingVectorCursor {
    fn selected_receipt(&self) -> Option<&dyn std::any::Any> { self.selected.as_ref().map(|_| self as &dyn std::any::Any) }
    fn next(&mut self, request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool) -> Result<Option<RawVectorDirectoryEntry>> {
        self.next_with_body_identity(request).map(|next| next.map(|(interval, _)| interval))
    }
    fn next_with_body_identity(&mut self, request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool)
        -> Result<Option<(RawVectorDirectoryEntry, RawVectorDirectoryEntry)>> {
        let Some(window) = self.next_member(request)? else { return Ok(None); };
        let identities = (raw_interval(&window.value), raw_body_identity(&window.value));
        self.selected = Some(window);
        Ok(Some(identities))
    }
}
struct WorkingVectorSelection {
    capture: super::received_working_source::WorkingVectorCapture,
    source: super::received_working_source::WorkingReadSource,
    member: writer_source::WriterWindow<VectorPartitionMembershipRecord>,
}
impl RawVectorMembershipSelection for WorkingVectorSelection {
    fn identity(&self) -> RawVectorDirectoryEntry { raw_body_identity(&self.member.value) }
    fn as_any(&self) -> &dyn std::any::Any { self }
}
impl RawVectorMembershipSource for WorkingVectorPartitionSource {
    fn for_renamed_index(&self, index: &VectorIndexRef, _owner: &mut VectorCopyAdmission, _retained: VectorCopyAdmission)
        -> Result<Arc<dyn RawVectorMembershipSource>> {
        if let WorkingPartitionOwner::Query { .. } = &self.owner {
            return Err(Error::Other("working vector query selection must be recaptured after schema rebind".into()));
        }
        let summary = RawVectorMembershipSource::current_summary(self, _owner)?;
        let control = self.catalog.source_control::<WorkingVectorPartitionSource>(None)?;
        let partition = self.catalog.copy_partition(index, &self.partition.partition_key)?;
        Ok(Arc::new(WorkingVectorPartitionSource {
            partition: partition.value, catalog: self.catalog.clone(), owner: WorkingPartitionOwner::Original { control: control.clone(), summary,
                _partition_memory: partition.allocation },
        }))
    }
    fn native_source_control(&self) -> Option<contextdb_tx::SourceCharge> {
        match &self.owner { WorkingPartitionOwner::Original { control, .. } => Some(control.clone()), _ => None }
    }
    fn query_source_control(&self) -> Option<contextdb_vector::store::QuerySourceControl> {
        match &self.owner { WorkingPartitionOwner::Query { control, .. } => Some(control.clone()), _ => None }
    }
    fn visit_retained_copy_credits(&self, caller: &dyn MemoryBudget, visit: &mut dyn FnMut(usize, usize, bool)) {
        if let WorkingPartitionOwner::Query { control, .. } = &self.owner { control.visit_retained_copy_credit(caller, visit); }
    }
    fn published_summary(&self) -> RawVectorMembershipSummary {
        match &self.owner { WorkingPartitionOwner::Query { summary, .. } => *summary,
            WorkingPartitionOwner::Original { summary, .. } => *summary }
    }
    fn current_summary(&self, owner: &mut VectorCopyAdmission) -> Result<RawVectorMembershipSummary> {
        match &self.owner {
            WorkingPartitionOwner::Query { summary, .. } => { self.source()?; Ok(*summary) },
            WorkingPartitionOwner::Original { .. } => self.catalog.point_summary(&self.partition, owner),
        }
    }
    fn visible_count(&self, snapshot: SnapshotId, before: &mut dyn FnMut() -> bool, owner: &mut VectorCopyAdmission) -> Result<usize> {
        if !before() { return Err(Error::ReadCancelled); }
        let summary = self.current_summary(owner)?;
        // max_tx includes every interval start and close. At or beyond it,
        // the maintained same-image live scalar is the exact snapshot count.
        if snapshot.0 >= summary.max_tx.0 { return Ok(summary.live_entries); }
        let mut count = 0usize;
        self.find(None, before, owner, |member| { if visible_member(member, snapshot) { count = count.saturating_add(1); } false })?;
        Ok(count)
    }
    fn visible_by_row(&self, row: RowId, snapshot: SnapshotId, before: &mut dyn FnMut() -> bool, owner: &mut VectorCopyAdmission)
        -> Result<Option<RawVectorDirectoryEntry>> {
        // Preserve the newest owning-row interval, rather than returning the
        // first historical placement that happens to remain visible.
        let mut found = None::<RawVectorDirectoryEntry>;
        self.find(Some(row), before, owner, |member| {
            if visible_member(member, snapshot) && found.as_ref().is_none_or(|seen|
                (member.row_created_tx, member.row_lsn, member.vector_created_tx, member.vector_lsn)
                    > (seen.membership.owning_row_tx, seen.membership.row_lsn, seen.created_tx, seen.lsn)) {
                found = Some(raw_body_identity(member));
            }
            false
        })?;
        Ok(found)
    }
    fn contains_row_lsn(&self, row: RowId, lsn: Lsn, before: &mut dyn FnMut() -> bool, owner: &mut VectorCopyAdmission) -> Result<bool> {
        Ok(self.identity_by_row_lsn(row, lsn, before, owner)?.is_some())
    }
    fn identity_by_row_lsn(&self, row: RowId, lsn: Lsn, before: &mut dyn FnMut() -> bool, owner: &mut VectorCopyAdmission)
        -> Result<Option<RawVectorDirectoryEntry>> {
        Ok(self.find(Some(row), before, owner, |member| member.vector_lsn == lsn)?.map(|window| raw_body_identity(&window.value)))
    }
    fn visit_row_versions(&self, row: RowId, before: &mut dyn FnMut() -> bool,
        visit: &mut dyn FnMut(RawVectorDirectoryEntry) -> Result<()>, owner: &mut VectorCopyAdmission) -> Result<()> {
        let mut failure = None;
        self.find(Some(row), before, owner, |member| { if let Err(error) = visit(raw_body_identity(member)) { failure = Some(error); true } else { false } })?;
        failure.map_or(Ok(()), Err)
    }
    fn visit_identities(&self, before: &mut dyn FnMut() -> bool,
        visit: &mut dyn FnMut(RawVectorDirectoryEntry) -> Result<()>, owner: &mut VectorCopyAdmission) -> Result<()> {
        let mut failure = None;
        self.find(None, before, owner, |member| { if let Err(error) = visit(raw_body_identity(member)) { failure = Some(error); true } else { false } })?;
        failure.map_or(Ok(()), Err)
    }
    fn open(&self, request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool) -> Result<Box<dyn RawVectorMembershipCursor>> {
        Ok(Box::new(self.cursor(None, request)?))
    }
    fn supports_root_selection(&self) -> bool { true }
    fn select_visible_by_row(&self, row: RowId, snapshot: SnapshotId, before: &mut dyn FnMut() -> bool,
        owner: &mut VectorCopyAdmission) -> Result<Option<Box<dyn RawVectorMembershipSelection>>> {
        let Some(identity) = self.visible_by_row(row, snapshot, before, owner)? else { return Ok(None); };
        let Some(member) = self.find(Some(row), before, owner, |member| raw_body_identity(member) == identity)? else {
            return Err(Error::Other("working vector selection disappeared".into()));
        };
        owner.reserve(std::mem::size_of::<WorkingVectorSelection>())?;
        let source = self.source()?;
        let capture = source.capture_vector_catalog()?;
        Ok(Some(Box::new(WorkingVectorSelection { capture, source, member })))
    }
    fn select_identity_by_row_lsn(&self, row: RowId, lsn: Lsn, before: &mut dyn FnMut() -> bool,
        owner: &mut VectorCopyAdmission) -> Result<Option<Box<dyn RawVectorMembershipSelection>>> {
        let Some(member) = self.find(Some(row), before, owner, |member| member.vector_lsn == lsn)? else { return Ok(None); };
        owner.reserve(std::mem::size_of::<WorkingVectorSelection>())?;
        let source = self.source()?;
        let capture = source.capture_vector_catalog()?;
        Ok(Some(Box::new(WorkingVectorSelection { capture, source, member })))
    }
    fn capture_serving_selection(&self, snapshot: SnapshotId, _base: Option<&contextdb_vector::store::VectorGraphGeneration>,
        _change: Option<&contextdb_vector::store::VectorGraphGeneration>, owner: &mut VectorCopyAdmission)
        -> Result<contextdb_vector::store::CapturedVectorServingSelection> {
        self.capture_working_serving(snapshot, owner, &mut || true)
    }
    fn capture_serving_selection_with_work(&self, snapshot: SnapshotId,
        _base: Option<&contextdb_vector::store::VectorGraphGeneration>,
        _change: Option<&contextdb_vector::store::VectorGraphGeneration>, owner: &mut VectorCopyAdmission,
        before: &mut dyn FnMut() -> bool) -> Result<contextdb_vector::store::CapturedVectorServingSelection> {
        self.capture_working_serving(snapshot, owner, before)
    }
}

fn reverse_membership_key_original(frame: &OriginalPersistenceFrame<'_>, member: &VectorPartitionMembershipRecord) -> Result<Vec<u8>> {
    let partition = partition_length(&member.partition_key)?;
    let length = member.index.table.len().checked_add(member.index.column.len())
        .and_then(|n| n.checked_add(partition)).and_then(|n| n.checked_add(64))
        .ok_or_else(|| Error::Other("working vector reverse identity overflow".into()))?;
    frame.reserve(length)?;
    let mut key = Vec::with_capacity(length);
    for name in [&member.index.table, &member.index.column] {
        key.extend_from_slice(&(name.len() as u64).to_be_bytes()); key.extend_from_slice(name.as_bytes());
    }
    for coordinate in [member.row_id.0, member.row_created_tx.0, member.row_lsn.0] { key.extend_from_slice(&coordinate.to_be_bytes()); }
    key.extend_from_slice(&(partition as u64).to_be_bytes()); append_partition(&mut key, &member.partition_key);
    key.extend_from_slice(&member.vector_created_tx.0.to_be_bytes()); key.extend_from_slice(&member.vector_lsn.0.to_be_bytes());
    if key.len() != length || key.capacity() != length { return Err(Error::Other("working reverse key capacity differs from admission".into())); }
    Ok(key)
}
pub(super) fn reverse_membership_key_matches(mut bytes: &[u8], member: &VectorPartitionMembershipRecord) -> bool {
    if component(&mut bytes) != Some(member.index.table.as_bytes())
        || component(&mut bytes) != Some(member.index.column.as_bytes())
        || !consume(&mut bytes, &member.row_id.0.to_be_bytes())
        || !consume(&mut bytes, &member.row_created_tx.0.to_be_bytes())
        || !consume(&mut bytes, &member.row_lsn.0.to_be_bytes()) { return false; }
    let Some(partition) = component(&mut bytes) else { return false; };
    partition_key_matches(partition, &member.partition_key)
        && consume(&mut bytes, &member.vector_created_tx.0.to_be_bytes())
        && consume(&mut bytes, &member.vector_lsn.0.to_be_bytes()) && bytes.is_empty()
}

struct WorkingVectorReverseCursor {
    capture: super::received_working_source::WorkingVectorCapture,
    source: super::received_working_source::WorkingReadSource,
    prefix: Vec<u8>, end: Option<Vec<u8>>,
    before_key: Option<(Vec<u8>, contextdb_core::read_memory::ReadCredit)>,
    _bounds: contextdb_core::read_memory::ReadCredit,
}
impl WorkingVectorReverseSource {
    fn cursor(&self, row: RowId) -> Result<WorkingVectorReverseCursor> {
        let source = self.catalog.source()?;
        let capture = source.capture_vector_catalog()?;
        let length = self.index.table.len().checked_add(self.index.column.len()).and_then(|n| n.checked_add(24))
            .ok_or_else(|| Error::Other("working vector reverse cursor overflow".into()))?;
        let bounds = contextdb_core::read_memory::ReadCredit::try_new(source.reader_budget(),
            length.checked_mul(2).ok_or_else(|| Error::Other("working reverse bounds overflow".into()))?,
            "reader", "working_vector_reverse_cursor", "Raise the declared reader memory limit.")?;
        let prefix = vector_membership_pages::reverse_row_prefix(&self.index, row);
        let end = writer_source::prefix_end(&prefix);
        Ok(WorkingVectorReverseCursor { capture, source, prefix, end, before_key: None, _bounds: bounds })
    }
}
impl WorkingVectorReverseCursor {
    fn next_member(&mut self, before: &mut dyn FnMut() -> bool)
        -> Result<Option<writer_source::WriterWindow<VectorPartitionMembershipRecord>>> {
        if !before() { return Err(Error::ReadCancelled); }
        self.source.check_vector_capture(&self.capture)?;
        let upper = self.before_key.as_ref().map(|key| key.0.as_slice()).or(self.end.as_deref())
            .map_or(Bound::Unbounded, Bound::Excluded);
        let window = self.source.read_vector_reverse_membership(Bound::Included(self.prefix.as_slice()), upper)?;
        self.source.check_vector_capture(&self.capture)?;
        let Some(window) = window else { return Ok(None); };
        let credit = contextdb_core::read_memory::ReadCredit::try_new(self.source.reader_budget(), window.position().len(),
            "reader", "working_vector_reverse_position", "Raise the declared reader memory limit.")?;
        self.before_key = Some((window.position().to_vec(), credit));
        Ok(Some(window))
    }
}
fn copy_read_partition(key: &VectorPartitionKey, owner: &mut VectorCopyAdmission) -> Result<VectorPartitionKey> {
    owner.reserve(key.estimated_bytes().checked_mul(2).ok_or_else(|| Error::Other("working read partition size overflow".into()))?)?;
    let mut components = Vec::with_capacity(key.components().len());
    components.extend(key.components().iter().cloned());
    Ok(VectorPartitionKey::from_components(components))
}
impl contextdb_vector::store::RawVectorReverseMembershipCursor for WorkingVectorReverseCursor {
    fn next(&mut self, before: &mut dyn FnMut() -> bool, owner: &mut VectorCopyAdmission) -> Result<Option<VectorPartitionKey>> {
        self.next_member(before)?.map(|window| copy_read_partition(&window.value.partition_key, owner)).transpose()
    }
}
impl RawVectorReverseMembershipSource for WorkingVectorReverseSource {
    fn for_renamed_index(&self, index: &VectorIndexRef, _owner: &mut VectorCopyAdmission, _retained: VectorCopyAdmission)
        -> Result<Arc<dyn RawVectorReverseMembershipSource>> {
        let control = self.catalog.source_control::<WorkingVectorReverseSource>(Some(index))?;
        Ok(Arc::new(WorkingVectorReverseSource {
            index: VectorIndexRef { table: crate::memory_accounting::preparation_copy_string(&index.table)?,
                column: crate::memory_accounting::preparation_copy_string(&index.column)? },
            catalog: self.catalog.clone(), control: control.clone(),
        }))
    }
    fn native_source_control(&self) -> Option<contextdb_tx::SourceCharge> { Some(self.control.clone()) }
    fn visible_partition_admitted(&self, row: RowId, snapshot: SnapshotId, before: &mut dyn FnMut() -> bool,
        owner: &mut VectorCopyAdmission) -> Result<Option<VectorPartitionKey>> {
        let mut cursor = self.cursor(row)?;
        while let Some(window) = cursor.next_member(before)? {
            if visible_member(&window.value, snapshot) { return copy_read_partition(&window.value.partition_key, owner).map(Some); }
        }
        Ok(None)
    }
    fn open_row(&self, row: RowId, before: &mut dyn FnMut() -> bool, owner: &mut VectorCopyAdmission)
        -> Result<Box<dyn contextdb_vector::store::RawVectorReverseMembershipCursor>> {
        if !before() { return Err(Error::ReadCancelled); }
        owner.reserve(std::mem::size_of::<WorkingVectorReverseCursor>())?;
        Ok(Box::new(self.cursor(row)?))
    }
}

impl WorkingVectorPartitionSource {
    fn selected_member(&self, identity: &RawVectorDirectoryEntry, before: &mut dyn FnMut() -> bool,
        owner: &mut VectorCopyAdmission) -> Result<writer_source::WriterWindow<VectorPartitionMembershipRecord>> {
        self.find(Some(identity.row_id), before, owner, |member| raw_body_identity(member) == *identity)?
            .ok_or_else(|| Error::StoreCorrupted { path: "received working vectors".into(), reason: "body request lost its exact selected placement".into() })
    }
    fn body_copy_bytes(body: &VectorEntry) -> Result<usize> {
        body.vector.len().checked_mul(std::mem::size_of::<f32>())
            .and_then(|n| n.checked_add(body.index.table.len())).and_then(|n| n.checked_add(body.index.column.len()))
            .ok_or_else(|| Error::Other("working vector copy size overflow".into()))
    }
    fn resident_budget(&self) -> Result<Arc<dyn MemoryBudget>> {
        match &self.owner {
            WorkingPartitionOwner::Query { control, .. } => Ok(control.funder()),
            WorkingPartitionOwner::Original { .. } =>
                crate::memory_accounting::persistence_original_projection(&self.catalog.catalog.storage.lock())?.vector_budget(),
        }
    }
    fn candidate_from_member(&self, source: &super::received_working_source::WorkingReadSource,
        member: &VectorPartitionMembershipRecord, caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<contextdb_vector::store::LoadedRawVectorCandidate> {
        let capture = source.capture_vector_catalog()?;
        if !request(DormantVectorLoadRequest::Checkpoint) { return Err(Error::ReadCancelled); }
        if member.index != self.partition.index || member.partition_key != self.partition.partition_key {
            return Err(Error::Other("working vector candidate escaped its source partition".into()));
        }
        source.check_vector_capture(&capture)?;
        self.source()?;
        let body = source.read_vector_body(member)?;
        source.check_vector_capture(&capture)?;
        let bytes = Self::body_copy_bytes(&body.value)?;
        let mut loader_charge = if caller_charged { None } else {
            let mut charge = VectorCopyAdmission::new(self.resident_budget()?); charge.reserve(bytes)?; Some(charge)
        };
        if caller_charged && !request(DormantVectorLoadRequest::Reserve(bytes)) { return Err(Error::ReadCancelled); }
        let mut entry = body.value.clone();
        entry.deleted_tx = member.deleted_tx;
        Ok(if let Some(charge) = loader_charge.take() {
            let budget = charge.funder(); charge.transfer();
            contextdb_vector::store::LoadedRawVectorCandidate::loader_charged(entry, bytes, budget)
        } else { contextdb_vector::store::LoadedRawVectorCandidate::caller_charged(entry, bytes) })
    }
}
impl DormantRawVectorLoader for WorkingVectorPartitionSource {
    fn for_renamed_index(&self, index: &VectorIndexRef, _owner: &mut VectorCopyAdmission, _retained: VectorCopyAdmission)
        -> Result<Arc<dyn DormantRawVectorLoader>> {
        if let WorkingPartitionOwner::Query { .. } = &self.owner {
            return Err(Error::Other("working vector query selection must be recaptured after schema rebind".into()));
        }
        let summary = RawVectorMembershipSource::current_summary(self, _owner)?;
        let control = self.catalog.source_control::<WorkingVectorPartitionSource>(None)?;
        let partition = self.catalog.copy_partition(index, &self.partition.partition_key)?;
        Ok(Arc::new(WorkingVectorPartitionSource {
            partition: partition.value, catalog: self.catalog.clone(), owner: WorkingPartitionOwner::Original { control: control.clone(), summary,
                _partition_memory: partition.allocation },
        }))
    }
    fn native_source_control(&self) -> Option<contextdb_tx::SourceCharge> {
        match &self.owner { WorkingPartitionOwner::Original { control, .. } => Some(control.clone()), _ => None }
    }
    fn query_source_control(&self) -> Option<contextdb_vector::store::QuerySourceControl> {
        match &self.owner { WorkingPartitionOwner::Query { control, .. } => Some(control.clone()), _ => None }
    }
    fn visit_retained_copy_credits(&self, caller: &dyn MemoryBudget, visit: &mut dyn FnMut(usize, usize, bool)) {
        if let WorkingPartitionOwner::Query { control, .. } = &self.owner { control.visit_retained_copy_credit(caller, visit); }
    }
    fn supports_bounded_candidate_load(&self) -> bool { true }
    fn load(&self, directory: &[RawVectorDirectoryEntry], owner: &mut VectorCopyAdmission)
        -> Result<contextdb_vector::store::LoadedRawVectorPartition> {
        let budget = self.resident_budget()?;
        let mut resident = VectorCopyAdmission::new(budget.clone());
        let mut workspace = owner.scratch();
        workspace.reserve(std::alloc::Layout::array::<contextdb_vector::store::LoadedRawVectorEntry>(directory.len())
            .map_err(|_| Error::Other("working loaded vector controls overflow".into()))?.size())?;
        let mut entries = Vec::with_capacity(directory.len());
        let mut resident_bytes = 0usize;
        let source = self.source()?;
        for identity in directory {
            let member = self.selected_member(identity, &mut || true, &mut workspace)?;
            let body = source.read_vector_body(&member.value)?;
            let bytes = Self::body_copy_bytes(&body.value)?;
            resident.reserve(bytes)?;
            resident_bytes = resident_bytes.checked_add(bytes).ok_or_else(|| Error::Other("working loaded body size overflow".into()))?;
            let canonical_deleted = body.value.deleted_tx;
            let mut entry = body.value.clone(); entry.deleted_tx = member.value.deleted_tx;
            entries.push(contextdb_vector::store::LoadedRawVectorEntry::new(entry,
                VectorMembershipProvenance::new(member.value.row_created_tx, member.value.row_lsn,
                    member.value.visible_from, member.value.deleted_tx), canonical_deleted));
        }
        resident.transfer();
        Ok(contextdb_vector::store::LoadedRawVectorPartition { entries, resident_bytes, workspace_owner: workspace, accountant: budget })
    }
    fn load_candidate(&self, identity: &RawVectorDirectoryEntry, caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool) -> Result<contextdb_vector::store::LoadedRawVectorCandidate> {
        let mut scratch = VectorCopyAdmission::new(self.resident_budget()?);
        let member = self.selected_member(identity, &mut || request(DormantVectorLoadRequest::Checkpoint), &mut scratch)?;
        self.candidate_from_member(&self.source()?, &member.value, caller_charged, request)
    }
    fn load_selected_candidate(&self, selected: &dyn RawVectorMembershipSelection, caller_charged: bool,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool) -> Result<contextdb_vector::store::LoadedRawVectorCandidate> {
        let selected = selected.as_any().downcast_ref::<WorkingVectorSelection>()
            .ok_or_else(|| Error::Other("working vector loader requires its original selection receipt".into()))?;
        selected.source.check_vector_capture(&selected.capture)?;
        if !selected.source.same_working_source(&self.source()?) {
            return Err(Error::Other("working vector selection belongs to another original loan".into()));
        }
        self.candidate_from_member(&selected.source, &selected.member.value, caller_charged, request)
    }
    fn bind_streamed_membership(&self, _scan: &mut dyn contextdb_vector::store::RawVectorBodyScan, receipt: &dyn std::any::Any) -> Result<()> {
        let cursor = receipt.downcast_ref::<WorkingVectorCursor>()
            .ok_or_else(|| Error::Other("working vector loader requires its original cursor receipt".into()))?;
        cursor.source.check_vector_capture(&cursor.capture)?;
        self.source()?;
        let member = cursor.selected.as_ref().ok_or_else(|| Error::Other("working vector cursor has no selected placement".into()))?;
        if member.value.index != self.partition.index || member.value.partition_key != self.partition.partition_key {
            return Err(Error::Other("working vector cursor belongs to another partition".into()));
        }
        Ok(())
    }
}

const WORKING_VECTOR_REWRITE_BODIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_rewrite_bodies");
const WORKING_VECTOR_REWRITE_MEMBERS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_rewrite_members");
const WORKING_VECTOR_REWRITE_FLAGS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("__received_working_vector_rewrite_flags");

pub(crate) struct WorkingVectorIdentity<'a> {
    pub(crate) index: &'a mut VectorIndexRef,
    pub(crate) row_id: &'a mut RowId,
    pub(crate) created_tx: &'a mut TxId,
    pub(crate) deleted_tx: &'a mut Option<TxId>,
    pub(crate) lsn: &'a mut Lsn,
}

impl ReceivedWorkingWrite {
    /// Source and destination are distinct, so changing row/version/index
    /// coordinates cannot revisit a newly written key or skip an old one.
    pub(crate) fn rewrite_vector_image(
        &self, original: &crate::memory_accounting::OwnedMemoryReservation,
        mut body_change: impl FnMut(&OriginalPersistenceFrame<'_>, &mut WorkingVectorIdentity<'_>) -> Result<bool>,
        mut member_change: impl FnMut(&OriginalPersistenceFrame<'_>, &mut VectorPartitionMembershipRecord) -> Result<bool>,
    ) -> Result<()> {
        self.require_original(original)?;
        if !self.vectors_prepared()? { return Ok(()); }
        for definition in [WORKING_VECTOR_REWRITE_BODIES, WORKING_VECTOR_REWRITE_MEMBERS, WORKING_VECTOR_REWRITE_FLAGS] {
            let frame = OriginalPersistenceFrame::new(original)?;
            frame.reserve(definition.name().len())?;
            self.keep_scratch_name(crate::memory_accounting::preparation_copy_string(definition.name())?, frame.into_reservation())?;
            self.with_write(|write| {
                match write.delete_table(definition) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {},
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                }
                write.open_table(definition).map(|_| ()).map_err(RedbPersistence::storage_error)
            })?;
        }
        let mut after = None::<VectorPosition>;
        loop {
            let frame = OriginalPersistenceFrame::new(original)?;
            let lower = after.as_ref().map_or(Bound::Unbounded, |key| Bound::Excluded(key.key.as_slice()));
            let Some(mut body) = self.with_write(|write| next_body_in_write(original, write, lower, Bound::Unbounded))? else { break; };
            let next = OriginalPersistenceFrame::new(original)?;
            let next_key = next.copy_bytes(body.position())?;
            after = Some(VectorPosition { key: next_key, _owner: next.into_reservation() });
            let VectorEntry { index, row_id, created_tx, deleted_tx, lsn, .. } = &mut body.entry;
            if !body_change(&frame, &mut WorkingVectorIdentity { index, row_id, created_tx, deleted_tx, lsn })? { continue; }
            let mut stored: PersistedVectorEntry = frame.decode(body.encoded())?;
            let encoded;
            let key;
            let unchanged = stored.index == body.entry.index && stored.row_id == body.entry.row_id
                && stored.created_tx == body.entry.created_tx && stored.deleted_tx == body.entry.deleted_tx && stored.lsn == body.entry.lsn;
            let (physical, bytes) = if unchanged { (body.position(), body.encoded()) } else {
                frame.record(&body.entry.index)?;
                stored.index = body.entry.index.clone(); stored.row_id = body.entry.row_id;
                stored.created_tx = body.entry.created_tx; stored.deleted_tx = body.entry.deleted_tx; stored.lsn = body.entry.lsn;
                encoded = frame.encode(&stored)?;
                frame.reserve(stored.index.table.len().saturating_add(stored.index.column.len()).saturating_add(34))?;
                key = RedbPersistence::vector_identity_key(&stored.index, stored.row_id, stored.created_tx, stored.lsn);
                (key.as_slice(), encoded.as_slice())
            };
            self.with_write(|write| {
                write.open_table(WORKING_VECTOR_REWRITE_BODIES).map_err(RedbPersistence::storage_error)?
                    .insert(physical, bytes).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
        }
        after = None;
        loop {
            let frame = OriginalPersistenceFrame::new(original)?;
            let lower = after.as_ref().map_or(Bound::Unbounded, |key| Bound::Excluded(key.key.as_slice()));
            let Some(mut member) = self.next_vector_membership(original, lower, Bound::Unbounded)? else { break; };
            let historical_only = self.with_write(|write| {
                let summaries = write.open_table(WORKING_VECTOR_SUMMARIES).map_err(RedbPersistence::storage_error)?;
                let prefix = &member.position()[..member.position().len().checked_sub(40).ok_or_else(summary_corruption)?];
                let value = summaries.get(prefix).map_err(RedbPersistence::storage_error)?.ok_or_else(summary_corruption)?;
                summary_decode(value.value())?;
                Ok(value.value()[24] != 0)
            })?;
            let next = OriginalPersistenceFrame::new(original)?;
            let key = next.copy_bytes(member.position())?;
            after = Some(VectorPosition { key, _owner: next.into_reservation() });
            if !member_change(&frame, &mut member.record)? { continue; }
            let key = membership_key_original(&frame, &member.record)?;
            let encoded = membership_encoded_original(&frame, &member.record)?;
            self.with_write(|write| {
                write.open_table(WORKING_VECTOR_REWRITE_MEMBERS).map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                write.open_table(WORKING_VECTOR_REWRITE_FLAGS).map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), [u8::from(historical_only)].as_slice()).map_err(RedbPersistence::storage_error)?;
                Ok(())
            })?;
        }
        // Only complete transformed sources replace the active directions.
        self.vector_became_unready();
        self.with_write(|write| {
            for definition in [WORKING_VECTOR_BODIES, WORKING_VECTOR_MEMBERSHIPS, WORKING_VECTOR_REVERSE,
            WORKING_VECTOR_SUMMARIES, WORKING_VECTOR_COUNTS, WORKING_VECTOR_SUMMARY_TX, WORKING_VECTOR_EMPTY] {
                write.delete_table(definition).map_err(RedbPersistence::storage_error)?;
                write.open_table(definition).map_err(RedbPersistence::storage_error)?;
            }
            Ok(())
        })?;
        // Encoded source windows keep representation and checksums intact.
        for (source, destination) in [(WORKING_VECTOR_REWRITE_BODIES, WORKING_VECTOR_BODIES),
            (WORKING_VECTOR_REWRITE_MEMBERS, WORKING_VECTOR_MEMBERSHIPS)] {
            let mut after = None::<VectorPosition>;
            loop {
                let frame = OriginalPersistenceFrame::new(original)?;
                let copied = self.with_write(|write| {
                    let table = write.open_table(source).map_err(RedbPersistence::storage_error)?;
                    let lower = after.as_ref().map_or(Bound::Unbounded, |key| Bound::Excluded(key.key.as_slice()));
                    let mut range = table.range::<&[u8]>((lower, Bound::Unbounded)).map_err(RedbPersistence::storage_error)?;
                    let Some((key, value)) = range.next().transpose().map_err(RedbPersistence::storage_error)? else { return Ok(None); };
                    Ok(Some((frame.copy_bytes(key.value())?, frame.copy_bytes(value.value())?)))
                })?;
                let Some((key, encoded)) = copied else { break; };
                self.with_write(|write| {
                    write.open_table(destination).map_err(RedbPersistence::storage_error)?
                        .insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                    Ok(())
                })?;
                if destination.name() == WORKING_VECTOR_MEMBERSHIPS.name() {
                    let payload = vector_partition_record_payload(VectorPartitionRecordKind::Membership, &encoded)
                        .map_err(|reason| Error::StoreCorrupted { path: "received working vector rewrite".into(), reason })?;
                    let member: VectorPartitionMembershipRecord = frame.decode(payload)?;
                    let reverse = reverse_membership_key_original(&frame, &member)?;
                    let (mut summary_lower, summary_upper) = summary_bounds(&frame, &key)?;
                    let historical_only = self.with_write(|write| {
                        let flags = write.open_table(WORKING_VECTOR_REWRITE_FLAGS).map_err(RedbPersistence::storage_error)?;
                        let value = flags.get(key.as_slice()).map_err(RedbPersistence::storage_error)?.ok_or_else(summary_corruption)?;
                        match value.value() { [0] => Ok(false), [1] => Ok(true), _ => Err(summary_corruption()) }
                    })?;
                    self.with_write(|write| {
                        update_partition_summary(write, &key, &mut summary_lower, &summary_upper, None, &member, historical_only)?;
                        write.open_table(WORKING_VECTOR_REVERSE).map_err(RedbPersistence::storage_error)?
                            .insert(reverse.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                        Ok(())
                    })?;
                }
                let next = OriginalPersistenceFrame::new(original)?;
                let key = next.copy_bytes(&key)?;
                after = Some(VectorPosition { key, _owner: next.into_reservation() });
            }
        }
        self.vector_became_ready();
        Ok(())
    }
}

pub(crate) fn derive_partition_original(frame: &OriginalPersistenceFrame<'_>, index: &VectorIndexRef,
    meta: &TableMeta, column: &ColumnDef, row: &VersionedRow) -> Result<VectorPartitionKey> {
    let columns = column.partition_key_columns.as_deref().unwrap_or(&[]);
    let controls: usize = std::alloc::Layout::array::<Value>(columns.len()).map_err(|_| Error::Other("working partition controls overflow".into()))?.size()
        .checked_add(std::alloc::Layout::array::<contextdb_core::VectorPartitionComponent>(columns.len())
            .map_err(|_| Error::Other("working partition controls overflow".into()))?.size().saturating_mul(2))
        .and_then(|n| n.checked_add(4 * std::mem::size_of::<usize>()))
        .ok_or_else(|| Error::Other("working partition controls overflow".into()))?;
    let bytes = columns.iter().try_fold(controls, |bytes: usize, name: &String| {
        let text = row.values.get(name).and_then(Value::as_text).map_or(0, str::len);
        bytes.checked_add(text.saturating_mul(2)).ok_or_else(|| Error::Other("working partition values overflow".into()))
    })?;
    frame.reserve(bytes)?;
    vector_partition_key_for_row(index, meta, column, row)
}

impl ReceivedWorkingWrite {
    fn original_vector_lineage(&self, frame: &OriginalPersistenceFrame<'_>, key: &[u8])
        -> Result<Option<VectorIndexRef>> {
        let observed = self.with_write(|write| {
            let root = write.normal_table_root(WORKING_VECTOR_LINEAGE.name()).map_err(RedbPersistence::storage_error)?;
            let table = write.open_table(WORKING_VECTOR_LINEAGE).map_err(RedbPersistence::storage_error)?;
            Ok(table.get(key).map_err(RedbPersistence::storage_error)?.map(|value| (root, value.value().len())))
        })?;
        let Some((root, bytes)) = observed else { return Ok(None); };
        frame.reserve(bytes)?;
        let encoded = self.with_write(|write| {
            if write.normal_table_root(WORKING_VECTOR_LINEAGE.name()).map_err(RedbPersistence::storage_error)? != root {
                return Err(summary_corruption());
            }
            let table = write.open_table(WORKING_VECTOR_LINEAGE).map_err(RedbPersistence::storage_error)?;
            let value = table.get(key).map_err(RedbPersistence::storage_error)?.ok_or_else(summary_corruption)?;
            if value.value().len() != bytes { return Err(summary_corruption()); }
            Ok(value.value().to_vec())
        })?;
        if encoded.capacity() != bytes { return Err(summary_corruption()); }
        frame.decode(&encoded).map(Some)
    }

    fn project_vector_lineage(&self, frame: &OriginalPersistenceFrame<'_>, table_name: &str,
        previous: Option<&TableMeta>, arriving: Option<&TableMeta>, renames: &[(String, String)]) -> Result<()> {
        let Some(previous) = previous else { return Ok(()); };
        let count = previous.columns.iter().filter(|column| matches!(column.column_type, ColumnType::Vector(_))).count();
        frame.array::<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)>(count)?;
        let mut updates = Vec::with_capacity(count);
        for old in previous.columns.iter().filter(|column| matches!(column.column_type, ColumnType::Vector(_))) {
            let index = VectorIndexRef { table: frame.copy_text(table_name)?, column: frame.copy_text(&old.name)? };
            let key = index_prefix_original(frame, &index)?;
            let initial = self.original_vector_lineage(frame, &key)?;
            let target_name = renames.iter().find(|(from, _)| *from == old.name)
                .map_or(old.name.as_str(), |(_, to)| to.as_str());
            let target = arriving.and_then(|meta| meta.columns.iter().find(|column| column.name == target_name)
                .map(|column| (meta, column)));
            let next = if let (Some(initial), Some((meta, column))) = (initial, target) {
                let old_keys = old.partition_key_columns.as_deref().unwrap_or(&[]);
                let new_keys = column.partition_key_columns.as_deref().unwrap_or(&[]);
                let compatible_keys = old_keys.len() == new_keys.len() && old_keys.iter().zip(new_keys).all(|(from, to)| {
                    let renamed = renames.iter().find(|(old, _)| old == from).map_or(from.as_str(), |(_, name)| name.as_str());
                    renamed == to && previous.columns.iter().find(|declared| declared.name == *from)
                        .zip(meta.columns.iter().find(|declared| declared.name == *to))
                        .is_some_and(|(old, new)| old.column_type == new.column_type)
                });
                if old.column_type == column.column_type && old.quantization == column.quantization && compatible_keys {
                    let destination = VectorIndexRef { table: frame.copy_text(table_name)?, column: frame.copy_text(&column.name)? };
                    Some((index_prefix_original(frame, &destination)?, frame.encode(&initial)?))
                } else { None }
            } else { None };
            updates.push((key, next));
        }
        if updates.capacity() != count { return Err(summary_corruption()); }
        for (position, (_, target)) in updates.iter().enumerate() {
            if target.as_ref().is_some_and(|(key, _)| updates[..position].iter()
                .any(|(_, old)| old.as_ref().is_some_and(|(old, _)| old == key))) { return Err(summary_corruption()); }
        }
        self.with_write(|write| {
            let mut table = write.open_table(WORKING_VECTOR_LINEAGE).map_err(RedbPersistence::storage_error)?;
            for (key, _) in &updates { table.remove(key.as_slice()).map_err(RedbPersistence::storage_error)?; }
            for (_, target) in &updates {
                if let Some((key, encoded)) = target {
                    table.insert(key.as_slice(), encoded.as_slice()).map_err(RedbPersistence::storage_error)?;
                }
            }
            Ok(())
        })
    }

    pub(crate) fn project_vectors_schema(
        &self, original: &crate::memory_accounting::OwnedMemoryReservation,
        table: &str, previous: Option<&TableMeta>, arriving: Option<&TableMeta>, ddl: &[DdlChange],
    ) -> Result<()> {
        if !self.vectors_prepared()? { return Ok(()); }
        let declaration = OriginalPersistenceFrame::new(original)?;
        for change in ddl { declaration.record(change)?; }
        if let Some(meta) = arriving { declaration.record(meta)?; }
        let renames = ddl.iter().filter_map(|change| match change {
            DdlChange::AlterTable { name, constraints, .. } if name == table => Some(constraints), _ => None,
        }).flat_map(|constraints| crate::database::sync_column_renames_from_constraints(constraints)
            .into_iter().chain(crate::database::sync_vector_rename_from_constraints(constraints))).collect::<Vec<_>>();
        let destination = |index: &VectorIndexRef| -> Option<(&ColumnDef, &str)> {
            let old = previous?.columns.iter().find(|column| column.name == index.column)?;
            let name = renames.iter().find(|(from, _)| *from == index.column).map_or(index.column.as_str(), |(_, to)| to.as_str());
            let next = arriving?.columns.iter().find(|column| column.name == name)?;
            if !matches!(next.column_type, ColumnType::Vector(_)) || old.column_type != next.column_type || old.quantization != next.quantization { return None; }
            Some((next, next.name.as_str()))
        };
        self.project_vector_lineage(&declaration, table, previous, arriving, &renames)?;
        self.rewrite_vector_image(original,
            |frame, body| {
                if body.index.table != table { return Ok(true); }
                let Some((_, name)) = destination(&body.index) else { return Ok(false); };
                if body.index.column != name {
                    frame.reserve(name.len())?;
                    body.index.column = crate::memory_accounting::preparation_copy_string(name)?;
                }
                Ok(true)
            },
            |frame, member| {
                if member.index.table != table { return Ok(true); }
                let Some((column, name)) = destination(&member.index) else { return Ok(false); };
                if member.index.column != name {
                    frame.reserve(name.len())?;
                    member.index.column = crate::memory_accounting::preparation_copy_string(name)?;
                }
                let mut key = [0u8; 24];
                key[..8].copy_from_slice(&member.row_id.0.to_be_bytes()); key[8..16].copy_from_slice(&member.row_created_tx.0.to_be_bytes());
                key[16..].copy_from_slice(&member.row_lsn.0.to_be_bytes());
                let Some(row) = self.next_row(original, table, Bound::Included(key.as_slice()), Bound::Included(key.as_slice()), false)? else { return Ok(false); };
                member.partition_key = derive_partition_original(frame, &member.index, arriving.expect("selected arriving column"), column, row.value())?;
                Ok(true)
            })
    }
}

impl WorkingVectorCatalogOwner {
    pub(crate) fn activate(&self, store: &contextdb_vector::VectorStore, owner: &mut VectorCopyAdmission) -> Result<()> {
        // Activation creates standing original-funded handles. It requires
        // the authentic loan; actual queries separately require their reader.
        self.loan_source()?.refresh_private_vector_catalog()?;
        let factory = WorkingVectorFactoryOwner::prepare(self.clone())?;
        factory.install(store);
        store.activate_durable_raw_columns(owner)
    }

}

use crate::persistence::writer_source::{WriterSnapshotHandle};
