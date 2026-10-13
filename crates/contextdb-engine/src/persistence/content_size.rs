//! Exact committed content lengths, maintained beside the selected physical writes.
use super::*;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
use std::ops::{Deref, DerefMut};

pub(super) const TOTALS: TableDefinition<&str, &[u8]> = TableDefinition::new("content_size_totals");
const ROW_LIVE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("content_size_row_live");
const BODY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("content_size_vector_bodies");

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentSizeTotals {
    pub(crate) live_row_bytes: u64,
    pub(crate) live_vector_bytes: u64,
    pub(crate) retained_row_bytes: u64,
    pub(crate) retained_vector_bytes: u64,
}
impl ContentSizeTotals {
    pub(crate) fn live_content_bytes(self) -> Result<u64> {
        self.live_row_bytes
            .checked_add(self.live_vector_bytes)
            .ok_or_else(|| corrupt("live content size overflow"))
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 32 {
            return Err(corrupt("invalid content-size totals"));
        }
        let field = |n| {
            u64::from_le_bytes(
                bytes[n..n + 8]
                    .try_into()
                    .expect("checked content-size field"),
            )
        };
        Ok(Self {
            live_row_bytes: field(0),
            live_vector_bytes: field(8),
            retained_row_bytes: field(16),
            retained_vector_bytes: field(24),
        })
    }
    fn encode(self) -> [u8; 32] {
        let mut bytes = [0; 32];
        for (offset, value) in [
            self.live_row_bytes,
            self.live_vector_bytes,
            self.retained_row_bytes,
            self.retained_vector_bytes,
        ]
        .into_iter()
        .enumerate()
        {
            bytes[offset * 8..offset * 8 + 8].copy_from_slice(&value.to_le_bytes());
        }
        bytes
    }
}
fn corrupt(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "content_size_totals".into(),
        reason: reason.into(),
    }
}
fn changed(value: u64, old: u64, new: u64) -> Result<u64> {
    value
        .checked_sub(old)
        .and_then(|value| value.checked_add(new))
        .ok_or_else(|| corrupt("content-size counter overflow/underflow"))
}
fn update(
    write: &redb::WriteTransaction,
    table: &str,
    change: impl FnOnce(&mut ContentSizeTotals) -> Result<()>,
) -> Result<()> {
    let mut totals = write
        .open_table(TOTALS)
        .map_err(RedbPersistence::storage_error)?;
    let mut value = totals
        .get(table)
        .map_err(RedbPersistence::storage_error)?
        .map(|value| ContentSizeTotals::decode(value.value()))
        .transpose()?
        .ok_or_else(|| corrupt("a declared content table has no maintained totals"))?;
    change(&mut value)?;
    totals
        .insert(table, value.encode().as_slice())
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}
pub(super) fn initialize(write: &redb::WriteTransaction) -> Result<()> {
    write
        .open_table(TOTALS)
        .map_err(RedbPersistence::storage_error)?;
    write
        .open_table(ROW_LIVE)
        .map_err(RedbPersistence::storage_error)?;
    write
        .open_table(BODY)
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}
pub(super) fn ensure_table(write: &redb::WriteTransaction, table: &str) -> Result<()> {
    let mut totals = write
        .open_table(TOTALS)
        .map_err(RedbPersistence::storage_error)?;
    if let Some(value) = totals.get(table).map_err(RedbPersistence::storage_error)? {
        ContentSizeTotals::decode(value.value())?;
        return Ok(());
    }
    let physical = RedbPersistence::rel_table_name(table);
    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(&physical);
    if write
        .table_exists(definition)
        .map_err(RedbPersistence::storage_error)?
        && write
            .open_table(definition)
            .map_err(RedbPersistence::storage_error)?
            .len()
            .map_err(RedbPersistence::storage_error)?
            != 0
    {
        return Err(corrupt(
            "a nonempty relation has no maintained content totals",
        ));
    }
    let mut vector_prefix = table.as_bytes().to_vec();
    vector_prefix.push(0);
    if write
        .open_table(BODY)
        .map_err(RedbPersistence::storage_error)?
        .prefix_bounded(vector_prefix.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .next()
        .is_some()
    {
        return Err(corrupt(
            "a vector body family has no maintained content totals",
        ));
    }
    totals
        .insert(table, ContentSizeTotals::default().encode().as_slice())
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}

pub(super) fn reset_rows(write: &redb::WriteTransaction, table: &str) -> Result<()> {
    ensure_table(write, table)?;
    update(write, table, |value| {
        value.live_row_bytes = 0;
        value.retained_row_bytes = 0;
        Ok(())
    })?;
    let mut live = write
        .open_table(ROW_LIVE)
        .map_err(RedbPersistence::storage_error)?;
    let mut prefix = table.as_bytes().to_vec();
    prefix.push(0);
    loop {
        let next = live
            .prefix_bounded(prefix.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .next()
            .transpose()
            .map_err(RedbPersistence::storage_error)?
            .map(|(key, _)| key.value().to_vec());
        let Some(key) = next else {
            break;
        };
        live.remove(key.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}
/// Reset one fixed-width component while preserving the independent component.
/// The scan is part of the existing whole-image replacement, never query/open.
fn reset_bodies(write: &redb::WriteTransaction, bytes: bool) -> Result<()> {
    use std::ops::Bound;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let selected = {
            let bodies = write
                .open_table(BODY)
                .map_err(RedbPersistence::storage_error)?;
            let bounds = (
                after.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
                Bound::Unbounded,
            );
            bodies
                .range::<&[u8]>(bounds)
                .map_err(RedbPersistence::storage_error)?
                .next()
                .transpose()
                .map_err(RedbPersistence::storage_error)?
                .map(|(key, _)| key.value().to_vec())
        };
        let Some(key) = selected else {
            return Ok(());
        };
        body_transition(write, &key, |state| {
            if bytes {
                state.bytes = 0;
            } else {
                state.live = 0;
            }
            Ok(())
        })?;
        after = Some(key);
    }
}
pub(super) fn reset_vector_bytes(write: &redb::WriteTransaction) -> Result<()> {
    reset_bodies(write, true)
}
pub(super) fn reset_memberships(write: &redb::WriteTransaction) -> Result<()> {
    reset_bodies(write, false)
}

/// A physical row value is retained until its authenticated head is published.
/// No decoding or row/history enumeration is needed to price the mutation.
pub(super) struct ContentTable<'a> {
    inner: redb::Table<'a, &'static [u8], &'static [u8]>,
    write: &'a redb::WriteTransaction,
    name: String,
    vector: bool,
}
impl<'a> Deref for ContentTable<'a> {
    type Target = redb::Table<'a, &'static [u8], &'static [u8]>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for ContentTable<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
fn storage(error: Error) -> redb::StorageError {
    redb::StorageError::Corrupted(error.to_string())
}
pub(super) fn open_rows<'a>(
    write: &'a redb::WriteTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
) -> std::result::Result<ContentTable<'a>, redb::TableError> {
    let name = definition
        .name()
        .strip_prefix("rel_")
        .expect("relational content table")
        .to_owned();
    let inner = write.open_table(definition)?;
    Ok(ContentTable {
        inner,
        write,
        name,
        vector: false,
    })
}
pub(super) fn open_vectors(
    write: &redb::WriteTransaction,
) -> std::result::Result<ContentTable<'_>, redb::TableError> {
    Ok(ContentTable {
        inner: write.open_table(VECTORS_TABLE)?,
        write,
        name: String::new(),
        vector: true,
    })
}
impl ContentTable<'_> {
    fn transition(&self, key: &[u8], old: u64, new: u64) -> Result<()> {
        if self.vector {
            return body_bytes(self.write, key, old, new);
        }
        update(self.write, &self.name, |value| {
            value.retained_row_bytes = changed(value.retained_row_bytes, old, new)?;
            Ok(())
        })
    }
    pub(super) fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> std::result::Result<Option<redb::AccessGuard<'_, &'static [u8]>>, redb::StorageError> {
        let old = self
            .inner
            .get(key)?
            .map_or(0, |value| value.value().len() as u64);
        self.transition(key, old, value.len() as u64)
            .map_err(storage)?;
        self.inner.insert(key, value)
    }
    pub(super) fn remove(
        &mut self,
        key: &[u8],
    ) -> std::result::Result<Option<redb::AccessGuard<'_, &'static [u8]>>, redb::StorageError> {
        let old = self
            .inner
            .get(key)?
            .map_or(0, |value| value.value().len() as u64);
        self.transition(key, old, 0).map_err(storage)?;
        self.inner.remove(key)
    }
}
/// Body value lengths and live placement counts are independent: a canonical
/// body may own several disjoint placement intervals, but is charged once.
#[derive(Default, Serialize, Deserialize)]
struct BodyState {
    bytes: u64,
    live: u64,
}
fn body_table(key: &[u8]) -> Result<&str> {
    std::str::from_utf8(
        key.split(|byte| *byte == 0)
            .next()
            .ok_or_else(|| corrupt("missing vector table"))?,
    )
    .map_err(|_| corrupt("invalid vector table"))
}
fn body_transition(
    write: &redb::WriteTransaction,
    key: &[u8],
    transition: impl FnOnce(&mut BodyState) -> Result<()>,
) -> Result<()> {
    let mut bodies = write
        .open_table(BODY)
        .map_err(RedbPersistence::storage_error)?;
    let mut state = bodies
        .get(key)
        .map_err(RedbPersistence::storage_error)?
        .map(|value| RedbPersistence::decode_exact::<BodyState>(value.value()))
        .transpose()?
        .unwrap_or_default();
    let old_live = if state.live > 0 { state.bytes } else { 0 };
    let old_retained = if state.live == 0 { state.bytes } else { 0 };
    transition(&mut state)?;
    update(write, body_table(key)?, |value| {
        value.live_vector_bytes = changed(
            value.live_vector_bytes,
            old_live,
            if state.live > 0 { state.bytes } else { 0 },
        )?;
        value.retained_vector_bytes = changed(
            value.retained_vector_bytes,
            old_retained,
            if state.live == 0 { state.bytes } else { 0 },
        )?;
        Ok(())
    })?;
    if state.bytes == 0 && state.live == 0 {
        bodies.remove(key).map_err(RedbPersistence::storage_error)?;
    } else {
        bodies
            .insert(key, RedbPersistence::encode(&state)?.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}
fn body_bytes(write: &redb::WriteTransaction, key: &[u8], old: u64, new: u64) -> Result<()> {
    body_transition(write, key, |state| {
        if state.bytes != old {
            return Err(corrupt(
                "canonical vector length disagrees with content summary",
            ));
        }
        state.bytes = new;
        Ok(())
    })
}
pub(super) fn membership_transition(
    write: &redb::WriteTransaction,
    previous: Option<&VectorPartitionMembershipRecord>,
    next: Option<&VectorPartitionMembershipRecord>,
) -> Result<()> {
    for (record, add) in previous
        .into_iter()
        .map(|record| (record, false))
        .chain(next.into_iter().map(|record| (record, true)))
    {
        if record.deleted_tx.is_some() {
            continue;
        }
        let key = RedbPersistence::vector_identity_key(
            &record.index,
            record.row_id,
            record.vector_created_tx,
            record.vector_lsn,
        );
        body_transition(write, &key, |state| {
            state.live = if add {
                state.live.checked_add(1)
            } else {
                state.live.checked_sub(1)
            }
            .ok_or_else(|| corrupt("vector live placement count overflow/underflow"))?;
            Ok(())
        })?;
    }
    Ok(())
}
pub(super) fn reconcile_row_heads(
    write: &redb::WriteTransaction,
    mutations: &[content_identity::ContentIdentityMutation],
    admission: &mut impl writer_row_heads::RowHeadAdmission,
) -> Result<()> {
    // The held relations are only read here. Dropping the holder closes them
    // and returns what holding them was admitted, on success and failure.
    let mut relations = writer_row_heads::HeldRelations::new(write, admission)?;
    reconcile_row_heads_holding(write, mutations, &mut relations)
}

fn reconcile_row_heads_holding<'name, 'txn, A: writer_row_heads::RowHeadAdmission>(
    write: &'txn redb::WriteTransaction,
    mutations: &'name [content_identity::ContentIdentityMutation],
    relations: &mut writer_row_heads::HeldRelations<'name, 'txn, '_, A>,
) -> Result<()> {
    let mut heads = write
        .open_table(ROW_LIVE)
        .map_err(RedbPersistence::storage_error)?;
    // Apply in source order: duplicate heads in one publication settle to the
    // final physical version without allocating a history-sized directory.
    for mutation in mutations {
        let Some(head) = mutation.row_head else {
            continue;
        };
        let key = &mutation.logical_key;
        let length =
            key.get(..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u64::from_le_bytes)
                .ok_or_else(|| corrupt("malformed row content identity"))? as usize;
        let table = std::str::from_utf8(
            key.get(8..8 + length)
                .ok_or_else(|| corrupt("malformed row content table"))?,
        )
        .map_err(|_| corrupt("invalid row content table"))?;
        let row_id = key
            .get(16 + length..24 + length)
            .ok_or_else(|| corrupt("malformed row content id"))?;
        let mut summary_key = table.as_bytes().to_vec();
        summary_key.push(0);
        summary_key.extend_from_slice(row_id);
        let prior = heads
            .get(summary_key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .map(|value| {
                let bytes = value.value();
                if bytes.len() != 32 {
                    return Err(corrupt("invalid current row size descriptor"));
                }
                Ok::<_, Error>((
                    <[u8; 24]>::try_from(&bytes[..24]).expect("checked head width"),
                    u64::from_le_bytes(bytes[24..].try_into().expect("checked length width")),
                ))
            })
            .transpose()?;
        let old = prior.map_or(0, |(_, bytes)| bytes);
        let rank = |head: [u8; 24]| {
            (
                u64::from_be_bytes(head[16..].try_into().expect("checked LSN")),
                u64::from_be_bytes(head[8..16].try_into().expect("checked transaction")),
            )
        };
        if let (Some((prior_head, _)), Some(next_head)) = (prior, head)
            && rank(prior_head) > rank(next_head)
        {
            // An absent family holds no prior body.
            if let Some(relation) = relations.get(table)?
                && relation
                    .get(prior_head.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .is_some()
            {
                continue;
            }
        }
        let new = if mutation.logical_value.is_some() {
            let head = head.ok_or_else(|| corrupt("live row has no physical head"))?;
            relations
                .get(table)?
                .ok_or_else(|| corrupt("live row has no stored body"))?
                .get(head.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| corrupt("live row has no stored body"))?
                .value()
                .len() as u64
        } else {
            0
        };
        update(write, table, |value| {
            value.live_row_bytes = changed(value.live_row_bytes, old, new)?;
            Ok(())
        })?;
        if let Some(head) = head {
            let mut descriptor = [0; 32];
            descriptor[..24].copy_from_slice(&head);
            descriptor[24..].copy_from_slice(&new.to_le_bytes());
            heads
                .insert(summary_key.as_slice(), descriptor.as_slice())
                .map_err(RedbPersistence::storage_error)?;
        } else {
            heads
                .remove(summary_key.as_slice())
                .map_err(RedbPersistence::storage_error)?;
        }
    }
    Ok(())
}
/// Abnormal transaction-order repair already selects and decodes each record.
/// Reprice its exact rewritten value while that selected record is in hand.
pub(super) fn repair_row_value(
    write: &redb::WriteTransaction,
    table: &str,
    old_key: &[u8],
    new_key: &[u8],
    old_bytes: usize,
    new_bytes: usize,
    current: bool,
) -> Result<()> {
    let row_id = old_key
        .get(..8)
        .ok_or_else(|| corrupt("short repair row identity"))?;
    let row_id = u64::from_be_bytes(row_id.try_into().expect("checked row id"));
    let mut summary_key = table.as_bytes().to_vec();
    summary_key.push(0);
    summary_key.extend_from_slice(&row_id.to_le_bytes());
    let mut heads = write
        .open_table(ROW_LIVE)
        .map_err(RedbPersistence::storage_error)?;
    let descriptor = heads
        .get(summary_key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .map(|value| {
            <[u8; 32]>::try_from(value.value())
                .map_err(|_| corrupt("malformed repair row descriptor"))
        })
        .transpose()?;
    let selected = descriptor.is_some_and(|descriptor| descriptor[..24] == *old_key);
    if current && !selected {
        return Err(corrupt(
            "current repair row has no matching size descriptor",
        ));
    }
    update(write, table, |totals| {
        totals.retained_row_bytes = changed(
            totals.retained_row_bytes,
            old_bytes as u64,
            new_bytes as u64,
        )?;
        if current {
            totals.live_row_bytes =
                changed(totals.live_row_bytes, old_bytes as u64, new_bytes as u64)?;
        }
        Ok(())
    })?;
    if selected {
        let mut descriptor = [0; 32];
        descriptor[..24].copy_from_slice(new_key);
        descriptor[24..]
            .copy_from_slice(&(if current { new_bytes as u64 } else { 0 }).to_le_bytes());
        heads
            .insert(summary_key.as_slice(), descriptor.as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}
pub(super) fn repair_vector_value(
    write: &redb::WriteTransaction,
    old_key: &[u8],
    new_key: &[u8],
    old_bytes: usize,
    new_bytes: usize,
) -> Result<()> {
    body_bytes(write, old_key, old_bytes as u64, 0)?;
    body_bytes(write, new_key, 0, new_bytes as u64)
}

pub(crate) fn read_totals(
    read: &redb::ReadTransaction,
    path: &str,
    memory: Arc<dyn ReadMemoryBudget>,
    before_table: &mut dyn FnMut() -> Result<()>,
) -> Result<ContentSizeTotals> {
    let fail = |reason: &str| Error::StoreCorrupted {
        path: path.into(),
        reason: reason.into(),
    };
    let mut control = ReadCredit::new(memory.clone());
    // ReadOnlyTable owns its native name String. These are the exact name
    // buffers opened below and by the fixed-root decoder, not a control floor.
    control.try_grow_for(
        CONTENT_IDENTITY_ROOTS_TABLE.name().len(),
        "content_size",
        "READ",
        "Raise the reader memory limit to open the committed content-size tables.",
    )?;
    let fingerprint = read
        .table_root_fingerprint(TOTALS)
        .map_err(RedbPersistence::storage_error)?
        .unwrap_or([0; 32]);
    let expected = content_identity::content_size_root_in_read(read)
        .map_err(|error| fail(&error.to_string()))?;
    control.try_shrink(CONTENT_IDENTITY_ROOTS_TABLE.name().len())?;
    if fingerprint != expected {
        return Err(fail(
            "content-size totals are not bound to this committed root",
        ));
    }
    control.try_grow_for(
        TOTALS.name().len() + META_TABLE.name().len(),
        "content_size",
        "READ",
        "Raise the reader memory limit to open the committed content-size tables.",
    )?;
    let totals = match read.open_table(TOTALS) {
        Ok(totals) => totals,
        Err(
            redb::TableError::TableDoesNotExist(_) | redb::TableError::TableTypeMismatch { .. },
        ) => {
            return Err(fail(
                "committed content-size totals table is missing or invalid",
            ));
        }
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let meta = read
        .open_table(META_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let mut answer = ContentSizeTotals::default();
    let mut count = 0u64;
    for entry in meta.iter().map_err(RedbPersistence::storage_error)? {
        let (name, _) = entry.map_err(RedbPersistence::storage_error)?;
        let Some(table_name) = name.value().strip_prefix("table:") else {
            continue;
        };
        before_table()?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_content_size_table_total();
        let value = totals
            .get(table_name)
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| fail("a committed table has no content-size totals"))?;
        let mut value =
            ContentSizeTotals::decode(value.value()).map_err(|error| fail(&error.to_string()))?;
        // The durable row counter stores all physical values; subtract the
        // authenticated current-head sum only when forming the public report.
        value.retained_row_bytes = value
            .retained_row_bytes
            .checked_sub(value.live_row_bytes)
            .ok_or_else(|| fail("live rows exceed physical row content"))?;
        answer.live_row_bytes = changed(answer.live_row_bytes, 0, value.live_row_bytes)?;
        answer.live_vector_bytes = changed(answer.live_vector_bytes, 0, value.live_vector_bytes)?;
        answer.retained_row_bytes =
            changed(answer.retained_row_bytes, 0, value.retained_row_bytes)?;
        answer.retained_vector_bytes =
            changed(answer.retained_vector_bytes, 0, value.retained_vector_bytes)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| fail("content-size table count overflow"))?;
    }
    if totals.len().map_err(RedbPersistence::storage_error)? != count {
        return Err(fail("content-size totals contain an undeclared table"));
    }
    answer.live_content_bytes()?;
    Ok(answer)
}
