//! Canonical proof bytes in immutable payload pages behind a compact directory.
use super::*;
use redb::{ReadableTableMetadata, StorageError, TableDefinition};
use std::borrow::Borrow;
use std::ops::{Range, RangeBounds};

pub(crate) const PAYLOADS: TableDefinition<u64, &[u8]> =
    TableDefinition::new("__content_identity_payloads");
const RETIRED: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__content_identity_retired_slots");
const LOCATOR_BYTES: usize = 48;
const HEADER_BYTES: usize = 9;
const SLOT_BYTES: usize = 41;
const RETIRED_HEADER: usize = 9;
const RETIRED_SLOT: usize = 12;
const LOCATOR_MAGIC: &[u8; 8] = b"proof-v2";

fn corrupt() -> StorageError {
    StorageError::Corrupted("invalid canonical proof payload directory".into())
}

#[derive(Clone, Copy, Default)]
struct Locator {
    block: u64,
    handle: [u8; 24],
    offset: u32,
    len: u32,
}
impl Locator {
    fn decode(bytes: &[u8]) -> redb::Result<Self> {
        if bytes.len() != LOCATOR_BYTES || &bytes[40..] != LOCATOR_MAGIC {
            return Err(corrupt());
        }
        let loc = Self {
            block: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
            handle: bytes[8..32].try_into().unwrap(),
            offset: u32::from_be_bytes(bytes[32..36].try_into().unwrap()),
            len: u32::from_be_bytes(bytes[36..40].try_into().unwrap()),
        };
        if loc.offset < HEADER_BYTES as u32 {
            return Err(corrupt());
        }
        Ok(loc)
    }
    fn encode(self) -> [u8; LOCATOR_BYTES] {
        let mut out = [0; LOCATOR_BYTES];
        out[..8].copy_from_slice(&self.block.to_be_bytes());
        out[8..32].copy_from_slice(&self.handle);
        out[32..36].copy_from_slice(&self.offset.to_be_bytes());
        out[36..40].copy_from_slice(&self.len.to_be_bytes());
        out[40..].copy_from_slice(LOCATOR_MAGIC);
        out
    }
}
fn payload_range(
    bytes: &[u8],
    key: &[u8],
    loc: Locator,
    normal: usize,
) -> redb::Result<Range<usize>> {
    if key.len() != 33 {
        return Err(corrupt());
    }
    if bytes.len() < HEADER_BYTES || bytes[0] != 1 || key.len() != 33 {
        return Err(corrupt());
    }
    let count = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
    let used = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let directory = bytes
        .len()
        .checked_sub(count.checked_mul(SLOT_BYTES).ok_or_else(corrupt)?)
        .ok_or_else(corrupt)?;
    if count == 0
        || used < HEADER_BYTES
        || used > directory
        || (bytes.len() != normal && count != 1)
        || count > normal.saturating_sub(HEADER_BYTES) / SLOT_BYTES
    {
        return Err(corrupt());
    }
    let end = (loc.offset as usize)
        .checked_add(loc.len as usize)
        .ok_or_else(corrupt)?;
    if end > used {
        return Err(corrupt());
    }
    for slot in 0..count {
        let at = bytes.len() - (slot + 1) * SLOT_BYTES;
        if &bytes[at..at + 33] == key
            && bytes[at + 33..at + 37] == loc.offset.to_be_bytes()
            && bytes[at + 37..at + 41] == loc.len.to_be_bytes()
        {
            return Ok(loc.offset as usize..end);
        }
    }
    Err(corrupt())
}
pub(crate) trait PayloadSource: ReadableTable<u64, &'static [u8]> {
    fn physical_get(
        &self,
        handle: [u8; 24],
        key: u64,
        value_len: usize,
    ) -> redb::Result<redb::AccessGuard<'_, &'static [u8]>>;
}
impl PayloadSource for redb::Table<'_, u64, &'static [u8]> {
    fn physical_get(
        &self,
        handle: [u8; 24],
        key: u64,
        value_len: usize,
    ) -> redb::Result<redb::AccessGuard<'_, &'static [u8]>> {
        self.singleton_u64_at(handle, key, value_len)
    }
}
impl PayloadSource for ReadOnlyTable<u64, &'static [u8]> {
    fn physical_get(
        &self,
        handle: [u8; 24],
        key: u64,
        value_len: usize,
    ) -> redb::Result<redb::AccessGuard<'_, &'static [u8]>> {
        self.singleton_u64_at(handle, key, value_len)
    }
}

fn resolve<'a>(
    payloads: &'a impl PayloadSource,
    key: &[u8],
    encoded: &[u8],
    normal: usize,
    dirty_blocks: [Option<u64>; 16],
) -> redb::Result<redb::AccessGuard<'a, &'static [u8]>> {
    let loc = Locator::decode(encoded)?;
    let body = if dirty_blocks.contains(&Some(loc.block)) {
        payloads.get(loc.block)?.ok_or_else(corrupt)?
    } else {
        payloads.physical_get(
            loc.handle,
            loc.block,
            normal.max((loc.len as usize).max(1) + HEADER_BYTES + SLOT_BYTES),
        )?
    };
    let range = payload_range(body.value(), key, loc, normal)?;
    body.slice_bytes(range)
}

pub(crate) trait CanonicalNodeSource {
    fn canonical_get(
        &self,
        key: &[u8],
    ) -> redb::Result<Option<redb::AccessGuard<'_, &'static [u8]>>>;
}
impl<T: ReadableTable<&'static [u8], &'static [u8]>> CanonicalNodeSource for T {
    fn canonical_get(
        &self,
        key: &[u8],
    ) -> redb::Result<Option<redb::AccessGuard<'_, &'static [u8]>>> {
        ReadableTable::get(self, key)
    }
}

pub(crate) struct NodeReadTable {
    directory: ReadOnlyTable<&'static [u8], &'static [u8]>,
    payloads: ReadOnlyTable<u64, &'static [u8]>,
    normal: usize,
}
impl NodeReadTable {
    pub(crate) fn get<'a>(
        &self,
        key: impl Borrow<&'a [u8]>,
    ) -> redb::Result<Option<redb::AccessGuard<'static, &'static [u8]>>> {
        let key = *key.borrow();
        let Some(encoded) = self.directory.get(key)? else {
            return Ok(None);
        };
        if encoded.value().len() != LOCATOR_BYTES {
            return Ok(Some(encoded));
        }
        let loc = Locator::decode(encoded.value())?;
        let body = self.payloads.singleton_u64_at(
            loc.handle,
            loc.block,
            self.normal
                .max((loc.len as usize).max(1) + HEADER_BYTES + SLOT_BYTES),
        )?;
        let range = payload_range(body.value(), key, loc, self.normal)?;
        body.slice_bytes(range).map(Some)
    }
    #[cfg(test)]
    pub(crate) fn iter(&self) -> redb::Result<NodeRange<'_, ReadOnlyTable<u64, &'static [u8]>>> {
        Ok(NodeRange {
            raw: self.directory.iter()?,
            payloads: &self.payloads,
            normal: self.normal,
            dirty_blocks: [None; 16],
        })
    }
}
impl CanonicalNodeSource for NodeReadTable {
    fn canonical_get(
        &self,
        key: &[u8],
    ) -> redb::Result<Option<redb::AccessGuard<'_, &'static [u8]>>> {
        self.get(key)
    }
}

pub(crate) struct NodeRange<'a, P> {
    raw: redb::Range<'a, &'static [u8], &'static [u8]>,
    payloads: &'a P,
    normal: usize,
    dirty_blocks: [Option<u64>; 16],
}
impl<'a, P: PayloadSource> Iterator for NodeRange<'a, P> {
    type Item = redb::Result<(
        redb::AccessGuard<'a, &'static [u8]>,
        redb::AccessGuard<'a, &'static [u8]>,
    )>;
    fn next(&mut self) -> Option<Self::Item> {
        self.raw.next().map(|entry| {
            let (key, locator) = entry?;
            let value = if locator.value().len() != LOCATOR_BYTES {
                locator
            } else {
                resolve(
                    self.payloads,
                    key.value(),
                    locator.value(),
                    self.normal,
                    self.dirty_blocks,
                )?
            };
            Ok((key, value))
        })
    }
}
impl<P: PayloadSource> DoubleEndedIterator for NodeRange<'_, P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.raw.next_back().map(|entry| {
            let (key, locator) = entry?;
            let value = if locator.value().len() != LOCATOR_BYTES {
                locator
            } else {
                resolve(
                    self.payloads,
                    key.value(),
                    locator.value(),
                    self.normal,
                    self.dirty_blocks,
                )?
            };
            Ok((key, value))
        })
    }
}

#[derive(Clone, Copy)]
struct Bin {
    id: u64,
    used: usize,
    count: usize,
    capacity: usize,
}

pub(crate) enum PayloadReserve<'a> {
    New(redb::AccessGuardMutInPlace<'a, &'static [u8]>, Range<usize>),
    Existing(redb::AccessGuardMut<'a, &'static [u8]>, Range<usize>),
}
impl AsMut<[u8]> for PayloadReserve<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            Self::New(guard, range) => &mut guard.as_mut()[range.clone()],
            Self::Existing(guard, range) => &mut guard.as_mut()[range.clone()],
        }
    }
}

struct Publication<'g, 'txn> {
    directory: &'g mut redb::Table<'txn, &'static [u8], &'static [u8]>,
    pending: &'g mut [Locator; 64],
    pending_count: &'g mut usize,
    key: [u8; 33],
    locator: Locator,
}
pub(crate) struct CanonicalReserve<'g, 'txn> {
    body: PayloadReserve<'g>,
    publication: Option<Publication<'g, 'txn>>,
    owner: &'txn redb::WriteTransaction,
    done: bool,
}
impl AsMut<[u8]> for CanonicalReserve<'_, '_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.body.as_mut()
    }
}
impl CanonicalReserve<'_, '_> {
    fn publish(&mut self) -> redb::Result<()> {
        self.done = true;
        let Some(publication) = self.publication.as_mut() else {
            return Ok(());
        };
        publication.locator.handle = match &self.body {
            PayloadReserve::New(guard, _) => {
                guard.singleton_u64_handle(publication.locator.block)?
            }
            PayloadReserve::Existing(guard, _) => {
                guard.singleton_u64_handle(publication.locator.block)?
            }
        };
        let old = publication
            .directory
            .insert(
                publication.key.as_slice(),
                publication.locator.encode().as_slice(),
            )?
            .filter(|value| value.value().len() == LOCATOR_BYTES)
            .map(|value| Locator::decode(value.value()))
            .transpose()?;
        if let Some(old) = old {
            if *publication.pending_count >= publication.pending.len() {
                return Err(corrupt());
            }
            publication.pending[*publication.pending_count] = old;
            *publication.pending_count += 1;
        }
        Ok(())
    }
    pub(super) fn finish(mut self) -> redb::Result<()> {
        self.publish()
    }
}
impl Drop for CanonicalReserve<'_, '_> {
    fn drop(&mut self) {
        if !self.done
            && let Err(error) = self.publish()
        {
            self.owner.record_internal_operation_error(error);
        }
    }
}

#[derive(Clone, Copy)]
struct CachedLocator {
    key: [u8; 33],
    locator: [u8; LOCATOR_BYTES],
}

pub(crate) struct NodeWriteTable<'a> {
    directory: std::cell::OnceCell<redb::Table<'a, &'static [u8], &'static [u8]>>,
    payloads: std::cell::OnceCell<redb::Table<'a, u64, &'static [u8]>>,
    retired: Option<redb::Table<'a, &'static [u8], &'static [u8]>>,
    metadata: Option<redb::Table<'a, &'static str, &'static [u8]>>,
    packing_loaded: bool,
    transaction: u64,
    retirement_page: u64,
    retirement_count: usize,
    next_block: u64,
    sealed_before: u64,
    packing_changed: bool,
    dirty_bins: u16,
    normal: std::cell::Cell<usize>,
    transaction_owner: &'a redb::WriteTransaction,
    // These fixed stack windows neither grow with history nor allocate.
    bins: [Option<Bin>; 16],
    pending_retirements: [Locator; 64],
    pending_count: usize,
    // Scalar locations are valid only within this owning transaction. This
    // fixed stack window retains neither pages nor canonical payload bodies.
    locator_cache: std::cell::Cell<[Option<CachedLocator>; 16]>,
    locator_next: std::cell::Cell<usize>,
}
fn add_slot(bytes: &mut [u8], key: &[u8], bin: Bin, len: usize) {
    let at = bin.capacity - (bin.count + 1) * SLOT_BYTES;
    bytes[at..at + 33].copy_from_slice(key);
    bytes[at + 33..at + 37].copy_from_slice(&(bin.used as u32).to_be_bytes());
    bytes[at + 37..at + 41].copy_from_slice(&(len as u32).to_be_bytes());
    bytes[0] = 1;
    bytes[1..5].copy_from_slice(&((bin.count + 1) as u32).to_be_bytes());
    bytes[5..9].copy_from_slice(&((bin.used + len.max(1)) as u32).to_be_bytes());
}

impl<'txn> NodeWriteTable<'txn> {
    fn directory(&self) -> redb::Result<&redb::Table<'txn, &'static [u8], &'static [u8]>> {
        if self.directory.get().is_none() {
            let table = self
                .transaction_owner
                .open_table(CONTENT_IDENTITY_NODES_TABLE)
                .map_err(table_error)?;
            self.directory.set(table).map_err(|_| corrupt())?;
        }
        Ok(self.directory.get().unwrap())
    }
    fn payloads(&self) -> redb::Result<&redb::Table<'txn, u64, &'static [u8]>> {
        if self.payloads.get().is_none() {
            let table = self
                .transaction_owner
                .open_table(PAYLOADS)
                .map_err(table_error)?;
            self.normal.set(table.inline_value_capacity(8));
            self.payloads.set(table).map_err(|_| corrupt())?;
        }
        Ok(self.payloads.get().unwrap())
    }
    fn ensure_packing(&mut self) -> redb::Result<()> {
        if self.packing_loaded {
            return Ok(());
        }
        self.directory()?;
        let payloads = self.payloads()?;
        let metadata = self
            .transaction_owner
            .open_table(CONTENT_IDENTITY_METADATA_TABLE)
            .map_err(table_error)?;
        let normal = payloads.inline_value_capacity(8);
        let transaction = self.transaction;
        let mut bins = [None; 16];
        let (next_block, sealed_before, retirement_page, retirement_count) = match metadata
            .get("payload_next_block")?
        {
            None if payloads.len()? == 0 => (0, 0, 0, 0),
            Some(value) => {
                let bytes = value.value();
                if bytes.len() < 37
                    || bytes[32] as usize > bins.len()
                    || bytes.len() != 37 + bytes[32] as usize * 20
                {
                    return Err(corrupt());
                }
                let next = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                let prior = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
                if prior == transaction {
                    let sealed = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
                    if sealed > next {
                        return Err(corrupt());
                    }
                    for (i, bin) in bins.iter_mut().take(bytes[32] as usize).enumerate() {
                        let at = 37 + i * 20;
                        let id = u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap());
                        let used =
                            u32::from_be_bytes(bytes[at + 8..at + 12].try_into().unwrap()) as usize;
                        let count = u32::from_be_bytes(bytes[at + 12..at + 16].try_into().unwrap())
                            as usize;
                        let capacity =
                            u32::from_be_bytes(bytes[at + 16..at + 20].try_into().unwrap())
                                as usize;
                        if id < sealed
                            || id >= next
                            || used < HEADER_BYTES
                            || count == 0
                            || used
                                .checked_add(count.checked_mul(SLOT_BYTES).ok_or_else(corrupt)?)
                                .ok_or_else(corrupt)?
                                > capacity
                            || (capacity != normal && count != 1)
                            || capacity > payloads.maximum_value_length(8)
                        {
                            return Err(corrupt());
                        }
                        *bin = Some(Bin {
                            id,
                            used,
                            count,
                            capacity,
                        });
                    }
                    (
                        next,
                        sealed,
                        u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
                        u32::from_be_bytes(bytes[33..37].try_into().unwrap()) as usize,
                    )
                } else {
                    (next, next, 0, 0)
                }
            }
            None => return Err(corrupt()),
        };
        self.metadata = Some(metadata);
        self.bins = bins;
        self.next_block = next_block;
        self.sealed_before = sealed_before;
        self.retirement_page = retirement_page;
        self.retirement_count = retirement_count;
        self.packing_loaded = true;
        Ok(())
    }

    fn dirty_blocks(&self) -> [Option<u64>; 16] {
        std::array::from_fn(|index| {
            if self.dirty_bins & (1 << index) != 0 {
                self.bins[index].map(|bin| bin.id)
            } else {
                None
            }
        })
    }
    fn seal_bin(&mut self, bin: Bin) -> redb::Result<()> {
        // The last slot was installed with its final checksum by its producer.
        if bin.count == 1 {
            return Ok(());
        }
        let body = self
            .payloads
            .get_mut()
            .unwrap()
            .get(bin.id)?
            .ok_or_else(corrupt)?;
        let bytes = body.value();
        if bytes.len() != bin.capacity
            || bytes[0] != 1
            || bytes[1..5] != (bin.count as u32).to_be_bytes()
            || bytes[5..9] != (bin.used as u32).to_be_bytes()
        {
            return Err(corrupt());
        }
        let handle = body.singleton_u64_handle(bin.id)?;
        for slot in 0..bin.count - 1 {
            let at = bytes.len() - (slot + 1) * SLOT_BYTES;
            let key = &bytes[at..at + 33];
            let offset = u32::from_be_bytes(bytes[at + 33..at + 37].try_into().unwrap());
            let len = u32::from_be_bytes(bytes[at + 37..at + 41].try_into().unwrap());
            let Some(mut value) = self.directory.get_mut().unwrap().get_mut(key)? else {
                continue;
            };
            if value.value().len() != LOCATOR_BYTES {
                continue;
            }
            let mut loc = Locator::decode(value.value())?;
            if loc.block != bin.id || loc.offset != offset {
                continue;
            }
            if loc.len != len || loc.handle[..8] != handle[..8] {
                return Err(corrupt());
            }
            loc.handle = handle;
            value.as_mut().copy_from_slice(&loc.encode());
        }
        Ok(())
    }
    pub(super) fn seal_pending(&mut self) -> redb::Result<()> {
        self.locator_cache.set([None; 16]);
        for index in 0..self.bins.len() {
            if self.dirty_bins & (1 << index) != 0 {
                self.seal_bin(self.bins[index].ok_or_else(corrupt)?)?;
                self.dirty_bins &= !(1 << index);
            }
        }
        Ok(())
    }
    fn flush_retirements(&mut self) -> redb::Result<()> {
        if self.pending_count == 0 {
            return Ok(());
        }
        if self.retired.is_none() {
            self.retired = Some(
                self.transaction_owner
                    .open_table(RETIRED)
                    .map_err(table_error)?,
            );
        }
        let retired = self.retired.as_mut().unwrap();
        let mut consumed = 0;
        // Each flush seals only its actual slots. Padding a small durable queue
        // record to a native page retains avoidable cache and tree working set.
        let mut encoded = [0; RETIRED_HEADER + 64 * RETIRED_SLOT];
        while consumed < self.pending_count {
            let mut key = [0; 16];
            key[..8].copy_from_slice(&self.transaction.to_be_bytes());
            key[8..].copy_from_slice(&self.retirement_page.to_be_bytes());
            let capacity = retired.inline_value_capacity(key.len());
            let maximum = capacity.saturating_sub(RETIRED_HEADER) / RETIRED_SLOT;
            if maximum == 0 {
                return Err(corrupt());
            }
            let count = (self.pending_count - consumed).min(maximum);
            encoded[0] = 1;
            encoded[1..5].copy_from_slice(&(count as u32).to_be_bytes());
            for (i, loc) in self.pending_retirements[consumed..consumed + count]
                .iter()
                .enumerate()
            {
                let at = RETIRED_HEADER + i * RETIRED_SLOT;
                encoded[at..at + 8].copy_from_slice(&loc.block.to_be_bytes());
                encoded[at + 8..at + 12].copy_from_slice(&loc.offset.to_be_bytes());
            }
            let len = RETIRED_HEADER + count * RETIRED_SLOT;
            if retired.insert(key.as_slice(), &encoded[..len])?.is_some() {
                return Err(corrupt());
            }
            self.retirement_page = self.retirement_page.checked_add(1).ok_or_else(corrupt)?;
            consumed += count;
        }
        self.retirement_count = 0;
        self.pending_count = 0;
        Ok(())
    }
    fn retire(&mut self, locator: Locator) -> redb::Result<()> {
        if self.pending_count == self.pending_retirements.len() {
            self.flush_retirements()?;
        }
        self.packing_changed = true;
        self.pending_retirements[self.pending_count] = locator;
        self.pending_count += 1;
        Ok(())
    }
    #[cfg(all(test, feature = "test-seams"))]
    pub(crate) fn len(&self) -> redb::Result<u64> {
        self.directory()?.len()
    }

    pub(crate) fn get<'k>(
        &self,
        key: impl Borrow<&'k [u8]>,
    ) -> redb::Result<Option<redb::AccessGuard<'_, &'static [u8]>>> {
        let key = *key.borrow();
        if let Some(cached) = self
            .locator_cache
            .get()
            .iter()
            .flatten()
            .find(|entry| entry.key == key)
        {
            return resolve(
                self.payloads()?,
                key,
                &cached.locator,
                self.normal.get(),
                self.dirty_blocks(),
            )
            .map(Some);
        }
        let Some(encoded) = self.directory()?.get(key)? else {
            return Ok(None);
        };
        if encoded.value().len() != LOCATOR_BYTES {
            return Ok(Some(encoded));
        }
        if let Ok(key) = <[u8; 33]>::try_from(key) {
            let mut cache = self.locator_cache.get();
            let slot = self.locator_next.get();
            cache[slot] = Some(CachedLocator {
                key,
                locator: encoded.value().try_into().unwrap(),
            });
            self.locator_cache.set(cache);
            self.locator_next.set((slot + 1) % cache.len());
        }
        resolve(
            self.payloads()?,
            key,
            encoded.value(),
            self.normal.get(),
            self.dirty_blocks(),
        )
        .map(Some)
    }
    pub(crate) fn insert_zeroed_reserve<'k>(
        &mut self,
        key: impl Borrow<&'k [u8]>,
        len: usize,
    ) -> redb::Result<CanonicalReserve<'_, 'txn>> {
        self.ensure_packing()?;
        self.locator_cache.set([None; 16]);
        let key = *key.borrow();
        if key.len() != 33 || len > u32::MAX as usize {
            return Err(corrupt());
        }
        if len > self.payloads.get_mut().unwrap().maximum_value_length(33) {
            return Err(StorageError::ValueTooLarge(len));
        }
        if self.pending_count == self.pending_retirements.len() {
            self.flush_retirements()?;
        }
        // Preserve the old native maximum without adding private framing.
        // Only this near-limit case remains directly inline in the directory.
        if len
            > self.payloads.get_mut().unwrap().maximum_value_length(33) - HEADER_BYTES - SLOT_BYTES
        {
            let old = self
                .directory
                .get()
                .unwrap()
                .get(key)?
                .filter(|v| v.value().len() == LOCATOR_BYTES)
                .map(|v| Locator::decode(v.value()))
                .transpose()?;
            if let Some(old) = old {
                self.retire(old)?;
            }
            return Ok(CanonicalReserve {
                body: PayloadReserve::New(
                    self.directory
                        .get_mut()
                        .unwrap()
                        .insert_zeroed_reserve(key, len)?,
                    0..len,
                ),
                publication: None,
                owner: self.transaction_owner,
                done: false,
            });
        }
        let occupied = len.max(1);
        self.packing_changed = true;
        let minimum = occupied
            .checked_add(HEADER_BYTES + SLOT_BYTES)
            .ok_or_else(corrupt)?;
        let reusable = self
            .bins
            .iter()
            .enumerate()
            .filter_map(|(i, bin)| {
                let bin = (*bin)?;
                let remaining = bin.capacity - bin.used - bin.count * SLOT_BYTES;
                (remaining >= occupied + SLOT_BYTES).then_some((i, bin, remaining))
            })
            .min_by_key(|(_, _, remaining)| *remaining);
        let (index, bin, fresh) = match reusable {
            Some((i, bin, _)) => (i, bin, false),
            None => {
                let index = self
                    .bins
                    .iter()
                    .position(Option::is_none)
                    .unwrap_or_else(|| {
                        self.bins
                            .iter()
                            .enumerate()
                            .min_by_key(|(_, bin)| {
                                let bin = bin.unwrap();
                                bin.capacity - bin.used - bin.count * SLOT_BYTES
                            })
                            .unwrap()
                            .0
                    });
                if self.dirty_bins & (1 << index) != 0 {
                    self.seal_bin(self.bins[index].ok_or_else(corrupt)?)?;
                    self.dirty_bins &= !(1 << index);
                }
                let id = self.next_block;
                self.next_block = id.checked_add(1).ok_or_else(corrupt)?;
                (
                    index,
                    Bin {
                        id,
                        used: HEADER_BYTES,
                        count: 0,
                        capacity: self.normal.get().max(minimum),
                    },
                    true,
                )
            }
        };
        self.bins[index] = Some(Bin {
            used: bin.used + occupied,
            count: bin.count + 1,
            ..bin
        });
        self.dirty_bins |= 1 << index;
        let range = bin.used..bin.used + len;
        let mut value = if fresh {
            PayloadReserve::New(
                self.payloads
                    .get_mut()
                    .unwrap()
                    .insert_zeroed_reserve(bin.id, bin.capacity)?,
                range,
            )
        } else {
            let mut guard = self
                .payloads
                .get_mut()
                .unwrap()
                .get_mut(bin.id)?
                .ok_or_else(corrupt)?;
            let bytes = guard.as_mut();
            if bytes.len() != bin.capacity
                || bytes[0] != 1
                || bytes[1..5] != (bin.count as u32).to_be_bytes()
                || bytes[5..9] != (bin.used as u32).to_be_bytes()
            {
                return Err(corrupt());
            }
            PayloadReserve::Existing(guard, range)
        };
        match &mut value {
            PayloadReserve::New(guard, _) => add_slot(guard.as_mut(), key, bin, len),
            PayloadReserve::Existing(guard, _) => add_slot(guard.as_mut(), key, bin, len),
        }
        Ok(CanonicalReserve {
            body: value,
            publication: Some(Publication {
                directory: self.directory.get_mut().unwrap(),
                pending: &mut self.pending_retirements,
                pending_count: &mut self.pending_count,
                key: key.try_into().unwrap(),
                locator: Locator {
                    block: bin.id,
                    handle: [0; 24],
                    offset: bin.used as u32,
                    len: len as u32,
                },
            }),
            owner: self.transaction_owner,
            done: false,
        })
    }
    #[cfg(test)]
    pub(crate) fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<&'k [u8]>,
        value: impl Borrow<&'v [u8]>,
    ) -> redb::Result<Option<()>> {
        let key = *key.borrow();
        let value = *value.borrow();
        let existed = self.directory()?.get(key)?.is_some();
        let mut destination = self.insert_zeroed_reserve(key, value.len())?;
        destination.as_mut().copy_from_slice(value);
        destination.finish()?;
        Ok(existed.then_some(()))
    }
    pub(crate) fn remove<'k>(&mut self, key: impl Borrow<&'k [u8]>) -> redb::Result<Option<()>> {
        self.ensure_packing()?;
        self.locator_cache.set([None; 16]);
        let old = self.directory.get_mut().unwrap().remove(*key.borrow())?;
        let existed = old.is_some();
        let loc = old
            .filter(|value| value.value().len() == LOCATOR_BYTES)
            .map(|value| Locator::decode(value.value()))
            .transpose()?;
        if let Some(loc) = loc {
            self.retire(loc)?;
        }
        Ok(existed.then_some(()))
    }
    pub(crate) fn range<'k, KR>(
        &self,
        range: impl RangeBounds<KR> + 'k,
    ) -> redb::Result<NodeRange<'_, redb::Table<'_, u64, &'static [u8]>>>
    where
        KR: Borrow<&'k [u8]> + 'k,
    {
        Ok(NodeRange {
            raw: self.directory()?.range(range)?,
            payloads: self.payloads()?,
            normal: self.normal.get(),
            dirty_blocks: self.dirty_blocks(),
        })
    }
    #[cfg(test)]
    pub(crate) fn iter(&self) -> redb::Result<NodeRange<'_, redb::Table<'_, u64, &'static [u8]>>> {
        self.range::<&[u8]>(..)
    }
}
impl Drop for NodeWriteTable<'_> {
    fn drop(&mut self) {
        let result = self
            .seal_pending()
            .and_then(|()| self.flush_retirements())
            .and_then(|()| {
                if self.packing_changed {
                    // The bounded packing window survives family/table adapters in
                    // this write transaction. A later transaction seals every bin.
                    let mut encoded = [0; 37 + 16 * 20];
                    encoded[..8].copy_from_slice(&self.next_block.to_be_bytes());
                    encoded[8..16].copy_from_slice(&self.transaction.to_be_bytes());
                    encoded[16..24].copy_from_slice(&self.sealed_before.to_be_bytes());
                    encoded[24..32].copy_from_slice(&self.retirement_page.to_be_bytes());
                    let mut count = 0;
                    for bin in self.bins.iter().flatten() {
                        let at = 37 + count * 20;
                        encoded[at..at + 8].copy_from_slice(&bin.id.to_be_bytes());
                        encoded[at + 8..at + 12].copy_from_slice(&(bin.used as u32).to_be_bytes());
                        encoded[at + 12..at + 16]
                            .copy_from_slice(&(bin.count as u32).to_be_bytes());
                        encoded[at + 16..at + 20]
                            .copy_from_slice(&(bin.capacity as u32).to_be_bytes());
                        count += 1;
                    }
                    encoded[32] = count as u8;
                    encoded[33..37].copy_from_slice(&(self.retirement_count as u32).to_be_bytes());
                    self.metadata
                        .as_mut()
                        .unwrap()
                        .insert("payload_next_block", &encoded[..37 + count * 20])?;
                }
                Ok(())
            });
        if let Err(error) = result {
            self.transaction_owner
                .record_internal_operation_error(error);
        }
    }
}
impl CanonicalNodeSource for NodeWriteTable<'_> {
    fn canonical_get(
        &self,
        key: &[u8],
    ) -> redb::Result<Option<redb::AccessGuard<'_, &'static [u8]>>> {
        self.get(key)
    }
}

pub(crate) trait OpenNodeTables {
    type Nodes<'a>
    where
        Self: 'a;
    fn open_nodes(&self) -> std::result::Result<Self::Nodes<'_>, redb::TableError>;
}
impl OpenNodeTables for redb::WriteTransaction {
    type Nodes<'a> = NodeWriteTable<'a>;
    fn open_nodes(&self) -> std::result::Result<NodeWriteTable<'_>, redb::TableError> {
        Ok(NodeWriteTable {
            directory: std::cell::OnceCell::new(),
            payloads: std::cell::OnceCell::new(),
            metadata: None,
            packing_loaded: false,
            retired: None,
            transaction: self.native_transaction_id(),
            retirement_page: 0,
            retirement_count: 0,
            next_block: 0,
            sealed_before: 0,
            packing_changed: false,
            dirty_bins: 0,
            normal: std::cell::Cell::new(0),
            transaction_owner: self,
            bins: [None; 16],
            pending_retirements: [Locator::default(); 64],
            pending_count: 0,
            locator_cache: std::cell::Cell::new([None; 16]),
            locator_next: std::cell::Cell::new(0),
        })
    }
}
impl OpenNodeTables for redb::ReadTransaction {
    type Nodes<'a> = NodeReadTable;
    fn open_nodes(&self) -> std::result::Result<NodeReadTable, redb::TableError> {
        let payloads = self.open_singleton_u64_table(PAYLOADS)?;
        let normal = payloads.inline_value_capacity(8);
        Ok(NodeReadTable {
            directory: self.open_table(CONTENT_IDENTITY_NODES_TABLE)?,
            payloads,
            normal,
        })
    }
}
impl<T: OpenNodeTables> OpenNodeTables for &T {
    type Nodes<'a>
        = T::Nodes<'a>
    where
        Self: 'a;
    fn open_nodes(&self) -> std::result::Result<Self::Nodes<'_>, redb::TableError> {
        (**self).open_nodes()
    }
}
pub(crate) fn open<T: OpenNodeTables>(
    transaction: &T,
) -> std::result::Result<T::Nodes<'_>, redb::TableError> {
    transaction.open_nodes()
}

pub(crate) fn relocation_event(
    write: &redb::WriteTransaction,
    name: &str,
    event: redb::TableRootEvent,
) -> redb::Result<()> {
    if name == PAYLOADS.name() && matches!(event, redb::TableRootEvent::AfterRelocate) {
        rebind_relocated(write)?;
    }
    Ok(())
}

/// Rebind only live slots in leaves from this actual native relocation batch.
pub(crate) fn rebind_relocated(write: &redb::WriteTransaction) -> redb::Result<()> {
    let normal = {
        let payloads = write.open_table(PAYLOADS).map_err(table_error)?;
        payloads.inline_value_capacity(8)
    };
    let mut directory = write
        .open_table(CONTENT_IDENTITY_NODES_TABLE)
        .map_err(table_error)?;
    write.visit_relocated_u64_singletons(|old, handle, id, body| {
        let bytes = body.value();
        if bytes.len() < HEADER_BYTES || bytes[0] != 1 {
            return Err(corrupt());
        }
        let count = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
        let used = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let descriptors = count.checked_mul(SLOT_BYTES).ok_or_else(corrupt)?;
        if count == 0
            || count > normal.saturating_sub(HEADER_BYTES) / SLOT_BYTES
            || (bytes.len() != normal && count != 1)
            || used < HEADER_BYTES
            || used > bytes.len().checked_sub(descriptors).ok_or_else(corrupt)?
        {
            return Err(corrupt());
        }
        for slot in 0..count {
            let at = bytes.len() - (slot + 1) * SLOT_BYTES;
            let key = &bytes[at..at + 33];
            let offset = u32::from_be_bytes(bytes[at + 33..at + 37].try_into().unwrap());
            let len = u32::from_be_bytes(bytes[at + 37..at + 41].try_into().unwrap());
            let Some(mut value) = directory.get_mut(key)? else {
                continue;
            };
            if value.value().len() != LOCATOR_BYTES {
                continue;
            }
            let mut loc = Locator::decode(value.value())?;
            if loc.block != id || loc.offset != offset {
                continue;
            }
            if loc.len != len
                || loc.handle[8..] != handle[8..]
                || (loc.handle[..8] != old && loc.handle[..8] != handle[..8])
            {
                return Err(corrupt());
            }
            loc.handle = handle;
            value.as_mut().copy_from_slice(&loc.encode());
        }
        Ok(())
    })
}

fn table_error(error: redb::TableError) -> StorageError {
    match error {
        redb::TableError::Storage(error) => error,
        other => StorageError::Corrupted(other.to_string()),
    }
}

/// Consume only the caller's finite slot budget. The queue cursor and bitmaps
/// share the same transaction as deletion, so replay cannot double-retire a slot.
pub(crate) fn reclaim(
    write: &redb::WriteTransaction,
    maximum_slots: usize,
) -> redb::Result<(usize, usize)> {
    reclaim_before(write, maximum_slots, u64::MAX)
}

/// A maintenance cycle consumes only retirements older than its captured
/// native boundary. Concurrent foreground writes remain for a later cycle.
pub(crate) fn reclaim_before(
    write: &redb::WriteTransaction,
    maximum_slots: usize,
    before: u64,
) -> redb::Result<(usize, usize)> {
    const BITMAPS: TableDefinition<u64, &[u8]> =
        TableDefinition::new("__content_identity_retired_bitmaps");
    if maximum_slots == 0 {
        return Ok((0, 0));
    }
    let mut queue = write.open_table(RETIRED).map_err(table_error)?;
    {
        let Some((key, _)) = queue.first()? else {
            return Ok((0, 0));
        };
        if key.value().len() != 16 {
            return Err(corrupt());
        }
        if u64::from_be_bytes(key.value()[..8].try_into().unwrap()) >= before {
            return Ok((0, 0));
        }
    }
    let mut payloads = write.open_table(PAYLOADS).map_err(table_error)?;
    let mut bitmaps = write.open_table(BITMAPS).map_err(table_error)?;
    let normal = payloads.inline_value_capacity(8);
    let mut remaining = maximum_slots;
    let mut reclaimed = 0;
    while remaining != 0 {
        let Some((key_guard, record)) = queue.first()? else {
            break;
        };
        if key_guard.value().len() != 16 {
            return Err(corrupt());
        }
        if u64::from_be_bytes(key_guard.value()[..8].try_into().unwrap()) >= before {
            break;
        }
        if record.value().len() < RETIRED_HEADER || record.value()[0] != 1 {
            return Err(corrupt());
        }
        let key: [u8; 16] = key_guard.value().try_into().unwrap();
        let count = u32::from_be_bytes(record.value()[1..5].try_into().unwrap()) as usize;
        let cursor = u32::from_be_bytes(record.value()[5..9].try_into().unwrap()) as usize;
        if count == 0
            || cursor > count
            || count > (record.value().len() - RETIRED_HEADER) / RETIRED_SLOT
        {
            return Err(corrupt());
        }
        let end = count.min(cursor.saturating_add(remaining));
        for position in cursor..end {
            let at = RETIRED_HEADER + position * RETIRED_SLOT;
            let id = u64::from_be_bytes(record.value()[at..at + 8].try_into().unwrap());
            let offset = u32::from_be_bytes(record.value()[at + 8..at + 12].try_into().unwrap());
            let (slots, slot) = {
                let body = payloads.get(id)?.ok_or_else(corrupt)?;
                let bytes = body.value();
                if bytes.len() < HEADER_BYTES || bytes[0] != 1 {
                    return Err(corrupt());
                }
                let slots = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
                if slots == 0
                    || slots > normal.saturating_sub(HEADER_BYTES) / SLOT_BYTES
                    || (bytes.len() != normal && slots != 1)
                    || slots > (bytes.len() - HEADER_BYTES) / SLOT_BYTES
                {
                    return Err(corrupt());
                }
                let slot = (0..slots)
                    .find(|slot| {
                        let start = bytes.len() - (slot + 1) * SLOT_BYTES + 33;
                        bytes[start..start + 4] == offset.to_be_bytes()
                    })
                    .ok_or_else(corrupt)?;
                (slots, slot)
            };
            let len = slots.div_ceil(8);
            let exists = {
                let old = bitmaps.get(id)?;
                if let Some(old) = old.as_ref()
                    && old.value().len() != len
                {
                    return Err(corrupt());
                }
                old.is_some()
            };
            if !exists {
                drop(bitmaps.insert_zeroed_reserve(id, len)?);
            }
            let complete = {
                let mut bits = bitmaps.get_mut(id)?.ok_or_else(corrupt)?;
                let bits = bits.as_mut();
                let mask = 1 << (slot % 8);
                if bits[slot / 8] & mask != 0 {
                    return Err(corrupt());
                }
                bits[slot / 8] |= mask;
                bits.iter().map(|x| x.count_ones() as usize).sum::<usize>() == slots
            };
            if complete {
                drop(payloads.remove(id)?);
                drop(bitmaps.remove(id)?);
                reclaimed += 1;
            }
            remaining -= 1;
        }
        drop(record);
        drop(key_guard);
        if end == count {
            drop(queue.remove(key.as_slice())?);
        } else {
            let mut record = queue.get_mut(key.as_slice())?.ok_or_else(corrupt)?;
            record.as_mut()[5..9].copy_from_slice(&(end as u32).to_be_bytes());
        }
    }
    Ok((maximum_slots - remaining, reclaimed))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn key(n: u64) -> [u8; 33] {
        let mut key = [0; 33];
        key[25..].copy_from_slice(&n.to_be_bytes());
        key
    }
    fn put(write: &redb::WriteTransaction, entries: &[(u64, &[u8])]) {
        let mut table = open(write).unwrap();
        for (n, bytes) in entries {
            table.insert(key(*n).as_slice(), *bytes).unwrap();
        }
    }
    #[test]
    fn transaction_packing_survives_adapter_reopens_and_seals_at_commit() {
        let dir = tempfile::tempdir().unwrap();
        let db = redb::Database::create(dir.path().join("packing.redb")).unwrap();
        let write = db.begin_write().unwrap();
        put(&write, &[(1, b"one")]);
        put(&write, &[(2, b"two")]);
        assert_eq!(write.open_table(PAYLOADS).unwrap().len().unwrap(), 1);
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        let sealed = read
            .open_table(PAYLOADS)
            .unwrap()
            .get(0)
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        let write = db.begin_write().unwrap();
        put(&write, &[(3, b"three")]);
        assert_eq!(write.open_table(PAYLOADS).unwrap().len().unwrap(), 2);
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        assert_eq!(
            read.open_table(PAYLOADS)
                .unwrap()
                .get(0)
                .unwrap()
                .unwrap()
                .value(),
            sealed
        );
        let nodes = open(&read).unwrap();
        for (id, value) in [
            (1, b"one".as_slice()),
            (2, b"two".as_slice()),
            (3, b"three".as_slice()),
        ] {
            assert_eq!(
                nodes.get(key(id).as_slice()).unwrap().unwrap().value(),
                value
            );
        }
    }

    #[test]
    fn sealed_slots_reclaim_only_when_all_retirements_commit_and_old_roots_still_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payloads.redb");
        let db = redb::Database::create(&path).unwrap();
        let write = db.begin_write().unwrap();
        put(&write, &[(1, b"one"), (2, b"two")]);
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        let old = open(&read).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = open(&write).unwrap();
            table
                .insert(key(1).as_slice(), b"successor".as_slice())
                .unwrap();
            table.remove(key(2).as_slice()).unwrap();
        }
        // An interrupted consumer cannot advance the durable cursor or remove data.
        assert_eq!(reclaim(&write, 1).unwrap(), (1, 0));
        write.abort().unwrap();
        assert_eq!(
            open(&db.begin_read().unwrap())
                .unwrap()
                .get(key(1).as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"one"
        );
        let write = db.begin_write().unwrap();
        {
            let mut table = open(&write).unwrap();
            table
                .insert(key(1).as_slice(), b"successor".as_slice())
                .unwrap();
            table.remove(key(2).as_slice()).unwrap();
        }
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        assert_eq!(reclaim(&write, 1).unwrap(), (1, 0));
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        assert_eq!(reclaim(&write, 1).unwrap(), (1, 1));
        write.commit().unwrap();
        assert_eq!(old.get(key(1).as_slice()).unwrap().unwrap().value(), b"one");
        assert_eq!(old.get(key(2).as_slice()).unwrap().unwrap().value(), b"two");
        drop(old);
        drop(read);
        let mut cursor = redb::CompactionCursor::default();
        loop {
            let write = db
                .begin_write()
                .unwrap()
                .with_table_root_hook(relocation_event);
            let progress = write.compact_step(&mut cursor, 1, |_| Ok(())).unwrap();
            if progress.complete {
                break;
            }
        }
        drop(db);
        let db = redb::Database::open(&path).unwrap();
        let read = db.begin_read().unwrap();
        let table = open(&read).unwrap();
        assert_eq!(
            table.get(key(1).as_slice()).unwrap().unwrap().value(),
            b"successor"
        );
        assert!(table.get(key(2).as_slice()).unwrap().is_none());
        assert_eq!(read.open_table(PAYLOADS).unwrap().len().unwrap(), 1);
        assert_eq!(read.open_table(RETIRED).unwrap().len().unwrap(), 0);
    }
    #[test]
    fn a_corrupted_native_binding_refuses_without_returning_other_canonical_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let db = redb::Database::create(dir.path().join("checksum.redb")).unwrap();
        let write = db.begin_write().unwrap();
        put(&write, &[(1, b"one"), (2, b"two")]);
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut directory = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
            let mut locator = directory.get_mut(key(1).as_slice()).unwrap().unwrap();
            // Corrupt the expected native checksum; keep the canonical bytes intact.
            locator.as_mut()[16] ^= 1;
        }
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        let table = open(&read).unwrap();
        assert!(matches!(
            table.get(key(1).as_slice()),
            Err(StorageError::Corrupted(_))
        ));
        assert_eq!(
            table.get(key(2).as_slice()).unwrap().unwrap().value(),
            b"two"
        );
    }

    #[test]
    fn relocation_rebinds_live_slots_and_preserves_retired_slot_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let db = redb::Database::create(dir.path().join("relocation.redb")).unwrap();
        const HOLES: TableDefinition<u64, &[u8]> = TableDefinition::new("relocation_holes");
        let write = db.begin_write().unwrap();
        {
            let mut holes = write.open_table(HOLES).unwrap();
            for n in 0..16 {
                drop(holes.insert_zeroed_reserve(n, 12000).unwrap());
            }
        }
        put(&write, &[(1, b"one"), (2, b"two")]);
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        write.delete_table(HOLES).unwrap();
        write.commit().unwrap();
        db.drain_deferred_frees().unwrap();
        let read = db.begin_read().unwrap();
        let old = open(&read).unwrap();
        let write = db.begin_write().unwrap();
        put(&write, &[(1, b"successor")]);
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        assert_eq!(reclaim(&write, 1).unwrap(), (1, 0));
        write.commit().unwrap();
        let mut cursor = redb::CompactionCursor::default();
        let mut relocated = 0;
        loop {
            let write = db
                .begin_write()
                .unwrap()
                .with_table_root_hook(relocation_event);
            let progress = write.compact_step(&mut cursor, 1, |_| Ok(())).unwrap();
            relocated += progress.pages_relocated;
            if progress.complete {
                break;
            }
        }
        assert!(
            relocated > 0,
            "the fixture creates lower free extents before pinning its old root"
        );
        assert_eq!(old.get(key(1).as_slice()).unwrap().unwrap().value(), b"one");
        assert_eq!(old.get(key(2).as_slice()).unwrap().unwrap().value(), b"two");
        let now = db.begin_read().unwrap();
        let current = open(&now).unwrap();
        assert_eq!(
            current.get(key(1).as_slice()).unwrap().unwrap().value(),
            b"successor"
        );
        assert_eq!(
            current.get(key(2).as_slice()).unwrap().unwrap().value(),
            b"two"
        );
    }

    #[test]
    fn retirement_windows_and_durable_queue_pages_resume_under_a_finite_slot_budget() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payloads.redb");
        let db = redb::Database::create(&path).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = open(&write).unwrap();
            for n in 0..450 {
                table
                    .insert(key(n).as_slice(), n.to_be_bytes().as_slice())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        let retired_at = write.native_transaction_id();
        {
            let mut table = open(&write).unwrap();
            for n in 0..450 {
                table.remove(key(n).as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
        let write = db.begin_write().unwrap();
        assert_eq!(reclaim_before(&write, 450, retired_at).unwrap(), (0, 0));
        assert_eq!(reclaim_before(&write, 1, retired_at + 1).unwrap().0, 1);
        write.abort().unwrap();
        drop(db);
        let db = redb::Database::open(&path).unwrap();
        for _ in 0..450 {
            let write = db.begin_write().unwrap();
            reclaim(&write, 1).unwrap();
            write.commit().unwrap();
        }
        let read = db.begin_read().unwrap();
        assert_eq!(read.open_table(PAYLOADS).unwrap().len().unwrap(), 0);
        assert_eq!(read.open_table(RETIRED).unwrap().len().unwrap(), 0);
    }
    #[test]
    fn wide_and_empty_canonical_values_keep_their_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("payloads.redb");
        let db = redb::Database::create(&path).unwrap();
        let bytes = vec![0xa5; 12000];
        let write = db.begin_write().unwrap();
        put(&write, &[(1, &bytes), (2, b""), (3, b"tail")]);
        write.commit().unwrap();
        let read = db.begin_read().unwrap();
        let table = open(&read).unwrap();
        assert_eq!(
            table.get(key(1).as_slice()).unwrap().unwrap().value(),
            bytes
        );
        assert!(
            table
                .get(key(2).as_slice())
                .unwrap()
                .unwrap()
                .value()
                .is_empty()
        );
        assert_eq!(
            table.get(key(3).as_slice()).unwrap().unwrap().value(),
            b"tail"
        );
        assert_eq!(read.open_table(PAYLOADS).unwrap().len().unwrap(), 2);
    }
}
