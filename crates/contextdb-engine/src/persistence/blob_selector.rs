//! Immutable complete hash unions. Sorting, deduplication and membership never
//! retain a historical hash population or enter a database/source/registry.
use super::*;
use crate::database::event_bus::OwnedEventBusHandle;
use crate::memory_accounting::OwnedMemoryReservation;

fn invalid() -> Error { Error::Other("blob selector lost its authentic immutable backing".into()) }
fn io(error: std::io::Error) -> Error {
    Error::ScratchWorkingStorageUnavailable { operation: "blob_selector".into(), io_kind: error.kind() }
}
struct HashFile {
    backend: Box<dyn redb::ScratchBackend>,
    count: u64,
    // Physical storage is destroyed before its descendant invoice.
    _memory: OwnedMemoryReservation,
}
impl HashFile {
    fn open(store: &RedbPersistence, original: &OwnedMemoryReservation) -> Result<Self> {
        let memory = crate::memory_accounting::reserve_preparation_from_original(original,
            redb::scratch_backend_allocation_size().checked_add(std::mem::size_of::<Self>() + std::mem::size_of::<FrozenBlobSelector>() + std::mem::size_of::<BlobSelectorBuilder>() + 4 * std::mem::size_of::<usize>())
                .ok_or_else(invalid)?, "discard", "blob_selector_backend",
            "Raise MEMORY_LIMIT to prepare blob erasure.")?;
        let writer = store.writer_file.read().map_err(|_| invalid())?;
        let backend = writer.as_ref().ok_or_else(invalid)?.scratch_backend().map_err(io)?;
        Ok(Self { backend, count: 0, _memory: memory })
    }
    fn append(&mut self, hash: &[u8;32]) -> Result<()> {
        let at = self.count.checked_mul(64).ok_or_else(invalid)?;
        let end = at.checked_add(64).ok_or_else(invalid)?;
        let digest = Self::digest(self.count, hash);
        self.backend.set_len(end).map_err(io)?;
        self.backend.write(at, hash).map_err(io)?;
        self.backend.write(at + 32, digest.as_bytes()).map_err(io)?;
        self.count = self.count.checked_add(1).ok_or_else(invalid)?;
        Ok(())
    }
    fn digest(ordinal: u64, hash: &[u8;32]) -> blake3::Hash {
        let mut digest = blake3::Hasher::new();
        digest.update(b"contextdb-immutable-blob-selector");
        digest.update(&ordinal.to_be_bytes()); digest.update(hash); digest.finalize()
    }
    fn at(&self, ordinal: u64) -> Result<[u8;32]> {
        if ordinal >= self.count { return Err(invalid()); }
        let mut record = [0u8;64];
        self.backend.read(ordinal.checked_mul(64).ok_or_else(invalid)?, &mut record).map_err(io)?;
        let hash: [u8;32] = record[..32].try_into().map_err(|_| invalid())?;
        if Self::digest(ordinal, &hash).as_bytes() != &record[32..] { return Err(invalid()); }
        Ok(hash)
    }
    fn clear(&mut self) -> Result<()> { self.backend.set_len(0).map_err(io)?; self.count=0; Ok(()) }
}

/// Builder's only inputs are authenticated source visits. Coordinates are not
/// authority: the actual source root, store and Original owner remain retained.
pub(crate) struct BlobSelectorBuilder {
    input: HashFile,
    scratch: HashFile,
    store: Arc<RedbPersistence>,
    source: writer_source::WriterSnapshotHandle,
    original_address: usize,
    _original_owner: OwnedEventBusHandle,
}
impl BlobSelectorBuilder {
    pub(crate) fn new(store: Arc<RedbPersistence>, source: writer_source::WriterSnapshotHandle,
        events: &OwnedEventBusHandle, original: &OwnedMemoryReservation) -> Result<Self> {
        source.require_working_store(&store)?;
        events.with_owned_persistence_original(|held| {
            if !std::ptr::eq(held,original) { return Err(invalid()); } Ok(())
        })?;
        Ok(Self { input: HashFile::open(&store,original)?, scratch: HashFile::open(&store,original)?,
            store,source,original_address:original as *const _ as usize,_original_owner:events.clone() })
    }
    pub(crate) fn push(&mut self, hash: [u8;32]) -> Result<()> { self.input.append(&hash) }
    pub(crate) fn extend(&mut self, selector: &FrozenBlobSelector) -> Result<()> {
        if !Arc::ptr_eq(&self.store,&selector.store) || self.original_address != selector.original_address {
            return Err(invalid());
        }
        for ordinal in 0..selector.len() { self.push(selector.at(ordinal)?)?; } Ok(())
    }
    pub(crate) fn seal(mut self) -> Result<Arc<FrozenBlobSelector>> {
        // Bottom-up external merge: two current hashes and two unlinked files,
        // O(N log N) I/O, with no run directory proportional to N.
        let count = self.input.count;
        let mut width = 1u64;
        while width < count {
            self.scratch.clear()?;
            let mut start = 0u64;
            while start < count {
                let middle = start.saturating_add(width).min(count);
                let end = middle.saturating_add(width).min(count);
                let (mut left,mut right) = (start,middle);
                while left < middle || right < end {
                    let take_left = right == end || (left < middle && self.input.at(left)? <= self.input.at(right)?);
                    let hash = if take_left { let value=self.input.at(left)?; left+=1; value }
                        else { let value=self.input.at(right)?; right+=1; value };
                    self.scratch.append(&hash)?;
                }
                start=end;
            }
            std::mem::swap(&mut self.input,&mut self.scratch);
            width = width.checked_mul(2).unwrap_or(count);
        }
        self.scratch.clear()?;
        let mut prior = None;
        for ordinal in 0..count {
            let hash=self.input.at(ordinal)?;
            if prior.is_some_and(|prior| prior > hash) { return Err(invalid()); }
            if prior != Some(hash) { self.scratch.append(&hash)?; prior=Some(hash); }
        }
        Ok(Arc::new(FrozenBlobSelector { file:self.scratch,store:self.store,source:self.source,
            original_address:self.original_address,_original_owner:self._original_owner }))
    }
}

pub(crate) struct FrozenBlobSelector {
    file: HashFile,
    store: Arc<RedbPersistence>,
    source: writer_source::WriterSnapshotHandle,
    original_address: usize,
    _original_owner: OwnedEventBusHandle,
}
impl FrozenBlobSelector {
    pub(crate) fn registry_memory(&self, bytes:usize) -> Result<OwnedMemoryReservation> {
        self._original_owner.with_owned_persistence_original(|original| {
            self.require(&self.store,original)?;
            crate::memory_accounting::reserve_preparation_from_original(original,bytes,
                "discard","blob_selector_registry","Raise MEMORY_LIMIT to exclude blob erasure candidates.")
        })
    }
    pub(crate) fn len(&self) -> u64 { self.file.count }
    pub(crate) fn at(&self, ordinal:u64) -> Result<[u8;32]> { self.file.at(ordinal) }
    pub(crate) fn contains(&self, hash:&[u8;32]) -> Result<bool> {
        let (mut lo,mut hi)=(0,self.len());
        while lo < hi { let mid=lo+(hi-lo)/2; match self.at(mid)?.cmp(hash) {
            std::cmp::Ordering::Equal => return Ok(true),
            std::cmp::Ordering::Less => lo=mid+1, std::cmp::Ordering::Greater => hi=mid,
        }} Ok(false)
    }
    pub(crate) fn require_store(&self, store:&RedbPersistence) -> Result<()> {
        self.source.require_working_store(store)?;
        if !std::ptr::eq(self.store.as_ref(),store) {return Err(invalid());}
        Ok(())
    }
    pub(crate) fn require(&self, store:&RedbPersistence, original:&OwnedMemoryReservation) -> Result<()> {
        self.require_store(store)?;
        if self.original_address != original as *const _ as usize {
            return Err(invalid());
        } Ok(())
    }
    pub(crate) fn is_subset_of(&self, other:&Self) -> Result<bool> {
        let mut right=0;
        for left in 0..self.len() {
            let hash=self.at(left)?;
            while right < other.len() && other.at(right)? < hash { right+=1; }
            if right == other.len() || other.at(right)? != hash { return Ok(false); }
        } Ok(true)
    }
}
