//! In-place authoritative erasure under the caller's existing transaction/fence.
use super::super::storage::PAGE_BYTES;
use super::*;

impl<S: PageStore> ClusteredIndex<S> {
    /// Erase one serving slot's stored vector and every physical int8 copy.
    /// Slot addresses remain valid for eligibility and run links. Row identities
    /// and vector bytes are zeroed in place; the deletion is journaled.
    pub fn erase_row(&mut self, partition: u64, slot: u64) -> Result<()> {
        self.store.check_cancel()?;
        let manifest = self
            .store
            .manifest(partition)?
            .ok_or_else(|| phase("clustered erasure has no serving generation"))?;
        if slot >= manifest.slots {
            return Err(phase(
                "clustered erasure slot exceeds the serving generation",
            ));
        }
        let stride = manifest
            .dimensions
            .checked_mul(4)
            .and_then(|n| n.checked_add(8))
            .ok_or_else(overflow)?;
        let offset = slot.checked_mul(stride as u64).ok_or_else(overflow)?;
        let list_stride = manifest.dimensions.checked_add(16).ok_or_else(overflow)?;
        let buffer_stride = self.buffer_stride();
        codec::read(
            &self.store,
            manifest,
            PageKind::Canonical,
            0,
            offset,
            &mut self.raw[..stride],
        )?;
        let mut visibility = [0; PAGE_BYTES];
        let mut loaded = None;
        if self.raw[..stride].iter().all(|byte| *byte == 0)
            && !codec::visible(&self.store, manifest, slot, &mut visibility, &mut loaded)?
        {
            return Ok(());
        }
        let row = u64::from_le_bytes(self.raw[..8].try_into().map_err(|_| corrupt())?);
        let mut tx = self.store.begin()?;
        let mut version = Version::load(&tx, manifest, row)?;
        version.invalidate(&mut tx, manifest)?;
        codec::set_visible(&mut tx, manifest, slot, false)?;
        self.raw.fill(0);
        codec::write(
            &mut tx,
            manifest,
            PageKind::Canonical,
            0,
            offset,
            &self.raw[..stride],
        )?;
        let mut allocator = [0; PAGE_BYTES];
        let mut page = [0; PAGE_BYTES];
        for list in 0..manifest.lists {
            tx.check_cancel()?;
            let address = u64::from(list).checked_mul(8).ok_or_else(overflow)?;
            let allocator_key =
                manifest.key(PageKind::ListAllocator, 0, address / PAGE_BYTES as u64);
            if !super::super::storage::read_page(&tx, allocator_key, &mut allocator)? {
                continue;
            }
            let at = (address % PAGE_BYTES as u64) as usize;
            let allocated =
                u64::from_le_bytes(allocator[at..at + 8].try_into().map_err(|_| corrupt())?);
            if allocated > manifest.slots {
                return Err(corrupt());
            }
            for ordinal in 0..allocated {
                tx.check_cancel()?;
                let mut count = [0; 8];
                codec::read(
                    &tx,
                    manifest,
                    PageKind::ListUsed,
                    list,
                    ordinal.checked_mul(8).ok_or_else(overflow)?,
                    &mut count,
                )?;
                let used = usize::try_from(u64::from_le_bytes(count)).map_err(|_| corrupt())?;
                if used > PAGE_BYTES / list_stride {
                    return Err(corrupt());
                }
                let key = manifest.key(PageKind::Lists, list, ordinal);
                if !super::super::storage::read_page(&tx, key, &mut page)? {
                    return Err(super::super::memory::corrupt_page(key));
                }
                let mut changed = false;
                for record in page[..used * list_stride].chunks_exact_mut(list_stride) {
                    if u64::from_le_bytes(record[..8].try_into().map_err(|_| corrupt())?) == slot {
                        record[8..].fill(0);
                        changed = true;
                    }
                }
                if changed {
                    super::super::storage::put_page(&mut tx, key, &page)?;
                }
            }
        }
        version.reset(tx.commit_lsn()?, true);
        version.store(&mut tx, manifest, row)?;
        tx.commit()?;
        // An acknowledged erasure also removes its evictable writer-window copy.
        let old_len = self.buffer.len();
        let mut retained = 0;
        for start in (0..old_len).step_by(buffer_stride) {
            let record = &self.buffer[start..start + buffer_stride];
            let erased = u64::from_le_bytes(record[..8].try_into().map_err(|_| corrupt())?)
                == partition
                && u64::from_le_bytes(record[16..24].try_into().map_err(|_| corrupt())?) == slot
                && u64::from_le_bytes(record[24..32].try_into().map_err(|_| corrupt())?)
                    == manifest.generation;
            if !erased {
                self.buffer
                    .copy_within(start..start + buffer_stride, retained);
                retained += buffer_stride;
            }
        }
        self.buffer[retained..old_len].fill(0);
        self.buffer.truncate(retained);
        self.raw.fill(0);
        self.values.fill(0.0);
        self.encoded.fill(0);
        Ok(())
    }
}
