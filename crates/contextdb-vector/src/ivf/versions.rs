//! Writer-only row/version journal. Queries use paged visibility, never a row lookup.
use super::{
    codec,
    memory::corrupt,
    storage::{Manifest, PAGE_BYTES, PageKind, ReadPages, WritePages},
};
use contextdb_core::Result;
const BYTES: usize = 48;
const PER_PAGE: usize = PAGE_BYTES / BYTES;
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Version {
    pub lsn: u64,
    pub current: Option<(u64, u64)>,
    pub other: Option<(u64, u64)>,
    pub deleted: bool,
}
impl Version {
    pub fn load(s: &impl ReadPages, m: Manifest, row: u64) -> Result<Self> {
        let mut page = [0; PAGE_BYTES];
        if !super::storage::read_page(
            s,
            m.key(PageKind::RowVersions, 0, row / PER_PAGE as u64),
            &mut page,
        )? {
            return Ok(Self::default());
        }
        let at = (row % PER_PAGE as u64) as usize * BYTES;
        let w = |i: usize| -> Result<u64> {
            Ok(u64::from_le_bytes(
                page[at + i * 8..at + i * 8 + 8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            ))
        };
        let flags = w(5)?;
        if flags & !7 != 0 {
            return Err(corrupt());
        }
        Ok(Self {
            lsn: w(0)?,
            current: if flags & 1 != 0 {
                Some((w(1)?, w(2)?))
            } else {
                None
            },
            other: if flags & 2 != 0 {
                Some((w(3)?, w(4)?))
            } else {
                None
            },
            deleted: flags & 4 != 0,
        })
    }
    pub fn store(self, tx: &mut impl WritePages, m: Manifest, row: u64) -> Result<()> {
        let key = m.key(PageKind::RowVersions, 0, row / PER_PAGE as u64);
        let mut page = [0; PAGE_BYTES];
        super::storage::read_page(tx, key, &mut page)?;
        let at = (row % PER_PAGE as u64) as usize * BYTES;
        let flags = u64::from(self.current.is_some())
            | u64::from(self.other.is_some()) << 1
            | u64::from(self.deleted) << 2;
        for (dst, w) in page[at..at + BYTES].chunks_exact_mut(8).zip([
            self.lsn,
            self.current.map_or(0, |x| x.0),
            self.current.map_or(0, |x| x.1),
            self.other.map_or(0, |x| x.0),
            self.other.map_or(0, |x| x.1),
            flags,
        ]) {
            dst.copy_from_slice(&w.to_le_bytes());
        }
        super::storage::put_page(tx, key, &page)
    }
    pub fn invalidate(self, tx: &mut impl WritePages, m: Manifest) -> Result<()> {
        let c = super::catalog::Catalog::load(tx, m)?;
        let building = tx
            .build_state(m.partition)?
            .filter(|b| b.phase != super::storage::BuildPhase::Ready)
            .map(|b| b.manifest.generation);
        for (generation, slot) in self.current.into_iter().chain(self.other) {
            // Retired generations may already be reclaimed. Never recreate their pages.
            if generation != m.generation
                && c.legacy.map(|x| x.generation) != Some(generation)
                && building != Some(generation)
            {
                continue;
            }
            let mut view = m;
            view.generation = generation;
            codec::set_visible(tx, view, slot, false)?;
        }
        Ok(())
    }
    /// Promote a former build/remap copy when it is now the serving generation.
    pub fn promote(&mut self, generation: u64) {
        if self.other.is_some_and(|x| x.0 == generation) {
            self.current = self.other;
            self.other = None;
        }
    }
}
