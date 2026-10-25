//! Writer-only identity/copy journal. Query eligibility uses paged visibility.
//! Copy links are native pages, never a resident list of members or generations.
use super::{
    codec,
    memory::corrupt,
    storage::{BuildPhase, Manifest, PAGE_BYTES, PageKind, ReadPages, WritePages},
};
use contextdb_core::Result;
const HEADER_BYTES: usize = 40;
const HEADERS_PER_PAGE: usize = PAGE_BYTES / HEADER_BYTES;
const LINK_BYTES: usize = 24;
const LINKS_PER_PAGE: usize = PAGE_BYTES / LINK_BYTES;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CopyRef {
    generation: u64,
    slot: u64,
}
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Version {
    pub lsn: Option<u64>,
    head: Option<CopyRef>,
    copies: u64,
    pub deleted: bool,
}
#[derive(Clone, Copy)]
struct Active {
    target: u64,
    serving: Option<u64>,
    legacy: Option<u64>,
    building: Option<u64>,
}
impl Active {
    fn load(s: &impl ReadPages, m: Manifest) -> Result<Self> {
        let serving = s.manifest(m.partition)?;
        let legacy = if let Some(current) = serving {
            super::catalog::Catalog::load(s, current)?
                .legacy
                .map(|x| x.generation)
        } else {
            None
        };
        let building = s
            .build_state(m.partition)?
            .filter(|b| b.phase != BuildPhase::Ready)
            .map(|b| b.manifest.generation);
        Ok(Self {
            target: m.generation,
            serving: serving.map(|x| x.generation),
            legacy,
            building,
        })
    }
    fn contains(self, g: u64) -> bool {
        g == self.target
            || self.serving == Some(g)
            || self.legacy == Some(g)
            || self.building == Some(g)
    }
    fn minimum(self) -> u64 {
        [Some(self.target), self.serving, self.legacy, self.building]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(self.target)
    }
}
impl CopyRef {
    fn key(self, m: Manifest) -> super::storage::PageKey {
        let mut view = m;
        view.generation = 0;
        view.run = self.generation;
        view.key(PageKind::RowCopies, 0, self.slot / LINKS_PER_PAGE as u64)
    }
    fn next(self, s: &impl ReadPages, m: Manifest) -> Result<Option<Self>> {
        let mut page = [0; PAGE_BYTES];
        if !super::storage::read_page(s, self.key(m), &mut page)? {
            return Err(corrupt());
        }
        let at = (self.slot % LINKS_PER_PAGE as u64) as usize * LINK_BYTES;
        let w = |i: usize| -> Result<u64> {
            Ok(u64::from_le_bytes(
                page[at + i * 8..at + i * 8 + 8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            ))
        };
        let flags = w(0)?;
        if flags != 1 && flags != 3 {
            return Err(corrupt());
        }
        let next = if flags == 3 {
            Some(Self {
                generation: w(1)?,
                slot: w(2)?,
            })
        } else {
            None
        };
        // Allocation order is monotone across generation/slot identities; reject cycles/forward links.
        if next.is_some_and(|x| x >= self) {
            return Err(corrupt());
        }
        Ok(next)
    }
    fn store(self, t: &mut impl WritePages, m: Manifest, next: Option<Self>) -> Result<()> {
        if next.is_some_and(|x| x >= self) {
            return Err(corrupt());
        }
        let key = self.key(m);
        let mut page = [0; PAGE_BYTES];
        super::storage::read_page(t, key, &mut page)?;
        let at = (self.slot % LINKS_PER_PAGE as u64) as usize * LINK_BYTES;
        let words = [
            if next.is_some() { 3u64 } else { 1 },
            next.map_or(0, |x| x.generation),
            next.map_or(0, |x| x.slot),
        ];
        for (dst, w) in page[at..at + LINK_BYTES]
            .as_chunks_mut::<8>()
            .0
            .iter_mut()
            .zip(words)
        {
            dst.copy_from_slice(&w.to_le_bytes());
        }
        super::storage::put_page(t, key, &page)
    }
}
impl Version {
    pub fn load(s: &impl ReadPages, m: Manifest, row: u64) -> Result<Self> {
        let mut page = [0; PAGE_BYTES];
        if !super::storage::read_page(
            s,
            m.key(PageKind::RowVersions, 0, row / HEADERS_PER_PAGE as u64),
            &mut page,
        )? {
            return Ok(Self::default());
        }
        let at = (row % HEADERS_PER_PAGE as u64) as usize * HEADER_BYTES;
        let w = |i: usize| -> Result<u64> {
            Ok(u64::from_le_bytes(
                page[at + i * 8..at + i * 8 + 8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            ))
        };
        let flags = w(4)?;
        let copies = w(3)?;
        copies.checked_add(1).ok_or_else(corrupt)?;
        if flags & !3 != 0
            || (flags & 1 == 0 && (copies != 0 || flags != 0))
            || (flags & 2 != 0 && copies != 0)
        {
            return Err(corrupt());
        }
        Ok(Self {
            lsn: if flags & 1 != 0 { Some(w(0)?) } else { None },
            head: if copies != 0 {
                Some(CopyRef {
                    generation: w(1)?,
                    slot: w(2)?,
                })
            } else {
                None
            },
            copies,
            deleted: flags & 2 != 0,
        })
    }
    pub fn store(self, t: &mut impl WritePages, m: Manifest, row: u64) -> Result<()> {
        let key = m.key(PageKind::RowVersions, 0, row / HEADERS_PER_PAGE as u64);
        let mut page = [0; PAGE_BYTES];
        super::storage::read_page(t, key, &mut page)?;
        let at = (row % HEADERS_PER_PAGE as u64) as usize * HEADER_BYTES;
        let flags = u64::from(self.lsn.is_some()) | u64::from(self.deleted) << 1;
        let words = [
            self.lsn.unwrap_or(0),
            self.head.map_or(0, |x| x.generation),
            self.head.map_or(0, |x| x.slot),
            self.copies,
            flags,
        ];
        for (dst, w) in page[at..at + HEADER_BYTES]
            .as_chunks_mut::<8>()
            .0
            .iter_mut()
            .zip(words)
        {
            dst.copy_from_slice(&w.to_le_bytes());
        }
        super::storage::put_page(t, key, &page)
    }
    pub fn current_lsn(self) -> Option<u64> {
        if self.deleted { None } else { self.lsn }
    }
    pub fn invalidate(&mut self, t: &mut impl WritePages, m: Manifest) -> Result<()> {
        let active = Active::load(t, m)?;
        let mut node = self.head;
        let mut seen = 0u64;
        while let Some(copy) = node {
            t.check_cancel()?;
            if copy.generation < active.minimum() {
                break;
            }
            seen = seen.checked_add(1).ok_or_else(corrupt)?;
            if seen > self.copies {
                return Err(corrupt());
            }
            node = copy.next(t, m)?;
            if active.contains(copy.generation) {
                let mut view = m;
                view.generation = copy.generation;
                codec::set_visible(t, view, copy.slot, false)?;
            }
        }
        self.head = None;
        self.copies = 0;
        Ok(())
    }
    pub fn reset(&mut self, lsn: u64, deleted: bool) {
        self.lsn = Some(lsn);
        self.head = None;
        self.copies = 0;
        self.deleted = deleted;
    }
    /// Keep every still-active copy, including legacy and unpublished remap copies.
    /// An older retired suffix can be dropped without reading/recreating reclaimed pages.
    pub fn add_copy(
        &mut self,
        t: &mut impl WritePages,
        m: Manifest,
        generation: u64,
        slot: u64,
    ) -> Result<()> {
        self.copies.checked_add(1).ok_or_else(corrupt)?;
        let active = Active::load(t, m)?;
        let fresh = CopyRef { generation, slot };
        let mut node = self.head;
        let mut kept_head = None;
        let mut last: Option<CopyRef> = None;
        let mut count = 0u64;
        let mut seen = 0u64;
        while let Some(copy) = node {
            t.check_cancel()?;
            if copy.generation < active.minimum() {
                break;
            }
            seen = seen.checked_add(1).ok_or_else(corrupt)?;
            if seen > self.copies {
                return Err(corrupt());
            }
            node = copy.next(t, m)?;
            if active.contains(copy.generation) {
                if copy == fresh {
                    return Err(corrupt());
                }
                if let Some(previous) = last {
                    previous.store(t, m, Some(copy))?;
                } else {
                    kept_head = Some(copy);
                }
                last = Some(copy);
                count = count.checked_add(1).ok_or_else(corrupt)?;
            }
        }
        if node.is_none() && seen != self.copies {
            return Err(corrupt());
        }
        if let Some(last) = last {
            last.store(t, m, None)?;
        }
        fresh.store(t, m, kept_head)?;
        self.head = Some(fresh);
        self.copies = count.checked_add(1).ok_or_else(corrupt)?;
        Ok(())
    }
}
