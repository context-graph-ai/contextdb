//! Production logical-payload ledger, including failed admissions and AUTO fallback work.
use super::{memory::corrupt, storage::*, types::Work};
use contextdb_core::{Result, VectorIndexRef};
use std::cell::Cell;
pub(crate) struct Tracked<'a, S> {
    pub source: &'a S,
    pages: Cell<Work>,
}
impl<'a, S> Tracked<'a, S> {
    pub fn new(source: &'a S) -> Self {
        Self {
            source,
            pages: Cell::new(Work::default()),
        }
    }
    pub fn apply(&self, w: &mut Work) {
        let p = self.pages.get();
        w.list_pages_read = p.list_pages_read;
        w.codebook_pages_read = p.codebook_pages_read;
        w.accumulator_pages_read = p.accumulator_pages_read;
        w.directory_pages_read = p.directory_pages_read;
        w.directory_presence_pages_read = p.directory_presence_pages_read;
        w.canonical_pages_read = p.canonical_pages_read;
        w.visibility_pages_read = p.visibility_pages_read;
        w.catalog_pages_read = p.catalog_pages_read;
        w.slot_metadata_pages_read = p.slot_metadata_pages_read;
        w.tail_pages_read = p.tail_pages_read;
        w.list_link_pages_read = p.list_link_pages_read;
        w.row_version_pages_read = p.row_version_pages_read;
        w.row_copy_pages_read = p.row_copy_pages_read;
    }
}
impl<S: ReadPages> ReadPages for Tracked<'_, S> {
    fn retained_codebook(&self, manifest: Manifest) -> Option<&super::RetainedCodebook> {
        self.source.retained_codebook(manifest)
    }
    fn inspection_verdict(&self, p: u64, g: u64) -> Option<Verdict> {
        self.source.inspection_verdict(p, g)
    }
    fn record_inspection_verdict(&self, p: u64, g: u64, v: Verdict) {
        self.source.record_inspection_verdict(p, g, v);
    }

    fn snapshot_lsn(&self) -> Result<u64> {
        self.source.snapshot_lsn()
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.source.index_identity()
    }
    fn check_cancel(&self) -> Result<()> {
        self.source.check_cancel()
    }
    fn before_distance(&self, k: DistanceKind) -> Result<()> {
        self.source.before_distance(k)
    }
    fn manifest(&self, p: u64) -> Result<Option<Manifest>> {
        self.source.manifest(p)
    }
    fn build_state(&self, p: u64) -> Result<Option<BuildState>> {
        self.source.build_state(p)
    }
    fn next_partition(&self, p: Option<u64>) -> Result<Option<u64>> {
        self.source.next_partition(p)
    }
    fn read_admitted_page(&self, k: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        let mut p = self.pages.get();
        let field = match k.kind {
            PageKind::Lists => Some(&mut p.list_pages_read),
            PageKind::Codebook => Some(&mut p.codebook_pages_read),
            PageKind::Accumulators => Some(&mut p.accumulator_pages_read),
            PageKind::Directory => Some(&mut p.directory_pages_read),
            PageKind::DirectoryPresence => Some(&mut p.directory_presence_pages_read),
            PageKind::Canonical => Some(&mut p.canonical_pages_read),
            PageKind::Visibility => Some(&mut p.visibility_pages_read),
            PageKind::Catalog => Some(&mut p.catalog_pages_read),
            PageKind::SlotMetadata => Some(&mut p.slot_metadata_pages_read),
            PageKind::Tail => Some(&mut p.tail_pages_read),
            PageKind::ListLinks | PageKind::ListUsed => Some(&mut p.list_link_pages_read),
            PageKind::RowVersions => Some(&mut p.row_version_pages_read),
            PageKind::RowCopies => Some(&mut p.row_copy_pages_read),
            PageKind::ListAllocator | PageKind::Reservoir => None,
        };
        if let Some(f) = field {
            *f = f.checked_add(1).ok_or_else(corrupt)?;
        }
        self.pages.set(p);
        self.source.read_admitted_page(k, out)
    }
}
