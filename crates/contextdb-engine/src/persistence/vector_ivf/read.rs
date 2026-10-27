//! Captured native scope admission and role-specific authenticated payloads.
//! The existing retained holder supplies the root; no latest read is opened.
use super::*;
use super::{
    control::ScopeRecord,
    header::{AdmittedIvfPage, HeaderIdentity},
    io::{KindTableName, NativeIo},
    memory::{Invoice, ScopeConstruction},
    semantic::SemanticView,
};
use contextdb_vector::ivf::storage::{
    BuildState, DistanceKind, Manifest, PageKey, PageKind, ReadPages, Verdict,
};
use contextdb_vector::{MemoryBudgetOwner, ivf::Work};
use std::cell::{Cell, RefCell};

/// This borrowed capability keeps the existing source/holder and its original
/// native invoice alive. Its constructor authenticates shared authorities once.
pub(super) struct NativeAuthority<'a> {
    pub root: &'a super::super::vector_paged::CapturedPagedRoot,
    pub snapshot: contextdb_core::SnapshotId,
    pub snapshot_lsn: u64,
    owner: MemoryBudgetOwner,
    admission: super::super::native_storage_admission::StorageAdmissionScope,
    _invoice: Invoice,
}
impl<'a> NativeAuthority<'a> {
    pub fn open(
        root: &'a super::super::vector_paged::CapturedPagedRoot,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Self> {
        use super::super::native_storage_admission::{
            StorageAdmissionScope, original_storage_error,
        };
        let owner = MemoryBudgetOwner::from(memory.clone()).preserving_read_copy_errors();
        let invoice = Invoice::reserve(
            &owner,
            std::mem::size_of::<Self>(),
            "clustered_source_control",
        )?;
        let admission =
            StorageAdmissionScope::reader_operation(memory).map_err(original_storage_error)?;
        let (snapshot, snapshot_lsn) = admission.run_original_unqualified(|| {
            let (_, snapshot, lsn) = root.authenticate()?;
            super::super::vector_membership_pages::validate_in_read(&root.read)?;
            let scopes = root
                .read
                .table_root_fingerprint(SCOPES)
                .map_err(RedbPersistence::storage_error)?;
            if scopes.is_some() {
                let expected =
                    super::super::content_identity::vector_pages_root_in_read(&root.read)?;
                let actual =
                    super::super::content_identity::vector_schema_pages_root_in_read(&root.read)?;
                if actual != expected {
                    return Err(corrupt());
                }
            }
            Ok((snapshot, lsn.0))
        })?;
        Ok(Self {
            root,
            snapshot,
            snapshot_lsn,
            owner,
            admission,
            _invoice: invoice,
        })
    }
    pub fn vector_owner(&self) -> &MemoryBudgetOwner {
        &self.owner
    }
    fn native(&self) -> NativeIo<'_> {
        NativeIo::CapturedTransaction {
            read: &self.root.read,
            admission: &self.admission,
        }
    }
    fn source_summary(
        &self,
        route: &[u8],
    ) -> Result<super::super::vector_membership_pages::MembershipSummary> {
        use super::super::vector_membership_pages::{
            MembershipSummary, SOURCE, SUMMARY, decode_summary,
        };
        if let Some(summary) = self.native().with_value(SUMMARY.name(), route, |value| {
            value.map(|v| decode_summary(route, v)).transpose()
        })? {
            return Ok(summary);
        }
        self.admission.run_original_unqualified(|| {
            match self.root.read.open_byte_table_bounded(SOURCE) {
                Ok(source) => {
                    let mut cursor = source
                        .prefix_bounded(route)
                        .map_err(RedbPersistence::storage_error)?;
                    if cursor
                        .next()
                        .transpose()
                        .map_err(RedbPersistence::storage_error)?
                        .is_some()
                    {
                        return Err(corrupt());
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            }
            Ok(MembershipSummary::default())
        })
    }
}

/// Complete fixed scope metadata plus the one exact-capacity tuple lookup key.
/// Native/cache frames retain their own original invoices.
pub(super) struct PageSource<'a> {
    pub authority: &'a NativeAuthority<'a>,
    pub index: &'a VectorIndexRef,
    pub tuple: &'a VectorPartitionKey,
    pub route: Vec<u8>,
    pub record: RefCell<ScopeRecord>,
    pub source_summary: super::super::vector_membership_pages::MembershipSummary,
    pub scope_key: [u8; 64],
    verified: [Cell<u16>; control::ROOTS],
    verdicts: [Cell<Option<Verdict>>; control::ROOTS],
    page_work: Cell<Work>,
    checkpoint: &'a dyn Fn() -> Result<()>,
    distance: &'a dyn Fn(DistanceKind) -> Result<()>,
    _invoice: Invoice,
}
impl<'a> PageSource<'a> {
    pub fn open(
        authority: &'a NativeAuthority<'a>,
        index: &'a VectorIndexRef,
        tuple: &'a VectorPartitionKey,
        checkpoint: &'a dyn Fn() -> Result<()>,
        distance: &'a dyn Fn(DistanceKind) -> Result<()>,
    ) -> Result<Option<Self>> {
        checkpoint()?;
        let construction =
            ScopeConstruction::reserve::<Self>(authority.vector_owner(), index, tuple)?;
        let route = construction.route(index, tuple)?;
        let schema = authority.admission.run_original_unqualified(|| {
            RedbPersistence::vector_schema_binding_in_read(&authority.root.read, index)
        })?;
        let digest = control::tuple_digest(tuple);
        let key = control::scope_key(control::index_digest(index, schema), digest);
        let native = authority.native();
        let record = native.with_value(SCOPES.name(), &key, |value| {
            value
                .map(|value| ScopeRecord::decode(index, &key, value, schema, digest))
                .transpose()
        })?;
        let Some(record) = record else {
            return Ok(None);
        };
        if record.committed_through_lsn > authority.snapshot_lsn {
            return Err(corrupt());
        }
        let source_summary = authority.source_summary(&route)?;
        // Incomplete private builds are inspectable but cannot supply a native
        // canonical search. Serving scopes must match actual applied coverage.
        if record.canonical_complete
            && (record.current_membership_entries != source_summary.entries
                || record.current_membership_live_entries != source_summary.live_entries
                || record.current_membership_digest != source_summary.source_identity())
        {
            return Err(corrupt());
        }
        for family in record.families.iter().flatten() {
            if family.committed_through_lsn > authority.snapshot_lsn {
                return Err(corrupt());
            }
        }
        Ok(Some(Self {
            authority,
            index,
            tuple,
            route,
            record: RefCell::new(record),
            source_summary,
            scope_key: key,
            verified: std::array::from_fn(|_| Cell::new(0)),
            verdicts: std::array::from_fn(|_| Cell::new(None)),
            page_work: Cell::new(Work::default()),
            checkpoint,
            distance,
            _invoice: construction.invoice,
        }))
    }
    fn native(&self) -> NativeIo<'_> {
        self.authority.native()
    }
    fn family_slot(&self, generation: u64) -> Option<usize> {
        self.record
            .borrow()
            .families
            .iter()
            .position(|f| f.as_ref().is_some_and(|f| f.generation == generation))
    }
    /// Verify only a role actually loaded. Unused list/build/journal damage
    /// cannot poison healthy canonical fallback through a shared root stamp.
    fn verify_kind(&self, key: PageKey) -> Result<bool> {
        let Some(slot) = self.family_slot(key.generation) else {
            return Ok(false);
        };
        let record = self.record.borrow();
        let family = record.families[slot].as_ref().ok_or_else(corrupt)?;
        let tag = header::kind_tag(key.kind);
        let bit = 1u16 << (tag - 1);
        let present = family.kinds.present & bit != 0;
        if self.verified[slot].get() & bit != 0 {
            return Ok(present);
        }
        let name = KindTableName::new(family.storage, key.kind)?;
        let actual = self.native().fingerprint(name.as_str()?)?;
        let expected = present.then_some(family.kinds.roots[usize::from(tag - 1)]);
        if actual != expected {
            return Err(corrupt());
        }
        self.verified[slot].set(self.verified[slot].get() | bit);
        Ok(present)
    }
    fn attempted(&self, key: PageKey) -> Result<()> {
        let mut work = self.page_work.get();
        let counter = match key.kind {
            PageKind::Canonical => &mut work.canonical_pages_read,
            PageKind::Lists => &mut work.list_pages_read,
            PageKind::Directory => &mut work.directory_pages_read,
            PageKind::DirectoryPresence => &mut work.directory_presence_pages_read,
            PageKind::Visibility => &mut work.visibility_pages_read,
            PageKind::Codebook => &mut work.codebook_pages_read,
            PageKind::Accumulators => &mut work.accumulator_pages_read,
            PageKind::Reservoir => return Ok(()),
            PageKind::Catalog => &mut work.catalog_pages_read,
            PageKind::SlotMetadata => &mut work.slot_metadata_pages_read,
            PageKind::Tail => &mut work.tail_pages_read,
            PageKind::ListLinks => &mut work.list_link_pages_read,
            PageKind::ListAllocator | PageKind::ListUsed => &mut work.directory_pages_read,
            PageKind::RowVersions => &mut work.row_version_pages_read,
            PageKind::RowCopies => &mut work.row_copy_pages_read,
        };
        *counter = counter.checked_add(1).ok_or_else(corrupt)?;
        self.page_work.set(work);
        Ok(())
    }
    fn with_page<T>(
        &self,
        key: PageKey,
        visit: impl FnOnce(Option<AdmittedIvfPage<'_>>) -> Result<T>,
    ) -> Result<T> {
        (self.checkpoint)()?;
        self.attempted(key)?;
        if key.partition != 0 {
            return Err(corrupt());
        }
        if !self.verify_kind(key)? {
            return visit(None);
        }
        let record = self.record.borrow();
        let family = record.family(key.generation).ok_or_else(corrupt)?;
        let name = KindTableName::new(family.storage, key.kind)?;
        self.native()
            .with_existing_value(name.as_str()?, &header::key_bytes(key), |value| {
                let value = value
                    .map(|value| -> Result<_> {
                        let page = AdmittedIvfPage::open(
                            HeaderIdentity {
                                index: &record.index,
                                tuple: &record.tuple,
                                family,
                            },
                            key,
                            value,
                        )?;
                        super::frontier::validate(
                            &self.native(),
                            &self.scope_key,
                            family,
                            key,
                            self.authority.snapshot_lsn,
                        )?;
                        Ok(page)
                    })
                    .transpose()?;
                visit(value)
            })
    }
    /// Actual payload attempts include internal semantic-validation metadata
    /// reads. Distance counters remain the kernel's actual completed scores.
    pub fn actual_page_work(&self) -> Work {
        self.page_work.get()
    }
    pub fn canonical_ready(&self) -> bool {
        let record = self.record.borrow();
        record.canonical_complete
            && (record.serving.is_some() || self.source_summary.live_entries == 0)
    }
}
impl ReadPages for PageSource<'_> {
    fn inspection_verdict(&self, partition: u64, generation: u64) -> Option<Verdict> {
        if partition != 0 {
            return None;
        }
        self.family_slot(generation)
            .and_then(|s| self.verdicts[s].get())
    }
    fn record_inspection_verdict(&self, partition: u64, generation: u64, verdict: Verdict) {
        if partition == 0
            && let Some(slot) = self.family_slot(generation)
        {
            self.verdicts[slot].set(Some(verdict));
        }
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        Ok(self.authority.snapshot_lsn)
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.index
    }
    fn check_cancel(&self) -> Result<()> {
        (self.checkpoint)()
    }
    fn before_distance(&self, kind: DistanceKind) -> Result<()> {
        (self.distance)(kind)
    }
    fn manifest(&self, partition: u64) -> Result<Option<Manifest>> {
        if partition != 0 {
            return Ok(None);
        }
        self.record.borrow().manifest()
    }
    fn build_state(&self, partition: u64) -> Result<Option<BuildState>> {
        if partition != 0 {
            return Ok(None);
        }
        self.record.borrow().build_state()
    }
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>> {
        Ok(
            if after.is_none() && self.record.borrow().serving.is_some() {
                Some(0)
            } else {
                None
            },
        )
    }
    fn read_admitted_page(&self, key: PageKey, output: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.with_page(key, |page| {
            let Some(page) = page else { return Ok(false) };
            // The kernel's already admitted output page is this validation's
            // bounded related window, then receives the actual payload.
            let records = semantic::derive(self, key, page.payload, output)?;
            if records != page.records {
                return Err(corrupt());
            }
            output.copy_from_slice(page.payload);
            (self.checkpoint)()?;
            Ok(true)
        })
    }
}
impl SemanticView for PageSource<'_> {
    fn checkpoint(&self) -> Result<()> {
        (self.checkpoint)()
    }
    fn generation_manifest(&self, generation: u64) -> Result<Manifest> {
        self.record
            .borrow()
            .family(generation)
            .and_then(|f| f.manifest)
            .ok_or_else(corrupt)?
            .manifest()
    }
    fn staged_build(&self) -> Result<Option<BuildState>> {
        self.record.borrow().build_state()
    }
    fn reservoir_samples(&self, generation: u64) -> Result<u64> {
        self.record
            .borrow()
            .family(generation)
            .and_then(|f| f.reservoir_samples)
            .ok_or_else(corrupt)
    }
    fn payload(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        // Dependency/validation reads authenticate native role/content but do
        // not recursively derive semantic counts from their own dependencies.
        self.with_page(key, |page| {
            let Some(page) = page else { return Ok(false) };
            out.copy_from_slice(page.payload);
            Ok(true)
        })
    }
}
