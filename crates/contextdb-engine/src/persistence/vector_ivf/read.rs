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
#[cfg(feature = "test-seams")]
use crate::vector_ivf_observations::*;
use contextdb_vector::ivf::storage::{
    BuildState, DistanceKind, Manifest, PageKey, PageKind, ReadPages, Verdict,
};
use contextdb_vector::{MemoryBudgetOwner, ivf::Work};
use std::cell::{Cell, RefCell};

/// Private candidate facts are bound to one exact immutable native root.
/// Ordinary serving-descriptor caches never receive this window.
pub(super) type BuildVerification =
    contextdb_core::read_memory::ReadShared<BuildVerificationWindow, Invoice>;
pub(super) struct BuildVerificationWindow {
    scope: [u8; 64],
    snapshot: redb::StorageSnapshotIdentity,
    manifest: Manifest,
    record: ScopeRecord,
    summary: super::super::vector_membership_pages::MembershipSummary,
    #[cfg(feature = "test-seams")]
    scopes_fingerprint: Option<[u8; 32]>,
    #[cfg(feature = "test-seams")]
    configuration_vector_binding: [u8; 32],
    verified: parking_lot::Mutex<([u16; control::ROOTS], [Option<Verdict>; control::ROOTS])>,
}
/// This borrowed capability keeps the existing source/holder and its original
/// native invoice alive. Its constructor authenticates shared authorities once.
pub(super) struct NativeAuthority<'a> {
    pub root: &'a super::super::vector_paged::CapturedPagedRoot,
    pub snapshot: contextdb_core::SnapshotId,
    pub snapshot_lsn: u64,
    owner: MemoryBudgetOwner,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    descriptor_cache: bool,
    pub(super) admission: super::super::native_storage_admission::StorageAdmissionScope,
    _invoice: Invoice,
    #[cfg(feature = "test-seams")]
    observer: Producer,
    #[cfg(feature = "test-seams")]
    source_instance: NativeIvfSourceInstance,
}
impl<'a> NativeAuthority<'a> {
    pub fn open(
        root: &'a super::super::vector_paged::CapturedPagedRoot,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Self> {
        Self::open_inner(root, memory, None, true)
    }
    pub(super) fn open_uncached(
        root: &'a super::super::vector_paged::CapturedPagedRoot,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Self> {
        Self::open_inner(root, memory, None, false)
    }
    pub(super) fn open_prepared(
        root: &'a super::super::vector_paged::CapturedPagedRoot,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
        prepared: &BuildVerification,
    ) -> Result<Self> {
        Self::open_inner(root, memory, Some(prepared), false)
    }
    fn open_inner(
        root: &'a super::super::vector_paged::CapturedPagedRoot,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
        prepared: Option<&BuildVerification>,
        descriptor_cache: bool,
    ) -> Result<Self> {
        #[cfg(feature = "test-seams")]
        let observer = Producer::current();
        #[cfg(feature = "test-seams")]
        observer.emit(NativeIvfSealEvent::ReadSetupEntered {
            phase: NativeIvfReadSetupPhase::Authority,
            snapshot: root.read.storage_snapshot_identity(),
        });
        use super::super::native_storage_admission::{
            StorageAdmissionScope, original_storage_error,
        };
        let owner = MemoryBudgetOwner::from(memory.clone()).preserving_read_copy_errors();
        let invoice = Invoice::reserve(
            &owner,
            std::mem::size_of::<Self>(),
            "clustered_source_control",
        )?;
        let admission = StorageAdmissionScope::reader_operation(memory.clone())
            .map_err(original_storage_error)?;
        let (snapshot, snapshot_lsn) = admission.run_original_unqualified(|| {
            let (_, snapshot, lsn) = root.authenticate()?;
            if let Some(prepared) = prepared {
                if prepared.snapshot != root.read.storage_snapshot_identity() {
                    return Err(corrupt());
                }
            } else {
                super::super::vector_membership_pages::validate_in_read(&root.read)?;
                let scopes = root
                    .read
                    .table_root_fingerprint(SCOPES)
                    .map_err(RedbPersistence::storage_error)?;
                if scopes.is_some() {
                    let expected =
                        super::super::content_identity::vector_pages_root_in_read(&root.read)?;
                    let actual = super::super::content_identity::vector_schema_pages_root_in_read(
                        &root.read,
                    )?;
                    if actual != expected {
                        return Err(corrupt());
                    }
                }
            }
            Ok((snapshot, lsn.0))
        })?;
        Ok(Self {
            #[cfg(feature = "test-seams")]
            observer,
            #[cfg(feature = "test-seams")]
            source_instance: NativeIvfSourceInstance::Read {
                admitted_object: crate::vector_ivf_observations::instance()?,
                snapshot: root.read.storage_snapshot_identity(),
            },
            root,
            snapshot,
            snapshot_lsn,
            owner,
            memory,
            descriptor_cache: descriptor_cache && root.read.installed_cache_capacity() != 0,
            admission,
            _invoice: invoice,
        })
    }
    pub fn vector_owner(&self) -> &MemoryBudgetOwner {
        &self.owner
    }
    pub(super) fn native(&self) -> NativeIo<'_> {
        NativeIo::CapturedTransaction {
            read: &self.root.read,
            admission: &self.admission,
        }
    }
    pub(super) fn source_summary(
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
    #[cfg(feature = "test-seams")]
    pub tuple: &'a VectorPartitionKey,
    #[cfg(feature = "test-seams")]
    pub route: Vec<u8>,
    pub record: RefCell<ScopeRecord>,
    pub source_summary: super::super::vector_membership_pages::MembershipSummary,
    pub scope_key: [u8; 64],
    retained_entry: Option<super::retained::EntryOwner>,
    query_manifest: Cell<Option<Manifest>>,
    build_verification: RefCell<Option<BuildVerification>>,
    cache: RefCell<Option<contextdb_core::read_memory::ReadShared<super::cache::DescriptorWindow>>>,
    #[cfg(feature = "test-seams")]
    attempts: Cell<u64>,
    verified: [Cell<u16>; control::ROOTS],
    verdicts: [Cell<Option<Verdict>>; control::ROOTS],
    page_work: Cell<Work>,
    checkpoint: &'a dyn Fn() -> Result<()>,
    distance: &'a dyn Fn(DistanceKind) -> Result<()>,
    _invoice: Invoice,
}
impl<'a> PageSource<'a> {
    #[cfg(feature = "test-seams")]
    fn observed_scope(&self) -> NativeIvfScopeRef<'_> {
        let schema = self.record.borrow().schema;
        NativeIvfScopeRef {
            index: self.index,
            tuple: self.tuple,
            table_generation: schema.table_generation,
            column_incarnation: schema.column_incarnation,
            snapshot_lsn: contextdb_core::Lsn(self.authority.snapshot_lsn),
            row_commit_lsn: None,
        }
    }
    #[cfg(feature = "test-seams")]
    fn observed_operation(&self, key: PageKey) -> NativeIvfOperationRef<'_> {
        NativeIvfOperationRef {
            scope: self.observed_scope(),
            operation: None,
            source_instance: self.authority.source_instance,
            generation: key.generation,
            storage: self
                .record
                .borrow()
                .family(key.generation)
                .map_or(0, |f| f.storage),
        }
    }
    #[cfg(feature = "test-seams")]
    pub(super) fn inspect_control<R>(
        &self,
        visit: impl FnOnce(NativeIvfCapturedRootRef, NativeIvfStoredScopeRef<'_>, &[u8]) -> Result<R>,
    ) -> Result<R> {
        let record = self.record.borrow();
        let root = NativeIvfCapturedRootRef {
            snapshot: self.authority.root.read.storage_snapshot_identity(),
            scopes_fingerprint: self.native().fingerprint(SCOPES.name())?,
            configuration_vector_binding: self.authority.admission.run_original_unqualified(
                || {
                    super::super::content_identity::vector_pages_root_in_read(
                        &self.authority.root.read,
                    )
                },
            )?,
        };
        self.native()
            .with_existing_value(SCOPES.name(), &self.scope_key, |value| {
                visit(
                    root,
                    record.observed(&self.scope_key)?,
                    value.ok_or_else(corrupt)?,
                )
            })
    }
    #[cfg(feature = "test-seams")]
    pub(super) fn inspect_descriptor(
        &self,
    ) -> Result<Option<crate::vector_ivf_observations::NativeIvfServingDescriptorForTest>> {
        self.native()
            .with_value(SCOPES.name(), &descriptor::key(&self.scope_key), |value| {
                value
                    .map(|value| {
                        Ok(
                            crate::vector_ivf_observations::NativeIvfServingDescriptorForTest {
                                manifest: descriptor::decode(
                                    &self.scope_key,
                                    &self.record.borrow(),
                                    value,
                                )?,
                                bytes: value.try_into().map_err(|_| corrupt())?,
                            },
                        )
                    })
                    .transpose()
            })
    }
    pub(super) fn load_serving_descriptor(&self) -> Result<Option<Manifest>> {
        if let Some(manifest) = self.query_manifest.get() {
            return Ok(Some(manifest));
        }
        if self.authority.descriptor_cache
            && let Some(window) = self.authority.root.native_ivf_cache().get(&self.scope_key)
        {
            // The holder is an authenticated immutable storage root. Match its
            // scope's current manifest before borrowing the cached descriptor.
            if descriptor::decode(&self.scope_key, &self.record.borrow(), &window.descriptor)?
                != window.manifest
                || Some(window.manifest) != self.record.borrow().manifest()?
            {
                return Err(corrupt());
            }
            self.query_manifest.set(Some(window.manifest));
            #[cfg(feature = "test-seams")]
            self.authority
                .observer
                .emit(NativeIvfSealEvent::QueryManifestSelected {
                    scope: self.observed_scope(),
                    descriptor: &window.descriptor,
                    manifest: window.manifest,
                });
            let manifest = window.manifest;
            *self.cache.borrow_mut() = Some(window);
            return Ok(Some(manifest));
        }
        let record = self.record.borrow();
        if record.serving.is_none() {
            return Ok(None);
        }
        let manifest = self.native().with_existing_value(
            SCOPES.name(),
            &descriptor::key(&self.scope_key),
            |value| {
                let bytes = value.ok_or_else(corrupt)?;
                let manifest =
                    descriptor::decode(&self.scope_key, &record, bytes).map_err(|_| {
                        Error::IndexCorrupted {
                            index: self.index.clone(),
                            reason:
                                "the saved serving descriptor differs from its authenticated scope"
                                    .into(),
                        }
                    })?;
                #[cfg(feature = "test-seams")]
                {
                    let stored = bytes.try_into().map_err(|_| corrupt())?;
                    self.authority
                        .observer
                        .emit(NativeIvfSealEvent::ServingDescriptorLoaded {
                            scope: self.observed_scope(),
                            stored,
                            manifest,
                        });
                    self.authority
                        .observer
                        .emit(NativeIvfSealEvent::QueryManifestSelected {
                            scope: self.observed_scope(),
                            descriptor: stored,
                            manifest,
                        });
                    crate::writer_query_test_seams::note_saved_graph_load(bytes.len());
                }
                if self.authority.descriptor_cache {
                    let stored = bytes.try_into().map_err(|_| corrupt())?;
                    let window = self.authority.root.native_ivf_cache().install(
                        self.scope_key,
                        stored,
                        manifest,
                        self.authority.memory.clone(),
                    )?;
                    *self.cache.borrow_mut() = Some(window);
                }
                Ok(manifest)
            },
        )?;
        self.query_manifest.set(Some(manifest));
        Ok(Some(manifest))
    }

    pub fn open(
        authority: &'a NativeAuthority<'a>,
        index: &'a VectorIndexRef,
        tuple: &'a VectorPartitionKey,
        checkpoint: &'a dyn Fn() -> Result<()>,
        distance: &'a dyn Fn(DistanceKind) -> Result<()>,
    ) -> Result<Option<Self>> {
        Self::open_inner(authority, index, tuple, checkpoint, distance, None)
    }
    pub(super) fn open_prepared(
        authority: &'a NativeAuthority<'a>,
        index: &'a VectorIndexRef,
        tuple: &'a VectorPartitionKey,
        checkpoint: &'a dyn Fn() -> Result<()>,
        distance: &'a dyn Fn(DistanceKind) -> Result<()>,
        prepared: &BuildVerification,
    ) -> Result<Self> {
        Self::open_inner(
            authority,
            index,
            tuple,
            checkpoint,
            distance,
            Some(prepared),
        )?
        .ok_or_else(corrupt)
    }
    fn open_inner(
        authority: &'a NativeAuthority<'a>,
        index: &'a VectorIndexRef,
        tuple: &'a VectorPartitionKey,
        checkpoint: &'a dyn Fn() -> Result<()>,
        distance: &'a dyn Fn(DistanceKind) -> Result<()>,
        prepared: Option<&BuildVerification>,
    ) -> Result<Option<Self>> {
        #[cfg(feature = "test-seams")]
        authority
            .observer
            .emit(NativeIvfSealEvent::ReadSetupEntered {
                phase: NativeIvfReadSetupPhase::TupleScope,
                snapshot: authority.root.read.storage_snapshot_identity(),
            });
        checkpoint()?;
        let construction =
            ScopeConstruction::reserve::<Self>(authority.vector_owner(), index, tuple)?;
        let route = construction.route(index, tuple)?;
        let (key, record, source_summary) = if let Some(prepared) = prepared {
            let key = control::scope_key(
                control::index_digest(index, prepared.record.schema),
                control::tuple_digest(tuple),
            );
            if prepared.snapshot != authority.root.read.storage_snapshot_identity()
                || prepared.scope != key
            {
                return Err(corrupt());
            }
            (key, prepared.record, prepared.summary.clone())
        } else {
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
            (key, record, source_summary)
        };
        if record.committed_through_lsn > authority.snapshot_lsn {
            return Err(corrupt());
        }
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
            if family.committed_through_lsn > authority.snapshot_lsn
                || family.source_snapshot > authority.snapshot.0
            {
                return Err(corrupt());
            }
        }
        let result = Self {
            authority,
            retained_entry: authority
                .root
                .native_ivf_cache()
                .retained_entry(super::retained::route_identity(index, tuple), key),
            query_manifest: Cell::new(None),
            build_verification: RefCell::new(None),
            cache: RefCell::new(None),
            #[cfg(feature = "test-seams")]
            attempts: Cell::new(0),
            index,
            #[cfg(feature = "test-seams")]
            tuple,
            #[cfg(feature = "test-seams")]
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
        };
        #[cfg(feature = "test-seams")]
        {
            let (scopes_fingerprint, configuration_vector_binding) =
                if let Some(prepared) = prepared {
                    (
                        prepared.scopes_fingerprint,
                        prepared.configuration_vector_binding,
                    )
                } else {
                    let scopes_fingerprint = authority.native().fingerprint(SCOPES.name())?;
                    let configuration_vector_binding =
                        authority.admission.run_original_unqualified(|| {
                            super::super::content_identity::vector_pages_root_in_read(
                                &authority.root.read,
                            )
                        })?;
                    (scopes_fingerprint, configuration_vector_binding)
                };
            let record = result.record.borrow();
            if prepared.is_none() {
                authority
                    .observer
                    .emit(NativeIvfSealEvent::RootControlLoaded {
                        scope: result.observed_scope(),
                        stored_bytes: PAGE_BYTES,
                    });
            }
            authority.observer.emit(NativeIvfSealEvent::ScopeOpened {
                scope: result.observed_scope(),
                source_instance: authority.source_instance,
                captured_root: NativeIvfCapturedRootRef {
                    snapshot: authority.root.read.storage_snapshot_identity(),
                    scopes_fingerprint,
                    configuration_vector_binding,
                },
                stored: record.observed(&result.scope_key)?,
            });
        }
        Ok(Some(result))
    }
    pub(super) fn native(&self) -> NativeIo<'_> {
        self.authority.native()
    }
    /// Bytes the generation's published kind tables hold: persisted entry
    /// count times the page size, from table metadata with no page walk.
    pub(super) fn durable_index_bytes(&self, generation: u64) -> Result<u64> {
        let record = self.record.borrow();
        let family = record.family(generation).ok_or_else(corrupt)?;
        let mut entries = 0u64;
        for tag in 1..=16u8 {
            if family.kinds.present & (1u16 << (tag - 1)) == 0 {
                continue;
            }
            let name = KindTableName::new(family.storage, header::kind_from_tag(tag)?)?;
            entries = entries
                .checked_add(self.native().table_entries(name.as_str()?)?)
                .ok_or_else(corrupt)?;
        }
        entries
            .checked_mul(PAGE_BYTES as u64)
            .ok_or_else(corrupt)
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
        if let Some(window) = self.cache.borrow().as_ref() {
            self.verified[slot].set(self.verified[slot].get() | window.verified.lock().0[slot]);
        }
        if let Some(window) = self.build_verification.borrow().as_ref() {
            self.verified[slot].set(self.verified[slot].get() | window.verified.lock().0[slot]);
        }
        if self.verified[slot].get() & bit != 0 {
            #[cfg(feature = "test-seams")]
            self.authority
                .observer
                .emit(NativeIvfSealEvent::VerificationMaskUsed {
                    operation: self.observed_operation(key),
                    kind: key.kind,
                    mask: self.verified[slot].get(),
                });
            return Ok(present);
        }
        let name = KindTableName::new(family.storage, key.kind)?;
        let actual = self.native().fingerprint(name.as_str()?)?;
        let expected = present.then_some(family.kinds.roots[usize::from(tag - 1)]);
        #[cfg(feature = "test-seams")]
        self.authority
            .observer
            .emit(NativeIvfSealEvent::KindFingerprintCompared {
                operation: self.observed_operation(key),
                kind: key.kind,
                expected,
                actual,
            });
        if actual != expected {
            return Err(corrupt());
        }
        self.verified[slot].set(self.verified[slot].get() | bit);
        if let Some(window) = self.cache.borrow().as_ref() {
            window.verified.lock().0[slot] |= bit;
        }
        if let Some(window) = self.build_verification.borrow().as_ref() {
            window.verified.lock().0[slot] |= bit;
        }
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
        #[cfg(feature = "test-seams")]
        let attempt = {
            let next = self.attempts.get().checked_add(1).ok_or_else(corrupt)?;
            self.attempts.set(next);
            next
        };
        #[cfg(feature = "test-seams")]
        self.authority
            .observer
            .emit(NativeIvfSealEvent::PageReadAttempt {
                operation: self.observed_operation(key),
                source_instance: self.authority.source_instance,
                attempt,
                key,
            });
        #[cfg(feature = "test-seams")]
        let present = Cell::new(false);
        let result = (|| {
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
                            #[cfg(feature = "test-seams")]
                            self.authority
                                .observer
                                .emit(NativeIvfSealEvent::PageOpened {
                                    operation: self.observed_operation(key),
                                    key,
                                    header: value[..192].try_into().map_err(|_| corrupt())?,
                                    payload: page.payload,
                                });
                            #[cfg(feature = "test-seams")]
                            present.set(true);
                            Ok(page)
                        })
                        .transpose()?;
                    visit(value)
                })
        })();
        #[cfg(feature = "test-seams")]
        self.authority
            .observer
            .emit(NativeIvfSealEvent::PageReadOutcome {
                operation: self.observed_operation(key),
                source_instance: self.authority.source_instance,
                attempt,
                key,
                outcome: match &result {
                    Err(e) => NativeIvfPageReadOutcome::Failed(e),
                    Ok(_) => {
                        if present.get() {
                            NativeIvfPageReadOutcome::Authenticated
                        } else {
                            NativeIvfPageReadOutcome::Absent
                        }
                    }
                },
            });
        result
    }
    /// Actual payload attempts include internal semantic-validation metadata
    /// reads. Distance counters remain the kernel's actual completed scores.
    pub fn actual_page_work(&self) -> Work {
        self.page_work.get()
    }
    /// Exhaustive canonical search takes its manifest from the authenticated
    /// source-coverage control, independent of the derived serving descriptor.
    pub fn select_canonical_authority(&self) -> Result<()> {
        self.query_manifest.set(self.record.borrow().manifest()?);
        Ok(())
    }
    /// Maintenance's view is the actual completed, unpublished producer build.
    /// No caller-supplied manifest can become this authority.
    pub(super) fn select_build_authority(&self) -> Result<Manifest> {
        let record = self.record.borrow();
        let build = record
            .build_state()?
            .filter(|build| build.phase == contextdb_vector::ivf::storage::BuildPhase::Publish)
            .ok_or_else(corrupt)?;
        let family = record
            .family(build.manifest.generation)
            .ok_or_else(corrupt)?;
        if family
            .manifest
            .map(control::ManifestRecord::manifest)
            .transpose()?
            != Some(build.manifest)
        {
            return Err(corrupt());
        }
        self.query_manifest.set(Some(build.manifest));
        Ok(build.manifest)
    }
    pub(super) fn new_build_verification(&self) -> Result<BuildVerification> {
        let manifest = self.select_build_authority()?;
        let invoice = Invoice::reserve(
            self.authority.vector_owner(),
            BuildVerification::layout().size(),
            "clustered_candidate_verification_window",
        )?;
        let window = BuildVerification::try_from_invoice_native(
            BuildVerificationWindow {
                scope: self.scope_key,
                snapshot: self.authority.root.read.storage_snapshot_identity(),
                manifest,
                record: *self.record.borrow(),
                summary: self.source_summary.clone(),
                #[cfg(feature = "test-seams")]
                scopes_fingerprint: self.authority.native().fingerprint(SCOPES.name())?,
                #[cfg(feature = "test-seams")]
                configuration_vector_binding: self.authority.admission.run_original_unqualified(
                    || {
                        super::super::content_identity::vector_pages_root_in_read(
                            &self.authority.root.read,
                        )
                    },
                )?,
                verified: parking_lot::Mutex::new(([0; control::ROOTS], [None; control::ROOTS])),
            },
            invoice,
        )
        .map_err(Error::NativeEngineOther)?;
        *self.build_verification.borrow_mut() = Some(window.clone());
        Ok(window)
    }
    pub(super) fn select_build_authority_with_verification(
        &self,
        window: &BuildVerification,
    ) -> Result<Manifest> {
        let manifest = self.select_build_authority()?;
        if window.scope != self.scope_key
            || window.manifest != manifest
            || window.snapshot != self.authority.root.read.storage_snapshot_identity()
        {
            return Err(corrupt());
        }
        *self.build_verification.borrow_mut() = Some(window.clone());
        Ok(manifest)
    }
    pub fn canonical_ready(&self) -> bool {
        let record = self.record.borrow();
        record.canonical_complete
            && (record.serving.is_some() || self.source_summary.live_entries == 0)
    }
}
impl ReadPages for PageSource<'_> {
    fn retained_codebook(
        &self,
        manifest: Manifest,
    ) -> Option<&contextdb_vector::ivf::RetainedCodebook> {
        self.retained_entry
            .as_ref()
            .filter(|entry| entry.book.matches(manifest))
            .map(|entry| &entry.book)
    }
    fn inspection_verdict(&self, partition: u64, generation: u64) -> Option<Verdict> {
        if partition != 0 {
            return None;
        }
        self.family_slot(generation).and_then(|slot| {
            self.verdicts[slot]
                .get()
                .or_else(|| {
                    self.build_verification
                        .borrow()
                        .as_ref()
                        .and_then(|window| window.verified.lock().1[slot])
                })
                .or_else(|| {
                    self.cache
                        .borrow()
                        .as_ref()
                        .and_then(|window| window.verified.lock().1[slot])
                })
        })
    }
    fn record_inspection_verdict(&self, partition: u64, generation: u64, verdict: Verdict) {
        if partition == 0
            && let Some(slot) = self.family_slot(generation)
        {
            if self.verdicts[slot].get().is_none() {
                #[cfg(feature = "test-seams")]
                if self
                    .cache
                    .borrow()
                    .as_ref()
                    .is_none_or(|window| window.verified.lock().1[slot].is_none())
                    && self
                        .build_verification
                        .borrow()
                        .as_ref()
                        .is_none_or(|window| window.verified.lock().1[slot].is_none())
                    && self
                        .record
                        .borrow()
                        .family(generation)
                        .and_then(|family| family.manifest)
                        .and_then(|manifest| manifest.manifest().ok())
                        .is_some_and(|manifest| manifest.active_lists != 0)
                {
                    crate::vector_ivf_observations::observe_verification(
                        crate::vector_ivf_observations::NativeIvfVerificationForTest {
                            window: self
                                .authority
                                .root
                                .native_ivf_cache()
                                .observation_identity_for_test(),
                            index: self.index,
                            generation,
                            verdict,
                        },
                    );
                }
                self.verdicts[slot].set(Some(verdict));
            }
            if let Some(window) = self.cache.borrow().as_ref() {
                window.verified.lock().1[slot].get_or_insert(verdict);
            }
            if let Some(window) = self.build_verification.borrow().as_ref() {
                window.verified.lock().1[slot].get_or_insert(verdict);
            }
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
        #[cfg(feature = "test-seams")]
        if matches!(
            kind,
            DistanceKind::Quantized | DistanceKind::Canonical | DistanceKind::Buffer
        ) {
            work_point(NativeIvfWorkPoint::CandidateScoring);
            (self.checkpoint)()?;
        }
        (self.distance)(kind)
    }
    fn manifest(&self, partition: u64) -> Result<Option<Manifest>> {
        if partition != 0 {
            return Ok(None);
        }
        self.load_serving_descriptor()
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
            #[cfg(feature = "test-seams")]
            if key.kind == PageKind::Lists {
                work_point(NativeIvfWorkPoint::ListPageRead);
                (self.checkpoint)()?;
            }
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

#[cfg(feature = "test-seams")]
impl PageSource<'_> {
    pub(super) fn inspect_scope_roots(
        &self,
    ) -> Result<crate::vector_ivf_observations::NativeIvfScopeRootsForTest> {
        use crate::vector_ivf_observations::NativeIvfScopeRootsForTest;
        let record = self.record.borrow();
        let manifest = record.manifest()?.ok_or_else(corrupt)?;
        let family = record.family(manifest.generation).ok_or_else(corrupt)?;
        let mut actual_roots = [[0; 32]; 16];
        for tag in 1..=16 {
            let name = KindTableName::new(family.storage, header::kind_from_tag(tag)?)?;
            let actual = self.native().fingerprint(name.as_str()?)?;
            actual_roots[usize::from(tag - 1)] = actual.unwrap_or([0; 32]);
        }
        Ok(NativeIvfScopeRootsForTest {
            index_digest: record.index,
            tuple_digest: record.tuple,
            snapshot_lsn: self.authority.snapshot_lsn,
            table_generation: record.schema.table_generation,
            column_incarnation: record.schema.column_incarnation,
            frozen_frontier: family.source_frontier,
            frozen_source_identity: family.source_identity,
            current_frontier: record.committed_through_lsn,
            current_digest: record.current_membership_digest,
            current_entries: record.current_membership_entries,
            current_live_entries: record.current_membership_live_entries,
            expected_roots: family.kinds.roots,
            actual_roots,
            present: family.kinds.present,
        })
    }
    pub(super) fn visit_authoritative_source(
        &self,
        visit: &mut dyn FnMut(crate::vector_ivf_observations::NativeIvfSourceRecordForTest<'_>),
    ) -> Result<()> {
        self.authority.admission.run_original_unqualified(|| {
            let source = self
                .authority
                .root
                .read
                .open_byte_table_bounded(super::super::vector_membership_pages::SOURCE)
                .map_err(RedbPersistence::storage_error)?;
            let mut cursor = source
                .prefix_bounded(&self.route)
                .map_err(RedbPersistence::storage_error)?;
            while let Some(next) = cursor.next() {
                self.check_cancel()?;
                let (key, value) = next.map_err(RedbPersistence::storage_error)?;
                let record = super::super::vector_membership_pages::decode_source_member(
                    key.value(),
                    value.value(),
                )?;
                if record.index != *self.index || record.partition_key != *self.tuple {
                    return Err(corrupt());
                }
                visit(
                    crate::vector_ivf_observations::NativeIvfSourceRecordForTest {
                        key: key.value(),
                        encoded: value.value(),
                        live: record.deleted_tx.is_none(),
                    },
                );
            }
            Ok(())
        })
    }
}

impl PageSource<'_> {
    pub(super) fn activate_retained(
        &mut self,
        tuple: &VectorPartitionKey,
        selection: super::retained::Selection<'_>,
    ) -> Result<()> {
        // The initial generation cannot be in replacement state B. This
        // existing durable build metadata avoids a derived catalog read there.
        let possible = {
            let record = self.record.borrow();
            let serving = record.manifest()?;
            record.build_state()?.is_some_and(|build| {
                build.phase == contextdb_vector::ivf::storage::BuildPhase::Ready
                    && build.serving_at_start.is_some()
                    && serving
                        .is_some_and(|serving| serving.generation == build.manifest.generation)
            })
        };
        if !possible {
            return Ok(());
        }
        let Some(manifest) = contextdb_vector::ivf::retained_codebook_manifest(self, 0)? else {
            return Ok(());
        };
        if self
            .retained_entry
            .as_ref()
            .is_some_and(|entry| entry.book.matches(manifest))
        {
            return Ok(());
        }
        let route = super::retained::route_identity(self.index, tuple);
        (selection.boundary)(&mut || {
            let entry = selection.cache.ensure_retained(
                &(selection.owner)()?,
                self,
                route,
                self.scope_key,
                manifest,
            )?;
            self.authority
                .root
                .native_ivf_cache()
                .adopt_retained(selection.cache);
            self.retained_entry = Some(entry);
            Ok(())
        })?;
        Ok(())
    }
}

#[cfg(feature = "test-seams")]
impl PageSource<'_> {
    pub(super) fn retained_for_test(
        &self,
    ) -> crate::vector_ivf_observations::NativeIvfRetainedCodebookForTest {
        use crate::vector_ivf_observations::NativeIvfRetainedCodebookForTest;
        if let Some(entry) = &self.retained_entry {
            NativeIvfRetainedCodebookForTest {
                manifest: Some(entry.book.manifest()),
                charged_bytes: entry.book.charged_bytes(),
                original_owner: entry.book.original_owner_for_test(),
            }
        } else {
            NativeIvfRetainedCodebookForTest {
                manifest: None,
                charged_bytes: 0,
                original_owner: 0,
            }
        }
    }
}
