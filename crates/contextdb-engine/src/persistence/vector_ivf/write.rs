//! Borrowed native writer. Kernel commits seal inside one outer transaction;
//! only the outer owner can commit rows and the final Configuration binding.
use super::*;
mod erasure;
mod qualification;
use super::{
    control::{Family, KindRoots, ScopeRecord},
    header::HeaderIdentity,
    io::{KindTableName, NativeIo},
    memory::{Invoice, ScopeConstruction, SealWorkspace},
    seal::NativeSealView,
    semantic::{SealView, SemanticView},
};
#[cfg(feature = "test-seams")]
use crate::vector_ivf_observations::*;
use contextdb_core::Lsn;
use contextdb_vector::MemoryBudgetOwner;
use contextdb_vector::ivf::storage::{
    BuildState, DistanceKind, Manifest, PageKey, PageKind, PageStore, ReadPages, Verdict,
    WritePages,
};
use std::cell::{Cell, Ref, RefCell};

/// Missing summaries are legitimate only for an actually empty route. A
/// bounded first-key seek proves that case; existing source rows never become
/// an invented empty baseline for native coverage.
pub(super) fn source_summary(
    write: &redb::WriteTransaction,
    route: &[u8],
) -> Result<super::super::vector_membership_pages::MembershipSummary> {
    use super::super::vector_membership_pages::{
        MembershipSummary, SOURCE, SUMMARY, decode_summary,
    };
    use redb::ReadableTable;
    let native = NativeIo::Borrowed(write);
    if let Some(summary) = native.with_value(SUMMARY.name(), route, |value| {
        value.map(|v| decode_summary(route, v)).transpose()
    })? {
        return Ok(summary);
    }
    if write
        .table_exists(SOURCE)
        .map_err(RedbPersistence::storage_error)?
    {
        let source = write
            .open_table(SOURCE)
            .map_err(RedbPersistence::storage_error)?;
        let mut cursor = source
            .range(route..)
            .map_err(RedbPersistence::storage_error)?;
        if let Some(next) = cursor.next() {
            let (key, _) = next.map_err(RedbPersistence::storage_error)?;
            if key.value().starts_with(route) {
                return Err(corrupt());
            }
        }
    }
    Ok(MembershipSummary::default())
}

/// One borrowed outer write. A mutable borrow admits at most one scope window.
/// This holder never commits or opens a second native write transaction.
pub(crate) struct WriteSession<'a> {
    write: &'a redb::WriteTransaction,
    admission: &'a super::super::native_storage_admission::StorageAdmissionScope,
    owner: MemoryBudgetOwner,
    snapshot: contextdb_core::SnapshotId,
    snapshot_lsn: Lsn,
    row_commit_lsn: Lsn,
    #[cfg(feature = "test-seams")]
    pub(super) disk_owner: Option<NativeIvfPhysicalDiskOwnerForTest>,
    next_operation: Cell<u64>,
    failed: Cell<bool>,
    bound: Cell<bool>,
    _invoice: Invoice,
    #[cfg(feature = "test-seams")]
    observer: Producer,
    #[cfg(feature = "test-seams")]
    source_instance: NativeIvfSourceInstance,
}
impl<'a> WriteSession<'a> {
    pub fn borrow(
        write: &'a redb::WriteTransaction,
        admission: &'a super::super::native_storage_admission::StorageAdmissionScope,
        owner: MemoryBudgetOwner,
        snapshot: contextdb_core::SnapshotId,
        snapshot_lsn: Lsn,
        row_commit_lsn: Lsn,
    ) -> Result<Self> {
        if row_commit_lsn < snapshot_lsn {
            return Err(corrupt());
        }
        #[cfg(feature = "test-seams")]
        let (observer, source_instance) = (
            Producer::current(),
            NativeIvfSourceInstance::Write {
                admitted_session: crate::vector_ivf_observations::instance()?,
                outer_write_object: write as *const _ as usize,
            },
        );
        let invoice =
            Invoice::reserve(&owner, std::mem::size_of::<Self>(), "clustered_outer_write")?;
        admission.run_original_unqualified(|| {
            super::super::content_identity::validate_clustered_scope_root_in_write(write)?;
            super::super::vector_membership_pages::validate_in_write(write)?;
            let result = seal::require_global_empty(write);
            #[cfg(feature = "test-seams")]
            observer.emit(NativeIvfSealEvent::GlobalPendingChecked {
                source_instance,
                empty: result.is_ok(),
                stage: NativeIvfGlobalPendingStage::SessionBegin,
            });
            result
        })?;
        Ok(Self {
            write,
            admission,
            owner,
            snapshot,
            snapshot_lsn,
            row_commit_lsn,
            #[cfg(feature = "test-seams")]
            disk_owner: None,
            next_operation: Cell::new(1),
            failed: Cell::new(false),
            bound: Cell::new(false),
            _invoice: invoice,
            #[cfg(feature = "test-seams")]
            observer,
            #[cfg(feature = "test-seams")]
            source_instance,
        })
    }
    fn usable(&self) -> Result<()> {
        if self.failed.get() || self.bound.get() {
            return Err(corrupt());
        }
        Ok(())
    }
    fn operation(&self) -> Result<u64> {
        self.usable()?;
        let operation = self.next_operation.get();
        self.next_operation
            .set(operation.checked_add(1).ok_or_else(corrupt)?);
        Ok(operation)
    }
    /// The original native writer admission remains installed across every
    /// kernel call, including pending cursors, family allocation and sealing.
    /// The mutable session borrow excludes another simultaneously live scope.
    pub fn with_scope<R>(
        &mut self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        checkpoint: &dyn Fn() -> Result<()>,
        distance: &dyn Fn(DistanceKind) -> Result<()>,
        run: impl FnOnce(&mut WriteScope<'_, 'a>) -> Result<R>,
    ) -> Result<R> {
        self.usable()?;
        let admission = self.admission;
        let result = admission.run_original_unqualified(|| {
            let mut scope = WriteScope::open(self, index, tuple, checkpoint, distance)?;
            let result = run(&mut scope);
            scope.completed = result.is_ok();
            result
        });
        if result.is_err() {
            self.failed.set(true);
        }
        result
    }
    /// Called after every row/scope/schema write, immediately before the real
    /// outer commit. The mutable borrow excludes any still-live scope holder.
    pub fn bind_final(&mut self) -> Result<()> {
        self.usable()?;
        let result = self.admission.run_original_unqualified(|| {
            let result = seal::require_global_empty(self.write);
            #[cfg(feature = "test-seams")]
            self.observer
                .emit(NativeIvfSealEvent::GlobalPendingChecked {
                    source_instance: self.source_instance,
                    empty: result.is_ok(),
                    stage: NativeIvfGlobalPendingStage::BeforeFinalBinding,
                });
            result?;
            #[cfg(feature = "test-seams")]
            self.observer
                .boundary(NativeIvfFaultBoundary::BeforeConfigurationBind)?;
            super::super::content_identity::bind_vector_pages_root_in_write(self.write)?;
            #[cfg(feature = "test-seams")]
            {
                let root =
                    super::super::content_identity::vector_schema_pages_root_in_write(self.write)?;
                self.observer
                    .emit(NativeIvfSealEvent::ConfigurationBound { root });
            }
            Ok(())
        });
        if result.is_err() {
            self.failed.set(true);
        } else {
            self.bound.set(true);
        }
        result
    }
}

/// The exact charged tuple window. Fixed root/verification controls remain
/// inline; there is no process-wide map of contexts, generations or pages.
pub struct WriteScope<'a, 'tx> {
    session: &'a mut WriteSession<'tx>,
    index: &'a VectorIndexRef,
    tuple: &'a VectorPartitionKey,
    route: Vec<u8>,
    pub(super) key: [u8; 64],
    pub(super) record: RefCell<ScopeRecord>,
    identity_lsn: Cell<Option<u64>>,
    writer_loan: Cell<bool>,
    operation_open: Cell<bool>,
    verified: [Cell<u16>; control::ROOTS],
    dirty: [Cell<u16>; control::ROOTS],
    verdicts: [Cell<Option<Verdict>>; control::ROOTS],
    checkpoint: &'a dyn Fn() -> Result<()>,
    distance: &'a dyn Fn(DistanceKind) -> Result<()>,
    completed: bool,
    #[cfg(feature = "test-seams")]
    active_operation: Cell<Option<u64>>,
    #[cfg(feature = "test-seams")]
    attempts: Cell<u64>,
    _invoice: Invoice,
}
impl Drop for WriteScope<'_, '_> {
    fn drop(&mut self) {
        if !self.completed {
            self.session.failed.set(true);
        }
    }
}
impl<'a, 'tx> WriteScope<'a, 'tx> {
    pub(crate) fn native_entry_backing_quote(&self) -> Result<usize> {
        self.session
            .write
            .native_dirty_byte_entry_layout(header::KEY_BYTES, header::VALUE_BYTES)
            .map(|layout| layout.size())
            .ok_or_else(corrupt)
    }

    #[cfg(feature = "test-seams")]
    pub(super) fn constructor_context_for_test(
        &self,
    ) -> (
        &'tx redb::WriteTransaction,
        contextdb_core::SnapshotId,
        MemoryBudgetOwner,
    ) {
        (
            self.session.write,
            self.session.snapshot,
            self.session.owner.clone(),
        )
    }

    #[cfg(feature = "test-seams")]
    fn observed_scope(&self) -> NativeIvfScopeRef<'_> {
        let schema = self.record.borrow().schema;
        NativeIvfScopeRef {
            index: self.index,
            tuple: self.tuple,
            table_generation: schema.table_generation,
            column_incarnation: schema.column_incarnation,
            snapshot_lsn: self.session.snapshot_lsn,
            row_commit_lsn: Some(self.session.row_commit_lsn),
        }
    }
    #[cfg(feature = "test-seams")]
    fn observed_operation(
        &self,
        operation: Option<u64>,
        key: PageKey,
    ) -> Result<NativeIvfOperationRef<'_>> {
        let record = self.record.borrow();
        Ok(NativeIvfOperationRef {
            scope: self.observed_scope(),
            operation,
            source_instance: self.session.source_instance,
            storage: record
                .family(key.generation)
                .map_or(0, |family| family.storage),
            generation: key.generation,
        })
    }

    /// Borrow this admitted native store into the production kernel with its
    /// retained original payer. Callers cannot replace that funding owner.
    pub fn with_clustered_writer<R>(
        &mut self,
        policy: contextdb_vector::ivf::Policy,
        training: contextdb_vector::ivf::TrainingPolicy,
        run: impl FnOnce(&mut contextdb_vector::ivf::ClusteredIndex<&mut Self>) -> Result<R>,
    ) -> Result<R> {
        self.check_cancel()?;
        let owner = self.session.owner.clone();
        let result = {
            let mut writer =
                contextdb_vector::ivf::ClusteredIndex::new(&mut *self, policy, training, owner)?;
            let result = run(&mut writer);
            #[cfg(feature = "test-seams")]
            crate::vector_ivf_observations::observe_buffer(
                writer
                    .storage()
                    .session
                    .owner
                    .as_ref()
                    .stable_funder_identity(),
                writer.storage().index,
                writer.buffer(),
            );
            result
        };
        if result.is_err() {
            self.session.failed.set(true);
        }
        result
    }
    pub(super) fn with_row_writer<R>(
        &self,
        policy: contextdb_vector::ivf::Policy,
        training: contextdb_vector::ivf::TrainingPolicy,
        run: impl FnOnce(&mut contextdb_vector::ivf::ClusteredIndex<&Self>) -> Result<R>,
    ) -> Result<R> {
        self.check_cancel()?;
        if self.writer_loan.replace(true) {
            return Err(Error::InvalidStateTransition(
                "native partition writer is already borrowed".into(),
            ));
        }
        struct ReturnLoan<'a>(&'a Cell<bool>);
        impl Drop for ReturnLoan<'_> {
            fn drop(&mut self) {
                self.0.set(false);
            }
        }
        let _loan = ReturnLoan(&self.writer_loan);
        let owner = self.session.owner.clone();
        let result = {
            let mut writer =
                contextdb_vector::ivf::ClusteredIndex::new(self, policy, training, owner)?;
            let result = run(&mut writer);
            #[cfg(feature = "test-seams")]
            crate::vector_ivf_observations::observe_buffer(
                writer
                    .storage()
                    .session
                    .owner
                    .as_ref()
                    .stable_funder_identity(),
                writer.storage().index,
                writer.buffer(),
            );
            result
        };
        if result.is_err() {
            self.session.failed.set(true);
        }
        result
    }
    fn open(
        session: &'a mut WriteSession<'tx>,
        index: &'a VectorIndexRef,
        tuple: &'a VectorPartitionKey,
        checkpoint: &'a dyn Fn() -> Result<()>,
        distance: &'a dyn Fn(DistanceKind) -> Result<()>,
    ) -> Result<Self> {
        session.usable()?;
        checkpoint()?;
        let construction = ScopeConstruction::reserve::<Self>(&session.owner, index, tuple)?;
        let route = construction.route(index, tuple)?;
        let schema = RedbPersistence::vector_schema_binding_in_write(session.write, index)?;
        let digest = control::tuple_digest(tuple);
        let key = control::scope_key(control::index_digest(index, schema), digest);
        let record =
            NativeIo::Borrowed(session.write).with_value(SCOPES.name(), &key, |value| {
                value
                    .map(|v| ScopeRecord::decode(index, &key, v, schema, digest))
                    .transpose()
            })?;
        let record = match record {
            Some(record) => record,
            None => ScopeRecord::empty(index, schema, digest)?,
        };
        if record.committed_through_lsn > session.row_commit_lsn.0 {
            return Err(corrupt());
        }
        Ok(Self {
            session,
            index,
            tuple,
            route,
            key,
            record: RefCell::new(record),
            identity_lsn: Cell::new(None),
            writer_loan: Cell::new(false),
            operation_open: Cell::new(false),
            verified: std::array::from_fn(|_| Cell::new(0)),
            dirty: std::array::from_fn(|_| Cell::new(0)),
            verdicts: std::array::from_fn(|_| Cell::new(None)),
            checkpoint,
            distance,
            completed: false,
            #[cfg(feature = "test-seams")]
            active_operation: Cell::new(None),
            #[cfg(feature = "test-seams")]
            attempts: Cell::new(0),
            _invoice: construction.invoice,
        })
    }
    fn slot(&self, generation: u64) -> Option<usize> {
        self.record
            .borrow()
            .families
            .iter()
            .position(|f| f.as_ref().is_some_and(|f| f.generation == generation))
    }
    fn verify_kind(&self, key: PageKey) -> Result<bool> {
        let Some(slot) = self.slot(key.generation) else {
            return Ok(false);
        };
        let record = self.record.borrow();
        let family = record.families[slot].as_ref().ok_or_else(corrupt)?;
        let tag = header::kind_tag(key.kind);
        let bit = 1u16 << (tag - 1);
        let name = KindTableName::new(family.storage, key.kind)?;
        if self.dirty[slot].get() & bit != 0 {
            // An exclusive operation already verified the old captured kind
            // before its first mutation. Its staged root is not published yet.
            return Ok(true);
        }
        let present = family.kinds.present & bit != 0;
        if self.verified[slot].get() & bit == 0 {
            let actual = NativeIo::Borrowed(self.session.write).fingerprint(name.as_str()?)?;
            let expected = present.then_some(family.kinds.roots[usize::from(tag - 1)]);
            #[cfg(feature = "test-seams")]
            self.session
                .observer
                .emit(NativeIvfSealEvent::KindFingerprintCompared {
                    operation: self.observed_operation(self.active_operation.get(), key)?,
                    kind: key.kind,
                    expected,
                    actual,
                });
            if actual != expected {
                return Err(corrupt());
            }
            self.verified[slot].set(self.verified[slot].get() | bit);
        }
        #[cfg(feature = "test-seams")]
        self.session
            .observer
            .emit(NativeIvfSealEvent::VerificationMaskUsed {
                operation: self.observed_operation(self.active_operation.get(), key)?,
                kind: key.kind,
                mask: self.verified[slot].get(),
            });
        Ok(present)
    }
    fn changed(&self, key: PageKey) -> Result<()> {
        let slot = self.slot(key.generation).ok_or_else(corrupt)?;
        let bit = 1u16 << (header::kind_tag(key.kind) - 1);
        self.dirty[slot].set(self.dirty[slot].get() | bit);
        #[cfg(feature = "test-seams")]
        let previous = self.verified[slot].get();
        self.verified[slot].set(0);
        #[cfg(feature = "test-seams")]
        self.session
            .observer
            .emit(NativeIvfSealEvent::VerificationMaskReset {
                operation: self.observed_operation(self.active_operation.get(), key)?,
                previous,
                current: 0,
            });
        self.verdicts[slot].set(None);
        Ok(())
    }
    /// Pending-directory backing is a real original-owned object. Construct
    /// the named root slots before choosing a DEFAULT workspace; later root
    /// finalization reuses that backing and still admits its own native credit.
    pub(super) fn prepare_kind_tables(&self, generation: u64, kinds: &[PageKind]) -> Result<()> {
        let operation = if self.slot(generation).is_none() {
            let operation = self.begin_operation()?;
            self.ensure_family(generation, &mut operation.workspace.borrow_mut())?;
            Some(operation)
        } else {
            None
        };
        for &kind in kinds {
            let key = PageKey {
                partition: 0,
                generation,
                kind,
                list: 0,
                ordinal: 0,
                run: 0,
            };
            self.verify_kind(key)?;
            let storage = self
                .record
                .borrow()
                .family(generation)
                .ok_or_else(corrupt)?
                .storage;
            let name = KindTableName::new(storage, kind)?;
            drop(
                self.session
                    .write
                    .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str()?))
                    .map_err(RedbPersistence::storage_error)?,
            );
        }
        // An empty table has no payload fingerprint. Actual kernel writes
        // publish its first authenticated root; no empty root is invented.
        if let Some(operation) = operation {
            operation.commit()?;
        }
        Ok(())
    }
    pub(super) fn prepare_existing_roots(&self) -> Result<()> {
        for definition in [
            SCOPES,
            io::PENDING,
            super::super::vector_membership_pages::SOURCE,
            super::super::vector_membership_pages::SUMMARY,
            super::super::VECTOR_PARTITION_MEMBERSHIP_TABLE,
            super::super::VECTORS_TABLE,
        ] {
            drop(
                self.session
                    .write
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?,
            );
        }
        for slot in 0..control::ROOTS {
            let family = self.record.borrow().families[slot];
            if let Some(family) = family {
                for tag in 1..=16 {
                    if family.kinds.present & (1 << (tag - 1)) != 0 {
                        let name = KindTableName::new(family.storage, header::kind_from_tag(tag)?)?;
                        drop(
                            self.session
                                .write
                                .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(
                                    name.as_str()?,
                                ))
                                .map_err(RedbPersistence::storage_error)?,
                        );
                    }
                }
            }
        }
        Ok(())
    }
    pub(super) fn prepare_row_roots(&self) -> Result<()> {
        self.prepare_existing_roots()?;
        self.prepare_kind_tables(0, &[PageKind::RowVersions, PageKind::RowCopies])?;
        let generation = self.manifest(0)?.map_or(0, |manifest| manifest.generation);
        self.prepare_kind_tables(
            generation,
            &[
                PageKind::Canonical,
                PageKind::Visibility,
                PageKind::Tail,
                PageKind::Catalog,
                PageKind::SlotMetadata,
            ],
        )
    }
    pub(super) fn prepare_tail_roots(&self, derived_usable: bool) -> Result<()> {
        // A recorded derived-page verdict makes the kernel seal only canonical
        // tail/run metadata. Preopening an unused broken directory would stop
        // the rebuild before that independent recovery path can start.
        if !derived_usable {
            return Ok(());
        }
        if let Some(manifest) = self.manifest(0)? {
            if manifest.slots > manifest.sealed_slots {
                self.prepare_kind_tables(
                    manifest.generation,
                    &[PageKind::Directory, PageKind::DirectoryPresence],
                )?;
                if manifest.active_lists != 0 {
                    self.prepare_kind_tables(
                        manifest.generation,
                        &[
                            PageKind::Lists,
                            PageKind::ListUsed,
                            PageKind::ListLinks,
                            PageKind::ListAllocator,
                        ],
                    )?;
                }
            }
        }
        Ok(())
    }
    pub(super) fn prepare_build_roots(
        &self,
        generation: u64,
        phase: contextdb_vector::ivf::storage::BuildPhase,
    ) -> Result<()> {
        use contextdb_vector::ivf::storage::BuildPhase;
        match phase {
            BuildPhase::Ingest => {
                self.prepare_kind_tables(0, &[PageKind::RowVersions, PageKind::RowCopies])?;
                self.prepare_kind_tables(
                    generation,
                    &[
                        PageKind::Canonical,
                        PageKind::Visibility,
                        PageKind::SlotMetadata,
                        PageKind::Reservoir,
                    ],
                )
            }
            BuildPhase::Seed | BuildPhase::Train => self.prepare_kind_tables(
                generation,
                &[
                    PageKind::Codebook,
                    PageKind::Accumulators,
                    PageKind::Directory,
                    PageKind::DirectoryPresence,
                ],
            ),
            BuildPhase::Assign => self.prepare_kind_tables(
                generation,
                &[
                    PageKind::Lists,
                    PageKind::ListUsed,
                    PageKind::ListLinks,
                    PageKind::ListAllocator,
                    PageKind::Catalog,
                ],
            ),
            BuildPhase::Publish => self.prepare_kind_tables(generation, &[PageKind::Catalog]),
            BuildPhase::Ready => Ok(()),
        }
    }

    fn ensure_family(&self, generation: u64, work: &mut SealWorkspace) -> Result<()> {
        if self.slot(generation).is_some() {
            return Ok(());
        }
        (self.checkpoint)()?;
        let available = self
            .record
            .borrow()
            .families
            .iter()
            .position(Option::is_none);
        let slot = match available {
            Some(slot) => slot,
            None => self.retire_inactive_family(work)?,
        };
        let summary = source_summary(self.session.write, &self.route)?;
        let storage = io::allocate_storage(self.session.write, &mut work.native_value)?;
        let mut record = self.record.borrow_mut();
        record.families[slot] = Some(Family {
            generation,
            storage,
            kinds: KindRoots::empty(),
            qualification_root: None,
            policy_revision: record.policy_revision,
            source_frontier: self.session.snapshot_lsn.0,
            source_snapshot: self.session.snapshot.0,
            source_identity: summary.source_identity(),
            manifest: None,
            committed_through_lsn: self.session.row_commit_lsn.0,
            reservoir_samples: None,
            training_seed: None,
            basis_entries: summary.entries,
            basis_live_entries: summary.live_entries,
        });
        if generation != 0 {
            // A new build counts changes since the snapshot it reads, in this
            // same transaction, so publication can promote exact counts.
            let partition =
                contextdb_vector::VectorPartitionRef::new(self.index.clone(), self.tuple.clone());
            let covered_tx = contextdb_core::TxId(self.session.snapshot.0);
            super::super::vector_membership_pages::update_pending_summary(
                self.session.write,
                &partition,
                |summary| {
                    summary.candidate_pending =
                        Some(super::super::vector_membership_pages::PendingCounts {
                            covered_tx,
                            pending_inserts: 0,
                            tombstones: 0,
                        });
                    Ok(())
                },
            )?;
        }
        // Register the actual storage family before its first native page
        // allocation. This provisional control is private to the outer write;
        // successful kernel commit replaces it with final sealed kind roots.
        // The paged inventory retains the actual logical index/tuple route,
        // not merely an irreversible digest. Erasure/reclaim can identify
        // old scoped families after their authoritative members retire.
        work.control_key[..64].copy_from_slice(&self.key);
        work.control_key[64] = b'I';
        NativeIo::Borrowed(self.session.write).put(
            SCOPES.name(),
            &work.control_key[..65],
            &self.route,
        )?;
        record.encode(&self.key, work.control_payload()?)?;
        NativeIo::Borrowed(self.session.write).put(
            SCOPES.name(),
            &self.key,
            &work.native_value[..PAGE_BYTES],
        )?;
        Ok(())
    }
    fn retire_inactive_family(&self, work: &mut SealWorkspace) -> Result<usize> {
        let serving = self.record.borrow().manifest()?.ok_or_else(corrupt)?;
        // Catalog wire format: word9 is the legacy Option tag. A
        // retired run alone is insufficient; the whole legacy generation
        // remains active until this actual authenticated control reaches C.
        if !self.payload(
            serving.key(PageKind::Catalog, 0, 0),
            &mut work.related_metadata,
        )? || &work.related_metadata[..8] != b"IVFCAT02"
            || work.related_metadata[9 * 8..10 * 8] != 0u64.to_le_bytes()
        {
            return Err(corrupt());
        }
        let slot = {
            let record = self.record.borrow();
            let active_build = record
                .build_state()?
                .filter(|b| b.phase != contextdb_vector::ivf::storage::BuildPhase::Ready)
                .map(|b| b.manifest.generation);
            record
                .families
                .iter()
                .position(|f| {
                    f.as_ref().is_some_and(|family| {
                        family.generation != 0
                            && family.generation != serving.generation
                            && Some(family.generation) != active_build
                    })
                })
                .ok_or_else(corrupt)?
        };
        if self.dirty[slot].get() != 0 {
            return Err(corrupt());
        }
        let record = self.record.borrow();
        let family = record.families[slot].as_ref().ok_or_else(corrupt)?;
        work.marker = seal::marker_key(
            &self.key,
            work.operation,
            family.storage,
            serving.key(PageKind::Catalog, 0, 0),
        );
        seal::require_pending_prefix_empty(
            self.session.write,
            &work.marker[..memory::OPERATION_PREFIX_BYTES + 8],
        )?;
        let key = control::retired_key(&self.key, family.storage);
        let native = NativeIo::Borrowed(self.session.write);
        if native.with_value(SCOPES.name(), &key, |v| Ok(v.is_some()))? {
            return Err(corrupt());
        }
        // Serialize the borrowed existing control directly into NativeValue;
        // no fourth resident family or copied control-page window is created.
        control::encode_retired_family(&key, family, work.control_payload()?)?;
        native.put(SCOPES.name(), &key, &work.native_value[..PAGE_BYTES])?;
        let retired_generation = family.generation;
        drop(record);
        let mut record = self.record.borrow_mut();
        record.families[slot] = None;
        if record
            .build_state()?
            .is_some_and(|b| b.manifest.generation == retired_generation)
        {
            record.build = None;
        }
        self.verified[slot].set(0);
        self.verdicts[slot].set(None);
        Ok(slot)
    }
    fn payload(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        #[cfg(feature = "test-seams")]
        let attempt = {
            let next = self.attempts.get().checked_add(1).ok_or_else(corrupt)?;
            self.attempts.set(next);
            next
        };
        #[cfg(feature = "test-seams")]
        self.session
            .observer
            .emit(NativeIvfSealEvent::PageReadAttempt {
                operation: self.observed_operation(self.active_operation.get(), key)?,
                source_instance: self.session.source_instance,
                attempt,
                key,
            });
        let result = (|| {
            self.session.usable()?;
            (self.checkpoint)()?;
            if key.partition != 0 {
                return Err(corrupt());
            }
            if !self.verify_kind(key)? {
                return Ok(false);
            }
            let record = self.record.borrow();
            let family = record.family(key.generation).ok_or_else(corrupt)?;
            let name = KindTableName::new(family.storage, key.kind)?;
            NativeIo::Borrowed(self.session.write).with_existing_value(
                name.as_str()?,
                &header::key_bytes(key),
                |value| {
                    let Some(value) = value else { return Ok(false) };
                    let page = header::AdmittedIvfPage::open(
                        HeaderIdentity {
                            index: &record.index,
                            tuple: &record.tuple,
                            family,
                        },
                        key,
                        value,
                    )?;
                    #[cfg(feature = "test-seams")]
                    self.session.observer.emit(NativeIvfSealEvent::PageOpened {
                        operation: self.observed_operation(self.active_operation.get(), key)?,
                        key,
                        header: value[..192].try_into().map_err(|_| corrupt())?,
                        payload: page.payload,
                    });
                    out.copy_from_slice(page.payload);
                    Ok(true)
                },
            )
        })();
        #[cfg(feature = "test-seams")]
        self.session
            .observer
            .emit(NativeIvfSealEvent::PageReadOutcome {
                operation: self.observed_operation(self.active_operation.get(), key)?,
                source_instance: self.session.source_instance,
                attempt,
                key,
                outcome: match &result {
                    Err(e) => NativeIvfPageReadOutcome::Failed(e),
                    Ok(true) => NativeIvfPageReadOutcome::Authenticated,
                    Ok(false) => NativeIvfPageReadOutcome::Absent,
                },
            });
        result
    }
}

impl ReadPages for WriteScope<'_, '_> {
    fn inspection_verdict(&self, partition: u64, generation: u64) -> Option<Verdict> {
        (partition == 0)
            .then(|| self.slot(generation).and_then(|s| self.verdicts[s].get()))
            .flatten()
    }
    fn record_inspection_verdict(&self, partition: u64, generation: u64, verdict: Verdict) {
        if partition == 0
            && let Some(slot) = self.slot(generation)
        {
            if self.verdicts[slot].get().is_none() {
                self.verdicts[slot].set(Some(verdict));
            }
        }
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        Ok(self.session.snapshot_lsn.0)
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.index
    }
    fn check_cancel(&self) -> Result<()> {
        self.session.usable()?;
        (self.checkpoint)()
    }
    fn before_distance(&self, kind: DistanceKind) -> Result<()> {
        (self.distance)(kind)?;
        if kind == DistanceKind::Centroid {
            let mut record = self.record.borrow_mut();
            match record.build_state()?.map(|b| b.phase) {
                Some(contextdb_vector::ivf::storage::BuildPhase::Train) => {
                    record.training_evaluations = record
                        .training_evaluations
                        .checked_add(1)
                        .ok_or_else(corrupt)?
                }
                Some(contextdb_vector::ivf::storage::BuildPhase::Assign) => {
                    record.assignment_evaluations = record
                        .assignment_evaluations
                        .checked_add(1)
                        .ok_or_else(corrupt)?
                }
                _ => {}
            }
        }
        Ok(())
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
        Ok((after.is_none() && self.record.borrow().serving.is_some()).then_some(0))
    }
    fn read_admitted_page(&self, key: PageKey, output: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.payload(key, output)
    }
}
impl<'a, 'tx> PageStore for WriteScope<'a, 'tx> {
    type Transaction<'s>
        = WriteOperation<'s, 'a, 'tx>
    where
        Self: 's;
    fn begin(&mut self) -> Result<Self::Transaction<'_>> {
        self.begin_operation()
    }
}
impl<'a, 'tx> WriteScope<'a, 'tx> {
    fn begin_operation(&self) -> Result<WriteOperation<'_, 'a, 'tx>> {
        self.check_cancel()?;
        if self.operation_open.get() {
            return Err(Error::InvalidStateTransition(
                "native kernel operation is already open".into(),
            ));
        }
        let operation = self.session.operation()?;
        let prefix = seal::operation_prefix(&self.key, operation);
        let checked = seal::require_operation_empty(self.session.write, &prefix);
        #[cfg(feature = "test-seams")]
        self.session
            .observer
            .emit(NativeIvfSealEvent::OperationPrefixChecked {
                source_instance: self.session.source_instance,
                scope_key: &self.key,
                operation,
                prefix: &prefix,
                empty: checked.is_ok(),
                stage: NativeIvfOperationPrefixStage::Begin,
            });
        checked?;
        #[cfg(feature = "test-seams")]
        self.active_operation.set(Some(operation));
        let workspace = SealWorkspace::reserve_for::<WriteOperation<'_, '_, '_>>(
            &self.session.owner,
            operation,
        )?;
        #[cfg(feature = "test-seams")]
        let before_build = self.record.borrow().build_state()?;
        self.operation_open.set(true);
        let result = WriteOperation {
            scope: self,
            operation,
            finished: false,
            #[cfg(feature = "test-seams")]
            before_build,
            workspace: RefCell::new(workspace),
        };
        #[cfg(feature = "test-seams")]
        let mut result = result;
        #[cfg(feature = "test-seams")]
        {
            let base = &result as *const _ as usize;
            let workspace = result.workspace.get_mut();
            let windows = [
                NativeIvfWindowRange {
                    role: NativeIvfWindowRole::NativeValue,
                    offset: &workspace.native_value as *const _ as usize - base,
                    layout: std::alloc::Layout::new::<[u8; 4096]>(),
                },
                NativeIvfWindowRange {
                    role: NativeIvfWindowRole::PreviousPayload,
                    offset: &workspace.previous_payload as *const _ as usize - base,
                    layout: std::alloc::Layout::new::<[u8; 3904]>(),
                },
                NativeIvfWindowRange {
                    role: NativeIvfWindowRole::RelatedMetadata,
                    offset: &workspace.related_metadata as *const _ as usize - base,
                    layout: std::alloc::Layout::new::<[u8; 3904]>(),
                },
            ];
            workspace._invoice.observe_object(
                NativeIvfObjectKind::SealWorkspace,
                std::alloc::Layout::new::<WriteOperation<'_, '_, '_>>(),
                Some(result.scope.key),
                Some(&windows),
            )?;
        }
        Ok(result)
    }
}

// A kernel window borrows the admitted scope instead of moving or cloning it.
// These implementations forward to the same native owner and transaction.
impl ReadPages for &WriteScope<'_, '_> {
    fn inspection_verdict(&self, partition: u64, generation: u64) -> Option<Verdict> {
        (**self).inspection_verdict(partition, generation)
    }
    fn record_inspection_verdict(&self, partition: u64, generation: u64, verdict: Verdict) {
        (**self).record_inspection_verdict(partition, generation, verdict)
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        (**self).snapshot_lsn()
    }
    fn index_identity(&self) -> &VectorIndexRef {
        (**self).index_identity()
    }
    fn check_cancel(&self) -> Result<()> {
        (**self).check_cancel()
    }
    fn before_distance(&self, kind: DistanceKind) -> Result<()> {
        (**self).before_distance(kind)
    }
    fn manifest(&self, partition: u64) -> Result<Option<Manifest>> {
        (**self).manifest(partition)
    }
    fn build_state(&self, partition: u64) -> Result<Option<BuildState>> {
        (**self).build_state(partition)
    }
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>> {
        (**self).next_partition(after)
    }
    fn read_admitted_page(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        (**self).read_admitted_page(key, out)
    }
}
impl<'a, 'tx> PageStore for &WriteScope<'a, 'tx> {
    type Transaction<'s>
        = WriteOperation<'s, 'a, 'tx>
    where
        Self: 's;
    fn begin(&mut self) -> Result<Self::Transaction<'_>> {
        (**self).begin_operation()
    }
}
impl ReadPages for &mut WriteScope<'_, '_> {
    fn inspection_verdict(&self, partition: u64, generation: u64) -> Option<Verdict> {
        (**self).inspection_verdict(partition, generation)
    }
    fn record_inspection_verdict(&self, partition: u64, generation: u64, verdict: Verdict) {
        (**self).record_inspection_verdict(partition, generation, verdict)
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        (**self).snapshot_lsn()
    }
    fn index_identity(&self) -> &VectorIndexRef {
        (**self).index_identity()
    }
    fn check_cancel(&self) -> Result<()> {
        (**self).check_cancel()
    }
    fn before_distance(&self, kind: DistanceKind) -> Result<()> {
        (**self).before_distance(kind)
    }
    fn manifest(&self, partition: u64) -> Result<Option<Manifest>> {
        (**self).manifest(partition)
    }
    fn build_state(&self, partition: u64) -> Result<Option<BuildState>> {
        (**self).build_state(partition)
    }
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>> {
        (**self).next_partition(after)
    }
    fn read_admitted_page(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        (**self).read_admitted_page(key, out)
    }
}
impl<'a, 'tx> PageStore for &mut WriteScope<'a, 'tx> {
    type Transaction<'s>
        = WriteOperation<'s, 'a, 'tx>
    where
        Self: 's;
    fn begin(&mut self) -> Result<Self::Transaction<'_>> {
        (**self).begin_operation()
    }
}

/// The complete operation layout, with exactly the three copied windows in
/// workspace. Dropping without a successful seal poisons the outer session.
pub struct WriteOperation<'s, 'a, 'tx> {
    scope: &'s WriteScope<'a, 'tx>,
    operation: u64,
    finished: bool,
    #[cfg(feature = "test-seams")]
    before_build: Option<BuildState>,
    workspace: RefCell<SealWorkspace>,
}
impl Drop for WriteOperation<'_, '_, '_> {
    fn drop(&mut self) {
        self.scope.operation_open.set(false);
        #[cfg(feature = "test-seams")]
        self.scope.active_operation.set(None);
        if !self.finished {
            self.scope.session.failed.set(true);
        }
    }
}
impl SemanticView for WriteOperation<'_, '_, '_> {
    fn checkpoint(&self) -> Result<()> {
        self.scope.check_cancel()
    }
    fn generation_manifest(&self, generation: u64) -> Result<Manifest> {
        self.scope
            .record
            .borrow()
            .family(generation)
            .and_then(|f| f.manifest)
            .ok_or_else(corrupt)?
            .manifest()
    }
    fn reservoir_samples(&self, generation: u64) -> Result<u64> {
        self.scope
            .record
            .borrow()
            .family(generation)
            .and_then(|f| f.reservoir_samples)
            .ok_or_else(corrupt)
    }
    fn payload(&self, key: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.scope.payload(key, out)
    }
}
impl SealView for WriteOperation<'_, '_, '_> {
    fn mark(&self, key: PageKey) -> Result<()> {
        self.checkpoint()?;
        let record = self.scope.record.borrow();
        let family = record.family(key.generation).ok_or_else(corrupt)?;
        let marker = seal::marker_key(&self.scope.key, self.operation, family.storage, key);
        #[cfg(feature = "test-seams")]
        self.scope
            .session
            .observer
            .boundary(NativeIvfFaultBoundary::BeforeMarkerWrite {
                operation: self.observed_operation(key)?,
                key,
            })?;
        let newly_inserted = seal::mark(self.scope.session.write, &marker, None)?;
        #[cfg(feature = "test-seams")]
        self.scope
            .session
            .observer
            .emit(NativeIvfSealEvent::MarkerInserted {
                operation: self.observed_operation(key)?,
                key,
                newly_inserted,
            });
        #[cfg(not(feature = "test-seams"))]
        let _ = newly_inserted;
        Ok(())
    }
}
impl NativeSealView for WriteOperation<'_, '_, '_> {
    #[cfg(feature = "test-seams")]
    fn observe_page_write_for_test(
        &self,
        key: PageKey,
        insert: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        crate::vector_ivf_observations::disk::observe_native_ivf_write_for_test(
            self.scope.session.disk_owner.as_ref(),
            self.scope.observed_scope(),
            crate::vector_ivf_observations::disk::native_ivf_write_class_for_test(
                self.before_build,
                key.kind,
            ),
            insert,
        )
    }
    #[cfg(feature = "test-seams")]
    fn observed_operation(&self, key: PageKey) -> Result<NativeIvfOperationRef<'_>> {
        self.scope.observed_operation(Some(self.operation), key)
    }
    #[cfg(feature = "test-seams")]
    fn observer(&self) -> &Producer {
        &self.scope.session.observer
    }
    #[cfg(feature = "test-seams")]
    fn source_instance(&self) -> NativeIvfSourceInstance {
        self.scope.session.source_instance
    }
    #[cfg(feature = "test-seams")]
    fn observed_scope(&self) -> NativeIvfScopeRef<'_> {
        self.scope.observed_scope()
    }

    fn write(&self) -> &redb::WriteTransaction {
        self.scope.session.write
    }
    fn scope_key(&self) -> &[u8; 64] {
        &self.scope.key
    }
    fn record(&self) -> Ref<'_, ScopeRecord> {
        self.scope.record.borrow()
    }
    fn record_cell(&self) -> &RefCell<ScopeRecord> {
        &self.scope.record
    }
    fn dirty_kinds(&self) -> [u16; control::ROOTS] {
        std::array::from_fn(|i| self.scope.dirty[i].get())
    }
    fn commit_frontier(&self) -> Result<u64> {
        self.scope.check_cancel()?;
        Ok(self.scope.session.row_commit_lsn.0)
    }
    fn header_written(&self, key: PageKey) -> Result<()> {
        self.scope.changed(key)
    }
    fn roots_staged(&self) -> Result<()> {
        for slot in 0..control::ROOTS {
            self.scope.dirty[slot].set(0);
            self.scope.verified[slot].set(0);
            self.scope.verdicts[slot].set(None);
        }
        Ok(())
    }
}
impl ReadPages for WriteOperation<'_, '_, '_> {
    fn inspection_verdict(&self, partition: u64, generation: u64) -> Option<Verdict> {
        self.scope.inspection_verdict(partition, generation)
    }
    fn record_inspection_verdict(&self, partition: u64, generation: u64, verdict: Verdict) {
        self.scope
            .record_inspection_verdict(partition, generation, verdict)
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        self.scope.snapshot_lsn()
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.scope.index_identity()
    }
    fn check_cancel(&self) -> Result<()> {
        self.scope.check_cancel()
    }
    fn before_distance(&self, kind: DistanceKind) -> Result<()> {
        self.scope.before_distance(kind)
    }
    fn manifest(&self, partition: u64) -> Result<Option<Manifest>> {
        self.scope.manifest(partition)
    }
    fn build_state(&self, partition: u64) -> Result<Option<BuildState>> {
        self.scope.build_state(partition)
    }
    fn next_partition(&self, after: Option<u64>) -> Result<Option<u64>> {
        self.scope.next_partition(after)
    }
    fn read_admitted_page(&self, key: PageKey, output: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.scope.payload(key, output)
    }
}
impl WritePages for WriteOperation<'_, '_, '_> {
    fn commit_lsn(&self) -> Result<u64> {
        Ok(self
            .scope
            .identity_lsn
            .get()
            .unwrap_or(self.scope.session.row_commit_lsn.0))
    }
    fn put_page(&mut self, key: PageKey, payload: &[u8; PAGE_BYTES]) -> Result<()> {
        self.check_cancel()?;
        self.scope
            .ensure_family(key.generation, &mut self.workspace.borrow_mut())?;
        // Verify the old root before the first write, including insertion into
        // a previously absent kind. Corrupt old data is never reauthenticated.
        let existed = self.scope.verify_kind(key)?;
        let record = self.scope.record.borrow();
        let family = record.family(key.generation).ok_or_else(corrupt)?;
        let name = KindTableName::new(family.storage, key.kind)?;
        let marker = seal::marker_key(&self.scope.key, self.operation, family.storage, key);
        let native = NativeIo::Borrowed(self.scope.session.write);
        #[cfg(feature = "test-seams")]
        self.scope
            .session
            .observer
            .boundary(NativeIvfFaultBoundary::BeforeMarkerWrite {
                operation: self.observed_operation(key)?,
                key,
            })?;
        let newly_inserted = if existed {
            native.with_existing_value(name.as_str()?, &header::key_bytes(key), |old| {
                let old = old
                    .map(|value| {
                        header::AdmittedIvfPage::open(
                            HeaderIdentity {
                                index: &record.index,
                                tuple: &record.tuple,
                                family,
                            },
                            key,
                            value,
                        )
                    })
                    .transpose()?;
                let counter = matches!(
                    key.kind,
                    PageKind::ListUsed | PageKind::ListAllocator | PageKind::Catalog
                );
                // New counter pages have an all-zero old state; preserve it
                // without allocating an additional owned native window.
                let mut work = self.workspace.borrow_mut();
                let previous = if counter {
                    work.previous_payload.fill(0);
                    if let Some(old) = old {
                        work.previous_payload.copy_from_slice(old.payload);
                    }
                    Some(&work.previous_payload)
                } else {
                    None
                };
                seal::mark(self.scope.session.write, &marker, previous)
            })?
        } else {
            let mut work = self.workspace.borrow_mut();
            work.previous_payload.fill(0);
            let counter = matches!(
                key.kind,
                PageKind::ListUsed | PageKind::ListAllocator | PageKind::Catalog
            );
            seal::mark(
                self.scope.session.write,
                &marker,
                counter.then_some(&work.previous_payload),
            )?
        };
        #[cfg(feature = "test-seams")]
        self.scope
            .session
            .observer
            .emit(NativeIvfSealEvent::MarkerInserted {
                operation: self.observed_operation(key)?,
                key,
                newly_inserted,
            });
        #[cfg(not(feature = "test-seams"))]
        let _ = newly_inserted;
        let mut work = self.workspace.borrow_mut();
        header::encode(
            HeaderIdentity {
                index: &record.index,
                tuple: &record.tuple,
                family,
            },
            key,
            semantic::Records { first: 0, count: 0 },
            payload,
            &mut work.native_value,
        )?;
        #[cfg(feature = "test-seams")]
        self.scope
            .session
            .observer
            .boundary(NativeIvfFaultBoundary::BeforePayloadWrite {
                operation: self.observed_operation(key)?,
                key,
            })?;
        #[cfg(feature = "test-seams")]
        native.put_using(
            name.as_str()?,
            &header::key_bytes(key),
            &work.native_value,
            |insert| self.observe_page_write_for_test(key, insert),
        )?;
        #[cfg(not(feature = "test-seams"))]
        native.put(name.as_str()?, &header::key_bytes(key), &work.native_value)?;
        #[cfg(feature = "test-seams")]
        self.scope
            .session
            .observer
            .emit(NativeIvfSealEvent::PayloadWritten {
                operation: self.observed_operation(key)?,
                key,
            });
        drop(record);
        self.scope.changed(key)
    }
    fn stage_build(&mut self, state: BuildState) -> Result<()> {
        self.check_cancel()?;
        self.scope
            .ensure_family(state.manifest.generation, &mut self.workspace.borrow_mut())?;
        let before = self.scope.record.borrow().build_state()?;
        #[cfg(feature = "test-seams")]
        if before.is_none_or(|b| b.manifest.generation != state.manifest.generation) {
            self.scope
                .session
                .observer
                .emit(NativeIvfSealEvent::BuildStarted {
                    scope: self.scope.observed_scope(),
                });
        }
        {
            let mut record = self.scope.record.borrow_mut();
            let slot = record
                .families
                .iter()
                .position(|f| {
                    f.as_ref()
                        .is_some_and(|f| f.generation == state.manifest.generation)
                })
                .ok_or_else(corrupt)?;
            let family = record.families[slot].as_mut().ok_or_else(corrupt)?;
            family.manifest = Some(state.manifest.into());
            family.reservoir_samples = Some(state.training.reservoir_samples as u64);
            family.training_seed = Some(state.training.seed);
            family.committed_through_lsn = self.scope.session.row_commit_lsn.0;
            record.build = Some(state.into());
        }
        semantic::build_dependencies(
            self,
            before,
            state,
            &mut self.workspace.borrow_mut().related_metadata,
        )
    }
    fn publish_manifest(&mut self, manifest: Manifest) -> Result<()> {
        self.check_cancel()?;
        self.scope
            .ensure_family(manifest.generation, &mut self.workspace.borrow_mut())?;
        let (before, source_snapshot) = {
            let mut record = self.scope.record.borrow_mut();
            let slot = record
                .families
                .iter()
                .position(|f| {
                    f.as_ref()
                        .is_some_and(|f| f.generation == manifest.generation)
                })
                .ok_or_else(corrupt)?;
            let family = record.families[slot].as_mut().ok_or_else(corrupt)?;
            let before = family.manifest.map(|m| m.manifest()).transpose()?;
            family.manifest = Some(manifest.into());
            family.committed_through_lsn = self.scope.session.row_commit_lsn.0;
            let source_snapshot = family.source_snapshot;
            record.serving = Some(manifest.into());
            (before, source_snapshot)
        };
        if manifest.generation != 0 && before.is_none() {
            // The build generation starts serving: its counts become the
            // partition's pending counts. Tail and remap manifests keep them.
            let partition = contextdb_vector::VectorPartitionRef::new(
                self.scope.index.clone(),
                self.scope.tuple.clone(),
            );
            super::super::vector_membership_pages::update_pending_summary(
                self.scope.session.write,
                &partition,
                |summary| {
                    let candidate = summary.candidate_pending.take().ok_or_else(corrupt)?;
                    if candidate.covered_tx != contextdb_core::TxId(source_snapshot) {
                        return Err(corrupt());
                    }
                    summary.pending = candidate;
                    Ok(())
                },
            )?;
        }
        // Prepared remap slots are created exactly once, before their run root
        // becomes visible. Count their real persisted marker in this same seal.
        if let Some(old) = before.filter(|old| old.generation == manifest.generation) {
            let mut prepared = 0u64;
            let mut work = self.workspace.borrow_mut();
            for slot in old.slots..manifest.slots {
                let offset = slot
                    .checked_mul(40)
                    .and_then(|n| n.checked_add(32))
                    .ok_or_else(corrupt)?;
                let mut raw = [0; 8];
                let mut copied = 0;
                while copied < 8 {
                    let position = offset.checked_add(copied as u64).ok_or_else(corrupt)?;
                    if !self.scope.payload(
                        manifest.key(PageKind::SlotMetadata, 0, position / PAGE_BYTES as u64),
                        &mut work.previous_payload,
                    )? {
                        return Err(corrupt());
                    }
                    let start = (position % PAGE_BYTES as u64) as usize;
                    let take = (8 - copied).min(PAGE_BYTES - start);
                    raw[copied..copied + take]
                        .copy_from_slice(&work.previous_payload[start..start + take]);
                    copied += take;
                }
                match u64::from_le_bytes(raw) {
                    0 => {}
                    1 => prepared = prepared.checked_add(1).ok_or_else(corrupt)?,
                    _ => return Err(corrupt()),
                }
            }
            let mut record = self.scope.record.borrow_mut();
            record.legacy_entries_prepared = record
                .legacy_entries_prepared
                .checked_add(prepared)
                .ok_or_else(corrupt)?;
            #[cfg(feature = "test-seams")]
            if prepared != 0 {
                let total = record.legacy_entries_prepared;
                drop(record);
                self.scope
                    .session
                    .observer
                    .emit(NativeIvfSealEvent::LegacyEntriesPrepared {
                        scope: self.scope.observed_scope(),
                        entries: prepared,
                        total,
                    });
            }
        }
        semantic::manifest_dependencies(
            self,
            before,
            manifest,
            &mut self.workspace.borrow_mut().related_metadata,
        )
    }
    fn commit(mut self) -> Result<()> {
        self.check_cancel()?;
        {
            let record = self.scope.record.borrow();
            record.validate(self.scope.index, record.schema, record.tuple)?;
        }
        seal::seal_operation(&self, self.operation, &mut self.workspace.borrow_mut())?;
        #[cfg(feature = "test-seams")]
        self.observe_ingested()?;
        self.finished = true;
        Ok(())
    }
}

pub(super) fn control_bytes(index: &VectorIndexRef, tuple: &VectorPartitionKey) -> Result<usize> {
    memory::route_bytes(index, tuple)?
        .checked_add(std::mem::size_of::<WriteSession<'_>>())
        .and_then(|n| n.checked_add(std::mem::size_of::<WriteScope<'_, '_>>()))
        .and_then(|n| n.checked_add(std::mem::size_of::<WriteOperation<'_, '_, '_>>()))
        .ok_or_else(corrupt)
}

impl WriteScope<'_, '_> {
    pub(super) fn with_source_row_identity<R>(
        &self,
        identity: contextdb_vector::ivf::RowIdentity,
        run: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        if self
            .identity_lsn
            .replace(Some(identity.commit_lsn))
            .is_some()
        {
            return Err(corrupt());
        }
        struct Reset<'a>(&'a Cell<Option<u64>>);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.set(None);
            }
        }
        let _reset = Reset(&self.identity_lsn);
        let value = run()?;
        #[cfg(feature = "test-seams")]
        self.session
            .observer
            .emit(NativeIvfSealEvent::JournalInitialized {
                scope: self.observed_scope(),
                identity,
                deleted: false,
            });
        Ok(value)
    }
    pub(super) fn after_row_staged(
        &self,
        identity: contextdb_vector::ivf::RowIdentity,
        deleted: bool,
    ) -> Result<()> {
        #[cfg(feature = "test-seams")]
        self.session.observer.boundary(
            NativeIvfFaultBoundary::AfterAuthoritativeAndJournalStaged {
                scope: self.observed_scope(),
                identity,
                deleted,
            },
        )?;
        #[cfg(not(feature = "test-seams"))]
        let _ = (identity, deleted);
        Ok(())
    }
    pub(super) fn prepare_source_changes(
        &mut self,
        before: &super::super::vector_membership_pages::MembershipSummary,
        after: &super::super::vector_membership_pages::MembershipSummary,
    ) -> Result<()> {
        {
            let mut record = self.record.borrow_mut();
            if record.canonical_complete
                && (record.current_membership_digest != before.source_identity()
                    || record.current_membership_entries != before.entries
                    || record.current_membership_live_entries != before.live_entries)
            {
                return Err(corrupt());
            }
            // A complete empty source plus every successfully applied mutation
            // establishes coverage. An old populated source needs bounded ingestion.
            record.canonical_complete |= before.entries == 0;
            if record
                .build_state()?
                .is_some_and(|b| b.phase != contextdb_vector::ivf::storage::BuildPhase::Ready)
            {
                for (delta, (a, b)) in record.build_changes.digest.iter_mut().zip(
                    before
                        .source_identity()
                        .into_iter()
                        .zip(after.source_identity()),
                ) {
                    *delta ^= a ^ b;
                }
                let changes = &mut record.build_changes;
                changes.inserted = changes
                    .inserted
                    .checked_add(after.entries.saturating_sub(before.entries))
                    .ok_or_else(corrupt)?;
                changes.removed = changes
                    .removed
                    .checked_add(before.entries.saturating_sub(after.entries))
                    .ok_or_else(corrupt)?;
                changes.live_inserted = changes
                    .live_inserted
                    .checked_add(after.live_entries.saturating_sub(before.live_entries))
                    .ok_or_else(corrupt)?;
                changes.live_removed = changes
                    .live_removed
                    .checked_add(before.live_entries.saturating_sub(after.live_entries))
                    .ok_or_else(corrupt)?;
            }
            record.current_membership_digest = after.source_identity();
            record.current_membership_entries = after.entries;
            record.current_membership_live_entries = after.live_entries;
            record.committed_through_lsn = self.session.row_commit_lsn.0;
        }
        Ok(())
    }
}

#[cfg(feature = "test-seams")]
impl WriteOperation<'_, '_, '_> {
    fn observe_ingested(&self) -> Result<()> {
        let Some(before) = self
            .before_build
            .filter(|b| b.phase == contextdb_vector::ivf::storage::BuildPhase::Ingest)
        else {
            return Ok(());
        };
        let Some(after) = self.scope.record.borrow().build_state()? else {
            return Ok(());
        };
        if before.manifest.generation != after.manifest.generation {
            return Ok(());
        }
        let stride = (after.manifest.dimensions as u64)
            .checked_mul(4)
            .and_then(|n| n.checked_add(8))
            .ok_or_else(corrupt)?;
        for slot in before.manifest.slots..after.manifest.slots {
            let mut row_bytes = [0u8; 8];
            let offset = slot.checked_mul(stride).ok_or_else(corrupt)?;
            let mut copied = 0usize;
            let mut work = self.workspace.borrow_mut();
            while copied < 8 {
                let position = offset + copied as u64;
                let ordinal = position / PAGE_BYTES as u64;
                let start = (position % PAGE_BYTES as u64) as usize;
                if !self.scope.payload(
                    after.manifest.key(PageKind::Canonical, 0, ordinal),
                    &mut work.previous_payload,
                )? {
                    return Err(corrupt());
                }
                let take = (8 - copied).min(PAGE_BYTES - start);
                row_bytes[copied..copied + take]
                    .copy_from_slice(&work.previous_payload[start..start + take]);
                copied += take;
            }
            let offset = slot.checked_mul(40).ok_or_else(corrupt)?;
            let mut lsn = [0u8; 8];
            let mut copied = 0;
            while copied < 8 {
                let position = offset + copied as u64;
                let ordinal = position / PAGE_BYTES as u64;
                let start = (position % PAGE_BYTES as u64) as usize;
                if !self.scope.payload(
                    after.manifest.key(PageKind::SlotMetadata, 0, ordinal),
                    &mut work.previous_payload,
                )? {
                    return Err(corrupt());
                }
                let take = (8 - copied).min(PAGE_BYTES - start);
                lsn[copied..copied + take]
                    .copy_from_slice(&work.previous_payload[start..start + take]);
                copied += take;
            }
            drop(work);
            self.scope
                .session
                .observer
                .emit(NativeIvfSealEvent::SourceRowIngested {
                    scope: self.scope.observed_scope(),
                    identity: contextdb_vector::ivf::RowIdentity {
                        row: u64::from_le_bytes(row_bytes),
                        commit_lsn: u64::from_le_bytes(lsn),
                    },
                });
        }
        Ok(())
    }
}

impl WriteScope<'_, '_> {
    /// Delete at most the declared number of inactive payloads. Native COW
    /// retains the old physical roots for existing registered source holders.
    pub(super) fn reclaim_inactive(&mut self, mut limit: usize) -> Result<bool> {
        use redb::{ReadableTable, ReadableTableMetadata};
        let serving = self.record.borrow().manifest()?.ok_or_else(corrupt)?;
        let build = self
            .record
            .borrow()
            .build_state()?
            .filter(|b| b.phase != contextdb_vector::ivf::storage::BuildPhase::Ready)
            .map(|b| b.manifest.generation);
        let slot = self.record.borrow().families.iter().position(|f| {
            f.is_some_and(|f| {
                f.generation != serving.generation
                    && Some(f.generation) != build
                    && (f.generation != 0 || f.kinds.present & !((1u16 << 14) | (1u16 << 15)) != 0)
            })
        });
        let mut retired_key = None;
        let mut family = if let Some(slot) = slot {
            self.record.borrow().families[slot].ok_or_else(corrupt)?
        } else {
            let mut prefix = [0; 65];
            prefix[..64].copy_from_slice(&self.key);
            prefix[64] = b'R';
            let scopes = self
                .session
                .write
                .open_table(SCOPES)
                .map_err(RedbPersistence::storage_error)?;
            let mut cursor = scopes
                .range(prefix.as_slice()..)
                .map_err(RedbPersistence::storage_error)?;
            let retired = match cursor.next() {
                Some(entry) => {
                    let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                    if key.value().starts_with(&prefix) {
                        Some((
                            <[u8; 73]>::try_from(key.value()).map_err(|_| corrupt())?,
                            control::decode_retired_family(key.value(), value.value())?,
                        ))
                    } else {
                        None
                    }
                }
                None => None,
            };
            drop(cursor);
            drop(scopes);
            let Some((key, family)) = retired else {
                return self.reclaim_obsolete_copy_runs(serving.generation, build, &mut limit);
            };
            retired_key = Some(key);
            family
        };
        #[cfg(feature = "test-seams")]
        crate::vector_ivf_observations::work_point(NativeIvfWorkPoint::ReclaimBatch);
        self.check_cancel()?;
        let mut remaining = limit;
        let native = NativeIo::Borrowed(self.session.write);
        for tag in 1..=16 {
            let bit = 1 << (tag - 1);
            // Generation zero also owns the permanent version/copy journal.
            // Its pre-codebook vectors and runs retire at C; the journal stays.
            if family.generation == 0 && matches!(tag, 15 | 16) {
                continue;
            }
            if family.kinds.present & bit == 0 {
                continue;
            }
            let kind = header::kind_from_tag(tag)?;
            let name = io::KindTableName::new(family.storage, kind)?;
            let definition = redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str()?);
            {
                let mut table = self
                    .session
                    .write
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                while remaining != 0 {
                    self.check_cancel()?;
                    let key = {
                        let mut cursor = table.iter().map_err(RedbPersistence::storage_error)?;
                        match cursor.next() {
                            Some(entry) => Some(
                                <[u8; header::KEY_BYTES]>::try_from(
                                    entry.map_err(RedbPersistence::storage_error)?.0.value(),
                                )
                                .map_err(|_| corrupt())?,
                            ),
                            None => None,
                        }
                    };
                    let Some(key) = key else {
                        break;
                    };
                    table
                        .remove(key.as_slice())
                        .map_err(RedbPersistence::storage_error)?;
                    remaining -= 1;
                }
                let empty = table.is_empty().map_err(RedbPersistence::storage_error)?;
                drop(table);
                if empty {
                    self.session
                        .write
                        .delete_table(definition)
                        .map_err(RedbPersistence::storage_error)?;
                    family.kinds.present &= !bit;
                    family.kinds.roots[(tag - 1) as usize] = [0; 32];
                } else {
                    family.kinds.roots[(tag - 1) as usize] =
                        native.fingerprint(name.as_str()?)?.ok_or_else(corrupt)?;
                }
            }
            if remaining == 0 {
                break;
            }
        }
        self.reclaim_qualification(&mut family, &mut remaining)?;
        if let Some(slot) = slot {
            self.record.borrow_mut().families[slot] =
                if family.kinds.present == 0 && family.qualification_root.is_none() {
                    None
                } else {
                    Some(family)
                };
            self.verified[slot].set(0);
            self.verdicts[slot].set(None);
        } else if let Some(key) = retired_key {
            if family.kinds.present == 0 && family.qualification_root.is_none() {
                self.session
                    .write
                    .open_table(SCOPES)
                    .map_err(RedbPersistence::storage_error)?
                    .remove(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            } else {
                let mut payload = [0; PAGE_BYTES];
                control::encode_retired_family(&key, &family, &mut payload)?;
                native.put(SCOPES.name(), &key, &payload)?;
            }
        }
        self.begin()?.commit()?;
        self.check_cancel()?;
        let pending = self
            .record
            .borrow()
            .families
            .iter()
            .flatten()
            .any(|family| {
                family.generation != serving.generation
                    && Some(family.generation) != build
                    && (family.generation != 0
                        || family.kinds.present & !((1u16 << 14) | (1u16 << 15)) != 0)
            });
        if pending {
            return Ok(true);
        }
        let mut prefix = [0; 65];
        prefix[..64].copy_from_slice(&self.key);
        prefix[64] = b'R';
        let scopes = self
            .session
            .write
            .open_table(SCOPES)
            .map_err(RedbPersistence::storage_error)?;
        let mut cursor = scopes
            .range(prefix.as_slice()..)
            .map_err(RedbPersistence::storage_error)?;
        let retired_pending = match cursor.next() {
            Some(entry) => entry
                .map_err(RedbPersistence::storage_error)?
                .0
                .value()
                .starts_with(&prefix),
            None => false,
        };
        drop(cursor);
        drop(scopes);
        if retired_pending {
            return Ok(true);
        }
        self.reclaim_obsolete_copy_runs(serving.generation, build, &mut remaining)
    }

    /// The permanent journal table also contains per-generation copy runs.
    /// After inactive families are gone, writer traversal stops before runs
    /// older than every serving/build generation. The caller already holds
    /// the ordinary snapshot-safe reclamation gate.
    fn reclaim_obsolete_copy_runs(
        &mut self,
        serving: u64,
        building: Option<u64>,
        remaining: &mut usize,
    ) -> Result<bool> {
        use redb::{ReadableTable, ReadableTableMetadata};
        let floor = building.map_or(serving, |generation| generation.min(serving));
        if floor == 0 {
            return Ok(false);
        }
        let Some(slot) = self
            .record
            .borrow()
            .families
            .iter()
            .position(|family| family.is_some_and(|family| family.generation == 0))
        else {
            return Ok(false);
        };
        let family = self.record.borrow().families[slot].ok_or_else(corrupt)?;
        let kind_index = usize::from(header::kind_tag(PageKind::RowCopies) - 1);
        let bit = 1u16 << kind_index;
        if family.kinds.present & bit == 0 {
            return Ok(false);
        }
        // This journal remains live authority. Authenticate its captured
        // kind root before removing even an obsolete run from that table.
        self.prepare_kind_tables(0, &[PageKind::RowCopies])?;
        let name = io::KindTableName::new(family.storage, PageKind::RowCopies)?;
        let definition = redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str()?);
        let mut table = self
            .session
            .write
            .open_table(definition)
            .map_err(RedbPersistence::storage_error)?;
        let mut after = [0; header::KEY_BYTES];
        let mut changed = false;
        let pending = loop {
            self.check_cancel()?;
            let next = {
                let mut cursor = table
                    .range(after.as_slice()..)
                    .map_err(RedbPersistence::storage_error)?;
                let mut next = None;
                for entry in &mut cursor {
                    self.check_cancel()?;
                    let (key, _) = entry.map_err(RedbPersistence::storage_error)?;
                    let key =
                        <[u8; header::KEY_BYTES]>::try_from(key.value()).map_err(|_| corrupt())?;
                    let decoded = header::key_from_bytes(&key)?;
                    if decoded.kind != PageKind::RowCopies
                        || decoded.partition != 0
                        || decoded.generation != 0
                        || decoded.list != 0
                    {
                        return Err(corrupt());
                    }
                    if decoded.run < floor {
                        next = Some(key);
                        break;
                    }
                }
                next
            };
            let Some(key) = next else {
                break false;
            };
            if *remaining == 0 {
                break true;
            }
            #[cfg(feature = "test-seams")]
            if !changed {
                crate::vector_ivf_observations::work_point(NativeIvfWorkPoint::ReclaimBatch);
            }
            if table
                .remove(key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .is_none()
            {
                return Err(corrupt());
            }
            *remaining -= 1;
            changed = true;
            after = key;
        };
        let empty = table.is_empty().map_err(RedbPersistence::storage_error)?;
        drop(table);
        if changed {
            let root = if empty {
                self.session
                    .write
                    .delete_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                None
            } else {
                Some(
                    NativeIo::Borrowed(self.session.write)
                        .fingerprint(name.as_str()?)?
                        .ok_or_else(corrupt)?,
                )
            };
            {
                let mut record = self.record.borrow_mut();
                let family = record.families[slot].as_mut().ok_or_else(corrupt)?;
                if let Some(root) = root {
                    family.kinds.roots[kind_index] = root;
                } else {
                    family.kinds.present &= !bit;
                    family.kinds.roots[kind_index] = [0; 32];
                }
            }
            self.verified[slot].set(0);
            self.verdicts[slot].set(None);
            self.begin()?.commit()?;
            self.check_cancel()?;
        }
        Ok(pending)
    }
}
