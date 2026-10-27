//! Borrowed native writer. Kernel commits seal inside one outer transaction;
//! only the outer owner can commit rows and the final Configuration binding.
use super::*;
use super::{
    control::{Family, KindRoots, ScopeRecord},
    header::HeaderIdentity,
    io::{KindTableName, NativeIo},
    memory::{Invoice, ScopeConstruction, SealWorkspace},
    seal::NativeSealView,
    semantic::{SealView, SemanticView},
};
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
fn source_summary(
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
pub(super) struct WriteSession<'a> {
    write: &'a redb::WriteTransaction,
    admission: &'a super::super::native_storage_admission::StorageAdmissionScope,
    owner: MemoryBudgetOwner,
    snapshot: contextdb_core::SnapshotId,
    snapshot_lsn: Lsn,
    row_commit_lsn: Lsn,
    next_operation: Cell<u64>,
    failed: Cell<bool>,
    bound: Cell<bool>,
    _invoice: Invoice,
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
        let invoice =
            Invoice::reserve(&owner, std::mem::size_of::<Self>(), "clustered_outer_write")?;
        admission.run_original_unqualified(|| {
            super::super::content_identity::validate_clustered_scope_root_in_write(write)?;
            super::super::vector_membership_pages::validate_in_write(write)?;
            seal::require_global_empty(write)
        })?;
        Ok(Self {
            write,
            admission,
            owner,
            snapshot,
            snapshot_lsn,
            row_commit_lsn,
            next_operation: Cell::new(1),
            failed: Cell::new(false),
            bound: Cell::new(false),
            _invoice: invoice,
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
            seal::require_global_empty(self.write)?;
            super::super::content_identity::bind_vector_pages_root_in_write(self.write)
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
    key: [u8; 64],
    record: RefCell<ScopeRecord>,
    verified: [Cell<u16>; control::ROOTS],
    dirty: [Cell<u16>; control::ROOTS],
    verdicts: [Cell<Option<Verdict>>; control::ROOTS],
    checkpoint: &'a dyn Fn() -> Result<()>,
    distance: &'a dyn Fn(DistanceKind) -> Result<()>,
    completed: bool,
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
            run(&mut writer)
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
            verified: std::array::from_fn(|_| Cell::new(0)),
            dirty: std::array::from_fn(|_| Cell::new(0)),
            verdicts: std::array::from_fn(|_| Cell::new(None)),
            checkpoint,
            distance,
            completed: false,
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
            if actual != expected {
                return Err(corrupt());
            }
            self.verified[slot].set(self.verified[slot].get() | bit);
        }
        Ok(present)
    }
    fn changed(&self, key: PageKey) -> Result<()> {
        let slot = self.slot(key.generation).ok_or_else(corrupt)?;
        let bit = 1u16 << (header::kind_tag(key.kind) - 1);
        self.dirty[slot].set(self.dirty[slot].get() | bit);
        self.verified[slot].set(0);
        self.verdicts[slot].set(None);
        Ok(())
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
            policy_revision: record.policy_revision,
            source_frontier: self.session.snapshot_lsn.0,
            source_snapshot: self.session.snapshot.0,
            source_identity: summary.source_identity(),
            manifest: None,
            committed_through_lsn: self.session.row_commit_lsn.0,
            reservoir_samples: None,
            basis_entries: summary.entries,
            basis_live_entries: summary.live_entries,
        });
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
        // Exact accepted Catalog wire: word9 is the legacy Option tag. A
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
                out.copy_from_slice(page.payload);
                Ok(true)
            },
        )
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
            self.verdicts[slot].set(Some(verdict));
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
        self.check_cancel()?;
        let operation = self.session.operation()?;
        seal::require_operation_empty(
            self.session.write,
            &seal::operation_prefix(&self.key, operation),
        )?;
        let workspace = SealWorkspace::reserve_for::<WriteOperation<'_, '_, '_>>(
            &self.session.owner,
            operation,
        )?;
        Ok(WriteOperation {
            scope: self,
            operation,
            finished: false,
            workspace: RefCell::new(workspace),
        })
    }
}

// A kernel window borrows the admitted scope instead of moving or cloning it.
// These implementations forward to the same native owner and transaction.
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
        (**self).begin()
    }
}

/// The complete operation layout, with exactly the three copied windows in
/// workspace. Dropping without a successful seal poisons the outer session.
pub struct WriteOperation<'s, 'a, 'tx> {
    scope: &'s mut WriteScope<'a, 'tx>,
    operation: u64,
    finished: bool,
    workspace: RefCell<SealWorkspace>,
}
impl Drop for WriteOperation<'_, '_, '_> {
    fn drop(&mut self) {
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
    fn staged_build(&self) -> Result<Option<BuildState>> {
        self.scope.record.borrow().build_state()
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
        seal::mark(self.scope.session.write, &marker, None)?;
        Ok(())
    }
}
impl NativeSealView for WriteOperation<'_, '_, '_> {
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
        Ok(self.scope.session.row_commit_lsn.0)
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
        if existed {
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
                seal::mark(self.scope.session.write, &marker, previous)?;
                Ok(())
            })?;
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
            )?;
        }
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
        native.put(name.as_str()?, &header::key_bytes(key), &work.native_value)?;
        drop(record);
        self.scope.changed(key)
    }
    fn stage_build(&mut self, state: BuildState) -> Result<()> {
        self.check_cancel()?;
        self.scope
            .ensure_family(state.manifest.generation, &mut self.workspace.borrow_mut())?;
        let before = self.scope.record.borrow().build_state()?;
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
        let before = {
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
            record.serving = Some(manifest.into());
            before
        };
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
        self.finished = true;
        Ok(())
    }
}
