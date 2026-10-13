//! Durable raw membership access. Implementations own the pinned storage read,
//! authenticate each record when used, and reuse one admitted decode window.
//! No decoded directory is retained by the vector store.
use super::{DormantVectorLoadRequest, RawVectorDirectoryEntry, VectorGraphGeneration};
use crate::MemoryBudget;
use contextdb_core::{
    Error, Lsn, Result, RowId, SnapshotId, TxId, VectorIndexRef, VectorPartitionKey,
};

/// Catalog counts maintained atomically with membership publication. These
/// describe the current published frontier, not a reconstructed history map.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RawVectorMembershipSummary {
    pub entries: usize,
    pub live_entries: usize,
    pub max_tx: TxId,
}

/// Fixed inspection metadata authenticated at one captured operation root.
/// Resident cache charges remain separate from this durable publication view.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawVectorInspectionSnapshot {
    /// The captured physical root differs from the current writer publication.
    /// Its catalog descriptors and counts must remain paired.
    pub captured_catalog: bool,
    pub summary: RawVectorMembershipSummary,
    pub base: Option<VectorGraphGeneration>,
    pub change: Option<VectorGraphGeneration>,
    pub pending_inserts: usize,
    pub tombstones: usize,
    #[cfg(feature = "test-seams")]
    pub base_pending: Option<(usize, usize)>,
}

impl RawVectorInspectionSnapshot {
    #[doc(hidden)]
    pub fn new(
        captured_catalog: bool,
        summary: RawVectorMembershipSummary,
        base: Option<VectorGraphGeneration>,
        change: Option<VectorGraphGeneration>,
        pending_inserts: usize,
        tombstones: usize,
        base_pending: Option<(usize, usize)>,
    ) -> Self {
        #[cfg(not(feature = "test-seams"))]
        let _ = base_pending;
        Self {
            captured_catalog,
            summary,
            base,
            change,
            pending_inserts,
            tombstones,
            #[cfg(feature = "test-seams")]
            base_pending,
        }
    }
}

/// One membership change of a durable LSN-ordered change suffix.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawVectorSuffixChange {
    /// A membership was published; `identity` is its body identity as
    /// published, visible from `visible_from`.
    Opened {
        identity: RawVectorDirectoryEntry,
        visible_from: TxId,
    },
    /// The membership with this body identity closed at `deleted_tx`.
    Closed {
        row_id: RowId,
        created_tx: TxId,
        lsn: Lsn,
        deleted_tx: TxId,
    },
}

/// Opaque selected membership. Engines accept only their own private receipt,
/// which retains the physical operation root and exact forward record.
#[doc(hidden)]
pub trait RawVectorMembershipSelection: Send {
    fn identity(&self) -> RawVectorDirectoryEntry;
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Storage-neutral access to a partition's authoritative membership directory.
/// The engine supplies the root and record verification. Implementations must
/// never hydrate a partition to answer any of these methods.
/// The source and its raw-body loader must share the same pinned durable root.
/// Point/count operations admit transient scratch through the explicit executing owner and release it on
/// success or error. Before each physical record read, including invisible
/// intervals, they must call `before_read` to check cancellation and charge
/// statement work. A false result stops before that read and returns an error.
/// Catalog-only answers need no record callback. A source cannot replace its
/// root while a reader holds it.
#[doc(hidden)]
pub trait RawVectorMembershipSource: Send + Sync {
    /// Borrow retained metadata credits without resolving or copying a source.
    /// Callbacks must not call store or source operations, including reads;
    /// visitation holds store guards or the lazy source mutex.
    fn visit_retained_copy_credits(
        &self,
        _caller: &dyn MemoryBudget,
        _visit: &mut dyn FnMut(usize, usize, bool),
    ) {
    }

    /// Authenticate one serving selection on this operation's physical root.
    /// No default is permitted: every supplier must bind its root explicitly.
    fn capture_serving_selection(
        &self,
        snapshot: SnapshotId,
        frozen_base: Option<&VectorGraphGeneration>,
        frozen_change: Option<&VectorGraphGeneration>,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<super::CapturedVectorServingSelection>;

    /// Rebind a live writer source after its durable index keys are renamed.
    /// Captured cursors retain their original source and root.
    fn for_renamed_index(
        &self,
        index: &VectorIndexRef,
        owner: &mut super::VectorCopyAdmission,
        retained: super::VectorCopyAdmission,
    ) -> Result<std::sync::Arc<dyn RawVectorMembershipSource>> {
        let _ = (index, owner, retained);
        Err(Error::Other(
            "this membership source cannot prepare an index rename".into(),
        ))
    }

    /// Already-validated catalog metadata for the published frontier. This
    /// method performs no historical record reads or decoded allocations.
    fn published_summary(&self) -> RawVectorMembershipSummary;

    /// The maintained counts at the writer's current published root, read
    /// after a durable removal the store did not publish itself (retention,
    /// version cleanup, purge). A source pinned to one root returns its
    /// published summary.
    fn current_summary(
        &self,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<RawVectorMembershipSummary> {
        let _ = owner;
        Ok(self.published_summary())
    }

    /// Whether any retained membership is visible to one of `snapshots`,
    /// answered from a maintained occupancy-interval index with a fixed
    /// number of indexed lookups per snapshot and no membership-record read.
    /// `None` means this source keeps no such index; the caller then streams
    /// the directory.
    fn occupancy_visible_at_any(
        &self,
        snapshots: &[SnapshotId],
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<bool>> {
        let _ = (snapshots, owner);
        Ok(None)
    }

    /// Count visible intervals against the same root used by `open` and point
    /// probes. Older frontiers use indexed durable interval work. Verify every
    /// membership record used, including checksum and partition identity.
    fn visible_count(
        &self,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<usize>;

    fn supports_root_selection(&self) -> bool {
        false
    }

    fn select_visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<Box<dyn RawVectorMembershipSelection>>> {
        let _ = (row, snapshot, before_read, owner);
        Err(Error::Other(
            "this membership source has no root-bound selection".into(),
        ))
    }

    fn select_identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<Box<dyn RawVectorMembershipSelection>>> {
        let _ = (row, lsn, before_read, owner);
        Err(Error::Other(
            "this membership source has no root-bound selection".into(),
        ))
    }

    fn visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<RawVectorDirectoryEntry>>;

    /// Borrowed native reservation seam. Providers with already funded
    /// copies keep their existing visible lookup through this default.
    fn visible_by_row_with_copy_admission(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
        _admit: super::VectorCopyAdmissionCallback<'_>,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.visible_by_row(row, snapshot, before_read, owner)
    }

    fn contains_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<bool>;

    /// Resolve the exact retained row version within this partition's row
    /// prefix. Absence is authenticated by the pinned root; this must not
    /// scan unrelated rows or reconstruct a resident directory.
    fn identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<RawVectorDirectoryEntry>>;

    /// Whether commits already maintain authoritative pending summaries in
    /// durable metadata. Such sources need no resident membership event log.
    fn has_durable_pending_counts(&self) -> bool {
        false
    }

    /// Fixed durable metadata for the published serving frontier. No journal,
    /// directory or body reads are permitted; failure must reach inspection.
    fn pending_counts(
        &self,
        covered: Option<TxId>,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<(usize, usize)>> {
        let _ = (covered, owner);
        Ok(None)
    }

    /// Return counts and catalog metadata from one authorized operation root.
    /// When installation was refused, count against the actual serving frontier.
    /// Sources without a durable catalog keep the resident path.
    fn inspection_snapshot(
        &self,
        serving_covered: Option<TxId>,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<RawVectorInspectionSnapshot>> {
        let _ = (serving_covered, owner);
        Ok(None)
    }

    /// Live memberships published after `covered_lsn` and memberships closed
    /// after it, counted from a durable LSN-ordered change suffix without
    /// reading the covered memberships. `None` when this source keeps no such
    /// suffix.
    fn pending_after(
        &self,
        covered_lsn: Lsn,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<(usize, usize)>> {
        let _ = (covered_lsn, owner);
        Ok(None)
    }

    /// Visit every membership change after `after_lsn` in LSN order.
    /// `Ok(false)` when this source keeps no such suffix.
    fn visit_suffix(
        &self,
        after_lsn: Lsn,
        visit: &mut dyn FnMut(RawVectorSuffixChange) -> Result<()>,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<bool> {
        let _ = (after_lsn, visit, owner);
        Ok(false)
    }

    /// Visit every retained version of one row in this partition, as body
    /// identities, reading only that row's prefix.
    fn visit_row_versions(
        &self,
        row: RowId,
        before_read: &mut dyn FnMut() -> bool,
        visit: &mut dyn FnMut(RawVectorDirectoryEntry) -> Result<()>,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<()> {
        let _ = (row, before_read, visit, owner);
        Err(Error::Other(
            "this vector membership source cannot visit row versions".into(),
        ))
    }

    /// Every membership's body identity, in durable key order.
    fn visit_identities(
        &self,
        before_read: &mut dyn FnMut() -> bool,
        visit: &mut dyn FnMut(RawVectorDirectoryEntry) -> Result<()>,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<()> {
        let _ = (before_read, visit, owner);
        Err(Error::Other(
            "this vector membership source cannot list body identities".into(),
        ))
    }

    /// A root-pinned stream in full durable key order. Scratch is admitted
    /// before allocation through `request`. The caller owns the returned
    /// reservation until the cursor drops; the implementation must not also
    /// charge or release it through the memory accountant. Open errors drop
    /// their allocations before returning so the caller can release charges.
    fn open(
        &self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Box<dyn RawVectorMembershipCursor>>;
}

#[doc(hidden)]
pub trait RawVectorMembershipCursor: Send {
    fn selected_receipt(&self) -> Option<&dyn std::any::Any> {
        None
    }

    /// `open` reserves the initial decode window before returning. Advances
    /// reuse it and request admission before decoding a larger record. Refused
    /// growth preserves the same record for retry under the captured root.
    ///
    /// Return one authenticated interval, including intervals invisible at the
    /// statement snapshot so physical work and cancellation stay observable.
    /// `None` is terminal. An error must leave the last successful position
    /// unchanged, allowing a suspended statement to resume the same record.
    fn next(
        &mut self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>>;

    /// Return the visibility interval and its authenticated body identity.
    /// A row-only publication can start later than the unchanged vector body.
    fn next_with_body_identity(
        &mut self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Option<(RawVectorDirectoryEntry, RawVectorDirectoryEntry)>> {
        self.next(request)
            .map(|entry| entry.map(|entry| (entry.clone(), entry)))
    }
}

impl std::fmt::Debug for dyn RawVectorMembershipCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawVectorMembershipCursor")
            .finish_non_exhaustive()
    }
}

/// Column-scoped durable row reverse directory. Point probes seek the row's
/// prefix, never scan partition catalogs. The root must match the partition
/// membership sources installed for the column. Scratch is admitted by the
/// implementation before allocation and released on success/error/cursor drop.
/// Every physical record read calls `before_read`, including excluded history.
/// A false callback stops before the read and returns an error.
#[doc(hidden)]
pub trait RawVectorReverseMembershipSource: Send + Sync {
    /// Borrow retained metadata credits without resolving or copying a source.
    /// Callbacks must not call store or source operations, including reads;
    /// visitation holds store guards or the lazy source mutex.
    fn visit_retained_copy_credits(
        &self,
        _caller: &dyn MemoryBudget,
        _visit: &mut dyn FnMut(usize, usize, bool),
    ) {
    }

    /// Rebind a live writer source after its durable index keys are renamed.
    /// Captured cursors retain their original source and root.
    fn for_renamed_index(
        &self,
        index: &VectorIndexRef,
        owner: &mut super::VectorCopyAdmission,
        retained: super::VectorCopyAdmission,
    ) -> Result<std::sync::Arc<dyn RawVectorReverseMembershipSource>> {
        let _ = (index, owner, retained);
        Err(Error::Other(
            "this membership source cannot prepare an index rename".into(),
        ))
    }

    /// Resolve a point route with the executing copy owner. Admit route scratch
    /// before allocating it; the caller retains the charge until the key dies.
    fn visible_partition_admitted(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<Option<VectorPartitionKey>>;

    /// Stream only this row's retained partition identities, current first.
    /// No population-sized row/partition projection may be retained.
    fn open_row(
        &self,
        row: RowId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<Box<dyn RawVectorReverseMembershipCursor>>;
}

#[doc(hidden)]
pub trait RawVectorReverseMembershipCursor: Send {
    fn next(
        &mut self,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut crate::VectorCopyAdmission,
    ) -> Result<Option<VectorPartitionKey>>;
}

/// Bridge a storage callback to the statement's typed checkpoint/work error.
/// Storage's interruption error must never hide the caller's actual refusal.
pub(super) fn with_membership_read<E, T>(
    before_read: &mut impl FnMut() -> std::result::Result<(), E>,
    read: impl FnOnce(&mut dyn FnMut() -> bool) -> Result<T>,
) -> std::result::Result<T, E>
where
    E: From<Error>,
{
    let mut interrupted = None;
    let result = read(&mut || {
        if interrupted.is_some() {
            return false;
        }
        match before_read() {
            Ok(()) => true,
            Err(error) => {
                interrupted = Some(error);
                false
            }
        }
    });
    match interrupted {
        Some(error) => Err(error),
        None => result.map_err(E::from),
    }
}

/// Opens one authenticated partition membership source on first use. Its root
/// is fixed when the factory is captured; opening must not enumerate records
/// or retain decoded membership directories. The factory admits any transient
/// verification scratch itself and calls before_read for every physical read.
#[doc(hidden)]
pub trait RawVectorMembershipFactory: Send + Sync {
    fn supports_root_selection(&self) -> bool {
        false
    }

    fn load(
        &self,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<std::sync::Arc<dyn RawVectorMembershipSource>>;
}

/// A catalog summary and a lazy, evictable source handle. Registration reads
/// no membership record. Resolving a handle verifies its published summary
/// against the captured catalog before any record can be consumed.
#[doc(hidden)]
pub struct LazyRawVectorMembershipSource {
    summary: RawVectorMembershipSummary,
    factory: std::sync::Arc<dyn RawVectorMembershipFactory>,
    source: parking_lot::Mutex<LazyMembershipSlot>,
}
#[derive(Default)]
struct LazyMembershipSlot {
    source: Option<std::sync::Arc<dyn RawVectorMembershipSource>>,
    epoch: u64,
}
impl LazyRawVectorMembershipSource {
    pub fn new(
        summary: RawVectorMembershipSummary,
        factory: std::sync::Arc<dyn RawVectorMembershipFactory>,
    ) -> Result<Self> {
        if summary.live_entries > summary.entries {
            return Err(Error::Other(
                "lazy vector membership catalog count is invalid".into(),
            ));
        }
        Ok(Self {
            summary,
            factory,
            source: parking_lot::Mutex::new(LazyMembershipSlot::default()),
        })
    }
    fn resolve(
        &self,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<std::sync::Arc<dyn RawVectorMembershipSource>> {
        let epoch = {
            let slot = self.source.lock();
            if let Some(source) = slot.source.as_ref() {
                return Ok(std::sync::Arc::clone(source));
            }
            slot.epoch
        };
        // Admission and physical reads may evict an idle handle, including
        // this wrapper. No handle mutex is held across that callback.
        let source = self.factory.load(before_read)?;
        if source.published_summary() != self.summary {
            return Err(Error::Other(
                "lazy vector membership source disagrees with its captured catalog".into(),
            ));
        }
        let mut slot = self.source.lock();
        if let Some(current) = slot.source.as_ref() {
            return Ok(std::sync::Arc::clone(current));
        }
        if slot.epoch == epoch {
            slot.source = Some(std::sync::Arc::clone(&source));
        }
        // An eviction that raced this load wins installation. The operation
        // may still use its own verified captured-root handle.
        Ok(source)
    }
    /// Drop only this idle handle. Open cursors/readers preserve their own
    /// source root and reservations; the next operation verifies a new handle.
    pub fn evict_idle_handle(&self) -> bool {
        let removed = {
            let mut slot = self.source.lock();
            slot.epoch = slot.epoch.wrapping_add(1);
            slot.source.take()
        };
        let existed = removed.is_some();
        drop(removed);
        existed
    }
}
impl RawVectorMembershipSource for LazyRawVectorMembershipSource {
    // Nonreentrant: even source reads can reacquire this held mutex.
    fn visit_retained_copy_credits(
        &self,
        caller: &dyn MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        let slot = self.source.lock();
        if let Some(source) = slot.source.as_ref() {
            source.visit_retained_copy_credits(caller, visit);
        }
    }

    fn capture_serving_selection(
        &self,
        snapshot: SnapshotId,
        frozen_base: Option<&VectorGraphGeneration>,
        frozen_change: Option<&VectorGraphGeneration>,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<super::CapturedVectorServingSelection> {
        self.resolve(&mut || true)?.capture_serving_selection(
            snapshot,
            frozen_base,
            frozen_change,
            owner,
        )
    }

    fn supports_root_selection(&self) -> bool {
        self.source.lock().source.as_ref().map_or_else(
            || self.factory.supports_root_selection(),
            |source| source.supports_root_selection(),
        )
    }
    fn select_visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<Box<dyn RawVectorMembershipSelection>>> {
        self.resolve(before_read)?
            .select_visible_by_row(row, snapshot, before_read, owner)
    }
    fn select_identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<Box<dyn RawVectorMembershipSelection>>> {
        self.resolve(before_read)?
            .select_identity_by_row_lsn(row, lsn, before_read, owner)
    }

    fn published_summary(&self) -> RawVectorMembershipSummary {
        self.summary
    }
    fn visible_count(
        &self,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<usize> {
        self.resolve(before_read)?
            .visible_count(snapshot, before_read, owner)
    }
    fn visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.resolve(before_read)?
            .visible_by_row(row, snapshot, before_read, owner)
    }
    fn visible_by_row_with_copy_admission(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
        admit: &mut dyn FnMut(
            &mut super::VectorCopyAdmission,
            usize,
            &str,
            &str,
            &str,
        ) -> Result<()>,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.resolve(before_read)?
            .visible_by_row_with_copy_admission(row, snapshot, before_read, owner, admit)
    }
    fn contains_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<bool> {
        self.resolve(before_read)?
            .contains_row_lsn(row, lsn, before_read, owner)
    }
    fn identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
        owner: &mut super::VectorCopyAdmission,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.resolve(before_read)?
            .identity_by_row_lsn(row, lsn, before_read, owner)
    }
    fn open(
        &self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Box<dyn RawVectorMembershipCursor>> {
        self.resolve(&mut || request(DormantVectorLoadRequest::Checkpoint))?
            .open(request)
    }
}
