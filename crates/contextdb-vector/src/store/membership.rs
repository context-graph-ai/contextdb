//! Durable raw membership access. Implementations own the pinned storage read,
//! authenticate each record when used, and reuse one admitted decode window.
//! No decoded directory is retained by the vector store.
use super::{DormantVectorLoadRequest, RawVectorDirectoryEntry};
use contextdb_core::{Error, Lsn, Result, RowId, SnapshotId, TxId, VectorPartitionKey};

/// Catalog counts maintained atomically with membership publication. These
/// describe the current published frontier, not a reconstructed history map.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RawVectorMembershipSummary {
    pub entries: usize,
    pub live_entries: usize,
    pub max_tx: TxId,
}

/// Storage-neutral access to a partition's authoritative membership directory.
/// The engine supplies the root and record verification. Implementations must
/// never hydrate a partition to answer any of these methods.
/// The source and its raw-body loader must share the same pinned durable root.
/// Point/count operations admit their own transient scratch and release it on
/// success or error. Before each physical record read, including invisible
/// intervals, they must call `before_read` to check cancellation and charge
/// statement work. A false result stops before that read and returns an error.
/// Catalog-only answers need no record callback. A source cannot replace its
/// root while a reader holds it.
#[doc(hidden)]
pub trait RawVectorMembershipSource: Send + Sync {
    /// Already-validated catalog metadata for the published frontier. This
    /// method performs no historical record reads or decoded allocations.
    fn published_summary(&self) -> RawVectorMembershipSummary;

    /// Count visible intervals against the same root used by `open` and point
    /// probes. Older frontiers use indexed durable interval work. Verify every
    /// membership record used, including checksum and partition identity.
    fn visible_count(
        &self,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<usize>;

    fn visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>>;

    fn contains_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<bool>;

    /// Resolve the exact retained row version within this partition's row
    /// prefix. Absence is authenticated by the pinned root; this must not
    /// scan unrelated rows or reconstruct a resident directory.
    fn identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>>;

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
    /// `open` reserves the complete fixed decode window before returning.
    /// Advances reuse it; they may request checkpoints but no further reserve.
    ///
    /// Return one authenticated interval, including intervals invisible at the
    /// statement snapshot so physical work and cancellation stay observable.
    /// `None` is terminal. An error must leave the last successful position
    /// unchanged, allowing a suspended statement to resume the same record.
    fn next(
        &mut self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>>;
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
    fn visible_partition(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<Option<VectorPartitionKey>>;

    /// Stream only this row's retained partition identities, current first.
    /// No population-sized row/partition projection may be retained.
    fn open_row(
        &self,
        row: RowId,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<Box<dyn RawVectorReverseMembershipCursor>>;
}

#[doc(hidden)]
pub trait RawVectorReverseMembershipCursor: Send {
    fn next(&mut self, before_read: &mut dyn FnMut() -> bool)
    -> Result<Option<VectorPartitionKey>>;
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
    fn published_summary(&self) -> RawVectorMembershipSummary {
        self.summary
    }
    fn visible_count(
        &self,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<usize> {
        self.resolve(before_read)?
            .visible_count(snapshot, before_read)
    }
    fn visible_by_row(
        &self,
        row: RowId,
        snapshot: SnapshotId,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.resolve(before_read)?
            .visible_by_row(row, snapshot, before_read)
    }
    fn contains_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<bool> {
        self.resolve(before_read)?
            .contains_row_lsn(row, lsn, before_read)
    }
    fn identity_by_row_lsn(
        &self,
        row: RowId,
        lsn: Lsn,
        before_read: &mut dyn FnMut() -> bool,
    ) -> Result<Option<RawVectorDirectoryEntry>> {
        self.resolve(before_read)?
            .identity_by_row_lsn(row, lsn, before_read)
    }
    fn open(
        &self,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<Box<dyn RawVectorMembershipCursor>> {
        self.resolve(&mut || request(DormantVectorLoadRequest::Checkpoint))?
            .open(request)
    }
}
