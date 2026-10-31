//! Captured storage roots shared by the native clustered-index readers.

use super::vector_membership_pages;
use super::*;

/// Frontier of an in-flight native build, per partition route; reloaded at
/// open so its snapshot floor is held again.
pub(super) const FLOORS: TableDefinition<&[u8], u64> = TableDefinition::new("vector_build_floors");

pub(crate) fn corruption(reason: impl Into<contextdb_core::error::DiagnosticText>) -> Error {
    Error::StoreCorrupted {
        path: "vector_paged_generation_pages".into(),
        reason: reason.into(),
    }
}

/// One captured storage root plus the owner that keeps it readable: the
/// writer's published snapshot, or a committed image's retained source.
pub(crate) type PagedRootCapture = Arc<dyn Fn() -> Result<CapturedPagedRoot> + Send + Sync>;

/// Authentic lifetime holder, kept typed before the old Any boundary. Sharing
/// these existing source Arcs never transfers their original invoice.
pub(crate) enum PagedRootHolder {
    Writer(WriterSnapshotHandle),
    Image(RetainedReadSource),
}
pub(crate) struct CapturedPagedRoot {
    pub(crate) read: Arc<redb::ReadTransaction>,
    pub(crate) holder: PagedRootHolder,
}
impl CapturedPagedRoot {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn from_writer(root: WriterSnapshotHandle) -> Result<Self> {
        let read = root.owner_read()?;
        Ok(Self {
            read,
            holder: PagedRootHolder::Writer(root),
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn from_image(
        read: Arc<redb::ReadTransaction>,
        source: RetainedReadSource,
    ) -> Result<Self> {
        let captured = Self {
            read,
            holder: PagedRootHolder::Image(source),
        };
        captured.authenticate()?;
        Ok(captured)
    }
    /// Compare against the holder's EXPECTED registered root, never an answer
    /// obtained by opening a new/latest read. No caller id mints this proof.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn authenticate(&self) -> Result<(redb::StorageSnapshotIdentity, SnapshotId, Lsn)> {
        let expected = match &self.holder {
            PagedRootHolder::Writer(root) => (root.storage_identity(), root.snapshot, root.lsn),
            PagedRootHolder::Image(source) => {
                if source.registration.snapshot.storage_snapshot != source.snapshot
                    || source.registration.registration.is_none()
                {
                    return Err(corruption(
                        "retained vector source has no authentic registration",
                    ));
                }
                (
                    source.snapshot,
                    source.logical_frontier,
                    source.graph_lsn_frontier,
                )
            }
        };
        if self.read.storage_snapshot_identity() != expected.0 {
            return Err(corruption(
                "retained vector read differs from its original holder",
            ));
        }
        Ok(expected)
    }
}

/// Only an active serving operation supplies this root. Maintenance may read
/// immutable candidate records before the next serving root is published.
pub(crate) type ScopedWriterRootCapture =
    Arc<dyn Fn() -> Result<Option<WriterSnapshotHandle>> + Send + Sync>;

impl CapturedPagedRoot {
    pub(crate) fn native_ivf_cache(&self) -> &super::vector_ivf::NativeIvfCache {
        match &self.holder {
            PagedRootHolder::Writer(root) => &root.native_ivf_cache,
            PagedRootHolder::Image(source) => &source.native_ivf_cache,
        }
    }
}

impl RedbPersistence {
    /// The first durable construction floor after `after`, in key order,
    /// from its own read.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn next_construction_floor_after(
        &self,
        after: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, VectorPartitionRef, TxId)>> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let floors = match read.open_table(FLOORS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(Self::storage_error(error)),
            };
            let mut range = match after {
                Some(after) => floors
                    .range::<&[u8]>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded)),
                None => floors.range::<&[u8]>(..),
            }
            .map_err(Self::storage_error)?;
            let Some(entry) = range.next() else {
                return Ok(None);
            };
            let (key, value) = entry.map_err(Self::storage_error)?;
            Ok(Some((
                key.value().to_vec(),
                vector_membership_pages::partition_from_prefix(key.value())?,
                TxId(value.value()),
            )))
        })
    }
}
