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
    pub(crate) fn from_image(
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
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit_vector_image_build_route(
        &self,
        key: &[u8],
        stored_bytes: usize,
    ) -> Result<Option<crate::memory_accounting::OwnedMemoryReservation>> {
        self.prepare_proof_owner()?;
        Self::admit_vector_image_build_route_with_owner(self.proof_accountant(), key, stored_bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn admit_vector_image_build_route_with_owner(
        accountant: Option<crate::memory_accounting::AccountantHandle>,
        key: &[u8],
        stored_bytes: usize,
    ) -> Result<Option<crate::memory_accounting::OwnedMemoryReservation>> {
        if stored_bytes != 0 && accountant.is_none() {
            return Err(corruption(
                "vector image record has no operation memory owner",
            ));
        }
        accountant
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    key.len()
                        .saturating_add(stored_bytes)
                        .saturating_mul(64)
                        .saturating_add(8192),
                    "vector",
                    "replace_vector_image_build",
                    "Raise MEMORY_LIMIT to reconcile vector construction with this image.",
                )
            })
            .transpose()
    }

    /// Writer serving readers capture the published writer snapshot.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn set_paged_root_capture<R: Into<WriterSnapshotHandle> + 'static>(
        &self,
        capture: Arc<dyn Fn() -> Result<R> + Send + Sync>,
        scoped_capture: ScopedWriterRootCapture,
    ) {
        let _ = self.scoped_vector_root_capture.set(scoped_capture);
        let capture: PagedRootCapture =
            Arc::new(move || CapturedPagedRoot::from_writer(capture()?.into()));
        let _ = self.paged_root_capture.set(capture);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn paged_root_capture(&self) -> Result<PagedRootCapture> {
        self.paged_root_capture
            .get()
            .cloned()
            .ok_or_else(|| Error::Other("paged vector serving has no captured writer root".into()))
    }

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

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn bind_pages_root(write: &redb::WriteTransaction) -> Result<()> {
    content_identity::bind_vector_pages_root_in_write(write)
}

/// The schema half of the pages-root binding, checked once when a read opens.
/// A store that never bound a pages root has no schema half to check.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn validate_schema_meta_root(read: &redb::ReadTransaction) -> Result<()> {
    if content_identity::vector_schema_meta_root_bound_in_read(read)?
        && !content_identity::vector_schema_meta_root_matches_in_read(read)?
    {
        return Err(corruption(
            "schema metadata physical root differs from its published binding",
        ));
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn validate_pages_root(read: &redb::ReadTransaction) -> Result<()> {
    let actual = content_identity::vector_schema_pages_root_in_read(read)?;
    let expected = content_identity::vector_pages_root_in_read(read)
        .map_err(|error| corruption(format!("pages root binding is invalid: {error}")))?;
    if actual != expected && !content_identity::vector_schema_meta_root_matches_in_read(read)? {
        return Err(corruption(
            "pages or schema physical root differs from its published binding",
        ));
    }
    Ok(())
}
