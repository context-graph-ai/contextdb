//! Cold committed source foundation over the actual retained writer root.
//!
//! No decoded cache, resident version map, fresh read transaction or committed
//! body invoice lives here. The legacy `_unqualified` cursors retain their
//! original engine key admission but require lower storage qualification.
//! The earlier zero-owner decode/JSON bootstrap and typed seeds are excluded.
//! They are not production entry points and cannot certify native storage.
use super::*;

/// Borrow the published descriptor; the private handle is the same immutable
/// redb transaction already retained by that descriptor, never a reopened root.
pub(crate) struct ColdCommittedProvider<'root> {
    root: &'root WriterSnapshot,
    read: Arc<redb::ReadTransaction>,
}

impl<'root> ColdCommittedProvider<'root> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn capture(root: &'root WriterSnapshot) -> Result<Self> {
        Ok(Self {
            root,
            read: root.owner_read()?,
        })
    }
}

impl ColdCommittedProvider<'_> {}

#[cfg(feature = "sync-orchestration")]
impl ColdCommittedProvider<'_> {
    /// Descriptor of THIS retained transaction, paid by the original request.
    /// Existing store-funded descriptors and bootstrap are unchanged.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn native_read_metadata(
        &self,
        owner: &contextdb_core::read_memory::ReadMemoryOwner,
    ) -> std::result::Result<
        contextdb_core::read_memory::ReadShared<
            super::super::native_storage_admission::ColdSnapshotMetadata,
        >,
        super::super::native_storage_admission::NativeStorageFailure,
    > {
        use super::super::native_storage_admission::{ColdSnapshotMetadata, NativeStorageFailure};
        use contextdb_core::read_memory::{ReadCredit, ReadShared};
        let invoice = ReadCredit::try_new_owned_native(
            owner.clone(),
            ReadShared::<ColdSnapshotMetadata>::layout().size(),
            "writer_source",
            "config_metadata",
            HINT,
        )
        .map_err(NativeStorageFailure::Refused)?;
        ReadShared::try_from_prepaid_native(
            ColdSnapshotMetadata {
                identity: self.read.storage_snapshot_identity(),
                snapshot: self.root.snapshot,
                lsn: self.root.lsn,
            },
            invoice,
        )
        .map_err(NativeStorageFailure::Refused)
    }

    /// Exact config lookup in the same cold transaction as the original cursor.
    /// The scalar storage refusal is extracted BEFORE any callback/body error.
    /// Text-table/cache/backend/erasure errors retain typed Unqualified egress.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn lend_native_read<T, E>(
        &self,
        scope: &super::super::native_storage_admission::StorageAdmissionScope,
        metadata: &super::super::native_storage_admission::ColdSnapshotMetadata,
        key: &str,
        before: &mut dyn FnMut() -> Result<()>,
        visit: impl FnOnce(&[u8]) -> std::result::Result<T, E>,
    ) -> std::result::Result<
        std::result::Result<std::result::Result<Option<T>, E>, redb::NativeColdReadError>,
        super::super::native_storage_admission::NativeStorageFailure,
    > {
        scope.run_native(|| {
            before()?;
            if metadata.identity != self.read.storage_snapshot_identity()
                || metadata.snapshot != self.root.snapshot
                || metadata.lsn != self.root.lsn
            {
                return Err(Error::StoreCorrupted {
                    path: "writer source".into(),
                    reason: "config metadata differs from its retained committed view".into(),
                });
            }
            let erasure = self.root.erasure.read();
            erasure.check_scope(None, CONFIG_TABLE.name())?;
            match self.read.get_native_text_value(CONFIG_TABLE.name(), key) {
                Ok(Some(value)) => {
                    before()?;
                    erasure.check_key(CONFIG_TABLE.name(), key.as_bytes())?;
                    Ok(Ok(visit(value.value()).map(Some)))
                }
                Ok(None) | Err(redb::NativeColdReadError::TableAbsent) => Ok(Ok(Ok(None))),
                Err(fault) => Ok(Err(fault)),
            }
            // Native complete-page checksum validation and actual page/control
            // admission stay in the existing cold storage kernel. No ordinary
            // open_table, copied TypeName, cached config or new read is used.
        })
    }
}

/// Owned private range source retaining the actual captured read transaction.
/// No new provider/request/read transaction is acquired between cursor steps.
#[cfg(feature = "sync-orchestration")]
pub(crate) struct NativeConfigRangeSource {
    read: Arc<redb::ReadTransaction>,
    root: WriterSnapshotHandle,
}
#[cfg(feature = "sync-orchestration")]
impl ColdCommittedProvider<'_> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn into_native_config_range(
        self,
        root: WriterSnapshotHandle,
    ) -> std::result::Result<NativeConfigRangeSource, super::NativeConfigFailure> {
        if !std::ptr::eq(self.root, root.as_ref()) {
            return Err(super::NativeConfigFailure::OwnerMismatch);
        }
        Ok(NativeConfigRangeSource {
            root,
            read: self.read,
        })
    }
}
#[cfg(feature = "sync-orchestration")]
impl NativeConfigRangeSource {
    pub(crate) fn root(&self) -> &WriterSnapshotHandle {
        &self.root
    }

    /// Actual CONFIG_TABLE range producer. The callback receives only actual
    /// immutable page guards; refusal extraction precedes callback/cold errors.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn lend_next_native_config<T, E>(
        &self,
        scope: &super::super::native_storage_admission::StorageAdmissionScope,
        metadata: &super::super::native_storage_admission::ColdSnapshotMetadata,
        lower: &[u8],
        exclusive: bool,
        upper: Option<&[u8]>,
        before: &mut dyn FnMut() -> Result<()>,
        visit: impl FnOnce(
            redb::AccessGuard<'static, &'static [u8]>,
            redb::AccessGuard<'static, &'static [u8]>,
        ) -> std::result::Result<T, E>,
    ) -> std::result::Result<
        std::result::Result<std::result::Result<Option<T>, E>, redb::NativeColdReadError>,
        super::super::native_storage_admission::NativeStorageFailure,
    > {
        scope.run_native(|| {
            before()?;
            if metadata.identity != self.read.storage_snapshot_identity()
                || metadata.snapshot != self.root.snapshot
                || metadata.lsn != self.root.lsn
            {
                return Err(Error::StoreCorrupted {
                    path: "writer source".into(),
                    reason: "raw config metadata differs from retained committed view".into(),
                });
            }
            let erasure = self.root.erasure.read_recursive();
            erasure.check_scope(None, CONFIG_TABLE.name())?;
            let mut checkpoint_fault = None;
            let next = self.read.next_native_text_range(
                CONFIG_TABLE.name(),
                lower,
                exclusive,
                upper,
                &mut || match before() {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        checkpoint_fault = Some(error);
                        // Inline lower interruption is only a stop signal. The
                        // actual engine checkpoint error remains Unqualified.
                        Err(redb::NativeColdReadError::Io(
                            std::io::ErrorKind::Interrupted.into(),
                        ))
                    }
                },
            );
            if let Some(error) = checkpoint_fault {
                return Err(error);
            }
            match next {
                Ok(Some((key, value))) => {
                    before()?;
                    erasure.check_key(CONFIG_TABLE.name(), key.value())?;
                    Ok(Ok(visit(key, value).map(Some)))
                }
                Ok(None) => {
                    before()?;
                    Ok(Ok(Ok(None)))
                }
                Err(fault) => Ok(Err(fault)),
            }
            // Erasure fence spans callback and raw page construction.
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn lend_next_native_group<T, E>(
        &self,
        scope: &super::super::native_storage_admission::StorageAdmissionScope,
        metadata: &super::super::native_storage_admission::ColdSnapshotMetadata,
        lower: &[u8],
        exclusive: bool,
        upper: Option<&[u8]>,
        before: &mut dyn FnMut() -> Result<()>,
        visit: impl FnOnce(
            redb::AccessGuard<'static, &'static [u8]>,
            redb::AccessGuard<'static, &'static [u8]>,
        ) -> std::result::Result<T, E>,
    ) -> std::result::Result<
        std::result::Result<std::result::Result<Option<T>, E>, redb::NativeColdReadError>,
        super::super::native_storage_admission::NativeStorageFailure,
    > {
        scope.run_native(|| {
            before()?;
            if metadata.identity != self.read.storage_snapshot_identity()
                || metadata.snapshot != self.root.snapshot
                || metadata.lsn != self.root.lsn
            {
                return Err(Error::StoreCorrupted {
                    path: "writer source".into(),
                    reason: "raw config metadata differs from retained committed view".into(),
                });
            }
            let erasure = self.root.erasure.read_recursive();
            erasure.check_scope(None, CONFIG_TABLE.name())?;
            let mut checkpoint_fault = None;
            let next = self.read.next_native_byte_range(
                super::super::custody_groups::GROUP_TABLE.name(),
                lower,
                exclusive,
                upper,
                &mut || match before() {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        checkpoint_fault = Some(error);
                        // Inline lower interruption is only a stop signal. The
                        // actual engine checkpoint error remains Unqualified.
                        Err(redb::NativeColdReadError::Io(
                            std::io::ErrorKind::Interrupted.into(),
                        ))
                    }
                },
            );
            if let Some(error) = checkpoint_fault {
                return Err(error);
            }
            match next {
                Ok(Some((key, value))) => {
                    before()?;
                    erasure.check_key(CONFIG_TABLE.name(), key.value())?;
                    Ok(Ok(visit(key, value).map(Some)))
                }
                Ok(None) => {
                    before()?;
                    Ok(Ok(Ok(None)))
                }
                Err(fault) => Ok(Err(fault)),
            }
            // Erasure fence spans callback and raw page construction.
        })
    }
}

// The existing native committed-root path owns this private diagnostic.
#[cfg(all(feature = "sync-orchestration", any(test, feature = "test-seams")))]
#[path = "vector_physical_observation.rs"]
pub(crate) mod physical_vector;

use crate::persistence::writer_source::WriterSnapshotHandle;
