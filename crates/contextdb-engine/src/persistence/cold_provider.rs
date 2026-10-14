//! Cold committed source foundation over the actual retained writer root.
//!
//! No decoded cache, resident version map, fresh read transaction or committed
//! body invoice lives here. The legacy `_unqualified` cursors retain their
//! original engine key admission but require lower storage qualification.
//! The earlier zero-owner decode/JSON bootstrap and typed seeds are excluded.
//! They are not production entry points and cannot certify native storage.
use super::*;
use crate::memory_accounting::{
    copy_owned_native_slice, reserve_preparation_from_original, validate_owned_original,
    writer_adapter_type_layout,
};

/// Private identity captured from the retained view. It cannot mint a root,
/// a publication capability or a durable commit outcome.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColdRootIdentity(redb::StorageSnapshotIdentity);

/// Borrow the published descriptor; the private handle is the same immutable
/// redb transaction already retained by that descriptor, never a reopened root.
pub(crate) struct ColdCommittedProvider<'root> {
    root: &'root WriterSnapshot,
    read: Arc<redb::ReadTransaction>,
}

impl<'root> ColdCommittedProvider<'root> {
    pub(crate) fn capture(root: &'root WriterSnapshot) -> Result<Self> {
        Ok(Self {
            root,
            read: root.owner_read()?,
        })
    }

    pub(crate) fn identity(&self) -> ColdRootIdentity {
        ColdRootIdentity(self.read.storage_snapshot_identity())
    }

    fn validate_original(
        &self,
        original: &OwnedMemoryReservation,
        funder: &contextdb_tx::SourceFunder,
    ) -> Result<()> {
        validate_owned_original(original, &self.root.accountant, funder)
    }

    pub(crate) fn commits(
        &self,
        lower: Bound<Lsn>,
        upper: Bound<Lsn>,
    ) -> super::super::writer_commit_directory::ColdCommitCursor<'_> {
        super::super::writer_commit_directory::ColdCommitCursor::new(
            self.read.as_ref(),
            lower,
            upper,
        )
    }

    pub(crate) fn commit_floor_unqualified(
        &self,
        upper: Bound<Lsn>,
    ) -> Result<Option<(Lsn, TxId)>> {
        super::super::writer_commit_directory::cold_commit_floor(self.read.as_ref(), upper)
    }

    /// Count and end-key bounds only; no directory or body scan at startup.
    pub(crate) fn commit_bounds_unqualified(
        &self,
    ) -> Result<super::super::writer_commit_directory::ColdCommitBounds> {
        super::super::writer_commit_directory::cold_commit_bounds(self.read.as_ref())
    }

    /// End-key startup probes in the captured root, with no payload decode.
    /// Recovery authentication and lower storage admission remain unqualified.
    pub(crate) fn open_bounds_unqualified(
        &self,
        original: &OwnedMemoryReservation,
        funder: &contextdb_tx::SourceFunder,
    ) -> Result<WriterOpenBounds> {
        self.validate_original(original, funder)?;
        let recovery = RedbPersistence::read_writer_recovery_bounds_in_read(self.read.as_ref())?;
        let directory = self.commit_bounds_unqualified()?;
        if recovery.commit_count != directory.count {
            return Err(Error::StoreCorrupted {
                path: "writer_recovery".into(),
                reason: "cold directory count differs from recovery bounds".into(),
            });
        }
        let mut max_row_id = RowId(recovery.external_row_id_floor);
        for (logical, _) in self.root.captured_table_metadata() {
            let name = OriginalBuffer::join(original, &[b"rel_", logical.as_bytes()])?;
            let name = std::str::from_utf8(&name.bytes).expect("captured schema name is UTF-8");
            let table = self
                .read
                .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                .map_err(RedbPersistence::storage_error)?;
            if let Some((key, _)) = table.last().map_err(RedbPersistence::storage_error)? {
                let bytes = key.value();
                if bytes.len() != 24 {
                    return Err(Error::StoreCorrupted {
                        path: "relational row".into(),
                        reason: "cold startup row key has wrong width".into(),
                    });
                }
                let row_id = RowId(u64::from_be_bytes(
                    bytes[..8].try_into().expect("checked row key width"),
                ));
                max_row_id = max_row_id.max(row_id);
            }
        }
        Ok(WriterOpenBounds {
            max_row_id,
            max_tx: TxId(recovery.identity_tx_floor)
                .max(directory.last.map_or(TxId(0), |(_, tx)| tx)),
            max_lsn: Lsn(recovery.source_lsn_floor)
                .max(directory.last.map_or(Lsn(0), |(lsn, _)| lsn)),
            commit_first: directory.first,
            commit_last: directory.last,
            commit_count: directory.count,
        })
    }

    pub(crate) fn row_cursor_unqualified<'source, 'original>(
        &'source self,
        original: &'original OwnedMemoryReservation,
        funder: &contextdb_tx::SourceFunder,
        table: &'source str,
        row_id: RowId,
    ) -> Result<ColdByteCursor<'source, 'root, 'original>> {
        self.validate_original(original, funder)?;
        let name = OriginalBuffer::join(original, &[b"rel_", table.as_bytes()])?;
        let prefix = row_id.0.to_be_bytes();
        let lower = Bound::Included(OriginalBuffer::copy(original, &prefix)?);
        let upper = OriginalBuffer::prefix_end(original, &prefix)?
            .map_or(Bound::Unbounded, Bound::Excluded);
        Ok(ColdByteCursor {
            source: self,
            original,
            name,
            logical: Some(table),
            lower,
            upper,
            after: None,
            reverse: true,
            finished: false,
        })
    }

    pub(crate) fn exact_row_unqualified<'source, 'original>(
        &'source self,
        original: &'original OwnedMemoryReservation,
        funder: &contextdb_tx::SourceFunder,
        table: &'source str,
        key: &[u8; 24],
    ) -> Result<ColdByteCursor<'source, 'root, 'original>> {
        self.validate_original(original, funder)?;
        Ok(ColdByteCursor {
            source: self,
            original,
            name: OriginalBuffer::join(original, &[b"rel_", table.as_bytes()])?,
            logical: Some(table),
            lower: Bound::Included(OriginalBuffer::copy(original, key)?),
            upper: Bound::Included(OriginalBuffer::copy(original, key)?),
            after: None,
            reverse: false,
            finished: false,
        })
    }

    pub(crate) fn graph_cursor_unqualified<'source, 'original>(
        &'source self,
        original: &'original OwnedMemoryReservation,
        funder: &contextdb_tx::SourceFunder,
        node: NodeId,
        reverse: bool,
    ) -> Result<ColdByteCursor<'source, 'root, 'original>> {
        self.validate_original(original, funder)?;
        let name = if reverse {
            GRAPH_REV_TABLE.name()
        } else {
            GRAPH_FWD_TABLE.name()
        };
        let prefix = node.as_bytes();
        Ok(ColdByteCursor {
            source: self,
            original,
            name: OriginalBuffer::copy(original, name.as_bytes())?,
            logical: None,
            lower: Bound::Included(OriginalBuffer::copy(original, prefix)?),
            upper: OriginalBuffer::prefix_end(original, prefix)?
                .map_or(Bound::Unbounded, Bound::Excluded),
            after: None,
            reverse: false,
            finished: false,
        })
    }
}

/// Actual heap bytes are freed before their actual original is refunded.
struct OriginalBuffer {
    bytes: Vec<u8>,
    _allocation: OwnedMemoryReservation,
}
impl OriginalBuffer {
    fn copy(original: &OwnedMemoryReservation, bytes: &[u8]) -> Result<Self> {
        let (bytes, allocation) = copy_owned_native_slice(original, bytes)?;
        Ok(Self {
            bytes,
            _allocation: allocation,
        })
    }
    fn join(original: &OwnedMemoryReservation, parts: &[&[u8]]) -> Result<Self> {
        let len = parts
            .iter()
            .try_fold(0usize, |n, part| n.checked_add(part.len()))
            .ok_or_else(|| Error::Other("cold key size overflow".into()))?;
        let (bytes, allocation) =
            crate::memory_accounting::new_owned_native_vec::<u8>(original, len)?;
        let mut result = Self {
            bytes,
            _allocation: allocation,
        };
        for part in parts {
            result.bytes.extend_from_slice(part);
        }
        Ok(result)
    }
    fn prefix_end(original: &OwnedMemoryReservation, prefix: &[u8]) -> Result<Option<Self>> {
        let Some(last) = prefix.iter().rposition(|byte| *byte != u8::MAX) else {
            return Ok(None);
        };
        let mut result = Self::copy(original, &prefix[..=last])?;
        result.bytes[last] += 1;
        Ok(Some(result))
    }
}

/// Only names, endpoints and one resume key are retained. Each key is admitted
/// by the authentic original before allocation; no payload lives on the cursor.
pub(crate) struct ColdByteCursor<'source, 'root, 'original> {
    source: &'source ColdCommittedProvider<'root>,
    original: &'original OwnedMemoryReservation,
    name: OriginalBuffer,
    logical: Option<&'source str>,
    lower: Bound<OriginalBuffer>,
    upper: Bound<OriginalBuffer>,
    after: Option<OriginalBuffer>,
    reverse: bool,
    finished: bool,
}
impl ColdByteCursor<'_, '_, '_> {
    /// Lent guards cannot escape this synchronous callback. Caller-owned typed
    /// decode/proof is still required for rows and graph lifecycle records.
    /// Failed cancellation, admission or callback leaves the resume key intact.
    pub(crate) fn lend_next_unqualified<T>(
        &mut self,
        before: &mut dyn FnMut() -> Result<()>,
        visit: impl FnOnce(&[u8], &[u8]) -> Result<T>,
    ) -> Result<Option<T>> {
        if self.finished {
            return Ok(None);
        }
        before()?;
        let name = std::str::from_utf8(&self.name.bytes)
            .map_err(|_| Error::Other("cold physical name is not UTF-8".into()))?;
        let erasure = self.source.root.erasure.read();
        erasure.check_scope(self.logical, name)?;
        let table = self
            .source
            .read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
            .map_err(RedbPersistence::storage_error)?;
        let mut lower = self.lower.as_ref().map(|v| v.bytes.as_slice());
        let mut upper = self.upper.as_ref().map(|v| v.bytes.as_slice());
        if let Some(after) = &self.after {
            if self.reverse {
                upper = Bound::Excluded(after.bytes.as_slice());
            } else {
                lower = Bound::Excluded(after.bytes.as_slice());
            }
        }
        let empty = match (&lower, &upper) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
                a >= b
            }
            _ => false,
        };
        if empty {
            self.finished = true;
            return Ok(None);
        }
        let mut range = table
            .range::<&[u8]>((lower, upper))
            .map_err(RedbPersistence::storage_error)?;
        let next = if self.reverse {
            range.next_back()
        } else {
            range.next()
        };
        let Some((key, body)) = next.transpose().map_err(RedbPersistence::storage_error)? else {
            self.finished = true;
            return Ok(None);
        };
        erasure.check_key(name, key.value())?;
        let resume = OriginalBuffer::copy(self.original, key.value())?;
        before()?;
        let result = visit(key.value(), body.value())?;
        // No callback occurs after advancing; errors and unwind above leave
        // the previous key in place. Deallocate old key before refunding it.
        let prior = self.after.replace(resume);
        drop(prior);
        Ok(Some(result))
    }
}

/// Prepared control only. No durable marker can be minted by this module.
/// Actual metadata originals continue retaining WriterUse through physical free.
pub(crate) struct PreparedColdSuccessor<'p, 'o> {
    persistence: &'p RedbPersistence,
    original: &'o OwnedMemoryReservation,
    funder: &'o contextdb_tx::SourceFunder,
    predecessor: ColdRootIdentity,
    snapshot: SnapshotId,
    lsn: Lsn,
    _control: OwnedMemoryReservation,
}
impl RedbPersistence {
    pub(crate) fn prepare_cold_successor<'p, 'o>(
        &'p self,
        original: &'o OwnedMemoryReservation,
        funder: &'o contextdb_tx::SourceFunder,
        predecessor: &ColdCommittedProvider<'_>,
        snapshot: SnapshotId,
        lsn: Lsn,
    ) -> Result<PreparedColdSuccessor<'p, 'o>> {
        validate_owned_original(original, &predecessor.root.accountant, funder)?;
        let accountant = self.proof_accountant().ok_or_else(|| {
            Error::Other("cold successor requires its attached accountant".into())
        })?;
        validate_owned_original(original, &accountant, funder)?;
        // Pinned std Arc header followed by its aligned WriterSnapshot payload.
        // This reserves only that concrete allocation, never estimates redb work.
        let layout = writer_adapter_type_layout::<WriterSnapshot>();
        let control = reserve_preparation_from_original(
            original,
            layout.size(),
            "writer_source",
            "cold_root_control",
            HINT,
        )?;
        Ok(PreparedColdSuccessor {
            persistence: self,
            original,
            funder,
            predecessor: predecessor.identity(),
            snapshot,
            lsn,
            _control: control,
        })
    }
}
impl PreparedColdSuccessor<'_, '_> {
    /// Preflight MUST refuse before durability. Calling the old root constructor
    /// here would silently admit neither redb allocations nor registry growth.
    pub(crate) fn publication_preflight(&self) -> Result<()> {
        let accountant = self
            .persistence
            .proof_accountant()
            .ok_or_else(|| Error::Other("cold successor accountant is detached".into()))?;
        validate_owned_original(self.original, &accountant, self.funder)?;
        Err(Error::Other("cold successor publication requires native storage control/table/name admission, durable outcome marker, root registry control and original metadata lifetime companions".into()))
    }
    pub(crate) fn intended_identity(&self) -> (ColdRootIdentity, SnapshotId, Lsn) {
        (self.predecessor, self.snapshot, self.lsn)
    }
}

impl ColdCommittedProvider<'_> {
    /// The scalar descriptor is separately store-funded under an original
    /// UNTAGGED handle. It does not remove the retained WriterSchema or certify
    /// WriterSnapshot construction/registry/publication.
    #[allow(
        private_interfaces,
        reason = "the type is crate-private on purpose; the crate-visible item is only reachable inside the crate"
    )]
    pub(crate) fn scalar_metadata_unqualified(
        &self,
    ) -> Result<super::super::native_storage_admission::ColdMetadataOwner> {
        use super::super::native_storage_admission::{ColdMetadataOwner, ColdSnapshotMetadata};
        ColdMetadataOwner::new_unqualified(
            &self.root.accountant,
            ColdSnapshotMetadata {
                identity: self.read.storage_snapshot_identity(),
                snapshot: self.root.snapshot,
                lsn: self.root.lsn,
            },
        )
    }

    /// ONE exact [u8;24] lookup, lending only the body in this captured view.
    /// Storage admission refusals are scalar. Bootstrap, backend I/O, legacy
    /// cache refusal, callback and erasure errors remain unqualified.
    /// The inner result retains lower inline faults without error formatting;
    /// admission returns the original scalar through the outer result first.
    pub(crate) fn lend_exact_row_body_original_unqualified<T>(
        &self,
        original: &OwnedMemoryReservation,
        funder: &contextdb_tx::SourceFunder,
        logical: &str,
        key: &[u8; 24],
        before: &mut dyn FnMut() -> Result<()>,
        visit: impl FnOnce(&[u8]) -> Result<T>,
    ) -> std::result::Result<
        std::result::Result<Option<T>, redb::NativeColdReadError>,
        super::super::native_storage_admission::NativeStorageFailure,
    > {
        use super::super::native_storage_admission::{NativeStorageFailure, StorageAdmissionScope};
        before().map_err(NativeStorageFailure::Unqualified)?;
        let scope =
            StorageAdmissionScope::writer_unqualified(original, &self.root.accountant, funder)
                .map_err(NativeStorageFailure::Unqualified)?;
        scope.run_native(|| {
            scope.with_rel_name(logical, |physical| {
                self.lend_exact_body_in_scope(logical, physical, key, visit)
            })
        })
    }

    /// Same storage primitive and captured transaction, with one actual native
    /// operation child. Never uses child(), the legacy budget, or writer memory.
    pub(crate) fn lend_exact_row_body_reader_unqualified<T>(
        &self,
        declared: &Arc<crate::vector_read_memory::DeclaredReadMemory>,
        logical: &str,
        key: &[u8; 24],
        before: &mut dyn FnMut() -> Result<()>,
        visit: impl FnOnce(&[u8]) -> Result<T>,
    ) -> std::result::Result<
        std::result::Result<Option<T>, redb::NativeColdReadError>,
        super::super::native_storage_admission::NativeStorageFailure,
    > {
        use super::super::native_storage_admission::{NativeStorageFailure, StorageAdmissionScope};
        before().map_err(NativeStorageFailure::Unqualified)?;
        let child = declared
            .native_child()
            .map_err(NativeStorageFailure::Refused)?;
        let scope = StorageAdmissionScope::reader_unqualified(child)
            .map_err(NativeStorageFailure::Unqualified)?;
        scope.run_native(|| {
            scope.with_rel_name(logical, |physical| {
                self.lend_exact_body_in_scope(logical, physical, key, visit)
            })
        })
    }

    fn lend_exact_body_in_scope<T>(
        &self,
        logical: &str,
        physical: &str,
        key: &[u8; 24],
        visit: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<std::result::Result<Option<T>, redb::NativeColdReadError>> {
        let key = *key; // exact relational identity remains inline on this stack
        let erasure = self.root.erasure.read();
        erasure.check_scope(Some(logical), physical)?;
        erasure.check_key(physical, &key)?;
        match self.read.get_native_byte_value(physical, &key) {
            Ok(Some(body)) => visit(body.value()).map(|value| Ok(Some(value))),
            Ok(None) => Ok(Ok(None)),
            Err(fault) => Ok(Err(fault)),
        }
        // erasure fence and body guard cover the callback. The borrowed body
        // cannot escape as a T referring to this locally owned AccessGuard.
    }
}

#[cfg(feature = "sync-orchestration")]
impl ColdCommittedProvider<'_> {
    /// Descriptor of THIS retained transaction, paid by the original request.
    /// Existing store-funded descriptors and bootstrap are unchanged.
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
    root: Arc<WriterSnapshot>,
    read: Arc<redb::ReadTransaction>,
}
#[cfg(feature = "sync-orchestration")]
impl ColdCommittedProvider<'_> {
    pub(crate) fn into_native_config_range(
        self,
        root: Arc<WriterSnapshot>,
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
    pub(crate) fn root(&self) -> &Arc<WriterSnapshot> {
        &self.root
    }

    /// Actual CONFIG_TABLE range producer. The callback receives only actual
    /// immutable page guards; refusal extraction precedes callback/cold errors.
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
