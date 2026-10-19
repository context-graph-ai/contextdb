//! Private raw observation at one authentic committed root. This is an
//! explicit table visit, not a certificate that every retained source was
//! enumerated. In particular it cannot inventory resident graph/page holders.
use super::ColdCommittedProvider;
use crate::executor::bounded::NativeReadOpenContext;
use crate::owner_read::NativeReadRequest;
use crate::persistence::native_storage_admission::{NativeStorageFailure, StorageAdmissionScope};
use crate::persistence::writer_source::{NativeConfigBytes, NativeConfigFailure};
use contextdb_core::{Error, Lsn, SnapshotId, read_memory::ReadMemoryOwner};
use redb::TableHandle as _;

/// Closed physical families, never an arbitrary caller-supplied table name.
/// A native tail is addressed by physical storage id, NOT logical generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PhysicalVectorTable {
    Bodies,
    Memberships,
    OpenDirectory,
    Journal,
    Tombstones,
    BaseGenerations,
    ChangeGenerations,
    GenerationCatalog,
    GraphPages,
    NativePages,
    NativeBuilds,
    NativeCleanup,
    NativeChangeBackings,
    NativeTail { storage_id: u64 },
}
impl PhysicalVectorTable {
    fn with_name<T>(self, visit: impl FnOnce(&str) -> T) -> T {
        use crate::persistence::*;
        match self {
            Self::Bodies => visit(VECTORS_TABLE.name()),
            Self::Memberships => visit(VECTOR_PARTITION_MEMBERSHIP_TABLE.name()),
            Self::OpenDirectory => visit(VECTOR_OPEN_DIRECTORY_TABLE.name()),
            Self::Journal => visit(VECTOR_PARTITION_JOURNAL_TABLE.name()),
            Self::Tombstones => visit(VECTOR_PARTITION_TOMBSTONE_TABLE.name()),
            Self::BaseGenerations => visit(VECTOR_PARTITION_BASE_GENERATION_TABLE.name()),
            Self::ChangeGenerations => visit(VECTOR_PARTITION_CHANGE_GENERATION_TABLE.name()),
            Self::GenerationCatalog => visit(VECTOR_PARTITION_GENERATION_CATALOG_TABLE.name()),
            Self::GraphPages => visit(VECTOR_PARTITION_GRAPH_PAGE_TABLE.name()),
            Self::NativePages => visit(vector_paged::PAGES.name()),
            Self::NativeBuilds => visit(vector_paged::BUILDS.name()),
            Self::NativeCleanup => visit(vector_paged::CLEANUP.name()),
            Self::NativeChangeBackings => visit(vector_paged::import_tail::CHANGE_BACKINGS.name()),
            Self::NativeTail { storage_id } => {
                // Same name as page_table_name, with no formatted String.
                let mut name = [0u8; 34];
                name[..18].copy_from_slice(b"vector_paged_tail_");
                for (i, byte) in name[18..].iter_mut().enumerate() {
                    let nibble = ((storage_id >> (4 * (15 - i))) & 15) as usize;
                    *byte = b"0123456789abcdef"[nibble];
                }
                visit(std::str::from_utf8(&name).expect("static ASCII table name"))
            }
        }
    }
}

/// Borrowed actual stored bytes. Page guards, erasure fence and native source
/// admission remain alive throughout the callback. A consumer that retains a
/// copy must admit it to `owner` before copying and retain its credit to free.
/// No row, version, catalog, visibility or graph-count selection was applied.
pub(crate) struct PhysicalVectorRecordView<'a> {
    pub(crate) table: PhysicalVectorTable,
    pub(crate) root: redb::StorageSnapshotIdentity,
    pub(crate) snapshot: SnapshotId,
    pub(crate) lsn: Lsn,
    pub(crate) key: &'a [u8],
    pub(crate) stored: &'a [u8],
    pub(crate) owner: &'a ReadMemoryOwner,
    read: &'a redb::ReadTransaction,
    context: &'a NativeReadOpenContext,
}
impl PhysicalVectorRecordView<'_> {
    /// The native identity and member-directory codecs can be borrowed without
    /// decoding a point, exact-key Vec, graph, row body or output collection.
    /// Other page kinds remain in the raw stream; None NEVER means absence.
    pub(crate) fn native_identity(
        &self,
    ) -> contextdb_core::Result<Option<crate::persistence::vector_paged::PhysicalPagedIdentity<'_>>> {
        match self.table {
            PhysicalVectorTable::NativePages | PhysicalVectorTable::NativeTail { .. } => {
                if let PhysicalVectorTable::NativeTail { storage_id } = self.table
                    && self.key.get(..8) != Some(storage_id.to_be_bytes().as_slice())
                {
                    return Err(crate::persistence::vector_paged::corruption(
                        "physical tail key belongs to another storage identity",
                    ));
                }
                crate::persistence::vector_paged::physical_identity(self.key, self.stored)
            }
            _ => Ok(None),
        }
    }
}

/// Successful exhaustion of ONLY the explicit table list at this root.
/// Absence is authenticated by native tree navigation, never cached length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalVectorRootObservation {
    pub(crate) root: redb::StorageSnapshotIdentity,
    pub(crate) snapshot: SnapshotId,
    pub(crate) lsn: Lsn,
    pub(crate) tables: usize,
    pub(crate) records: u64,
}

impl ColdCommittedProvider<'_> {
    pub(crate) fn visit_physical_vector_records(
        &self,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
        tables: &[PhysicalVectorTable],
        mut visit: impl FnMut(PhysicalVectorRecordView<'_>) -> Result<(), NativeConfigFailure>,
    ) -> Result<PhysicalVectorRootObservation, NativeConfigFailure> {
        let unqualified = |error| NativeConfigFailure::Storage(NativeStorageFailure::Unqualified(error));
        context.checkpoint().map_err(unqualified)?;
        let owner = operation.owner();
        if !owner.same_owner(&context.owner().map_err(unqualified)?) {
            return Err(NativeConfigFailure::OwnerMismatch);
        }
        let metadata = self.native_read_metadata(&owner)?;
        let storage = StorageAdmissionScope::reader_native_owner(owner.clone())?;
        let mut observation = PhysicalVectorRootObservation {
            root: metadata.identity,
            snapshot: metadata.snapshot,
            lsn: metadata.lsn,
            tables: 0,
            records: 0,
        };
        for &table in tables {
            if let PhysicalVectorTable::NativeTail { storage_id } = table
                && storage_id & crate::persistence::vector_paged::MUTABLE_STORAGE == 0
            {
                return Err(NativeConfigFailure::InvalidCanonicalIndex);
            }
            let mut after: Option<NativeConfigBytes> = None;
            loop {
                let next = table.with_name(|name| {
                    storage.run_native(|| {
                        context.checkpoint()?;
                        if metadata.identity != self.read.storage_snapshot_identity()
                            || metadata.snapshot != self.root.snapshot
                            || metadata.lsn != self.root.lsn
                        {
                            return Err(Error::StoreCorrupted {
                                path: "writer source".into(),
                                reason: "physical vector metadata differs from its retained committed view".into(),
                            });
                        }
                        // Root invalidation is terminal, never an empty scan.
                        // Keep the existing erasure fence across the raw loan.
                        let erasure = self.root.erasure.read_recursive();
                        erasure.check_scope(None, name)?;
                        let mut interrupted = None;
                        let mut before = || context.checkpoint().map_err(|error| {
                            interrupted = Some(error);
                            redb::NativeColdReadError::Io(std::io::ErrorKind::Interrupted.into())
                        });
                        let next = if table == PhysicalVectorTable::NativeCleanup {
                            let lower = match after.as_ref() {
                                Some(key) => u64::from_le_bytes(key.as_slice().try_into()
                                    .map_err(|_| crate::persistence::vector_paged::corruption("cleanup key has invalid width"))?),
                                None => 0,
                            };
                            self.read.next_native_u64_bytes_range(name, lower, after.is_some(), &mut before)
                        } else {
                            self.read.next_native_byte_range(name,
                                after.as_ref().map_or(&[], |key| key.as_slice()), after.is_some(), None, &mut before)
                        };
                        if let Some(error) = interrupted {
                            return Err(error);
                        }
                        match next {
                            Ok(Some((key, value))) => {
                                context.touch_row()?;
                                erasure.check_key(name, key.value())?;
                                let mut consume = || -> Result<NativeConfigBytes, NativeConfigFailure> {
                                    let resume = NativeConfigBytes::join(&owner, &[key.value()])?;
                                    visit(PhysicalVectorRecordView {
                                        table,
                                        root: metadata.identity,
                                        snapshot: metadata.snapshot,
                                        lsn: metadata.lsn,
                                        key: key.value(),
                                        stored: value.value(),
                                        owner: &owner,
                                        read: &self.read,
                                        context,
                                    })?;
                                    context.checkpoint().map_err(unqualified)?;
                                    Ok(resume)
                                };
                                Ok(Ok(consume().map(Some)))
                            }
                            Ok(None) | Err(redb::NativeColdReadError::TableAbsent) => {
                                context.checkpoint()?;
                                Ok(Ok(Ok(None)))
                            }
                            Err(error) => Ok(Err(error)),
                        }
                    })
                })
                .map_err(NativeConfigFailure::Storage)?
                .map_err(NativeConfigFailure::ColdUnqualified)??;
                let Some(resume) = next else { break };
                after = Some(resume);
                observation.records = observation.records.checked_add(1)
                    .ok_or(NativeConfigFailure::InvalidCanonicalIndex)?;
            }
            observation.tables = observation.tables.checked_add(1)
                .ok_or(NativeConfigFailure::InvalidCanonicalIndex)?;
        }
        context.checkpoint().map_err(unqualified)?;
        Ok(observation)
    }
}

impl PhysicalVectorRecordView<'_> {
    /// Exact identity joins use THIS read and the erasure/storage guards already
    /// covering the raw callback. Neither MVCC nor graph counts filter a join.
    pub(crate) fn native_owner(&self)
        -> Result<Option<crate::persistence::vector_paged::physical::PhysicalOwner>, NativeConfigFailure> {
        use crate::persistence::vector_paged::{self as paged, PhysicalPagedIdentity};
        use paged::physical::{owner_from_identity, point_origin};
        let unqualified = |e| NativeConfigFailure::Storage(NativeStorageFailure::Unqualified(e));
        let invalid = || unqualified(paged::corruption("physical point/member lost its exact same-root owner"));
        self.context.checkpoint().map_err(unqualified)?;
        let Some(projected) = self.native_identity().map_err(unqualified)? else { return Ok(None); };
        let (storage_id, data_id, expected_member, expected_point) = match projected {
            PhysicalPagedIdentity::Point { storage_id, point, encoded } =>
                (storage_id, point_origin(encoded, point).map_err(unqualified)?, None, Some(point)),
            PhysicalPagedIdentity::Identity { storage_id, data_id, point, .. } =>
                (storage_id, data_id, None, Some(point)),
            PhysicalPagedIdentity::Member { storage_id, data_id, graph_origin, row_id, lsn } =>
                (storage_id, data_id, Some((graph_origin, row_id, lsn)), None),
        };
        self.table.with_name(|name| {
            let key = paged::identity_key(&storage_id.to_be_bytes(), data_id);
            self.context.touch_row().map_err(unqualified)?;
            let frame = self.read.get_native_byte_value(name, &key)
                .map_err(NativeConfigFailure::ColdUnqualified)?.ok_or_else(invalid)?;
            let owner = owner_from_identity(&key, frame.value()).map_err(unqualified)?;
            if expected_member.is_some_and(|m| m != (owner.graph_origin, owner.row_id, owner.lsn))
                || expected_point.is_some_and(|p| p != owner.point)
            { return Err(invalid()); }
            let representative = paged::identity_key(&storage_id.to_be_bytes(), owner.graph_origin);
            self.context.touch_row().map_err(unqualified)?;
            let frame = self.read.get_native_byte_value(name, &representative)
                .map_err(NativeConfigFailure::ColdUnqualified)?.ok_or_else(invalid)?;
            let representative = owner_from_identity(&representative, frame.value()).map_err(unqualified)?;
            if representative.data_id != owner.graph_origin
                || representative.graph_origin != representative.data_id || representative.point != owner.point
            { return Err(invalid()); }
            let key = paged::point_key(&storage_id.to_be_bytes(), owner.point);
            self.context.touch_row().map_err(unqualified)?;
            let frame = self.read.get_native_byte_value(name, &key)
                .map_err(NativeConfigFailure::ColdUnqualified)?.ok_or_else(invalid)?;
            let Some(PhysicalPagedIdentity::Point { encoded, point, .. }) =
                paged::physical_identity(&key, frame.value()).map_err(unqualified)?
            else { return Err(invalid()); };
            if point_origin(encoded, point).map_err(unqualified)? != owner.graph_origin { return Err(invalid()); }
            self.context.checkpoint().map_err(unqualified)?;
            Ok(Some(owner))
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PhysicalVectorDiscovery {
    pub(crate) visited_tables: u64,
    pub(crate) raw_records: u64,
    pub(crate) unclassified_tables: u64,
}
impl PhysicalVectorTable {
    fn discovered(name: &str) -> Option<Self> {
        const FIXED: [PhysicalVectorTable; 13] = [
            PhysicalVectorTable::Bodies, PhysicalVectorTable::Memberships,
            PhysicalVectorTable::OpenDirectory, PhysicalVectorTable::Journal,
            PhysicalVectorTable::Tombstones, PhysicalVectorTable::BaseGenerations,
            PhysicalVectorTable::ChangeGenerations, PhysicalVectorTable::GenerationCatalog,
            PhysicalVectorTable::GraphPages, PhysicalVectorTable::NativePages,
            PhysicalVectorTable::NativeBuilds, PhysicalVectorTable::NativeCleanup,
            PhysicalVectorTable::NativeChangeBackings,
        ];
        for table in FIXED { if table.with_name(|known| known == name) { return Some(table); } }
        let tail = name.strip_prefix("vector_paged_tail_")?;
        if tail.len() != 16 || !tail.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) { return None; }
        let storage_id = u64::from_str_radix(tail, 16).ok()?;
        (storage_id & crate::persistence::vector_paged::MUTABLE_STORAGE != 0)
            .then_some(Self::NativeTail { storage_id })
    }
}
impl ColdCommittedProvider<'_> {
    /// Enumerate actual directory entries, including orphan mutable tables;
    /// caller-selected scopes and graph population counts cannot hide a table.
    /// Unsupported families are reported, never treated as empty or complete.
    pub(crate) fn discover_physical_vector_records(&self, operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
        mut unclassified: impl FnMut(&str) -> Result<(), NativeConfigFailure>,
        mut visit: impl FnMut(PhysicalVectorRecordView<'_>) -> Result<(), NativeConfigFailure>,
    ) -> Result<PhysicalVectorDiscovery, NativeConfigFailure> {
        let unqualified = |e| NativeConfigFailure::Storage(NativeStorageFailure::Unqualified(e));
        let owner = operation.owner();
        if !owner.same_owner(&context.owner().map_err(unqualified)?) { return Err(NativeConfigFailure::OwnerMismatch); }
        let storage = StorageAdmissionScope::reader_native_owner(owner.clone())?;
        let mut after: Option<NativeConfigBytes> = None;
        let mut receipt = PhysicalVectorDiscovery::default();
        loop {
            let next = storage.run_native(|| {
                context.checkpoint()?;
                let erasure = self.root.erasure.read_recursive();
                erasure.check_scope(None, "vector source directory")?;
                let mut interrupted = None;
                let next = self.read.next_native_directory_record(
                    after.as_ref().map_or(&[], |k| k.as_slice()), after.is_some(),
                    &mut || context.checkpoint().map_err(|e| {
                        interrupted = Some(e);
                        redb::NativeColdReadError::Io(std::io::ErrorKind::Interrupted.into())
                    }));
                if let Some(e) = interrupted { return Err(e); }
                Ok(next.map(|next| next.map(|(key, _)| {
                    context.touch_row().map_err(unqualified)?;
                    NativeConfigBytes::join(&owner, &[key.value()])
                }).transpose()))
            }).map_err(NativeConfigFailure::Storage)?
                .map_err(NativeConfigFailure::ColdUnqualified)??;
            let Some(next) = next else { break; };
            let name = std::str::from_utf8(next.as_slice())
                .map_err(|_| NativeConfigFailure::InvalidCanonicalIndex)?;
            if let Some(table) = PhysicalVectorTable::discovered(name) {
                let observed = self.visit_physical_vector_records(operation, context, &[table], &mut visit)?;
                receipt.visited_tables = receipt.visited_tables.checked_add(1).ok_or(NativeConfigFailure::InvalidCanonicalIndex)?;
                receipt.raw_records = receipt.raw_records.checked_add(observed.records).ok_or(NativeConfigFailure::InvalidCanonicalIndex)?;
            } else if name.starts_with("vector_") || name.starts_with("__vector_") {
                unclassified(name)?;
                receipt.unclassified_tables = receipt.unclassified_tables.checked_add(1).ok_or(NativeConfigFailure::InvalidCanonicalIndex)?;
            }
            after = Some(next);
        }
        context.checkpoint().map_err(unqualified)?;
        Ok(receipt)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalChangeBacking {
    pub(crate) original_storage: u64,
    pub(crate) physical_storage: u64,
    pub(crate) original_logical_generation: u64,
    pub(crate) source_digest: Option<[u8; 32]>,
    pub(crate) route_digest: Option<[u8; 32]>,
}
impl PhysicalVectorRecordView<'_> {
    /// Describes the actual original-to-mutable record under this raw root.
    /// Full normalized descriptor equality is checked without owned encoding.
    /// A canonical generation/route/schema join is still separately required.
    pub(crate) fn native_change_backing(&self) -> contextdb_core::Result<Option<PhysicalChangeBacking>> {
        if self.table != PhysicalVectorTable::NativeChangeBackings { return Ok(None); }
        let key: [u8; 8] = self.key.try_into().map_err(|_|
            crate::persistence::vector_paged::corruption("native backing key has invalid width"))?;
        let (original, physical) = crate::persistence::vector_paged::physical::borrowed_change_backing(
            u64::from_be_bytes(key), self.stored)?;
        Ok(Some(PhysicalChangeBacking { original_storage: original.storage_id,
            physical_storage: physical.storage_id,
            original_logical_generation: original.progress.generation_id,
            source_digest: original.saved.as_ref().map(|saved| saved.source_digest),
            route_digest: original.saved.as_ref().map(|saved| saved.route) }))
    }
}
