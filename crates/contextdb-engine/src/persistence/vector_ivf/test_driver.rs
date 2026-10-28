//! Storage-contract drive through the real borrowed adapter. This driver can
//! never publish its possibly incomplete controls as a committed SQL result.
use super::*;
use contextdb_core::Lsn;
use redb::ReadableTable;

impl RedbPersistence {
    pub(crate) fn with_aborting_native_ivf_write<R>(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        snapshot: contextdb_core::SnapshotId,
        frontier: Lsn,
        checkpoint: &dyn Fn() -> Result<()>,
        distance: &dyn Fn(contextdb_vector::ivf::storage::DistanceKind) -> Result<()>,
        run: impl FnOnce(&mut write::WriteScope<'_, '_>) -> Result<R>,
    ) -> Result<R> {
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let owner = projection.vector_budget()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        admission.run_original_unqualified(|| {
            self.with_db(|db| {
                let write = Self::begin_history_write(db)?;
                let result = {
                    let mut session = write::WriteSession::borrow(
                        &write, &admission, owner, snapshot, frontier, frontier,
                    )?;
                    let result = session.with_scope(index, tuple, checkpoint, distance, run);
                    match result {
                        Ok(value) => session.bind_final().map(|()| value),
                        Err(error) => Err(error),
                    }
                };
                // Even a successful drive is physically rolled back. An
                // error also drops this actual write rather than publishing.
                let observer = Producer::current();
                let aborted = write.abort().map_err(RedbPersistence::storage_error);
                if aborted.is_ok() {
                    observer.emit(NativeIvfSealEvent::OuterAborted);
                }
                match result {
                    Err(error) => Err(error),
                    Ok(value) => aborted.map(|()| value),
                }
            })
        })
    }
}

use crate::vector_ivf_observations::{
    NativeIvfCapturedRootRef, NativeIvfSealEvent, NativeIvfServingDescriptorForTest,
    NativeIvfStoredScopeRef, Producer,
};
use contextdb_vector::ivf::storage::{
    BuildState, DistanceKind, Manifest, PageKey, ReadPages, Verdict,
};
/// Borrows the actual captured source, including its native root and original admission.
pub struct NativeIvfReadForTest<'a>(read::PageSource<'a>);
impl NativeIvfReadForTest<'_> {
    pub fn with_scope_control_for_test<R>(
        &self,
        visit: impl FnOnce(NativeIvfCapturedRootRef, NativeIvfStoredScopeRef<'_>, &[u8]) -> Result<R>,
    ) -> Result<R> {
        self.0.inspect_control(visit)
    }
    pub fn serving_descriptor(&self) -> Result<Option<NativeIvfServingDescriptorForTest>> {
        self.0.inspect_descriptor()
    }
}
impl ReadPages for NativeIvfReadForTest<'_> {
    fn retained_codebook(
        &self,
        manifest: Manifest,
    ) -> Option<&contextdb_vector::ivf::RetainedCodebook> {
        self.0.retained_codebook(manifest)
    }
    fn inspection_verdict(&self, p: u64, g: u64) -> Option<Verdict> {
        self.0.inspection_verdict(p, g)
    }
    fn record_inspection_verdict(&self, p: u64, g: u64, v: Verdict) {
        self.0.record_inspection_verdict(p, g, v);
    }
    fn snapshot_lsn(&self) -> Result<u64> {
        self.0.snapshot_lsn()
    }
    fn index_identity(&self) -> &VectorIndexRef {
        self.0.index_identity()
    }
    fn check_cancel(&self) -> Result<()> {
        self.0.check_cancel()
    }
    fn before_distance(&self, k: DistanceKind) -> Result<()> {
        self.0.before_distance(k)
    }
    fn manifest(&self, p: u64) -> Result<Option<Manifest>> {
        self.0.manifest(p)
    }
    fn build_state(&self, p: u64) -> Result<Option<BuildState>> {
        self.0.build_state(p)
    }
    fn next_partition(&self, p: Option<u64>) -> Result<Option<u64>> {
        self.0.next_partition(p)
    }
    fn read_admitted_page(&self, k: PageKey, out: &mut [u8; PAGE_BYTES]) -> Result<bool> {
        self.0.read_admitted_page(k, out)
    }
}
pub(crate) fn with_captured_read<R>(
    root: &super::super::vector_paged::CapturedPagedRoot,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    callbacks: crate::vector_ivf_observations::NativeIvfDriveCallbacks<'_>,
    maintenance: Option<super::retained::Selection<'_>>,
    run: impl FnOnce(&NativeIvfReadForTest<'_>) -> Result<R>,
) -> Result<R> {
    let authority = read::NativeAuthority::open(root, memory)?;
    let mut pages = read::PageSource::open(
        &authority,
        index,
        tuple,
        callbacks.checkpoint,
        callbacks.before_distance,
    )?
    .ok_or_else(|| Error::Other("no native clustered scope at the selected snapshot".into()))?;
    if let Some(selection) = maintenance {
        pages.activate_retained(tuple, selection)?;
    }
    run(&NativeIvfReadForTest(pages))
}

impl NativeIvfReadForTest<'_> {
    pub fn build_progress(
        &self,
    ) -> Result<crate::vector_ivf_observations::NativeIvfBuildProgressForTest> {
        use crate::vector_ivf_observations::*;
        let record = self.0.record.borrow();
        Ok(NativeIvfBuildProgressForTest {
            replacement_trigger: None,
            build: record.build_state()?,
            source_cursor: record.source_cursor.map(|s| NativeIvfSourceCursorForTest {
                canonical: s.canonical,
                membership: s.membership,
            }),
            frozen_live_entries: record
                .build_state()?
                .and_then(|build| record.family(build.manifest.generation))
                .map_or(0, |family| family.basis_live_entries),
            ingested_vectors: record.ingested_vectors,
            aborted_builds: record.aborted_builds,
            canonical_complete: record.canonical_complete,
        })
    }
    pub fn journal_identity(
        &self,
        row: contextdb_core::RowId,
    ) -> Result<crate::vector_ivf_observations::NativeIvfJournalIdentityForTest> {
        use crate::vector_ivf_observations::*;
        use contextdb_vector::ivf::storage::PageKind;
        let record = self.0.record.borrow();
        let manifest = record
            .manifest()?
            .or(record.build_state()?.map(|b| b.manifest));
        let Some(manifest) = manifest else {
            return Ok(NativeIvfJournalIdentityForTest {
                initialized: false,
                identity: None,
                deleted: false,
                copies: 0,
            });
        };
        drop(record);
        let mut page = [0u8; PAGE_BYTES];
        if !self.0.read_admitted_page(
            manifest.key(PageKind::RowVersions, 0, row.0 / (PAGE_BYTES as u64 / 40)),
            &mut page,
        )? {
            return Ok(NativeIvfJournalIdentityForTest {
                initialized: false,
                identity: None,
                deleted: false,
                copies: 0,
            });
        }
        let at = (row.0 % (PAGE_BYTES as u64 / 40)) as usize * 40;
        let word = |offset| -> Result<u64> {
            Ok(u64::from_le_bytes(
                page[at + offset..at + offset + 8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            ))
        };
        let flags = word(32)?;
        let copies = word(24)?;
        if flags & !3 != 0 || flags == 2 || flags & 2 != 0 && copies != 0 {
            return Err(corrupt());
        }
        Ok(NativeIvfJournalIdentityForTest {
            initialized: flags & 1 != 0,
            identity: if flags & 1 != 0 {
                Some(contextdb_vector::ivf::RowIdentity {
                    row: row.0,
                    commit_lsn: word(0)?,
                })
            } else {
                None
            },
            deleted: flags & 2 != 0,
            copies,
        })
    }
}

impl NativeIvfReadForTest<'_> {
    pub fn scope_roots(
        &self,
    ) -> Result<crate::vector_ivf_observations::NativeIvfScopeRootsForTest> {
        self.0.inspect_scope_roots()
    }
    pub fn visit_authoritative_source(
        &self,
        visit: &mut dyn FnMut(crate::vector_ivf_observations::NativeIvfSourceRecordForTest<'_>),
    ) -> Result<()> {
        self.0.visit_authoritative_source(visit)
    }
}

impl NativeIvfReadForTest<'_> {
    pub fn maintenance_state(
        &self,
    ) -> Result<crate::vector_ivf_observations::NativeIvfMaintenanceStateForTest> {
        use crate::vector_ivf_observations::*;
        let record = self.0.record.borrow();
        let manifest = record.manifest()?;
        Ok(NativeIvfMaintenanceStateForTest {
            manifest,
            build: record.build_state()?,
            serving_seed: manifest
                .and_then(|m| record.family(m.generation).and_then(|f| f.training_seed)),
            policy_revision: record.policy_revision,
            training_evaluations: record.training_evaluations,
            assignment_evaluations: record.assignment_evaluations,
            source_live_entries: self.0.source_summary.live_entries,
            legacy_entries_prepared: record.legacy_entries_prepared,
        })
    }
    pub fn catalog(
        &self,
        max_delta_runs: u64,
    ) -> Result<Option<contextdb_vector::ivf::CatalogState>> {
        contextdb_vector::ivf::inspect_catalog(&self.0, 0, max_delta_runs)
    }
    pub fn visit_physical_pages(
        &self,
        visit: &mut dyn FnMut(crate::vector_ivf_observations::NativeIvfPhysicalPageForTest<'_>),
    ) -> Result<()> {
        use crate::vector_ivf_observations::*;
        self.0.authority.admission.run_original_unqualified(|| {
            let record = self.0.record.borrow();
            let building = record
                .build_state()?
                .filter(|b| b.phase != contextdb_vector::ivf::storage::BuildPhase::Ready)
                .map(|b| b.manifest.generation);
            let family_pages = |family: control::Family,
                                visit: &mut dyn FnMut(NativeIvfPhysicalPageForTest<'_>)|
             -> Result<()> {
                for tag in 1..=16 {
                    if family.kinds.present & (1 << (tag - 1)) == 0 {
                        continue;
                    }
                    let kind = header::kind_from_tag(tag)?;
                    let name = io::KindTableName::new(family.storage, kind)?;
                    let table = self
                        .0
                        .authority
                        .root
                        .read
                        .open_byte_table_bounded(redb::TableDefinition::new(name.as_str()?))
                        .map_err(RedbPersistence::storage_error)?;
                    let mut cursor = table
                        .prefix_bounded(&[])
                        .map_err(RedbPersistence::storage_error)?;
                    while let Some(entry) = cursor.next() {
                        let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                        let key = header::key_from_bytes(key.value())?;
                        let admitted = header::AdmittedIvfPage::open(
                            header::HeaderIdentity {
                                index: &record.index,
                                tuple: &record.tuple,
                                family: &family,
                            },
                            key,
                            value.value(),
                        )?;
                        visit(NativeIvfPhysicalPageForTest {
                            storage: family.storage,
                            key,
                            building: building == Some(family.generation),
                            payload: admitted.payload,
                            bytes: value.value(),
                        });
                    }
                }
                Ok(())
            };
            for family in record.families.iter().flatten() {
                family_pages(*family, visit)?;
            }
            let scopes = self
                .0
                .authority
                .root
                .read
                .open_byte_table_bounded(SCOPES)
                .map_err(RedbPersistence::storage_error)?;
            let mut prefix = [0; 65];
            prefix[..64].copy_from_slice(&self.0.scope_key);
            prefix[64] = b'R';
            let mut cursor = scopes
                .prefix_bounded(&prefix)
                .map_err(RedbPersistence::storage_error)?;
            while let Some(entry) = cursor.next() {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                family_pages(
                    control::decode_retired_family(key.value(), value.value())?,
                    visit,
                )?;
            }
            Ok(())
        })
    }
    /// Extract the actual stored quantized code, never a duplicate codec.
    pub fn quantized_record(&self, vector: &[f32]) -> Result<Vec<u8>> {
        let record = self.0.record.borrow();
        let selected = record.manifest()?.filter(|m| m.active_lists != 0).or(record
            .build_state()?
            .filter(|b| {
                matches!(
                    b.phase,
                    contextdb_vector::ivf::storage::BuildPhase::Assign
                        | contextdb_vector::ivf::storage::BuildPhase::Publish
                        | contextdb_vector::ivf::storage::BuildPhase::Ready
                )
            })
            .map(|b| b.manifest));
        drop(record);
        let Some(manifest) = selected else {
            return Ok(Vec::new());
        };
        if vector.len() != manifest.dimensions {
            return Err(corrupt());
        }
        let owner = self.0.authority.vector_owner();
        let bytes = manifest.dimensions.checked_mul(4).ok_or_else(corrupt)?;
        let _invoice = memory::Invoice::reserve(owner, bytes, "clustered_inspect_vector")?;
        let mut raw = vec![0u8; bytes];
        let stride = bytes.checked_add(8).ok_or_else(corrupt)?;
        let mut wanted = None;
        for slot in 0..manifest.slots {
            let mut copied = 0;
            while copied < bytes {
                let offset = slot
                    .checked_mul(stride as u64)
                    .and_then(|n| n.checked_add(8 + copied as u64))
                    .ok_or_else(corrupt)?;
                let mut page = [0; PAGE_BYTES];
                if !self.0.read_admitted_page(
                    manifest.key(
                        contextdb_vector::ivf::storage::PageKind::Canonical,
                        0,
                        offset / PAGE_BYTES as u64,
                    ),
                    &mut page,
                )? {
                    return Err(corrupt());
                }
                let start = (offset % PAGE_BYTES as u64) as usize;
                let take = (bytes - copied).min(PAGE_BYTES - start);
                raw[copied..copied + take].copy_from_slice(&page[start..start + take]);
                copied += take;
            }
            if raw
                .chunks_exact(4)
                .zip(vector)
                .all(|(raw, v)| raw == v.to_le_bytes())
            {
                wanted = Some(slot);
                break;
            }
        }
        let Some(wanted) = wanted else {
            return Ok(Vec::new());
        };
        let mut encoded = Vec::new();
        self.visit_physical_pages(&mut |page| {
            if !encoded.is_empty()
                || page.key.generation != manifest.generation
                || page.key.kind != contextdb_vector::ivf::storage::PageKind::Lists
            {
                return;
            }
            for record in page.payload.chunks_exact(manifest.dimensions + 16) {
                if record[..8] == wanted.to_le_bytes() {
                    encoded.extend_from_slice(&record[16..]);
                    break;
                }
            }
        })?;
        Ok(encoded)
    }
}

impl RedbPersistence {
    pub(crate) fn corrupt_native_ivf_descriptor(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        offset: usize,
        mask: u8,
    ) -> Result<()> {
        if offset >= descriptor::BYTES || mask == 0 {
            return Err(Error::Other("invalid descriptor corruption target".into()));
        }
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        admission.run_original_unqualified(|| {
            self.with_db(|db| {
                let write = Self::begin_history_write(db)?;
                let schema = Self::vector_schema_binding_in_write(&write, index)?;
                let scope = control::scope_key(
                    control::index_digest(index, schema),
                    control::tuple_digest(tuple),
                );
                let key = descriptor::key(&scope);
                let mut bytes = [0; descriptor::BYTES];
                {
                    let table = write.open_table(SCOPES).map_err(Self::storage_error)?;
                    let value = table
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .ok_or_else(corrupt)?;
                    bytes.copy_from_slice(value.value());
                }
                bytes[offset] ^= mask;
                write
                    .open_table(SCOPES)
                    .map_err(Self::storage_error)?
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(Self::storage_error)?;
                // Authenticate the native container while leaving the descriptor's
                // own checksum and its ScopeRecord authority unchanged.
                super::super::content_identity::bind_vector_pages_root_in_write(&write)?;
                Self::write_writer_recovery_bounds_in_write(&write, None)?;
                write.commit().map_err(Self::storage_error)
            })
        })
    }
}

impl NativeIvfReadForTest<'_> {
    pub fn visit_codebook_centroids(&self, visit: &mut dyn FnMut(u32, &[f32])) -> Result<()> {
        use contextdb_vector::ivf::storage::PageKind;
        let Some(manifest) = self.0.manifest(0)? else {
            return Ok(());
        };
        let bytes = manifest
            .dimensions
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(corrupt)?;
        let _invoice = memory::Invoice::reserve(
            self.0.authority.vector_owner(),
            bytes,
            "clustered_inspect_centroid",
        )?;
        let mut centroid = vec![0.0; manifest.dimensions];
        let mut page = [0; PAGE_BYTES];
        for list in 0..manifest.active_lists {
            for (axis, value) in centroid.iter_mut().enumerate() {
                let position = (list as usize)
                    .checked_mul(manifest.dimensions)
                    .and_then(|n| n.checked_add(axis))
                    .and_then(|n| n.checked_mul(4))
                    .ok_or_else(corrupt)?;
                if !self.0.read_admitted_page(
                    manifest.key(
                        PageKind::Codebook,
                        0,
                        position as u64 / PAGE_BYTES as u64 + 1,
                    ),
                    &mut page,
                )? {
                    return Err(corrupt());
                }
                let offset = position % PAGE_BYTES;
                *value =
                    f32::from_le_bytes(page[offset..offset + 4].try_into().map_err(|_| corrupt())?);
            }
            visit(list, &centroid);
        }
        Ok(())
    }
}

impl RedbPersistence {
    pub(crate) fn corrupt_native_ivf_page(
        &self,
        storage: u64,
        key: PageKey,
        payload: bool,
        offset: usize,
        mask: u8,
    ) -> Result<()> {
        let extent = if payload {
            PAGE_BYTES
        } else {
            header::HEADER_BYTES
        };
        if offset >= extent || mask == 0 {
            return Err(Error::Other("invalid native page corruption target".into()));
        }
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        let _invoice = memory::Invoice::reserve(
            &projection.vector_budget()?,
            header::VALUE_BYTES,
            "clustered_page_fault_input",
        )?;
        admission.run_original_unqualified(|| {
            self.with_db(|db| {
                let write = Self::begin_history_write(db)?;
                let name = io::KindTableName::new(storage, key.kind)?;
                let definition = redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str()?);
                let key = header::key_bytes(key);
                let mut bytes = [0; header::VALUE_BYTES];
                {
                    let table = write.open_table(definition).map_err(Self::storage_error)?;
                    let value = table
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .ok_or_else(corrupt)?;
                    if value.value().len() != bytes.len() {
                        return Err(corrupt());
                    }
                    bytes.copy_from_slice(value.value());
                }
                bytes[if payload {
                    header::HEADER_BYTES + offset
                } else {
                    offset
                }] ^= mask;
                write
                    .open_table(definition)
                    .map_err(Self::storage_error)?
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(Self::storage_error)?;
                // The captured scope's expected kind root and page authority remain unchanged.
                super::super::content_identity::bind_vector_pages_root_in_write(&write)?;
                Self::write_writer_recovery_bounds_in_write(&write, None)?;
                write.commit().map_err(Self::storage_error)
            })
        })
    }
}

impl NativeIvfReadForTest<'_> {
    pub fn retained_codebook_for_test(
        &self,
    ) -> crate::vector_ivf_observations::NativeIvfRetainedCodebookForTest {
        self.0.retained_for_test()
    }
}

impl NativeIvfReadForTest<'_> {
    /// The real exhaustive canonical visitor exposes only logically visible
    /// copies, including the still-searchable legacy runs during replacement.
    pub fn visit_visible_identities_for_test(
        &self,
        visit: &mut dyn FnMut(
            contextdb_vector::ivf::CandidateIdentity<'_>,
            Option<[u8; 32]>,
        ) -> Result<()>,
    ) -> Result<()> {
        let Some(manifest) = self.0.manifest(0)? else {
            return Ok(());
        };
        let bytes = manifest.dimensions.checked_mul(4).ok_or_else(corrupt)?;
        let _invoice = memory::Invoice::reserve(
            self.0.authority.vector_owner(),
            bytes,
            "clustered_inspect_query",
        )?;
        let query = vec![0.; manifest.dimensions];
        contextdb_vector::ivf::visit_visible_canonical(
            &self.0,
            self.0.authority.vector_owner(),
            &query,
            Some(&[0]),
            &mut |_| Ok(true),
            &mut |candidate| {
                let identity = candidate.identity;
                let book = self
                    .0
                    .record
                    .borrow()
                    .family(identity.generation)
                    .and_then(|family| family.manifest)
                    .map(|manifest| manifest.manifest())
                    .transpose()?
                    .filter(|manifest| manifest.active_lists != 0)
                    .filter(|_| {
                        identity.source != contextdb_vector::ivf::CandidateSource::UnsealedTail
                    })
                    .map(|manifest| manifest.codebook);
                visit(identity, book)
            },
            &mut Default::default(),
        )
    }
}

impl NativeIvfReadForTest<'_> {
    pub fn replacement_trigger_for_test(
        &self,
        column: &contextdb_core::ColumnDef,
    ) -> Result<Option<&'static str>> {
        let record = self.0.record.borrow();
        if record
            .build_state()?
            .is_some_and(|build| build.phase != contextdb_vector::ivf::storage::BuildPhase::Ready)
        {
            return Ok(None);
        }
        let catalog = contextdb_vector::ivf::inspect_catalog(
            &self.0,
            0,
            column.vector_engine_policy.max_delta_runs.unwrap_or(2),
        )?;
        Ok(maintenance::replacement_trigger(
            column,
            &record,
            record.manifest()?,
            self.0.source_summary.live_entries,
            catalog.is_some_and(|catalog| catalog.unserviceable.is_some()),
        ))
    }
}

impl NativeIvfReadForTest<'_> {
    pub fn generation_manifest_for_test(&self, generation: u64) -> Result<Option<Manifest>> {
        self.0
            .record
            .borrow()
            .family(generation)
            .and_then(|family| family.manifest)
            .map(|manifest| manifest.manifest())
            .transpose()
    }
}

impl RedbPersistence {
    pub(crate) fn remove_native_ivf_page_for_test(&self, storage: u64, key: PageKey) -> Result<()> {
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        let _input = memory::Invoice::reserve(
            &projection.vector_budget()?,
            std::mem::size_of::<(io::KindTableName, PageKey, [u8; header::KEY_BYTES])>(),
            "clustered_page_fault_control",
        )?;
        admission.run_original_unqualified(|| {
            self.with_db(|database| {
                let write = Self::begin_history_write(database)?;
                let name = io::KindTableName::new(storage, key.kind)?;
                let encoded = header::key_bytes(key);
                {
                    let mut table = write
                        .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str()?))
                        .map_err(Self::storage_error)?;
                    if table
                        .remove(encoded.as_slice())
                        .map_err(Self::storage_error)?
                        .is_none()
                    {
                        return Err(corrupt());
                    }
                }
                super::super::content_identity::bind_vector_pages_root_in_write(&write)?;
                Self::write_writer_recovery_bounds_in_write(&write, None)?;
                write.commit().map_err(Self::storage_error)
            })
        })
    }
}

impl NativeIvfReadForTest<'_> {
    pub fn visit_qualification(
        &self,
        visit: &mut dyn FnMut(super::qualification::Record),
    ) -> Result<()> {
        use super::qualification::Record;
        let Some(header) = self.0.qualification_header()? else {
            return Ok(());
        };
        visit(Record::Header(header));
        let mut previous = 0;
        for ordinal in 0..header.depths {
            let depth = self.0.qualification_depth(ordinal)?;
            visit(Record::Depth(depth));
            if let Some(counts) = depth.reranks {
                for sample in 0..counts.samples {
                    visit(Record::SampleWork(
                        self.0.qualification_sample_work(ordinal, sample)?,
                    ));
                }
            }
            for k in previous + 1..=depth.depth {
                visit(Record::Prefix(self.0.qualification_prefix(k)?));
            }
            previous = depth.depth;
        }
        Ok(())
    }
}

impl NativeIvfReadForTest<'_> {
    pub fn requalification_pending_for_test(
        &self,
        column: &contextdb_core::ColumnDef,
    ) -> Result<bool> {
        self.0.requalification_pending(column, self.0.tuple)
    }
}

impl RedbPersistence {
    pub(crate) fn native_ivf_cache_observation_for_test(
        &self,
    ) -> Result<redb::ReadCacheObservation> {
        self.with_db(|database| Ok(database.read_cache_observation()))
    }
}

impl RedbPersistence {
    /// Deliberate fixture damage uses the same original writer and native
    /// container publication sequence as descriptor corruption. The closure
    /// may leave a value's own checksum invalid while the container is rebound.
    #[cfg(feature = "test-seams")]
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "KEEP-SEAM: native fixture writer is called by independently authored engine unit guards under cfg(test)"
        )
    )]
    pub(crate) fn with_native_ivf_fixture_write_for_test<T>(
        &self,
        run: impl FnOnce(&redb::WriteTransaction) -> Result<T>,
    ) -> Result<T> {
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        admission.run_original_unqualified(|| {
            self.with_db(|database| {
                let write = Self::begin_history_write(database)?;
                let result = run(&write)?;
                super::super::content_identity::bind_vector_pages_root_in_write(&write)?;
                Self::write_writer_recovery_bounds_in_write(&write, None)?;
                write.commit().map_err(Self::storage_error)?;
                Ok(result)
            })
        })
    }
}

#[cfg(feature = "test-seams")]
impl RedbPersistence {
    /// Borrow an authenticated captured transaction under this writer's genuine
    /// native admission. The caller uses the ordinary byte/cursor read APIs.
    pub(crate) fn with_native_ivf_fixture_read_for_test<T>(
        &self,
        run: impl FnOnce(&redb::ReadTransaction) -> Result<T>,
    ) -> Result<T> {
        let _decode = self.writer_decode_scope()?;
        let projection = self.require_decode_original()?;
        let original = projection.preparation_original()?;
        let admission =
            super::super::native_storage_admission::StorageAdmissionScope::writer_original(
                &original,
            )?;
        let root = (self.paged_root_capture()?)()?;
        admission.run_original_unqualified(|| {
            root.authenticate()?;
            run(&root.read)
        })
    }
}
