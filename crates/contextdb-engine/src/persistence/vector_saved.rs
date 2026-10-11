//! Writer-side finite publication of existing saved topology into native pages.
//! No serving path calls this module.
use super::vector_paged::*;
use super::*;
use contextdb_vector::hnsw::paged_builder::{
    PagedGenerationTransaction, PagedVectorBatchPlan, PagedVectorIdentity, PagedVectorPhase,
    PagedVectorPoint, PagedVectorTransaction, encode_paged_point_into,
};
use contextdb_vector::hnsw::saved_topology::{SavedPointCursor, SavedTopology};

impl RedbPersistence {
    pub(crate) fn prepare_saved_topology_candidate(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
        admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<SavedTopologyCandidate>> {
        let binding = self.vector_schema_binding(&candidate.base.index)?;
        if binding == VectorSchemaBinding::UNBOUND {
            return Ok(None);
        }
        self.prepare_saved_topology_candidate_with_schema_binding(candidate, binding, admit)
    }

    pub(crate) fn prepare_saved_topology_candidate_with_schema_binding(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
        schema_binding: VectorSchemaBinding,
        admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<SavedTopologyCandidate>> {
        self.prepare_saved_topology_candidate_inner(candidate, schema_binding, false, admit)
    }

    pub(super) fn prepare_saved_topology_candidate_for_purge(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
        admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<SavedTopologyCandidate>> {
        if candidate.change.is_some() {
            return Err(corruption("purge replacement must be one new base"));
        }
        let binding = self.vector_schema_binding(&candidate.base.index)?;
        if binding == VectorSchemaBinding::UNBOUND {
            return Ok(None);
        }
        self.prepare_saved_topology_candidate_inner(candidate, binding, true, admit)
    }

    fn prepare_saved_topology_candidate_inner(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
        schema_binding: VectorSchemaBinding,
        preserve_purge_identity: bool,
        admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<Option<SavedTopologyCandidate>> {
        let base_saved = candidate.base.graph_format_version == 1
            && !candidate.base.graph.bytes.starts_with(&META_MAGIC);
        let change_saved = candidate.change.as_ref().is_some_and(|c| {
            c.graph_format_version == 1 && !c.graph.bytes.starts_with(&META_MAGIC)
        });
        if !base_saved && !change_saved {
            return Ok(None);
        }
        let route = VectorPartitionRef::new(
            candidate.base.index.clone(),
            candidate.base.partition_key.clone(),
        );
        let mut ownership = SavedExportOwnership::new(self, &route);
        let mut catalog = candidate.catalog.clone();
        let base_graph = if base_saved {
            self.export_saved_topology(
                &route,
                schema_binding,
                &candidate.base.graph,
                candidate.base.dimension,
                candidate.base.quantization,
                &mut catalog.base,
                true,
                candidate.base.generation_id,
                candidate.base.highest_included_tx,
                candidate.base.highest_included_lsn,
                preserve_purge_identity,
                admit,
                &mut ownership,
            )?
        } else {
            candidate.base.graph.clone()
        };
        catalog.base.durable_bytes = base_graph.bytes.len() as u64;
        catalog.base.resident_bytes =
            crate::database::vector_paged_build::PAGED_GENERATION_RESIDENT_BYTES;
        let base = VectorPartitionBaseGenerationRecord {
            index: candidate.base.index.clone(),
            partition_key: candidate.base.partition_key.clone(),
            generation_id: catalog.base.generation_id,
            base_tx: candidate.base.base_tx,
            base_lsn: candidate.base.base_lsn,
            highest_included_tx: candidate.base.highest_included_tx,
            highest_included_lsn: candidate.base.highest_included_lsn,
            dimension: candidate.base.dimension,
            quantization: candidate.base.quantization,
            algorithm_version: candidate.base.algorithm_version,
            graph_format_version: candidate.base.graph_format_version,
            graph: base_graph,
            state_digest: candidate.base.state_digest,
        };
        if base_saved {
            self.install_saved_generation_objects(&VectorPartitionGenerationCandidate {
                base: base.clone(),
                change: None,
                catalog: VectorPartitionGenerationCatalogRecord {
                    change: None,
                    ..catalog.clone()
                },
            })?;
        }
        let change = candidate
            .change
            .as_ref()
            .map(|change| -> Result<_> {
                let descriptor = catalog
                    .change
                    .as_mut()
                    .ok_or_else(|| corruption("saved change has no catalog descriptor"))?;
                let graph = if change_saved {
                    self.export_saved_topology(
                        &route,
                        schema_binding,
                        &change.graph,
                        change.dimension,
                        change.quantization,
                        descriptor,
                        false,
                        base.generation_id,
                        base.highest_included_tx,
                        base.highest_included_lsn,
                        preserve_purge_identity,
                        admit,
                        &mut ownership,
                    )?
                } else {
                    change.graph.clone()
                };
                descriptor.durable_bytes = graph.bytes.len() as u64;
                descriptor.resident_bytes =
                    crate::database::vector_paged_build::PAGED_GENERATION_RESIDENT_BYTES;
                Ok(VectorPartitionChangeGenerationRecord {
                    index: change.index.clone(),
                    partition_key: change.partition_key.clone(),
                    base_generation_id: base.generation_id,
                    change_generation_id: descriptor.generation_id,
                    covered_through_tx: change.covered_through_tx,
                    covered_through_lsn: change.covered_through_lsn,
                    fresh_tail_from_tx: change.fresh_tail_from_tx,
                    fresh_tail_from_lsn: change.fresh_tail_from_lsn,
                    dimension: change.dimension,
                    quantization: change.quantization,
                    algorithm_version: change.algorithm_version,
                    graph_format_version: change.graph_format_version,
                    graph,
                })
            })
            .transpose()?;
        let paged = VectorPartitionGenerationCandidate {
            base,
            change,
            catalog,
        };
        // Only immutable generation objects become durable here. Catalog
        // visibility is still owned by the ordinary atomic publication path.
        self.install_saved_generation_objects(&paged)?;
        Ok(Some(SavedTopologyCandidate {
            candidate: paged,
            ownership: Some(ownership),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn export_saved_topology(
        &self,
        route: &VectorPartitionRef,
        schema_binding: VectorSchemaBinding,
        graph: &VectorPartitionGraphBytes,
        dimension: u32,
        quantization: VectorQuantization,
        descriptor: &mut VectorPartitionGenerationDescriptor,
        is_base: bool,
        base_generation_id: u64,
        base_tx: TxId,
        base_lsn: Lsn,
        preserve_purge_identity: bool,
        admit: &mut dyn FnMut(usize) -> Result<()>,
        ownership: &mut SavedExportOwnership,
    ) -> Result<VectorPartitionGraphBytes> {
        #[cfg(feature = "test-seams")]
        crate::vector_observations::observe_saved_topology_export(
            route,
            descriptor.covered_lsn,
            is_base,
            descriptor.generation_id,
            &graph.bytes,
        );
        let topology =
            SavedTopology::inspect(&graph.bytes, dimension as usize, quantization, &mut || {
                admit(4096)
            })?;
        if topology.m != descriptor.hnsw_m as usize
            || topology.ef_construction != descriptor.hnsw_ef_construction as usize
        {
            return Err(corruption(
                "saved topology disagrees with its catalog options",
            ));
        }
        let declaration = PagedDeclaration {
            dimension,
            quantization,
            hnsw_m: descriptor.hnsw_m,
            hnsw_ef_construction: descriptor.hnsw_ef_construction,
            hnsw_ef_search: u32::try_from(topology.ef_search)
                .map_err(|_| corruption("saved search breadth exceeds u32"))?,
            policy_revision: descriptor.policy_revision,
        };
        self.saved_export_recovery_cutoff()?;
        let record = self.begin_resident_paged_export(
            route,
            declaration.clone(),
            PagedBuildReason::NewChanges,
            descriptor.generation_id,
            0,
            descriptor.covered_tx,
            descriptor.covered_lsn,
            topology.state.points,
            schema_binding,
        )?;
        ownership.acquire(record.storage_id)?;
        let original_generation_id = descriptor.generation_id;
        // Purge has already sealed the survivor generation in its prepared
        // serving image. Keep that identity through its atomic disk publication.
        // Ordinary exports keep distinct artifact keys across failed retries.
        if !preserve_purge_identity {
            descriptor.generation_id = record.storage_id;
        }
        let base_generation_id = if is_base {
            descriptor.generation_id
        } else {
            base_generation_id
        };
        #[cfg(any(test, feature = "test-seams"))]
        {
            let generation_key = if is_base {
                Self::vector_partition_base_generation_key_from_parts(
                    &route.index,
                    &route.partition_key,
                    descriptor.generation_id,
                )
            } else {
                Self::vector_partition_change_generation_key_from_parts(
                    &route.index,
                    &route.partition_key,
                    base_generation_id,
                    descriptor.generation_id,
                )
            };
            let root_key = Self::vector_partition_graph_page_root_key(&generation_key, is_base);
            crate::vector_observations::observe_export_identity_for_test(
                original_generation_id,
                record.storage_id,
                record.storage_id,
                is_base,
                graph.checksum,
                &generation_key,
                &root_key,
            );
        }
        #[cfg(not(any(test, feature = "test-seams")))]
        let _ = original_generation_id;
        let key = Self::vector_partition_prefix(&route.index, &route.partition_key);
        let mut progress = record.progress()?;
        progress.generation_id = descriptor.generation_id;
        progress.graph = topology.state.clone();
        let plan = PagedVectorBatchPlan {
            max_points: 64,
            scratch_points: 1,
            max_node_bytes: topology.max_point_bytes.max(64),
            max_node_writes: 64,
            dirty_page_bytes: 64 * 1024,
            source_key_bytes: key.len() + 64,
            progress_bytes: 4096,
        };
        let hooks = PagedBuildHooks {
            checkpoint: Arc::new(|| Ok(())),
            inserted: Arc::new(|_| {}),
            gate: Arc::new(|work| work()),
        };
        let mut distinct_rows = 0usize;
        let mut record_bytes = 0u64;
        let mut record_digest = [0u8; 32];
        let mut page_set = None;
        let result = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            Self::validate_vector_schema_binding_in_read(
                &read,
                &route.index,
                record.schema_binding,
            )?;
            vector_membership_pages::validate_in_read(&read)?;
            let mut cursor = SavedPointCursor::default();
            loop {
                let write = db.begin_write().map_err(Self::storage_error)?;
                Self::validate_vector_schema_binding_in_write(
                    &write,
                    &route.index,
                    record.schema_binding,
                )?;
                let mut next = progress.clone();
                {
                    let mut tx = NativeBatch::open(
                        self,
                        &write,
                        route,
                        record.storage_id,
                        &declaration,
                        &progress,
                        plan,
                        &hooks,
                        None,
                    )?;
                    for _ in 0..plan.max_points {
                        let Some(point) = topology.next_point(&mut cursor)? else {
                            break;
                        };
                        admit(
                            point
                                .workspace_bytes()?
                                .saturating_mul(2)
                                .saturating_add(key.len() * 32),
                        )?;
                        let member = saved_member(
                            &read,
                            route,
                            point.row,
                            descriptor.covered_tx,
                            descriptor.covered_lsn,
                            admit,
                        )?;
                        let identity = PagedVectorIdentity {
                            point: point.id,
                            data_id: point.origin,
                            row_id: point.row,
                            lsn: member.row_lsn,
                            created_tx: member.visible_from,
                            deleted_tx: member.deleted_tx,
                            exact_key: point.exact_key(),
                        };
                        self.validate_saved_original_body(
                            &read,
                            &member,
                            &identity.exact_key,
                            dimension as usize,
                            quantization,
                            admit,
                        )?;
                        let native = point.decode()?;
                        tx.store_point(&native)?;
                        tx.store_identity(&identity)?;
                        let mut encoded = Vec::new();
                        match &native {
                            PagedVectorPoint::F32(point) => {
                                encode_paged_point_into(point, &mut encoded)
                            }
                            PagedVectorPoint::Quantized(point) => {
                                encode_paged_point_into(point, &mut encoded)
                            }
                        }
                        .map_err(|_| corruption("saved native point cannot be encoded"))?;
                        tx.seal_point(point.id, &encoded)?;
                        for byte in &encoded {
                            next.encoding_digest = (next.encoding_digest ^ u64::from(*byte))
                                .wrapping_mul(0x100000001b3);
                        }
                        next.encoding_bytes = next
                            .encoding_bytes
                            .checked_add(encoded.len() as u64)
                            .ok_or_else(|| corruption("saved export size overflow"))?;
                        next.encoded_points += 1;
                        next.encoding_cursor = Some(point.id);
                    }
                    tx.finish()?;
                }
                next.phase = PagedVectorPhase::Encoding;
                let next_record = PagedBuildRecord {
                    progress: ProgressRecord::from_progress(&next),
                    ..record.clone()
                };
                write
                    .open_table(BUILDS)
                    .map_err(Self::storage_error)?
                    .insert(
                        key.as_slice(),
                        seal_value(&key, &Self::encode(&next_record)?).as_slice(),
                    )
                    .map_err(Self::storage_error)?;
                bind_pages_root(&write)?;
                Self::write_read_bounds_roots_in_write(&write)?;
                write.commit().map_err(Self::storage_error)?;
                progress = next;
                if progress.encoded_points == topology.state.points {
                    break;
                }
            }
            // Cross-reference validation streams the original records once,
            // joining neighbours and mappings by the native physical keys.
            let validated = db.begin_read().map_err(Self::storage_error)?;
            validate_pages_root(&validated)?;
            let name = super::vector_paged::page_table_name(record.storage_id);
            let pages = validated
                .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(&name))
                .map_err(Self::storage_error)?;
            let lookup = PagedLookup {
                admission: None,
                pages: &pages,
                storage: storage_prefix(record.storage_id),
                dimension: dimension as usize,
                quantization,
                node_limit: plan.max_node_bytes,
            };
            let mut cursor = SavedPointCursor::default();
            while let Some(original) = topology.next_point(&mut cursor)? {
                admit(
                    original
                        .workspace_bytes()?
                        .saturating_mul(4)
                        .saturating_add(key.len() * 32),
                )?;
                let identity = lookup.identity(original.origin)?;
                if identity.point != original.id
                    || identity.row_id != original.row
                    || identity.exact_key != original.exact_key()
                {
                    return Err(corruption("saved identity binding is incomplete"));
                }
                let point = lookup.load_point(original.id)?;
                let edges = match &*point {
                    PagedVectorPoint::F32(p) => &p.neighbours,
                    PagedVectorPoint::Quantized(p) => &p.neighbours,
                };
                for edge in edges.iter().flatten() {
                    admit(original.workspace_bytes()?.saturating_mul(4))?;
                    let target = lookup.load_point(edge.point_id)?;
                    let origin = match &*target {
                        PagedVectorPoint::F32(p) => p.origin_id,
                        PagedVectorPoint::Quantized(p) => p.origin_id,
                    };
                    if origin != edge.origin_id {
                        return Err(corruption("saved edge origin binding is invalid"));
                    }
                }
            }
            let mut row_cursor = lookup.row_at_or_after(RowId(0))?;
            let mut prior = None;
            while let Some((row, origin, _)) = row_cursor {
                admit(plan.max_node_bytes.saturating_mul(8).saturating_add(4096))?;
                if prior != Some(row) {
                    distinct_rows += 1;
                }
                prior = Some(row);
                row_cursor = lookup.row_after(row, origin)?;
            }
            let mut posting_count = 0usize;
            topology.visit_exact(&mut |exact, row| {
                admit(exact.len().saturating_mul(8).saturating_add(4096))?;
                let found = lookup.exact_after(exact, Some(RowId(row.0.wrapping_sub(1))))?;
                // Row zero has no numeric predecessor, so start at the prefix.
                let found = if row.0 == 0 {
                    lookup.exact_after(exact, None)?
                } else {
                    found
                };
                if found.is_none_or(|(found, _)| found != row) {
                    return Err(corruption("saved exact posting differs from its topology"));
                }
                posting_count += 1;
                Ok(())
            })?;
            let storage = storage_prefix(record.storage_id);
            let prefix = kind_prefix(&storage, 4);
            let end = writer_source::prefix_end(&prefix)
                .ok_or_else(|| corruption("saved exact prefix has no successor"))?;
            let mut previous: Vec<u8> = Vec::new();
            let mut actual_postings = 0usize;
            for entry in pages
                .range::<&[u8]>(prefix.as_slice()..end.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, _) = entry.map_err(Self::storage_error)?;
                admit(key.value().len().saturating_mul(4).saturating_add(4096))?;
                let posting = key
                    .value()
                    .get(
                        ..key
                            .value()
                            .len()
                            .checked_sub(8)
                            .ok_or_else(|| corruption("saved exact key is truncated"))?,
                    )
                    .ok_or_else(|| corruption("saved exact key is invalid"))?;
                if posting != previous {
                    actual_postings += 1;
                    previous.clear();
                    previous.extend_from_slice(posting);
                }
            }
            let storage_end = writer_source::prefix_end(&storage)
                .ok_or_else(|| corruption("saved storage has no successor"))?;
            let mut hasher = blake3::Hasher::new();
            let mut page_root = PageSetHasher::new();
            for entry in pages
                .range::<&[u8]>(storage.as_slice()..storage_end.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                admit(
                    key.value()
                        .len()
                        .saturating_add(value.value().len())
                        .saturating_mul(2)
                        .saturating_add(4096),
                )?;
                hasher.update(&(key.value().len() as u64).to_be_bytes());
                hasher.update(key.value());
                hasher.update(&(value.value().len() as u64).to_be_bytes());
                hasher.update(value.value());
                page_root.page(key.value(), value.value())?;
                record_bytes = record_bytes
                    .checked_add((key.value().len() + value.value().len()) as u64)
                    .ok_or_else(|| corruption("saved record bytes overflow"))?;
            }
            record_digest = *hasher.finalize().as_bytes();
            page_set = Some(page_root.finish());
            if posting_count != actual_postings {
                return Err(corruption("saved exact directory coverage is incomplete"));
            }
            Ok(())
        });
        result?;
        if record.storage_id & super::vector_paged::MUTABLE_STORAGE != 0 {
            page_set = Some(self.paged_storage_page_set_root(record.storage_id)?);
        }
        progress.phase = PagedVectorPhase::Complete;
        progress.last_source_key = None;
        let saved = SavedGenerationBinding {
            route: *blake3::hash(&key).as_bytes(),
            is_base,
            base_generation_id,
            base_tx,
            base_lsn,
            source_digest: graph.checksum,
            distinct_rows: distinct_rows as u64,
            record_bytes,
            record_digest,
            node_limit: plan.max_node_bytes as u64,
            extend_candidates: topology.extend_candidates,
            keep_pruned: topology.keep_pruned,
            max_layer: topology.max_layer as u8,
            scale_bits: topology.scale_bits,
        };
        let meta = PagedGenerationMeta {
            magic: META_MAGIC,
            storage_id: record.storage_id,
            declaration,
            progress: ProgressRecord::from_progress(&progress),
            saved: Some(saved),
            schema_binding: record.schema_binding,
            page_set: page_set.ok_or_else(|| corruption("saved export has no page set root"))?,
        };
        let bytes = Self::encode(&meta)?;
        // The caller installs the authenticated complete object before removing
        // BUILDS. This restart record remains discoverable on every failure.
        Ok(VectorPartitionGraphBytes::verified(bytes))
    }
    fn validate_saved_original_body(
        &self,
        read: &redb::ReadTransaction,
        member: &VectorPartitionMembershipRecord,
        exact: &[u8],
        dimension: usize,
        quantization: VectorQuantization,
        admit: &mut dyn FnMut(usize) -> Result<()>,
    ) -> Result<()> {
        let key = Self::vector_identity_key(
            &member.index,
            member.row_id,
            member.vector_created_tx,
            member.vector_lsn,
        );
        let table = read
            .open_table(VECTORS_TABLE)
            .map_err(Self::storage_error)?;
        let value = table
            .get(key.as_slice())
            .map_err(Self::storage_error)?
            .ok_or_else(|| corruption("saved mapping has no original vector body"))?;
        admit(
            value
                .value()
                .len()
                .saturating_mul(8)
                .saturating_add(exact.len() * 4)
                .saturating_add(4096),
        )?;
        self.validate_raw_vector_entry_encoded_width(value.value(), &member.index, dimension)?;
        let persisted: PersistedVectorEntry = Self::decode_exact(value.value())
            .map_err(|_| corruption("saved original body is invalid"))?;
        if persisted.index != member.index
            || persisted.row_id != member.row_id
            || persisted.created_tx != member.vector_created_tx
            || persisted.lsn != member.vector_lsn
        {
            return Err(corruption("saved mapping names another body"));
        }
        let same = match (persisted.vector, quantization) {
            (PersistedVector::F32(values), VectorQuantization::F32) => {
                values.len() == dimension
                    && values
                        .iter()
                        .flat_map(|value| value.to_bits().to_be_bytes())
                        .eq(exact.iter().copied())
            }
            (
                PersistedVector::SQ8 {
                    min,
                    max,
                    len,
                    payload,
                },
                VectorQuantization::SQ8,
            )
            | (
                PersistedVector::SQ4 {
                    min,
                    max,
                    len,
                    payload,
                },
                VectorQuantization::SQ4,
            ) => {
                len as usize == dimension
                    && exact.len() == 12 + payload.len()
                    && exact[..4] == len.to_le_bytes()
                    && exact[4..8] == min.to_bits().to_le_bytes()
                    && exact[8..12] == max.to_bits().to_le_bytes()
                    && exact[12..] == payload
            }
            _ => false,
        };
        if !same {
            return Err(corruption(
                "saved exact mapping disagrees with its original body",
            ));
        }
        Ok(())
    }

    fn install_saved_generation_objects(
        &self,
        candidate: &VectorPartitionGenerationCandidate,
    ) -> Result<()> {
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            for graph in std::iter::once(&candidate.base.graph)
                .chain(candidate.change.as_ref().map(|change| &change.graph))
            {
                if graph.bytes.starts_with(&META_MAGIC) {
                    let meta = PagedGenerationMeta::decode(&graph.bytes)?;
                    Self::validate_vector_schema_binding_in_write(
                        &write,
                        &candidate.base.index,
                        meta.schema_binding,
                    )?;
                }
            }
            let base_key = Self::vector_partition_base_generation_key(&candidate.base);
            let base_bytes = Self::encode_vector_partition_record(
                VectorPartitionRecordKind::BaseGeneration,
                &candidate.base,
            )?;
            Self::insert_preencoded_immutable_vector_partition_record(
                &mut write
                    .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                    .map_err(Self::storage_error)?,
                &base_key,
                &base_bytes,
                self,
            )?;
            if let Some(change) = &candidate.change {
                let key = Self::vector_partition_change_generation_key(change);
                let bytes = Self::encode_vector_partition_record(
                    VectorPartitionRecordKind::ChangeGeneration,
                    change,
                )?;
                Self::insert_preencoded_immutable_vector_partition_record(
                    &mut write
                        .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                        .map_err(Self::storage_error)?,
                    &key,
                    &bytes,
                    self,
                )?;
            }
            let route =
                Self::vector_partition_prefix(&candidate.base.index, &candidate.base.partition_key);
            let mut builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
            let record: Option<PagedBuildRecord> = {
                let build = builds.get(route.as_slice()).map_err(Self::storage_error)?;
                match build {
                    Some(build) => Some(
                        Self::decode_exact(open_value(&route, build.value())?)
                            .map_err(|_| corruption("saved export ownership record is invalid"))?,
                    ),
                    None => None,
                }
            };
            if let Some(record) = record {
                let graph = std::iter::once(&candidate.base.graph)
                    .chain(candidate.change.as_ref().map(|change| &change.graph))
                    .find(|graph| {
                        graph.bytes.starts_with(&META_MAGIC)
                            && authenticated_native_storage_id(&graph.bytes).ok()
                                == Some(record.storage_id)
                    })
                    .ok_or_else(|| {
                        corruption("saved publication cannot transfer an unrelated build")
                    })?;
                let meta = PagedGenerationMeta::decode(&graph.bytes)?;
                let completed = meta.progress()?;
                let previous = record.progress()?;
                if completed.phase != PagedVectorPhase::Complete
                    || completed.encoded_points != previous.encoded_points
                    || completed.encoding_digest != previous.encoding_digest
                    || completed.graph != previous.graph
                    || meta.saved.is_none()
                {
                    return Err(corruption(
                        "saved publication has no validated complete export",
                    ));
                }
                let saved = meta
                    .saved
                    .as_ref()
                    .ok_or_else(|| corruption("saved export has no original binding"))?;
                let generation_key = if saved.is_base {
                    Self::vector_partition_base_generation_key(&candidate.base)
                } else {
                    Self::vector_partition_change_generation_key(
                        candidate.change.as_ref().ok_or_else(|| {
                            corruption("saved change export has no change object")
                        })?,
                    )
                };
                let owner = SavedExportRecord {
                    storage_id: record.storage_id,
                    generation_key,
                    is_base: saved.is_base,
                };
                let owner_bytes = Self::encode(&owner)?;
                write
                    .open_table(SAVED_EXPORTS)
                    .map_err(Self::storage_error)?
                    .insert(
                        record.storage_id,
                        seal_value(&record.storage_id.to_be_bytes(), &owner_bytes).as_slice(),
                    )
                    .map_err(Self::storage_error)?;
                builds
                    .remove(route.as_slice())
                    .map_err(Self::storage_error)?;
            }
            drop(builds);
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }
}
fn saved_member(
    read: &redb::ReadTransaction,
    route: &VectorPartitionRef,
    row: RowId,
    frontier: TxId,
    lsn: Lsn,
    admit: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<VectorPartitionMembershipRecord> {
    let prefix = vector_membership_pages::reverse_row_prefix(&route.index, row);
    let upper = writer_source::prefix_end(&prefix)
        .ok_or_else(|| corruption("saved membership prefix has no successor"))?;
    let table = read
        .open_table(vector_membership_pages::REVERSE)
        .map_err(RedbPersistence::storage_error)?;
    let mut selected = None;
    // A saved generation keeps a point for a row deleted at or before its
    // frontier as a tombstone; that point's membership is the version deleted
    // by then. It is chosen only when no version is live at the frontier and
    // exactly one such version exists.
    let mut tombstoned = None;
    let mut tombstoned_count = 0usize;
    for entry in table
        .range::<&[u8]>(prefix.as_slice()..upper.as_slice())
        .map_err(RedbPersistence::storage_error)?
    {
        let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
        admit(
            value
                .value()
                .len()
                .saturating_mul(64)
                .saturating_add(key.value().len())
                .saturating_add(4096),
        )?;
        let member = vector_membership_pages::decode_reverse_member(key.value(), value.value())?;
        if member.partition_key == route.partition_key
            && member.row_lsn <= lsn
            && member.visible_from <= frontier
            && member.deleted_tx.is_none_or(|tx| tx > frontier)
        {
            if selected.is_some() {
                return Err(corruption("saved row has ambiguous frontier membership"));
            }
            selected = Some(member);
        } else if member.partition_key == route.partition_key
            && member.row_lsn <= lsn
            && member.visible_from <= frontier
            && member.deleted_tx.is_some_and(|tx| tx <= frontier)
        {
            tombstoned_count += 1;
            tombstoned = Some(member);
        }
    }
    if selected.is_none() && tombstoned_count == 1 {
        selected = tombstoned;
    }
    selected.ok_or_else(|| corruption("saved point has no original frontier membership"))
}

/// Storage references owned by completed candidates until catalog publication.
/// This durable writer progress never participates in query lookup. The
/// monotone storage counter separates restart orphans from live candidates
/// using one scalar cutoff rather than a population-sized process registry.
pub(super) const SAVED_EXPORTS: TableDefinition<u64, &[u8]> =
    TableDefinition::new("vector_saved_publication_owners");
#[derive(Serialize, Deserialize)]
struct SavedExportRecord {
    storage_id: u64,
    generation_key: Vec<u8>,
    is_base: bool,
}
fn decode_saved_export(storage: u64, encoded: &[u8]) -> Result<SavedExportRecord> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    let owner: SavedExportRecord =
        RedbPersistence::decode_exact(open_value(&storage.to_be_bytes(), encoded)?)
            .map_err(|_| corruption("saved publication owner is invalid"))?;
    let suffix = if owner.is_base { 8 } else { 16 };
    if owner.storage_id != storage
        || storage == 0
        || owner.generation_key.len() < suffix
        || owner.generation_key[owner.generation_key.len() - 8..] == [0; 8]
    {
        return Err(corruption(
            "saved publication owner names another artifact identity",
        ));
    }
    Ok(owner)
}
/// Only the still-owned private artifact may be retired. Atomic catalog
/// publication removes this owner in the same transaction as selection.
fn retire_saved_export_roots_in_write(
    write: &redb::WriteTransaction,
    owner: &SavedExportRecord,
) -> Result<()> {
    let route_len = owner.generation_key.len() - if owner.is_base { 8 } else { 16 };
    let catalogs = write
        .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    if let Some(value) = catalogs
        .get(&owner.generation_key[..route_len])
        .map_err(RedbPersistence::storage_error)?
        // An owned export is unpublished by construction: publication moves
        // its owner in the catalog transaction. A catalog that no longer
        // decodes is a quarantined route and cannot select this export, so
        // the cross-check applies only to a readable catalog.
        && let Ok(payload) = vector_partition_record_payload(
            VectorPartitionRecordKind::GenerationCatalog,
            value.value(),
        )
        && let Ok(catalog) =
            RedbPersistence::decode_exact::<VectorPartitionGenerationCatalogRecord>(payload)
    {
        let selected = if owner.is_base {
            owner.generation_key[route_len..] == catalog.base.generation_id.to_be_bytes()
        } else {
            catalog.change.is_some_and(|change| {
                owner.generation_key[owner.generation_key.len() - 8..]
                    == change.generation_id.to_be_bytes()
            }) && owner.generation_key[route_len..route_len + 8]
                == catalog.base.generation_id.to_be_bytes()
        };
        if selected {
            return Err(corruption(
                "saved abandonment cannot retire a catalog-selected artifact",
            ));
        }
    }
    drop(catalogs);
    let definition = if owner.is_base {
        VECTOR_PARTITION_BASE_GENERATION_TABLE
    } else {
        VECTOR_PARTITION_CHANGE_GENERATION_TABLE
    };
    let kind = if owner.is_base {
        VectorPartitionRecordKind::BaseGeneration
    } else {
        VectorPartitionRecordKind::ChangeGeneration
    };
    let mut objects = write
        .open_table(definition)
        .map_err(RedbPersistence::storage_error)?;
    let matches = match objects
        .get(owner.generation_key.as_slice())
        .map_err(RedbPersistence::storage_error)?
    {
        Some(value) => {
            let payload =
                vector_partition_record_payload(kind, value.value()).map_err(corruption)?;
            let graph = if owner.is_base {
                RedbPersistence::decode_exact::<VectorPartitionBaseGenerationRecord>(payload)?.graph
            } else {
                RedbPersistence::decode_exact::<VectorPartitionChangeGenerationRecord>(payload)?
                    .graph
            };
            graph.bytes.starts_with(&META_MAGIC)
                && authenticated_native_storage_id(&graph.bytes)? == owner.storage_id
        }
        None => false,
    };
    if matches {
        objects
            .remove(owner.generation_key.as_slice())
            .map_err(RedbPersistence::storage_error)?;
        drop(objects);
        // A saved native envelope is a bounded manifest, not a population of
        // graph nodes. Its ordinary root pages retire with this exact key;
        // native point records remain in the existing finite cleanup queue.
        RedbPersistence::remove_vector_partition_graph_pages_in_write(
            write,
            &owner.generation_key,
            owner.is_base,
        )?;
    }
    Ok(())
}
pub(super) fn saved_export_owned_in_write(
    write: &redb::WriteTransaction,
    storage: u64,
) -> Result<bool> {
    let exports = write
        .open_table(SAVED_EXPORTS)
        .map_err(RedbPersistence::storage_error)?;
    let value = exports
        .get(storage)
        .map_err(RedbPersistence::storage_error)?;
    if let Some(value) = value {
        decode_saved_export(storage, value.value())?;
        return Ok(true);
    }
    Ok(false)
}
pub(super) fn release_saved_export_in_write(
    write: &redb::WriteTransaction,
    storage: u64,
) -> Result<()> {
    if saved_export_owned_in_write(write, storage)? {
        write
            .open_table(SAVED_EXPORTS)
            .map_err(RedbPersistence::storage_error)?
            .remove(storage)
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}
pub(super) fn recover_saved_exports_in_write(
    write: &redb::WriteTransaction,
    cutoff: u64,
    admit: &mut dyn FnMut(usize) -> Result<()>,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let mut exports = write
        .open_table(SAVED_EXPORTS)
        .map_err(RedbPersistence::storage_error)?;
    for _ in 0..64 {
        checkpoint()?;
        let next = {
            let entry = exports
                .range(..cutoff)
                .map_err(RedbPersistence::storage_error)?
                .next()
                .transpose()
                .map_err(RedbPersistence::storage_error)?;
            let entry = if entry.is_some() {
                entry
            } else {
                let start = super::vector_paged::MUTABLE_STORAGE;
                let end = start
                    .checked_add(cutoff)
                    .filter(|end| *end <= 2 * start)
                    .ok_or_else(|| corruption("saved native recovery cutoff is invalid"))?;
                exports
                    .range(start..end)
                    .map_err(RedbPersistence::storage_error)?
                    .next()
                    .transpose()
                    .map_err(RedbPersistence::storage_error)?
            };
            match entry {
                Some((id, value)) => {
                    admit(value.value().len().saturating_add(4096))?;
                    let storage = id.value();
                    let owner = decode_saved_export(storage, value.value())?;
                    Some(owner)
                }
                None => None,
            }
        };
        let Some(owner) = next else {
            break;
        };
        let storage = owner.storage_id;
        retire_saved_export_roots_in_write(write, &owner)?;
        exports
            .remove(storage)
            .map_err(RedbPersistence::storage_error)?;
        write
            .open_table(CLEANUP)
            .map_err(RedbPersistence::storage_error)?
            .insert(storage, [].as_slice())
            .map_err(RedbPersistence::storage_error)?;
    }
    Ok(())
}
impl RedbPersistence {
    pub(super) fn has_recoverable_saved_exports(&self) -> Result<bool> {
        let cutoff = self.saved_export_recovery_cutoff()?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            match read.open_table(SAVED_EXPORTS) {
                Ok(exports) => {
                    if exports
                        .range(..cutoff)
                        .map_err(Self::storage_error)?
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .is_some()
                    {
                        return Ok(true);
                    }
                    let start = super::vector_paged::MUTABLE_STORAGE;
                    let end = start
                        .checked_add(cutoff)
                        .filter(|end| *end <= 2 * start)
                        .ok_or_else(|| corruption("saved native recovery cutoff is invalid"))?;
                    Ok(exports
                        .range(start..end)
                        .map_err(Self::storage_error)?
                        .next()
                        .transpose()
                        .map_err(Self::storage_error)?
                        .is_some())
                }
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(false),
                Err(error) => Err(Self::storage_error(error)),
            }
        })
    }
}

pub(crate) struct SavedTopologyCandidate {
    pub(crate) candidate: VectorPartitionGenerationCandidate,
    pub(crate) ownership: Option<SavedExportOwnership>,
}

/// A completed export remains private until its catalog transaction releases
/// the durable owner. Dropping this candidate releases only exports acquired
/// here; a published object has already transferred that owner atomically.
pub(crate) struct SavedExportOwnership {
    db: Arc<std::sync::RwLock<Option<redb::Database>>>,
    companion: Arc<std::sync::RwLock<Option<CompanionGuard>>>,
    failure: Arc<parking_lot::Mutex<Option<String>>>,
    route: Vec<u8>,
    storage: [Option<u64>; 2],
}
impl SavedExportOwnership {
    fn new(persistence: &RedbPersistence, route: &VectorPartitionRef) -> Self {
        Self {
            db: persistence.db.clone(),
            companion: persistence.lock_file.clone(),
            failure: persistence.saved_export_abandonment_error.clone(),
            route: RedbPersistence::vector_partition_prefix(&route.index, &route.partition_key),
            storage: [None; 2],
        }
    }
    fn acquire(&mut self, storage: u64) -> Result<()> {
        let slot = self
            .storage
            .iter_mut()
            .find(|id| id.is_none())
            .ok_or_else(|| corruption("saved candidate has too many exports"))?;
        *slot = Some(storage);
        Ok(())
    }
    fn abandon(&self) -> Result<()> {
        let companion = self
            .companion
            .read()
            .map_err(RedbPersistence::storage_error)?;
        if companion.is_none() {
            return Err(Error::Other("database persistence is closed".into()));
        }
        let database = self.db.read().map_err(RedbPersistence::storage_error)?;
        let db = database
            .as_ref()
            .ok_or_else(|| Error::Other("database persistence is closed".into()))?;
        let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
        let mut changed = false;
        for storage in self.storage.into_iter().flatten() {
            let mut builds = write
                .open_table(BUILDS)
                .map_err(RedbPersistence::storage_error)?;
            let owned_build = match builds
                .get(self.route.as_slice())
                .map_err(RedbPersistence::storage_error)?
            {
                Some(value) => {
                    let record: PagedBuildRecord =
                        RedbPersistence::decode_exact(open_value(&self.route, value.value())?)
                            .map_err(|_| corruption("saved abandonment build record is invalid"))?;
                    record.storage_id == storage
                }
                None => false,
            };
            if owned_build {
                builds
                    .remove(self.route.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
            }
            drop(builds);
            let owned_export = saved_export_owned_in_write(&write, storage)?;
            if owned_build || owned_export {
                if owned_export {
                    let exports = write
                        .open_table(SAVED_EXPORTS)
                        .map_err(RedbPersistence::storage_error)?;
                    let value = exports
                        .get(storage)
                        .map_err(RedbPersistence::storage_error)?
                        .ok_or_else(|| corruption("saved publication owner disappeared"))?;
                    let owner = decode_saved_export(storage, value.value())?;
                    retire_saved_export_roots_in_write(&write, &owner)?;
                }
                release_saved_export_in_write(&write, storage)?;
                write
                    .open_table(CLEANUP)
                    .map_err(RedbPersistence::storage_error)?
                    .insert(storage, [].as_slice())
                    .map_err(RedbPersistence::storage_error)?;
                changed = true;
            }
        }
        if changed {
            bind_pages_root(&write)?;
            RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(RedbPersistence::storage_error)?;
        }
        Ok(())
    }
}
impl Drop for SavedExportOwnership {
    fn drop(&mut self) {
        if self.storage.iter().all(Option::is_none) {
            return;
        }
        if let Err(error) = self.abandon() {
            // Drop cannot return an I/O error. Preserve it for the next normal
            // persistence operation; the unchanged durable owner is recovered
            // after reopening if the write could not commit.
            let mut failure = self.failure.lock();
            if failure.is_none() {
                *failure = Some(error.to_string());
            }
        }
    }
}

impl RedbPersistence {
    /// Existing inspection callers expect the canonical durable envelope.
    /// This materialization is absent from every production-only build.
    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn materialize_paged_topology_for_test(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        fn u64_into(bytes: &mut Vec<u8>, value: usize) {
            bytes.extend_from_slice(&(value as u64).to_le_bytes());
        }
        fn point_id_into(bytes: &mut Vec<u8>, id: contextdb_vector::hnsw::paged_builder::PointId) {
            bytes.push(id.0);
            bytes.extend_from_slice(&id.1.to_le_bytes());
        }
        fn point_into<T>(
            out: &mut Vec<u8>,
            point: &contextdb_vector::hnsw::paged_builder::PagedPoint<T>,
            values: impl FnOnce(&mut Vec<u8>, &[T]),
        ) {
            u64_into(out, point.origin_id);
            point_id_into(out, point.id);
            u64_into(out, point.values.len());
            values(out, &point.values);
            u64_into(out, point.neighbours.len());
            for layer in &point.neighbours {
                u64_into(out, layer.len());
                for edge in layer {
                    u64_into(out, edge.origin_id);
                    point_id_into(out, edge.point_id);
                    out.extend_from_slice(&edge.distance.to_bits().to_le_bytes());
                }
            }
        }
        let meta = PagedGenerationMeta::decode(bytes)?;
        let progress = meta.progress()?;
        if progress.phase != PagedVectorPhase::Complete {
            return Err(corruption(
                "inspection requires a complete paged generation",
            ));
        }
        let quantized = meta.declaration.quantization != VectorQuantization::F32;
        let (extend, pruned, max_layer, scale) = meta
            .saved
            .as_ref()
            .map(|saved| {
                (
                    saved.extend_candidates,
                    saved.keep_pruned,
                    saved.max_layer as usize,
                    saved.scale_bits,
                )
            })
            .unwrap_or((
                quantized,
                quantized,
                16,
                (1.0 / (meta.declaration.hnsw_m as f64).ln()).to_bits(),
            ));
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            validate_pages_root(&read)?;
            let name = super::vector_paged::page_table_name(meta.storage_id);
            let pages = read
                .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(&name))
                .map_err(Self::storage_error)?;
            let lookup = PagedLookup {
                admission: None,
                pages: &pages,
                storage: storage_prefix(meta.storage_id),
                dimension: meta.declaration.dimension as usize,
                quantization: meta.declaration.quantization,
                node_limit: meta
                    .saved
                    .as_ref()
                    .map_or(usize::MAX, |saved| saved.node_limit as usize),
            };
            let mut envelope = b"CDHNSW\0\x01".to_vec();
            envelope.extend_from_slice(&1u16.to_le_bytes());
            envelope.extend_from_slice(&0u64.to_le_bytes());
            u64_into(&mut envelope, lookup.dimension);
            envelope.push(match lookup.quantization {
                VectorQuantization::F32 => 0,
                VectorQuantization::SQ8 => 1,
                VectorQuantization::SQ4 => 2,
            });
            u64_into(&mut envelope, meta.declaration.hnsw_ef_search as usize);
            let length_offset = envelope.len();
            u64_into(&mut envelope, 0);
            let graph_offset = envelope.len();
            envelope.extend_from_slice(&1u16.to_le_bytes());
            u64_into(&mut envelope, meta.declaration.hnsw_m as usize);
            u64_into(
                &mut envelope,
                meta.declaration.hnsw_ef_construction as usize,
            );
            envelope.push(u8::from(extend));
            envelope.push(u8::from(pruned));
            u64_into(&mut envelope, max_layer);
            let values = match lookup.quantization {
                VectorQuantization::F32 => lookup.dimension,
                VectorQuantization::SQ8 => 12 + lookup.dimension,
                VectorQuantization::SQ4 => 12 + lookup.dimension.div_ceil(2),
            };
            u64_into(&mut envelope, values);
            envelope.extend_from_slice(&scale.to_le_bytes());
            u64_into(&mut envelope, max_layer);
            let mut rows = std::collections::BTreeMap::new();
            let mut exact: std::collections::BTreeMap<Vec<u8>, std::collections::BTreeSet<u64>> =
                std::collections::BTreeMap::new();
            for layer in 0..max_layer {
                u64_into(&mut envelope, progress.graph.layer_counts[layer]);
                for rank in 0..progress.graph.layer_counts[layer] {
                    let id =
                        contextdb_vector::hnsw::paged_builder::PointId(layer as u8, rank as i32);
                    let point = lookup.load_point(id)?;
                    let origin = match &*point {
                        PagedVectorPoint::F32(point) => {
                            point_into(&mut envelope, point, |out, values| {
                                for value in values {
                                    out.extend_from_slice(&value.to_bits().to_le_bytes());
                                }
                            });
                            point.origin_id
                        }
                        PagedVectorPoint::Quantized(point) => {
                            point_into(&mut envelope, point, |out, values| {
                                out.extend_from_slice(values)
                            });
                            point.origin_id
                        }
                    };
                    let identity = lookup.identity(origin)?;
                    if identity.point != id || rows.insert(origin, identity.row_id).is_some() {
                        return Err(corruption("inspection point has an invalid identity"));
                    }
                }
            }
            let prefix = kind_prefix(&lookup.storage, 4);
            let end = writer_source::prefix_end(&prefix)
                .ok_or_else(|| corruption("inspection exact directory has no successor"))?;
            for entry in pages
                .range::<&[u8]>(prefix.as_slice()..end.as_slice())
                .map_err(Self::storage_error)?
            {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if key.len() < 29 || !open_value(key, value.value())?.is_empty() {
                    return Err(corruption("inspection exact posting is invalid"));
                }
                let size = u32::from_be_bytes(key[9..13].try_into().unwrap()) as usize;
                if size.checked_add(29) != Some(key.len()) {
                    return Err(corruption("inspection exact posting length is invalid"));
                }
                let row = u64::from_be_bytes(key[13 + size..21 + size].try_into().unwrap());
                exact
                    .entry(key[13..13 + size].to_vec())
                    .or_default()
                    .insert(row);
            }
            match progress.graph.entry_point {
                None => envelope.push(0),
                Some(id) => {
                    envelope.push(1);
                    point_id_into(&mut envelope, id);
                }
            }
            let graph_length = envelope.len() - graph_offset;
            envelope[length_offset..graph_offset]
                .copy_from_slice(&(graph_length as u64).to_le_bytes());
            u64_into(&mut envelope, rows.len());
            for (origin, row) in rows {
                u64_into(&mut envelope, origin);
                envelope.extend_from_slice(&row.0.to_le_bytes());
            }
            u64_into(&mut envelope, exact.len());
            for (key, rows) in exact {
                u64_into(&mut envelope, key.len());
                envelope.extend_from_slice(&key);
                u64_into(&mut envelope, rows.len());
                for row in rows {
                    envelope.extend_from_slice(&row.to_le_bytes());
                }
            }
            let checksum = envelope[18..]
                .iter()
                .fold(0xcbf2_9ce4_8422_2325u64, |sum, byte| {
                    (sum ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
                });
            envelope[10..18].copy_from_slice(&checksum.to_le_bytes());
            contextdb_vector::hnsw::saved_topology::SavedTopology::inspect(
                &envelope,
                lookup.dimension,
                lookup.quantization,
                &mut || Ok(()),
            )?;
            if let Some(saved) = &meta.saved
                && blake3::hash(&envelope).as_bytes() != &saved.source_digest
            {
                return Err(corruption(
                    "inspection differs from the original saved topology",
                ));
            }
            Ok(envelope)
        })
    }
}
