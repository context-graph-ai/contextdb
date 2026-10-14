//! Atomic native tail updates for a logical import's newly committed records.
use super::*;
use contextdb_vector::hnsw::paged_builder::{PagedVectorCommittedBatch, PagedVectorPersistence};

pub(crate) struct NativeImportTail {
    pub(crate) persistence: Arc<RedbPersistence>,
    pub(crate) partition: VectorPartitionRef,
    pub(crate) storage_id: u64,
    pub(crate) declaration: PagedDeclaration,
    pub(crate) schema_binding: VectorSchemaBinding,
    pub(crate) catalog: parking_lot::Mutex<VectorPartitionGenerationCatalogRecord>,
    pub(crate) hooks: PagedBuildHooks,
}

impl NativeImportTail {
    pub(crate) fn source(
        &self,
        meta: &PagedGenerationMeta,
    ) -> Result<Arc<dyn PagedVectorGraphSource>> {
        // Capture the operation's one selected root when its reader opens.
        // The physical root must match these controls before any node is read.
        let capture = self.persistence.paged_root_capture()?;
        let progress = meta.progress()?;
        let node_limit = crate::database::vector_paged_build::paged_batch_plan(&self.declaration)?
            .max_node_bytes;
        Ok(Arc::new(NativePagedGraphSource {
            capture,
            index: self.partition.index.clone(),
            schema_binding: self.schema_binding,
            storage_id: self.storage_id,
            declaration: self.declaration.clone(),
            frontier: progress.frontier_tx,
            points: progress.graph.points,
            rows: meta
                .saved
                .as_ref()
                .map_or(progress.graph.points, |saved| saved.distinct_rows as usize),
            node_limit,
            checkpoint: Arc::clone(&self.hooks.checkpoint),
            page_set: meta.page_set,
            verified: Arc::new(parking_lot::Mutex::new(false)),
        }))
    }
}

impl PagedVectorPersistence for NativeImportTail {
    fn commit_batch(
        &self,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        batch: &mut dyn FnMut(&mut dyn PagedVectorTransaction) -> Result<PagedVectorProgress>,
    ) -> Result<PagedVectorCommittedBatch> {
        let expected = self.catalog.lock().clone();
        let catalog_key = RedbPersistence::vector_partition_catalog_key(&expected);
        let mut committed = None;
        (self.hooks.gate)(&mut || {
            self.persistence.with_db(|db| {
                let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
                RedbPersistence::validate_vector_schema_binding_in_write(
                    &write,
                    &self.partition.index,
                    self.schema_binding,
                )?;
                let actual: VectorPartitionGenerationCatalogRecord = {
                    let catalogs = write
                        .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    let value = catalogs
                        .get(catalog_key.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .ok_or_else(|| corruption("native tail lost its base catalog"))?;
                    self.persistence.decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::GenerationCatalog,
                        value.value(),
                        &self.partition.index,
                        &self.partition.partition_key,
                    )?
                };
                if actual != expected || previous.generation_id != self.storage_id {
                    return Err(Error::Other(
                        "native tail publication was superseded".into(),
                    ));
                }
                if let Some(change) = actual.change {
                    let key = RedbPersistence::vector_partition_change_generation_key_from_parts(
                        &self.partition.index,
                        &self.partition.partition_key,
                        actual.base.generation_id,
                        change.generation_id,
                    );
                    let changes = write
                        .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    let value = changes
                        .get(key.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .ok_or_else(|| corruption("native tail lost its change record"))?;
                    let record: VectorPartitionChangeGenerationRecord =
                        self.persistence.decode_expected_vector_partition_metadata(
                            VectorPartitionRecordKind::ChangeGeneration,
                            value.value(),
                            &self.partition.index,
                            &self.partition.partition_key,
                        )?;
                    let saved_meta = PagedGenerationMeta::decode(&record.graph.bytes)?;
                    let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
                    let physical = resolve_native_change_backing(&read, &saved_meta)?;
                    let mut saved = physical.progress()?;
                    saved.generation_id = self.storage_id;
                    saved.phase = PagedVectorPhase::Inserting;
                    saved.encoded_points = previous.encoded_points;
                    if saved != *previous {
                        return Err(Error::Other("native tail progress is stale".into()));
                    }
                } else if previous.graph.points != 0 {
                    return Err(corruption("unpublished native tail has points"));
                }
                let next = {
                    let mut transaction = NativeBatch::open(
                        &self.persistence,
                        &write,
                        &self.partition,
                        self.storage_id,
                        &self.declaration,
                        previous,
                        plan,
                        &self.hooks,
                        None,
                    )?;
                    let next = batch(&mut transaction)?;
                    transaction.finish()?;
                    next
                };
                let page_name = page_table_name(self.storage_id);
                let table = TableDefinition::<&[u8], &[u8]>::new(&page_name);
                let distinct_rows = {
                    let pages = write
                        .open_table(table)
                        .map_err(RedbPersistence::storage_error)?;
                    let mut key = storage_prefix(self.storage_id).to_vec();
                    key.push(6);
                    pages
                        .get(key.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .map(|value| {
                            let payload = open_value(&key, value.value())?;
                            let bytes: [u8; 8] = payload.try_into().map_err(|_| {
                                corruption("native row counter has the wrong width")
                            })?;
                            Ok::<_, Error>(u64::from_be_bytes(bytes))
                        })
                        .transpose()?
                        .unwrap_or(0)
                };
                let physical_root = write
                    .table_root_fingerprint(table)
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| corruption("native tail page table disappeared"))?;
                let page_bytes = {
                    let pages = write
                        .open_table(table)
                        .map_err(RedbPersistence::storage_error)?;
                    native_bytes_in(&pages, self.storage_id)?
                };
                let page_set = PageSetRoot {
                    pages: MUTABLE_PAGE_SET,
                    bytes: page_bytes,
                    digest: physical_root,
                };
                let mut serving = next.clone();
                serving.generation_id = actual
                    .change
                    .map_or(self.storage_id, |change| change.generation_id);
                serving.phase = PagedVectorPhase::Complete;
                serving.encoded_points = serving.graph.points;
                let meta_bytes = RedbPersistence::encode_paged_generation_meta(
                    self.storage_id,
                    &self.declaration,
                    &serving,
                    self.schema_binding,
                    page_set,
                )?;
                let mut meta = PagedGenerationMeta::decode(&meta_bytes)?;
                let route = RedbPersistence::vector_partition_prefix(
                    &self.partition.index,
                    &self.partition.partition_key,
                );
                meta.saved = Some(SavedGenerationBinding {
                    route: *blake3::hash(&route).as_bytes(),
                    is_base: false,
                    base_generation_id: actual.base.generation_id,
                    base_tx: actual.base.covered_tx,
                    base_lsn: actual.base.covered_lsn,
                    source_digest: physical_root,
                    distinct_rows,
                    record_bytes: 0,
                    record_digest: [0; 32],
                    node_limit: plan.max_node_bytes as u64,
                    extend_candidates: self.declaration.quantization != VectorQuantization::F32,
                    keep_pruned: self.declaration.quantization != VectorQuantization::F32,
                    max_layer: 16,
                    scale_bits: (1.0 / f64::ln(self.declaration.hnsw_m as f64)).to_bits(),
                });
                let meta_bytes = RedbPersistence::encode(&meta)?;
                let record = VectorPartitionChangeGenerationRecord {
                    index: self.partition.index.clone(),
                    partition_key: self.partition.partition_key.clone(),
                    base_generation_id: actual.base.generation_id,
                    change_generation_id: serving.generation_id,
                    covered_through_tx: next.frontier_tx,
                    covered_through_lsn: next.frontier_lsn,
                    fresh_tail_from_tx: next.frontier_tx,
                    fresh_tail_from_lsn: next.frontier_lsn,
                    dimension: self.declaration.dimension,
                    quantization: self.declaration.quantization,
                    algorithm_version: crate::database::VECTOR_GRAPH_ALGORITHM_VERSION,
                    graph_format_version: crate::database::VECTOR_GRAPH_FORMAT_VERSION,
                    graph: VectorPartitionGraphBytes::verified(meta_bytes),
                };
                if let Some(change) = actual.change {
                    let old_key =
                        RedbPersistence::vector_partition_change_generation_key_from_parts(
                            &self.partition.index,
                            &self.partition.partition_key,
                            actual.base.generation_id,
                            change.generation_id,
                        );
                    let old_storage = {
                        let changes = write
                            .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        let value = changes
                            .get(old_key.as_slice())
                            .map_err(RedbPersistence::storage_error)?
                            .ok_or_else(|| corruption("native suffix lost its prior change"))?;
                        RedbPersistence::authenticated_generation_storage(
                            value.value(),
                            &old_key,
                            false,
                        )?
                    };
                    if let Some(old_storage) = old_storage.filter(|old| *old != self.storage_id) {
                        let mut backings = write
                            .open_table(CHANGE_BACKINGS)
                            .map_err(RedbPersistence::storage_error)?;
                        backings
                            .remove(old_storage.to_be_bytes().as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                        write
                            .open_table(CLEANUP)
                            .map_err(RedbPersistence::storage_error)?
                            .insert(old_storage, [].as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                }
                let key = RedbPersistence::vector_partition_change_generation_key(&record);
                let bytes = RedbPersistence::encode_vector_partition_record(
                    VectorPartitionRecordKind::ChangeGeneration,
                    &record,
                )?;
                let prepared = RedbPersistence::prepare_vector_partition_graph_pages_with_binding(
                    &record.index,
                    &record.partition_key,
                    record.base_generation_id,
                    record.change_generation_id,
                    false,
                    &key,
                    &record.graph,
                    self.schema_binding,
                )?;
                // Only this native mutable format permits replacing controls
                // at one key. The catalog and the changed point pages commit
                // together; older captured roots retain their predecessor.
                write
                    .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                    .map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
                {
                    let mut pages = write
                        .open_table(VECTOR_PARTITION_GRAPH_PAGE_TABLE)
                        .map_err(RedbPersistence::storage_error)?;
                    for (ordinal, body) in
                        prepared.graph_bytes.chunks(prepared.page_bytes).enumerate()
                    {
                        let key = RedbPersistence::vector_partition_graph_page_key(
                            &prepared.root_key,
                            ordinal as u32,
                        );
                        let mut bytes = Vec::with_capacity(
                            body.len() + VECTOR_PARTITION_GRAPH_PAGE_HEADER_BYTES,
                        );
                        bytes.extend_from_slice(&(ordinal as u32).to_le_bytes());
                        bytes.extend_from_slice(blake3::hash(body).as_bytes());
                        bytes.extend_from_slice(body);
                        pages
                            .insert(key.as_slice(), bytes.as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                    pages
                        .insert(
                            prepared.root_key.as_slice(),
                            prepared.manifest_bytes.as_slice(),
                        )
                        .map_err(RedbPersistence::storage_error)?;
                }
                let mut catalog = actual;
                catalog.change = Some(VectorPartitionGenerationDescriptor {
                    generation_id: serving.generation_id,
                    covered_tx: next.frontier_tx,
                    covered_lsn: next.frontier_lsn,
                    resident_bytes:
                        crate::database::vector_paged_build::PAGED_GENERATION_RESIDENT_BYTES,
                    durable_bytes: record.graph.bytes.len() as u64,
                    policy_revision: self.declaration.policy_revision,
                    hnsw_m: self.declaration.hnsw_m,
                    hnsw_ef_construction: self.declaration.hnsw_ef_construction,
                });
                catalog.published_tx = next.frontier_tx;
                catalog.published_lsn = next.frontier_lsn;
                // A logical import can first acquire its tail while the
                // replacement is already queued. Rebase only its publication
                // expectation; its sampled source and durable progress remain.
                if expected.change.is_none() {
                    let route = RedbPersistence::vector_partition_prefix(
                        &self.partition.index,
                        &self.partition.partition_key,
                    );
                    let mut builds = write
                        .open_table(BUILDS)
                        .map_err(RedbPersistence::storage_error)?;
                    let active = builds
                        .get(route.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .map(|value| {
                            RedbPersistence::decode_exact::<PagedBuildRecord>(open_value(
                                &route,
                                value.value(),
                            )?)
                        })
                        .transpose()?;
                    if let Some(mut active) = active {
                        if active.previous_generation_id != expected.base.generation_id {
                            return Err(corruption(
                                "native first tail disagrees with replacement predecessor",
                            ));
                        }
                        active.previous_generation_id =
                            self.storage_id.max(catalog.base.generation_id);
                        let payload = RedbPersistence::encode(&active)?;
                        builds
                            .insert(route.as_slice(), seal_value(&route, &payload).as_slice())
                            .map_err(RedbPersistence::storage_error)?;
                    }
                }
                // This commit covers exactly the journal range it absorbed,
                // (previous frontier, next frontier]. Count what that range
                // retires from the maintained counters -- its upserts still
                // live, its tombstones -- so changes committed after it stay
                // pending. The range is one import cohort, already admitted
                // by the caller, so its identity set is cohort-bounded.
                let (absorbed_live_inserts, absorbed_tombstones) = {
                    let mut live_inserts = 0u64;
                    let mut tombstones = 0u64;
                    if let Some((lower, upper)) =
                        RedbPersistence::vector_partition_journal_suffix_bounds(
                            &self.partition.index,
                            &self.partition.partition_key,
                            previous.frontier_lsn,
                            Some(next.frontier_lsn),
                        )?
                    {
                        let outside = |tx: TxId| {
                            tx <= previous.frontier_tx || tx > next.frontier_tx
                        };
                        let mut upserted = std::collections::BTreeSet::new();
                        {
                            let journal = write
                                .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
                                .map_err(RedbPersistence::storage_error)?;
                            for entry in journal
                                .range(lower.as_slice()..upper.as_slice())
                                .map_err(RedbPersistence::storage_error)?
                            {
                                let (_, value) = entry.map_err(RedbPersistence::storage_error)?;
                                let record: VectorPartitionJournalRecord =
                                    self.persistence.decode_expected_vector_partition_metadata(
                                        VectorPartitionRecordKind::Journal,
                                        value.value(),
                                        &self.partition.index,
                                        &self.partition.partition_key,
                                    )?;
                                match record.change {
                                    VectorPartitionJournalChange::Upsert(membership) => {
                                        if outside(membership.visible_from) {
                                            return Err(corruption(
                                                "native tail cohort is not its own committed transaction range",
                                            ));
                                        }
                                        upserted.insert(
                                            RedbPersistence::vector_partition_membership_key(
                                                &membership,
                                            ),
                                        );
                                    }
                                    VectorPartitionJournalChange::Tombstone(tombstone) => {
                                        if outside(tombstone.deleted_tx) {
                                            return Err(corruption(
                                                "native tail cohort is not its own committed transaction range",
                                            ));
                                        }
                                        tombstones += 1;
                                    }
                                }
                            }
                        }
                        let memberships = write
                            .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                            .map_err(RedbPersistence::storage_error)?;
                        for key in upserted {
                            let Some(value) = memberships
                                .get(key.as_slice())
                                .map_err(RedbPersistence::storage_error)?
                            else {
                                continue;
                            };
                            let membership: VectorPartitionMembershipRecord =
                                self.persistence.decode_expected_vector_partition_metadata(
                                    VectorPartitionRecordKind::Membership,
                                    value.value(),
                                    &self.partition.index,
                                    &self.partition.partition_key,
                                )?;
                            if membership.deleted_tx.is_none() {
                                live_inserts += 1;
                            }
                        }
                    }
                    (live_inserts, tombstones)
                };
                vector_membership_pages::publish_native_tail_frontier(
                    &write,
                    &self.partition,
                    next.frontier_tx,
                    catalog.base.covered_tx,
                    previous.frontier_tx,
                    absorbed_live_inserts,
                    absorbed_tombstones,
                )?;
                let bytes = RedbPersistence::encode_vector_partition_record(
                    VectorPartitionRecordKind::GenerationCatalog,
                    &catalog,
                )?;
                write
                    .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .map_err(RedbPersistence::storage_error)?
                    .insert(catalog_key.as_slice(), bytes.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
                // The global inventory keeps the storage discoverable even
                // if its catalog is later lost or quarantined.
                let mut marker = storage_prefix(self.storage_id).to_vec();
                marker.push(0);
                let route = RedbPersistence::vector_partition_prefix(
                    &self.partition.index,
                    &self.partition.partition_key,
                );
                let mut marker_value = (route.len() as u32).to_be_bytes().to_vec();
                marker_value.extend_from_slice(&route);
                marker_value.extend_from_slice(&physical_root);
                write
                    .open_table(PAGES)
                    .map_err(RedbPersistence::storage_error)?
                    .insert(
                        marker.as_slice(),
                        seal_value(&marker, &marker_value).as_slice(),
                    )
                    .map_err(RedbPersistence::storage_error)?;
                bind_pages_root(&write)?;
                RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                (self.hooks.checkpoint)()?;
                #[cfg(feature = "test-seams")]
                crate::writer_commit_test_seams::reached(
                    crate::writer_commit_test_seams::CommitBoundary::GenerationBeforeRedb,
                );
                write.commit().map_err(RedbPersistence::storage_error)?;
                committed = Some((
                    next,
                    catalog,
                    PagedGenerationMeta::decode(&record.graph.bytes)?,
                ));
                Ok(())
            })
        })?;
        let (progress, catalog, meta) =
            committed.ok_or_else(|| Error::Other("native tail batch did not commit".into()))?;
        *self.catalog.lock() = catalog;
        Ok(PagedVectorCommittedBatch {
            progress,
            source: self.source(&meta)?,
        })
    }
}

pub(super) fn source_identity_key(storage: u64, row: RowId, created: TxId, lsn: Lsn) -> Vec<u8> {
    let mut key = storage.to_be_bytes().to_vec();
    key.push(5);
    key.extend_from_slice(&row.0.to_be_bytes());
    key.extend_from_slice(&created.0.to_be_bytes());
    key.extend_from_slice(&lsn.0.to_be_bytes());
    key
}

impl RedbPersistence {
    pub(crate) fn allocate_native_tail_storage(&self) -> Result<u64> {
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            let next = {
                let mut counters = write.open_table(COUNTERS).map_err(Self::storage_error)?;
                let next = counters
                    .get(NEXT_STORAGE)
                    .map_err(Self::storage_error)?
                    .map_or(1, |value| value.value());
                if next == 0 || next >= MUTABLE_STORAGE {
                    return Err(Error::Other(
                        "native vector storage identities are exhausted".into(),
                    ));
                }
                counters
                    .insert(NEXT_STORAGE, next + 1)
                    .map_err(Self::storage_error)?;
                next
            };
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)?;
            Ok(next | MUTABLE_STORAGE)
        })
    }

    pub(crate) fn native_tail_entries_not_stored(
        &self,
        storage: u64,
        entries: &mut Vec<VectorEntry>,
        meta: Option<&PagedGenerationMeta>,
    ) -> Result<()> {
        let Some(meta) = meta else {
            return Ok(());
        };
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let page_name = page_table_name(storage);
            let table = TableDefinition::<&[u8], &[u8]>::new(&page_name);
            if read
                .table_root_fingerprint(table)
                .map_err(Self::storage_error)?
                != Some(meta.page_set.digest)
            {
                return Err(corruption(
                    "native tail preparation found a changed page root",
                ));
            }
            let pages = read.open_table(table).map_err(Self::storage_error)?;
            let mut failure = None;
            entries.retain(|entry| {
                let key = source_identity_key(storage, entry.row_id, entry.created_tx, entry.lsn);
                match pages
                    .get(key.as_slice())
                    .map_err(Self::storage_error)
                    .and_then(|value| {
                        value
                            .map(|value| {
                                let payload = open_value(&key, value.value())?;
                                if payload.len() != 8 {
                                    return Err(corruption(
                                        "native source locator has the wrong width",
                                    ));
                                }
                                Ok(())
                            })
                            .transpose()
                    }) {
                    Ok(Some(())) => false,
                    Ok(None) => true,
                    Err(error) => {
                        failure.get_or_insert(error);
                        true
                    }
                }
            });
            failure.map_or(Ok(()), Err)
        })
    }
}

impl RedbPersistence {
    pub(crate) fn truncate_native_tail_journal_in_write(
        &self,
        write: &redb::WriteTransaction,
        catalog: &VectorPartitionGenerationCatalogRecord,
        checkpoint: &mut impl FnMut() -> Result<()>,
    ) -> Result<Option<bool>> {
        let generation = catalog.change.unwrap_or(catalog.base);
        let native = if let Some(change) = catalog.change {
            let key = Self::vector_partition_change_generation_key_from_parts(
                &catalog.index,
                &catalog.partition_key,
                catalog.base.generation_id,
                change.generation_id,
            );
            let changes = write
                .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
                .map_err(Self::storage_error)?;
            let value = changes
                .get(key.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| corruption("native journal cleanup lost its generation"))?;
            let record: VectorPartitionChangeGenerationRecord = self
                .decode_expected_vector_partition_metadata(
                    VectorPartitionRecordKind::ChangeGeneration,
                    value.value(),
                    &catalog.index,
                    &catalog.partition_key,
                )?;
            let meta = PagedGenerationMeta::decode(&record.graph.bytes)?;
            let backings = write
                .open_table(CHANGE_BACKINGS)
                .map_err(Self::storage_error)?;
            meta.page_set.pages == MUTABLE_PAGE_SET
                || backings
                    .get(meta.storage_id.to_be_bytes().as_slice())
                    .map_err(Self::storage_error)?
                    .is_some()
        } else {
            let key = Self::vector_partition_base_generation_key_from_parts(
                &catalog.index,
                &catalog.partition_key,
                catalog.base.generation_id,
            );
            let bases = write
                .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
                .map_err(Self::storage_error)?;
            let value = bases
                .get(key.as_slice())
                .map_err(Self::storage_error)?
                .ok_or_else(|| corruption("native journal cleanup lost its base"))?;
            let record: VectorPartitionBaseGenerationRecord = self
                .decode_expected_vector_partition_metadata(
                    VectorPartitionRecordKind::BaseGeneration,
                    value.value(),
                    &catalog.index,
                    &catalog.partition_key,
                )?;
            PagedGenerationMeta::decode(&record.graph.bytes)?
                .page_set
                .pages
                == MUTABLE_PAGE_SET
        };
        if !native {
            return Ok(None);
        }
        let partition =
            VectorPartitionRef::new(catalog.index.clone(), catalog.partition_key.clone());
        let prefix = Self::vector_partition_prefix(&catalog.index, &catalog.partition_key);
        let mut covered = generation.covered_lsn;
        {
            let builds = write.open_table(BUILDS).map_err(Self::storage_error)?;
            if let Some(value) = builds.get(prefix.as_slice()).map_err(Self::storage_error)? {
                let build: PagedBuildRecord =
                    Self::decode_exact(open_value(&prefix, value.value())?).map_err(|_| {
                        corruption("native journal cleanup has a corrupt construction floor")
                    })?;
                covered = covered.min(build.progress()?.frontier_lsn);
            }
        }
        let mut journal = write
            .open_table(VECTOR_PARTITION_JOURNAL_TABLE)
            .map_err(Self::storage_error)?;
        let mut complete = false;
        for _ in 0..64 {
            checkpoint()?;
            let candidate = {
                let mut range = journal
                    .prefix_bounded(prefix.as_slice())
                    .map_err(Self::storage_error)?;
                let Some(entry) = range.next() else {
                    complete = true;
                    break;
                };
                let (key, value) = entry.map_err(Self::storage_error)?;
                let record: VectorPartitionJournalRecord = self
                    .decode_expected_vector_partition_metadata(
                        VectorPartitionRecordKind::Journal,
                        value.value(),
                        &catalog.index,
                        &catalog.partition_key,
                    )?;
                if Self::vector_partition_journal_key(&record).as_slice() != key.value() {
                    return Err(corruption(
                        "native journal cleanup found a substituted journal key",
                    ));
                }
                if record.lsn > covered {
                    complete = true;
                    break;
                }
                key.value().to_vec()
            };
            journal
                .remove(candidate.as_slice())
                .map_err(Self::storage_error)?;
        }
        #[cfg(any(test, feature = "test-seams"))]
        crate::database::maybe_pause_vector_journal_cleanup_for_test();
        // The sampled construction frontier retains every later record.
        // A replacement can therefore publish without losing its new tail.
        let _ = partition;
        Ok(Some(complete))
    }
}

impl RedbPersistence {
    /// Preserve the completed base topology while giving it a separately
    /// authenticated physical root. This is the initial serving-layout setup;
    /// subsequent native replacement builders write that layout directly.
    pub(crate) fn prepare_native_import_base(
        &self,
        expected: &VectorPartitionGenerationCatalogRecord,
        mut admit: impl FnMut(usize, usize) -> Result<()>,
    ) -> Result<Option<VectorPartitionGenerationCatalogRecord>> {
        if expected.change.is_some() {
            return Ok(None);
        }
        let partition =
            VectorPartitionRef::new(expected.index.clone(), expected.partition_key.clone());
        let base = self.load_vector_partition_base_generation(
            &expected.index,
            &expected.partition_key,
            expected.base.generation_id,
        )?;
        let meta = PagedGenerationMeta::decode(&base.graph.bytes)?;
        if meta.page_set.pages == MUTABLE_PAGE_SET {
            return Ok(Some(expected.clone()));
        }
        if self.load_paged_build_record(&partition)?.is_some() {
            return Ok(None);
        }
        let plan = crate::database::vector_paged_build::paged_batch_plan(&meta.declaration)?;
        admit(
            plan.max_node_bytes
                .checked_mul(3)
                .and_then(|bytes| bytes.checked_add(128 * 1024))
                .ok_or_else(|| corruption("native base page admission overflow"))?,
            usize::try_from(meta.page_set.bytes)
                .ok()
                .and_then(|bytes| bytes.checked_add(64 * 1024))
                .ok_or_else(|| corruption("native base disk admission overflow"))?,
        )?;
        let storage = self.allocate_native_tail_storage()?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let write = db.begin_write().map_err(Self::storage_error)?;
            let actual = {
                let catalogs = write.open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::vector_partition_catalog_key(expected);
                let value = catalogs.get(key.as_slice()).map_err(Self::storage_error)?
                    .ok_or_else(|| corruption("native base setup lost its catalog"))?;
                self.decode_expected_vector_partition_metadata::<VectorPartitionGenerationCatalogRecord>(
                    VectorPartitionRecordKind::GenerationCatalog, value.value(),
                    &expected.index, &expected.partition_key,
                )?
            };
            if actual != *expected { return Ok(None); }
            let page_name = page_table_name(storage);
            let table = TableDefinition::<&[u8], &[u8]>::new(&page_name);
            {
                let pages = read.open_table(PAGES).map_err(Self::storage_error)?;
                let old = storage_prefix(meta.storage_id);
                let end = writer_source::prefix_end(&old)
                    .ok_or_else(|| corruption("native base source has no successor"))?;
                let mut target = write.open_table(table).map_err(Self::storage_error)?;
                let mut source_root = PageSetHasher::new();
                for entry in pages.range::<&[u8]>(old.as_slice()..end.as_slice())
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    source_root.page(key.value(), value.value())?;
                    let payload = open_graph_page(key.value(), value.value())?;
                    if key.value().len() < 9 { return Err(route_page_corrupt()); }
                    let mut successor = key.value().to_vec();
                    successor[..8].copy_from_slice(&storage.to_be_bytes());
                    target.insert(successor.as_slice(), seal_value(&successor, payload).as_slice())
                        .map_err(Self::storage_error)?;
                }
                if source_root.finish() != meta.page_set { return Err(route_page_corrupt()); }
                let mut row_counter = storage.to_be_bytes().to_vec(); row_counter.push(6);
                let rows = meta.saved.as_ref().map_or(meta.progress.graph.points as u64, |saved| saved.distinct_rows);
                let row_value = seal_value(&row_counter, &rows.to_be_bytes());
                target.insert(row_counter.as_slice(), row_value.as_slice()).map_err(Self::storage_error)?;
                let bytes = meta.page_set.bytes.checked_add((row_counter.len() + row_value.len()) as u64)
                    .ok_or_else(|| corruption("native base byte count overflow"))?;
                let mut byte_counter = storage.to_be_bytes().to_vec(); byte_counter.push(7);
                target.insert(byte_counter.as_slice(), seal_value(&byte_counter, &bytes.to_be_bytes()).as_slice())
                    .map_err(Self::storage_error)?;
            }
            let root = write.table_root_fingerprint(table).map_err(Self::storage_error)?
                .ok_or_else(|| corruption("native base setup lost its page table"))?;
            let previous_storage = meta.storage_id;
            let bytes = {
                let pages = write.open_table(table).map_err(Self::storage_error)?;
                native_bytes_in(&pages, storage)?
            };
            let mut meta = meta.clone();
            meta.storage_id = storage;
            meta.progress.generation_id = storage;
            meta.page_set = PageSetRoot { pages: MUTABLE_PAGE_SET, bytes, digest: root };
            let route = Self::vector_partition_prefix(&expected.index, &expected.partition_key);
            if let Some(saved) = &mut meta.saved {
                saved.base_generation_id = storage;
            } else {
                let plan = crate::database::vector_paged_build::paged_batch_plan(&meta.declaration)?;
                meta.saved = Some(SavedGenerationBinding {
                    route: *blake3::hash(&route).as_bytes(), is_base: true,
                    base_generation_id: storage, base_tx: expected.base.covered_tx,
                    base_lsn: expected.base.covered_lsn, source_digest: root,
                    distinct_rows: meta.progress.graph.points as u64,
                    record_bytes: 0, record_digest: [0; 32], node_limit: plan.max_node_bytes as u64,
                    extend_candidates: meta.declaration.quantization != VectorQuantization::F32,
                    keep_pruned: meta.declaration.quantization != VectorQuantization::F32,
                    max_layer: 16, scale_bits: (1.0 / f64::ln(meta.declaration.hnsw_m as f64)).to_bits(),
                });
            }
            let mut base = base.clone();
            base.generation_id = storage;
            base.graph = VectorPartitionGraphBytes::verified(Self::encode(&meta)?);
            base.state_digest = *blake3::hash(&base.graph.bytes).as_bytes();
            let key = Self::vector_partition_base_generation_key(&base);
            let encoded = Self::encode_vector_partition_record(VectorPartitionRecordKind::BaseGeneration, &base)?;
            write.open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE).map_err(Self::storage_error)?
                .insert(key.as_slice(), encoded.as_slice()).map_err(Self::storage_error)?;
            let prepared = Self::prepare_vector_partition_graph_pages_with_binding(
                &base.index, &base.partition_key, storage, storage, true, &key, &base.graph, meta.schema_binding,
            )?;
            self.insert_preencoded_vector_partition_graph_pages_in_write(&write, &prepared)?;
            let mut catalog = expected.clone();
            catalog.base.generation_id = storage;
            catalog.base.durable_bytes = base.graph.bytes.len() as u64;
            let key = Self::vector_partition_catalog_key(&catalog);
            let encoded = Self::encode_vector_partition_record(VectorPartitionRecordKind::GenerationCatalog, &catalog)?;
            write.open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE).map_err(Self::storage_error)?
                .insert(key.as_slice(), encoded.as_slice()).map_err(Self::storage_error)?;
            write.open_table(CLEANUP).map_err(Self::storage_error)?
                .insert(previous_storage, [].as_slice()).map_err(Self::storage_error)?;
            // Publication changes no coverage frontier or default counters.
            let mut marker = storage.to_be_bytes().to_vec(); marker.push(0);
            let mut owner = (route.len() as u32).to_be_bytes().to_vec();
            owner.extend_from_slice(&route); owner.extend_from_slice(&root);
            write.open_table(PAGES).map_err(Self::storage_error)?
                .insert(marker.as_slice(), seal_value(&marker, &owner).as_slice()).map_err(Self::storage_error)?;
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)?;
            Ok(Some(catalog))
        })
    }

    /// One-time adoption of an existing saved change topology. Later imports
    /// append only their uncovered suffix to the same native serving storage.
    pub(crate) fn prepare_native_import_change(
        &self,
        expected: &VectorPartitionGenerationCatalogRecord,
        mut admit: impl FnMut(usize, usize) -> Result<()>,
    ) -> Result<Option<VectorPartitionGenerationCatalogRecord>> {
        let Some(identity) = expected.change else {
            return Ok(Some(expected.clone()));
        };
        let base = self.load_vector_partition_change_generation(
            &expected.index,
            &expected.partition_key,
            expected.base.generation_id,
            identity.generation_id,
        )?;
        if !base.graph.bytes.starts_with(&META_MAGIC) {
            return Ok(None);
        }
        let meta = PagedGenerationMeta::decode(&base.graph.bytes)?;
        if meta.page_set.pages == MUTABLE_PAGE_SET {
            return Ok(Some(expected.clone()));
        }
        if self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            Ok(resolve_native_change_backing(&read, &meta)?.storage_id != meta.storage_id)
        })? {
            return Ok(Some(expected.clone()));
        }
        let progress = meta.progress()?;
        let route = Self::vector_partition_prefix(&expected.index, &expected.partition_key);
        let saved = meta
            .saved
            .as_ref()
            .ok_or_else(|| corruption("saved change adoption has no topology binding"))?;
        if progress.generation_id != identity.generation_id
            || progress.phase != PagedVectorPhase::Complete
            || meta.declaration.dimension != base.dimension
            || meta.declaration.quantization != base.quantization
            || progress.frontier_tx != identity.covered_tx
            || progress.frontier_lsn != identity.covered_lsn
            || meta.declaration.policy_revision != identity.policy_revision
            || meta.declaration.hnsw_m != identity.hnsw_m
            || meta.declaration.hnsw_ef_construction != identity.hnsw_ef_construction
            || base.graph.bytes.len() as u64 != identity.durable_bytes
            || saved.route != *blake3::hash(&route).as_bytes()
            || saved.is_base
            || saved.base_generation_id != expected.base.generation_id
            || (saved.base_tx, saved.base_lsn)
                != (expected.base.covered_tx, expected.base.covered_lsn)
            || progress.declaration_digest != meta.declaration.digest()?
        {
            return Err(corruption(
                "saved change adoption disagrees with its exact logical identity",
            ));
        }
        let plan = crate::database::vector_paged_build::paged_batch_plan(&meta.declaration)?;
        admit(
            plan.max_node_bytes
                .checked_mul(3)
                .and_then(|bytes| bytes.checked_add(128 * 1024))
                .ok_or_else(|| corruption("native base page admission overflow"))?,
            usize::try_from(meta.page_set.bytes)
                .ok()
                .and_then(|bytes| bytes.checked_add(64 * 1024))
                .ok_or_else(|| corruption("native base disk admission overflow"))?,
        )?;
        let storage = self.allocate_native_tail_storage()?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let write = db.begin_write().map_err(Self::storage_error)?;
            let actual = {
                let catalogs = write.open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::vector_partition_catalog_key(expected);
                let value = catalogs.get(key.as_slice()).map_err(Self::storage_error)?
                    .ok_or_else(|| corruption("native base setup lost its catalog"))?;
                self.decode_expected_vector_partition_metadata::<VectorPartitionGenerationCatalogRecord>(
                    VectorPartitionRecordKind::GenerationCatalog, value.value(),
                    &expected.index, &expected.partition_key,
                )?
            };
            if actual != *expected { return Ok(None); }
            Self::validate_vector_schema_binding_in_write(&write, &expected.index, meta.schema_binding)?;
            let old_key = Self::vector_partition_change_generation_key(&base);
            {
                let changes = write.open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE).map_err(Self::storage_error)?;
                let value = changes.get(old_key.as_slice()).map_err(Self::storage_error)?
                    .ok_or_else(|| corruption("saved change adoption lost its exact old record"))?;
                let current: VectorPartitionChangeGenerationRecord = self.decode_expected_vector_partition_metadata(
                    VectorPartitionRecordKind::ChangeGeneration, value.value(), &expected.index, &expected.partition_key,
                )?;
                if current != base { return Err(corruption("saved change adoption old record changed")); }
            }
            let page_name = page_table_name(storage);
            let table = TableDefinition::<&[u8], &[u8]>::new(&page_name);
            {
                let pages = read.open_table(PAGES).map_err(Self::storage_error)?;
                let old = storage_prefix(meta.storage_id);
                let end = writer_source::prefix_end(&old)
                    .ok_or_else(|| corruption("native base source has no successor"))?;
                let mut target = write.open_table(table).map_err(Self::storage_error)?;
                let mut source_root = PageSetHasher::new();
                for entry in pages.range::<&[u8]>(old.as_slice()..end.as_slice())
                    .map_err(Self::storage_error)?
                {
                    let (key, value) = entry.map_err(Self::storage_error)?;
                    source_root.page(key.value(), value.value())?;
                    let payload = open_graph_page(key.value(), value.value())?;
                    if key.value().len() < 9 { return Err(route_page_corrupt()); }
                    let mut successor = key.value().to_vec();
                    successor[..8].copy_from_slice(&storage.to_be_bytes());
                    target.insert(successor.as_slice(), seal_value(&successor, payload).as_slice())
                        .map_err(Self::storage_error)?;
                }
                if source_root.finish() != meta.page_set { return Err(route_page_corrupt()); }
                let mut row_counter = storage.to_be_bytes().to_vec(); row_counter.push(6);
                let rows = meta.saved.as_ref().map_or(meta.progress.graph.points as u64, |saved| saved.distinct_rows);
                let row_value = seal_value(&row_counter, &rows.to_be_bytes());
                target.insert(row_counter.as_slice(), row_value.as_slice()).map_err(Self::storage_error)?;
                let bytes = meta.page_set.bytes.checked_add((row_counter.len() + row_value.len()) as u64)
                    .ok_or_else(|| corruption("native base byte count overflow"))?;
                let mut byte_counter = storage.to_be_bytes().to_vec(); byte_counter.push(7);
                target.insert(byte_counter.as_slice(), seal_value(&byte_counter, &bytes.to_be_bytes()).as_slice())
                    .map_err(Self::storage_error)?;
            }
            let root = write.table_root_fingerprint(table).map_err(Self::storage_error)?
                .ok_or_else(|| corruption("native base setup lost its page table"))?;
            let bytes = {
                let pages = write.open_table(table).map_err(Self::storage_error)?;
                native_bytes_in(&pages, storage)?
            };
            let mut physical = meta.clone();
            physical.storage_id = storage;
            physical.page_set = PageSetRoot { pages: MUTABLE_PAGE_SET, bytes, digest: root };
            // Exact original controls, including logical generation, source digest,
            // coverage and topology, remain unchanged. Only backing location differs.
            let binding = NativeChangeBacking { original: meta.clone(), physical };
            let payload = encode_native_change_backing(&binding)?;
            let key = meta.storage_id.to_be_bytes();
            write.open_table(CHANGE_BACKINGS).map_err(Self::storage_error)?
                .insert(key.as_slice(), seal_value(&key, &payload).as_slice()).map_err(Self::storage_error)?;
            let catalog = expected.clone();
            // Publication changes no coverage frontier or default counters.
            let mut marker = storage.to_be_bytes().to_vec(); marker.push(0);
            let mut owner = (route.len() as u32).to_be_bytes().to_vec();
            owner.extend_from_slice(&route); owner.extend_from_slice(&root);
            write.open_table(PAGES).map_err(Self::storage_error)?
                .insert(marker.as_slice(), seal_value(&marker, &owner).as_slice()).map_err(Self::storage_error)?;
            bind_pages_root(&write)?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            #[cfg(feature = "test-seams")]
            crate::writer_commit_test_seams::reached(crate::writer_commit_test_seams::CommitBoundary::GenerationBeforeRedb);
            write.commit().map_err(Self::storage_error)?;
            #[cfg(feature = "test-seams")]
            crate::writer_commit_test_seams::reached(crate::writer_commit_test_seams::CommitBoundary::GenerationAfterRedb);
            Ok(Some(catalog))
        })
    }
}

/// A mutable storage can be referenced by successive catalog records. An old
/// record's cleanup must never erase the graph the current catalog still serves.
pub(super) fn native_storage_is_selected_in_write(
    persistence: &RedbPersistence,
    write: &redb::WriteTransaction,
    storage: u64,
) -> Result<bool> {
    let mut key = storage.to_be_bytes().to_vec();
    key.push(0);
    let route = {
        let pages = write
            .open_table(PAGES)
            .map_err(RedbPersistence::storage_error)?;
        let value = pages
            .get(key.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| corruption("native cleanup has no authenticated route owner"))?;
        let payload = open_value(&key, value.value())?;
        let width: [u8; 4] = payload
            .get(..4)
            .ok_or_else(|| corruption("native owner is truncated"))?
            .try_into()
            .map_err(|_| corruption("native owner width is invalid"))?;
        let length = u32::from_be_bytes(width) as usize;
        if payload.len() != length.saturating_add(36) {
            return Err(corruption("native owner has invalid framing"));
        }
        payload[4..4 + length].to_vec()
    };
    let catalogs = write
        .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let Some(value) = catalogs
        .get(route.as_slice())
        .map_err(RedbPersistence::storage_error)?
    else {
        return Ok(false);
    };
    let catalog: VectorPartitionGenerationCatalogRecord = persistence
        .decode_vector_partition_record(
            VectorPartitionRecordKind::GenerationCatalog,
            value.value(),
        )?;
    if RedbPersistence::vector_partition_catalog_key(&catalog) != route {
        return Err(corruption("native owner and catalog route disagree"));
    }
    let base_key = RedbPersistence::vector_partition_base_generation_key_from_parts(
        &catalog.index,
        &catalog.partition_key,
        catalog.base.generation_id,
    );
    let bases = write
        .open_table(VECTOR_PARTITION_BASE_GENERATION_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let base_value = bases
        .get(base_key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| corruption("native physical owner lost its base record"))?;
    let base: VectorPartitionBaseGenerationRecord = persistence
        .decode_expected_vector_partition_metadata(
            VectorPartitionRecordKind::BaseGeneration,
            base_value.value(),
            &catalog.index,
            &catalog.partition_key,
        )?;
    if base.graph.bytes.starts_with(&META_MAGIC)
        && PagedGenerationMeta::decode(&base.graph.bytes)?.storage_id == storage
    {
        return Ok(true);
    }
    let Some(change) = catalog.change else {
        return Ok(false);
    };
    let key = RedbPersistence::vector_partition_change_generation_key_from_parts(
        &catalog.index,
        &catalog.partition_key,
        catalog.base.generation_id,
        change.generation_id,
    );
    let changes = write
        .open_table(VECTOR_PARTITION_CHANGE_GENERATION_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let value = changes
        .get(key.as_slice())
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| corruption("native physical owner lost its change record"))?;
    let record: VectorPartitionChangeGenerationRecord = persistence
        .decode_expected_vector_partition_metadata(
            VectorPartitionRecordKind::ChangeGeneration,
            value.value(),
            &catalog.index,
            &catalog.partition_key,
        )?;
    let meta = PagedGenerationMeta::decode(&record.graph.bytes)?;
    if meta.storage_id == storage {
        return Ok(true);
    }
    let backings = write
        .open_table(CHANGE_BACKINGS)
        .map_err(RedbPersistence::storage_error)?;
    let Some(value) = backings
        .get(meta.storage_id.to_be_bytes().as_slice())
        .map_err(RedbPersistence::storage_error)?
    else {
        return Ok(false);
    };
    let binding = decode_native_change_backing(meta.storage_id, value.value())?;
    if RedbPersistence::encode(&binding.original)? != record.graph.bytes {
        return Err(corruption(
            "native physical owner has a stale logical backing",
        ));
    }
    Ok(binding.physical.storage_id == storage)
}

impl RedbPersistence {
    /// Select one committed LSN cohort by a single authenticated suffix seek.
    pub(crate) fn native_candidate_first_uncovered_lsn(
        &self,
        partition: &VectorPartitionRef,
        covered: Lsn,
    ) -> Result<Option<Lsn>> {
        let Some((lower, upper)) = Self::vector_partition_journal_suffix_bounds(
            &partition.index,
            &partition.partition_key,
            covered,
            None,
        )?
        else {
            return Ok(None);
        };
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let journal = read
                .open_byte_table_bounded(VECTOR_PARTITION_JOURNAL_TABLE)
                .map_err(Self::storage_error)?;
            let mut range = journal
                .range(lower.as_slice()..upper.as_slice())
                .map_err(Self::storage_error)?;
            let Some(entry) = range.next() else {
                return Ok(None);
            };
            let (key, value) = entry.map_err(Self::storage_error)?;
            let record: VectorPartitionJournalRecord = self
                .decode_expected_vector_partition_metadata(
                    VectorPartitionRecordKind::Journal,
                    value.value(),
                    &partition.index,
                    &partition.partition_key,
                )?;
            if Self::vector_partition_journal_key(&record).as_slice() != key.value() {
                return Err(corruption(
                    "native candidate journal key disagrees with its record",
                ));
            }
            Ok(Some(record.lsn))
        })
    }
}

/// Catch up a completed replacement with one finite committed cohort. It
/// remains unpublished, and its durable BUILDS record resumes after restart.
pub(crate) struct NativeImportCandidate {
    pub(crate) source_owner: NativeImportTail,
}

impl PagedVectorPersistence for NativeImportCandidate {
    fn commit_batch(
        &self,
        previous: &PagedVectorProgress,
        plan: PagedVectorBatchPlan,
        batch: &mut dyn FnMut(&mut dyn PagedVectorTransaction) -> Result<PagedVectorProgress>,
    ) -> Result<PagedVectorCommittedBatch> {
        let owner = &self.source_owner;
        let key = RedbPersistence::vector_partition_prefix(
            &owner.partition.index,
            &owner.partition.partition_key,
        );
        let mut committed = None;
        (owner.hooks.gate)(&mut || {
            owner.persistence.with_db(|db| {
                let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
                RedbPersistence::validate_vector_schema_binding_in_write(
                    &write,
                    &owner.partition.index,
                    owner.schema_binding,
                )?;
                let mut record = {
                    let builds = write
                        .open_table(BUILDS)
                        .map_err(RedbPersistence::storage_error)?;
                    let value = builds
                        .get(key.as_slice())
                        .map_err(RedbPersistence::storage_error)?
                        .ok_or_else(|| corruption("native candidate lost its durable build"))?;
                    RedbPersistence::decode_exact::<PagedBuildRecord>(open_value(
                        &key,
                        value.value(),
                    )?)?
                };
                let mut durable = record.progress()?;
                durable.phase = PagedVectorPhase::Inserting;
                if record.storage_id != owner.storage_id || durable != *previous {
                    return Err(Error::Other(
                        "native candidate catch-up was superseded".into(),
                    ));
                }
                let next = {
                    let mut transaction = NativeBatch::open(
                        &owner.persistence,
                        &write,
                        &owner.partition,
                        owner.storage_id,
                        &owner.declaration,
                        previous,
                        plan,
                        &owner.hooks,
                        None,
                    )?;
                    let next = batch(&mut transaction)?;
                    transaction.finish()?;
                    next
                };
                let mut saved = next.clone();
                saved.phase = PagedVectorPhase::Complete;
                saved.encoded_points = saved.graph.points;
                saved.last_source_key = None;
                saved.encoding_cursor = None;
                record.progress = ProgressRecord::from_progress(&saved);
                let payload = RedbPersistence::encode(&record)?;
                write
                    .open_table(BUILDS)
                    .map_err(RedbPersistence::storage_error)?
                    .insert(key.as_slice(), seal_value(&key, &payload).as_slice())
                    .map_err(RedbPersistence::storage_error)?;
                vector_membership_pages::advance_native_candidate_frontier(
                    &write,
                    &owner.partition,
                    previous.frontier_tx,
                    next.frontier_tx,
                )?;
                let name = page_table_name(owner.storage_id);
                let table = TableDefinition::<&[u8], &[u8]>::new(&name);
                let digest = write
                    .table_root_fingerprint(table)
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| corruption("native candidate lost its point pages"))?;
                let meta =
                    PagedGenerationMeta::decode(&RedbPersistence::encode_paged_generation_meta(
                        owner.storage_id,
                        &owner.declaration,
                        &saved,
                        owner.schema_binding,
                        PageSetRoot {
                            pages: MUTABLE_PAGE_SET,
                            bytes: {
                                let pages = write
                                    .open_table(table)
                                    .map_err(RedbPersistence::storage_error)?;
                                native_bytes_in(&pages, owner.storage_id)?
                            },
                            digest,
                        },
                    )?)?;
                bind_pages_root(&write)?;
                RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                (owner.hooks.checkpoint)()?;
                write.commit().map_err(RedbPersistence::storage_error)?;
                committed = Some((next, meta));
                Ok(())
            })
        })?;
        let (progress, meta) =
            committed.ok_or_else(|| corruption("native candidate did not commit"))?;
        Ok(PagedVectorCommittedBatch {
            progress,
            source: owner.source(&meta)?,
        })
    }
}

impl RedbPersistence {
    /// Catch-up advances serving coverage, while unconsumed journal identities
    /// still need the replacement's original retained snapshot after restart.
    pub(crate) fn native_candidate_retention_floor(
        &self,
        partition: &VectorPartitionRef,
        frontier: TxId,
    ) -> Result<TxId> {
        let key = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            match read.open_table(FLOORS) {
                Ok(floors) => Ok(floors
                    .get(key.as_slice())
                    .map_err(Self::storage_error)?
                    .map_or(frontier, |value| frontier.min(TxId(value.value())))),
                Err(redb::TableError::TableDoesNotExist(_)) => Ok(frontier),
                Err(error) => Err(Self::storage_error(error)),
            }
        })
    }
}

impl NativeBatch<'_> {
    /// Track durable native bytes by changed keys, without asking redb to
    /// enumerate the table's allocation tree on each import.
    pub(super) fn write_graph_page(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let native = u64::from_be_bytes(self.storage) & MUTABLE_STORAGE != 0;
        let previous = if native {
            self.pages
                .get(key)
                .map_err(RedbPersistence::storage_error)?
                .map(|body| body.value().len())
        } else {
            None
        };
        self.pages
            .insert(key, value)
            .map_err(RedbPersistence::storage_error)?;
        if native {
            let mut counter_key = self.storage.to_vec();
            counter_key.push(7);
            let total = self
                .pages
                .get(counter_key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .map(|body| {
                    let encoded = open_value(&counter_key, body.value())?;
                    let encoded: [u8; 8] = encoded
                        .try_into()
                        .map_err(|_| corruption("native byte counter has invalid framing"))?;
                    Ok::<_, Error>(u64::from_be_bytes(encoded))
                })
                .transpose()?
                .unwrap_or(0);
            let old = previous.map_or(0, |bytes| bytes.saturating_add(key.len())) as u64;
            let new = value
                .len()
                .checked_add(key.len())
                .ok_or_else(|| corruption("native byte charge overflow"))?
                as u64;
            let total = total
                .checked_sub(old)
                .and_then(|bytes| bytes.checked_add(new))
                .ok_or_else(|| corruption("native byte counter overflow/underflow"))?;
            self.pages
                .insert(
                    counter_key.as_slice(),
                    seal_value(&counter_key, &total.to_be_bytes()).as_slice(),
                )
                .map_err(RedbPersistence::storage_error)?;
        }
        Ok(())
    }
}

pub(super) fn native_bytes_in<T: redb::ReadableTable<&'static [u8], &'static [u8]>>(
    pages: &T,
    storage: u64,
) -> Result<u64> {
    let mut key = storage.to_be_bytes().to_vec();
    key.push(7);
    let Some(value) = pages
        .get(key.as_slice())
        .map_err(RedbPersistence::storage_error)?
    else {
        return Ok(0);
    };
    let body = open_value(&key, value.value())?;
    let count: [u8; 8] = body
        .try_into()
        .map_err(|_| corruption("native byte counter has invalid framing"))?;
    u64::from_be_bytes(count)
        .checked_add((key.len() + value.value().len()) as u64)
        .ok_or_else(|| corruption("native byte count overflow"))
}

impl RedbPersistence {
    pub(crate) fn native_base_meta_bytes(
        &self,
        partition: &VectorPartitionRef,
        record: &PagedBuildRecord,
        progress: &PagedVectorProgress,
        page_set: PageSetRoot,
    ) -> Result<Vec<u8>> {
        let distinct_rows = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let name = page_table_name(record.storage_id);
            let pages = read
                .open_table(TableDefinition::<&[u8], &[u8]>::new(&name))
                .map_err(Self::storage_error)?;
            let mut key = record.storage_id.to_be_bytes().to_vec();
            key.push(6);
            let Some(value) = pages.get(key.as_slice()).map_err(Self::storage_error)? else {
                return Ok(0);
            };
            let bytes = open_value(&key, value.value())?;
            let bytes: [u8; 8] = bytes
                .try_into()
                .map_err(|_| corruption("native base row counter is invalid"))?;
            Ok(u64::from_be_bytes(bytes))
        })?;
        let mut meta = PagedGenerationMeta::decode(&Self::encode_paged_generation_meta(
            record.storage_id,
            &record.declaration,
            progress,
            record.schema_binding,
            page_set,
        )?)?;
        let route = Self::vector_partition_prefix(&partition.index, &partition.partition_key);
        let plan = crate::database::vector_paged_build::paged_batch_plan(&record.declaration)?;
        meta.saved = Some(SavedGenerationBinding {
            route: *blake3::hash(&route).as_bytes(),
            is_base: true,
            base_generation_id: progress.generation_id,
            base_tx: progress.frontier_tx,
            base_lsn: progress.frontier_lsn,
            source_digest: page_set.digest,
            distinct_rows,
            record_bytes: 0,
            record_digest: [0; 32],
            node_limit: plan.max_node_bytes as u64,
            extend_candidates: record.declaration.quantization != VectorQuantization::F32,
            keep_pruned: record.declaration.quantization != VectorQuantization::F32,
            max_layer: 16,
            scale_bits: (1.0 / f64::ln(record.declaration.hnsw_m as f64)).to_bits(),
        });
        Self::encode(&meta)
    }
}

pub(super) const CHANGE_BACKINGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_native_change_backings");

#[derive(Clone)]
pub(super) struct NativeChangeBacking {
    pub(super) original: PagedGenerationMeta,
    pub(super) physical: PagedGenerationMeta,
}

pub(super) fn decode_native_change_backing(
    storage: u64,
    frame: &[u8],
) -> Result<NativeChangeBacking> {
    if frame.len() > 64 * 1024 {
        return Err(corruption(
            "native change backing exceeds its admitted window",
        ));
    }
    let payload = open_value(&storage.to_be_bytes(), frame)?;
    let ((original, physical), consumed): ((&[u8], &[u8]), usize) =
        bincode::serde::borrow_decode_from_slice(payload, bincode::config::standard())
            .map_err(|_| corruption("native change backing cannot be decoded"))?;
    if consumed != payload.len() {
        return Err(corruption("native change backing has trailing bytes"));
    }
    let binding = NativeChangeBacking {
        original: PagedGenerationMeta::decode(original)?,
        physical: PagedGenerationMeta::decode(physical)?,
    };
    let mut normalized = binding.physical.clone();
    normalized.storage_id = binding.original.storage_id;
    normalized.page_set = binding.original.page_set;
    if binding.original.storage_id != storage
        || storage & MUTABLE_STORAGE != 0
        || binding.physical.storage_id & MUTABLE_STORAGE == 0
        || binding.physical.storage_id > i64::MAX as u64
        || binding.original.page_set.pages == MUTABLE_PAGE_SET
        || binding.physical.page_set.pages != MUTABLE_PAGE_SET
        || RedbPersistence::encode(&normalized)? != RedbPersistence::encode(&binding.original)?
    {
        return Err(corruption(
            "native change backing is not semantically equivalent",
        ));
    }
    Ok(binding)
}

pub(crate) fn resolve_native_change_backing(
    read: &redb::ReadTransaction,
    meta: &PagedGenerationMeta,
) -> Result<PagedGenerationMeta> {
    if meta.storage_id & MUTABLE_STORAGE != 0 {
        return Ok(meta.clone());
    }
    let table = match read.open_byte_table_bounded(CHANGE_BACKINGS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(meta.clone()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let Some(value) = table
        .get_bounded(meta.storage_id.to_be_bytes().as_slice())
        .map_err(RedbPersistence::storage_error)?
    else {
        return Ok(meta.clone());
    };
    let binding = decode_native_change_backing(meta.storage_id, value.value())?;
    if RedbPersistence::encode(&binding.original)? != RedbPersistence::encode(meta)? {
        return Err(corruption(
            "native change backing has a stale original descriptor",
        ));
    }
    Ok(binding.physical)
}

impl RedbPersistence {
    pub(crate) fn catalog_has_native_import_backing(
        &self,
        catalog: &VectorPartitionGenerationCatalogRecord,
    ) -> Result<bool> {
        let Some(change) = catalog.change else {
            return Ok(catalog.base.generation_id & MUTABLE_STORAGE != 0);
        };
        let record = self.load_vector_partition_change_generation(
            &catalog.index,
            &catalog.partition_key,
            catalog.base.generation_id,
            change.generation_id,
        )?;
        if !record.graph.bytes.starts_with(&META_MAGIC) {
            return Ok(false);
        }
        let meta = PagedGenerationMeta::decode(&record.graph.bytes)?;
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            Ok(resolve_native_change_backing(&read, &meta)?.storage_id & MUTABLE_STORAGE != 0)
        })
    }
}

pub(super) fn rebind_native_change_backing_in_write(
    write: &redb::WriteTransaction,
    original: &[u8],
    updated: &PagedGenerationMeta,
    route: &[u8],
) -> Result<()> {
    let mut table = write
        .open_table(CHANGE_BACKINGS)
        .map_err(RedbPersistence::storage_error)?;
    let binding = table
        .get(updated.storage_id.to_be_bytes().as_slice())
        .map_err(RedbPersistence::storage_error)?
        .map(|value| decode_native_change_backing(updated.storage_id, value.value()))
        .transpose()?;
    let Some(mut binding) = binding else {
        return Ok(());
    };
    if RedbPersistence::encode(&binding.original)? != original {
        return Err(corruption("rename has a stale native backing descriptor"));
    }
    let storage = binding.physical.storage_id;
    let root = binding.physical.page_set;
    binding.original = updated.clone();
    binding.physical = updated.clone();
    binding.physical.storage_id = storage;
    binding.physical.page_set = root;
    let payload = encode_native_change_backing(&binding)?;
    table
        .insert(
            updated.storage_id.to_be_bytes().as_slice(),
            seal_value(&updated.storage_id.to_be_bytes(), &payload).as_slice(),
        )
        .map_err(RedbPersistence::storage_error)?;
    let mut marker = storage.to_be_bytes().to_vec();
    marker.push(0);
    let mut owner = (route.len() as u32).to_be_bytes().to_vec();
    owner.extend_from_slice(route);
    owner.extend_from_slice(&root.digest);
    write
        .open_table(PAGES)
        .map_err(RedbPersistence::storage_error)?
        .insert(marker.as_slice(), seal_value(&marker, &owner).as_slice())
        .map_err(RedbPersistence::storage_error)?;
    Ok(())
}

fn encode_native_change_backing(binding: &NativeChangeBacking) -> Result<Vec<u8>> {
    RedbPersistence::encode(&(
        RedbPersistence::encode(&binding.original)?,
        RedbPersistence::encode(&binding.physical)?,
    ))
}
